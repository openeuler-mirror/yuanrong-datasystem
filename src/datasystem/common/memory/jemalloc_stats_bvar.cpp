/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/common/memory/jemalloc_stats_bvar.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>

#include <bvar/variable.h>
#include <dlfcn.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::memory {
namespace {

using MallctlFunc = int (*)(const char *, void *, size_t *, void *, size_t);

MallctlFunc ResolveProcessMallctl()
{
    return reinterpret_cast<MallctlFunc>(reinterpret_cast<intptr_t>(dlsym(RTLD_DEFAULT, "mallctl")));
}

}  // namespace

class JemallocStatsBvar::Impl {
public:
    Impl() : mallctlFunc_(ResolveProcessMallctl())
    {
    }

    ~Impl()
    {
        Shutdown();
    }

    Status Init()
    {
        if (initialized_) {
            return Status::OK();
        }
        for (size_t i = 0; i < bvars_.size(); ++i) {
            auto bvar = std::make_unique<OnDemandBvar>(this, static_cast<Metric>(i));
            if (bvar->Init(kBvarNames[i]) != 0) {
                const std::string name = kBvarNames[i];
                Shutdown();
                return Status(StatusCode::K_RUNTIME_ERROR, "Failed to expose bvar: " + name);
            }
            bvars_[i] = std::move(bvar);
        }
        initialized_ = true;
        return Status::OK();
    }

    void Shutdown()
    {
        for (auto &bvar : bvars_) {
            bvar.reset();
        }
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot_ = Snapshot{};
        lastRefreshTime_ = {};
        readFailures_ = 0;
        hasRefreshTime_ = false;
        statsAvailable_ = false;
        initialized_ = false;
    }

private:
    enum class Stat : size_t {
        ALLOCATED,
        ACTIVE,
        RESIDENT,
        METADATA,
        MAPPED,
        RETAINED,
        DIRTY,
        MUZZY,
        COUNT
    };

    enum class Metric : size_t {
        ALLOCATED,
        ACTIVE,
        RESIDENT,
        METADATA,
        MAPPED,
        RETAINED,
        DIRTY,
        MUZZY,
        AVAILABLE,
        READ_FAILURES,
        COUNT
    };

    static constexpr size_t kStatCount = static_cast<size_t>(Stat::COUNT);
    static constexpr size_t kMetricCount = static_cast<size_t>(Metric::COUNT);
    static constexpr auto kStatsCacheInterval = std::chrono::seconds(1);
    static constexpr uint32_t kFailureLogInterval = 60;
    static constexpr char kEpochName[] = "epoch";
    static constexpr char kPageSizeName[] = "arenas.page";
    static constexpr std::array<const char *, 6> kByteStatNames{
        "stats.allocated", "stats.active", "stats.resident", "stats.metadata", "stats.mapped", "stats.retained",
    };
    // jemalloc 5.3 defines MALLCTL_ARENAS_ALL as 4096 for aggregate arena stats.
    static constexpr char kDirtyPagesName[] = "stats.arenas.4096.pdirty";
    static constexpr char kMuzzyPagesName[] = "stats.arenas.4096.pmuzzy";
    static_assert(kByteStatNames.size() == static_cast<size_t>(Stat::DIRTY));
    static_assert(kStatCount == static_cast<size_t>(Metric::AVAILABLE));

    class OnDemandBvar final : public bvar::Variable {
    public:
        OnDemandBvar(Impl *owner, Metric metric) : owner_(owner), metric_(metric)
        {
        }

        ~OnDemandBvar() override
        {
            hide();
        }

        int Init(const char *name)
        {
            return expose(name);
        }

        void describe(std::ostream &output, bool) const override
        {
            output << owner_->GetMetric(metric_);
        }

    private:
        Impl *owner_;
        Metric metric_;
    };

    struct Snapshot {
        std::array<uint64_t, kStatCount> values{};
    };

    static constexpr std::array<const char *, kMetricCount> kBvarNames{
        "anon_jemalloc_allocated_bytes",
        "anon_jemalloc_active_bytes",
        "anon_jemalloc_resident_bytes",
        "anon_jemalloc_metadata_bytes",
        "anon_jemalloc_mapped_bytes",
        "anon_jemalloc_retained_bytes",
        "anon_jemalloc_dirty_bytes",
        "anon_jemalloc_muzzy_bytes",
        "anon_jemalloc_stats_available",
        "anon_jemalloc_stats_read_failures",
    };

    uint64_t GetMetric(Metric metric)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        RefreshIfExpiredLocked();
        if (metric == Metric::AVAILABLE) {
            return statsAvailable_ ? 1 : 0;
        }
        if (metric == Metric::READ_FAILURES) {
            return readFailures_;
        }
        return snapshot_.values[static_cast<size_t>(metric)];
    }

    void RefreshIfExpiredLocked()
    {
        const auto now = std::chrono::steady_clock::now();
        if (hasRefreshTime_ && now - lastRefreshTime_ < kStatsCacheInterval) {
            return;
        }
        hasRefreshTime_ = true;
        lastRefreshTime_ = now;

        Snapshot candidate;
        const char *failedControl = nullptr;
        const int errorCode = RefreshSnapshot(candidate, failedControl);
        if (errorCode == 0) {
            snapshot_ = candidate;
            statsAvailable_ = true;
            return;
        }

        statsAvailable_ = false;
        ++readFailures_;
        LOG_FIRST_AND_EVERY_N(ERROR, kFailureLogInterval)
            << "Failed to refresh process jemalloc statistics, mallctl=" << failedControl << ", rc=" << errorCode;
    }

    int RefreshSnapshot(Snapshot &candidate, const char *&failedControl) const
    {
        if (mallctlFunc_ == nullptr) {
            failedControl = "mallctl(symbol)";
            return ENOENT;
        }

        uint64_t epoch = 1;
        size_t epochSize = sizeof(epoch);
        int rc = mallctlFunc_(kEpochName, &epoch, &epochSize, &epoch, sizeof(epoch));
        if (rc != 0) {
            failedControl = kEpochName;
            return rc;
        }

        size_t value = 0;
        for (size_t i = 0; i < kByteStatNames.size(); ++i) {
            rc = ReadSize(kByteStatNames[i], value);
            if (rc != 0) {
                failedControl = kByteStatNames[i];
                return rc;
            }
            candidate.values[i] = static_cast<uint64_t>(value);
        }

        rc = ReadSize(kPageSizeName, value);
        if (rc != 0) {
            failedControl = kPageSizeName;
            return rc;
        }
        const uint64_t pageSize = static_cast<uint64_t>(value);
        uint64_t pageBytes = 0;
        rc = ReadPageBytes(kDirtyPagesName, pageSize, pageBytes);
        if (rc != 0) {
            failedControl = kDirtyPagesName;
            return rc;
        }
        candidate.values[static_cast<size_t>(Stat::DIRTY)] = pageBytes;
        rc = ReadPageBytes(kMuzzyPagesName, pageSize, pageBytes);
        if (rc != 0) {
            failedControl = kMuzzyPagesName;
            return rc;
        }
        candidate.values[static_cast<size_t>(Stat::MUZZY)] = pageBytes;
        return 0;
    }

    int ReadSize(const char *name, size_t &value) const
    {
        value = 0;
        size_t valueSize = sizeof(value);
        return mallctlFunc_(name, &value, &valueSize, nullptr, 0);
    }

    int ReadPageBytes(const char *name, uint64_t pageSize, uint64_t &value) const
    {
        size_t pages = 0;
        const int rc = ReadSize(name, pages);
        if (rc != 0) {
            return rc;
        }
        if (pageSize == 0 || pages > std::numeric_limits<uint64_t>::max() / pageSize) {
            return EOVERFLOW;
        }
        value = static_cast<uint64_t>(pages) * pageSize;
        return 0;
    }

    MallctlFunc mallctlFunc_{ nullptr };
    std::mutex mutex_;
    Snapshot snapshot_;
    std::chrono::steady_clock::time_point lastRefreshTime_{};
    uint64_t readFailures_{ 0 };
    bool hasRefreshTime_{ false };
    bool statsAvailable_{ false };
    bool initialized_{ false };
    std::array<std::unique_ptr<OnDemandBvar>, kMetricCount> bvars_{};
};

JemallocStatsBvar::JemallocStatsBvar() : impl_(std::make_unique<Impl>())
{
}

JemallocStatsBvar::~JemallocStatsBvar() = default;

Status JemallocStatsBvar::Init()
{
    return impl_->Init();
}

void JemallocStatsBvar::Shutdown()
{
    impl_->Shutdown();
}

}  // namespace datasystem::memory
