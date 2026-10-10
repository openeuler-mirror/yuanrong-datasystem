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

/**
 * Description: Process-local estimated metadata memory admission.
 */
#ifndef DATASYSTEM_COMMON_UTIL_METADATA_MEMORY_LIMITER_H
#define DATASYSTEM_COMMON_UTIL_METADATA_MEMORY_LIMITER_H

#include <array>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_uint64(max_object_metadata_size_mb);

namespace datasystem {
class MetadataMemoryLimiter {
public:
    enum class Source { OBJECT, META };
    using Counter = std::function<uint64_t()>;

    ~MetadataMemoryLimiter() = default;

    // Counters must not retain their owning service or call back into this limiter.
    void RegisterCounter(Source source, Counter counter)
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        counters_[static_cast<size_t>(source)] = std::move(counter);
    }

    Status CheckAdmission() const
    {
        const uint64_t limitBytes = FLAGS_max_object_metadata_size_mb * MB_TO_BYTES;
        constexpr std::array<uint64_t, SOURCE_COUNT> bytesPerEntry = { 800, 1200 };
        std::array<uint64_t, SOURCE_COUNT> counts{};
        {
            std::shared_lock<std::shared_timed_mutex> lock(mutex_);
            for (size_t i = 0; i < counters_.size(); ++i) {
                counts[i] = counters_[i] ? counters_[i]() : 0;
            }
        }
        uint64_t bytes = 0;
        for (size_t i = 0; i < counts.size(); ++i) {
            if (counts[i] > (std::numeric_limits<uint64_t>::max() - bytes) / bytesPerEntry[i]) {
                RETURN_STATUS(K_OUT_OF_MEMORY, "Object metadata estimate overflow");
            }
            bytes += counts[i] * bytesPerEntry[i];
        }
        CHECK_FAIL_RETURN_STATUS(bytes <= limitBytes, K_OUT_OF_MEMORY,
            FormatString("Object metadata usage %llu bytes exceeds max_object_metadata_size_mb limit %llu bytes "
                         "(object=%llu, meta=%llu)",
                         static_cast<unsigned long long>(bytes), static_cast<unsigned long long>(limitBytes),
                         static_cast<unsigned long long>(counts[static_cast<size_t>(Source::OBJECT)]),
                         static_cast<unsigned long long>(counts[static_cast<size_t>(Source::META)])));
        return Status::OK();
    }

private:
    static constexpr size_t SOURCE_COUNT = 2;
    mutable std::shared_timed_mutex mutex_;
    std::array<Counter, SOURCE_COUNT> counters_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_METADATA_MEMORY_LIMITER_H
