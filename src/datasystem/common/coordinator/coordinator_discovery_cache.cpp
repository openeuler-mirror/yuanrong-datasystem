/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/common/coordinator/coordinator_discovery_cache.h"

#include <unordered_set>
#include <utility>

#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace {
constexpr uint32_t DISCOVERY_FAILURE_LOG_EVERY_N = 100;

std::vector<std::string> ValidCandidates(const std::vector<std::string> &candidates)
{
    std::vector<std::string> valid;
    valid.reserve(candidates.size());
    std::unordered_set<std::string> seen;
    for (const auto &candidate : candidates) {
        HostPort address;
        if (address.ParseString(candidate).IsOk() && !address.Empty() && seen.emplace(address.ToString()).second) {
            valid.emplace_back(address.ToString());
        }
    }
    return valid;
}
}  // namespace

CoordinatorDiscoveryCache::CoordinatorDiscoveryCache(std::shared_ptr<ICoordinatorDiscovery> discovery,
                                                     std::vector<std::string> initialCandidates)
    : discovery_(std::move(discovery)),
      candidates_(ValidCandidates(initialCandidates)),
      thread_([this] { Run(); })
{
}

CoordinatorDiscoveryCache::~CoordinatorDiscoveryCache()
{
    LOG(INFO) << "Coordinator Discovery cache destruction started";
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_one();
    if (thread_.joinable()) {
        thread_.join();
    }
    LOG(INFO) << "Coordinator Discovery cache destruction finished";
}

std::vector<std::string> CoordinatorDiscoveryCache::GetCandidateSnapshot() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return candidates_;
}

void CoordinatorDiscoveryCache::RefreshAsync()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        refreshRequested_ = true;
    }
    cv_.notify_one();
}

void CoordinatorDiscoveryCache::Run()
{
    while (WaitForRefresh()) {
        std::vector<std::string> discovered;
        Status status;
        try {
            status = discovery_->GetCoordinators(discovered);
        } catch (...) {
            status = Status(K_RUNTIME_ERROR, "Coordinator Discovery refresh threw an exception");
        }
        if (status.IsError()) {
            LOG_EVERY_N(WARNING, DISCOVERY_FAILURE_LOG_EVERY_N)
                << "Coordinator Discovery refresh failed: " << status.ToString();
            continue;
        }
        auto valid = ValidCandidates(discovered);
        if (valid.empty()) {
            LOG_EVERY_N(WARNING, DISCOVERY_FAILURE_LOG_EVERY_N)
                << "Coordinator Discovery refresh returned no valid candidates";
            continue;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        candidates_ = std::move(valid);
    }
}

bool CoordinatorDiscoveryCache::WaitForRefresh()
{
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return stopping_ || refreshRequested_; });
    refreshRequested_ = false;
    return !stopping_;
}

}  // namespace datasystem
