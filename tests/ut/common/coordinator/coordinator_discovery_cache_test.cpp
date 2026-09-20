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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <set>
#include <thread>

#include "gtest/gtest.h"

namespace datasystem {
namespace {

class RefreshDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        ++calls_;
        serviceList = { "127.0.0.1:30002", "invalid", "127.0.0.1:30002" };
        return Status::OK();
    }

    size_t Calls() const
    {
        return calls_.load();
    }

private:
    std::atomic<size_t> calls_{ 0 };
};

TEST(CoordinatorDiscoveryCacheTest, RefreshesSnapshotOffTheCallingThread)
{
    auto discovery = std::make_shared<RefreshDiscovery>();
    CoordinatorDiscoveryCache cache(discovery, { "127.0.0.1:30001" });

    cache.RefreshAsync();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (cache.GetCandidateSnapshot() != std::vector<std::string>{ "127.0.0.1:30002" }
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(cache.GetCandidateSnapshot(), (std::vector<std::string>{ "127.0.0.1:30002" }));
}

class OrderedDiscovery final : public ICoordinatorDiscovery {
public:
    const std::vector<std::string> candidates{
        "127.0.0.1:30001", "127.0.0.1:30002", "127.0.0.1:30003", "127.0.0.1:30004", "127.0.0.1:30005"
    };

    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        serviceList = candidates;
        serviceList.push_back("invalid");
        serviceList.push_back(candidates.front());
        return Status::OK();
    }
};

TEST(CoordinatorDiscoveryCacheTest, SpreadsInitialCandidatesWithoutChangingMembership)
{
    auto discovery = std::make_shared<OrderedDiscovery>();
    std::vector<std::string> initial;
    ASSERT_TRUE(discovery->GetCoordinators(initial).IsOk());
    std::set<std::vector<std::string>> orders;
    for (size_t i = 0; i < 16; ++i) {
        CoordinatorDiscoveryCache cache(discovery, initial);
        auto snapshot = cache.GetCandidateSnapshot();
        orders.insert(snapshot);
        EXPECT_EQ(snapshot, cache.GetCandidateSnapshot());
        std::sort(snapshot.begin(), snapshot.end());
        EXPECT_EQ(snapshot, discovery->candidates);
    }
    EXPECT_GT(orders.size(), 1U);
}

TEST(CoordinatorDiscoveryCacheTest, SpreadsRefreshedCandidatesWithoutChangingMembership)
{
    auto discovery = std::make_shared<OrderedDiscovery>();
    std::set<std::vector<std::string>> orders;
    for (size_t i = 0; i < 16; ++i) {
        CoordinatorDiscoveryCache cache(discovery, { "127.0.0.1:30006" });
        cache.RefreshAsync();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        auto snapshot = cache.GetCandidateSnapshot();
        while (snapshot.size() != discovery->candidates.size() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            snapshot = cache.GetCandidateSnapshot();
        }
        ASSERT_EQ(snapshot.size(), discovery->candidates.size());
        orders.insert(snapshot);
        std::sort(snapshot.begin(), snapshot.end());
        EXPECT_EQ(snapshot, discovery->candidates);
    }
    EXPECT_GT(orders.size(), 1U);
}

}  // namespace
}  // namespace datasystem
