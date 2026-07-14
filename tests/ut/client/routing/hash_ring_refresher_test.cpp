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

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/routing/hash_ring_refresher.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

class HashRingRefresherTest : public CommonTest {
protected:
    static void FillRing(::datasystem::ClusterTopologyPb &ring, std::unordered_map<std::string, std::string> &hostIdMap,
                         const std::string &address = "127.0.0.1:1000")
    {
        auto &worker = (*ring.mutable_members())[address];
        worker.set_state(::datasystem::MembershipPb::ACTIVE);
        worker.add_tokens(100u);
        hostIdMap[address] = "host-a";
    }
};

TEST_F(HashRingRefresherTest, TestInitialFetchRunsBeforePeriodicThread)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    int fetchCount = 0;
    auto fetch = [&fetchCount](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                               uint64_t &newVersion, bool &changed,
                               std::unordered_map<std::string, std::string> &hostIdMap) {
        ++fetchCount;
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = true;
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    EXPECT_EQ(fetchCount, 1);

    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::SelectStrategy::HASH_RING_AFFINITY, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:1000");
}

TEST_F(HashRingRefresherTest, TestForceRefreshDuringFetchIsNotLost)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    int fetchCount = 0;
    bool secondFetchStarted = false;
    bool releaseSecondFetch = false;
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::unique_lock<std::mutex> lock(mutex);
        const int currentFetchCount = ++fetchCount;
        if (currentFetchCount == 2) {
            secondFetchStarted = true;
            cv.notify_all();
            cv.wait(lock, [&] { return releaseSecondFetch; });
        }
        lock.unlock();
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = currentFetchCount == 1;
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    bool started = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        started = cv.wait_for(lock, std::chrono::seconds(2), [&] { return secondFetchStarted; });
    }
    if (!started) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            releaseSecondFetch = true;
        }
        cv.notify_all();
        refresher.Stop();
        FAIL() << "Periodic refresh did not start";
    }

    refresher.ForceRefresh();
    {
        std::lock_guard<std::mutex> lock(mutex);
        releaseSecondFetch = true;
    }
    cv.notify_all();

    bool refreshed = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        refreshed = cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 3; });
    }
    refresher.Stop();
    EXPECT_TRUE(refreshed);
}

TEST_F(HashRingRefresherTest, TestSuccessfulRefreshUpdatesWorkerCandidates)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::string> fetchedWorkers;
    auto fetch = [&](const HostPort &worker, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            fetchedWorkers.emplace_back(worker.ToString());
        }
        FillRing(ring, hostIdMap, "127.0.0.1:2000");
        newVersion = 1;
        changed = true;
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    bool fetchedUpdatedWorker = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        fetchedUpdatedWorker = cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return fetchedWorkers.size() >= 2 && fetchedWorkers[1] == "127.0.0.1:2000";
        });
    }
    refresher.Stop();
    EXPECT_TRUE(fetchedUpdatedWorker);
}

TEST_F(HashRingRefresherTest, TestInvalidRefreshIntervalFails)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    client::HashRingRefresher refresher(router, fetch);

    EXPECT_EQ(refresher.StartPeriodicRefresh(0).GetCode(), K_INVALID);
}

TEST_F(HashRingRefresherTest, TestInitialFetchValidatesDependencies)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    client::HashRingRefresher noFetch(router, {});
    EXPECT_EQ(noFetch.InitialFetch(HostPort("127.0.0.1", 1000)).GetCode(), K_INVALID);

    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    client::HashRingRefresher noRouter(nullptr, fetch);
    EXPECT_TRUE(noRouter.InitialFetch(HostPort("127.0.0.1", 1000)).IsError());

    client::HashRingRefresher invalidAddress(router, fetch);
    EXPECT_EQ(invalidAddress.InitialFetch(HostPort()).GetCode(), K_INVALID);
}

}  // namespace ut
}  // namespace datasystem
