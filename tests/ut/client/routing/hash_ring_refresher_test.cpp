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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/object_cache/routing/hash_ring_refresher.h"
#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/client/object_cache/routing/worker_router.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace client {
class HashRingRefresherTestPeer {
public:
    static std::string Digest(const ClusterTopologyPb &ring)
    {
        return HashRingRefresher::BuildRingDigest(ring);
    }

    static Status Refresh(HashRingRefresher &refresher)
    {
        return refresher.DoRefresh(false);
    }
};
}  // namespace client

namespace ut {

class RecordingFilter : public client::IWorkerFilter {
public:
    bool IsAvailable(const HostPort &, client::WorkerAccessAction action) const override
    {
        (void)action;
        return true;
    }

    void OnHashRingUpdated(const ::datasystem::ClusterTopologyPb &) override
    {
        updateCount_.fetch_add(1, std::memory_order_release);
    }

    int UpdateCount() const
    {
        return updateCount_.load(std::memory_order_acquire);
    }

private:
    std::atomic<int> updateCount_{ 0 };
};

class HashRingRefresherTest : public CommonTest {
protected:
    static void FillRing(::datasystem::ClusterTopologyPb &ring, std::unordered_map<std::string, std::string> &hostIdMap,
                         const std::string &address = "127.0.0.1:1000")
    {
        ring.set_tokens_per_member(1);
        auto &worker = (*ring.mutable_members())[address];
        worker.set_state(::datasystem::MembershipPb::ACTIVE);
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
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
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

TEST_F(HashRingRefresherTest, TestConcurrentForceRefreshKeepsRetryInterval)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    std::mutex waitMutex;
    std::condition_variable waitCv;
    std::vector<std::chrono::milliseconds> requestedWaits;
    std::atomic<size_t> releasedWaits{ 0 };
    std::atomic<std::condition_variable *> refresherCv{ nullptr };
    int fetchCount = 0;
    bool releaseForcedFetch = false;
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::unique_lock<std::mutex> lock(mutex);
        const int currentFetchCount = ++fetchCount;
        cv.notify_all();
        if (currentFetchCount == 3) {
            cv.wait(lock, [&] { return releaseForcedFetch; });
        }
        lock.unlock();
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = currentFetchCount == 1;
        return Status::OK();
    };
    auto wait = [&](std::condition_variable &refreshCv, std::unique_lock<std::mutex> &lock,
                    std::chrono::milliseconds duration, const std::function<bool()> &wakePredicate) {
        size_t waitSequence;
        {
            std::lock_guard<std::mutex> waitLock(waitMutex);
            requestedWaits.emplace_back(duration);
            waitSequence = requestedWaits.size();
            refresherCv.store(&refreshCv, std::memory_order_release);
        }
        waitCv.notify_all();
        refreshCv.wait(lock, [&] {
            return wakePredicate() || releasedWaits.load(std::memory_order_acquire) >= waitSequence;
        });
    };
    client::HashRingRefresher refresher(router, fetch, {}, wait);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 2; }));
    }
    {
        std::unique_lock<std::mutex> lock(waitMutex);
        ASSERT_TRUE(waitCv.wait_for(lock, std::chrono::seconds(2), [&] { return requestedWaits.size() >= 1; }));
        EXPECT_EQ(requestedWaits[0], std::chrono::seconds(60));
    }
    refresher.ForceRefresh();
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 3; }));
    }

    std::vector<std::thread> callers;
    callers.reserve(64);
    for (int i = 0; i < 64; ++i) {
        callers.emplace_back([&refresher] { refresher.ForceRefresh(); });
    }
    for (auto &caller : callers) {
        caller.join();
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        releaseForcedFetch = true;
    }
    cv.notify_all();

    {
        std::unique_lock<std::mutex> lock(waitMutex);
        ASSERT_TRUE(waitCv.wait_for(lock, std::chrono::seconds(2), [&] { return requestedWaits.size() >= 2; }));
        EXPECT_EQ(requestedWaits[1], std::chrono::milliseconds(250));
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        EXPECT_EQ(fetchCount, 3);
    }

    releasedWaits.store(2, std::memory_order_release);
    refresherCv.load(std::memory_order_acquire)->notify_all();
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 4; }));
    }
    refresher.Stop();
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

TEST_F(HashRingRefresherTest, ForceRefreshCoalescesOneRetryWindow)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    client::HashRingRefresher refresher(
        router, [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                   std::unordered_map<std::string, std::string> &) { return Status::OK(); });

    EXPECT_TRUE(refresher.ForceRefresh());
    EXPECT_FALSE(refresher.ForceRefresh());
}

TEST_F(HashRingRefresherTest, ForceRefreshSynchronizesWithWaitTransition)
{
    constexpr char injectPoint[] = "HashRingRefresher.RefreshLoop.beforeWait";
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    client::HashRingRefresher refresher(
        router, [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                   std::unordered_map<std::string, std::string> &) { return Status::OK(); });

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    for (size_t retry = 0; retry < 2'000 && inject::GetExecuteCount(injectPoint) == 0; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GT(inject::GetExecuteCount(injectPoint), 0);
    auto force = std::async(std::launch::async, [&] { return refresher.ForceRefresh(); });
    EXPECT_EQ(force.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    DS_ASSERT_OK(inject::Clear(injectPoint));
    ASSERT_EQ(force.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_TRUE(force.get());
    refresher.Stop();
}

TEST_F(HashRingRefresherTest, DeadlineExtensionAtExpiryKeepsForcedRefreshCadence)
{
    constexpr char injectPoint[] = "HashRingRefresher.RefreshLoop.afterDeadlineRead";
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::atomic<int> fetchCount{ 0 };
    client::HashRingRefresher refresher(
        router, [&fetchCount](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &,
                              bool &, std::unordered_map<std::string, std::string> &) {
            fetchCount.fetch_add(1, std::memory_order_release);
            return Status::OK();
        });

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    for (size_t retry = 0; retry < 2'000 && fetchCount.load(std::memory_order_acquire) < 2; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GE(fetchCount.load(std::memory_order_acquire), 2);

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    ASSERT_TRUE(refresher.ForceRefresh());
    for (size_t retry = 0; retry < 2'000 && inject::GetExecuteCount(injectPoint) == 0; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GT(inject::GetExecuteCount(injectPoint), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(5'700));
    EXPECT_FALSE(refresher.ForceRefresh());
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    const int fetchesBeforeResume = fetchCount.load(std::memory_order_acquire);
    DS_ASSERT_OK(inject::Clear(injectPoint));

    for (size_t retry = 0; retry < 1'500 && fetchCount.load(std::memory_order_acquire) == fetchesBeforeResume;
         ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    refresher.Stop();
    EXPECT_GT(fetchCount.load(std::memory_order_acquire), fetchesBeforeResume);
}

TEST_F(HashRingRefresherTest, StopSynchronizesWithWaitTransition)
{
    constexpr char injectPoint[] = "HashRingRefresher.RefreshLoop.afterWaitPredicateRead";
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    client::HashRingRefresher refresher(
        router, [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                   std::unordered_map<std::string, std::string> &) { return Status::OK(); });

    DS_ASSERT_OK(inject::Set(injectPoint, "pause"));
    Raii clearInject([&] { (void)inject::Clear(injectPoint); });
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    for (size_t retry = 0; retry < 2'000 && inject::GetExecuteCount(injectPoint) == 0; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GT(inject::GetExecuteCount(injectPoint), 0);
    auto stop = std::async(std::launch::async, [&] { refresher.Stop(); });

    DS_ASSERT_OK(inject::Clear(injectPoint));
    const bool stoppedWithoutRecovery = stop.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready;
    if (!stoppedWithoutRecovery) {
        (void)refresher.ForceRefresh();
    }
    EXPECT_TRUE(stoppedWithoutRecovery);
    EXPECT_EQ(stop.wait_for(std::chrono::seconds(1)), std::future_status::ready);
}

TEST_F(HashRingRefresherTest, TestRingUpdateHookRunsBeforeRoutePublication)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                    uint64_t &newVersion, bool &changed,
                    std::unordered_map<std::string, std::string> &hostIdMap) {
        FillRing(ring, hostIdMap);
        newVersion = 5;
        changed = true;
        return Status::OK();
    };
    uint64_t hookVersion = 0;
    bool routeWasUnpublished = false;
    auto hook = [router, &hookVersion, &routeWasUnpublished](
        uint64_t version, const ::datasystem::ClusterTopologyPb &ring,
        const std::unordered_map<std::string, std::string> &, bool) {
        hookVersion = version;
        EXPECT_EQ(ring.members_size(), 1);
        HostPort selected;
        routeWasUnpublished =
            router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected).IsError();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch, hook);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    EXPECT_EQ(hookVersion, 5u);
    EXPECT_TRUE(routeWasUnpublished);
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:1000");
}

TEST_F(HashRingRefresherTest, InvalidTopologyDoesNotRunUpdateHook)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                    uint64_t &newVersion, bool &changed,
                    std::unordered_map<std::string, std::string> &hostIdMap) {
        FillRing(ring, hostIdMap);
        ring.set_tokens_per_member(0);
        newVersion = 5;
        changed = true;
        return Status::OK();
    };
    int hookCount = 0;
    auto hook = [&hookCount](uint64_t, const ::datasystem::ClusterTopologyPb &,
                             const std::unordered_map<std::string, std::string> &, bool) {
        ++hookCount;
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch, hook);

    EXPECT_EQ(refresher.InitialFetch(HostPort("127.0.0.1", 1000)).GetCode(), K_INVALID);
    EXPECT_EQ(hookCount, 0);
    HostPort selected;
    EXPECT_TRUE(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected).IsError());
}

TEST_F(HashRingRefresherTest, TestFailedRingUpdateHookRetainsVersionAndRetries)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<uint64_t> requestedVersions;
    int hookCount = 0;
    auto fetch = [&mutex, &requestedVersions](const HostPort &, uint64_t currentVersion,
                                              ::datasystem::ClusterTopologyPb &ring, std::string &,
                                              uint64_t &newVersion, bool &changed,
                                              std::unordered_map<std::string, std::string> &hostIdMap) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            requestedVersions.push_back(currentVersion);
        }
        FillRing(ring, hostIdMap);
        newVersion = 6;
        changed = true;
        return Status::OK();
    };
    auto hook = [&mutex, &cv, &hookCount](uint64_t, const ::datasystem::ClusterTopologyPb &,
                                         const std::unordered_map<std::string, std::string> &, bool) {
        std::lock_guard<std::mutex> lock(mutex);
        ++hookCount;
        cv.notify_all();
        return hookCount == 1 ? Status(K_RUNTIME_ERROR, "injected snapshot rejection") : Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch, hook);

    EXPECT_EQ(refresher.InitialFetch(HostPort("127.0.0.1", 1000)).GetCode(), K_RUNTIME_ERROR);
    HostPort selected;
    EXPECT_TRUE(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected).IsError());
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    bool retried = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        retried = cv.wait_for(lock, std::chrono::seconds(2), [&] { return hookCount >= 2; });
    }
    refresher.Stop();

    ASSERT_TRUE(retried);
    ASSERT_GE(requestedVersions.size(), 2u);
    EXPECT_EQ(requestedVersions[0], 0u);
    EXPECT_EQ(requestedVersions[1], 0u);
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
}

TEST_F(HashRingRefresherTest, TestStaleVersionDoesNotReplaceCurrentRing)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<uint64_t> requestedVersions;
    int fetchCount = 0;
    auto fetch = [&](const HostPort &, uint64_t currentVersion, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::lock_guard<std::mutex> lock(mutex);
        requestedVersions.emplace_back(currentVersion);
        ++fetchCount;
        if (fetchCount == 1) {
            FillRing(ring, hostIdMap, "127.0.0.1:1000");
            newVersion = 2;
        } else {
            FillRing(ring, hostIdMap, "127.0.0.1:2000");
            newVersion = 1;
        }
        changed = true;
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    bool fetchedStaleVersion = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        fetchedStaleVersion = cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 2; });
    }
    refresher.Stop();

    ASSERT_TRUE(fetchedStaleVersion);
    ASSERT_GE(requestedVersions.size(), 2u);
    EXPECT_EQ(requestedVersions[0], 0u);
    EXPECT_EQ(requestedVersions[1], 2u);
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:1000");
}

TEST_F(HashRingRefresherTest, TestUnchangedResponseKeepsCurrentRing)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<uint64_t> requestedVersions;
    auto fetch = [&](const HostPort &, uint64_t currentVersion, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::lock_guard<std::mutex> lock(mutex);
        requestedVersions.emplace_back(currentVersion);
        FillRing(ring, hostIdMap, requestedVersions.size() == 1 ? "127.0.0.1:1000" : "127.0.0.1:2000");
        newVersion = 5;
        changed = requestedVersions.size() == 1;
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return requestedVersions.size() >= 2; }));
    }
    refresher.Stop();

    EXPECT_EQ(requestedVersions[1], 5u);
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:1000");
}

TEST_F(HashRingRefresherTest, TestForcedRefreshRetriesUntilRingChanges)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    int fetchCount = 0;
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::lock_guard<std::mutex> lock(mutex);
        ++fetchCount;
        changed = fetchCount == 1 || fetchCount >= 5;
        newVersion = changed && fetchCount >= 5 ? 2 : 1;
        FillRing(ring, hostIdMap, newVersion == 2 ? "127.0.0.1:2000" : "127.0.0.1:1000");
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));

    refresher.ForceRefresh();
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(6), [&] { return fetchCount >= 5; }));
    }
    refresher.Stop();

    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:2000");
}

TEST_F(HashRingRefresherTest, RepeatedFailureExtendsForcedRefreshUntilIsolationPublishes)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    int fetchCount = 0;
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed, std::unordered_map<std::string, std::string> &hostIdMap) {
        std::lock_guard<std::mutex> lock(mutex);
        const auto currentFetch = ++fetchCount;
        changed = currentFetch == 1 || currentFetch == 3 || currentFetch >= 17;
        newVersion = currentFetch >= 17 ? 3 : (currentFetch >= 3 ? 2 : 1);
        FillRing(ring, hostIdMap, currentFetch >= 17 ? "127.0.0.1:2000" : "127.0.0.1:1000");
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 2; }));
    }

    ASSERT_TRUE(refresher.ForceRefresh());
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(6), [&] { return fetchCount >= 13; }));
    }
    EXPECT_FALSE(refresher.ForceRefresh());
    bool isolationPublished = false;
    {
        std::unique_lock<std::mutex> lock(mutex);
        isolationPublished = cv.wait_for(lock, std::chrono::seconds(4), [&] { return fetchCount >= 17; });
    }
    refresher.Stop();
    ASSERT_TRUE(isolationPublished);

    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:2000");
}

TEST_F(HashRingRefresherTest, TestAllWorkersUnreachableKeepsCurrentRing)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    int fetchCount = 0;
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::lock_guard<std::mutex> lock(mutex);
        ++fetchCount;
        cv.notify_all();
        if (fetchCount > 1) {
            return Status(K_RUNTIME_ERROR, "worker unreachable");
        }
        FillRing(ring, hostIdMap, "127.0.0.1:1000");
        FillRing(ring, hostIdMap, "127.0.0.1:2000");
        newVersion = 1;
        changed = true;
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    HostPort before;
    DS_ASSERT_OK(router->SelectWorker("stable-key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, before));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 3; }));
    }
    refresher.Stop();

    HostPort after;
    DS_ASSERT_OK(router->SelectWorker("stable-key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, after));
    EXPECT_EQ(after, before);
}

TEST_F(HashRingRefresherTest, TestFilterNotifiedOnlyWhenRingChanges)
{
    auto filter = std::make_shared<RecordingFilter>();
    auto router = std::make_shared<client::WorkerRouter>(
        "host-a", std::vector<std::shared_ptr<client::IWorkerFilter>>{ filter });
    std::mutex mutex;
    std::condition_variable cv;
    int fetchCount = 0;
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap) {
        std::lock_guard<std::mutex> lock(mutex);
        ++fetchCount;
        changed = fetchCount != 2;
        newVersion = fetchCount < 3 ? 1 : 2;
        if (changed) {
            FillRing(ring, hostIdMap, fetchCount == 1 ? "127.0.0.1:1000" : "127.0.0.1:2000");
        }
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    EXPECT_EQ(filter->UpdateCount(), 1);
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 2; }));
    }
    EXPECT_EQ(filter->UpdateCount(), 1);

    refresher.ForceRefresh();
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 3; }));
    }
    refresher.Stop();

    EXPECT_EQ(filter->UpdateCount(), 2);
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:2000");
}

TEST_F(HashRingRefresherTest, BackgroundRefreshContinuesPastReachableUnchangedWorker)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::mutex mutex;
    std::condition_variable cv;
    int fetchCount = 0;
    auto fetch = [&](const HostPort &worker, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed, std::unordered_map<std::string, std::string> &hostIdMap,
                     int32_t timeoutMs) {
        std::lock_guard<std::mutex> lock(mutex);
        ++fetchCount;
        if (timeoutMs == 0) {
            FillRing(ring, hostIdMap, "127.0.0.1:1000");
            FillRing(ring, hostIdMap, "127.0.0.1:2000");
            newVersion = 1;
            changed = true;
        } else if (worker == HostPort("127.0.0.1", 1000)) {
            EXPECT_EQ(timeoutMs, 250);
            newVersion = 1;
            changed = false;
        } else {
            EXPECT_EQ(timeoutMs, 250);
            FillRing(ring, hostIdMap, "127.0.0.1:2000");
            newVersion = 2;
            changed = true;
        }
        cv.notify_all();
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] { return fetchCount >= 3; }));
    }
    refresher.Stop();

    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected, HostPort("127.0.0.1", 2000));
}

TEST_F(HashRingRefresherTest, StopWaitsForAtMostOneBoundedBackgroundRpc)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::promise<void> backgroundStarted;
    std::atomic<int> fetchCount{ 0 };
    auto fetch = [&](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed, std::unordered_map<std::string, std::string> &hostIdMap,
                     int32_t timeoutMs) {
        ++fetchCount;
        if (timeoutMs == 0) {
            FillRing(ring, hostIdMap, "127.0.0.1:1000");
            FillRing(ring, hostIdMap, "127.0.0.1:2000");
            newVersion = 1;
            changed = true;
            return Status::OK();
        }
        EXPECT_EQ(timeoutMs, 250);
        backgroundStarted.set_value();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        return Status(K_RPC_DEADLINE_EXCEEDED, "simulated bounded timeout");
    };
    client::HashRingRefresher refresher(router, fetch);

    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));
    ASSERT_EQ(backgroundStarted.get_future().wait_for(std::chrono::seconds(2)), std::future_status::ready);
    const auto start = std::chrono::steady_clock::now();
    refresher.Stop();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(elapsed, std::chrono::milliseconds(500));
    EXPECT_EQ(fetchCount.load(), 2);
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
    client::HashRingRefresher noFetch(router, client::HashRingRefresher::FetchRpc{});
    EXPECT_EQ(noFetch.InitialFetch(HostPort("127.0.0.1", 1000)).GetCode(), K_INVALID);

    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    client::HashRingRefresher noRouter(nullptr, fetch);
    EXPECT_TRUE(noRouter.InitialFetch(HostPort("127.0.0.1", 1000)).IsError());

    client::HashRingRefresher invalidAddress(router, fetch);
    EXPECT_EQ(invalidAddress.InitialFetch(HostPort()).GetCode(), K_INVALID);
}

namespace {
bool WaitUntil(const std::function<bool()> &predicate, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

void FillTwoWorkerRing(::datasystem::ClusterTopologyPb &ring, std::unordered_map<std::string, std::string> &hostIdMap)
{
    ring.set_tokens_per_member(1);
    for (const auto *address : { "127.0.0.1:1000", "127.0.0.1:1001" }) {
        auto &worker = (*ring.mutable_members())[address];
        worker.set_state(::datasystem::MembershipPb::ACTIVE);
        hostIdMap[address] = "host-a";
    }
}

using EpochFetchScript = std::function<Status(const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &,
                                              std::string &, uint64_t &, bool &,
                                              std::unordered_map<std::string, std::string> &, int32_t)>;

// Round 1 (InitialFetch) publishes a two-worker ring at a high version; later rounds follow script.
std::unique_ptr<client::HashRingRefresher> BuildEpochResetRefresher(std::shared_ptr<client::WorkerRouter> &router,
                                                                    const EpochFetchScript &script,
                                                                    uint64_t &hookVersion, bool &hookEpochReset,
                                                                    std::atomic<int> &hookCalls)
{
    auto first = [script](const HostPort &workerAddr, uint64_t currentVersion,
                          ::datasystem::ClusterTopologyPb &ring, std::string &masterAddress, uint64_t &newVersion,
                          bool &changed, std::unordered_map<std::string, std::string> &hostIdMap, int32_t timeoutMs) {
        if (currentVersion == 0) {
            FillTwoWorkerRing(ring, hostIdMap);
            newVersion = 31;
            changed = true;
            return Status::OK();
        }
        return script(workerAddr, currentVersion, ring, masterAddress, newVersion, changed, hostIdMap, timeoutMs);
    };
    auto hook = [&hookVersion, &hookEpochReset, &hookCalls](uint64_t version, const ::datasystem::ClusterTopologyPb &,
                                                            const std::unordered_map<std::string, std::string> &,
                                                            bool epochResetConfirmed) {
        hookVersion = version;
        hookEpochReset = epochResetConfirmed;
        ++hookCalls;
        return Status::OK();
    };
    auto refresher = std::make_unique<client::HashRingRefresher>(router, first, hook);
    if (!refresher->InitialFetch(HostPort("127.0.0.1", 1000)).IsOk()) {
        return nullptr;
    }
    return refresher;
}

TEST_F(HashRingRefresherTest, TestCrossConfirmedLowerVersionAcceptsEpochReset)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    uint64_t hookVersion = 0;
    bool hookEpochReset = false;
    std::atomic<int> hookCalls{ 0 };
    auto script = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap, int32_t) {
        FillTwoWorkerRing(ring, hostIdMap);
        newVersion = 3;
        changed = true;
        return Status::OK();
    };
    auto refresher = BuildEpochResetRefresher(router, script, hookVersion, hookEpochReset, hookCalls);
    ASSERT_NE(refresher, nullptr);
    ASSERT_EQ(hookCalls, 1);
    ASSERT_EQ(hookVersion, 31);

    DS_ASSERT_OK(refresher->StartPeriodicRefresh(60'000));
    const bool published = WaitUntil([&hookCalls] { return hookCalls.load() > 1; }, std::chrono::seconds(2));
    refresher->Stop();

    ASSERT_TRUE(published);
    EXPECT_EQ(hookCalls, 2);
    EXPECT_EQ(hookVersion, 3);
    EXPECT_TRUE(hookEpochReset);
    HostPort selected;
    EXPECT_TRUE(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected).IsOk());
}

TEST_F(HashRingRefresherTest, TestLowerVersionDigestIsCanonicalActiveAddressSet)
{
    ClusterTopologyPb first;
    ClusterTopologyPb reversed;
    const std::vector<std::string> addresses{ "127.0.0.1:1000", "127.0.0.1:2000", "127.0.0.1:3000" };
    for (const auto &address : addresses) {
        (*first.mutable_members())[address].set_state(MembershipPb::ACTIVE);
    }
    for (auto it = addresses.rbegin(); it != addresses.rend(); ++it) {
        (*reversed.mutable_members())[*it].set_state(MembershipPb::ACTIVE);
    }
    (*reversed.mutable_members())["127.0.0.1:4000"].set_state(MembershipPb::FAILED);
    EXPECT_EQ(client::HashRingRefresherTestPeer::Digest(first), "127.0.0.1:1000,127.0.0.1:2000,127.0.0.1:3000,");
    EXPECT_EQ(client::HashRingRefresherTestPeer::Digest(first), client::HashRingRefresherTestPeer::Digest(reversed));
}

TEST_F(HashRingRefresherTest, TestDisjointReorderedLowerVersionResponsesConfirmWithoutBatchEpoch)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    uint64_t version = 0;
    bool reset = false;
    std::atomic<int> calls{ 0 };
    auto script = [](const HostPort &worker, uint64_t, ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIds, int32_t) {
        ring.set_tokens_per_member(1);
        std::vector<std::string> addresses{ "127.0.0.1:2000", "127.0.0.1:3000" };
        if (worker.Port() != 1000) {
            std::reverse(addresses.begin(), addresses.end());
        }
        for (const auto &address : addresses) {
            (*ring.mutable_members())[address].set_state(MembershipPb::ACTIVE);
            hostIds[address] = "host-a";
        }
        newVersion = 3;
        changed = true;
        return Status::OK();
    };
    auto refresher = BuildEpochResetRefresher(router, script, version, reset, calls);
    ASSERT_NE(refresher, nullptr);
    DS_ASSERT_OK(client::HashRingRefresherTestPeer::Refresh(*refresher));
    EXPECT_EQ(calls.load(), 2);
    EXPECT_EQ(version, 3U);
    EXPECT_TRUE(reset);
}

TEST_F(HashRingRefresherTest, SameVersionHostIdsAreAcknowledgedOnlyAfterSuccessfulPublication)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    bool changedHost = false;
    bool reject = false;
    auto fetch = [&](const HostPort &, uint64_t, ClusterTopologyPb &ring, std::string &,
                     uint64_t &version, bool &changed, std::unordered_map<std::string, std::string> &hostIds) {
        FillTwoWorkerRing(ring, hostIds);
        for (auto &[address, host] : hostIds) {
            (void)address;
            host = changedHost ? "host-b" : "host-a";
        }
        version = 31;
        changed = true;
        return Status::OK();
    };
    auto hook = [&](uint64_t, const ClusterTopologyPb &, const auto &, bool reset) {
        EXPECT_FALSE(reset);
        return reject ? Status(K_NOT_READY, "injected snapshot rejection") : Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch, hook);
    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    const auto original = refresher.GetHostIdsDigest(31);
    ASSERT_FALSE(original.empty());
    ASSERT_FALSE(router->GetAvailableSameNodeWorkers().empty());
    changedHost = true;
    reject = true;
    EXPECT_EQ(client::HashRingRefresherTestPeer::Refresh(refresher).GetCode(), K_NOT_READY);
    EXPECT_EQ(refresher.GetHostIdsDigest(31), original);
    EXPECT_FALSE(router->GetAvailableSameNodeWorkers().empty());
    reject = false;
    DS_ASSERT_OK(client::HashRingRefresherTestPeer::Refresh(refresher));
    EXPECT_NE(refresher.GetHostIdsDigest(31), original);
    EXPECT_TRUE(router->GetAvailableSameNodeWorkers().empty());
    EXPECT_TRUE(refresher.GetHostIdsDigest(30).empty());
}

TEST_F(HashRingRefresherTest, TestSingleWorkerLowerVersionIsStillIgnored)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    uint64_t hookVersion = 0;
    bool hookEpochReset = false;
    std::atomic<int> hookCalls{ 0 };
    auto script = [](const HostPort &workerAddr, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap, int32_t) {
        if (workerAddr.ToString() != "127.0.0.1:1000") {
            return Status::OK();
        }
        FillTwoWorkerRing(ring, hostIdMap);
        newVersion = 3;
        changed = true;
        return Status::OK();
    };
    auto refresher = BuildEpochResetRefresher(router, script, hookVersion, hookEpochReset, hookCalls);
    ASSERT_NE(refresher, nullptr);
    const int callsAfterInitial = hookCalls;

    DS_ASSERT_OK(refresher->StartPeriodicRefresh(60'000));
    ASSERT_TRUE(refresher->ForceRefresh());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    refresher->Stop();

    EXPECT_EQ(hookCalls, callsAfterInitial);
    EXPECT_EQ(hookVersion, 31);
    EXPECT_FALSE(hookEpochReset);
}

TEST_F(HashRingRefresherTest, TestDifferentLowerVersionsAreNotConfirmed)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    uint64_t hookVersion = 0;
    bool hookEpochReset = false;
    std::atomic<int> hookCalls{ 0 };
    auto script = [](const HostPort &workerAddr, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                     uint64_t &newVersion, bool &changed,
                     std::unordered_map<std::string, std::string> &hostIdMap, int32_t) {
        FillTwoWorkerRing(ring, hostIdMap);
        newVersion = workerAddr.ToString() == "127.0.0.1:1000" ? 2 : 3;
        changed = true;
        return Status::OK();
    };
    auto refresher = BuildEpochResetRefresher(router, script, hookVersion, hookEpochReset, hookCalls);
    ASSERT_NE(refresher, nullptr);
    const int callsAfterInitial = hookCalls;

    DS_ASSERT_OK(refresher->StartPeriodicRefresh(60'000));
    ASSERT_TRUE(refresher->ForceRefresh());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    refresher->Stop();

    EXPECT_EQ(hookCalls, callsAfterInitial);
    EXPECT_EQ(hookVersion, 31);
    EXPECT_FALSE(hookEpochReset);
}

// Measures the ForceRefresh-to-hook-publish latency with an in-memory fetch (network cost
// excluded, so this is the lower bound clients can expect on a healthy Worker). This bounds how
// much of a client request deadline the stale-location retry consumes before the published
// WorkerSnapshot sees the new ring.
TEST_F(HashRingRefresherTest, ForceRefreshPublishesNewRingWithinRetryBudget)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    std::atomic<uint64_t> currentVersion{ 1 };
    auto fetch = [&currentVersion](const HostPort &, uint64_t requested, ::datasystem::ClusterTopologyPb &ring,
                                   std::string &, uint64_t &newVersion, bool &changed,
                                   std::unordered_map<std::string, std::string> &hostIdMap) {
        FillRing(ring, hostIdMap, "127.0.0.1:2000");
        const uint64_t latest = currentVersion.load(std::memory_order_acquire);
        changed = latest != requested;
        newVersion = latest;
        return Status::OK();
    };
    std::atomic<int> hookCalls{ 0 };
    auto hook = [&hookCalls](uint64_t, const ::datasystem::ClusterTopologyPb &,
                             const std::unordered_map<std::string, std::string> &, bool) {
        hookCalls.fetch_add(1, std::memory_order_release);
        return Status::OK();
    };
    client::HashRingRefresher refresher(router, fetch, hook);
    DS_ASSERT_OK(refresher.InitialFetch(HostPort("127.0.0.1", 1000)));
    ASSERT_EQ(hookCalls.load(), 1);
    DS_ASSERT_OK(refresher.StartPeriodicRefresh(60'000));

    currentVersion.store(2, std::memory_order_release);
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(refresher.ForceRefresh());
    const bool published = WaitUntil([&hookCalls] { return hookCalls.load() > 1; }, std::chrono::seconds(2));
    const auto publishLatencyMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    refresher.Stop();

    ASSERT_TRUE(published);
    // In-memory lower bound is single-digit ms; 50ms keeps the hang-detection intent while
    // tolerating scheduler jitter on shared CI machines.
    EXPECT_LT(publishLatencyMs, 50) << "publish latency " << publishLatencyMs << "ms";
    LOG(INFO) << "ForceRefresh publish latency (in-memory lower bound): " << publishLatencyMs << "ms";
}
}  // namespace

}  // namespace ut
}  // namespace datasystem
