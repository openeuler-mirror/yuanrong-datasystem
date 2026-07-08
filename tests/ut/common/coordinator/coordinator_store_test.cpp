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
 * Description: Unit tests for coordinator store.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/coordinator/coordinator_service_impl.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"

namespace datasystem {
namespace ut {
namespace {
class MockWatchDispatcher : public WatchDispatcher {
public:
    explicit MockWatchDispatcher(WatchRegistry *watchRegistry) : WatchDispatcher(watchRegistry)
    {
    }
    ~MockWatchDispatcher() override = default;

    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        (void)watcherAddr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            maxBatchSize_[watchId] = std::max(maxBatchSize_[watchId], events.size());
            auto &target = events_[watchId];
            target.insert(target.end(), events.begin(), events.end());
        }
        cv_.notify_all();
        return Status::OK();
    }

    bool WaitEventCount(int64_t watchId, size_t expected, uint64_t timeoutMs = 2000)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this, watchId, expected] { return events_[watchId].size() >= expected; });
    }

    std::vector<std::shared_ptr<WatchEvent>> GetEvents(int64_t watchId)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return events_[watchId];
    }

    size_t GetMaxBatchSize(int64_t watchId)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return maxBatchSize_[watchId];
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::unordered_map<int64_t, std::vector<std::shared_ptr<WatchEvent>>> events_;
    std::unordered_map<int64_t, size_t> maxBatchSize_;
};

class FailingWatchDispatcher : public MockWatchDispatcher {
public:
    explicit FailingWatchDispatcher(WatchRegistry *watchRegistry) : MockWatchDispatcher(watchRegistry)
    {
    }

    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        ++notifyAttempts_;
        if (failNext_) {
            failNext_ = false;
            return Status(StatusCode::K_RUNTIME_ERROR, "injected notify failure");
        }
        return MockWatchDispatcher::DoNotify(watchId, watcherAddr, events);
    }

    int GetNotifyAttempts() const
    {
        return notifyAttempts_.load();
    }

private:
    std::atomic<int> notifyAttempts_{ 0 };
    bool failNext_ = true;
};

class FailingRewatchWatchDispatcher : public MockWatchDispatcher {
public:
    explicit FailingRewatchWatchDispatcher(WatchRegistry *watchRegistry) : MockWatchDispatcher(watchRegistry)
    {
    }

    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        if (failNextRewatch_ && std::any_of(events.begin(), events.end(), [](const auto &event) {
                return event->type == WatchEvent::Type::REWATCH;
            })) {
            failNextRewatch_ = false;
            return Status(StatusCode::K_RUNTIME_ERROR, "injected rewatch notify failure");
        }
        return MockWatchDispatcher::DoNotify(watchId, watcherAddr, events);
    }

private:
    bool failNextRewatch_ = true;
};

class BlockingWatchDispatcher : public MockWatchDispatcher {
public:
    explicit BlockingWatchDispatcher(WatchRegistry *watchRegistry) : MockWatchDispatcher(watchRegistry)
    {
    }

    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            notifyStarted_ = true;
        }
        cv_.notify_all();

        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return unblock_; });
        lock.unlock();
        return MockWatchDispatcher::DoNotify(watchId, watcherAddr, events);
    }

    bool WaitNotifyStarted(uint64_t timeoutMs = 2000)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this] { return notifyStarted_; });
    }

    void Unblock()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            unblock_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool notifyStarted_ = false;
    bool unblock_ = false;
};

}  // namespace

class CoordinatorStoreTest : public CommonTest {
protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        memStore_ = std::make_shared<MemoryKvStore>();
        registry_ = std::make_shared<WatchRegistry>();
        dispatcher_ = std::make_shared<MockWatchDispatcher>(registry_.get());
        clock_ = std::make_shared<SteadyClockMock>();
        ttlManager_ = std::make_shared<TtlManager>(clock_);
        store_ = std::make_unique<CoordinatorStore>(memStore_, registry_, dispatcher_, ttlManager_);
        dispatcher_->Start();
        ttlManager_->Start();
    }

    void TearDown() override
    {
        if (ttlManager_) {
            ttlManager_->Stop();
        }
        if (dispatcher_) {
            dispatcher_->Stop();
        }
        CommonTest::TearDown();
    }

    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> registry_;
    std::shared_ptr<MockWatchDispatcher> dispatcher_;
    std::shared_ptr<SteadyClockMock> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> store_;
};

TEST_F(CoordinatorStoreTest, CoordinatorServiceForwardsStoreOperationsAndMarksLeader)
{
    coordinator::CoordinatorServiceImpl service(HostPort("127.0.0.1", 18480));
    DS_ASSERT_OK(service.Init());
    DS_ASSERT_OK(service.Start());

    coordinator::PutReqPb putReq;
    putReq.set_key("/svc/key");
    putReq.set_value("value");
    coordinator::PutRspPb putRsp;
    DS_ASSERT_OK(service.Put(putReq, putRsp));
    ASSERT_TRUE(putRsp.header().is_leader());
    ASSERT_TRUE(putRsp.header().leader_address().empty());
    ASSERT_EQ(putRsp.version(), 1);
    ASSERT_GT(putRsp.revision(), 0);

    coordinator::RangeReqPb rangeReq;
    rangeReq.set_key("/svc/key");
    coordinator::RangeRspPb rangeRsp;
    DS_ASSERT_OK(service.Range(rangeReq, rangeRsp));
    ASSERT_TRUE(rangeRsp.header().is_leader());
    ASSERT_EQ(rangeRsp.kvs_size(), 1);
    ASSERT_EQ(rangeRsp.kvs(0).key(), "/svc/key");
    ASSERT_EQ(rangeRsp.kvs(0).value(), "value");
    ASSERT_EQ(rangeRsp.kvs(0).version(), putRsp.version());
}

TEST_F(CoordinatorStoreTest, MemoryKvStoreSupportsPutRangeCasAndDelete)
{
    MemoryKvStore store;
    std::vector<std::shared_ptr<WatchEvent>> events;
    store.SetMutationCallback([&events](std::shared_ptr<WatchEvent> event) { events.push_back(std::move(event)); });

    int64_t version = 0;
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    DS_ASSERT_OK(store.Put("/a", "1", 0, 0, version, revision, ttlGeneration));
    ASSERT_EQ(version, 1);
    ASSERT_EQ(revision, 2);

    DS_ASSERT_OK(store.Put("/a", "2", 0, version, version, revision, ttlGeneration));
    ASSERT_EQ(version, 2);

    int64_t badVersion = 0;
    int64_t badRevision = 0;
    uint64_t badTtlGeneration = 0;
    ASSERT_TRUE(store.Put("/a", "3", 0, 1, badVersion, badRevision, badTtlGeneration).IsError());

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    store.Range("/a", "", kvs, rangeRevision);
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "2");

    int64_t deleted = 0;
    std::vector<KeyValueEntry> deletedEntries;
    store.Delete("/a", "", deleted, revision, deletedEntries);
    ASSERT_EQ(deleted, 1);
    ASSERT_EQ(deletedEntries.size(), 1ul);
    ASSERT_EQ(events.size(), 3ul);
    ASSERT_EQ(events[0]->type, WatchEvent::Type::PUT);
    ASSERT_EQ(events[2]->type, WatchEvent::Type::DELETE);
}

TEST_F(CoordinatorStoreTest, MemoryKvStorePutWithZeroVersionRequiresMissingKey)
{
    MemoryKvStore store;
    std::vector<std::shared_ptr<WatchEvent>> events;
    store.SetMutationCallback([&events](std::shared_ptr<WatchEvent> event) { events.push_back(std::move(event)); });

    int64_t version = 0;
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    DS_ASSERT_OK(store.Put("/cas/create", "1", 0, 0, version, revision, ttlGeneration));
    ASSERT_EQ(version, 1);

    int64_t overwriteVersion = 0;
    int64_t overwriteRevision = 0;
    uint64_t overwriteTtlGeneration = 0;
    auto status = store.Put("/cas/create", "2", 0, 0, overwriteVersion, overwriteRevision, overwriteTtlGeneration);
    ASSERT_EQ(status.GetCode(), K_INVALID);

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    store.Range("/cas/create", "", kvs, rangeRevision);
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "1");
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->type, WatchEvent::Type::PUT);
}

TEST_F(CoordinatorStoreTest, MemoryKvStorePutWithoutVersionCheckCreatesAndOverwrites)
{
    MemoryKvStore store;

    int64_t version = 0;
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    DS_ASSERT_OK(store.Put("/put/no-check", "1", 0, COORDINATOR_NO_VERSION_CHECK, version, revision, ttlGeneration));
    ASSERT_EQ(version, 1);

    DS_ASSERT_OK(store.Put("/put/no-check", "2", 0, COORDINATOR_NO_VERSION_CHECK, version, revision, ttlGeneration));
    ASSERT_EQ(version, 2);

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    store.Range("/put/no-check", "", kvs, rangeRevision);
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "2");
}

TEST_F(CoordinatorStoreTest, CoordinatorStorePutPropagatesExpectedVersionSemantics)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/coordinator/cas", "1", 0, 0, version, revision));
    ASSERT_EQ(version, 1);

    int64_t overwriteVersion = 0;
    int64_t overwriteRevision = 0;
    auto status = store_->Put("/coordinator/cas", "2", 0, 0, overwriteVersion, overwriteRevision);
    ASSERT_EQ(status.GetCode(), K_INVALID);

    DS_ASSERT_OK(store_->Put("/coordinator/cas", "2", 0, COORDINATOR_NO_VERSION_CHECK, version, revision));
    ASSERT_EQ(version, 2);

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/coordinator/cas", "", kvs, rangeRevision));
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "2");
}

TEST_F(CoordinatorStoreTest, CoordinatorStoreCreateIfAbsentAllowsOnlyOneConcurrentWriter)
{
    constexpr int THREAD_COUNT = 8;
    std::atomic<int> successCount{ 0 };
    std::vector<std::thread> threads;

    for (int i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back([this, i, &successCount] {
            int64_t version = 0;
            int64_t revision = 0;
            auto status = store_->Put("/coordinator/create-once", std::to_string(i), 0, 0, version, revision);
            if (status.IsOk()) {
                ASSERT_EQ(version, 1);
                successCount.fetch_add(1);
            } else {
                ASSERT_EQ(status.GetCode(), K_INVALID);
            }
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    ASSERT_EQ(successCount.load(), 1);

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/coordinator/create-once", "", kvs, rangeRevision));
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].version, 1);
}

TEST_F(CoordinatorStoreTest, WatchRegistryMatchesSingleKeyAndRange)
{
    WatchRegistry registry;
    int64_t single = registry.Register("/ring/1/key", "", "single");
    int64_t range = registry.Register("/ring/1", "/ring/9", "range");
    int64_t outside = registry.Register("/node/1", "/node/9", "outside");

    std::vector<std::shared_ptr<WatcherEntry>> matched;
    registry.MatchWatchers("/ring/1/key", matched);
    ASSERT_EQ(matched.size(), 2ul);

    std::vector<int64_t> ids;
    for (auto &entry : matched) {
        ids.push_back(entry->watchId);
    }
    ASSERT_NE(std::find(ids.begin(), ids.end(), single), ids.end());
    ASSERT_NE(std::find(ids.begin(), ids.end(), range), ids.end());
    ASSERT_EQ(std::find(ids.begin(), ids.end(), outside), ids.end());

    DS_ASSERT_OK(registry.Cancel(single, "single"));
    matched.clear();
    registry.MatchWatchers("/ring/1/key", matched);
    ASSERT_EQ(matched.size(), 1ul);
    ASSERT_EQ(matched[0]->watchId, range);
}

TEST_F(CoordinatorStoreTest, WatchRegistryHandlesGroupedRangeAndHighFrequencyCancel)
{
    WatchRegistry registry;
    constexpr int WATCHER_COUNT = 1000;
    std::vector<int64_t> watchIds;
    watchIds.reserve(WATCHER_COUNT);

    for (int i = 0; i < WATCHER_COUNT; ++i) {
        watchIds.push_back(registry.Register("/ring/1", "/ring/9", "addr" + std::to_string(i)));
    }

    for (int i = 0; i < WATCHER_COUNT; i += 2) {
        DS_ASSERT_OK(registry.Cancel(watchIds[i], "addr" + std::to_string(i)));
    }

    std::vector<std::shared_ptr<WatcherEntry>> matched;
    registry.MatchWatchers("/ring/1/key", matched);
    ASSERT_EQ(matched.size(), static_cast<size_t>(WATCHER_COUNT / 2));

    std::unordered_set<int64_t> matchedIds;
    for (const auto &entry : matched) {
        matchedIds.insert(entry->watchId);
    }
    for (int i = 0; i < WATCHER_COUNT; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(matchedIds.count(watchIds[i]), 0ul);
        } else {
            ASSERT_EQ(matchedIds.count(watchIds[i]), 1ul);
        }
    }
}

TEST_F(CoordinatorStoreTest, WatchDispatcherWaitsForSnapshotRevisionBeforeDispatch)
{
    int64_t watchId = registry_->Register("/barrier", "", "addr");
    dispatcher_->AddChannel(watchId, "addr");

    auto oldEvent = std::make_shared<WatchEvent>();
    oldEvent->type = WatchEvent::Type::PUT;
    oldEvent->entry = KeyValueEntry{ "/barrier", "old", 1, 2 };
    oldEvent->revision = 2;
    dispatcher_->Enqueue(oldEvent);

    auto newEvent = std::make_shared<WatchEvent>();
    newEvent->type = WatchEvent::Type::PUT;
    newEvent->entry = KeyValueEntry{ "/barrier", "new", 2, 3 };
    newEvent->revision = 3;
    dispatcher_->Enqueue(newEvent);

    ASSERT_FALSE(dispatcher_->WaitEventCount(watchId, 1, 200));

    dispatcher_->SetSnapshotRevision(watchId, 2);
    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 1));
    auto events = dispatcher_->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->entry.value, "new");
}

TEST_F(CoordinatorStoreTest, WatchDispatcherFansOutEventsAndFiltersSnapshotRevision)
{
    int64_t watchId = registry_->Register("/k", "", "addr");
    dispatcher_->AddChannel(watchId, "addr");
    dispatcher_->SetSnapshotRevision(watchId, 10);

    auto oldEvent = std::make_shared<WatchEvent>();
    oldEvent->type = WatchEvent::Type::PUT;
    oldEvent->entry = KeyValueEntry{ "/k", "old", 1, 9 };
    oldEvent->revision = 9;
    dispatcher_->Enqueue(oldEvent);

    auto newEvent = std::make_shared<WatchEvent>();
    newEvent->type = WatchEvent::Type::PUT;
    newEvent->entry = KeyValueEntry{ "/k", "new", 2, 11 };
    newEvent->revision = 11;
    dispatcher_->Enqueue(newEvent);

    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 1));
    auto events = dispatcher_->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->entry.value, "new");
}

TEST_F(CoordinatorStoreTest, WatchDispatcherLimitsDoNotifyBatchSize)
{
    int64_t watchId = registry_->Register("/batch", "", "addr");
    dispatcher_->AddChannel(watchId, "addr");
    dispatcher_->SetSnapshotRevision(watchId, 1);

    constexpr int EVENT_COUNT = 100;
    constexpr size_t MAX_BATCH_SIZE = 32;
    for (int i = 0; i < EVENT_COUNT; ++i) {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ "/batch", std::to_string(i), i + 1, i + 2 };
        event->revision = i + 2;
        dispatcher_->Enqueue(event);
    }

    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, EVENT_COUNT));
    ASSERT_LE(dispatcher_->GetMaxBatchSize(watchId), MAX_BATCH_SIZE);
}

TEST_F(CoordinatorStoreTest, WatchDispatcherRetriesFailedNotify)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<FailingWatchDispatcher>(registry.get());
    dispatcher->Start();

    int64_t watchId = registry->Register("/retry", "", "addr");
    dispatcher->AddChannel(watchId, "addr");
    dispatcher->SetSnapshotRevision(watchId, 1);

    auto event = std::make_shared<WatchEvent>();
    event->type = WatchEvent::Type::PUT;
    event->entry = KeyValueEntry{ "/retry", "value", 1, 2 };
    event->revision = 2;
    dispatcher->Enqueue(event);

    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 1));
    ASSERT_GE(dispatcher->GetNotifyAttempts(), 2);
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, WatchDispatcherCommitsSuccessfulBatchByMaxRevision)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<BlockingWatchDispatcher>(registry.get());
    dispatcher->Start();

    int64_t watchId = registry->Register("/same-revision", "", "addr");
    dispatcher->AddChannel(watchId, "addr");
    dispatcher->SetSnapshotRevision(watchId, 1);

    auto firstEvent = std::make_shared<WatchEvent>();
    firstEvent->type = WatchEvent::Type::PUT;
    firstEvent->entry = KeyValueEntry{ "/same-revision", "first", 1, 2 };
    firstEvent->revision = 2;
    dispatcher->Enqueue(firstEvent);
    ASSERT_TRUE(dispatcher->WaitNotifyStarted());

    constexpr int QUEUED_EVENT_COUNT = 100;
    for (int i = 0; i < QUEUED_EVENT_COUNT; ++i) {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ "/same-revision", std::to_string(i), i + 2, 2 };
        event->revision = 2;
        dispatcher->Enqueue(event);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    dispatcher->Unblock();
    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto events = dispatcher->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->entry.value, "first");
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, MemoryKvStoreTtlGenerationProtectsKeepAliveFromStaleExpiry)
{
    MemoryKvStore store;
    int64_t version = 0;
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    DS_ASSERT_OK(store.Put("/ttl/generation", "v", 100, 0, version, revision, ttlGeneration));
    ASSERT_EQ(ttlGeneration, 1ul);

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    int64_t keepAliveRevision = 0;
    uint64_t keepAliveGeneration = 0;
    DS_ASSERT_OK(store.KeepAlive("/ttl/generation", ttlMs, remainingTtlMs, keepAliveRevision, keepAliveGeneration));
    ASSERT_EQ(keepAliveRevision, revision);
    ASSERT_GT(keepAliveGeneration, ttlGeneration);

    ASSERT_FALSE(store.DeleteIfTtlExpired("/ttl/generation", revision, ttlGeneration));

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    store.Range("/ttl/generation", "", kvs, rangeRevision);
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "v");

    ASSERT_TRUE(store.DeleteIfTtlExpired("/ttl/generation", keepAliveRevision, keepAliveGeneration));
    kvs.clear();
    store.Range("/ttl/generation", "", kvs, rangeRevision);
    ASSERT_TRUE(kvs.empty());
}

TEST_F(CoordinatorStoreTest, KeepAliveInvalidatesStaleExpirySchedule)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/ttl/keepalive", "v", 100, 0, version, revision));

    clock_->AdvanceMs(90);
    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    DS_ASSERT_OK(store_->KeepAlive("/ttl/keepalive", ttlMs, remainingTtlMs));
    ASSERT_EQ(ttlMs, 100);
    ASSERT_EQ(remainingTtlMs, 100);

    clock_->AdvanceMs(20);
    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/ttl/keepalive", "", kvs, rangeRevision));
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "v");

    clock_->AdvanceMs(100);
    bool expired = false;
    for (int i = 0; i < 20; ++i) {
        kvs.clear();
        DS_ASSERT_OK(store_->Range("/ttl/keepalive", "", kvs, rangeRevision));
        if (kvs.empty()) {
            expired = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(expired);
}

TEST_F(CoordinatorStoreTest, TtlManagerScheduleUpdatesSameKeyExpiry)
{
    std::vector<std::string> expiredKeys;
    std::mutex mutex;
    std::condition_variable cv;
    TtlManager manager(clock_);
    manager.SetExpireCallback([&](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
        (void)revision;
        (void)ttlGeneration;
        {
            std::lock_guard<std::mutex> lock(mutex);
            expiredKeys.push_back(key);
        }
        cv.notify_all();
        return true;
    });
    manager.Start();

    DS_ASSERT_OK(manager.Schedule("/ttl/key1", 100, 1, 1));
    DS_ASSERT_OK(manager.Schedule("/ttl/key2", 100, 2, 1));

    clock_->AdvanceMs(90);
    DS_ASSERT_OK(manager.Schedule("/ttl/key1", 100, 1, 2));

    clock_->AdvanceMs(20);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(1000), [&] { return expiredKeys.size() == 1; }));
    }
    ASSERT_EQ(expiredKeys[0], "/ttl/key2");

    clock_->AdvanceMs(100);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(1000), [&] { return expiredKeys.size() == 2; }));
    }
    ASSERT_EQ(expiredKeys[1], "/ttl/key1");

    manager.Stop();
}

TEST_F(CoordinatorStoreTest, TtlManagerScheduleReplacesPreviousExpiry)
{
    std::vector<std::string> expiredKeys;
    std::mutex mutex;
    std::condition_variable cv;
    TtlManager manager(clock_);
    manager.SetExpireCallback([&](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
        (void)revision;
        (void)ttlGeneration;
        {
            std::lock_guard<std::mutex> lock(mutex);
            expiredKeys.push_back(key);
        }
        cv.notify_all();
        return true;
    });
    manager.Start();

    DS_ASSERT_OK(manager.Schedule("/ttl", 100, 1, 1));
    clock_->AdvanceMs(50);
    DS_ASSERT_OK(manager.Schedule("/ttl", 100, 1, 2));

    clock_->AdvanceMs(60);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_FALSE(cv.wait_for(lock, std::chrono::milliseconds(120), [&] { return !expiredKeys.empty(); }));
    }

    clock_->AdvanceMs(50);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(1000), [&] { return expiredKeys.size() == 1; }));
    }
    ASSERT_EQ(expiredKeys[0], "/ttl");

    manager.Stop();
}

TEST_F(CoordinatorStoreTest, CoordinatorStoreIntegratesPutRangeWatchDeleteAndTtl)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/ring/1/key", "v1", 100, 0, version, revision));

    std::vector<KeyValueEntry> initial;
    int64_t watchId = 0;
    DS_ASSERT_OK(store_->WatchRange("/ring/1", "/ring/9", "addr", watchId, initial));
    ASSERT_EQ(initial.size(), 1ul);
    ASSERT_EQ(initial[0].key, "/ring/1/key");

    DS_ASSERT_OK(store_->Put("/ring/1/key", "v2", 0, version, version, revision));
    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 1));
    auto events = dispatcher_->GetEvents(watchId);
    ASSERT_EQ(events.back()->type, WatchEvent::Type::PUT);
    ASSERT_EQ(events.back()->entry.value, "v2");

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    ASSERT_TRUE(store_->KeepAlive("/ring/1/key", ttlMs, remainingTtlMs).IsError());

    DS_ASSERT_OK(store_->Put("/ring/1/key", "v3", 100, version, version, revision));
    DS_ASSERT_OK(store_->KeepAlive("/ring/1/key", ttlMs, remainingTtlMs));
    ASSERT_EQ(ttlMs, 100);

    clock_->AdvanceMs(150);
    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 3));
    events = dispatcher_->GetEvents(watchId);
    ASSERT_EQ(events.back()->type, WatchEvent::Type::DELETE);

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/ring/1/key", "", kvs, rangeRevision));
    ASSERT_TRUE(kvs.empty());
}

TEST_F(CoordinatorStoreTest, WatchDispatcherEnqueueWaitsWhenPendingQueueIsFull)
{
    dispatcher_->Stop();
    constexpr size_t WATCH_PENDING_QUEUE_LIMIT = 10000;
    for (size_t i = 0; i < WATCH_PENDING_QUEUE_LIMIT; ++i) {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ "/pending/" + std::to_string(i), "v", 1, static_cast<int64_t>(i + 2) };
        event->revision = static_cast<int64_t>(i + 2);
        dispatcher_->Enqueue(std::move(event));
    }
    ASSERT_EQ(dispatcher_->GetPendingEventCount(), WATCH_PENDING_QUEUE_LIMIT);

    std::atomic<bool> enqueueDone{ false };
    std::thread writer([this, &enqueueDone] {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ "/pending/blocked", "v", 1, 10002 };
        event->revision = 10002;
        dispatcher_->Enqueue(std::move(event));
        enqueueDone.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_FALSE(enqueueDone.load(std::memory_order_acquire));

    dispatcher_->Start();
    for (int i = 0; i < 100 && !enqueueDone.load(std::memory_order_acquire); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(enqueueDone.load(std::memory_order_acquire));
    writer.join();
}

TEST_F(CoordinatorStoreTest, ConstructorStartsTtlAndWatchDispatcher)
{
    auto memStore = std::make_shared<MemoryKvStore>();
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<MockWatchDispatcher>(registry.get());
    auto clock = std::make_shared<SteadyClockMock>();
    auto ttlManager = std::make_shared<TtlManager>(clock);
    auto store = std::make_unique<CoordinatorStore>(memStore, registry, dispatcher, ttlManager);

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initial;
    DS_ASSERT_OK(store->WatchRange("/auto/key", "", "addr", watchId, initial));

    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store->Put("/auto/key", "v", 100, 0, version, revision));
    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 1));

    clock->AdvanceMs(150);
    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 2));
    auto events = dispatcher->GetEvents(watchId);
    ASSERT_EQ(events.back()->type, WatchEvent::Type::DELETE);
}

TEST_F(CoordinatorStoreTest, WatchRangeSnapshotDoesNotReplayInitialData)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/snapshot/key", "v1", 0, 0, version, revision));

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initial;
    DS_ASSERT_OK(store_->WatchRange("/snapshot/", "/snapshot0", "addr", watchId, initial));
    ASSERT_EQ(initial.size(), 1ul);
    ASSERT_EQ(initial[0].value, "v1");
    ASSERT_FALSE(dispatcher_->WaitEventCount(watchId, 1, 200));

    DS_ASSERT_OK(store_->Put("/snapshot/key", "v2", 0, version, version, revision));
    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 1));
    auto events = dispatcher_->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->entry.value, "v2");
}

TEST_F(CoordinatorStoreTest, DeleteRangeUsesOneRevisionForDeliveredDeleteEvents)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/delete/1", "v1", 0, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/delete/2", "v2", 0, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/delete/3", "v3", 0, 0, version, revision));

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initial;
    DS_ASSERT_OK(store_->WatchRange("/delete/", "/delete0", "addr", watchId, initial));
    ASSERT_EQ(initial.size(), 3ul);

    int64_t deleted = 0;
    int64_t deleteRevision = 0;
    DS_ASSERT_OK(store_->DeleteRange("/delete/", "/delete0", deleted, deleteRevision));
    ASSERT_EQ(deleted, 3);
    ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 1));

    auto events = dispatcher_->GetEvents(watchId);
    ASSERT_FALSE(events.empty());
    for (const auto &event : events) {
        ASSERT_EQ(event->type, WatchEvent::Type::DELETE);
        ASSERT_EQ(event->revision, deleteRevision);
    }
}

TEST_F(CoordinatorStoreTest, WatchRegistryHandlesGroupedSingleKeyAndHighFrequencyCancel)
{
    WatchRegistry registry;
    constexpr int WATCHER_COUNT = 1000;
    std::vector<int64_t> watchIds;
    watchIds.reserve(WATCHER_COUNT);

    for (int i = 0; i < WATCHER_COUNT; ++i) {
        watchIds.push_back(registry.Register("/single/key", "", "addr" + std::to_string(i)));
    }

    for (int i = 0; i < WATCHER_COUNT; i += 2) {
        DS_ASSERT_OK(registry.Cancel(watchIds[i], "addr" + std::to_string(i)));
    }

    std::vector<std::shared_ptr<WatcherEntry>> matched;
    registry.MatchWatchers("/single/key", matched);
    ASSERT_EQ(matched.size(), static_cast<size_t>(WATCHER_COUNT / 2));

    matched.clear();
    registry.MatchWatchers("/single/other", matched);
    ASSERT_TRUE(matched.empty());
}

TEST_F(CoordinatorStoreTest, TtlExpirationUsesRevisionToAvoidDeletingNewerValue)
{
    auto memStore = std::make_shared<MemoryKvStore>();
    TtlManager manager(clock_);
    std::mutex mutex;
    std::condition_variable cv;
    bool callbackStarted = false;
    bool allowDelete = false;
    manager.SetExpireCallback([&](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            callbackStarted = true;
        }
        cv.notify_all();

        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&] { return allowDelete; });
        lock.unlock();
        return memStore->DeleteIfTtlExpired(key, revision, ttlGeneration);
    });
    manager.Start();

    int64_t version = 0;
    int64_t oldRevision = 0;
    uint64_t oldTtlGeneration = 0;
    DS_ASSERT_OK(memStore->Put("/ttl/race", "old", 100, 0, version, oldRevision, oldTtlGeneration));
    DS_ASSERT_OK(manager.Schedule("/ttl/race", 100, oldRevision, oldTtlGeneration));

    clock_->AdvanceMs(150);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(1000), [&] { return callbackStarted; }));
    }

    int64_t newRevision = 0;
    uint64_t newTtlGeneration = 0;
    DS_ASSERT_OK(
        memStore->Put("/ttl/race", "new", 100, COORDINATOR_NO_VERSION_CHECK, version, newRevision, newTtlGeneration));
    DS_ASSERT_OK(manager.Schedule("/ttl/race", 100, newRevision, newTtlGeneration));
    {
        std::lock_guard<std::mutex> lock(mutex);
        allowDelete = true;
    }
    cv.notify_all();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    memStore->Range("/ttl/race", "", kvs, rangeRevision);
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "new");

    manager.Stop();
}

TEST_F(CoordinatorStoreTest, TtlManagerIgnoresStaleScheduleForSameKey)
{
    std::vector<std::string> expiredKeys;
    std::mutex mutex;
    std::condition_variable cv;
    TtlManager manager(clock_);
    manager.SetExpireCallback([&](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
        (void)revision;
        (void)ttlGeneration;
        {
            std::lock_guard<std::mutex> lock(mutex);
            expiredKeys.push_back(key);
        }
        cv.notify_all();
        return true;
    });
    manager.Start();

    DS_ASSERT_OK(manager.Schedule("/ttl/stale", 200, 2, 1));
    DS_ASSERT_OK(manager.Schedule("/ttl/stale", 50, 1, 1));

    clock_->AdvanceMs(60);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_FALSE(cv.wait_for(lock, std::chrono::milliseconds(120), [&] { return !expiredKeys.empty(); }));
    }

    clock_->AdvanceMs(150);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(1000), [&] { return expiredKeys.size() == 1; }));
    }
    ASSERT_EQ(expiredKeys[0], "/ttl/stale");
    manager.Stop();
}

TEST_F(CoordinatorStoreTest, TtlManagerScheduleSameKeyUpdatesExpiry)
{
    std::vector<std::string> expiredKeys;
    std::mutex mutex;
    std::condition_variable cv;
    TtlManager manager(clock_);
    manager.SetExpireCallback([&](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
        (void)revision;
        (void)ttlGeneration;
        {
            std::lock_guard<std::mutex> lock(mutex);
            expiredKeys.push_back(key);
        }
        cv.notify_all();
        return true;
    });
    manager.Start();

    DS_ASSERT_OK(manager.Schedule("/ttl/update", 100, 1, 1));
    clock_->AdvanceMs(50);
    DS_ASSERT_OK(manager.Schedule("/ttl/update", 200, 2, 1));

    clock_->AdvanceMs(60);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_FALSE(cv.wait_for(lock, std::chrono::milliseconds(120), [&] { return !expiredKeys.empty(); }));
    }

    clock_->AdvanceMs(150);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(1000), [&] { return expiredKeys.size() == 1; }));
    }
    ASSERT_EQ(expiredKeys[0], "/ttl/update");
    manager.Stop();
}

TEST_F(CoordinatorStoreTest, PutWithoutTtlRevokesPreviousTtl)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/ttl/revoke-by-put", "ttl", 100, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/ttl/revoke-by-put", "persist", 0, version, version, revision));

    clock_->AdvanceMs(150);
    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/ttl/revoke-by-put", "", kvs, rangeRevision));
    ASSERT_EQ(kvs.size(), 1ul);
    ASSERT_EQ(kvs[0].value, "persist");
}

TEST_F(CoordinatorStoreTest, TtlExpiredKeyCannotBeKeptAlive)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/ttl/expire", "v", 100, 0, version, revision));

    clock_->AdvanceMs(150);
    bool expired = false;
    for (int i = 0; i < 20; ++i) {
        std::vector<KeyValueEntry> kvs;
        int64_t rangeRevision = 0;
        DS_ASSERT_OK(store_->Range("/ttl/expire", "", kvs, rangeRevision));
        if (kvs.empty()) {
            expired = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(expired);

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    ASSERT_TRUE(store_->KeepAlive("/ttl/expire", ttlMs, remainingTtlMs).IsError());
}

TEST_F(CoordinatorStoreTest, RemoveChannelStopsFurtherNotifications)
{
    int64_t watchId = registry_->Register("/remove", "", "addr");
    dispatcher_->AddChannel(watchId, "addr");
    dispatcher_->SetSnapshotRevision(watchId, 1);
    dispatcher_->RemoveChannel(watchId);

    auto event = std::make_shared<WatchEvent>();
    event->type = WatchEvent::Type::PUT;
    event->entry = KeyValueEntry{ "/remove", "v", 1, 2 };
    event->revision = 2;
    dispatcher_->Enqueue(event);

    ASSERT_FALSE(dispatcher_->WaitEventCount(watchId, 1, 200));
}

TEST_F(CoordinatorStoreTest, DoNotifyFailureKeepsBatchForRetry)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<FailingWatchDispatcher>(registry.get());
    dispatcher->Start();

    int64_t watchId = registry->Register("/fail", "", "addr");
    dispatcher->AddChannel(watchId, "addr");
    dispatcher->SetSnapshotRevision(watchId, 1);

    auto event = std::make_shared<WatchEvent>();
    event->type = WatchEvent::Type::PUT;
    event->entry = KeyValueEntry{ "/fail", "retry", 1, 2 };
    event->revision = 2;
    dispatcher->Enqueue(event);

    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 1));
    auto events = dispatcher->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->entry.value, "retry");
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, BackpressureEnqueuesRewatchEventAndRemovesWatcher)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<BlockingWatchDispatcher>(registry.get());
    dispatcher->Start();

    int64_t watchId = registry->Register("/pressure", "", "addr");
    dispatcher->AddChannel(watchId, "addr");
    dispatcher->SetSnapshotRevision(watchId, 1);

    auto firstEvent = std::make_shared<WatchEvent>();
    firstEvent->type = WatchEvent::Type::PUT;
    firstEvent->entry = KeyValueEntry{ "/pressure", "first", 1, 2 };
    firstEvent->revision = 2;
    dispatcher->Enqueue(firstEvent);
    ASSERT_TRUE(dispatcher->WaitNotifyStarted());

    constexpr int OVERFLOW_EVENTS = 1500;
    int64_t lastOverflowRevision = 0;
    for (int i = 0; i < OVERFLOW_EVENTS; ++i) {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ "/pressure", std::to_string(i), i + 2, i + 3 };
        event->revision = i + 3;
        lastOverflowRevision = event->revision;
        dispatcher->Enqueue(event);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    dispatcher->Unblock();
    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 2));
    auto events = dispatcher->GetEvents(watchId);
    ASSERT_EQ(events.size(), 2ul);
    ASSERT_EQ(events[0]->type, WatchEvent::Type::PUT);
    ASSERT_EQ(events[1]->type, WatchEvent::Type::REWATCH);
    ASSERT_LE(events[1]->revision, lastOverflowRevision);

    bool removed = false;
    for (int i = 0; i < 20; ++i) {
        std::vector<std::shared_ptr<WatcherEntry>> matched;
        registry->MatchWatchers("/pressure", matched);
        if (matched.empty()) {
            removed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(removed);
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, RewatchNotifyFailureKeepsWatcherForRetry)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<FailingRewatchWatchDispatcher>(registry.get());
    dispatcher->Start();

    int64_t watchId = registry->Register("/rebuild-fail", "", "addr");
    dispatcher->AddChannel(watchId, "addr");

    constexpr int OVERFLOW_EVENTS = 1100;
    for (int i = 0; i < OVERFLOW_EVENTS; ++i) {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ "/rebuild-fail", std::to_string(i), i + 1, i + 2 };
        event->revision = i + 2;
        dispatcher->Enqueue(event);
    }

    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 1));
    auto events = dispatcher->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1ul);
    ASSERT_EQ(events[0]->type, WatchEvent::Type::REWATCH);

    bool removed = false;
    for (int i = 0; i < 20; ++i) {
        std::vector<std::shared_ptr<WatcherEntry>> matched;
        registry->MatchWatchers("/rebuild-fail", matched);
        if (matched.empty()) {
            removed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_TRUE(removed);
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, ConcurrentPutsAndRangesRemainConsistent)
{
    constexpr int THREAD_COUNT = 8;
    constexpr int OPS_PER_THREAD = 200;
    std::atomic<int> okPuts{ 0 };
    std::vector<std::thread> threads;

    for (int t = 0; t < THREAD_COUNT; ++t) {
        threads.emplace_back([this, t, &okPuts] {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                int64_t version = 0;
                int64_t revision = 0;
                std::string key = "/concurrent/" + std::to_string(t) + "/" + std::to_string(i);
                Status status = store_->Put(key, std::to_string(i), 0, 0, version, revision);
                if (status.IsOk()) {
                    okPuts.fetch_add(1);
                }

                std::vector<KeyValueEntry> kvs;
                int64_t rangeRevision = 0;
                DS_ASSERT_OK(store_->Range("/concurrent/", "/concurrent0", kvs, rangeRevision));
            }
        });
    }

    for (auto &thread : threads) {
        thread.join();
    }

    ASSERT_EQ(okPuts.load(), THREAD_COUNT * OPS_PER_THREAD);
    std::vector<KeyValueEntry> all;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/concurrent/", "/concurrent0", all, rangeRevision));
    ASSERT_EQ(all.size(), static_cast<size_t>(THREAD_COUNT * OPS_PER_THREAD));
}

TEST_F(CoordinatorStoreTest, ConcurrentWatchRegistrationAndEventsAreSafe)
{
    constexpr int WATCHER_COUNT = 64;
    std::vector<int64_t> watchIds(WATCHER_COUNT);
    std::vector<std::thread> registerThreads;

    for (int i = 0; i < WATCHER_COUNT; ++i) {
        registerThreads.emplace_back([this, i, &watchIds] {
            std::vector<KeyValueEntry> initial;
            DS_ASSERT_OK(store_->WatchRange("/watch/", "/watch0", "addr" + std::to_string(i), watchIds[i], initial));
        });
    }
    for (auto &thread : registerThreads) {
        thread.join();
    }

    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/watch/key", "v", 0, 0, version, revision));

    for (auto watchId : watchIds) {
        ASSERT_TRUE(dispatcher_->WaitEventCount(watchId, 1));
        auto events = dispatcher_->GetEvents(watchId);
        ASSERT_EQ(events.size(), 1ul);
        ASSERT_EQ(events[0]->entry.key, "/watch/key");
    }

    std::vector<std::thread> cancelThreads;
    for (int i = 0; i < WATCHER_COUNT; ++i) {
        cancelThreads.emplace_back([this, watchId = watchIds[i], watcherAddr = "addr" + std::to_string(i)] {
            DS_ASSERT_OK(registry_->Cancel(watchId, watcherAddr));
            dispatcher_->RemoveChannel(watchId);
        });
    }
    for (auto &thread : cancelThreads) {
        thread.join();
    }
}

TEST_F(CoordinatorStoreTest, RangeReturnsLexicographicHalfOpenInterval)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/range/1", "v1", 0, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/range/2", "v2", 0, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/range/3", "v3", 0, 0, version, revision));

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/range/1", "/range/3", kvs, rangeRevision));

    ASSERT_EQ(kvs.size(), 2UL);
    ASSERT_EQ(kvs[0].key, "/range/1");
    ASSERT_EQ(kvs[1].key, "/range/2");
    ASSERT_EQ(kvs[0].value, "v1");
    ASSERT_EQ(kvs[1].value, "v2");
    ASSERT_EQ(rangeRevision, revision);
}

TEST_F(CoordinatorStoreTest, DeleteRangeDeletesHalfOpenIntervalAndNoopDoesNotBumpRevision)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/delete-half-open/1", "v1", 0, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/delete-half-open/2", "v2", 0, 0, version, revision));
    DS_ASSERT_OK(store_->Put("/delete-half-open/3", "v3", 0, 0, version, revision));
    int64_t revisionBeforeDelete = revision;

    int64_t deleted = 0;
    int64_t deleteRevision = 0;
    DS_ASSERT_OK(store_->DeleteRange("/delete-half-open/1", "/delete-half-open/3", deleted, deleteRevision));
    ASSERT_EQ(deleted, 2);
    ASSERT_GT(deleteRevision, revisionBeforeDelete);

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/delete-half-open/", "/delete-half-open0", kvs, rangeRevision));
    ASSERT_EQ(kvs.size(), 1UL);
    ASSERT_EQ(kvs[0].key, "/delete-half-open/3");

    int64_t noopDeleted = 0;
    int64_t noopRevision = 0;
    DS_ASSERT_OK(store_->DeleteRange("/delete-half-open/missing", "", noopDeleted, noopRevision));
    ASSERT_EQ(noopDeleted, 0);
    ASSERT_EQ(noopRevision, deleteRevision);
}

TEST_F(CoordinatorStoreTest, DefaultConstructedCoordinatorStoreReturnsNotReady)
{
    CoordinatorStore store;
    int64_t version = 0;
    int64_t revision = 0;
    ASSERT_TRUE(store.Put("/not-ready", "v", 0, 0, version, revision).IsError());

    std::vector<KeyValueEntry> kvs;
    ASSERT_TRUE(store.Range("/not-ready", "", kvs, revision).IsError());

    int64_t deleted = 0;
    ASSERT_TRUE(store.DeleteRange("/not-ready", "", deleted, revision).IsError());

    int64_t watchId = 0;
    ASSERT_TRUE(store.WatchRange("/not-ready", "", "addr", watchId, kvs).IsError());

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    ASSERT_TRUE(store.KeepAlive("/not-ready", ttlMs, remainingTtlMs).IsError());
}

TEST_F(CoordinatorStoreTest, TtlManagerNonPositiveTtlDoesNotScheduleExpiry)
{
    std::vector<std::string> expiredKeys;
    std::mutex mutex;
    std::condition_variable cv;
    TtlManager manager(clock_);
    manager.SetExpireCallback([&](const std::string &key, int64_t revision, uint64_t ttlGeneration) {
        (void)revision;
        (void)ttlGeneration;
        {
            std::lock_guard<std::mutex> lock(mutex);
            expiredKeys.push_back(key);
        }
        cv.notify_all();
        return true;
    });
    manager.Start();
    DS_ASSERT_OK(manager.Schedule("/ttl/zero", 0, 1, 1));
    DS_ASSERT_OK(manager.Schedule("/ttl/negative", -1, 1, 1));

    clock_->AdvanceMs(100);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_FALSE(cv.wait_for(lock, std::chrono::milliseconds(120), [&] { return !expiredKeys.empty(); }));
    }
    manager.Stop();
}

TEST_F(CoordinatorStoreTest, PutWithoutTtlCannotBeKeptAlive)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/ttl/no-lease", "v", 0, 0, version, revision));

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    ASSERT_TRUE(store_->KeepAlive("/ttl/no-lease", ttlMs, remainingTtlMs).IsError());
}

TEST_F(CoordinatorStoreTest, WatchRegistryCancelUnknownReturnsErrorAndLastCancelCleansGroup)
{
    WatchRegistry registry;
    int64_t watchId = registry.Register("/registry/key", "", "addr");

    ASSERT_TRUE(registry.Cancel(watchId + 1, "addr").IsError());
    DS_ASSERT_OK(registry.Cancel(watchId, "addr"));

    std::vector<std::shared_ptr<WatcherEntry>> matched;
    registry.MatchWatchers("/registry/key", matched);
    ASSERT_TRUE(matched.empty());
}

TEST_F(CoordinatorStoreTest, WatchRegistryRangeBoundaryMatchingIsHalfOpen)
{
    WatchRegistry registry;
    int64_t watchId = registry.Register("/ring/1", "/ring/9", "addr");

    std::vector<std::shared_ptr<WatcherEntry>> matched;
    registry.MatchWatchers("/ring/1", matched);
    ASSERT_EQ(matched.size(), 1UL);
    ASSERT_EQ(matched[0]->watchId, watchId);

    matched.clear();
    registry.MatchWatchers("/ring/1/key", matched);
    ASSERT_EQ(matched.size(), 1UL);
    ASSERT_EQ(matched[0]->watchId, watchId);

    matched.clear();
    registry.MatchWatchers("/ring/9", matched);
    ASSERT_TRUE(matched.empty());
}

TEST_F(CoordinatorStoreTest, WatchDispatcherStartStopAreIdempotent)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<MockWatchDispatcher>(registry.get());
    dispatcher->Start();
    dispatcher->Start();
    dispatcher->Stop();
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, WatchDispatcherDispatchesEventsEnqueuedBeforeStart)
{
    auto registry = std::make_shared<WatchRegistry>();
    auto dispatcher = std::make_shared<MockWatchDispatcher>(registry.get());

    int64_t watchId = registry->Register("/before-start", "", "addr");
    dispatcher->AddChannel(watchId, "addr");
    dispatcher->SetSnapshotRevision(watchId, 1);

    auto event = std::make_shared<WatchEvent>();
    event->type = WatchEvent::Type::PUT;
    event->entry = KeyValueEntry{ "/before-start", "v", 1, 2 };
    event->revision = 2;
    dispatcher->Enqueue(event);

    dispatcher->Start();
    ASSERT_TRUE(dispatcher->WaitEventCount(watchId, 1));
    auto events = dispatcher->GetEvents(watchId);
    ASSERT_EQ(events.size(), 1UL);
    ASSERT_EQ(events[0]->entry.value, "v");
    dispatcher->Stop();
}

TEST_F(CoordinatorStoreTest, CasFailureDoesNotChangeValueRevisionEventOrTtl)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/cas/key", "v1", 100, 0, version, revision));
    int64_t revisionAfterPut = revision;

    int64_t watchId = 0;
    std::vector<KeyValueEntry> initial;
    DS_ASSERT_OK(store_->WatchRange("/cas/key", "", "addr", watchId, initial));
    ASSERT_EQ(initial.size(), 1UL);

    int64_t failedVersion = 0;
    int64_t failedRevision = 0;
    ASSERT_TRUE(store_->Put("/cas/key", "bad", 0, version + 1, failedVersion, failedRevision).IsError());

    std::vector<KeyValueEntry> kvs;
    int64_t rangeRevision = 0;
    DS_ASSERT_OK(store_->Range("/cas/key", "", kvs, rangeRevision));
    ASSERT_EQ(kvs.size(), 1UL);
    ASSERT_EQ(kvs[0].value, "v1");
    ASSERT_EQ(kvs[0].version, version);
    ASSERT_EQ(rangeRevision, revisionAfterPut);
    ASSERT_FALSE(dispatcher_->WaitEventCount(watchId, 1, 200));

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    DS_ASSERT_OK(store_->KeepAlive("/cas/key", ttlMs, remainingTtlMs));
    ASSERT_EQ(ttlMs, 100);
    ASSERT_EQ(remainingTtlMs, 100);
}

TEST_F(CoordinatorStoreTest, ExplicitDeleteRevokesTtl)
{
    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(store_->Put("/ttl/delete", "v", 100, 0, version, revision));

    int64_t deleted = 0;
    int64_t deleteRevision = 0;
    DS_ASSERT_OK(store_->DeleteRange("/ttl/delete", "", deleted, deleteRevision));
    ASSERT_EQ(deleted, 1);

    int64_t ttlMs = 0;
    int64_t remainingTtlMs = 0;
    ASSERT_TRUE(store_->KeepAlive("/ttl/delete", ttlMs, remainingTtlMs).IsError());
}

TEST_F(CoordinatorStoreTest, TtlManagerStartStopAreIdempotent)
{
    TtlManager manager(clock_);
    manager.Start();
    manager.Start();
    manager.Stop();
    manager.Stop();
}

TEST_F(CoordinatorStoreTest, MemoryKvStoreCasMissingKeyDoesNotBumpRevisionOrEmitEvent)
{
    MemoryKvStore store;
    std::vector<std::shared_ptr<WatchEvent>> events;
    store.SetMutationCallback([&events](std::shared_ptr<WatchEvent> event) { events.push_back(std::move(event)); });

    int64_t version = 0;
    int64_t revision = 0;
    uint64_t ttlGeneration = 0;
    ASSERT_TRUE(store.Put("/missing", "v", 0, 1, version, revision, ttlGeneration).IsError());

    ASSERT_EQ(store.CurrentRevision(), 1);
    ASSERT_TRUE(events.empty());
}
}  // namespace ut
}  // namespace datasystem
