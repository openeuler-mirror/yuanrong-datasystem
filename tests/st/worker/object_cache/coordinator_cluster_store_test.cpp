/*
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
 * Description: Test coordinator-backed cluster store behavior.
 */
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/worker/cluster_manager/cluster_constants.h"

DS_DECLARE_bool(auto_del_dead_node);
DS_DECLARE_uint32(node_dead_timeout_s);
DS_DECLARE_uint32(node_timeout_s);

#define DS_ASSERT_OK(_s)                                  \
    do {                                                  \
        Status __rc = (_s);                               \
        if (!__rc.IsOk()) {                               \
            ASSERT_TRUE(false) << __rc.ToString() << "."; \
        }                                                 \
    } while (false)

namespace datasystem {
namespace ut {
namespace {
constexpr int64_t KEEPALIVE_INTERVAL_MS = 10;
constexpr int64_t DEATH_TIMEOUT_MS = 20;
constexpr int64_t TEST_WAIT_TIMEOUT_MS = 2'000;
constexpr int64_t NO_EVENT_WAIT_MS = 200;
constexpr uint32_t SHORT_NODE_TIMEOUT_S = 1;
constexpr uint32_t SHORT_NODE_DEAD_TIMEOUT_S = 2;

class LocalWatchDispatcher : public WatchDispatcher {
public:
    using EventHandler = std::function<void(topology::CoordinationEvent &&)>;

    explicit LocalWatchDispatcher(WatchRegistry *watchRegistry) : WatchDispatcher(watchRegistry)
    {
    }

    ~LocalWatchDispatcher() override = default;

    void RegisterWatcherHandler(const std::string &watcherAddr, EventHandler handler)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_[watcherAddr] = std::move(handler);
    }

protected:
    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        (void)watchId;
        EventHandler handler;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = handlers_.find(watcherAddr);
            if (it == handlers_.end()) {
                return Status::OK();
            }
            handler = it->second;
        }

        for (const auto &event : events) {
            if (event == nullptr || event->type == WatchEvent::Type::REWATCH) {
                continue;
            }
            topology::CoordinationEvent clusterEvent;
            clusterEvent.type = event->type == WatchEvent::Type::DELETE ? topology::CoordinationEventType::DELETE
                                                                        : topology::CoordinationEventType::PUT;
            clusterEvent.key = event->entry.key;
            clusterEvent.value = event->entry.value;
            clusterEvent.version = event->entry.version;
            clusterEvent.revision = event->revision;
            handler(std::move(clusterEvent));
        }
        return Status::OK();
    }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, EventHandler> handlers_;
};

class LocalCoordinatorServiceProxy : public ICoordinatorServiceProxy {
public:
    LocalCoordinatorServiceProxy()
    {
        memKvStore_ = std::make_shared<MemoryKvStore>();
        watchRegistry_ = std::make_shared<WatchRegistry>();
        watchDispatcher_ = std::make_shared<LocalWatchDispatcher>(watchRegistry_.get());
        ttlManager_ = std::make_shared<TtlManager>();
        coordinatorStore_ =
            std::make_unique<CoordinatorStore>(memKvStore_, watchRegistry_, watchDispatcher_, ttlManager_);
        watchDispatcher_->Start();
        ttlManager_->Start();
    }

    ~LocalCoordinatorServiceProxy() override
    {
        watchDispatcher_->Stop();
        ttlManager_->Stop();
    }

    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision) override
    {
        return coordinatorStore_->Put(key, value, ttlMs, expectedVersion, version, revision);
    }

    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision) override
    {
        return coordinatorStore_->Range(key, rangeEnd, kvs, revision);
    }

    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted,
                       int64_t &revision) override
    {
        return coordinatorStore_->DeleteRange(key, rangeEnd, deleted, revision);
    }

    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs) override
    {
        return coordinatorStore_->WatchRange(key, rangeEnd, watcherAddr, watchId, initialKvs);
    }

    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds) override
    {
        return coordinatorStore_->CancelWatch(watcherAddr, watchIds);
    }

    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs) override
    {
        return coordinatorStore_->KeepAlive(key, ttlMs, remainingTtlMs);
    }

    Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version, int64_t &revision) override
    {
        (void)key;
        (void)processFunc;
        (void)version;
        (void)revision;
        RETURN_STATUS(K_NOT_SUPPORTED, "CAS is not needed by this test proxy");
    }

    void RegisterWatcherHandler(const std::string &watcherAddr, LocalWatchDispatcher::EventHandler handler)
    {
        watchDispatcher_->RegisterWatcherHandler(watcherAddr, std::move(handler));
    }

private:
    std::shared_ptr<MemoryKvStore> memKvStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<LocalWatchDispatcher> watchDispatcher_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> coordinatorStore_;
};
}  // namespace

class CoordinatorClusterStoreTest : public testing::Test {
public:
    void SetUp() override
    {
        originNodeTimeoutS_ = FLAGS_node_timeout_s;
        originNodeDeadTimeoutS_ = FLAGS_node_dead_timeout_s;
        originAutoDelDeadNode_ = FLAGS_auto_del_dead_node;
        coordinatorProxy_ = std::make_unique<LocalCoordinatorServiceProxy>();
    }

    void TearDown() override
    {
        coordinatorProxy_.reset();
        FLAGS_node_timeout_s = originNodeTimeoutS_;
        FLAGS_node_dead_timeout_s = originNodeDeadTimeoutS_;
        FLAGS_auto_del_dead_node = originAutoDelDeadNode_;
        (void)inject::Clear("CoordinationBackend.KeepAlive.intervalMs");
        (void)inject::Clear("CoordinationBackend.KeepAlive.confirmTimes");
        (void)inject::Clear("CoordinationBackend.KeepAlive.returnError");
        (void)inject::Clear("CoordinationBackend.KeepAlive.kill");
        (void)inject::Clear("CoordinationBackend.KeepAlive.deathTimeoutMs");
    }

protected:
    std::unique_ptr<LocalCoordinatorServiceProxy> coordinatorProxy_;
    uint32_t originNodeTimeoutS_ = 0;
    uint32_t originNodeDeadTimeoutS_ = 0;
    bool originAutoDelDeadNode_ = true;
};

TEST_F(CoordinatorClusterStoreTest, KeepAliveLocalNetworkFailureTriggersKillFallback)
{
    FLAGS_node_timeout_s = SHORT_NODE_TIMEOUT_S;
    FLAGS_node_dead_timeout_s = SHORT_NODE_DEAD_TIMEOUT_S;
    FLAGS_auto_del_dead_node = true;
    DS_ASSERT_OK(
        inject::Set("CoordinationBackend.KeepAlive.intervalMs", "call(" + std::to_string(KEEPALIVE_INTERVAL_MS) + ")"));
    DS_ASSERT_OK(inject::Set("CoordinationBackend.KeepAlive.confirmTimes", "call(1)"));
    DS_ASSERT_OK(inject::Set("CoordinationBackend.KeepAlive.returnError", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(inject::Set("CoordinationBackend.KeepAlive.kill", "return(K_OK)"));
    DS_ASSERT_OK(
        inject::Set("CoordinationBackend.KeepAlive.deathTimeoutMs", "call(" + std::to_string(DEATH_TIMEOUT_MS) + ")"));

    topology::CoordinationBackend store(coordinatorProxy_.get(), "127.0.0.1:0");
    store.SetCheckStoreStateWhenNetworkFailedHandler([]() { return true; });

    const std::string workerAddr = "127.0.0.1:102";
    DS_ASSERT_OK(store.InitKeepAlive(CLUSTER_TABLE, workerAddr, false, true));

    Timer timer;
    while (timer.ElapsedMilliSecond() < TEST_WAIT_TIMEOUT_MS) {
        if (inject::GetExecuteCount("CoordinationBackend.KeepAlive.kill") > 0) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(KEEPALIVE_INTERVAL_MS));
    }
    ASSERT_GT(inject::GetExecuteCount("CoordinationBackend.KeepAlive.kill"), 0U);
}

TEST_F(CoordinatorClusterStoreTest, DestructorCancelsOwnedWatches)
{
    const std::string watcherAddr = "127.0.0.1:30000";
    std::mutex mutex;
    std::condition_variable cv;
    int eventCount = 0;
    coordinatorProxy_->RegisterWatcherHandler(watcherAddr, [&](topology::CoordinationEvent &&event) {
        if (event.type == topology::CoordinationEventType::PUT) {
            std::lock_guard<std::mutex> lock(mutex);
            ++eventCount;
            cv.notify_all();
        }
    });

    {
        topology::CoordinationBackend store(coordinatorProxy_.get(), watcherAddr);
        DS_ASSERT_OK(store.WatchEvents({ { CLUSTER_TABLE, "", 0 }, { CLUSTER_TABLE, "node", 0 } }));
    }

    int64_t version = 0;
    int64_t revision = 0;
    DS_ASSERT_OK(coordinatorProxy_->Put(std::string(CLUSTER_TABLE) + "/127.0.0.1:301", "1;start", 0,
                                        COORDINATOR_NO_VERSION_CHECK, version, revision));

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_FALSE(cv.wait_for(lock, std::chrono::milliseconds(NO_EVENT_WAIT_MS), [&]() { return eventCount > 0; }));
    ASSERT_EQ(eventCount, 0);
}
}  // namespace ut
}  // namespace datasystem
