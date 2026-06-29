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
 * Description: Test coordinator service mock and direct watch dispatcher.
 */
#ifndef DATASYSTEM_TESTS_ST_WORKER_OBJECT_CACHE_COORDINATOR_CLUSTER_STORE_MOCK_H
#define DATASYSTEM_TESTS_ST_WORKER_OBJECT_CACHE_COORDINATOR_CLUSTER_STORE_MOCK_H

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/worker/cluster_manager/cluster_constants.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/coordinator/watch_registry.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"
#include "datasystem/worker/cluster_manager/cluster_manager.h"

namespace datasystem {
namespace st {
class WatchDispatcherMock : public WatchDispatcher {
public:
    using EventHandler = std::function<void(topology::CoordinationEvent &&)>;

    explicit WatchDispatcherMock(WatchRegistry *watchRegistry) : WatchDispatcher(watchRegistry)
    {
    }

    ~WatchDispatcherMock() override = default;

    void RegisterWatcherHandler(const std::string &watcherAddr, EventHandler handler)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_[watcherAddr] = std::move(handler);
    }

    void UnregisterWatcherHandler(const std::string &watcherAddr)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_.erase(watcherAddr);
    }

    size_t HandlerCount()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return handlers_.size();
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

class CoordinatorServiceProxyMock : public ICoordinatorServiceProxy {
public:
    CoordinatorServiceProxyMock()
    {
        memKvStore_ = std::make_shared<MemoryKvStore>();
        watchRegistry_ = std::make_shared<WatchRegistry>();
        watchDispatcher_ = std::make_shared<WatchDispatcherMock>(watchRegistry_.get());
        ttlManager_ = std::make_shared<TtlManager>();
        coordinatorStore_ =
            std::make_unique<CoordinatorStore>(memKvStore_, watchRegistry_, watchDispatcher_, ttlManager_);
        watchDispatcher_->Start();
        ttlManager_->Start();
    }

    ~CoordinatorServiceProxyMock() override
    {
        watchDispatcher_->Stop();
        ttlManager_->Stop();
    }

    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision, int32_t timeoutMs) override
    {
        (void)timeoutMs;
        return coordinatorStore_->Put(key, value, ttlMs, expectedVersion, version, revision);
    }

    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision, int32_t timeoutMs) override
    {
        (void)timeoutMs;
        return coordinatorStore_->Range(key, rangeEnd, kvs, revision);
    }

    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                       int32_t timeoutMs) override
    {
        (void)timeoutMs;
        return coordinatorStore_->DeleteRange(key, rangeEnd, deleted, revision);
    }

    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      int64_t &watchId, std::vector<KeyValueEntry> &initialKvs, int32_t timeoutMs) override
    {
        (void)timeoutMs;
        return coordinatorStore_->WatchRange(key, rangeEnd, watcherAddr, watchId, initialKvs);
    }

    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds, int32_t timeoutMs) override
    {
        (void)timeoutMs;
        return coordinatorStore_->CancelWatch(watcherAddr, watchIds);
    }

    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t timeoutMs) override
    {
        (void)timeoutMs;
        return coordinatorStore_->KeepAlive(key, ttlMs, remainingTtlMs);
    }

    Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version, int64_t &revision) override
    {
        CHECK_FAIL_RETURN_STATUS(processFunc != nullptr, K_INVALID, "Coordinator CAS process function is null");
        constexpr uint32_t maxRetryNum = 1000;
        for (uint32_t i = 0; i < maxRetryNum; ++i) {
            std::vector<KeyValueEntry> kvs;
            int64_t rangeRevision = 0;
            RETURN_IF_NOT_OK(Range(key, "", kvs, rangeRevision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
            const bool exists = !kvs.empty();
            const std::string oldValue = exists ? kvs.front().value : "";
            const int64_t expectedVersion = exists ? kvs.front().version : COORDINATOR_KEY_NOT_EXISTS_VERSION;

            std::unique_ptr<std::string> newValue;
            bool retry = true;
            auto status = processFunc(oldValue, newValue, retry);
            if (status.IsError()) {
                if (!retry) {
                    return status;
                }
                continue;
            }
            if (newValue == nullptr) {
                return Status::OK();
            }

            status = Put(key, *newValue, 0, expectedVersion, version, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS);
            if (status.IsOk()) {
                return Status::OK();
            }
            if (status.GetCode() != K_INVALID && status.GetCode() != K_NOT_FOUND) {
                return status;
            }
        }
        RETURN_STATUS(K_TRY_AGAIN, "Coordinator CAS failed after retries");
    }

    void RegisterWatcherHandler(const std::string &watcherAddr, WatchDispatcherMock::EventHandler handler)
    {
        watchDispatcher_->RegisterWatcherHandler(watcherAddr, std::move(handler));
    }

    void UnregisterWatcherHandler(const std::string &watcherAddr)
    {
        watchDispatcher_->UnregisterWatcherHandler(watcherAddr);
    }

    size_t HandlerCount()
    {
        return watchDispatcher_->HandlerCount();
    }

private:
    std::shared_ptr<MemoryKvStore> memKvStore_;
    std::shared_ptr<WatchRegistry> watchRegistry_;
    std::shared_ptr<WatchDispatcherMock> watchDispatcher_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> coordinatorStore_;
};

inline Status ConstructClusterInfoViaCoordinator(topology::ICoordinationBackend *clusterStore, ClusterInfo &clusterInfo)
{
    CHECK_FAIL_RETURN_STATUS(clusterStore != nullptr, K_RUNTIME_ERROR, "Coordination backend is null");
    std::vector<std::pair<std::string, std::string>> workers;
    RETURN_IF_NOT_OK(clusterStore->GetAll(CLUSTER_TABLE, workers));
    clusterInfo.coordinatorAvailable = true;
    clusterInfo.revision = 0;
    clusterInfo.workers = std::move(workers);
    return Status::OK();
}
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_TESTS_ST_WORKER_OBJECT_CACHE_COORDINATOR_CLUSTER_STORE_MOCK_H
