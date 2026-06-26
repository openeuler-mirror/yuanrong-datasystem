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
 * Description: Cluster metadata store abstraction used by ClusterManager.
 */
#ifndef DATASYSTEM_TOPOLOGY_COORDINATION_BACKEND_COORDINATION_BACKEND_H
#define DATASYSTEM_TOPOLOGY_COORDINATION_BACKEND_COORDINATION_BACKEND_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/etcd/etcd_watch.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

enum class CoordinationEventType : uint8_t { UNSPECIFIED = 0, PUT, DELETE };

struct CoordinationEvent {
    CoordinationEventType type;
    std::string key;
    std::string value;
    int64_t version = 0;
    int64_t revision = 0;

    std::string ToString() const;
};

struct WatchKey {
    std::string tableName;
    std::string key;
    int64_t startRevision = 0;
};

class ICoordinationBackend {
public:
    using EventHandler = std::function<void(CoordinationEvent &&event)>;
    using ProcessFunction = std::function<Status(const std::string &, std::unique_ptr<std::string> &, bool &)>;

    virtual ~ICoordinationBackend() = default;

    virtual Status GetAll(const std::string &tableName,
                          std::vector<std::pair<std::string, std::string>> &outKeyValues) = 0;
    virtual Status Get(const std::string &tableName, const std::string &key, std::string &value) = 0;
    virtual Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
                       int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) = 0;
    virtual Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
                       RangeSearchResult &res) = 0;
    virtual Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc) = 0;
    virtual Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
                       const std::string &newValue) = 0;
    virtual Status Delete(const std::string &tableName, const std::string &key) = 0;
    virtual Status WatchEvents(const std::vector<WatchKey> &watchKeys) = 0;
    virtual Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                 bool isStoreAvailableWhenStart) = 0;
    virtual Status UpdateNodeState(const std::string &state) = 0;
    virtual Status GetStorePrefix(const std::string &tableName, std::string &prefix) = 0;
    virtual Status InformReconciliationDone(const HostPort &workerAddr) = 0;
    virtual bool IsKeepAliveTimeout() = 0;
    virtual bool IsCreateFirstLease() = 0;
    virtual void SetEventHandler(EventHandler &&eventHandler) = 0;
    virtual void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) = 0;
};

class CoordinationBackend : public ICoordinationBackend {
public:
    CoordinationBackend(ICoordinatorServiceProxy *proxy, std::string watcherAddr);
    ~CoordinationBackend() override;

    Status GetAll(const std::string &tableName,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues) override;
    Status Get(const std::string &tableName, const std::string &key, std::string &value) override;
    Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) override;
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
               RangeSearchResult &res) override;
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc) override;
    Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override;
    Status Delete(const std::string &tableName, const std::string &key) override;
    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override;
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override;
    Status UpdateNodeState(const std::string &state) override;
    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override;
    Status InformReconciliationDone(const HostPort &workerAddr) override;
    bool IsKeepAliveTimeout() override;
    bool IsCreateFirstLease() override;
    void SetEventHandler(EventHandler &&eventHandler) override;
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

    const std::string &GetWatcherAddr() const;
    void HandleWatchEvent(CoordinationEvent &&event);

private:
    static std::string RemoveTablePrefix(const std::string &key, const std::string &prefix);
    std::string BuildRealKey(const std::string &tableName, const std::string &key);
    Status AutoCreateKeepAliveKey();
    Status RenewKeepAliveOnce();
    void LaunchKeepAliveThread();
    void RunKeepAliveLoop();
    void HandleKeepAliveFailed(const std::string &realKey);
    Status KillLocalWorkerForKeepAliveFailure();
    void CancelWatches();
    void ShutdownKeepAliveThread();

    ICoordinatorServiceProxy *proxy_;
    std::unordered_map<std::string, std::string> tablePrefixes_;
    std::string watcherAddr_;
    std::vector<int64_t> watchIds_;
    std::mutex eventHandlerMutex_;
    EventHandler eventHandler_;
    std::function<bool()> checkStoreStateWhenNetworkFailedHandler_;
    std::string keepAliveTableName_;
    std::string keepAliveKey_;
    KeepAliveValue keepAliveValue_;
    std::mutex keepAliveMutex_;
    std::condition_variable keepAliveCv_;
    std::thread keepAliveThread_;
    std::atomic<bool> keepAliveExit_{ false };
    std::atomic<bool> keepAliveTimeout_{ false };
    std::atomic<bool> isCreateFirstLease_{ false };
    static constexpr int64_t MS_PER_SECOND = 1'000;
    int64_t keepAliveTtlMs_ = 0;
};
}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_COORDINATION_BACKEND_COORDINATION_BACKEND_H
