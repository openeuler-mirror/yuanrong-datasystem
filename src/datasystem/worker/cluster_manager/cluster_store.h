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
#ifndef DATASYSTEM_WORKER_CLUSTER_MANAGER_CLUSTER_STORE_H
#define DATASYSTEM_WORKER_CLUSTER_MANAGER_CLUSTER_STORE_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/utils/status.h"

namespace datasystem {
enum class ClusterStoreEventType : uint8_t { UNSPECIFIED = 0, PUT, DELETE };

struct ClusterStoreEvent {
    ClusterStoreEventType type;
    std::string key;
    std::string value;
    int64_t version = 0;
    int64_t revision = 0;

    std::string ToString() const;
};

struct ClusterWatchElement {
    std::string tableName;
    std::string key;
    int64_t startRevision = 0;
};

class IClusterStore {
public:
    using EventHandler = std::function<void(ClusterStoreEvent &&event)>;
    using ProcessFunction = std::function<Status(const std::string &, std::unique_ptr<std::string> &, bool &)>;

    virtual ~IClusterStore() = default;

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
    virtual Status WatchEvents(const std::vector<ClusterWatchElement> &watchKeys) = 0;
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

class EtcdClusterStore : public IClusterStore {
public:
    explicit EtcdClusterStore(EtcdStore *etcdStore);
    ~EtcdClusterStore() override = default;

    static ClusterStoreEvent FromEtcdEvent(const mvccpb::Event &event);

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
    Status WatchEvents(const std::vector<ClusterWatchElement> &watchKeys) override;
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override;
    Status UpdateNodeState(const std::string &state) override;
    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override;
    Status InformReconciliationDone(const HostPort &workerAddr) override;
    bool IsKeepAliveTimeout() override;
    bool IsCreateFirstLease() override;
    void SetEventHandler(EventHandler &&eventHandler) override;
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

private:
    EtcdStore *etcdStore_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_CLUSTER_MANAGER_CLUSTER_STORE_H
