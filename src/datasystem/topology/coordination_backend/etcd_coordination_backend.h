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
 * Description: Etcd-backed topology coordination backend.
 */
#ifndef DATASYSTEM_TOPOLOGY_COORDINATION_BACKEND_ETCD_COORDINATION_BACKEND_H
#define DATASYSTEM_TOPOLOGY_COORDINATION_BACKEND_ETCD_COORDINATION_BACKEND_H

#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/topology/coordination_backend/coordination_backend.h"

namespace datasystem {
namespace topology {

class EtcdCoordinationBackend : public ICoordinationBackend {
public:
    explicit EtcdCoordinationBackend(EtcdStore *etcdStore);
    ~EtcdCoordinationBackend() override = default;

    static CoordinationEvent FromEtcdEvent(const mvccpb::Event &event);

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
    Status Delete(const std::string &tableName, const std::string &key, int timeoutMs) override;
    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override;
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override;
    Status UpdateNodeState(const std::string &state) override;
    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override;
    Status InformReconciliationDone(const HostPort &workerAddr) override;
    bool IsKeepAliveTimeout() override;
    bool IsFirstKeepAliveSent() override;
    void SetEventHandler(EventHandler &&eventHandler) override;
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

private:
    EtcdStore *etcdStore_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_COORDINATION_BACKEND_ETCD_COORDINATION_BACKEND_H
