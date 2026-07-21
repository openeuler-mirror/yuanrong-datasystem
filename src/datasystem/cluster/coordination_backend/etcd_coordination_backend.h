/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: ETCD adapter for the cluster coordination backend contract.
 */
#ifndef DATASYSTEM_CLUSTER_COORDINATION_BACKEND_ETCD_COORDINATION_BACKEND_H
#define DATASYSTEM_CLUSTER_COORDINATION_BACKEND_ETCD_COORDINATION_BACKEND_H

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"

namespace datasystem::cluster {

/**
 * @brief Lightweight coordination adapter over a non-owned EtcdStore.
 */
class EtcdCoordinationBackend final : public ICoordinationBackend {
public:
    /**
     * @brief Construct the adapter using the current pointer-form interface.
     * @param[in] etcdStore Non-owning Store pointer; adapters may share one Store for synchronous operations.
     */
    explicit EtcdCoordinationBackend(EtcdStore *etcdStore);

    /**
     * @brief Destroy the ETCD backend adapter.
     */
    ~EtcdCoordinationBackend() override = default;

    /**
     * @brief Convert one ETCD watch event to the existing coordination event shape.
     * @param[in] event ETCD watch event.
     * @return Converted coordination event.
     */
    static CoordinationEvent FromEtcdEvent(const mvccpb::Event &event);

    /**
     * @brief Read all key/value pairs from one ETCD logical table.
     * @param[in] tableName Logical table name.
     * @param[out] outKeyValues Returned key/value pairs.
     * @return Backend operation status.
     */
    Status GetAll(const std::string &tableName,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    /**
     * @brief Read one exact ETCD key and return its value.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[out] value Returned value.
     * @return Backend operation status.
     */
    Status Get(const std::string &tableName, const std::string &key, std::string &value) override;

    /**
     * @brief Read one exact ETCD key with revision information.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[out] res Returned range-search result.
     * @param[in] timeoutMs Operation timeout in milliseconds.
     * @return Backend operation status.
     */
    Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) override;

    /**
     * @brief Idempotently create one ETCD logical table.
     * @param[in] tableName Logical table name.
     * @param[in] tablePrefix Physical table prefix.
     * @return Backend operation status.
     */
    Status CreateTable(const std::string &tableName, const std::string &tablePrefix) override;

    /**
     * @brief Put one exact ETCD key/value pair.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] value Value bytes.
     * @return Backend operation status.
     */
    Status Put(const std::string &tableName, const std::string &key, const std::string &value) override;

    /**
     * @brief Execute callback-form single-key ETCD CAS with revision information.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] processFunc Existing-value transformation callback.
     * @param[out] res Returned range-search result.
     * @return Backend operation status.
     */
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
               RangeSearchResult &res) override;

    /**
     * @brief Execute callback-form single-key ETCD CAS.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] processFunc Existing-value transformation callback.
     * @return Backend operation status.
     */
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc) override;

    /**
     * @brief Execute raw-value single-key ETCD CAS.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] oldValue Expected current value.
     * @param[in] newValue Desired value.
     * @return Backend operation status.
     */
    Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override;

    /**
     * @brief Delete one exact ETCD key using the default timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @return Backend operation status.
     */
    Status Delete(const std::string &tableName, const std::string &key) override;

    /**
     * @brief Delete one exact ETCD key using an explicit timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] timeoutMs Operation timeout in milliseconds.
     * @return Backend operation status.
     */
    Status Delete(const std::string &tableName, const std::string &key, int timeoutMs) override;

    /**
     * @brief Register role-scoped ETCD watch descriptors.
     * @param[in] watchKeys Watch descriptors.
     * @return Backend operation status.
     */
    Status WatchEvents(const std::vector<WatchKey> &watchKeys) override;

    /**
     * @brief Initialize the existing ETCD membership keepalive.
     * @param[in] tableName Logical membership table name.
     * @param[in] key Local membership key.
     * @param[in] isRestart Whether the local member is restarting.
     * @param[in] isStoreAvailableWhenStart Whether ETCD was available at process start.
     * @return Backend operation status.
     */
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override;

    /**
     * @brief Stop ETCD watch/keepalive event sources while preserving synchronous operations.
     * @return Backend operation status.
     */
    Status ShutdownEventSources() override;

    /**
     * @brief Shut down all runtime resources of the non-owning ETCD store binding.
     * @return Backend operation status.
     */
    Status Shutdown() override;

    /**
     * @brief Update the lifecycle state in the current ETCD keepalive value.
     * @param[in] state New lifecycle state.
     * @return Backend operation status.
     */
    Status UpdateNodeState(MemberLifecycleState state) override;

    /**
     * @brief Return the physical ETCD prefix for one logical table.
     * @param[in] tableName Logical table name.
     * @param[out] prefix Physical ETCD prefix.
     * @return Backend operation status.
     */
    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override;

    /**
     * @brief Invoke the existing ETCD reconciliation-complete hook.
     * @param[in] workerAddr Worker address expected by the current interface.
     * @return Backend operation status.
     */
    Status InformReconciliationDone(const HostPort &workerAddr) override;

    /**
     * @brief Check whether the ETCD keepalive has timed out.
     * @return True when the current keepalive is timed out.
     */
    bool IsKeepAliveTimeout() override;

    /**
     * @brief Check whether the first ETCD keepalive has been sent.
     * @return True after the first keepalive has been sent.
     */
    bool IsFirstKeepAliveSent() override;

    /**
     * @brief Install the event handler for this ETCD backend instance.
     * @param[in] eventHandler Event callback to consume.
     */
    void SetEventHandler(EventHandler &&eventHandler) override;

    /**
     * @brief Install the ETCD member local-isolation callback.
     * @param[in] handler Callback to consume.
     */
    void SetLocalIsolationHandler(LocalIsolationHandler handler) override;

    /**
     * @brief Install the ETCD member local-recovery callback.
     * @param[in] handler Callback to consume.
     */
    void SetLocalRecoveryHandler(LocalRecoveryHandler handler) override;

    /**
     * @brief Install the existing bool ETCD store-state callback.
     * @param[in] handler Store-state callback.
     */
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

private:
    EtcdStore *etcdStore_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_COORDINATION_BACKEND_ETCD_COORDINATION_BACKEND_H
