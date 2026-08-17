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
 * Description: Process-local Coordinator Store adapter for cluster topology control.
 */
#ifndef DATASYSTEM_COORDINATOR_COORDINATOR_STORE_BACKEND_H
#define DATASYSTEM_COORDINATOR_COORDINATOR_STORE_BACKEND_H

#include "datasystem/cluster/coordination_backend/coordination_backend.h"

namespace datasystem {
class CoordinatorStore;
struct KeyValueEntry;
}  // namespace datasystem

namespace datasystem::coordinator {

/**
 * @brief Adapt a borrowed process-local CoordinatorStore to the controller-role coordination contract.
 *
 * The Host guarantees that the Store outlives this adapter and destroys the Runtime before this adapter.
 */
class CoordinatorStoreBackend final : public cluster::ICoordinationBackend {
public:
    /**
     * @brief Construct a controller-only local Store adapter.
     * @param[in] store Store that outlives this backend.
     */
    explicit CoordinatorStoreBackend(CoordinatorStore &store);

    /**
     * @brief Release the non-owning adapter without shutting down the borrowed Store.
     */
    ~CoordinatorStoreBackend() override;

    /**
     * @brief Disable copying a non-owning Store adapter.
     */
    CoordinatorStoreBackend(const CoordinatorStoreBackend &) = delete;

    /**
     * @brief Disable copy assignment of a non-owning Store adapter.
     */
    CoordinatorStoreBackend &operator=(const CoordinatorStoreBackend &) = delete;

    /**
     * @brief Read all key/value pairs from one logical table.
     * @param[in] tableName Logical table name.
     * @param[out] outKeyValues Returned relative key/value pairs.
     * @return Backend operation status.
     */
    Status GetAll(const std::string &tableName,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    Status GetAll(const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues,
                  int64_t &responseRevision) override;

    /**
     * @brief Read one exact key and return its value.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[out] value Returned value.
     * @return Backend operation status.
     */
    Status Get(const std::string &tableName, const std::string &key, std::string &value) override;

    /**
     * @brief Read one exact key and preserve version information.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[out] result Returned Store result.
     * @param[in] timeoutMs Ignored local-operation timeout retained by the interface.
     * @return Backend operation status.
     */
    Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &result,
               int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) override;

    /**
     * @brief Execute bounded callback-form CAS and return version information.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] process Existing-value transformation callback.
     * @param[out] result Result replaced only after a successful operation.
     * @return Backend operation status.
     */
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &process,
               RangeSearchResult &result) override;

    Status CASAtRevision(const std::string &tableName, const std::string &key, const ProcessFunction &process,
                         int64_t expectedRevision, RangeSearchResult &result) override;

    /**
     * @brief Execute bounded callback-form CAS.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] process Existing-value transformation callback.
     * @return Backend operation status.
     */
    Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &process) override;

    /**
     * @brief Execute raw-value single-key CAS.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] oldValue Expected current value.
     * @param[in] newValue Desired value.
     * @return Backend operation status.
     */
    Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
               const std::string &newValue) override;

    /**
     * @brief Delete one exact key with the backend default timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @return Backend operation status.
     */
    Status Delete(const std::string &tableName, const std::string &key) override;

    /**
     * @brief Delete one exact key.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] timeoutMs Ignored local-operation timeout retained by the interface.
     * @return Backend operation status.
     */
    Status Delete(const std::string &tableName, const std::string &key, int timeoutMs) override;

    /**
     * @brief Reject watch registration because TopologyControlHost owns the event source.
     * @param[in] watchKeys Watch descriptors.
     * @return K_INVALID for this controller-only adapter.
     */
    Status WatchEvents(const std::vector<cluster::WatchKey> &watchKeys) override;

    /**
     * @brief Reject lease-bound writes because this controller-only adapter owns no Worker membership lease.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] value Sidecar value retained by the interface.
     * @return K_INVALID for this controller-only adapter.
     */
    Status PutWithKeepAliveLease(const std::string &tableName, const std::string &key,
                                 const std::string &value) override;

    /**
     * @brief Reject membership keepalive initialization for the controller-only role.
     * @param[in] tableName Logical membership table name.
     * @param[in] key Local membership key.
     * @param[in] isRestart Whether the local member is restarting.
     * @param[in] isStoreAvailableWhenStart Whether the Store was initially available.
     * @return K_INVALID for this controller-only adapter.
     */
    Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                         bool isStoreAvailableWhenStart) override;

    /**
     * @brief Preserve the contract without owning asynchronous event sources.
     * @return Always K_OK.
     */
    Status ShutdownEventSources() override;

    /**
     * @brief Preserve the contract without shutting down the borrowed Store.
     * @return Always K_OK.
     */
    Status Shutdown() override;

    /**
     * @brief Reject member lifecycle updates for the controller-only role.
     * @param[in] state Requested lifecycle state.
     * @return K_INVALID for this controller-only adapter.
     */
    Status UpdateNodeState(cluster::MemberLifecycleState state) override;

    /**
     * @brief Return the local Store prefix assigned to a logical table.
     * @param[in] tableName Logical table name.
     * @param[out] prefix Physical table prefix.
     * @return K_OK or K_INVALID for an empty table name.
     */
    Status GetStorePrefix(const std::string &tableName, std::string &prefix) override;

    /**
     * @brief Reject Worker reconciliation completion for the controller-only role.
     * @param[in] workerAddr Worker address retained by the interface.
     * @return K_INVALID for this controller-only adapter.
     */
    Status InformReconciliationDone(const HostPort &workerAddr) override;

    /**
     * @brief Report that this adapter has no keepalive timeout.
     * @return Always false.
     */
    bool IsKeepAliveTimeout() override;

    /**
     * @brief Report that this adapter never sends a keepalive.
     * @return Always false.
     */
    bool IsFirstKeepAliveSent() override;

    /**
     * @brief Accept and discard the unused backend event handler.
     * @param[in] eventHandler Event callback to discard.
     */
    void SetEventHandler(EventHandler &&eventHandler) override;

    /**
     * @brief Accept and discard the unused remote Store-state callback.
     * @param[in] handler Store-state callback to discard.
     */
    void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) override;

private:
    /**
     * @brief Read one physical key while preserving value and revision.
     * @param[in] physicalKey Exact physical Store key.
     * @param[out] entry Current Store entry; unchanged when absent.
     * @return K_OK, K_NOT_FOUND, or Store read status.
     */
    Status ReadExact(const std::string &physicalKey, KeyValueEntry &entry) const;

    /**
     * @brief Execute bounded callback CAS on one exact physical key.
     * @param[in] physicalKey Exact physical Store key.
     * @param[in] process Existing-value transformation callback.
     * @param[out] result Result replaced only after success.
     * @return K_OK, a non-retryable error, or the final retryable error.
     */
    Status RunCas(const std::string &physicalKey, const ProcessFunction &process, RangeSearchResult &result,
                  int64_t expectedRevision = 0);

    // Borrowed by the adapter and guaranteed to outlive it by TopologyControlHost.
    CoordinatorStore &store_;
};

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_COORDINATOR_STORE_BACKEND_H
