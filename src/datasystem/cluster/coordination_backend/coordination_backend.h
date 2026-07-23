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
 * Description: Primitive coordination backend contract for cluster topology.
 */
#ifndef DATASYSTEM_CLUSTER_COORDINATION_BACKEND_COORDINATION_BACKEND_H
#define DATASYSTEM_CLUSTER_COORDINATION_BACKEND_COORDINATION_BACKEND_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/kvstore/etcd/etcd_watch.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief Preserve the existing default exact-delete deadline constant.
 */
constexpr int DEFAULT_COORDINATION_DELETE_TIMEOUT_MS = 3'000;

/**
 * @brief Existing backend watch operation type.
 */
enum class CoordinationEventType : uint8_t { UNSPECIFIED = 0, PUT, DELETE, RESET };

/**
 * @brief Backend watch doorbell; value is never published directly as topology.
 */
struct CoordinationEvent {
    CoordinationEventType type;
    std::string key;
    std::string value;
    int64_t version = 0;
    int64_t revision = 0;

    /**
     * @brief Convert the coordination event to diagnostic text without its value.
     * @return Safe diagnostic string.
     */
    std::string ToString() const;
};

/**
 * @brief Existing backend watch descriptor; empty key means table prefix.
 */
struct WatchKey {
    std::string tableName;
    std::string key;
    int64_t startRevision{ 0 };
};

/**
 * @brief Preserve the existing public backend result alias.
 */
using CoordinationStoreResult = RangeSearchResult;

/**
 * @brief Master-compatible primitive coordination backend contract.
 */
class ICoordinationBackend {
public:
    using EventHandler = std::function<void(CoordinationEvent &&event)>;
    using LocalIsolationHandler = std::function<void(const Status &)>;
    using LocalRecoveryHandler = std::function<void()>;
    using MembershipReadyHandler = std::function<void(const std::string &, bool)>;
    using ProcessFunction = std::function<Status(const std::string &, std::unique_ptr<std::string> &, bool &)>;

    /**
     * @brief Destroy the backend interface.
     */
    virtual ~ICoordinationBackend() = default;

    /**
     * @brief Read all key/value pairs from one logical table.
     * @param[in] tableName Logical table name.
     * @param[out] outKeyValues Returned key/value pairs.
     * @return Backend operation status.
     */
    virtual Status GetAll(const std::string &tableName,
                          std::vector<std::pair<std::string, std::string>> &outKeyValues) = 0;

    /**
     * @brief Read one exact key and return its value.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[out] value Returned value.
     * @return Backend operation status.
     */
    virtual Status Get(const std::string &tableName, const std::string &key, std::string &value) = 0;

    /**
     * @brief Read one exact key and preserve backend revision information.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[out] res Returned range-search result.
     * @param[in] timeoutMs Operation timeout in milliseconds.
     * @return Backend operation status.
     */
    virtual Status Get(const std::string &tableName, const std::string &key, RangeSearchResult &res,
                       int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT) = 0;

    /**
     * @brief Idempotently create one logical table.
     * @param[in] tableName Logical table name.
     * @param[in] tablePrefix Physical table prefix.
     * @return Backend operation status.
     */
    virtual Status CreateTable(const std::string &tableName, const std::string &tablePrefix) = 0;

    /**
     * @brief Idempotently create one logical table using an already-canonical physical prefix.
     * @param[in] tableName Logical table name.
     * @param[in] tablePrefix Canonical physical table prefix.
     * @return Backend operation status.
     */
    virtual Status CreateTableWithExactPrefix(const std::string &tableName, const std::string &tablePrefix) = 0;

    /**
     * @brief Put one exact key/value pair.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] value Value bytes.
     * @return Backend operation status.
     */
    virtual Status Put(const std::string &tableName, const std::string &key, const std::string &value) = 0;

    /**
     * @brief Execute callback-form single-key CAS and return revision information.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] processFunc Existing-value transformation callback.
     * @param[out] res Returned range-search result.
     * @return Backend operation status.
     */
    virtual Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc,
                       RangeSearchResult &res) = 0;

    /**
     * @brief Execute callback-form single-key CAS.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] processFunc Existing-value transformation callback.
     * @return Backend operation status.
     */
    virtual Status CAS(const std::string &tableName, const std::string &key, const ProcessFunction &processFunc) = 0;

    /**
     * @brief Execute raw-value single-key CAS.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] oldValue Expected current value.
     * @param[in] newValue Desired value.
     * @return Backend operation status.
     */
    virtual Status CAS(const std::string &tableName, const std::string &key, const std::string &oldValue,
                       const std::string &newValue) = 0;

    /**
     * @brief Delete one exact key with the backend default timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @return Backend operation status.
     */
    virtual Status Delete(const std::string &tableName, const std::string &key) = 0;

    /**
     * @brief Delete one exact key with an explicit timeout.
     * @param[in] tableName Logical table name.
     * @param[in] key Exact relative key.
     * @param[in] timeoutMs Operation timeout in milliseconds.
     * @return Backend operation status.
     */
    virtual Status Delete(const std::string &tableName, const std::string &key, int timeoutMs) = 0;

    /**
     * @brief Register watches using the current backend contract.
     * @param[in] watchKeys Watch descriptors.
     * @return Backend operation status.
     */
    virtual Status WatchEvents(const std::vector<WatchKey> &watchKeys) = 0;

    /**
     * @brief Initialize the existing membership keepalive/session.
     * @param[in] tableName Logical membership table name.
     * @param[in] key Local membership key.
     * @param[in] isRestart Whether the local member is restarting.
     * @param[in] isStoreAvailableWhenStart Whether the store was available at process start.
     * @return Backend operation status.
     */
    virtual Status InitKeepAlive(const std::string &tableName, const std::string &key, bool isRestart,
                                 bool isStoreAvailableWhenStart) = 0;

    /**
     * @brief Idempotently stop asynchronous watch and keepalive callbacks while preserving synchronous operations.
     * @return Backend operation status.
     */
    virtual Status ShutdownEventSources() = 0;

    /**
     * @brief Idempotently stop watch event sources without stopping the membership keepalive.
     * @return Backend operation status.
     */
    virtual Status ShutdownWatchEventSources() = 0;

    /**
     * @brief Shut down all runtime resources owned by this backend instance.
     * @return Backend operation status.
     */
    virtual Status Shutdown() = 0;

    /**
     * @brief Update the lifecycle state stored in the current membership lease.
     * @param[in] state New lifecycle state.
     * @return Backend operation status.
     */
    virtual Status UpdateNodeState(MemberLifecycleState state) = 0;

    /**
     * @brief Return the physical prefix assigned to one logical table.
     * @param[in] tableName Logical table name.
     * @param[out] prefix Physical table prefix.
     * @return Backend operation status.
     */
    virtual Status GetStorePrefix(const std::string &tableName, std::string &prefix) = 0;

    /**
     * @brief Invoke the existing reconciliation-complete hook.
     * @param[in] workerAddr Worker address expected by the current interface.
     * @return Backend operation status.
     */
    virtual Status InformReconciliationDone(const HostPort &workerAddr) = 0;

    /**
     * @brief Check whether the local keepalive has timed out.
     * @return True when the current keepalive is timed out.
     */
    virtual bool IsKeepAliveTimeout() = 0;

    /**
     * @brief Check whether the first keepalive has been sent.
     * @return True after the first keepalive has been sent.
     */
    virtual bool IsFirstKeepAliveSent() = 0;

    /**
     * @brief Install the event handler for this backend instance.
     * @param[in] eventHandler Event callback to consume.
     */
    virtual void SetEventHandler(EventHandler &&eventHandler) = 0;

    /**
     * @brief Install the worker/member keepalive local-isolation callback owned by this backend role.
     * @param[in] handler Callback invoked when this member backend confirms local control-backend isolation.
     */
    virtual void SetLocalIsolationHandler(LocalIsolationHandler handler) = 0;

    /**
     * @brief Install the worker/member keepalive local-recovery callback owned by this backend role.
     * @param[in] handler Callback invoked when this member backend renews membership after local isolation.
     */
    virtual void SetLocalRecoveryHandler(LocalRecoveryHandler handler) = 0;

    /**
     * @brief Install the optional exact membership-operation callback for Coordinator-backed watches.
     * @param[in] handler Callback invoked with the Coordinator identity and whether existing watches were invalidated.
     */
    virtual void SetMembershipReadyHandler(const MembershipReadyHandler &handler);

    /**
     * @brief Check whether this backend owns one Coordinator watch identity.
     * @param[in] coordinatorId Coordinator process-lifetime identity.
     * @param[in] watchId Watch identity within that Coordinator lifetime.
     * @return True when this backend owns the identity.
     */
    virtual bool OwnsWatchIdentity(const std::string &coordinatorId, int64_t watchId) const;

    /**
     * @brief Check whether a watch registration transaction is still in progress.
     * @return True when ownership may not be visible yet.
     */
    virtual bool IsWatchRegistrationInProgress() const;

    /**
     * @brief Invalidate cached watch identity state and trigger a best-effort rewatch.
     */
    virtual void InvalidateWatches();

    /**
     * @brief Deliver one identity-bound Coordinator watch event.
     * @param[in] coordinatorId Coordinator process-lifetime identity.
     * @param[in] watchId Watch identity within that Coordinator lifetime.
     * @param[in] event Event delivered by the Worker watch RPC service.
     */
    virtual void HandleWatchEvent(const std::string &coordinatorId, int64_t watchId, CoordinationEvent &&event);

    /**
     * @brief Install the current bool store-state callback without changing its interface.
     * @param[in] handler Store-state callback.
     */
    virtual void SetCheckStoreStateWhenNetworkFailedHandler(std::function<bool()> handler) = 0;
};

/**
 * @brief Create a Coordinator-backed coordination backend behind the backend interface.
 * @param[in] proxy Coordinator proxy that must outlive the returned backend.
 * @param[in] watcherAddr Canonical Worker address used to register watches.
 * @return Coordinator-backed backend instance.
 */
std::unique_ptr<ICoordinationBackend> CreateDsCoordinationBackend(ICoordinatorServiceProxy *proxy,
                                                                  std::string watcherAddr);

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_COORDINATION_BACKEND_COORDINATION_BACKEND_H
