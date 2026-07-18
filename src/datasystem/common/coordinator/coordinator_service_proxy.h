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
 * Description: Coordinator service proxy and Coordinator process-lifetime identity fence.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/coordinator.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class RpcOptions;

constexpr int32_t DEFAULT_COORDINATOR_RPC_TIMEOUT_MS = 3'000;

class ICoordinatorServiceProxy {
public:
    using CasProcessFunc =
        std::function<Status(const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry)>;

    /**
     * @brief Release the proxy after callers and in-flight RPCs have stopped.
     */
    virtual ~ICoordinatorServiceProxy() = default;

    /**
     * @brief Put one key/value pair and optionally return the exact response CoordinatorId.
     * @param[in] key Physical key.
     * @param[in] value Value bytes.
     * @param[in] ttlMs TTL in milliseconds; zero disables expiration.
     * @param[in] expectedVersion Existing CAS contract.
     * @param[out] version Result key version.
     * @param[out] revision Result store revision.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @param[out] coordinatorId Exact response CoordinatorId; nullptr ignores it.
     * @param[in] expectedCoordinatorId Required Coordinator process lifetime; empty disables the fence.
     * @return Existing Put status contract.
     */
    virtual Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
                       int64_t &version, int64_t &revision, int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                       std::string *coordinatorId = nullptr, const std::string &expectedCoordinatorId = "") = 0;

    /**
     * @brief Read an exact key or range.
     * @param[in] key Physical start key.
     * @param[in] rangeEnd Physical range end; empty means exact read.
     * @param[out] kvs Result key/value entries.
     * @param[out] revision Result store revision.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @param[out] coordinatorId Exact response CoordinatorId; nullptr ignores it.
     * @return Existing Range status contract.
     */
    virtual Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                         int64_t &revision, int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                         std::string *coordinatorId = nullptr) = 0;

    /**
     * @brief Delete an exact key or range.
     * @param[in] key Physical start key.
     * @param[in] rangeEnd Physical range end; empty means exact delete.
     * @param[out] deleted Number of deleted keys.
     * @param[out] revision Result store revision.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @return Existing DeleteRange status contract.
     */
    virtual Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                               int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;

    /**
     * @brief Register one watch and optionally return the exact response CoordinatorId.
     * @param[in] key Physical start key.
     * @param[in] rangeEnd Physical range end; empty means exact watch.
     * @param[in] watcherAddr Worker callback address.
     * @param[in] registrationId Stable registration ID for ambiguous RPC retry.
     * @param[out] watchId Watch identity within the response CoordinatorId.
     * @param[out] initialKvs Initial snapshot.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @param[out] coordinatorId Exact response CoordinatorId; nullptr ignores it.
     * @return Existing WatchRange status contract.
     */
    virtual Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                              const std::string &registrationId, int64_t &watchId,
                              std::vector<KeyValueEntry> &initialKvs,
                              int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                              std::string *coordinatorId = nullptr) = 0;

    /**
     * @brief Cancel watches owned by one callback address.
     * @param[in] watcherAddr Worker callback address.
     * @param[in] watchIds Watch identities to cancel.
     * @param[in] expectedCoordinatorId Coordinator lifetime that owns watchIds.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @return Existing CancelWatch status contract.
     */
    virtual Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                               const std::string &expectedCoordinatorId,
                               int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;

    /**
     * @brief Renew one membership lease and optionally return the exact response CoordinatorId.
     * @param[in] key Physical membership key.
     * @param[out] ttlMs Configured TTL in milliseconds.
     * @param[out] remainingTtlMs Remaining TTL in milliseconds.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @param[out] coordinatorId Exact response CoordinatorId; nullptr ignores it.
     * @return Existing KeepAlive status contract.
     */
    virtual Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs,
                             int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS,
                             std::string *coordinatorId = nullptr) = 0;

    /**
     * @brief Run the existing read-modify-CAS retry contract.
     * @param[in] key Physical key.
     * @param[in] processFunc Caller mutation callback.
     * @param[out] version Committed key version.
     * @param[out] revision Committed store revision.
     * @return Existing CAS status contract.
     */
    virtual Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version,
                       int64_t &revision) = 0;

    /**
     * @brief Query the current CoordinatorId on a cold error or ambiguous-identity path.
     * @param[out] coordinatorId Current CoordinatorId.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @return RPC or response validation status.
     */
    virtual Status GetCoordinatorId(std::string &coordinatorId,
                                    int32_t timeoutMs = DEFAULT_COORDINATOR_RPC_TIMEOUT_MS) = 0;

    /**
     * @brief Report one Worker topology recovery evidence or payload to Coordinator.
     * @param[in] req Cluster, CoordinatorId, reporter and candidate evidence or payload.
     * @param[out] rsp Acceptance, recovery state and payload request.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @return RPC, admission or response validation status.
     */
    virtual Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &req,
                                                   coordinator::ReportTopologyRecoveryCandidateRspPb &rsp,
                                                   int32_t timeoutMs) = 0;

    /**
     * @brief Copy the current non-retired CoordinatorId observed from a successful response or identity probe.
     * @param[out] coordinatorId Current CoordinatorId; empty before first observation.
     */
    virtual void GetObservedCoordinatorId(std::string &coordinatorId) const = 0;
};

class CoordinatorServiceProxyBase : public ICoordinatorServiceProxy {
public:
    /**
     * @brief Construct a proxy without a configured Coordinator address.
     */
    CoordinatorServiceProxyBase() = default;

    /**
     * @brief Construct a proxy for one Coordinator endpoint.
     * @param[in] coordinatorAddr Coordinator RPC endpoint.
     */
    explicit CoordinatorServiceProxyBase(HostPort coordinatorAddr) : coordinatorAddr_(std::move(coordinatorAddr))
    {
    }

    /**
     * @brief Release proxy-local identity state after all callers stop.
     */
    ~CoordinatorServiceProxyBase() override = default;

    /**
     * @copydoc ICoordinatorServiceProxy::Put
     */
    Status Put(const std::string &key, const std::string &value, int64_t ttlMs, int64_t expectedVersion,
               int64_t &version, int64_t &revision, int32_t timeoutMs, std::string *coordinatorId,
               const std::string &expectedCoordinatorId) override;

    /**
     * @copydoc ICoordinatorServiceProxy::Range
     */
    Status Range(const std::string &key, const std::string &rangeEnd, std::vector<KeyValueEntry> &kvs,
                 int64_t &revision, int32_t timeoutMs, std::string *coordinatorId) override;

    /**
     * @copydoc ICoordinatorServiceProxy::DeleteRange
     */
    Status DeleteRange(const std::string &key, const std::string &rangeEnd, int64_t &deleted, int64_t &revision,
                       int32_t timeoutMs) override;

    /**
     * @copydoc ICoordinatorServiceProxy::WatchRange
     */
    Status WatchRange(const std::string &key, const std::string &rangeEnd, const std::string &watcherAddr,
                      const std::string &registrationId, int64_t &watchId,
                      std::vector<KeyValueEntry> &initialKvs, int32_t timeoutMs,
                      std::string *coordinatorId) override;

    /**
     * @copydoc ICoordinatorServiceProxy::CancelWatch
     */
    Status CancelWatch(const std::string &watcherAddr, const std::vector<int64_t> &watchIds,
                       const std::string &expectedCoordinatorId,
                       int32_t timeoutMs) override;

    /**
     * @copydoc ICoordinatorServiceProxy::KeepAlive
     */
    Status KeepAlive(const std::string &key, int64_t &ttlMs, int64_t &remainingTtlMs, int32_t timeoutMs,
                     std::string *coordinatorId) override;

    /**
     * @copydoc ICoordinatorServiceProxy::CAS
     */
    Status CAS(const std::string &key, const CasProcessFunc &processFunc, int64_t &version, int64_t &revision) override;

    /**
     * @copydoc ICoordinatorServiceProxy::GetCoordinatorId
     */
    Status GetCoordinatorId(std::string &coordinatorId, int32_t timeoutMs) override;

    /**
     * @copydoc ICoordinatorServiceProxy::ReportTopologyRecoveryCandidate
     */
    Status ReportTopologyRecoveryCandidate(const coordinator::ReportTopologyRecoveryCandidateReqPb &req,
                                           coordinator::ReportTopologyRecoveryCandidateRspPb &rsp,
                                           int32_t timeoutMs) override;

    /**
     * @brief Read raw topology and membership facts for one logical cluster.
     * @param[in] req Logical cluster name only.
     * @param[out] rsp Raw facts observed by the Coordinator.
     * @param[in] timeoutMs RPC deadline in milliseconds.
     * @return RPC or response validation status.
     */
    Status GetClusterRawSnapshot(const coordinator::GetClusterRawSnapshotReqPb &req,
                                 coordinator::GetClusterRawSnapshotRspPb &rsp, int32_t timeoutMs);

    /**
     * @copydoc ICoordinatorServiceProxy::GetObservedCoordinatorId
     */
    void GetObservedCoordinatorId(std::string &coordinatorId) const override;

protected:
    enum class Transport { ZMQ, BRPC };

    /**
     * @brief Return the concrete RPC transport used by this proxy.
     * @return ZMQ or BRPC transport kind.
     */
    virtual Transport GetTransport() const = 0;

    HostPort coordinatorAddr_;

private:
    class InFlightScope;

    /**
     * @brief Record the current identity for one new in-flight RPC.
     * @param[in] timeoutMs RPC deadline used by response confirmation.
     * @return Scope that completes the in-flight record.
     */
    InFlightScope BeginRpc(int32_t timeoutMs);

    /**
     * @brief Invoke one RPC through the configured transport without identity recursion.
     * @param[in,out] options RPC options.
     * @param[in] req RPC request.
     * @param[out] rsp RPC response.
     * @param[in] call Stub invocation.
     * @return Transport status.
     */
    template <typename ReqT, typename RspT, typename CallT>
    Status CallRaw(RpcOptions &options, const ReqT &req, RspT &rsp, CallT call);

    /**
     * @brief Validate one successful response against the process-lifetime fence.
     * @param[in] header Response metadata.
     * @param[in] timeoutMs Identity-probe deadline.
     * @param[out] coordinatorId Accepted identity; nullptr ignores it.
     * @return Header or identity status.
     */
    Status AcceptResponse(const coordinator::ResponseHeader &header, int32_t timeoutMs, std::string *coordinatorId);

    /**
     * @brief Confirm an unfamiliar response identity through one raw probe.
     * @param[in] responseId Unfamiliar response identity.
     * @param[in] timeoutMs Probe deadline.
     * @return K_OK only when the probe confirms responseId.
     */
    Status ConfirmResponseIdentity(const std::string &responseId, int32_t timeoutMs);

    /**
     * @brief Read the current CoordinatorId without entering the normal response fence.
     * @param[in] timeoutMs Probe deadline.
     * @param[out] coordinatorId Validated current identity.
     * @return Transport or response-validation status.
     */
    Status ProbeCoordinatorId(int32_t timeoutMs, std::string &coordinatorId);

    /**
     * @brief Replace current identity while rejecting an in-flight retired identity.
     * @param[in] coordinatorId Identity confirmed by a raw probe.
     * @return K_OK or K_TRY_AGAIN for a retired identity.
     */
    Status InstallProbedIdentity(const std::string &coordinatorId);

    /**
     * @brief Release one identity's in-flight reference.
     * @param[in] startedCoordinatorId Identity captured before RPC dispatch.
     */
    void CompleteRpc(const std::string &startedCoordinatorId);

    // Protects currentCoordinatorId_ and inFlightByCoordinatorId_. Non-current in-flight IDs are retired.
    mutable std::mutex identityMutex_;
    std::string currentCoordinatorId_;
    std::unordered_map<std::string, size_t> inFlightByCoordinatorId_;
    // Serializes cold-path identity probes; when both locks are needed, acquire this before identityMutex_.
    std::mutex identityRefreshMutex_;
};

class CoordinatorServiceProxyZmqImpl final : public CoordinatorServiceProxyBase {
public:
    using CoordinatorServiceProxyBase::CoordinatorServiceProxyBase;

    /**
     * @brief Release the ZMQ proxy after all callers have stopped.
     */
    ~CoordinatorServiceProxyZmqImpl() override = default;

protected:
    /**
     * @brief Select the ZMQ Coordinator RPC stub.
     * @return ZMQ transport kind.
     */
    Transport GetTransport() const override
    {
        return Transport::ZMQ;
    }
};

class CoordinatorServiceProxyBrpcImpl final : public CoordinatorServiceProxyBase {
public:
    using CoordinatorServiceProxyBase::CoordinatorServiceProxyBase;

    /**
     * @brief Release the BRPC proxy after all callers have stopped.
     */
    ~CoordinatorServiceProxyBrpcImpl() override = default;

protected:
    /**
     * @brief Select the BRPC Coordinator RPC stub.
     * @return BRPC transport kind.
     */
    Transport GetTransport() const override
    {
        return Transport::BRPC;
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_SERVICE_PROXY_H
