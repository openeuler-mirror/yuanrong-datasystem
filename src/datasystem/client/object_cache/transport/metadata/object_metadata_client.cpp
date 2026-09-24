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

/** Description: Implements Worker-routed batched metadata and inline-data access. */

#include "datasystem/client/object_cache/transport/metadata/object_metadata_client.h"

#include "datasystem/client/object_cache/transport/object_read/object_read_types.h"
#include "datasystem/client/object_cache/transport/transport_phase_latency_recorder.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

namespace {
// Confirmed owner migration: the route is known stale, wrap immediately so the outer retry reroutes.
bool IsConfirmedMetadataOwnerRouteFailure(StatusCode code)
{
    return code == K_RPC_PEER_DEAD || code == K_METADATA_OWNER_UNAVAILABLE;
}

// Ambiguous owner degradation: the same owner may still answer; retry it in place a bounded number
// of times before wrapping, so transient worker degradation does not cascade into route churn.
bool IsAmbiguousMetadataOwnerRouteFailure(StatusCode code)
{
    return code == K_RPC_DEADLINE_EXCEEDED || code == K_RPC_UNAVAILABLE || code == K_CLIENT_WORKER_DISCONNECT;
}

bool IsMetadataOwnerRouteFailure(StatusCode code)
{
    return IsConfirmedMetadataOwnerRouteFailure(code) || IsAmbiguousMetadataOwnerRouteFailure(code);
}

constexpr int32_t ROUTE_DEGRADATION_INNER_RETRIES = 2;
constexpr int64_t ROUTE_DEGRADATION_RETRY_BACKOFF_MS = 100;

Status MakeStaleMetadataRouteStatus(const Status &rc)
{
    Status stale(K_NOT_READY, std::string(STALE_TRANSPORT_SNAPSHOT_MESSAGE) + ": " + rc.ToString());
    if (IsNonRetryableRpcError(rc) && IsBrpcRequestDefinitelyNotSent(rc)) {
        stale.WithExtra(METADATA_INGRESS_NOT_SENT);
    }
    return stale;
}

Status ValidateAndResetItems(const ObjectMetadataBatch &items)
{
    CHECK_FAIL_RETURN_STATUS(!items.empty(), K_INVALID, "Metadata query items are empty");
    for (auto *item : items) {
        RETURN_RUNTIME_ERROR_IF_NULL(item);
        CHECK_FAIL_RETURN_STATUS(!item->objectKey.empty(), K_INVALID, "Object key is empty");
        item->status = Status(K_NOT_READY, "Object metadata is not resolved");
        item->location.Clear();
        item->inlineData.reset();
    }
    return Status::OK();
}

void CopyLocation(const QueryAndGetLocationInfoPb &source, master::ObjectLocationInfoPb &target)
{
    target.set_object_key(source.object_key());
    target.set_object_size(source.object_size());
    target.set_topology_version(source.topology_version());
    *target.mutable_object_locations() = source.object_locations();
}
}  // namespace

ObjectMetadataClient::ObjectMetadataClient(std::shared_ptr<DataPlaneManager> manager,
                                           std::shared_ptr<DeadlineRetry> retry,
                                           std::shared_ptr<TransportAdvisor> advisor,
                                           std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider,
                                           uint64_t ubBufferSize,
                                           std::function<void(const HostPort &, const Status &)> metadataFailureHandler,
                                           std::function<void(const HostPort &,
                                                              const ProviderUbFailureDetailPb &)> ubFailureHandler)
    : manager_(std::move(manager)),
      retry_(std::move(retry)),
      advisor_(std::move(advisor)),
      ubBufferProvider_(std::move(ubBufferProvider)),
      ubBufferSize_(ubBufferSize),
      metadataFailureHandler_(std::move(metadataFailureHandler)),
      ubFailureHandler_(std::move(ubFailureHandler))
{
}

Status ObjectMetadataClient::InitializeInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                                     std::shared_ptr<const TransportReadContext> readContext,
                                                     InlineRequestContext &context,
                                                     TransportPhaseLatencyRecorder *recorder) const
{
    context = InlineRequestContext{};
    const auto hint = advisor_ == nullptr ? TransportHint::TCP_ONLY : advisor_->GetTransportHint(address);
    if (hint == TransportHint::SHM_CANDIDATE) {
        RETURN_IF_NOT_OK(PrepareShmInlineRequest(address, std::move(readContext), context, recorder));
        RETURN_OK_IF_TRUE(context.mode == InlineTransportMode::SHM);
    }
    if (hint == TransportHint::UB_CANDIDATE || IsUrmaEnabled()) {
        return PrepareUbInlineRequest(address, items, context, recorder);
    }
    context.mode = InlineTransportMode::TCP;
    return Status::OK();
}

Status ObjectMetadataClient::PrepareShmInlineRequest(const HostPort &address,
                                                     std::shared_ptr<const TransportReadContext> readContext,
                                                     InlineRequestContext &context,
                                                     TransportPhaseLatencyRecorder *recorder) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    if (readContext == nullptr) {
        context.DisableInlineData();
        return Status::OK();
    }
    std::shared_ptr<IDataTransporter> transporter;
    Status rc = manager_->GetOrCreate(address, TransportHint::SHM_CANDIDATE, transporter, recorder);
    auto shmTransporter = std::dynamic_pointer_cast<ShmTransporter>(transporter);
    if (rc.IsError() || shmTransporter == nullptr) {
        context.DisableInlineData();
        return Status::OK();
    }
    std::shared_ptr<ShmSession> session;
    rc = shmTransporter->TryAcquireSession(readContext->requestContext, session, recorder);
    if (rc.IsError()) {
        context.DisableInlineData();
        return Status::OK();
    }
    context.mode = InlineTransportMode::SHM;
    context.shmTransporter = std::move(shmTransporter);
    context.shmSession = std::move(session);
    context.readContext = std::move(readContext);
    return Status::OK();
}

Status ObjectMetadataClient::PrepareShmInlineFallback(const HostPort &address,
                                                      const ObjectMetadataBatch &items,
                                                      InlineRequestContext &context,
                                                      TransportPhaseLatencyRecorder *recorder) const
{
    context.DisableInlineData();
    if (IsUrmaEnabled()) {
        return PrepareUbInlineRequest(address, items, context, recorder);
    }
    context.mode = InlineTransportMode::TCP;
    return Status::OK();
}

Status ObjectMetadataClient::PrepareUbInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                                    InlineRequestContext &context,
                                                    TransportPhaseLatencyRecorder *recorder) const
{
    if (ubBufferSize_ == 0 || ubBufferProvider_ == nullptr || ubBufferSize_ > ubBufferProvider_->MaxGetSize()
        || items.size() > ubBufferProvider_->MaxGetSize() / ubBufferSize_) {
        context.mode = InlineTransportMode::TCP;
        return Status::OK();
    }

    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    // A single entry lookup covers both read-path gates:
    // - the rebuild cooldown: skip a handshake the peer rejected a moment ago, and finish over TCP inline;
    // - the per-endpoint rebuild slot: a wave of readers can all pass the cooldown before the first handshake
    //   fails and arms it, so without a slot every queued request pays for its own handshake (N x handshake
    //   latency, and N doomed handshakes when the peer keeps rejecting). Only the request that wins the slot
    //   rebuilds; the others serve this request over TCP inline instead of queueing. Waiters never block, and
    //   no lock is held across the RPC.
    // Both gates stay private to the read path, so writers, direct leases and replica reads keep their own
    // behaviour. Both need the entry, and a lookup costs a HostPort::ToString() plus a hash probe, so they
    // share the single one made inside AdmitUbRead().
    uint64_t slotOwner = 0;
    bool degradedByCooldown = false;
    if (manager_->AdmitUbRead(address, slotOwner, &degradedByCooldown) == DataPlaneManager::UbReadAdmission::DEGRADE) {
        VLOG(1) << "[TransportGet][Metadata] Disable UB inline data because "
                << (degradedByCooldown ? "the data plane is cooling down" : "a UB rebuild is already in flight") << ": "
                << address.ToString();
        context.mode = InlineTransportMode::TCP;
        return Status::OK();
    }
    // Establish UB first so a connection miss does not consume receive-buffer capacity.
    std::shared_ptr<IDataTransporter> transporter;
    Status connectionRc;
    {
        // Release the slot as soon as the handshake attempt ends; the buffer allocation below must not
        // extend the window in which other readers are degraded. Only construct the guard when there is a
        // slot to release: the steady state (PROCEED) holds none, and building it anyway would cost a
        // shared_ptr copy plus a HostPort copy on every read.
        std::optional<DataPlaneManager::UbRebuildSlotGuard> slotGuard;
        if (slotOwner != 0) {
            slotGuard.emplace(manager_, address, slotOwner);
        }
        connectionRc = manager_->GetOrCreate(address, TransportHint::UB_CANDIDATE, transporter, recorder);
    }
    if (connectionRc.IsError()) {
        VLOG(1) << "[TransportGet][Metadata] Disable UB inline data because the connection is unavailable: "
                << connectionRc.ToString();
        // A failed handshake is not a K_URMA_NEED_CONNECT response, so it never reaches the cooldown marked
        // in PrepareQueryRetry, and GetOrCreate deliberately enforces no gate of its own. Bound read-path
        // handshake attempts here (still read-path private) so a peer that keeps rejecting the handshake
        // degrades to TCP instead of re-attempting the handshake on every request.
        manager_->MarkUbRebuildCooldown(address);
        context.mode = InlineTransportMode::TCP;
        return Status::OK();
    }

    if (AllocateUbInlineBuffers(items, context).IsError()) {
        context.mode = InlineTransportMode::TCP;
        return Status::OK();
    }
    context.mode = InlineTransportMode::UB;
    return Status::OK();
}

Status ObjectMetadataClient::AllocateUbInlineBuffers(const ObjectMetadataBatch &items,
                                                     InlineRequestContext &context) const
{
    context.ubBuffers.reserve(items.size());
    for (auto *item : items) {
        UbReceiveBuffer buffer;
        Status allocRc = ubBufferProvider_->Allocate(ubBufferSize_, buffer);
        if (allocRc.IsError() || buffer.data == nullptr || buffer.owner == nullptr || buffer.size < ubBufferSize_
            || buffer.transportInstanceId.empty()) {
            VLOG(1) << "[TransportGet][Metadata] Disable UB inline data because receive-buffer preparation failed: "
                    << allocRc.ToString();
            context.DisableInlineData();
            RETURN_STATUS(K_NOT_READY, "UB inline receive-buffer preparation failed");
        }
        if (context.transportInstanceId.empty()) {
            context.transportInstanceId = buffer.transportInstanceId;
        }
        if (context.transportInstanceId != buffer.transportInstanceId) {
            context.DisableInlineData();
            RETURN_STATUS(K_RUNTIME_ERROR, "UB receive buffers use different transport instances");
        }
        context.ubBuffers.emplace(item, std::move(buffer));
    }
    return Status::OK();
}

Status ObjectMetadataClient::AddInlineDataRequest(const ObjectMetadataBatch &items,
                                                  const InlineRequestContext &context,
                                                  QueryAndGetReqPb &request) const
{
    if (context.mode == InlineTransportMode::NONE) {
        return Status::OK();
    }
    if (context.mode == InlineTransportMode::TCP) {
        (void)request.mutable_data_request()->mutable_tcp();
        return Status::OK();
    }
    if (context.mode == InlineTransportMode::SHM) {
        CHECK_FAIL_RETURN_STATUS(context.shmSession != nullptr && context.shmSession->IsAlive(), K_NOT_READY,
                                 "QueryAndGet shared-memory session is unavailable");
        request.mutable_data_request()->mutable_shm()->set_client_id(context.shmSession->ClientId());
        return Status::OK();
    }

    auto *ubRequest = request.mutable_data_request()->mutable_ub();
    ubRequest->set_buffer_size(ubBufferSize_);
    ubRequest->set_urma_instance_id(context.transportInstanceId);
    // Buffer descriptors follow object_keys order.
    for (auto *item : items) {
        auto buffer = context.ubBuffers.find(item);
        CHECK_FAIL_RETURN_STATUS(buffer != context.ubBuffers.end(), K_RUNTIME_ERROR,
                                 "UB inline receive buffer does not match requested keys");
        *ubRequest->add_buffer_infos() = buffer->second.remoteAddr;
    }
    return Status::OK();
}

Status ObjectMetadataClient::InvokeQueryAndGet(const HostPort &address, QueryAndGetReqPb &request,
                                               QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                                               InlineRequestContext &context, bool &rpcDispatched,
                                               TransportPhaseLatencyRecorder *recorder)
{
    rpcDispatched = false;
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    if (context.mode == InlineTransportMode::UB || context.mode == InlineTransportMode::SHM) {
        bool invoked = false;
        Status leaseRc =
            InvokeInlineQueryAndGet(address, request, response, payloads, context, invoked, rpcDispatched, recorder);
        if (invoked) {
            return leaseRc;
        }
        if (context.mode == InlineTransportMode::UB
            && (context.requireUb || leaseRc.GetCode() == K_TRY_AGAIN)) {
            return leaseRc;
        }
        VLOG(1) << "[TransportGet][Metadata] Inline data plane is unavailable for " << address.ToString()
                << ", fallback to TCP: " << leaseRc.ToString();
        SwitchInlineRequestToTcp(request, payloads, context);
    }
    return InvokeTcpQueryAndGet(address, request, response, payloads, rpcDispatched);
}

Status ObjectMetadataClient::InvokeInlineQueryAndGet(const HostPort &address, QueryAndGetReqPb &request,
                                                     QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                                                     InlineRequestContext &context, bool &invoked,
                                                     bool &rpcDispatched,
                                                     TransportPhaseLatencyRecorder *recorder)
{
    const auto hint = context.mode == InlineTransportMode::UB ? TransportHint::UB_CANDIDATE
                                                             : TransportHint::SHM_CANDIDATE;
    invoked = false;
    return manager_->WithDataPlaneLease(
        address, hint,
        [&](const std::shared_ptr<IDataTransporter> &transporter,
            const std::shared_ptr<WorkerRpcClient> &rpcClient) {
            if (context.mode == InlineTransportMode::SHM
                && (transporter != context.shmTransporter || !context.shmSession->IsAlive())) {
                RETURN_STATUS(K_NOT_READY, "QueryAndGet shared-memory session changed before dispatch");
            }
            if (context.mode == InlineTransportMode::UB) {
                context.ubTransporter = transporter;
            }
            invoked = true;
            return rpcClient->InvokeQueryAndGet(request, response, payloads, &rpcDispatched);
        },
        recorder, context.mode == InlineTransportMode::UB);
}

Status ObjectMetadataClient::InvokeTcpQueryAndGet(const HostPort &address, QueryAndGetReqPb &request,
                                                  QueryAndGetRspPb &response, std::vector<RpcMessage> &payloads,
                                                  bool &rpcDispatched)
{
    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(manager_->GetOrCreateRpcClient(address, rpcClient));
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    return rpcClient->InvokeQueryAndGet(request, response, payloads, &rpcDispatched);
}

void ObjectMetadataClient::SwitchInlineRequestToTcp(QueryAndGetReqPb &request, std::vector<RpcMessage> &payloads,
                                                    InlineRequestContext &context) const
{
    context.DisableInlineData();
    context.mode = InlineTransportMode::TCP;
    (void)request.mutable_data_request()->mutable_tcp();
    payloads.clear();
}

Status ObjectMetadataClient::QueryWithRetry(const HostPort &address, const ObjectMetadataBatch &items,
                                            QueryAndGetRspPb &response,
                                            std::vector<RpcMessage> &payloads, InlineRequestContext &context,
                                            TransportPhaseLatencyRecorder *recorder)
{
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(!items.empty(), K_INVALID, "Metadata query items are empty");
    int64_t backoffMs = 1;
    int32_t routeDegradationRetries = 0;
    bool ubReconnectAttempted = false;
    size_t attempt = 0;
    // The context keeps prepared data-plane state reusable across RPC retries.
    while (true) {
        ++attempt;
        RETURN_IF_NOT_OK(retry_->CheckDeadline());
        QueryAndGetReqPb request;
        RETURN_IF_NOT_OK(BuildQueryRequest(address, items, context, request, recorder));
        response.Clear();
        payloads.clear();
        VLOG(1) << "[TransportGet][Metadata] Query, meta owner: " << address.ToString()
                << ", key count: " << items.size() << ", attempt: " << attempt;
        bool rpcDispatched = false;
        Status rc = InvokeQueryAndGet(address, request, response, payloads, context, rpcDispatched, recorder);
        RETURN_OK_IF_TRUE(rc.IsOk());
        RETURN_IF_NOT_OK(
            PrepareQueryRetry(address, items, rc, rpcDispatched, context, backoffMs, routeDegradationRetries,
                              ubReconnectAttempted, recorder));
    }
}

Status ObjectMetadataClient::PrepareQueryRetry(const HostPort &address, const ObjectMetadataBatch &items,
                                               const Status &rc, bool rpcDispatched, InlineRequestContext &context,
                                               int64_t &backoffMs, int32_t &routeDegradationRetries,
                                               bool &ubReconnectAttempted,
                                               TransportPhaseLatencyRecorder *recorder)
{
    // Handle 1006 before buffer quarantine: the Worker precheck returns it before writing inline data.
    if (rc.GetCode() == K_URMA_NEED_CONNECT && rpcDispatched && context.mode == InlineTransportMode::UB
        && !ubReconnectAttempted) {
        ubReconnectAttempted = true;
        const auto staleTransporter = context.ubTransporter;
        const std::string ubInstanceId = context.transportInstanceId;
        context.DisableInlineData();
        RETURN_RUNTIME_ERROR_IF_NULL(staleTransporter);
        Status rebuildRc;
        do {
            rebuildRc = manager_->RebuildStaleUbDataPlane(address, staleTransporter, recorder);
            if (rebuildRc.GetCode() == K_TRY_AGAIN) {
                RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
            }
        } while (rebuildRc.GetCode() == K_TRY_AGAIN);
        RETURN_IF_NOT_OK(rebuildRc);
        context.requireUb = true;
        RETURN_IF_NOT_OK(AllocateUbInlineBuffers(items, context));
        context.mode = InlineTransportMode::UB;
        // Dedicated atomic so each log line reports the cumulative failure volume; log streams are only
        // evaluated on output, so in-stream counting cannot accumulate across calls.
        const auto occurrences = urmaNeedConnectTotal_.fetch_add(1, std::memory_order_relaxed) + 1;
        SLOW_LOG(WARNING) << "[TransportGet][Metadata] Rebuild UB data plane and retry over UB, meta owner: "
            << address.ToString() << ", urma instance: " << ubInstanceId << ", occurrences: " << occurrences
            << ", status: " << rc.ToString();
        return Status::OK();
    }
    if (rc.GetCode() == K_URMA_NEED_CONNECT && rpcDispatched && context.mode == InlineTransportMode::UB
        && ubReconnectAttempted) {
        RETURN_RUNTIME_ERROR_IF_NULL(context.ubTransporter);
        const auto staleTransporter = context.ubTransporter;
        context.DisableInlineData();
        manager_->ResetStaleUbDataPlane(address, staleTransporter, true);
        return rc;
    }
    if ((rc.GetCode() == K_URMA_NEED_CONNECT || rc.GetCode() == K_TRY_AGAIN) && !rpcDispatched
        && context.mode == InlineTransportMode::UB && (context.requireUb || rc.GetCode() == K_TRY_AGAIN)) {
        if (rc.GetCode() == K_URMA_NEED_CONNECT && manager_->IsUbRebuildCoolingDown(address)) {
            return rc;
        }
        VLOG(1) << "[TransportGet][Metadata] Retry UB lease race, meta owner: " << address.ToString()
                << ", status: " << rc.ToString();
        RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
        return Status::OK();
    }
    const bool quarantineUbBuffers =
        rpcDispatched && context.mode == InlineTransportMode::UB && NeedDelayReleaseShmUnit(rc);
    if (quarantineUbBuffers) {
        DelayReleaseUbBuffers(context, rc, "rpc_status");
        context.DisableInlineData();
    }
    const bool routeFailure = IsMetadataOwnerRouteFailure(rc.GetCode());
    // UNAVAILABLE invalidates the channel, not necessarily the owner. Read-only non-SHM queries
    // may still use the bounded owner retry below, but must reconnect rather than reuse that channel.
    const bool teardownWarranted = IsNonRetryableRpcError(rc) || rc.GetCode() == K_RPC_UNAVAILABLE
                                   || (IsRetryableRpcError(rc) && IsBrpcRequestDefinitelyNotSent(rc));
    if (teardownWarranted) {
        manager_->Teardown(address);
    }
    if (routeFailure) {
        return HandleMetadataRouteFailure(address, items, rc, rpcDispatched, quarantineUbBuffers, context,
                                          routeDegradationRetries, recorder);
    }
    if (rpcDispatched && context.mode == InlineTransportMode::SHM) {
        VLOG(1) << "[TransportGet][Metadata] Do not replay an ambiguous SHM QueryAndGet: " << rc.ToString();
        return rc;
    }
    if (!retry_->IsRetryableRpcError(rc)) {
        VLOG(1) << "[TransportGet][Metadata] Query failed without retry, meta owner: " << address.ToString()
                << ", status: " << rc.ToString();
        return rc;
    }
    VLOG(1) << "[TransportGet][Metadata] Retrying query, meta owner: " << address.ToString()
            << ", status: " << rc.ToString();
    RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
    if (quarantineUbBuffers) {
        RETURN_IF_NOT_OK(PrepareUbInlineRequest(address, items, context, recorder));
    }
    return Status::OK();
}

Status ObjectMetadataClient::HandleMetadataRouteFailure(
    const HostPort &address, const ObjectMetadataBatch &items, const Status &rc, bool rpcDispatched,
    bool quarantineUbBuffers, InlineRequestContext &context, int32_t &routeDegradationRetries,
    TransportPhaseLatencyRecorder *recorder)
{
    if (IsAmbiguousMetadataOwnerRouteFailure(rc.GetCode())
        && routeDegradationRetries < ROUTE_DEGRADATION_INNER_RETRIES) {
        ++routeDegradationRetries;
        VLOG(1) << "[TransportGet][Metadata] Retry degraded meta owner in place, meta owner: "
                << address.ToString() << ", inner retry: " << routeDegradationRetries
                << ", status: " << rc.ToString();
        int64_t degradationBackoffMs = ROUTE_DEGRADATION_RETRY_BACKOFF_MS * routeDegradationRetries;
        RETURN_IF_NOT_OK(retry_->Backoff(degradationBackoffMs));
        if (quarantineUbBuffers) {
            RETURN_IF_NOT_OK(PrepareUbInlineRequest(address, items, context, recorder));
        }
        return Status::OK();
    }
    if (metadataFailureHandler_) {
        metadataFailureHandler_(address, rc);
    }
    VLOG(1) << "[TransportGet][Metadata] Return stale route for outer retry, meta owner: "
            << address.ToString() << ", dispatched: " << rpcDispatched << ", status: " << rc.ToString();
    return MakeStaleMetadataRouteStatus(rc);
}

void ObjectMetadataClient::DelayReleaseUbBuffers(InlineRequestContext &context, const Status &reason,
                                                 const std::string &reasonSource) const
{
    for (const auto &[item, buffer] : context.ubBuffers) {
        const std::string objectKey = item == nullptr ? std::string() : item->objectKey;
        ubBufferProvider_->DelayReleaseIfNeeded(buffer, reason, "QueryAndGet", reasonSource + ":" + objectKey);
    }
}

Status ObjectMetadataClient::HandleUbTransportStatus(const HostPort &provider, ObjectMetadataItem &item,
                                                     const QueryAndGetResultPb &result,
                                                     InlineRequestContext &context,
                                                     bool &hasUbTransportError) const
{
    hasUbTransportError = false;
    if (context.mode != InlineTransportMode::UB || !result.has_status()
        || result.status().error_code() == K_OK) {
        return Status::OK();
    }
    hasUbTransportError = true;
    Status handlerStatus = Status::OK();
    if (result.has_provider_ub_failure_detail()) {
        if (ubFailureHandler_) {
            ubFailureHandler_(provider, result.provider_ub_failure_detail());
        } else {
            handlerStatus = Status(K_RUNTIME_ERROR, "Provider UB failure handler is not configured");
        }
    }
    auto buffer = context.ubBuffers.find(&item);
    if (buffer != context.ubBuffers.end()) {
        const Status status(static_cast<StatusCode>(result.status().error_code()), result.status().error_msg());
        ubBufferProvider_->DelayReleaseIfNeeded(buffer->second, status, "QueryAndGet", "response_status");
    }
    return handlerStatus;
}

Status ObjectMetadataClient::BuildQueryRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                               InlineRequestContext &context, QueryAndGetReqPb &request,
                                               TransportPhaseLatencyRecorder *recorder) const
{
    for (const auto *item : items) {
        request.add_object_keys(item->objectKey);
    }
    if (context.mode == InlineTransportMode::SHM) {
        bool sessionAvailable = context.shmSession != nullptr && context.shmSession->IsAlive();
        INJECT_POINT_NO_RETURN("client.transport.query_and_get.shm_session_unavailable_before_build",
                               [&sessionAvailable]() { sessionAvailable = false; });
        if (!sessionAvailable) {
            VLOG(1) << "[TransportGet][Metadata] SHM session is unavailable while building QueryAndGet; "
                       "selecting UB or TCP fallback";
            RETURN_IF_NOT_OK(PrepareShmInlineFallback(address, items, context, recorder));
        }
    }
    return AddInlineDataRequest(items, context, request);
}

Status ObjectMetadataClient::ApplyResults(const HostPort &provider, const ObjectMetadataBatch &items,
                                          const QueryAndGetRspPb &response,
                                          std::vector<RpcMessage> &payloads, InlineRequestContext &context) const
{
    const auto session = context.mode == InlineTransportMode::SHM ? context.shmSession : nullptr;
    if (session != nullptr) {
        RETURN_IF_NOT_OK(session->RegisterReadReferences(response));
    }
    ShmReadResponseGuard<QueryAndGetRspPb> releaseUnread(session, response, context.readContext);
    // Keep the count check because results are accessed positionally below.
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(response.results_size()) == items.size(), K_RUNTIME_ERROR,
                             "QueryAndGet result count does not match requested keys");
    for (size_t i = 0; i < items.size(); ++i) {
        const auto &result = response.results(static_cast<int>(i));
        std::shared_ptr<IReceiveBufferOwner> owner;
        if (session != nullptr && result.has_data_result() && result.data_result().has_shm_info()) {
            RETURN_IF_NOT_OK(session->OwnReadReference(result.data_result().shm_info().shm_id(),
                                                       context.readContext, owner));
        }
        releaseUnread.Consume();
        RETURN_IF_NOT_OK(ApplyResult(provider, *items[i], result, payloads, context, std::move(owner)));
    }
    return Status::OK();
}

Status ObjectMetadataClient::ApplyResult(const HostPort &provider, ObjectMetadataItem &item,
                                         const QueryAndGetResultPb &result, std::vector<RpcMessage> &payloads,
                                         InlineRequestContext &context,
                                         std::shared_ptr<IReceiveBufferOwner> owner) const
{
    const auto &location = result.location();
    CHECK_FAIL_RETURN_STATUS(location.object_key() == item.objectKey, K_RUNTIME_ERROR,
                             "QueryAndGet result key does not match request order");
    bool hasUbTransportError = false;
    RETURN_IF_NOT_OK(HandleUbTransportStatus(provider, item, result, context, hasUbTransportError));
    if (location.object_locations_size() == 0) {
        item.status = Status(K_NOT_FOUND, "Object was not found");
        return Status::OK();
    }
    item.status = Status::OK();
    CopyLocation(location, item.location);
    if (hasUbTransportError) {
        return Status::OK();
    }
    if (!result.has_data_result()) {
        // Absence of data_result is a per-key fast-path miss; the caller will execute phase two.
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(context.mode != InlineTransportMode::NONE, K_RUNTIME_ERROR,
                             "QueryAndGet returned inline data without a requested transport");
    CHECK_FAIL_RETURN_STATUS(location.object_size() <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
                             K_RUNTIME_ERROR, "QueryAndGet object size exceeds the supported range");

    DataGetResult data;
    data.response.mutable_error()->set_error_code(K_OK);
    data.response.set_data_size(static_cast<int64_t>(location.object_size()));
    Status rc;
    if (context.mode == InlineTransportMode::TCP) {
        CHECK_FAIL_RETURN_STATUS(!result.data_result().has_shm_info(), K_RUNTIME_ERROR,
                                 "TCP QueryAndGet returned shared-memory data");
        rc = BuildTcpInlineData(result.data_result(), location.object_size(), payloads, data);
    } else if (context.mode == InlineTransportMode::UB) {
        CHECK_FAIL_RETURN_STATUS(!result.data_result().has_shm_info(), K_RUNTIME_ERROR,
                                 "UB QueryAndGet returned shared-memory data");
        CHECK_FAIL_RETURN_STATUS(result.data_result().payload_indexes_size() == 0, K_RUNTIME_ERROR,
                                 "UB QueryAndGet returned TCP payload indexes");
        rc = BuildUbInlineData(item, item.location, context, data);
    } else {
        CHECK_FAIL_RETURN_STATUS(result.data_result().has_shm_info(), K_RUNTIME_ERROR,
                                 "SHM QueryAndGet did not return shared-memory data");
        CHECK_FAIL_RETURN_STATUS(result.data_result().payload_indexes_size() == 0, K_RUNTIME_ERROR,
                                 "SHM QueryAndGet returned TCP payload indexes");
        rc = BuildShmInlineData(item, result.data_result().shm_info(), context, data, std::move(owner));
    }
    if (rc.IsError() && context.mode == InlineTransportMode::SHM) {
        VLOG(1) << "[ObjectKey " << item.objectKey
                << "] QueryAndGet SHM materialization fallback: " << rc.ToString();
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    item.inlineData.emplace(std::move(data));
    return Status::OK();
}

Status ObjectMetadataClient::BuildTcpInlineData(const QueryAndGetDataResultPb &dataResult, uint64_t objectSize,
                                                std::vector<RpcMessage> &payloads,
                                                DataGetResult &data) const
{
    data.rpcPayloads.reserve(dataResult.payload_indexes_size());
    uint64_t payloadSize = 0;
    for (uint32_t payloadIndex : dataResult.payload_indexes()) {
        CHECK_FAIL_RETURN_STATUS(payloadIndex < payloads.size(), K_RUNTIME_ERROR,
                                 "QueryAndGet payload index is out of range");
        CHECK_FAIL_RETURN_STATUS(
            payloadSize <= objectSize && payloads[payloadIndex].Size() <= objectSize - payloadSize,
            K_RUNTIME_ERROR, "QueryAndGet TCP payload exceeds object size");
        payloadSize += payloads[payloadIndex].Size();
        data.rpcPayloads.emplace_back(std::move(payloads[payloadIndex]));
    }
    CHECK_FAIL_RETURN_STATUS(payloadSize == objectSize, K_RUNTIME_ERROR,
                             "QueryAndGet TCP payload size does not match object size");
    data.response.set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
    data.kind = AccessTransportKind::TCP;
    return Status::OK();
}

Status ObjectMetadataClient::BuildShmInlineData(ObjectMetadataItem &item, const QueryAndGetShmInfoPb &shmInfo,
                                                InlineRequestContext &context, DataGetResult &data,
                                                std::shared_ptr<IReceiveBufferOwner> owner) const
{
    INJECT_POINT("client.transport.query_and_get.shm_materialization_failure");
    CHECK_FAIL_RETURN_STATUS(context.shmSession != nullptr && context.shmTransporter != nullptr
                                 && context.readContext != nullptr,
                             K_RUNTIME_ERROR, "SHM QueryAndGet session context is missing");
    CHECK_FAIL_RETURN_STATUS(shmInfo.data_size() >= 0
                                 && static_cast<uint64_t>(shmInfo.data_size()) == item.location.object_size(),
                             K_RUNTIME_ERROR, "SHM QueryAndGet data size does not match object size");
    DataGetRequest input{ item.objectKey, static_cast<uint64_t>(shmInfo.data_size()), context.readContext };
    return context.shmSession->BuildQueryAndGetResult(shmInfo, input, data, std::move(owner));
}

Status ObjectMetadataClient::BuildUbInlineData(ObjectMetadataItem &item,
                                               const master::ObjectLocationInfoPb &location,
                                               InlineRequestContext &context, DataGetResult &data) const
{
    auto buffer = context.ubBuffers.find(&item);
    CHECK_FAIL_RETURN_STATUS(buffer != context.ubBuffers.end(), K_RUNTIME_ERROR,
                             "UB QueryAndGet result has no receive buffer");
    CHECK_FAIL_RETURN_STATUS(location.object_size() <= buffer->second.size, K_RUNTIME_ERROR,
                             "UB QueryAndGet result exceeds the receive buffer");
    CHECK_FAIL_RETURN_STATUS(buffer->second.owner != nullptr
                                 && (buffer->second.data != nullptr || location.object_size() == 0),
                             K_RUNTIME_ERROR, "UB QueryAndGet receive buffer is invalid");
    data.response.set_data_source(DataTransferSource::DATA_ALREADY_TRANSFERRED);
    data.externalData = buffer->second.data;
    data.externalSize = location.object_size();
    // Move only the owner; object bytes stay in the pre-registered UB receive buffer.
    data.externalOwner = std::move(buffer->second.owner);
    data.kind = AccessTransportKind::UB;
    return Status::OK();
}

Status ObjectMetadataClient::Query(const HostPort &address, const ObjectMetadataBatch &items,
                                   bool enableInlineData, std::shared_ptr<const TransportReadContext> readContext,
                                   bool traceEnabled)
{
    RETURN_IF_NOT_OK(ValidateAndResetItems(items));
    const auto method = enableInlineData ? "QueryAndGet" : "QueryMetadata";
    std::optional<TransportPhaseLatencyRecorder> recorder;
    if (traceEnabled) {
        recorder.emplace(address);
    }
    InlineRequestContext context;
    if (enableInlineData) {
        RETURN_IF_NOT_OK(WithRpcDiag(
            InitializeInlineRequest(address, items, std::move(readContext), context, recorder ? &*recorder : nullptr),
            method, address));
    }

    QueryAndGetRspPb response;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(WithRpcDiag(
        QueryWithRetry(address, items, response, payloads, context, recorder ? &*recorder : nullptr), method, address));
    return WithRpcDiag(ApplyResults(address, items, response, payloads, context), method, address);
}

Status ObjectMetadataClient::QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items,
                                         std::shared_ptr<const TransportReadContext> readContext, bool traceEnabled)
{
    return Query(address, items, true, std::move(readContext), traceEnabled);
}

Status ObjectMetadataClient::QueryMetadata(const HostPort &address, const ObjectMetadataBatch &items)
{
    return Query(address, items, false);
}
}  // namespace client
}  // namespace datasystem
