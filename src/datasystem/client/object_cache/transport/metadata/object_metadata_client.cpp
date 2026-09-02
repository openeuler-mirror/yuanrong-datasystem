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

#include <cstdint>
#include <limits>
#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

namespace {
bool IsMetadataOwnerRouteFailure(StatusCode code)
{
    return code == K_RPC_UNAVAILABLE || code == K_RPC_DEADLINE_EXCEEDED || code == K_RPC_PEER_DEAD
           || code == K_CLIENT_WORKER_DISCONNECT || code == K_METADATA_OWNER_UNAVAILABLE;
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
                                           std::function<void(const HostPort &, const Status &)> metadataFailureHandler)
    : manager_(std::move(manager)),
      retry_(std::move(retry)),
      advisor_(std::move(advisor)),
      ubBufferProvider_(std::move(ubBufferProvider)),
      ubBufferSize_(ubBufferSize),
      metadataFailureHandler_(std::move(metadataFailureHandler))
{
}

Status ObjectMetadataClient::InitializeInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                                     std::shared_ptr<const TransportReadContext> readContext,
                                                     InlineRequestContext &context) const
{
    context = InlineRequestContext{};
    const auto hint = advisor_ == nullptr ? TransportHint::TCP_ONLY : advisor_->GetTransportHint(address);
    if (hint == TransportHint::SHM_CANDIDATE) {
        RETURN_IF_NOT_OK(PrepareShmInlineRequest(address, std::move(readContext), context));
        RETURN_OK_IF_TRUE(context.mode == InlineTransportMode::SHM);
    }
    if (hint == TransportHint::UB_CANDIDATE || IsUrmaEnabled()) {
        return PrepareUbInlineRequest(address, items, context);
    }
    context.mode = InlineTransportMode::TCP;
    return Status::OK();
}

Status ObjectMetadataClient::PrepareShmInlineRequest(const HostPort &address,
                                                     std::shared_ptr<const TransportReadContext> readContext,
                                                     InlineRequestContext &context) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    if (readContext == nullptr) {
        context.DisableInlineData();
        return Status::OK();
    }
    std::shared_ptr<IDataTransporter> transporter;
    Status rc = manager_->GetOrCreate(address, TransportHint::SHM_CANDIDATE, transporter);
    auto shmTransporter = std::dynamic_pointer_cast<ShmTransporter>(transporter);
    if (rc.IsError() || shmTransporter == nullptr) {
        context.DisableInlineData();
        return Status::OK();
    }
    std::shared_ptr<ShmSession> session;
    rc = shmTransporter->AcquireSession(readContext->requestContext, session);
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
                                                      InlineRequestContext &context) const
{
    context.DisableInlineData();
    if (IsUrmaEnabled()) {
        return PrepareUbInlineRequest(address, items, context);
    }
    context.mode = InlineTransportMode::TCP;
    return Status::OK();
}

Status ObjectMetadataClient::PrepareUbInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                                    InlineRequestContext &context) const
{
    if (ubBufferSize_ == 0 || ubBufferProvider_ == nullptr || ubBufferSize_ > ubBufferProvider_->MaxGetSize()
        || items.size() > ubBufferProvider_->MaxGetSize() / ubBufferSize_) {
        context.mode = InlineTransportMode::TCP;
        return Status::OK();
    }

    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    // Establish UB first so a connection miss does not consume receive-buffer capacity.
    std::shared_ptr<IDataTransporter> transporter;
    Status connectionRc = manager_->GetOrCreate(address, TransportHint::UB_CANDIDATE, transporter);
    if (connectionRc.IsError()) {
        VLOG(1) << "[TransportGet][Metadata] Disable UB inline data because the connection is unavailable: "
                << connectionRc.ToString();
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
                                               InlineRequestContext &context, bool &rpcDispatched)
{
    rpcDispatched = false;
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    if (context.mode == InlineTransportMode::UB || context.mode == InlineTransportMode::SHM) {
        bool invoked = false;
        Status leaseRc = InvokeInlineQueryAndGet(address, request, response, payloads, context, invoked, rpcDispatched);
        if (invoked) {
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
                                                     bool &rpcDispatched)
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
            invoked = true;
            return rpcClient->InvokeQueryAndGet(request, response, payloads, &rpcDispatched);
        });
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
                                            std::vector<RpcMessage> &payloads, InlineRequestContext &context)
{
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(!items.empty(), K_INVALID, "Metadata query items are empty");
    int64_t backoffMs = 1;
    size_t attempt = 0;
    // The context keeps prepared data-plane state reusable across RPC retries.
    while (true) {
        ++attempt;
        RETURN_IF_NOT_OK(retry_->CheckDeadline());
        QueryAndGetReqPb request;
        RETURN_IF_NOT_OK(BuildQueryRequest(address, items, context, request));
        response.Clear();
        payloads.clear();
        VLOG(1) << "[TransportGet][Metadata] Query, meta owner: " << address.ToString()
                << ", key count: " << items.size() << ", attempt: " << attempt;
        bool rpcDispatched = false;
        Status rc = InvokeQueryAndGet(address, request, response, payloads, context, rpcDispatched);
        RETURN_OK_IF_TRUE(rc.IsOk());
        RETURN_IF_NOT_OK(PrepareQueryRetry(address, items, rc, rpcDispatched, context, backoffMs));
    }
}

Status ObjectMetadataClient::PrepareQueryRetry(const HostPort &address, const ObjectMetadataBatch &items,
                                               const Status &rc, bool rpcDispatched, InlineRequestContext &context,
                                               int64_t &backoffMs)
{
    const bool quarantineUbBuffers =
        rpcDispatched && context.mode == InlineTransportMode::UB && NeedDelayReleaseShmUnit(rc);
    if (quarantineUbBuffers) {
        DelayReleaseUbBuffers(context, rc, "rpc_status");
        context.DisableInlineData();
    }
    const bool routeFailure = IsMetadataOwnerRouteFailure(rc.GetCode());
    if (routeFailure && metadataFailureHandler_) {
        metadataFailureHandler_(address, rc);
    }
    const bool teardownWarranted = IsNonRetryableRpcError(rc) || rc.GetCode() == K_RPC_UNAVAILABLE
                                   || (IsRetryableRpcError(rc) && IsBrpcRequestDefinitelyNotSent(rc));
    if (teardownWarranted) {
        manager_->Teardown(address);
    }
    if (routeFailure) {
        VLOG(1) << "[TransportGet][Metadata] Return stale route for outer retry, meta owner: "
                << address.ToString() << ", dispatched: " << rpcDispatched << ", status: " << rc.ToString();
        return Status(K_NOT_READY, STALE_TRANSPORT_SNAPSHOT_MESSAGE);
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
        RETURN_IF_NOT_OK(PrepareUbInlineRequest(address, items, context));
    }
    return Status::OK();
}

void ObjectMetadataClient::DelayReleaseUbBuffers(InlineRequestContext &context, const Status &reason,
                                                 const std::string &reasonSource) const
{
    for (const auto &[item, buffer] : context.ubBuffers) {
        const std::string objectKey = item == nullptr ? std::string() : item->objectKey;
        ubBufferProvider_->DelayReleaseIfNeeded(buffer, reason, "QueryAndGet", reasonSource + ":" + objectKey);
    }
}

bool ObjectMetadataClient::HandleUbTransportStatus(ObjectMetadataItem &item, const QueryAndGetResultPb &result,
                                                   InlineRequestContext &context) const
{
    if (context.mode != InlineTransportMode::UB || !result.has_status()
        || result.status().error_code() == K_OK) {
        return false;
    }
    auto buffer = context.ubBuffers.find(&item);
    if (buffer != context.ubBuffers.end()) {
        const Status status(static_cast<StatusCode>(result.status().error_code()), result.status().error_msg());
        ubBufferProvider_->DelayReleaseIfNeeded(buffer->second, status, "QueryAndGet", "response_status");
    }
    return true;
}

Status ObjectMetadataClient::BuildQueryRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                               InlineRequestContext &context, QueryAndGetReqPb &request) const
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
            RETURN_IF_NOT_OK(PrepareShmInlineFallback(address, items, context));
        }
    }
    return AddInlineDataRequest(items, context, request);
}

Status ObjectMetadataClient::ApplyResults(const ObjectMetadataBatch &items,
                                          const QueryAndGetRspPb &response,
                                          std::vector<RpcMessage> &payloads, InlineRequestContext &context) const
{
    // Keep the count check because results are accessed positionally below.
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(response.results_size()) == items.size(), K_RUNTIME_ERROR,
                             "QueryAndGet result count does not match requested keys");
    for (size_t i = 0; i < items.size(); ++i) {
        RETURN_IF_NOT_OK(ApplyResult(*items[i], response.results(static_cast<int>(i)), payloads, context));
    }
    return Status::OK();
}

Status ObjectMetadataClient::ApplyResult(ObjectMetadataItem &item, const QueryAndGetResultPb &result,
                                         std::vector<RpcMessage> &payloads, InlineRequestContext &context) const
{
    const auto &location = result.location();
    CHECK_FAIL_RETURN_STATUS(location.object_key() == item.objectKey, K_RUNTIME_ERROR,
                             "QueryAndGet result key does not match request order");
    const bool hasUbTransportError = HandleUbTransportStatus(item, result, context);
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
        rc = BuildShmInlineData(item, result.data_result().shm_info(), context, data);
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
                                                InlineRequestContext &context, DataGetResult &data) const
{
    INJECT_POINT("client.transport.query_and_get.shm_materialization_failure");
    CHECK_FAIL_RETURN_STATUS(context.shmSession != nullptr && context.shmTransporter != nullptr
                                 && context.readContext != nullptr,
                             K_RUNTIME_ERROR, "SHM QueryAndGet session context is missing");
    CHECK_FAIL_RETURN_STATUS(shmInfo.data_size() >= 0
                                 && static_cast<uint64_t>(shmInfo.data_size()) == item.location.object_size(),
                             K_RUNTIME_ERROR, "SHM QueryAndGet data size does not match object size");
    DataGetRequest input{ item.objectKey, static_cast<uint64_t>(shmInfo.data_size()), context.readContext };
    return context.shmSession->BuildQueryAndGetResult(shmInfo, input, data);
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
                                   bool enableInlineData, std::shared_ptr<const TransportReadContext> readContext)
{
    RETURN_IF_NOT_OK(ValidateAndResetItems(items));
    InlineRequestContext context;
    if (enableInlineData) {
        RETURN_IF_NOT_OK(InitializeInlineRequest(address, items, std::move(readContext), context));
    }

    QueryAndGetRspPb response;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(QueryWithRetry(address, items, response, payloads, context));
    return ApplyResults(items, response, payloads, context);
}

Status ObjectMetadataClient::QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items,
                                         std::shared_ptr<const TransportReadContext> readContext)
{
    return Query(address, items, true, std::move(readContext));
}

Status ObjectMetadataClient::QueryMetadata(const HostPort &address, const ObjectMetadataBatch &items)
{
    return Query(address, items, false);
}
}  // namespace client
}  // namespace datasystem
