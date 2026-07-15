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

/** Description: Implements batched object metadata access and one-layer redirect handling. */

#include "datasystem/client/transport/metadata/object_metadata_client.h"

#include <cstdint>
#include <deque>
#include <limits>
#include <unordered_map>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {

namespace {
struct RedirectBatch {
    HostPort address;
    ObjectMetadataBatch items;
};

using RedirectTargets = std::unordered_map<std::string, std::deque<HostPort>>;

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

Status BuildRedirectTargets(const master::QueryAndGetRspPb &response, RedirectTargets &redirectTargets)
{
    for (const auto &redirectInfo : response.info()) {
        HostPort address;
        RETURN_IF_NOT_OK(address.ParseString(redirectInfo.redirect_meta_address()));
        for (const auto &objectKey : redirectInfo.change_meta_ids()) {
            redirectTargets[objectKey].emplace_back(address);
        }
    }
    return Status::OK();
}

Status PartitionInitialResponse(const ObjectMetadataBatch &items, const master::QueryAndGetRspPb &response,
                                ObjectMetadataBatch &localItems,
                                std::vector<RedirectBatch> &redirectBatches)
{
    RedirectTargets redirectTargets;
    RETURN_IF_NOT_OK(BuildRedirectTargets(response, redirectTargets));

    localItems.clear();
    localItems.reserve(items.size());
    std::unordered_map<HostPort, RedirectBatch *> batchesByAddress;
    // The address map keeps pointers into redirectBatches, so prevent vector relocation while grouping.
    redirectBatches.reserve(response.info_size());
    batchesByAddress.reserve(response.info_size());
    for (auto *item : items) {
        auto target = redirectTargets.find(item->objectKey);
        if (target == redirectTargets.end() || target->second.empty()) {
            localItems.push_back(item);
            continue;
        }
        const HostPort address = std::move(target->second.front());
        target->second.pop_front();
        auto inserted = batchesByAddress.emplace(address, nullptr);
        if (inserted.second) {
            redirectBatches.push_back({ address, {} });
            inserted.first->second = &redirectBatches.back();
        }
        inserted.first->second->items.push_back(item);
    }
    return Status::OK();
}

void SetBatchError(const ObjectMetadataBatch &items, const Status &status)
{
    for (auto *item : items) {
        item->status = status;
    }
}
}  // namespace

ObjectMetadataClient::ObjectMetadataClient(std::shared_ptr<DataPlaneManager> manager,
                                           std::shared_ptr<DeadlineRetry> retry,
                                           std::shared_ptr<TransportAdvisor> advisor,
                                           std::shared_ptr<IUbReceiveBufferProvider> ubBufferProvider,
                                           uint64_t ubBufferSize)
    : manager_(std::move(manager)), retry_(std::move(retry)), advisor_(std::move(advisor)),
      ubBufferProvider_(std::move(ubBufferProvider)), ubBufferSize_(ubBufferSize)
{
}

Status ObjectMetadataClient::InitializeInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                                     InlineRequestContext &context) const
{
    context = InlineRequestContext{};
    // One batch uses one exclusive inline mode; phase one enables it only for a single key.
    if (items.size() != 1 || advisor_ == nullptr) {
        return Status::OK();
    }
    if (advisor_->GetTransportHint(address) == TransportHint::TCP_ONLY) {
        context.mode = InlineTransportMode::TCP;
        return Status::OK();
    }
    return PrepareUbInlineRequest(address, items, context);
}

Status ObjectMetadataClient::PrepareUbInlineRequest(const HostPort &address, const ObjectMetadataBatch &items,
                                                    InlineRequestContext &context) const
{
    if (ubBufferSize_ == 0 || ubBufferProvider_ == nullptr || ubBufferSize_ > ubBufferProvider_->MaxGetSize()) {
        return Status::OK();
    }

    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    // Establish UB first so a connection miss does not consume receive-buffer capacity.
    std::shared_ptr<IDataTransporter> transporter;
    Status connectionRc = manager_->GetOrCreate(address, TransportHint::UB_CANDIDATE, transporter);
    if (connectionRc.IsError()) {
        VLOG(1) << "[TransportGet][Metadata] Disable UB inline data because the connection is unavailable: "
                << connectionRc.ToString();
        return Status::OK();
    }

    if (AllocateUbInlineBuffers(items, context).IsError()) {
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
                                                  master::QueryAndGetReqPb &request) const
{
    if (context.mode == InlineTransportMode::NONE) {
        return Status::OK();
    }
    if (context.mode == InlineTransportMode::TCP) {
        (void)request.mutable_data_request()->mutable_tcp();
        return Status::OK();
    }

    auto *ubRequest = request.mutable_data_request()->mutable_ub();
    ubRequest->set_buffer_size(ubBufferSize_);
    ubRequest->set_urma_instance_id(context.transportInstanceId);
    // Buffer descriptors follow object_keys order, including redirected sub-batches.
    for (auto *item : items) {
        auto buffer = context.ubBuffers.find(item);
        CHECK_FAIL_RETURN_STATUS(buffer != context.ubBuffers.end(), K_RUNTIME_ERROR,
                                 "UB inline receive buffer does not match requested keys");
        *ubRequest->add_buffer_infos() = buffer->second.remoteAddr;
    }
    return Status::OK();
}

Status ObjectMetadataClient::InvokeQueryAndGet(const HostPort &address, master::QueryAndGetReqPb &request,
                                               master::QueryAndGetRspPb &response,
                                               std::vector<RpcMessage> &payloads,
                                               InlineRequestContext &context)
{
    RETURN_RUNTIME_ERROR_IF_NULL(manager_);
    if (context.mode == InlineTransportMode::UB) {
        bool invoked = false;
        Status leaseRc = manager_->WithDataPlaneLease(
            address, TransportHint::UB_CANDIDATE,
            [&](const std::shared_ptr<IDataTransporter> &, const std::shared_ptr<WorkerRpcClient> &rpcClient) {
                invoked = true;
                return rpcClient->InvokeQueryAndGet(request, response, payloads);
            });
        if (invoked) {
            // Once sent, RPC failures retain UB mode and follow the normal retry policy.
            return leaseRc;
        }
        VLOG(1) << "[TransportGet][Metadata] UB inline connection is unavailable for " << address.ToString()
                << ", query metadata only: " << leaseRc.ToString();
        // The RPC was not sent, so permanently downgrade this request chain to metadata-only mode.
        context.DisableInlineData();
        request.clear_data_request();
        payloads.clear();
    }

    std::shared_ptr<WorkerRpcClient> rpcClient;
    RETURN_IF_NOT_OK(manager_->GetOrCreateRpcClient(address, rpcClient));
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient);
    return rpcClient->InvokeQueryAndGet(request, response, payloads);
}

Status ObjectMetadataClient::QueryWithRetry(const HostPort &address, const ObjectMetadataBatch &items,
                                            bool allowRedirect, master::QueryAndGetRspPb &response,
                                            std::vector<RpcMessage> &payloads, InlineRequestContext &context)
{
    RETURN_RUNTIME_ERROR_IF_NULL(retry_);
    CHECK_FAIL_RETURN_STATUS(!items.empty(), K_INVALID, "Metadata query items are empty");
    int64_t backoffMs = 1;
    size_t attempt = 0;
    // The context keeps prepared UB buffers reusable across RPC retries and one redirect layer.
    while (true) {
        ++attempt;
        RETURN_IF_NOT_OK(retry_->CheckDeadline());
        master::QueryAndGetReqPb request;
        for (const auto *item : items) {
            request.add_object_keys(item->objectKey);
        }
        request.set_redirect(allowRedirect);
        RETURN_IF_NOT_OK(AddInlineDataRequest(items, context, request));
        response.Clear();
        payloads.clear();
        VLOG(1) << "[TransportGet][Metadata] Query, meta owner: " << address.ToString()
                << ", key count: " << items.size() << ", redirect: " << allowRedirect
                << ", attempt: " << attempt;
        Status rc = InvokeQueryAndGet(address, request, response, payloads, context);
        if (rc.IsError()) {
            if (!retry_->IsRetryableRpcError(rc)) {
                VLOG(1) << "[TransportGet][Metadata] Query failed without retry, meta owner: "
                        << address.ToString() << ", status: " << rc.ToString();
                return rc;
            }
            VLOG(1) << "[TransportGet][Metadata] Retrying query, meta owner: " << address.ToString()
                    << ", status: " << rc.ToString();
            if (rc.GetCode() == K_RPC_UNAVAILABLE && manager_ != nullptr) {
                manager_->Teardown(address);
            }
            RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
            continue;
        }
        if (!response.meta_is_moving()) {
            return Status::OK();
        }
        VLOG(1) << "[TransportGet][Metadata] Metadata is moving, meta owner: " << address.ToString()
                << ", key count: " << items.size();
        RETURN_IF_NOT_OK(retry_->Backoff(backoffMs));
    }
}

Status ObjectMetadataClient::ApplyResults(const ObjectMetadataBatch &items,
                                          const master::QueryAndGetRspPb &response,
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

Status ObjectMetadataClient::ApplyResult(ObjectMetadataItem &item, const master::QueryAndGetResultPb &result,
                                         std::vector<RpcMessage> &payloads, InlineRequestContext &context) const
{
    const auto &location = result.location();
    if (location.object_locations_size() == 0) {
        item.status = Status(K_NOT_FOUND, "Object was not found");
        return Status::OK();
    }
    item.status = Status::OK();
    item.location = location;
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
    Status rc = context.mode == InlineTransportMode::TCP
                    ? BuildTcpInlineData(result.data_result(), payloads, data)
                    : BuildUbInlineData(item, location, context, data);
    RETURN_IF_NOT_OK(rc);
    item.inlineData.emplace(std::move(data));
    return Status::OK();
}

Status ObjectMetadataClient::BuildTcpInlineData(const master::QueryAndGetDataResultPb &dataResult,
                                                std::vector<RpcMessage> &payloads,
                                                DataGetResult &data) const
{
    data.rpcPayloads.reserve(dataResult.payload_indexes_size());
    for (uint32_t payloadIndex : dataResult.payload_indexes()) {
        // Keep only the check required before indexing RPC payload storage.
        CHECK_FAIL_RETURN_STATUS(payloadIndex < payloads.size(), K_RUNTIME_ERROR,
                                 "QueryAndGet payload index is out of range");
        data.rpcPayloads.emplace_back(std::move(payloads[payloadIndex]));
    }
    data.response.set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
    data.kind = AccessTransportKind::TCP;
    return Status::OK();
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

Status ObjectMetadataClient::ResolveRedirectBatch(const HostPort &address, const ObjectMetadataBatch &items,
                                                  InlineRequestContext &context)
{
    master::QueryAndGetRspPb response;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(QueryWithRetry(address, items, false, response, payloads, context));
    return ApplyResults(items, response, payloads, context);
}

Status ObjectMetadataClient::QueryAndGet(const HostPort &address, const ObjectMetadataBatch &items)
{
    RETURN_IF_NOT_OK(ValidateAndResetItems(items));
    InlineRequestContext context;
    RETURN_IF_NOT_OK(InitializeInlineRequest(address, items, context));

    master::QueryAndGetRspPb response;
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(QueryWithRetry(address, items, true, response, payloads, context));

    ObjectMetadataBatch localItems;
    std::vector<RedirectBatch> redirectBatches;
    RETURN_IF_NOT_OK(PartitionInitialResponse(items, response, localItems, redirectBatches));
    VLOG(1) << "[TransportGet][Metadata] Query resolved, meta owner: " << address.ToString()
            << ", local keys: " << localItems.size() << ", redirect groups: " << redirectBatches.size();
    RETURN_IF_NOT_OK(ApplyResults(localItems, response, payloads, context));

    for (const auto &batch : redirectBatches) {
        VLOG(1) << "[TransportGet][Metadata] Follow redirect, target: " << batch.address.ToString()
                << ", key count: " << batch.items.size();
        Status rc = ResolveRedirectBatch(batch.address, batch.items, context);
        if (rc.IsError()) {
            VLOG(1) << "[TransportGet][Metadata] Redirect query failed, target: " << batch.address.ToString()
                    << ", key count: " << batch.items.size() << ", status: " << rc.ToString();
            SetBatchError(batch.items, rc);
        }
    }
    return Status::OK();
}
}  // namespace client
}  // namespace datasystem
