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

/** Description: Implements the TCP object data transporter. */

#include "datasystem/client/transport/data_plane/tcp_transporter.h"

#include <numeric>
#include <utility>

#include "datasystem/client/transport/object_buffer_internal.h"
#include "datasystem/client/transport/rpc/mset_request_builder.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_buffer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

Status TcpTransporter::Get(const DataGetRequest &input, DataGetResult &output)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
    output = DataGetResult{};
    GetObjectRemoteReqPb request;
    request.set_object_key(input.objectKey);
    request.set_data_size(input.expectedSize);
    request.set_try_lock(true);
    RETURN_IF_NOT_OK(rpcClient_->InvokeGetObject(request, output.response, output.rpcPayloads));
    Status responseStatus(static_cast<StatusCode>(output.response.error().error_code()),
                          output.response.error().error_msg());
    if (responseStatus.IsError()) {
        output.rpcPayloads.clear();
        return responseStatus;
    }
    CHECK_FAIL_RETURN_STATUS(output.response.data_source() == DataTransferSource::DATA_IN_PAYLOAD, K_RUNTIME_ERROR,
                             "TCP GetObjectRemote returned an invalid data source");
    output.kind = AccessTransportKind::TCP;
    return Status::OK();
}

Status TcpTransporter::BatchGet(const DataGetBatchRequest &inputs, DataGetBatchResult &outputs)
{
    outputs.clear();
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    CHECK_FAIL_RETURN_STATUS(!inputs.empty(), K_INVALID, "Batch get request is empty");
    if (inputs.size() == 1) {
        DataGetItemResult item;
        Status status = Get(inputs.front(), item.data);
        if (status.IsError()
            && static_cast<StatusCode>(item.data.response.error().error_code()) == K_OK) {
            return status;
        }
        item.status = status;
        outputs.emplace_back(std::move(item));
        return Status::OK();
    }

    BatchGetObjectRemoteReqPb request;
    for (const auto &input : inputs) {
        CHECK_FAIL_RETURN_STATUS(!input.objectKey.empty(), K_INVALID, "Object key is empty");
        auto *itemRequest = request.add_requests();
        itemRequest->set_object_key(input.objectKey);
        itemRequest->set_data_size(input.expectedSize);
        itemRequest->set_try_lock(true);
    }

    BatchGetObjectRemoteRspPb response;
    std::vector<RpcMessage> payloads;
    METRIC_INC(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_RPC_TOTAL);
    METRIC_ADD(metrics::KvMetricId::CLIENT_DIRECT_BATCH_GET_OBJECT_TOTAL, inputs.size());
    RETURN_IF_NOT_OK(rpcClient_->InvokeBatchGetObject(request, response, payloads));
    CHECK_FAIL_RETURN_STATUS(response.responses_size() == static_cast<int>(inputs.size()), K_RUNTIME_ERROR,
                             "BatchGetObjectRemote response count does not match request count");

    size_t expectedPayloadCount = 0;
    for (const auto &itemResponse : response.responses()) {
        Status itemStatus(static_cast<StatusCode>(itemResponse.error().error_code()),
                          itemResponse.error().error_msg());
        if (!itemStatus.IsOk()) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(itemResponse.data_source() == DataTransferSource::DATA_IN_PAYLOAD, K_RUNTIME_ERROR,
                                 "TCP BatchGetObjectRemote returned an invalid data source");
        ++expectedPayloadCount;
    }
    CHECK_FAIL_RETURN_STATUS(payloads.size() == expectedPayloadCount, K_RUNTIME_ERROR,
                             "BatchGetObjectRemote payload count does not match successful responses");

    outputs.reserve(inputs.size());
    size_t payloadIndex = 0;
    for (const auto &itemResponse : response.responses()) {
        DataGetItemResult item;
        item.status = Status(static_cast<StatusCode>(itemResponse.error().error_code()),
                             itemResponse.error().error_msg());
        item.data.response = itemResponse;
        item.data.kind = AccessTransportKind::TCP;
        if (item.status.IsOk()) {
            item.data.rpcPayloads.emplace_back(std::move(payloads[payloadIndex++]));
        }
        outputs.emplace_back(std::move(item));
    }
    return Status::OK();
}

Status TcpTransporter::Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                              const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    RETURN_IF_NOT_OK(ValidateCreateRequest(key, size, param));
    return CreateBuffer(workerAddr, key, size, param, buffer);
}

Status TcpTransporter::CreateBuffer(const HostPort &workerAddr, const std::string &key, uint64_t size,
                                    const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer)
{
    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = key;
    info->dataSize = size;
    info->metadataSize = 0;
    info->workerAddr = workerAddr;
    info->objectMode = ModeInfo(param.consistencyType, param.writeMode, param.cacheType);
    info->ubDataSentByMemoryCopy = false;
    // ObjectBuffer owns and validates the plain TCP allocation.
    info->pointer = nullptr;

    return ObjectBufferInternal::Create(info, buffer);
}

Status TcpTransporter::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);

    const ObjectBufferInfo &info = ObjectBufferInternal::GetInfo(buffer);
    PublishReqPb pubReq;
    RETURN_IF_NOT_OK(BuildSetRequest(info, param, pubReq));

    // TCP payload: send the data inline through the RPC
    MemView payload(info.pointer + info.metadataSize, info.dataSize);
    std::vector<MemView> payloads{ payload };

    PublishRspPb rsp;
    uint32_t workerVersion = 0;
    RETURN_IF_NOT_OK(rpcClient_->InvokeSet(param.subTimeoutMs, pubReq, payloads, rsp, workerVersion));
    return SetTransportResponseStatus(rsp, AccessTransportKind::TCP, param.isSeal, param.isRetry);
}

Status TcpTransporter::MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                               const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                               std::vector<std::shared_ptr<ObjectBuffer>> &buffers)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    RETURN_IF_NOT_OK(ValidateMultiCreateRequest(keys, sizes, param));
    std::vector<std::shared_ptr<ObjectBuffer>> created;
    created.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        std::shared_ptr<ObjectBuffer> buffer;
        RETURN_IF_NOT_OK(CreateBuffer(workerAddr, keys[i], sizes[i], param, buffer));
        created.emplace_back(std::move(buffer));
    }
    buffers = std::move(created);
    return Status::OK();
}

Status TcpTransporter::MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                            const TransportSetParam &param, TransportMSetResult &result)
{
    result.Clear();
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    std::vector<bool> tcpPayload(buffers.size(), true);
    MultiPublishReqPb request;
    std::vector<MemView> payloads;
    RETURN_IF_NOT_OK(BuildMultiPublishRequest(buffers, tcpPayload, param, request, payloads));
    MultiPublishRspPb response;
    uint32_t workerVersion = 0;
    if (!rpcClient_->IsAlive()) {
        return Status(K_RPC_UNAVAILABLE, "TCP MSet: RPC client not alive before publish");
    }
    result.publishAttempted = true;
    RETURN_IF_NOT_OK(rpcClient_->InvokeMultiSet(param.subTimeoutMs, request, payloads, response, workerVersion));
    const uint64_t payloadBytes = std::accumulate(
        buffers.begin(), buffers.end(), uint64_t{ 0 }, [](uint64_t total, const auto &buffer) {
            return total + ObjectBufferInternal::GetInfo(*buffer).dataSize;
        });
    METRIC_ADD(metrics::KvMetricId::CLIENT_PUT_TCP_WRITE_TOTAL_BYTES, payloadBytes);
    Status rc = SetMSetResponseResult(response, buffers.size(), AccessTransportKind::TCP, result);
    result.publishAttempted = true;
    return rc;
}

Status TcpTransporter::Release(const ShmKey &shmId, const TransportRequestContext &context)
{
    (void)shmId;
    (void)context;
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
