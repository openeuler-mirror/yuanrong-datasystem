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
#include "datasystem/client/transport/rpc/set_request_builder.h"
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
    RETURN_IF_NOT_OK(responseStatus);
    CHECK_FAIL_RETURN_STATUS(output.response.data_source() == DataTransferSource::DATA_IN_PAYLOAD, K_RUNTIME_ERROR,
                             "TCP GetObjectRemote returned an invalid data source");
    output.kind = AccessTransportKind::TCP;
    return Status::OK();
}

Status TcpTransporter::Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                              const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);
    RETURN_IF_NOT_OK(ValidateCreateRequest(key, size, param));

    auto info = std::make_shared<ObjectBufferInfo>();
    info->objectKey = key;
    info->dataSize = size;
    info->metadataSize = 0;
    info->workerAddr = workerAddr;
    info->objectMode = ModeInfo(param.consistencyType, WriteMode::NONE_L2_CACHE, param.cacheType);
    info->ubDataSentByMemoryCopy = false;
    // ObjectBuffer owns and validates the plain TCP allocation.
    info->pointer = nullptr;

    return ObjectBufferInternal::Create(info, buffer);
}

Status TcpTransporter::Set(ObjectBuffer &buffer, const TransportSetParam &param)
{
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);

    const ObjectBufferInfo &info = ObjectBufferInternal::GetInfo(buffer);
    RETURN_IF_NOT_OK(ValidateSetRequest(info, param));

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

}  // namespace client
}  // namespace datasystem
