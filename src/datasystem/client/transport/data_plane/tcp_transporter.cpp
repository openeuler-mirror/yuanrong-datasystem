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

/** Description: Implements the TCP object Get transporter. */

#include "datasystem/client/transport/data_plane/tcp_transporter.h"

#include <numeric>
#include <utility>

#include "datasystem/client/transport/rpc/get_request_builder.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

Status TcpTransporter::Get(const TransportGetRequest &input, TransportGetResult &output)
{
    output.Clear();
    RETURN_RUNTIME_ERROR_IF_NULL(rpcClient_);

    TransportGetBatchResult batch;
    batch.requestIndices.resize(input.objectKeys.size());
    std::iota(batch.requestIndices.begin(), batch.requestIndices.end(), 0);

    GetReqPb request;
    RETURN_IF_NOT_OK(BuildGetRequest(input, {}, request));
    Status rc = rpcClient_->InvokeGet(input.subTimeoutMs, request, batch.response, batch.rpcPayloads,
                                      batch.workerVersion);
    RETURN_IF_NOT_OK(rc);
    RETURN_IF_NOT_OK(GetTransportResponseStatus(batch.response, AccessTransportKind::TCP));

    batch.kind = AccessTransportKind::TCP;
    output.batches.emplace_back(std::move(batch));
    output.actualKind = AccessTransportKind::TCP;
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
