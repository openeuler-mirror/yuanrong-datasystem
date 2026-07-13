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

#include "datasystem/common/util/status_helper.h"

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

}  // namespace client
}  // namespace datasystem
