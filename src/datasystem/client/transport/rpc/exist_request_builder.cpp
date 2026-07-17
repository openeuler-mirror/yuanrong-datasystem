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

/** Description: Implements Exist request construction for data transport. */

#include "datasystem/client/transport/rpc/exist_request_builder.h"

#include <climits>
#include <cstddef>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
namespace client {

Status ValidateExistRequest(const TransportExistRequest &input)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsInNonNegativeInt32(input.subTimeoutMs) && input.subTimeoutMs > 0, K_INVALID,
                             "Exist sub-timeout must be a positive int32");
    CHECK_FAIL_RETURN_STATUS(!input.objectKeys.empty(), K_INVALID, "Exist request must contain at least one key");
    CHECK_FAIL_RETURN_STATUS(!(input.queryL2Cache && input.isLocal), K_INVALID,
                             "queryL2Cache and isLocal are mutually exclusive");
    for (const auto &key : input.objectKeys) {
        CHECK_FAIL_RETURN_STATUS(!key.empty(), K_INVALID, "Exist request contains an empty key");
    }
    return Status::OK();
}

Status BuildExistRequest(const TransportExistRequest &input, ExistReqPb &request)
{
    RETURN_IF_NOT_OK(ValidateExistRequest(input));

    request.Clear();
    const size_t count = input.objectKeys.size();
    CHECK_FAIL_RETURN_STATUS(count <= static_cast<size_t>(INT_MAX), K_INVALID,
                             "Exist object count exceeds protobuf repeated-field capacity");
    request.mutable_object_keys()->Reserve(static_cast<int>(count));
    for (const auto &key : input.objectKeys) {
        request.add_object_keys(key);
    }
    request.set_query_l2cache(input.queryL2Cache);
    request.set_is_local(input.isLocal);
    request.set_client_id(input.clientId);
    request.set_tenant_id(input.tenantId);
    request.set_is_routed(true);
    if (!input.token.Empty()) {
        request.set_token(input.token.GetData(), input.token.GetSize());
    }
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
