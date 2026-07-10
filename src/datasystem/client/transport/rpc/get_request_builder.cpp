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

/** Description: Implements common Get request construction and response status handling for data transporters. */

#include "datasystem/client/transport/rpc/get_request_builder.h"

#include <algorithm>
#include <climits>
#include <cstdint>

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/kv_client.h"

namespace datasystem {
namespace client {
namespace {

int64_t AdjustSubTimeout(int64_t timeoutMs)
{
    if (timeoutMs <= 0 || timeoutMs <= TimeoutDuration::SMALL_TIMEOUT_ROUND_THRESHOLD_MS) {
        return timeoutMs;
    }
    constexpr int64_t timeoutMarginMs = 1000;
    constexpr double timeoutFactor = 0.9;
    return std::max<int64_t>(static_cast<int64_t>(timeoutMs * timeoutFactor), timeoutMs - timeoutMarginMs);
}

bool IsAllGetFailed(const GetRspPb &response)
{
    for (const auto &object : response.objects()) {
        if (object.data_size() != -1) {
            return false;
        }
    }
    return true;
}

}  // namespace

Status ValidateGetRequest(const TransportGetRequest &input)
{
    CHECK_FAIL_RETURN_STATUS(Validator::IsInNonNegativeInt32(input.subTimeoutMs), K_INVALID,
                             "Get sub-timeout must fit a non-negative int32");
    CHECK_FAIL_RETURN_STATUS(input.readParams.empty() || input.readParams.size() == input.objectKeys.size(), K_INVALID,
                             "Read parameters must be empty or match the object count");
    return Status::OK();
}

Status BuildGetRequest(const TransportGetRequest &input, const std::vector<size_t> &requestIndices,
                       GetReqPb &request)
{
    RETURN_IF_NOT_OK(ValidateGetRequest(input));

    request.Clear();
    const size_t count = requestIndices.empty() ? input.objectKeys.size() : requestIndices.size();
    CHECK_FAIL_RETURN_STATUS(count <= static_cast<size_t>(INT_MAX), K_INVALID,
                             "Get object count exceeds protobuf repeated-field capacity");
    request.mutable_object_keys()->Reserve(static_cast<int>(count));
    if (!input.readParams.empty()) {
        request.mutable_read_offset_list()->Reserve(static_cast<int>(count));
        request.mutable_read_size_list()->Reserve(static_cast<int>(count));
    }
    auto append = [&](size_t index) -> Status {
        CHECK_FAIL_RETURN_STATUS(index < input.objectKeys.size(), K_INVALID, "Get request index is out of range");
        request.add_object_keys(input.objectKeys[index]);
        if (!input.readParams.empty()) {
            request.add_read_offset_list(input.readParams[index].offset);
            request.add_read_size_list(input.readParams[index].size);
        }
        return Status::OK();
    };
    if (requestIndices.empty()) {
        for (size_t i = 0; i < input.objectKeys.size(); ++i) {
            RETURN_IF_NOT_OK(append(i));
        }
    } else {
        for (size_t index : requestIndices) {
            RETURN_IF_NOT_OK(append(index));
        }
    }

    request.set_no_query_l2cache(!input.queryL2Cache);
    request.set_sub_timeout(AdjustSubTimeout(input.subTimeoutMs));
    request.set_return_object_index(true);
    const int64_t requestTimeoutUs =
        std::max<int64_t>(input.subTimeoutMs * ONE_THOUSAND, ApiDeadline::Instance().ApiRemainingUs());
    request.set_request_timeout(TimeoutDuration::CeilUsToMs(requestTimeoutUs));
    return Status::OK();
}

Status GetTransportResponseStatus(const GetRspPb &response, AccessTransportKind kind)
{
    Status responseStatus(static_cast<StatusCode>(response.last_rc().error_code()), response.last_rc().error_msg());
    if ((kind == AccessTransportKind::UB && responseStatus.GetCode() == K_URMA_NEED_CONNECT)
        || IsRpcTimeoutOrTryAgain(responseStatus)
        || (responseStatus.GetCode() == K_OUT_OF_MEMORY && IsAllGetFailed(response))) {
        return responseStatus;
    }
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
