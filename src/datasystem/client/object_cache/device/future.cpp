
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include "datasystem/hetero_cache/future.h"
#include "datasystem/common/device/ascend/acl_pointer_wrapper.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {

// Max wait timeout is 30 minutes.
constexpr uint64_t MAX_FUTURE_WAIT_TIMEOUT_MS = 1'800'000;

Future::Future(std::shared_future<Status> future, std::shared_ptr<AclRtEventWrapper> event,
               const std::string &objectKey)
    : future_(future), event_(std::move(event)), objectKey_(objectKey)
{
}

Status Future::Get(uint64_t subTimeoutMs)
{
    auto futureName = FormatString("future with objectKey: %s", objectKey_);
    CHECK_FAIL_RETURN_STATUS(subTimeoutMs <= MAX_FUTURE_WAIT_TIMEOUT_MS, K_INVALID,
                             "The subTimeoutMs should be in range of [0, 1'800'000].");
    if (!future_.valid()) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("The %s is invalid.", futureName));
    }
    TimeoutDuration timeoutDuration(subTimeoutMs);
    // Waiting for aclrtRecordEvent to be called
    if (future_.wait_for(std::chrono::milliseconds(subTimeoutMs)) == std::future_status::timeout) {
        RETURN_STATUS_LOG_ERROR(
            K_FUTURE_TIMEOUT,
            FormatString("The %s get is timeout, still waiting for promise setting value, please try again.",
                         futureName));
    }

    // future_.get() is non-blocked when std::future_status is ready.
    try {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(future_.get(), FormatString("The %s get error.", futureName));
    } catch (const std::exception &e) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Exception when %s get: %s", futureName, e.what()));
    }
    // When the event processing is complete but the status needs to be obtained
    if (event_ == nullptr) {
        return Status::OK();
    }

    auto remainingTime = timeoutDuration.CalcRealRemainingTime();
    // Wait with timeout if remainingTime > 0
    if (remainingTime > 0) {
        return event_->SynchronizeEvent(remainingTime);
    }
    if (event_->QueryEventStatus().IsError()) {
        RETURN_STATUS_LOG_ERROR(
            K_FUTURE_TIMEOUT,
            FormatString("The %s get is timeout, npu event is not sync ok, please try again.", futureName));
    }
    return Status::OK();
}
}  // namespace datasystem