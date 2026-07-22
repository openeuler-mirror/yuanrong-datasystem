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

/**
 * Description: Implementations of InitTimeoutsFromDispatch and GetRemainingUsForMeta.
 * Split from api_deadline.cpp to avoid a Bazel dependency cycle:
 *   common_util_impl -> api_deadline -> thread_local(=common_util_impl alias)
 * These primitives are only needed by worker / client call sites.
 */
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {

// Rate-limit the "ApiDeadline uninitialized" fallback log for request-receive threads.
constexpr uint32_t K_API_DEADLINE_FALLBACK_LOG_EVERY_N = 100;

Status InitTimeoutsFromDispatch(int64_t capturedRemainingUs, std::chrono::steady_clock::time_point dispatchTime)
{
    auto queueDelayUs = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - dispatchTime)
                            .count();
    int64_t actualRemainingUs = capturedRemainingUs - queueDelayUs;
    if (actualRemainingUs <= 0) {
        ApiDeadline::Instance().Reset();
        return Status(K_RPC_DEADLINE_EXCEEDED,
                      FormatString("RPC deadline exceeded after queue wait, remaining %ld us.", actualRemainingUs));
    }
    GetRequestContext()->reqTimeoutDuration.InitUs(actualRemainingUs);
    ApiDeadline::Instance().InitUs(actualRemainingUs);
    return Status::OK();
}

int64_t GetRemainingUsForMeta()
{
    if (ApiDeadline::Instance().IsInitialized()) {
        int64_t us = ApiDeadline::Instance().ApiRemainingUs();
        return us > 0 ? us : 1;
    }
    if (Trace::Instance().IsRequestLogTrace()) {
        VLOG_EVERY_N(1, K_API_DEADLINE_FALLBACK_LOG_EVERY_N)
            << "ApiDeadline uninitialized, falling back to reqTimeoutDuration.";
    }
    return GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
}

}  // namespace datasystem
