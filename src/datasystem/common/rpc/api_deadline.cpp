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

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/format.h"

namespace datasystem {
// Rate-limit the "ApiDeadline uninitialized" fallback log for request-receive threads.
constexpr uint32_t K_API_DEADLINE_FALLBACK_LOG_EVERY_N = 100;

ApiDeadline &ApiDeadline::Instance()
{
    static thread_local ApiDeadline instance;
    return instance;
}

void ApiDeadline::Init(int64_t requestTimeoutMs)
{
    deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(requestTimeoutMs);
    initialized_ = true;
}

void ApiDeadline::InitUs(int64_t requestTimeoutUs)
{
    deadline_ = std::chrono::steady_clock::now() + std::chrono::microseconds(requestTimeoutUs);
    initialized_ = true;
}

int64_t ApiDeadline::ApiRemainingUs() const
{
    if (!initialized_) {
        return RPC_TIMEOUT * ONE_THOUSAND;
    }
    auto currTime = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(deadline_ - currTime).count();
}

bool ApiDeadline::IsInitialized() const
{
    return initialized_;
}

Status ApiDeadline::CheckApiDeadline() const
{
    int64_t remainingUs = ApiRemainingUs();
    CHECK_FAIL_RETURN_STATUS(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("API deadline exceeded, remaining %ld us.", remainingUs));
    return Status::OK();
}

void ApiDeadline::Push()
{
    savedStates_.push_back({deadline_, static_cast<uint8_t>(initialized_ ? 1 : 0)});
}

void ApiDeadline::Pop()
{
    if (savedStates_.empty()) {
        LOG(WARNING) << "ApiDeadline::Pop called on empty stack, Push/Pop mismatch.";
        initialized_ = false;
        return;
    }
    deadline_ = savedStates_.back().deadline;
    initialized_ = savedStates_.back().initialized != 0;
    savedStates_.pop_back();
}

void ApiDeadline::Reset()
{
    initialized_ = false;
    savedStates_.clear();
}

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
    reqTimeoutDuration.InitUs(actualRemainingUs);
    ApiDeadline::Instance().InitUs(actualRemainingUs);
    return Status::OK();
}

int64_t GetRemainingUsForMeta()
{
    if (ApiDeadline::Instance().IsInitialized()) {
        int64_t us = ApiDeadline::Instance().ApiRemainingUs();
        return us > 0 ? us : 1;
    }
    // Background / fan-out threads (IsRequestLogTrace()==false): uninitialized ApiDeadline is
    // the expected steady state — stay silent. Request-receive threads: unexpected fallback
    // (deadline not propagated / exhausted in queue), bound to 1-per-N to avoid stress flooding.
    if (Trace::Instance().IsRequestLogTrace()) {
        VLOG_EVERY_N(1, K_API_DEADLINE_FALLBACK_LOG_EVERY_N)
            << "ApiDeadline uninitialized, falling back to reqTimeoutDuration.";
    }
    return reqTimeoutDuration.CalcRealRemainingTimeUs();
}

}  // namespace datasystem
