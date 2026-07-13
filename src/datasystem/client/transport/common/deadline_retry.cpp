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

/** Description: Implements deadline-aware retry mechanics shared by transport operations. */

#include "datasystem/client/transport/common/deadline_retry.h"

#include <algorithm>
#include <chrono>
#include <thread>

#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
constexpr int64_t MAX_RETRY_BACKOFF_MS = 128;
constexpr int64_t BACKOFF_GROWTH_FACTOR = 2;
}

bool DeadlineRetry::IsRetryableRpcError(const Status &status) const
{
    switch (status.GetCode()) {
        case K_TRY_AGAIN:
        case K_RPC_CANCELLED:
        case K_RPC_DEADLINE_EXCEEDED:
        case K_RPC_UNAVAILABLE:
            return true;
        default:
            return false;
    }
}

Status DeadlineRetry::CheckDeadline() const
{
    return ApiDeadline::Instance().CheckApiDeadline();
}

Status DeadlineRetry::Backoff(int64_t &backoffMs) const
{
    const int64_t remainingUs = ApiDeadline::Instance().ApiRemainingUs();
    CHECK_FAIL_RETURN_STATUS(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED, "API deadline exceeded during retry");
    const int64_t remainingMs = TimeoutDuration::CeilUsToMs(remainingUs);
    const int64_t sleepMs = std::min({ backoffMs, MAX_RETRY_BACKOFF_MS, remainingMs });
    if (sleepMs > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
    }
    backoffMs = std::min(backoffMs * BACKOFF_GROWTH_FACTOR, MAX_RETRY_BACKOFF_MS);
    return CheckDeadline();
}
}  // namespace client
}  // namespace datasystem
