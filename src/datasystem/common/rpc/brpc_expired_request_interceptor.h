/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Server-global brpc interceptor that rejects requests whose client-side
 * deadline already elapsed while queued, before the handler runs.
 */

#ifndef DATASYSTEM_COMMON_RPC_BRPC_EXPIRED_REQUEST_INTERCEPTOR_H
#define DATASYSTEM_COMMON_RPC_BRPC_EXPIRED_REQUEST_INTERCEPTOR_H

#include <cstdint>
#include <string>

#include <brpc/controller.h>
#include <brpc/interceptor.h>
#include <butil/time.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/rpc/brpc_status_util.h"

namespace datasystem {

// Milliseconds to microseconds conversion for the deadline arithmetic below.
inline constexpr int64_t BRPC_INTERCEPTOR_US_PER_MS = 1000;

// brpc stores the client-propagated timeout_ms on the server Controller but never checks it
// before running the handler, so a request that crossed its client deadline while sitting in
// the bthread run queue still runs the full handler (orphaned work) under load. This interceptor
// drops such requests before CallMethod + body deserialization: it returns ERPCTIMEDOUT, which
// the datasystem client maps to K_RPC_DEADLINE_EXCEEDED (the client retries on another worker
// within its remaining budget, and stops once the budget is exhausted). It is defense-in-depth
// on top of the per-handler ApiDeadline checks and covers all baidu-std-protocol brpc RPCs
// (datasystem's brpc traffic uses baidu-std; requests carrying no received_us - e.g. other
// protocols - are accepted by the guard, never dropped). Toggle at runtime via
// FLAGS_brpc_drop_expired_request (hot-updatable via monitor_config_file).
class ExpiredRequestInterceptor : public brpc::Interceptor {
public:
    bool Accept(const brpc::Controller *controller, int &error_code, std::string &error_txt) const override
    {
        if (!FLAGS_brpc_drop_expired_request) {
            return true;
        }
        int64_t timeoutMs = controller->timeout_ms();
        int64_t receivedUs = controller->get_rpc_received_us();
        // timeout_ms is UNSET_MAGIC_NUM (<0) or 0 when the client sent no deadline (e.g. a
        // non-datasystem client, or the deliver-timeout-ms gflag off); received_us == 0 means unset.
        // In all those cases there is no client deadline to enforce, so accept the request.
        if (timeoutMs <= 0 || receivedUs <= 0) {
            return true;
        }
        int64_t elapsedUs = butil::cpuwide_time_us() - receivedUs;
        // Safe over-estimate: received_us is when the SERVER read the request; timeout_ms is the
        // client's total budget from SEND time. elapsed >= timeout means the server waited longer
        // than the client's entire budget, so the client has certainly timed out (client->server
        // network latency is pure slack) - no false positives.
        if (elapsedUs >= timeoutMs * BRPC_INTERCEPTOR_US_PER_MS) {
            METRIC_ADD(metrics::KvMetricId::BRPC_EXPIRED_REQUEST_DROP_TOTAL, 1);
            error_code = kBrpcErpcTimedOut;  // 1008 -> client maps to K_RPC_DEADLINE_EXCEEDED
            error_txt = "client request deadline already elapsed before handler dispatch";
            return false;  // reject: brpc SetFailed + SendRpcResponse, skips CallMethod
        }
        return true;
    }
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_BRPC_EXPIRED_REQUEST_INTERCEPTOR_H
