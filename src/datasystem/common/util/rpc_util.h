/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: rpc util.
 */

#ifndef DATASYSTEM_RPC_UTIL_H
#define DATASYSTEM_RPC_UTIL_H

#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <unistd.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/network_latency_estimator.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/sensitive_value.h"

const static int32_t MAX_RPC_TIMEOUT_MS = 600'000;  // 10min

namespace datasystem {
// Retryable RPC/transport failures are safe for an idempotent caller to retry within
// its existing retry budget. RetryOnError still requires the caller's retry policy to
// include the returned code, except that K_RPC_NETWORK_BLIP is accepted by legacy
// K_RPC_UNAVAILABLE policies.
inline bool IsRetryableRpcError(StatusCode code)
{
    switch (code) {
        case StatusCode::K_RPC_CANCELLED:
        case StatusCode::K_RPC_DEADLINE_EXCEEDED:
        case StatusCode::K_RPC_UNAVAILABLE:
        case StatusCode::K_RPC_NETWORK_BLIP:
        case StatusCode::K_URMA_WAIT_TIMEOUT:
            return true;
        default:
            return false;
    }
}

inline bool IsRetryableRpcError(const Status &status)
{
    return IsRetryableRpcError(status.GetCode());
}

// Non-retryable RPC failures must fail fast. A dead peer may be rerouted by a
// higher-level owner, but RetryOnError must not sleep and retry the same target.
inline bool IsNonRetryableRpcError(StatusCode code)
{
    return code == StatusCode::K_RPC_PEER_DEAD;
}

inline bool IsNonRetryableRpcError(const Status &status)
{
    return IsNonRetryableRpcError(status.GetCode());
}

// The genuine-PEER-FAILURE subset of the global-eviction decision: an explicit disconnect or a
// dead peer. Transient retryable errors (DEADLINE_EXCEEDED, NETWORK_BLIP, UNAVAILABLE,
// URMA_WAIT_TIMEOUT, K_URMA_NEED_CONNECT) must NOT evict -- they indicate a slow peer and
// evicting on them collapsed routing capacity under load (code=37, 083cc75bd4 regression). The
// full eviction condition at the call site additionally includes workerNotReady (K_NOT_READY +
// stage), which is not a peer-failure signal and so is kept out of this predicate.
inline bool IsRoutingEvictionFailure(const Status &status)
{
    return status.GetCode() == StatusCode::K_CLIENT_WORKER_DISCONNECT
           || IsNonRetryableRpcError(status);  // K_RPC_PEER_DEAD
}

inline bool ShouldRetryOnStatusCode(StatusCode code, const std::unordered_set<StatusCode> &retryCode)
{
    return !IsNonRetryableRpcError(code)
           && (retryCode.find(code) != retryCode.end()
               || (code == StatusCode::K_RPC_NETWORK_BLIP
                   && retryCode.find(StatusCode::K_RPC_UNAVAILABLE) != retryCode.end()));
}

inline Status ConstructErrorMsg(Status status, const std::unordered_map<StatusCode, uint32_t> errorMap,
                                uint64_t retryCount, int32_t timeoutMs, bool logError)
{
    std::stringstream errorMsg;
    errorMsg << "RPC Retry detail: [ ";
    for (const auto &err : errorMap) {
        errorMsg << FormatString("%s * %ld ", Status::StatusCodeName(err.first), err.second);
    }
    errorMsg << "] with " << retryCount << " times in " << timeoutMs << " ms.";
    status.AppendMsg(errorMsg.str());
    LOG_IF(ERROR, logError) << "[RPC Retry]: " << status.ToString();
    return status;
}

inline void HandleRetryTime(int32_t &retryInterval, int32_t &remainTime, uint64_t &retryCount,
                            int32_t &minOnceRpcTimeoutMs)
{
    // Clamp the backoff sleep so a single retry interval cannot outlive the per-request
    // ApiDeadline budget. When ApiDeadline is uninitialized (background / fan-out threads),
    // ApiRemainingUs() returns RPC_TIMEOUT(60s)*1000, so the apiRemainingMs clamp does not
    // trigger and behavior is identical to the previous code path.
    int32_t apiRemainingMs =
        static_cast<int32_t>(TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs()));
    if (remainTime <= retryInterval) {
        retryInterval = remainTime - minOnceRpcTimeoutMs;
    }
    retryInterval = std::min({ retryInterval, remainTime - minOnceRpcTimeoutMs, apiRemainingMs });
    if (retryInterval < 0) {
        retryInterval = 0;
    }
    remainTime -= retryInterval;
    ++retryCount;
    if (retryInterval > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(retryInterval));
    }
}

/**
 * @brief Update the NetworkLatencyEstimator with the residual RPC budget after a call.
 * @param[in] rpcTimeoutMs Per-call RPC timeout that was in effect, in milliseconds.
 * @param[in] start Steady-clock time point when the call started.
 */
inline void UpdateNetworkLatencyEstimate(int32_t rpcTimeoutMs, const std::chrono::steady_clock::time_point &start)
{
    auto elapsedUs = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
    int64_t residualUs = static_cast<int64_t>(rpcTimeoutMs) * 1000LL - elapsedUs;
    if (residualUs > 0) {
        NetworkLatencyEstimator::Instance().Update(residualUs);
    }
}

template <class Function, class Handler>
Status RetryOnError(int32_t timeoutMs, Function &&func, Handler &&errorHandler,
                    const std::unordered_set<StatusCode> &retryCode, int32_t maxRpcTimeoutMs = MAX_RPC_TIMEOUT_MS,
                    const std::unordered_set<StatusCode> &exceptionCode = {}, bool logError = false,
                    int32_t minOnceRpcTimeoutMs = 10)
{
    if (timeoutMs <= 0) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
    }
    // timeoutMs is the aggregate retry budget. Each attempt receives a per-RPC timeout
    // clipped by maxRpcTimeoutMs, and failed retryable attempts sleep with this bounded
    // backoff sequence while enough budget remains for one more call.
    static std::vector<int32_t> retryIntervalsMs = { 1, 5, 50, 200, 1000, 5000 };
    auto startTime = std::chrono::steady_clock::now();
    uint64_t retryCount = 0;
    Status status;
    std::unordered_map<StatusCode, uint32_t> errorMap;
    int32_t remainTimeMs = timeoutMs;
    do {
        int32_t rpcTimeoutMs = std::max<int32_t>(std::min<int32_t>(remainTimeMs, maxRpcTimeoutMs), 1);
        // Check the per-request API deadline before each attempt. brpc set_timeout_ms only
        // truncates blocking inside CallMethod; synchronous SDK-side code outside CallMethod
        // (between this lambda's entry and DS_OC_DISPATCH) is NOT covered. A deadline already
        // exceeded here returns K_RPC_DEADLINE_EXCEEDED. When ApiDeadline is uninitialized
        // (background / fan-out threads), ApiRemainingUs() returns RPC_TIMEOUT (60s) and
        // CheckApiDeadline returns OK, so background callers are unaffected. retryCode includes
        // K_RPC_DEADLINE_EXCEEDED, but remainTimeMs<=minOnceRpcTimeoutMs breaks the loop, so
        // a genuine expiry exits cleanly rather than spinning.
        RETURN_IF_NOT_OK(ApiDeadline::Instance().CheckApiDeadline());
        auto iterStartTime = std::chrono::steady_clock::now();
        // Wrap the inject points in a lambda so an injected `return(...)` only
        // exits the lambda (yielding that status as the attempt's result), not
        // the whole RetryOnError function. Placing them at function scope would
        // short-circuit the retry loop: the first injected error would return
        // immediately without any retry, breaking callers that inject
        // K_RPC_UNAVAILABLE to exercise the retry/backoff path.
        auto invoke = [&func](int32_t rpcTimeoutMs) -> Status {
            INJECT_POINT("rpc_util.retry_on_error_before_func");
            Status rc = func(rpcTimeoutMs);
            INJECT_POINT("rpc_util.retry_on_error_after_func");
            return rc;
        };
        status = invoke(rpcTimeoutMs);
        UpdateNetworkLatencyEstimate(rpcTimeoutMs, iterStartTime);
        if (IsNonRetryableRpcError(status)) {
            // Peer-dead must fast-fail: never sleep or retry the same dead target. The stale
            // stub cache (if any) is dropped by the single errorHandler call in the post-loop
            // cleanup below, so the caller's next attempt builds a fresh stub → new connection.
            break;
        }
        StatusCode code = status.GetCode();
        bool isException = exceptionCode.find(code) != exceptionCode.end();
        if (isException && retryCount > 0) {  // exception on a retry is treated as success
            LOG(INFO) << "The retry succeeds and the response received is: " << status.ToString();
            return Status::OK();
        }
        if (!isException && !ShouldRetryOnStatusCode(code, retryCode)) {
            break;  // non-retryable, non-exception code stops the loop
        }
        errorMap[code]++;
        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - startTime).count();
        remainTimeMs = timeoutMs - static_cast<int32_t>(elapsedMs);
        int32_t retryIntervalMs = retryIntervalsMs[std::min<uint64_t>(retryCount, retryIntervalsMs.size() - 1)];
        VLOG(1) << "retryCount: " << retryCount << ", interval: " << retryIntervalMs << ", remain: " << remainTimeMs;
        if (remainTimeMs <= minOnceRpcTimeoutMs) { break; }  // not enough time for one more call
        HandleRetryTime(retryIntervalMs, remainTimeMs, retryCount, minOnceRpcTimeoutMs);
    } while (remainTimeMs > 0);

    if (status.IsError()) {
        status = ConstructErrorMsg(status, errorMap, retryCount, timeoutMs, logError);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(errorHandler(),
                                         FormatString("[RPC Retry]: Failed to do clean up. %s", status.ToString()));
    }
    return status;
}

template <class Function, class Handler>
Status RetryOnErrorRepent(int64_t timeoutMs, Function &&func, Handler &&errorHandler,
                          const std::unordered_set<StatusCode> &errCode, bool repent = true)
{
    return RetryOnError(timeoutMs, func, errorHandler, errCode, MAX_RPC_TIMEOUT_MS, {}, repent);
}

template <class Function, class Handler>
Status RetryOnErrorRepent(int64_t timeoutMs, Function &&func, Handler &&errorHandler,
                          const std::unordered_set<StatusCode> &errCode, int32_t minOnceRpcTimeoutMs)
{
    return RetryOnError(timeoutMs, std::forward<Function>(func), std::forward<Handler>(errorHandler), errCode,
                        MAX_RPC_TIMEOUT_MS, {}, true, minOnceRpcTimeoutMs);
}

template <class ReqType>
void SetToken(ReqType &req, const SensitiveValue &token)
{
    if (!token.Empty()) {
        req.set_token(token.GetData(), token.GetSize());
    }
}

/// Retry func until it returns OK. NO DEADLINE — the caller's func MUST self-terminate
/// (e.g. via a health check or topology-removal signal). For bounded graceful-exit retry,
/// use the overload below with a std::chrono::seconds grace argument.
template <class Function, class... Args>
Status RetryUntilSuccessDuringGracefulExit(Function &&func, Args &&...args)
{
    const int retryIntervalSecs = 1;
    Status status;
    do {
        status = func(std::forward<Args>(args)...);
        if (status.IsError()) {
            LOG_FIRST_N(WARNING, 1) << "Execute failed with error: " << status.ToString()
                                    << ". Will be executed repeatedly until successful";
            std::this_thread::sleep_for(std::chrono::seconds(retryIntervalSecs));
        }
    } while (status.IsError());
    LOG(INFO) << "Execute success";
    return status;
}

template <class Function, class... Args>
Status RetryUntilSuccessDuringGracefulExit(Function &&func, std::chrono::seconds grace, Args &&...args)
{
    const int retryIntervalSecs = 1;
    const auto start = std::chrono::steady_clock::now();
    Status status;
    do {
        status = func(std::forward<Args>(args)...);
        if (status.IsError()) {
            LOG_FIRST_N(WARNING, 1) << "Execute failed with error: " << status.ToString()
                                    << ". Will be executed repeatedly until successful or grace expires";
            if (std::chrono::steady_clock::now() - start >= grace) {
                LOG(WARNING) << "Graceful-exit retry gave up before success. last_error=" << status.ToString()
                             << ", grace_secs=" << grace.count();
                return status;
            }
            std::this_thread::sleep_for(std::chrono::seconds(retryIntervalSecs));
        }
    } while (status.IsError());
    LOG(INFO) << "Execute success";
    return status;
}

template <typename Worker, typename Breaker>
Status RetryUntil(Worker &&worker, Breaker &&breaker)
{
    const int retryIntervalSecs = 1;
    Status status;
    do {
        status = worker();
        if (breaker(status)) {
            break;
        }
        LOG_FIRST_N(WARNING, 1) << "Execute failed with error: " << status.ToString()
                                << ". Will be executed repeatedly until successful";
        std::this_thread::sleep_for(std::chrono::seconds(retryIntervalSecs));
    } while (true);
    LOG(INFO) << "Execute success";
    return status;
}
}  // namespace datasystem
#endif  // DATASYSTEM_RPC_UTIL_H
