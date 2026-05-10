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
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/sensitive_value.h"

const static int32_t MAX_RPC_TIMEOUT_MS = 600'000;  // 10min

namespace datasystem {
inline bool IsRpcTimeout(const Status &status)
{
    return status.GetCode() == StatusCode::K_RPC_CANCELLED || status.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED
           || status.GetCode() == StatusCode::K_RPC_UNAVAILABLE
           || status.GetCode() == StatusCode::K_URMA_WAIT_TIMEOUT;
}

inline bool IsRpcTimeoutOrTryAgain(const Status &status)
{
    return status.GetCode() == StatusCode::K_TRY_AGAIN || IsRpcTimeout(status);
}

template <class Function, class... Args>
Status RetryOnRPCError(Function &&func, Args &&...args)
{
    const int retryIntervalSecs = 1;
    const int maxRetryCount = 3;
    int retryCount = 0;
    Status status;
    do {
        status = func(std::forward<Args>(args)...);
        if (IsRpcTimeout(status)) {
            ++retryCount;
            std::this_thread::sleep_for(std::chrono::seconds(retryIntervalSecs));
            LOG(ERROR) << "retry " << retryCount << " times.";
        } else {
            // If network is ok, we will return the value.
            break;
        }
    } while (retryCount < maxRetryCount);

    return status;
}

template <class Function>
Status RetryOnRPCErrorByCount(int maxRetryCount, Function &&func, const std::unordered_set<StatusCode> &exceptionCode)
{
    const int retryIntervalSecs = 1;
    int retryCount = 0;
    INJECT_POINT("rpc_util.retry_on_rpc_error_by_count", [&maxRetryCount](int count) {
        LOG(INFO) << "set maxRetryCount to " << count;
        maxRetryCount = count;
        return Status::OK();
    });
    Status status;
    do {
        status = func();
        if (IsRpcTimeout(status)) {
            ++retryCount;
            std::this_thread::sleep_for(std::chrono::seconds(retryIntervalSecs));
            LOG(INFO) << "retry " << retryCount << " times.";
        } else {
            // If an exception code is received during retry, the retry is considered successful.
            if (retryCount > 0 && exceptionCode.find(status.GetCode()) != exceptionCode.end()) {
                LOG(INFO) << "The retry succeeds and the response received is: " << status.ToString();
                status = Status::OK();
            }
            // If network is ok, we will return the value.
            break;
        }
    } while (retryCount < maxRetryCount);

    return status;
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
    retryInterval = remainTime <= retryInterval ? remainTime - minOnceRpcTimeoutMs : retryInterval;
    remainTime -= retryInterval;
    ++retryCount;
    std::this_thread::sleep_for(std::chrono::milliseconds(retryInterval));
}

template <class Function, class Handler>
Status RetryOnError(int32_t timeoutMs, Function &&func, Handler &&errorHandler,
                    const std::unordered_set<StatusCode> &retryCode, int32_t maxRpcTimeoutMs = MAX_RPC_TIMEOUT_MS,
                    const std::unordered_set<StatusCode> &exceptionCode = {}, bool logError = false,
                    int32_t minOnceRpcTimeoutMs = 10)
{
    if (timeoutMs < 0) {
        RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
    }
    // The retries last for four times, and the interval is continuously extended,
    // which are 1, 5, 50, 200, 1000, 5000 according to the rpc timeout setting
    static std::vector<int32_t> retryIntervalsMs = { 1, 5, 50, 200, 1000, 5000 };
    auto startTime = std::chrono::steady_clock::now();
    uint64_t retryCount = 0;
    Status status;
    std::unordered_map<StatusCode, uint32_t> errorMap;
    int32_t remainTimeMs = timeoutMs;
    do {
        auto f = [&func](int32_t rpcTimeoutMs) {
            INJECT_POINT("rpc_util.retry_on_error_before_func");
            Status rc = func(rpcTimeoutMs);
            INJECT_POINT("rpc_util.retry_on_error_after_func");
            return rc;
        };
        int32_t rpcTimeoutMs = std::min<int32_t>(remainTimeMs, maxRpcTimeoutMs);
        rpcTimeoutMs = std::max<int32_t>(rpcTimeoutMs, 1);
        status = f(rpcTimeoutMs);
        if (exceptionCode.find(status.GetCode()) != exceptionCode.end()) {
            // If an exception code is received during retry, the retry is considered successful.
            if (retryCount > 0) {
                LOG(INFO) << "The retry succeeds and the response received is: " << status.ToString();
                return Status::OK();
            }
        } else if (retryCode.find(status.GetCode()) == retryCode.end()) {
            // If a retry code is not received, stop retrying and return the error code.
            break;
        }
        errorMap[status.GetCode()]++;

        remainTimeMs =
            timeoutMs
            - std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startTime)
                  .count();

        int32_t retryIntervalMs =
            retryCount < retryIntervalsMs.size() ? retryIntervalsMs[retryCount] : retryIntervalsMs.back();
        VLOG(1) << "retryCount: " << retryCount << ", retryIntervalMs: " << retryIntervalMs
                << ", remainTimeMs: " << remainTimeMs;

        if (remainTimeMs <= minOnceRpcTimeoutMs) {
            // If the remaining time is not enough to execute the function once, here need to exit.
            break;
        }
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

template <class Function>
Status RetryOnRPCErrorByTime(int64_t timeoutMs, Function &&func, bool repent = false)
{
    return RetryOnError(
        timeoutMs, func, []() { return Status::OK(); },
        { StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED, StatusCode::K_RPC_UNAVAILABLE,
          StatusCode::K_URMA_WAIT_TIMEOUT },
        MAX_RPC_TIMEOUT_MS, {}, repent);
}

template <class ReqType>
void SetToken(ReqType &req, const SensitiveValue &token)
{
    if (!token.Empty()) {
        req.set_token(token.GetData(), token.GetSize());
    }
}

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
