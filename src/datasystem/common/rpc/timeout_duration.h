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
 * Description: Calculate the RPC timeout duration.
 */
#ifndef DATASYSTEM_COMMON_RPC_TIMEOUT_DURATION_H
#define DATASYSTEM_COMMON_RPC_TIMEOUT_DURATION_H

#include <algorithm>
#include <chrono>

#include "datasystem/common/rpc/network_latency_estimator.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
#define SET_RPC_TIMEOUT(_timeoutDuration_, _opts_)                                                                    \
    do {                                                                                                              \
        int64_t _remainingTimeUs_ = (_timeoutDuration_)->CalcRemainingAfterDeductionUs();                             \
        CHECK_FAIL_RETURN_STATUS(_remainingTimeUs_ > 0, K_RPC_DEADLINE_EXCEEDED,                                      \
                                 FormatString("The remaining time of the RPC request is %ld us, which has exceeded " \
                                               "the deadline and cannot be continued.",                               \
                                               _remainingTimeUs_));                                                    \
        (_opts_).SetTimeout(TimeoutDuration::CeilUsToMs(_remainingTimeUs_));                                          \
    } while (false)

class TimeoutDuration {
public:
    /**
     * @brief Constructor.
     * @param[in] defaultDuration Default duration, in milliseconds.
     */
    explicit TimeoutDuration(int64_t defaultDuration = DEFAULT_TIMEOUT) : defaultDuration_(defaultDuration)
    {
    }

    ~TimeoutDuration() = default;

    /**
     * @brief Initialize using the default duration.
     */
    void Init()
    {
        Init(defaultDuration_);
    }

    /**
     * @brief Initialize with the input duration.
     * @param[in] timeout Timeout duration, in milliseconds.
     */
    void Init(int64_t timeout)
    {
        deadLine_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
        initialized_ = true;
    }

    /**
     * @brief Initialize the duration with positive time interval.
     * @param[in] timeout Timeout duration, in milliseconds.
     */
    void InitWithPositiveTime(int64_t timeout)
    {
        if (timeout <= 0) {
            timeout = RPC_TIMEOUT;
        }
        Init(timeout);
    }

    /**
     * @brief Initialize with the input duration in microseconds.
     * @param[in] timeoutUs Timeout duration, in microseconds.
     */
    void InitUs(int64_t timeoutUs)
    {
        deadLine_ = std::chrono::steady_clock::now() + std::chrono::microseconds(timeoutUs);
        initialized_ = true;
    }

    /**
     * @brief Initialize the duration with a positive microsecond interval,
     * falling back to RPC_TIMEOUT when non-positive.
     * @param[in] timeoutUs Timeout duration, in microseconds.
     */
    void InitWithPositiveTimeUs(int64_t timeoutUs)
    {
        if (timeoutUs <= 0) {
            LOG(WARNING) << FormatString(
                "InitWithPositiveTimeUs: timeoutUs=%ld <= 0, fallback to RPC_TIMEOUT=%dms.", timeoutUs, RPC_TIMEOUT);
            timeoutUs = RPC_TIMEOUT * MS_TO_US;
        }
        InitUs(timeoutUs);
    }

    /**
     * @brief Reset to set the initialization status to false.
     */
    void Reset()
    {
        initialized_ = false;
    }

    /**
     * @brief Calculate the real remaining duration.
     * @return Remaining duration, in milliseconds.
     */
    int64_t CalcRealRemainingTime()
    {
        if (!initialized_) {
            // If it is not initialized, return the default duration.
            return defaultDuration_;
        }
        auto currTime = std::chrono::steady_clock::now();
        int64_t remainingTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(deadLine_ - currTime).count();
        return ConvertRemainingTimeUsToMs(remainingTimeUs);
    }

    /**
     * @brief Calculate the real remaining duration.
     * @return Remaining duration, in microseconds.
     */
    int64_t CalcRealRemainingTimeUs()
    {
        if (!initialized_) {
            return defaultDuration_ * MS_TO_US;
        }
        auto currTime = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(deadLine_ - currTime).count();
    }

    /**
     * @brief Calculate the remaining duration, it's 4/5 of the real remaining time.
     * @return Remaining duration, in milliseconds.
     */
    int64_t CalcRemainingTime()
    {
        return ScaleTimeoutMs(CalcRealRemainingTime(), OP_USE_TIME_FACTOR);
    }

    /**
     * @brief Calculate the remaining duration in microseconds, it's 4/5 of the real remaining time,
     * with short timeouts returned unscaled.
     * @return Remaining duration, in microseconds.
     */
    int64_t CalcRemainingTimeUs()
    {
        int64_t realRemainingUs = CalcRealRemainingTimeUs();
        if (realRemainingUs <= 0) {
            return realRemainingUs;
        }
        if (realRemainingUs <= SHORT_TIMEOUT_THRESHOLD_US) {
            return realRemainingUs;
        }
        return ScaleTimeoutUs(realRemainingUs, OP_USE_TIME_FACTOR);
    }

    /**
     * @brief Calculate the remaining duration in microseconds after deducting the estimated network-latency margin.
     * @return Remaining duration in microseconds after deduction; 0 if exhausted.
     */
    int64_t CalcRemainingAfterDeductionUs()
    {
        int64_t remainingUs = CalcRemainingTimeUs();
        int64_t deductUs = NetworkLatencyEstimator::Instance().GetDeductUs();
        return remainingUs > deductUs ? remainingUs - deductUs : 0;
    }

    /**
     * @brief Convert remaining duration from microseconds to milliseconds.
     * @param[in] remainingTimeUs Remaining duration, in microseconds.
     * @return Remaining duration, in milliseconds.
     */
    static int64_t ConvertRemainingTimeUsToMs(int64_t remainingTimeUs)
    {
        if (remainingTimeUs <= 0) {
            return remainingTimeUs / MS_TO_US;
        }
        if (remainingTimeUs > SMALL_TIMEOUT_ROUND_THRESHOLD_MS * MS_TO_US) {
            return remainingTimeUs / MS_TO_US;
        }
        int64_t roundedTimeMs = (remainingTimeUs + ROUND_UP_US) / MS_TO_US;
        return roundedTimeMs > 0 ? roundedTimeMs : 1;
    }

    /**
     * @brief Scale a millisecond timeout by a factor, returning non-positive input unchanged.
     * @param[in] timeoutMs Timeout duration, in milliseconds.
     * @param[in] scaleFactor Multiplier applied to the timeout.
     * @return Scaled duration, in milliseconds.
     */
    static int64_t ScaleTimeoutMs(int64_t timeoutMs, double scaleFactor)
    {
        if (timeoutMs <= 0) {
            return timeoutMs;
        }
        if (timeoutMs > SMALL_TIMEOUT_ROUND_THRESHOLD_MS) {
            return timeoutMs * scaleFactor;
        }
        int64_t timeoutUs = timeoutMs * MS_TO_US * scaleFactor;
        int64_t scaledTimeoutMs = timeoutUs / MS_TO_US;
        if (timeoutUs % MS_TO_US >= ROUND_UP_US) {
            ++scaledTimeoutMs;
        }
        return scaledTimeoutMs > 0 ? scaledTimeoutMs : 1;
    }

    /**
     * @brief Scale a microsecond timeout by a factor, returning non-positive input unchanged.
     * @param[in] timeoutUs Timeout duration, in microseconds.
     * @param[in] scaleFactor Multiplier applied to the timeout.
     * @return Scaled duration, in microseconds.
     */
    static int64_t ScaleTimeoutUs(int64_t timeoutUs, double scaleFactor)
    {
        if (timeoutUs <= 0) {
            return timeoutUs;
        }
        return static_cast<int64_t>(timeoutUs * scaleFactor);
    }

    /**
     * @brief Convert a microsecond duration to milliseconds using ceiling rounding,
     * so short timeouts are not truncated to 0ms. Costs at most 1ms per hop.
     * @param[in] us Duration in microseconds.
     * @return Ceiling-converted duration in milliseconds; non-positive input returned as us/1000.
     */
    static int64_t CeilUsToMs(int64_t us)
    {
        if (us <= 0) {
            return us / MS_TO_US;
        }
        int64_t ms = us / MS_TO_US;
        if (us % MS_TO_US > 0) {
            ++ms;
        }
        return ms > 0 ? ms : 1;
    }

    static int64_t WorkerGetRequestTimeout(int32_t timeout)
    {
        if (timeout <= 0) {
            return timeout;
        }
        if (timeout <= SMALL_TIMEOUT_ROUND_THRESHOLD_MS) {
            return timeout;
        }
        return std::max(ScaleTimeoutMs(timeout, WORKER_TIMEOUT_DESCEND_FACTOR),
                        timeout - WORKER_TIMEOUT_MINUS_MILLISECOND);
    }

    static constexpr int64_t SHORT_TIMEOUT_THRESHOLD_US = 10'000;  // Timeouts at or below this skip scaling.
    static constexpr int64_t SLOW_PATH_LOG_THRESHOLD_US = 3000;  // Threshold for slow-path warning log.
    static constexpr int64_t SMALL_TIMEOUT_ROUND_THRESHOLD_MS = 10;  // Timeouts at or below this skip descend-factor.
    static constexpr double WORKER_TIMEOUT_DESCEND_FACTOR = 0.9;
    // Worker/master reserves more headroom (5000ms vs client's 1000ms) for server-side
    // processing (metadata persistence, replication, etc.). Only applies to >10ms timeouts.
    static constexpr int64_t WORKER_TIMEOUT_MINUS_MILLISECOND = 5 * 1000;

private:
    static constexpr int DEFAULT_TIMEOUT = RPC_TIMEOUT;
    static constexpr int64_t MS_TO_US = 1'000;
    static constexpr int64_t ROUND_UP_US = 500;
    static constexpr double OP_USE_TIME_FACTOR = 0.8;

    bool initialized_ = false;
    int64_t defaultDuration_ = DEFAULT_TIMEOUT;
    std::chrono::steady_clock::time_point deadLine_;
};
}  // namespace datasystem
#endif
