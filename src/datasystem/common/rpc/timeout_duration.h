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

#include <chrono>

#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
#define SET_RPC_TIMEOUT(_timeoutDuration_, _opts_)                                                                    \
    do {                                                                                                              \
        int64_t _remainingTime_ = (_timeoutDuration_).CalcRemainingTime();                                            \
        CHECK_FAIL_RETURN_STATUS(_remainingTime_ > 0, K_RPC_DEADLINE_EXCEEDED,                                        \
                                 FormatString("The remaining time of the RPC request is %ld, which has exceeded the " \
                                              "deadline and cannot be continued.",                                    \
                                              _remainingTime_));                                                      \
        (_opts_).SetTimeout(_remainingTime_);                                                                         \
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
     * @brief Calculate the remaining duration, it's 4/5 of the real remaining time.
     * @return Remaining duration, in milliseconds.
     */
    int64_t CalcRemainingTime()
    {
        return ScaleTimeoutMs(CalcRealRemainingTime(), OP_USE_TIME_FACTOR);
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

private:
    static constexpr int DEFAULT_TIMEOUT = RPC_TIMEOUT;
    static constexpr int64_t MS_TO_US = 1'000;
    static constexpr int64_t ROUND_UP_US = 500;
    static constexpr int64_t SMALL_TIMEOUT_ROUND_THRESHOLD_MS = 10;
    static constexpr double OP_USE_TIME_FACTOR = 0.8;

    bool initialized_ = false;
    int64_t defaultDuration_ = DEFAULT_TIMEOUT;
    std::chrono::steady_clock::time_point deadLine_;
};
}  // namespace datasystem
#endif
