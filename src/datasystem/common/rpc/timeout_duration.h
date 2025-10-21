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
        int64_t remainingTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(deadLine_ - currTime).count();
        return remainingTimeMs;
    }

    /**
     * @brief Calculate the remaining duration, it's 4/5 of the real remaining time.
     * @return Remaining duration, in milliseconds.
     */
    int64_t CalcRemainingTime()
    {
        return CalcRealRemainingTime() * OP_USE_TIME_FACTOR;
    }

private:
    static constexpr int DEFAULT_TIMEOUT = RPC_TIMEOUT;
    static constexpr float OP_USE_TIME_FACTOR = 0.8;

    bool initialized_ = false;
    int64_t defaultDuration_ = DEFAULT_TIMEOUT;
    std::chrono::steady_clock::time_point deadLine_;
};
}  // namespace datasystem
#endif
