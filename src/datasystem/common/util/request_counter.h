/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: When the worker exits, it needs to count whether there are any new client requests,
 * and ensure that no new requests arrive within a period of time before it is allowed to exit.
 */
#ifndef DATASYSTEM_COMMON_UTIL_REQUEST_COUNTER_H
#define DATASYSTEM_COMMON_UTIL_REQUEST_COUNTER_H

#include <atomic>
#include <string>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
class RequestCounter {
public:
    RequestCounter(const RequestCounter &other) = delete;

    RequestCounter(RequestCounter &&other) = delete;

    RequestCounter &operator=(const RequestCounter &) = delete;

    RequestCounter &operator=(RequestCounter &&) = delete;

    ~RequestCounter() = default;

    /**
     * @brief Singleton mode, obtaining instance.
     * @return Instance of RequestCounter.
     */
    static RequestCounter &GetInstance()
    {
        static RequestCounter instance;
        return instance;
    }

    /**
     * @brief Start to refresh lastArrivalTime_ when the worker starts to exit.
     */
    void StartCount()
    {
        startFlag_ = true;
    }

    /**
     * @brief Refresh lastArrivalTime_ when client request arrived.
     */
    void ResetLastArrivalTime(std::string &&requestName)
    {
        if (!startFlag_) {
            return;
        }
        LOG(WARNING) << "Worker is preparing to exit, but there are still external requests coming in, "
                     << "request name: " << requestName;
        lastArrivalTime_ = GetSteadyClockTimeStampUs();
    }

    /**
     * @brief Get the time when the last client request arrived.
     * @return The time when the last client request arrived.
     */
    int64_t GetLastArrivalTime()
    {
        return lastArrivalTime_;
    }

private:
    RequestCounter() = default;

    // This parameter will be set to true only when the worker starts to exit.
    std::atomic<bool> startFlag_{ false };
    int64_t lastArrivalTime_{ 0 };  // The time when the last client request arrived.
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_REQUEST_COUNTER_H
