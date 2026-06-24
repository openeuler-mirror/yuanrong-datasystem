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
 * Description: Steady clock abstraction for testable time control.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_STEADY_CLOCK_H
#define DATASYSTEM_COMMON_COORDINATOR_STEADY_CLOCK_H

#include <chrono>
#include <cstdint>
#include <mutex>

namespace datasystem {

/**
 * @brief Abstract steady clock interface for testable time control.
 */
class SteadyClock {
public:
    virtual ~SteadyClock() = default;

    /**
     * @brief Get current time in milliseconds since an arbitrary epoch.
     * @return Current time in milliseconds.
     */
    virtual uint64_t NowMs() = 0;

    /**
     * @brief Get current time point.
     * @return Current steady_clock time point.
     */
    virtual std::chrono::steady_clock::time_point Now() = 0;
};

/**
 * @brief Real steady clock using std::chrono::steady_clock.
 */
class SteadyClockReal : public SteadyClock {
public:
    uint64_t NowMs() override
    {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    }

    std::chrono::steady_clock::time_point Now() override
    {
        return std::chrono::steady_clock::now();
    }
};

/**
 * @brief Mock steady clock for testing. Time only advances via Advance().
 */
class SteadyClockMock : public SteadyClock {
public:
    explicit SteadyClockMock(uint64_t initialMs = 0) : currentMs_(initialMs)
    {
    }
    ~SteadyClockMock() override = default;

    uint64_t NowMs() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return currentMs_;
    }

    std::chrono::steady_clock::time_point Now() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return epoch_ + std::chrono::milliseconds(currentMs_);
    }

    /**
     * @brief Advance the mock clock by the given number of milliseconds.
     * @param[in] ms Milliseconds to advance.
     */
    void AdvanceMs(uint64_t ms)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        currentMs_ += ms;
    }

    /**
     * @brief Set the clock to a specific time.
     * @param[in] ms Absolute time in milliseconds.
     */
    void SetMs(uint64_t ms)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        currentMs_ = ms;
    }

private:
    uint64_t currentMs_;
    std::mutex mutex_;
    const std::chrono::steady_clock::time_point epoch_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_STEADY_CLOCK_H
