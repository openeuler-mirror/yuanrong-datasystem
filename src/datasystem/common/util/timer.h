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
 * Description: Timer utils.
 */
#ifndef DATASYSTEM_COMMON_UTIL_TIMER_H
#define DATASYSTEM_COMMON_UTIL_TIMER_H

#include <chrono>
#include <cmath>
#include <iostream>

namespace datasystem {
constexpr uint64_t SECS_TO_MS = 1000;

inline uint64_t TsToNs(struct timespec &ts)
{
    const uint64_t NS_TO_SECS = 1'000'000'000ul;
    return ts.tv_sec * (NS_TO_SECS) + ts.tv_nsec;
}

inline struct timespec NsToTs(uint64_t ns)
{
    const uint64_t NS_TO_SECS = 1'000'000'000ul;
    return { .tv_sec = static_cast<time_t>(ns / NS_TO_SECS), .tv_nsec = static_cast<time_t>(ns % NS_TO_SECS) };
}

inline std::time_t GetSteadyClockTimeStampMs()
{
    // Attention: System clock is not monotonic.
    // Instead, steady clock is monotonic.
    return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
        .time_since_epoch()
        .count();
}

inline std::time_t GetSteadyClockTimeStampUs()
{
    // Attention: System clock is not monotonic.
    // Instead, steady clock is monotonic.
    return std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now())
        .time_since_epoch()
        .count();
}

inline std::time_t GetSystemClockTimeStampUs()
{
    // Attention: System clock is not monotonic.
    return std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now())
        .time_since_epoch()
        .count();
}

inline std::string GetUtcTimestamp()
{
    std::time_t tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    std::tm utc;
    static const size_t timeStrLen = 20;
    char buffer[timeStrLen]{0};
    (void)strftime(buffer, sizeof(buffer), "%Y%m%dT%H%M%SZ", gmtime_r(&tt, &utc));
    return std::string(buffer);
}

/**
 * @brief Converts timeval structures to milliseconds.
 * @param[in] timeVal Timeval struct.
 * @return Input time, in milliseconds.
 */
inline int64_t TimevalToMsApprox(const timeval &timeVal)
{
    return timeVal.tv_sec * SECS_TO_MS + timeVal.tv_usec / SECS_TO_MS;
}

/**
 * @brief Converts milliseconds to timeval structures
 * @param[in] timeMs Input time, in milliseconds.
 * @return Timeval struct.
 */
inline timeval MsToTimeval(int64_t timeMs)
{
    timeval timeVal;
    timeVal.tv_sec = timeMs / SECS_TO_MS;          // ms to s
    timeVal.tv_usec = timeMs % SECS_TO_MS * SECS_TO_MS;  // ms to us
    return timeVal;
}

class Timer {
public:
    Timer() : beg_(clock::now()), timeoutMs_(0)
    {
    }

    Timer(int64_t timeoutMs) : beg_(clock::now()), timeoutMs_(timeoutMs)
    {
    }

    ~Timer() = default;

    void Reset()
    {
        beg_ = clock::now();
    }

    void AdjustTimeoutAndReset(int64_t timeoutMs)
    {
        timeoutMs_ = timeoutMs;
        Reset();
    }

    void Clear()
    {
        timeoutMs_ = 0;
    }

    bool IsTimeout()
    {
        return timeoutMs_ == 0 ? false : ElapsedMilliSecond() - timeoutMs_ >= 0;
    }

    double ElapsedSecond() const
    {
        return std::chrono::duration_cast<second>(clock::now() - beg_).count();
    }

    double ElapsedMilliSecond() const
    {
        return std::chrono::duration_cast<millisecond>(clock::now() - beg_).count();
    }

    double ElapsedMicroSecond() const
    {
        return std::chrono::duration_cast<microsecond>(clock::now() - beg_).count();
    }

    double ElapsedSecondAndReset()
    {
        double elapsed = std::chrono::duration_cast<second>(clock::now() - beg_).count();
        beg_ = clock::now();
        return elapsed;
    }

    double ElapsedMilliSecondAndReset()
    {
        double elapsed = std::chrono::duration_cast<millisecond>(clock::now() - beg_).count();
        beg_ = clock::now();
        return elapsed;
    }

    int64_t GetRemainingTimeMs() const
    {
        int64_t remaining = timeoutMs_ - ElapsedMilliSecond();
        return std::max((int64_t)0, remaining);
    }

private:
    typedef std::chrono::steady_clock clock;
    typedef std::chrono::duration<double, std::ratio<1> > second;
    typedef std::chrono::duration<double, std::milli> millisecond;
    typedef std::chrono::duration<double, std::micro> microsecond;
    std::chrono::time_point<clock> beg_;
    int64_t timeoutMs_;
};
}  // namespace datasystem

#endif  // COMMON_UTIL_TIMER_H
