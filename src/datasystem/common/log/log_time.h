/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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
 * Description: Log time util.
 */
#ifndef DATASYSTEM_COMMON_LOG_LOG_TIME_H
#define DATASYSTEM_COMMON_LOG_LOG_TIME_H

#include <chrono>
#include <iomanip>

#include <unistd.h>
#include <sys/syscall.h>

#include "datasystem/common/log/trace.h"

namespace datasystem {
struct LogTime {
    std::tm tm;
    int usec;

    LogTime()
    {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()) % 1000000;
        // UTC
        localtime_r(&time_t, &tm);
        usec = static_cast<int>(microseconds.count());
    }

    int sec() const   { return tm.tm_sec; }
    int min() const   { return tm.tm_min; }
    int hour() const  { return tm.tm_hour; }
    int day() const   { return tm.tm_mday; }
    int month() const { return tm.tm_mon; }
    int year() const  { return tm.tm_year; }
    int dayOfWeek() const { return tm.tm_wday; }
    int dayInYear() const { return tm.tm_yday; }
    int dst() const   { return tm.tm_isdst; }

    const std::tm* getTm() const { return &tm; }
    int getUsec() const { return usec; }
};

inline void ConstructLogPrefix(std::ostream &s, const struct ::tm *tm_time, int32_t usc, const char *baseFilename,
                               int line, const char *podName, const char severity, const std::string azName)
{
    static auto pid = getpid();
    pid_t tid = syscall(__NR_gettid);
    s << std::setw(4) << 1900 + tm_time->tm_year << "-" << std::setw(2) << std::setfill('0') << 1 + tm_time->tm_mon
      << "-" << std::setw(2) << tm_time->tm_mday << "T" << std::setw(2) << tm_time->tm_hour << ':' << std::setw(2)
      << tm_time->tm_min << ':' << std::setw(2) << tm_time->tm_sec << "." << std::setw(6) << usc << " | " << severity
      << " | " << baseFilename << ':' << line << " | " << podName << " | " << pid << ":" << tid << " | "
      << Trace::Instance().GetTraceID() << " | " << azName << " | ";
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LOG_TIME_H
