/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Log severity.
 */
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_SEVERITY_H__
#define DATASYSTEM_COMMON_LOG_SPDLOG_SEVERITY_H__

#include <spdlog/common.h>
#include "datasystem/common/log/spdlog/log_param.h"

namespace datasystem {

constexpr int NUM_SEVERITIES = 4;

inline const char *const LogSeverityNames[NUM_SEVERITIES] = { "INFO", "WARNING", "ERROR", "FATAL" };

inline const char *GetLogSeverityName(const int &logLevel)
{
    if (logLevel < 0) {
        return "INFO";
    } else if (logLevel >= NUM_SEVERITIES) {
        return "FATAL";
    }

    return LogSeverityNames[logLevel];
}

inline static spdlog::level::level_enum ToSpdlogLevel(LogSeverity severity)
{
    return static_cast<spdlog::level::level_enum>(
        static_cast<int>(severity) + 2  // INFO(0) → info(2)
    );
}

}  // namespace datasystem

#endif