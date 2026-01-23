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
 * Description: Log parameter.
 */
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_LOG_PARAM_H
#define DATASYSTEM_COMMON_LOG_SPDLOG_LOG_PARAM_H

#include <cstdint>
#include <string>
#include <vector>

namespace datasystem {

namespace log_param {
// GlobalLogParam
constexpr int DEFAULT_LOG_BUF_SECONDS = 10;               // 10s
constexpr uint32_t DEFAULT_ASYNC_THREAD_COUNT = 1;
constexpr uint32_t DEFAULT_MAX_ASYNC_QUEUE_SIZE = 65536;  // 64 * 1024

// LogParam
constexpr std::size_t DEFAULT_MAX_FILES = 5;
constexpr uint32_t DEFAULT_MAX_SIZE = 400;  // 400 MB
constexpr uint32_t SIZE_MEGA_BYTES = 1024 * 1024;  // 1 MB

const std::string DEFAULT_FILE_LOG_LEVEL = "INFO";
const std::string DEFAULT_LOG_DIR = "/.datasystem/logs";
const std::string DEFAULT_LOG_PATTERN =
    "%Y-%m-%dT%H:%M:%S.%6f | %^%L%$ | %s:%# | %v";  // %v = "pod_name | pid:tid | trace_id | cluster_name | message"
const std::string DEFAULT_STDERR_LOG_LEVEL = "FATAL";
}  // namespace log_param

struct LogParam {
    std::string logLevel = log_param::DEFAULT_FILE_LOG_LEVEL;
    std::string logDir = log_param::DEFAULT_LOG_DIR;
    std::string pattern = log_param::DEFAULT_LOG_PATTERN;
    std::vector<std::string> fileNamePatterns = {};
    bool alsoLog2Stderr = false;
    bool logAsync = false;
    uint32_t maxSize = log_param::DEFAULT_MAX_SIZE;
    std::size_t maxFiles = log_param::DEFAULT_MAX_FILES;
    std::string stderrLogLevel = log_param::DEFAULT_STDERR_LOG_LEVEL;
};

struct GlobalLogParam {
    int logBufSecs = log_param::DEFAULT_LOG_BUF_SECONDS;
    uint32_t maxAsyncQueueSize = log_param::DEFAULT_MAX_ASYNC_QUEUE_SIZE;
    uint32_t asyncThreadCount = log_param::DEFAULT_ASYNC_THREAD_COUNT;
};

enum LogSeverity { INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

}  // namespace datasystem

#endif
