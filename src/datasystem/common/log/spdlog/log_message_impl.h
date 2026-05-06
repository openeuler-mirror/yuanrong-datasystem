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
 * Description: Log message.
 */
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_LOG_MESSAGE_IMPL_H
#define DATASYSTEM_COMMON_LOG_SPDLOG_LOG_MESSAGE_IMPL_H

#include <iostream>
#include <streambuf>
#include <string>

#include "datasystem/common/log/spdlog/log_param.h"
#include "datasystem/common/log/spdlog/log_rate_limiter.h"
#include "datasystem/common/log/spdlog/logger_provider.h"

namespace datasystem {

extern bool g_ProviderAlive;

class LogStreamBuf : public std::streambuf {
public:
    LogStreamBuf(char *buf, int len);

    int_type overflow(int_type ch) override;

    size_t pcount() const;

    char *pbase() const;
};

class LogMessageImpl {
public:
    LogMessageImpl(LogSeverity logSeverity, const char *file, int line);

    ~LogMessageImpl();

    /**
     * @brief Return the output stream for log content.
     */
    std::ostream &Stream();

    static void SetPodName(const std::string &podName)
    {
        podName_ = podName;
    }

private:
    /**
     * @brief Initialize the logger and log prefix.
     */
    void Init();

    /**
     * @brief Flush the log buffer content.
     */
    void Flush();

    /**
     * @brief Output log content to the spdlog sink (file and stderr).
     */
    void ToSpdlog();

    /**
     * @brief Output log content to standard error stream (stderr).
     */
    void ToStderr();

    std::shared_ptr<ds_spdlog::logger> logger_;
    ds_spdlog::level::level_enum level_;
    ds_spdlog::source_loc sourceLoc_;
    static std::string podName_;
    LogStreamBuf streamBuf_;
    std::ostream logStream_;
    size_t msgSize_;
    bool skip_ = false;  // Request-level log sampling: dropped by limiter
};

}  // namespace datasystem

#endif
