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
 * Description: Logger context.
 */
#ifndef DATASYSTEM_COMMON_LOG_SPDLOG_LOGGER_CONTEXT_H
#define DATASYSTEM_COMMON_LOG_SPDLOG_LOGGER_CONTEXT_H

#include <chrono>

#include <spdlog/spdlog.h>

#include "datasystem/common/log/spdlog/log_param.h"

namespace datasystem {

using DsLogger = std::shared_ptr<spdlog::logger>;

const std::string DS_LOGGER_NAME = "DsLogger";

class LoggerContext {
public:
    LoggerContext() = default;
    explicit LoggerContext(const GlobalLogParam &globalLogParam) noexcept;
    ~LoggerContext() = default;

    bool ForceFlush(std::chrono::microseconds timeout = (std::chrono::microseconds::max)()) const noexcept;

    DsLogger CreateLogger(const LogParam &logParam);

    DsLogger GetLogger(const std::string &loggerName) const noexcept;

    static DsLogger GetDefaultLogger() noexcept;

    void DropLogger(const std::string &loggerName) const noexcept;

private:
    GlobalLogParam globalLogParam_;
};

}  // namespace datasystem

#endif
