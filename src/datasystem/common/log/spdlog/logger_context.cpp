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
#include "datasystem/common/log/spdlog/logger_context.h"

#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <limits.h>
#include <unistd.h>

#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include "datasystem/common/log/spdlog/log_severity.h"

namespace datasystem {

constexpr auto LOG_LEVEL_OFF = spdlog::level::off;

static std::vector<std::string> GetLogFiles(const LogParam &logParam)
{
    std::vector<std::string> logFiles;
    char resolvedPath[PATH_MAX + 1] = { 0 };

    if (realpath(logParam.logDir.c_str(), resolvedPath) == nullptr) {
        return logFiles;
    }

    logFiles.reserve(logParam.fileNamePatterns.size());
    std::string absoluteLogDir(resolvedPath);

    for (const auto &pattern : logParam.fileNamePatterns) {
        logFiles.emplace_back(absoluteLogDir + "/" + pattern + ".log");
    }

    return logFiles;
}

static void FlushLogger(DsLogger logger)
{
    if (logger) {
        logger->flush();
    }
}

static const std::map<std::string, spdlog::level::level_enum> &GetLogLevelMap()
{
    static const std::map<std::string, spdlog::level::level_enum> LOG_LEVEL_MAP = { { "INFO", spdlog::level::info },
                                                                                    { "WARNING", spdlog::level::warn },
                                                                                    { "ERROR", spdlog::level::err },
                                                                                    { "FATAL",
                                                                                      spdlog::level::critical } };
    return LOG_LEVEL_MAP;
}

static spdlog::level::level_enum GetLogLevel(const std::string &level)
{
    auto iter = GetLogLevelMap().find(level);
    return iter == GetLogLevelMap().end() ? spdlog::level::info : iter->second;
}

LoggerContext::LoggerContext(const GlobalLogParam &globalLogParam) noexcept : globalLogParam_(globalLogParam)
{
    spdlog::drop_all();
    if (!spdlog::thread_pool()) {
        spdlog::init_thread_pool(static_cast<size_t>(globalLogParam_.maxAsyncQueueSize),
                                 static_cast<size_t>(globalLogParam_.asyncThreadCount));
    }
    spdlog::flush_every(std::chrono::seconds(globalLogParam_.logBufSecs));
}

DsLogger LoggerContext::CreateLogger(const LogParam &logParam)
{
    try {
        std::vector<spdlog::sink_ptr> sinks{};
        std::vector<std::string> logFiles = GetLogFiles(logParam);
        for (size_t i = 0; i < logFiles.size(); ++i) {
            auto rotatingSink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                logFiles[i], logParam.maxSize * log_param::SIZE_MEGA_BYTES, logParam.maxFiles);
            const auto log2FileLevel = ToSpdlogLevel(LogSeverity(i % (NUM_SEVERITIES - 1)));
            rotatingSink->set_level(log2FileLevel);
            (void)sinks.emplace_back(rotatingSink);
        }

        const auto stderrLogLevel = (logParam.stderrLogLevel != "FATAL")
                                        ? GetLogLevel(logParam.stderrLogLevel)
                                        : (logParam.alsoLog2Stderr ? GetLogLevel(logParam.logLevel) : LOG_LEVEL_OFF);
        if (stderrLogLevel != LOG_LEVEL_OFF) {
            auto errSink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
            errSink->set_level(stderrLogLevel);
            sinks.emplace_back(errSink);
        }

        std::shared_ptr<spdlog::logger> logger;
        if (logParam.logAsync) {
            logger = std::make_shared<spdlog::async_logger>(DS_LOGGER_NAME, sinks.begin(), sinks.end(),
                                                            spdlog::thread_pool(),
                                                            spdlog::async_overflow_policy::overrun_oldest);
        } else {
            logger = std::make_shared<spdlog::logger>(DS_LOGGER_NAME, sinks.begin(), sinks.end());
        }

        spdlog::initialize_logger(logger);
        logger->set_pattern(logParam.pattern, spdlog::pattern_time_type::utc);

        const auto logLevel = GetLogLevel(logParam.logLevel);
        logger->set_level(logLevel);
        if (!logParam.logAsync) {
            logger->flush_on(logLevel);
        }

        return logger;
    } catch (std::exception &e) {
        std::cerr << "Failed to init logger, error: " << e.what() << std::endl;
        return nullptr;
    }
}

DsLogger LoggerContext::GetLogger(const std::string &loggerName) const noexcept
{
    return spdlog::get(loggerName);
}

DsLogger LoggerContext::GetDefaultLogger() noexcept
{
    return spdlog::default_logger();
}

void LoggerContext::DropLogger(const std::string &loggerName) const noexcept
{
    spdlog::drop(loggerName);
}

bool LoggerContext::ForceFlush(std::chrono::microseconds) const noexcept
{
    spdlog::apply_all(FlushLogger);
    return true;
}

}  // namespace datasystem