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
 * Description: Operation audit logger for config lifecycle events.
 */
#include "datasystem/common/log/operation_logger.h"

#include <sstream>
#include <unordered_set>

#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/spdlog/log_param.h"

DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);
DS_DECLARE_bool(log_async);
DS_DECLARE_uint32(max_log_size);

namespace datasystem {
namespace {
constexpr char OPERATION_LOGGER_NAME[] = "ds_operation_logger";
constexpr uint32_t HIGHEST_MAX_LOG_SIZE = 4096;
constexpr std::size_t HIGHEST_SPDLOG_MAX_FILE_NUM = 200000;
constexpr std::size_t K_GFLAG_NAME_PREFIX_LEN = 2;

const std::unordered_set<std::string> kSensitiveFlagNames = {
    "system_access_key", "system_secret_key", "system_data_key", "sc_encrypt_secret_key",
    "tenant_access_key", "tenant_secret_key", "obs_access_key", "obs_secret_key",
    "etcd_password", "etcd_key", "yuanrong_iam_passphrase", "yuanrong_iam_key",
};

uint32_t GetMaxLogSizeMb()
{
    return (FLAGS_max_log_size > 0 && FLAGS_max_log_size < HIGHEST_MAX_LOG_SIZE) ? FLAGS_max_log_size : 1;
}

void TrimTrailingCarriageReturn(std::string &line)
{
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
}

bool IsDoubleDashGflagLine(const std::string &line)
{
    return line.size() >= K_GFLAG_NAME_PREFIX_LEN && line[0] == '-' && line[1] == '-';
}

std::string FormatMaskedFlagLine(const std::string &line)
{
    if (!IsDoubleDashGflagLine(line)) {
        return line + '\n';
    }
    const auto eqPos = line.find('=');
    if (eqPos == std::string::npos) {
        return line + '\n';
    }
    const std::string flagName = line.substr(K_GFLAG_NAME_PREFIX_LEN, eqPos - K_GFLAG_NAME_PREFIX_LEN);
    if (kSensitiveFlagNames.count(flagName) == 0) {
        return line + '\n';
    }
    return "--" + flagName + "=xxx\n";
}

std::string MaskSensitiveFlagValue(const std::string &flagName, const std::string &value)
{
    if (kSensitiveFlagNames.count(flagName) == 0) {
        return value;
    }
    return "xxx";
}

std::string MaskSensitiveFlagsSnapshot(const std::string &snapshot)
{
    std::ostringstream out;
    std::istringstream in(snapshot);
    std::string line;
    while (std::getline(in, line)) {
        TrimTrailingCarriageReturn(line);
        if (line.empty()) {
            continue;
        }
        out << FormatMaskedFlagLine(line);
    }
    return out.str();
}
}  // namespace

OperationLogger &OperationLogger::Instance()
{
    // Keep this process-lifetime singleton available during Logging static destruction.
    static auto *instance = new OperationLogger();
    return *instance;
}

bool OperationLogger::Init(const std::string &role)
{
    bool shouldLogStart = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (logger_ != nullptr) {
            return true;
        }
        if (FLAGS_log_dir.empty() || FLAGS_log_filename.empty()) {
            return false;
        }
        role_ = role;
        logPath_ = FLAGS_log_dir + "/" + FLAGS_log_filename + "_operation.log";
        try {
            const uint32_t maxLogSizeMb = GetMaxLogSizeMb();
            auto sink = std::make_shared<ds_spdlog::sinks::rotating_file_sink_mt>(
                logPath_, maxLogSizeMb * log_param::SIZE_MEGA_BYTES, HIGHEST_SPDLOG_MAX_FILE_NUM);
            std::shared_ptr<ds_spdlog::logger> logger;
            if (FLAGS_log_async && ds_spdlog::thread_pool()) {
                logger = std::make_shared<ds_spdlog::async_logger>(OPERATION_LOGGER_NAME, sink,
                                                                     ds_spdlog::thread_pool(),
                                                                     ds_spdlog::async_overflow_policy::overrun_oldest);
            } else {
                logger = std::make_shared<ds_spdlog::logger>(OPERATION_LOGGER_NAME, sink);
            }
            ds_spdlog::initialize_logger(logger);
            logger->set_pattern(log_param::DEFAULT_LOG_PATTERN, ds_spdlog::pattern_time_type::local);
            logger->set_level(ds_spdlog::level::info);
            if (!FLAGS_log_async) {
                logger->flush_on(ds_spdlog::level::info);
            }
            logger_ = logger;
            shouldLogStart = true;
        } catch (const std::exception &) {
            logger_.reset();
            return false;
        }
    }
    if (shouldLogStart) {
        LogOperationStart();
    }
    return true;
}

void OperationLogger::Shutdown()
{
    std::shared_ptr<ds_spdlog::logger> logger;
    std::string role;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        logger = logger_;
        role = role_;
        logger_.reset();
        logPath_.clear();
        role_.clear();
    }
    if (logger != nullptr) {
        logger->info("OPERATION_STOP: role=" + role);
        logger->flush();
        ds_spdlog::drop(OPERATION_LOGGER_NAME);
    }
}

void OperationLogger::LogOperationStart()
{
    WriteLog("OPERATION_START: role=" + role_);
}

void OperationLogger::LogOperationStop()
{
    WriteLog("OPERATION_STOP: role=" + role_);
}

void OperationLogger::LogConfigInit(const std::string &flagsSnapshot)
{
    WriteLog("CONFIG_INIT: " + MaskSensitiveFlagsSnapshot(flagsSnapshot));
}

void OperationLogger::LogConfigInitFailed(const std::string &detail)
{
    WriteLog("CONFIG_INIT_FAILED: " + detail);
}

void OperationLogger::LogConfigChanged(const std::string &name, const std::string &oldVal, const std::string &newVal)
{
    WriteLog("CONFIG_CHANGED: " + name + "=" + MaskSensitiveFlagValue(name, oldVal) + " --> "
             + MaskSensitiveFlagValue(name, newVal));
}

void OperationLogger::LogConfigFailed(const std::string &name, const std::string &reason)
{
    WriteLog("CONFIG_FAILED: flag '" + name + "' " + reason);
}

std::string OperationLogger::OperationLogPath() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!logPath_.empty()) {
        return logPath_;
    }
    if (FLAGS_log_dir.empty() || FLAGS_log_filename.empty()) {
        return "";
    }
    return FLAGS_log_dir + "/" + FLAGS_log_filename + "_operation.log";
}

void OperationLogger::WriteLog(const std::string &message)
{
    std::shared_ptr<ds_spdlog::logger> logger;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        logger = logger_;
    }
    if (logger == nullptr) {
        return;
    }
    logger->info(message);
}

}  // namespace datasystem
