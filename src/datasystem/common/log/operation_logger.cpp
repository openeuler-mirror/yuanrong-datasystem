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
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log_time.h"
#include "datasystem/common/log/spdlog/log_param.h"
#include "datasystem/common/log/spdlog/provider.h"

DS_DECLARE_string(cluster_name);
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
constexpr char ROLE_CLIENT[] = "client";

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

void WriteLogToLogger(const std::shared_ptr<ds_spdlog::logger> &logger, const std::string &message,
                      const ds_spdlog::source_loc &sourceLoc)
{
    std::ostringstream out;
    AppendLogMessagePrefix(out, Provider::GetPodName(), FLAGS_cluster_name);
    out << message;
    logger->log(sourceLoc, ds_spdlog::level::info, out.str());
}

// Flags the client process actually touches at runtime.
const std::unordered_set<std::string> &ClientUsedFlagNames()
{
    static const std::unordered_set<std::string> kSet = {
        // ---- ZMQ client transport (ZmqStubConnMgrImpl, reached when use_brpc=false) ----
        "zmq_client_io_context",
        "zmq_client_io_thread",
        // ---- logging isClient_ startup branch (InitClientConfig / InitClientAdvancedConfig) ----
        "log_dir", "log_filename", "max_log_size", "max_log_file_num", "log_compress", "v",
        "minloglevel", "log_retention_day", "logtostderr", "alsologtostderr", "stderrthreshold",
        "log_async", "log_async_queue_size", "log_monitor", "log_only_write_info_file",
        "logbufsecs", "cluster_name",
        // ---- log sampling rates (InitLogSampler on client path) ----
        "request_sample_rate", "access_sample_rate", "diagnostic_sample_rate",
        // ---- client config file monitor ----
        "monitor_config_file",
        // ---- client slow-log thresholds (GetClientLatencyTraceConfig on client path) ----
        "client_slow_log_process_slower_than", "client_slow_log_rpc_slower_than",
        // ---- RPC backend selection (every client RPC / startup path) ----
        "use_brpc",
        // ---- URMA data-plane (client UB transport + failover) ----
        "enable_urma", "urma_send_jetty_lane_pool_size",
        "urma_failover_success_rate_ratio", "urma_failover_min_sample_count",
        // ---- conditionally reached by client ----
        "encrypt_kit", "shared_memory_distribution_policy",
        // read via etcd_keep_alive on the client etcd path (router_client / service_discovery)
        "heartbeat_interval_ms",
        // read via arena.cpp on the embedded, client + URMA arena registration path
        "arena_per_tenant", "shared_disk_arena_per_tenant",
    };
    return kSet;
}

std::string FlagNameFromLine(const std::string &line)
{
    if (!IsDoubleDashGflagLine(line)) {
        return "";
    }
    const auto eqPos = line.find('=');
    if (eqPos == std::string::npos) {
        return "";
    }
    return line.substr(K_GFLAG_NAME_PREFIX_LEN, eqPos - K_GFLAG_NAME_PREFIX_LEN);
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
        WriteLogToLogger(logger, "OPERATION_STOP: role=" + role,
                         ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ });
        logger->flush();
        ds_spdlog::drop(OPERATION_LOGGER_NAME);
    }
}

void OperationLogger::LogOperationStart()
{
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, false, "",
        [](const std::string &role) { return "OPERATION_START: role=" + role; });
}

void OperationLogger::LogOperationStop()
{
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, false, "",
        [](const std::string &role) { return "OPERATION_STOP: role=" + role; });
}

void OperationLogger::LogConfigInit(const std::string &flagsSnapshot)
{
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, false, "",
        [&flagsSnapshot](const std::string &role) {
            return "CONFIG_INIT: " + MaskSensitiveFlagsSnapshot(FilterByRole(flagsSnapshot, role));
        });
}

void OperationLogger::LogConfigInitFailed(const std::string &detail)
{
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, false, "",
        [&detail](const std::string &role) {
            (void)role;
            return "CONFIG_INIT_FAILED: " + detail;
        });
}

void OperationLogger::LogConfigChanged(const std::string &name, const std::string &oldVal, const std::string &newVal)
{
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, true, name,
        [&name, &oldVal, &newVal](const std::string &role) {
            (void)role;
            return "CONFIG_CHANGED: " + name + "=" + MaskSensitiveFlagValue(name, oldVal) + " --> "
                + MaskSensitiveFlagValue(name, newVal);
        });
}

void OperationLogger::LogConfigFailed(const std::string &name, const std::string &reason)
{
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, true, name,
        [&name, &reason](const std::string &role) {
            (void)role;
            return "CONFIG_FAILED: flag '" + name + "' " + reason;
        });
}

void OperationLogger::LogConfigApiFailed(const std::string &operation, const std::string &reason)
{
    // Operation-level failures (name is an operation tag, not a gflag) are always recorded,
    // regardless of role.
    WriteLogWithRole(ds_spdlog::source_loc{ __FILE__, __LINE__, __FUNCTION__ }, false, "",
        [&operation, &reason](const std::string &role) {
            (void)role;
            return "CONFIG_FAILED: " + operation + " " + reason;
        });
}

std::string OperationLogger::FilterByRole(const std::string &flagsSnapshot, const std::string &role)
{
    // Worker/master: log the full snapshot (only sensitive values are masked downstream).
    // Client: keep only flags the client process actually reaches (allow-list), so
    // worker/master-only flags registered via the common lib do not leak into the client log.
    if (role != ROLE_CLIENT) {
        return flagsSnapshot;
    }
    const auto &allow = ClientUsedFlagNames();
    std::ostringstream out;
    std::istringstream in(flagsSnapshot);
    std::string line;
    while (std::getline(in, line)) {
        TrimTrailingCarriageReturn(line);
        if (line.empty()) {
            continue;
        }
        const std::string name = FlagNameFromLine(line);
        if (!name.empty() && allow.count(name) == 0) {
            continue;
        }
        out << line << '\n';
    }
    return out.str();
}

bool OperationLogger::ShouldRecordChange(const std::string &flagName, const std::string &role)
{
    // Worker/master: record every dynamic change. Client: record only changes to flags the
    // client process actually reaches, so a worker-only flag mutated via the shared registry
    // does not pollute the client operation log.
    if (role == ROLE_CLIENT) {
        return ClientUsedFlagNames().count(flagName) > 0;
    }
    return true;
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

void OperationLogger::WriteLogWithRole(const ds_spdlog::source_loc &sourceLoc, bool gateByRole,
                                       const std::string &flagName,
                                       std::function<std::string(const std::string &role)> buildMessage)
{
    std::string role;
    std::shared_ptr<ds_spdlog::logger> logger;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        role = role_;
        logger = logger_;
    }
    if (logger == nullptr) {
        return;
    }
    if (gateByRole && !ShouldRecordChange(flagName, role)) {
        return;
    }
    WriteLogToLogger(logger, buildMessage(role), sourceLoc);
}

}  // namespace datasystem
