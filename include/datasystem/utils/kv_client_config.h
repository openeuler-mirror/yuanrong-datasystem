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
#ifndef DATASYSTEM_UTILS_KV_CLIENT_CONFIG_H
#define DATASYSTEM_UTILS_KV_CLIENT_CONFIG_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "datasystem/utils/status.h"

namespace datasystem {
/**
 * @brief KV client initialization config for logging, monitoring, and ZMQ flags.
 *
 * Build via Builder and pass to KVClient::Init. Does not cover ConnectOptions.
 * The first Init in a process freezes the config snapshot; per-field priority on that
 * first Init is: explicit Builder fields > environment variables > defaults. Later Init
 * calls reuse the frozen process-level settings and do not override
 * them. Later Init with config only checks fields explicitly present in that config;
 * missing fields, including an empty config, are treated as unspecified rather than as
 * a request to clear or compare against an empty snapshot.
 */
class __attribute((visibility("default"))) KVClientConfig {
public:
    KVClientConfig() = default;
    ~KVClientConfig() = default;

    /**
     * @brief Get configured arguments.
     * @return Map of argument name to string value.
     */
    const std::unordered_map<std::string, std::string> &GetArgs() const
    {
        return args_;
    }

    class __attribute((visibility("default"))) Builder {
    public:
        Builder() = default;
        ~Builder() = default;

        /**
         * @brief Directory to store log files.
         * @param[in] path Equivalent to flag log_dir.
         * @return Reference to self for chaining.
         */
        Builder &LogDir(const std::string &path);

        /**
         * @brief Client log file base name.
         * @param[in] name Equivalent to flag log_filename.
         * @return Reference to self for chaining.
         */
        Builder &LogName(const std::string &name);

        /**
         * @brief Enable client log file name without pid.
         * @param[in] enable Equivalent to client log without pid config.
         * @return Reference to self for chaining.
         */
        Builder &LogWithoutPid(bool enable);

        /**
         * @brief Client access log file base name.
         * @param[in] name Equivalent to client access log filename config.
         * @return Reference to self for chaining.
         */
        Builder &AccessLogName(const std::string &name);

        /**
         * @brief Minimum severity level to write to log file.
         * @param[in] level Equivalent to flag minloglevel.
         * @return Reference to self for chaining.
         */
        Builder &MinLogLevel(int level);

        /**
         * @brief Verbose log level (vlog).
         * @param[in] level Equivalent to flag v.
         * @return Reference to self for chaining.
         */
        Builder &VLogLevel(int level);

        /**
         * @brief Minimum severity level to write to stderr.
         * @param[in] level Equivalent to flag stderrthreshold.
         * @return Reference to self for chaining.
         */
        Builder &StderrThreshold(int level);

        /**
         * @brief Maximum size in MiB before log rotation.
         * @param[in] size Equivalent to flag max_log_size.
         * @return Reference to self for chaining.
         */
        Builder &MaxLogSize(int size);

        /**
         * @brief Maximum number of retained log files.
         * @param[in] num Equivalent to flag max_log_file_num.
         * @return Reference to self for chaining.
         */
        Builder &MaxLogFileNum(uint32_t num);

        /**
         * @brief Enable log compression.
         * @param[in] enable Equivalent to flag log_compress.
         * @return Reference to self for chaining.
         */
        Builder &LogCompress(bool enable);

        /**
         * @brief Retention days for log files.
         * @param[in] days Equivalent to flag log_retention_day.
         * @return Reference to self for chaining.
         */
        Builder &LogRetentionDay(uint32_t days);

        /**
         * @brief Write logs only to stderr.
         * @param[in] enable Equivalent to flag logtostderr.
         * @return Reference to self for chaining.
         */
        Builder &LogToStderr(bool enable);

        /**
         * @brief Also write logs to stderr.
         * @param[in] enable Equivalent to flag alsologtostderr.
         * @return Reference to self for chaining.
         */
        Builder &AlsoLogToStderr(bool enable);

        /**
         * @brief Only write info log file.
         * @param[in] enable Equivalent to flag log_only_write_info_file.
         * @return Reference to self for chaining.
         */
        Builder &LogOnlyWriteInfoFile(bool enable);

        /**
         * @brief Enable asynchronous logging.
         * @param[in] enable Equivalent to flag log_async.
         * @return Reference to self for chaining.
         */
        Builder &LogAsyncEnable(bool enable);

        /**
         * @brief Queue size for asynchronous logging.
         * @param[in] size Equivalent to flag log_async_queue_size.
         * @return Reference to self for chaining.
         */
        Builder &LogAsyncQueueSize(uint32_t size);

        /**
         * @brief Enable log monitor.
         * @param[in] enable Equivalent to flag log_monitor.
         * @return Reference to self for chaining.
         */
        Builder &LogMonitorEnable(bool enable);

        /**
         * @brief Log monitor config file path.
         * @param[in] path Equivalent to flag monitor_config_file. Empty path disables file monitoring.
         * @return Reference to self for chaining.
         */
        Builder &MonitorConfigPath(const std::string &path);

        /**
         * @brief Number of ZMQ client IO contexts.
         * @param[in] threads Equivalent to flag zmq_client_io_context.
         * @return Reference to self for chaining.
         */
        Builder &ZmqClientIoContext(int32_t threads);

        /**
         * @brief Number of ZMQ client IO threads.
         * @param[in] threads Equivalent to flag zmq_client_io_thread.
         * @return Reference to self for chaining.
         */
        Builder &ZmqClientIoThread(int32_t threads);

        /**
         * @brief Validate and build KVClientConfig.
         * @param[out] config Built config.
         * @return K_OK on success; the error code otherwise.
         */
        Status Build(KVClientConfig &config) const;

    private:
        std::unordered_map<std::string, std::string> args_;
    };

private:
    friend class Builder;
    std::unordered_map<std::string, std::string> args_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_UTILS_KV_CLIENT_CONFIG_H
