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
#include "datasystem/utils/kv_client_config.h"

#include <sstream>
#include <vector>

#include "re2/re2.h"

#include "datasystem/common/util/validator.h"

namespace datasystem {
namespace {
constexpr int MIN_LOG_SEVERITY = 0;
constexpr int MAX_LOG_SEVERITY = 3;
constexpr int MIN_VLOG_LEVEL = 0;
constexpr int MAX_VLOG_LEVEL = 10;
constexpr int MIN_MAX_LOG_SIZE_MB = 1;
constexpr int MAX_MAX_LOG_SIZE_MB = 4095;
constexpr uint32_t MAX_LOG_FILE_NUM = 200000;
constexpr uint32_t MIN_LOG_ASYNC_QUEUE_SIZE = 256;
constexpr uint32_t MAX_LOG_ASYNC_QUEUE_SIZE = 1048576;
constexpr int32_t MIN_ZMQ_CLIENT_IO_CONTEXT = 1;
constexpr int32_t MAX_ZMQ_CLIENT_IO_CONTEXT = 128;
constexpr int32_t MIN_ZMQ_CLIENT_IO_THREAD = 1;
constexpr int32_t MAX_ZMQ_CLIENT_IO_THREAD = 32;

std::string ConfigBoolToString(bool enable)
{
    return enable ? "true" : "false";
}

void AddError(std::vector<std::string> &errors, const std::string &name, const std::string &reason)
{
    errors.emplace_back(name + ": " + reason);
}

std::string JoinErrors(const std::vector<std::string> &errors)
{
    std::stringstream ss;
    for (size_t i = 0; i < errors.size(); ++i) {
        if (i > 0) {
            ss << "; ";
        }
        ss << errors[i];
    }
    return ss.str();
}

bool ValidateOptionalIntInRange(const std::unordered_map<std::string, std::string> &args, const std::string &key,
                                int minValue, int maxValue, std::string &errorReason)
{
    auto it = args.find(key);
    if (it == args.end()) {
        return true;
    }
    int value = 0;
    return Validator::ParseIntInRange(it->second, minValue, maxValue, value, errorReason);
}

bool ValidateOptionalUint32InRange(const std::unordered_map<std::string, std::string> &args, const std::string &key,
                                   uint32_t minValue, uint32_t maxValue, std::string &errorReason)
{
    auto it = args.find(key);
    if (it == args.end()) {
        return true;
    }
    uint32_t value = 0;
    return Validator::ParseUint32InRange(it->second, minValue, maxValue, value, errorReason);
}

bool ValidateStringArg(const std::unordered_map<std::string, std::string> &args, const std::string &key,
                       bool (*validator)(const char *, const std::string &))
{
    auto it = args.find(key);
    return it == args.end() || validator(key.c_str(), it->second);
}

bool ValidateNonEmptyArg(const std::unordered_map<std::string, std::string> &args, const std::string &key)
{
    auto it = args.find(key);
    return it == args.end() || !it->second.empty();
}

bool IsValidNonEmptyLogBaseName(const std::string &value)
{
    static const re2::RE2 pattern("^[a-zA-Z0-9_]*$");
    return re2::RE2::FullMatch(value, pattern);
}

bool ValidateAccessLogNameArg(const std::unordered_map<std::string, std::string> &args)
{
    auto it = args.find("client_access_log_filename");
    if (it == args.end() || it->second.empty()) {
        return true;
    }
    return IsValidNonEmptyLogBaseName(it->second);
}

void ValidateNonEmptyPathArg(const std::unordered_map<std::string, std::string> &args, const std::string &key,
                             const std::string &fieldName, std::vector<std::string> &errors)
{
    if (!ValidateNonEmptyArg(args, key)) {
        AddError(errors, fieldName, "must not be empty");
        return;
    }
    if (!ValidateStringArg(args, key, &Validator::ValidatePathString)) {
        AddError(errors, fieldName, "must be a valid path");
    }
}

void ValidateKvClientStringFields(const std::unordered_map<std::string, std::string> &args,
                                  std::vector<std::string> &errors)
{
    ValidateNonEmptyPathArg(args, "log_dir", "LogDir", errors);
    if (!ValidateNonEmptyArg(args, "log_filename")) {
        AddError(errors, "LogName", "must not be empty");
    } else if (!ValidateStringArg(args, "log_filename", &Validator::ValidateEligibleChar)) {
        AddError(errors, "LogName", "contains unsupported characters");
    }
    if (!ValidateAccessLogNameArg(args)) {
        AddError(errors, "AccessLogName", "must contain only a-z, A-Z, 0-9, and underscore");
    }
    ValidateNonEmptyPathArg(args, "monitor_config_file", "MonitorConfigPath", errors);
}

void ValidateKvClientNumericFields(const std::unordered_map<std::string, std::string> &args,
                                   std::vector<std::string> &errors)
{
    std::string reason;
    if (!ValidateOptionalIntInRange(args, "minloglevel", MIN_LOG_SEVERITY, MAX_LOG_SEVERITY, reason)) {
        AddError(errors, "MinLogLevel", reason);
    }
    if (!ValidateOptionalIntInRange(args, "v", MIN_VLOG_LEVEL, MAX_VLOG_LEVEL, reason)) {
        AddError(errors, "VLogLevel", reason);
    }
    if (!ValidateOptionalIntInRange(args, "stderrthreshold", MIN_LOG_SEVERITY, MAX_LOG_SEVERITY, reason)) {
        AddError(errors, "StderrThreshold", reason);
    }
    if (!ValidateOptionalIntInRange(args, "max_log_size", MIN_MAX_LOG_SIZE_MB, MAX_MAX_LOG_SIZE_MB, reason)) {
        AddError(errors, "MaxLogSize", reason);
    }
    if (!ValidateOptionalUint32InRange(args, "max_log_file_num", 0, MAX_LOG_FILE_NUM, reason)) {
        AddError(errors, "MaxLogFileNum", reason);
    }
    if (!ValidateOptionalUint32InRange(args, "log_async_queue_size", MIN_LOG_ASYNC_QUEUE_SIZE,
                                       MAX_LOG_ASYNC_QUEUE_SIZE, reason)) {
        AddError(errors, "LogAsyncQueueSize", reason);
    }
    if (!ValidateOptionalIntInRange(args, "zmq_client_io_context", MIN_ZMQ_CLIENT_IO_CONTEXT,
                                    MAX_ZMQ_CLIENT_IO_CONTEXT, reason)) {
        AddError(errors, "ZmqClientIoContext", reason);
    }
    if (!ValidateOptionalIntInRange(args, "zmq_client_io_thread", MIN_ZMQ_CLIENT_IO_THREAD, MAX_ZMQ_CLIENT_IO_THREAD,
                                    reason)) {
        AddError(errors, "ZmqClientIoThread", reason);
    }
}

void CollectKvClientBuildErrors(const std::unordered_map<std::string, std::string> &args,
                                std::vector<std::string> &errors)
{
    ValidateKvClientStringFields(args, errors);
    ValidateKvClientNumericFields(args, errors);
}
}  // namespace

KVClientConfig::Builder &KVClientConfig::Builder::LogDir(const std::string &path)
{
    args_["log_dir"] = path;
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogName(const std::string &name)
{
    args_["log_filename"] = name;
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogWithoutPid(bool enable)
{
    args_["client_log_without_pid"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::AccessLogName(const std::string &name)
{
    args_["client_access_log_filename"] = name;
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::MinLogLevel(int level)
{
    args_["minloglevel"] = std::to_string(level);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::VLogLevel(int level)
{
    args_["v"] = std::to_string(level);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::StderrThreshold(int level)
{
    args_["stderrthreshold"] = std::to_string(level);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::MaxLogSize(int size)
{
    args_["max_log_size"] = std::to_string(size);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::MaxLogFileNum(uint32_t num)
{
    args_["max_log_file_num"] = std::to_string(num);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogCompress(bool enable)
{
    args_["log_compress"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogRetentionDay(uint32_t days)
{
    args_["log_retention_day"] = std::to_string(days);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogToStderr(bool enable)
{
    args_["logtostderr"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::AlsoLogToStderr(bool enable)
{
    args_["alsologtostderr"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogOnlyWriteInfoFile(bool enable)
{
    args_["log_only_write_info_file"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogAsyncEnable(bool enable)
{
    args_["log_async"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogAsyncQueueSize(uint32_t size)
{
    args_["log_async_queue_size"] = std::to_string(size);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::LogMonitorEnable(bool enable)
{
    args_["log_monitor"] = ConfigBoolToString(enable);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::MonitorConfigPath(const std::string &path)
{
    args_["monitor_config_file"] = path;
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::ZmqClientIoContext(int32_t threads)
{
    args_["zmq_client_io_context"] = std::to_string(threads);
    return *this;
}

KVClientConfig::Builder &KVClientConfig::Builder::ZmqClientIoThread(int32_t threads)
{
    args_["zmq_client_io_thread"] = std::to_string(threads);
    return *this;
}

Status KVClientConfig::Builder::Build(KVClientConfig &config) const
{
    std::vector<std::string> errors;
    CollectKvClientBuildErrors(args_, errors);
    if (!errors.empty()) {
        return Status(K_INVALID, JoinErrors(errors));
    }
    config.args_ = args_;
    return Status::OK();
}
}  // namespace datasystem
