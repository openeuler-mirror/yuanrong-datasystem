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
 * Description: Logging util for spdlog.
 */
#include "datasystem/common/log/logging.h"

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <vector>

#include <fcntl.h>

#include <re2/re2.h>

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_manager.h"
#include "datasystem/common/log/log_time.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/spdlog/log_severity.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"

constexpr uint32_t DEFAULT_CLIENT_LOG_ASYNC_QUEUE_SIZE = 1024;
constexpr uint32_t DEFAULT_CLIENT_MAX_LOG_SIZE_MB = 100;
constexpr int DEFAULT_LOG_BUF_SECS = 10;
constexpr uint32_t DEFAULT_LOG_LEVEL = 0;
constexpr uint32_t DEFAULT_LOG_RETENTION_DAY = 0;
constexpr uint32_t DEFAULT_MAX_LOG_FILE_NUM = 5;
constexpr uint32_t HIGHEST_MAX_LOG_SIZE = 4096;
constexpr bool DEFAULT_ALSO_LOG_TO_STDERR = false;
constexpr bool DEFAULT_CLIENT_LOG_MONITOR = true;
constexpr bool DEFAULT_LOG_ASYNC_FLAG = true;
constexpr bool DEFAULT_LOG_COMPRESS = true;
constexpr bool DEFAULT_LOG_TO_STDERR = false;
constexpr int DEFAULT_STDERRTHRESHOLD = LogSeverity::ERROR;  // By default, errors always log to stderr.
constexpr int HIGHEST_STDERRTHRESHOLD = LogSeverity::FATAL;  // The errors log won't print to stderr.
constexpr std::size_t HIGHEST_SPDLOG_MAX_FILE_NUM = 200000;  // Maximum allowed by spdlog's rotating_file_sink.

DS_DEFINE_string(log_filename, "",
                 "Prefix of log filename, default is program invocation short name. Use standard characters only.");
DS_DEFINE_bool(log_async, DEFAULT_LOG_ASYNC_FLAG, "Enable asynchronous writing to log files.");
DS_DEFINE_uint32(
    max_log_file_num, DEFAULT_MAX_LOG_FILE_NUM,
    "Maximum number of log files to retain per severity level. And every log file size is limited by max_log_size."
    "If set to 0, the log num is not limited.");
DS_DEFINE_bool(log_compress, DEFAULT_LOG_COMPRESS,
               "Compress old log files in .gz format. This parameter takes effect only when "
               "the size of the generated log is greater than max log size.");
DS_DEFINE_uint32(log_retention_day, DEFAULT_LOG_RETENTION_DAY,
                 "If log_retention_day is greater than 0, any log file from your project whose last modified time is "
                 "greater than log_retention_day days will be "
                 "unlink()ed. If log_retention_day is equal 0, will not unlink log file by time.");
DS_DEFINE_int32(logbufsecs, DEFAULT_LOG_BUF_SECS, "Buffer log messages for at most this many seconds.");
DS_DEFINE_string(log_dir, GetStringFromEnv("GOOGLE_LOG_DIR", ""),
                 "If specified, logfiles are written into this directory instead of the default logging directory.");
DS_DEFINE_int32(logfile_mode, 0640, "Log file mode/permissions.");
DS_DEFINE_uint32(max_log_size, DEFAULT_MAX_LOG_SIZE_MB,
                 "approx. maximum log file size (in MB). A value of 0 will be silently overridden to 1.");
DS_DEFINE_bool(logtostderr, GetBoolFromEnv("GOOGLE_LOGTOSTDERR", false),
               "log messages go to stderr instead of logfiles.  This flag obsoletes");
DS_DEFINE_bool(alsologtostderr, GetBoolFromEnv("GOOGLE_ALSOLOGTOSTDERR", false),
               "log messages go to stderr in addition to logfiles");
DS_DEFINE_uint32(stderrthreshold, 2,
                 "log messages at or above this level are copied to stderr in "
                 "addition to logfiles.  This flag obsoletes --alsologtostderr.");
DS_DEFINE_int32(minloglevel, 0, "Messages logged at a lower level than this don't actually get logged anywhere.");
DS_DEFINE_uint32(log_async_queue_size, DEFAULT_LOG_ASYNC_QUEUE_SIZE, "Size of async logger's message queue.");
DS_DEFINE_validator(log_filename, &Validator::ValidateEligibleChar);

DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(cluster_name);

using namespace std::chrono;

namespace datasystem {

std::string Logging::podName_ = Provider::GetPodName();

Logging *Logging::GetInstance()
{
    static Logging instance;
    return &instance;
}

bool Logging::CreateLogDir()
{
    if (FLAGS_log_dir.empty()) {
        std::string homeDir;
        Status rc = Uri::GetHomeDir(homeDir);
        if (rc.IsError()) {
            LOG(ERROR) << "Get user's home dir failed, as " << rc.ToString();
            return false;
        }

        FLAGS_log_dir = homeDir + "/.datasystem/logs";
    }

    if (!FileExist(FLAGS_log_dir, W_OK | R_OK | X_OK)) {
        const int permission = 0700;  // Minimum permission for log dir.
        Status rc = CreateDir(FLAGS_log_dir, true, permission);
        if (rc.IsError()) {
            LOG(ERROR) << rc.ToString();
            return false;
        }
    }

    return true;
}

uint32_t Logging::MaxLogSize()
{
    return (FLAGS_max_log_size > 0 && FLAGS_max_log_size < HIGHEST_MAX_LOG_SIZE ? FLAGS_max_log_size : 1);
}

AccessRecorderManager *Logging::AccessRecorderManagerInstance()
{
    auto *instance = GetInstance();
    if (!instance->accessRecorderManagerInstance_) {
        instance->accessRecorderManagerInstance_ = std::make_unique<AccessRecorderManager>();
    }

    return instance->accessRecorderManagerInstance_.get();
}

bool Logging::InitLoggingWrapper(uint32_t logProcessInterval)
{
    if (!CreateLogDir()) {
        return false;
    }

    if (FLAGS_log_filename.empty()) {
        // if flag log_filename is empty, it may be caused by two reasons as follows:
        // - 1. flags is not initialized yet, google::ProgramInvocationShortName() is 'UNKNOWN'.
        // - 2. flag log_filename is empty, we just set it as google::ProgramInvocationShortName().
        std::string programName = ProgramInvocationShortName();
        CHECK_STRNE(programName.c_str(), "UNKNOWN") << ": must initialize flags before logging";
        FLAGS_log_filename = std::move(programName);
    }

    std::vector<std::string> fileNamePatterns = { (FLAGS_log_filename + ".INFO").c_str(),
                                                  (FLAGS_log_filename + ".WARNING").c_str(),
                                                  (FLAGS_log_filename + ".ERROR").c_str() };
    LogParam loggerParam;
    loggerParam.fileNamePatterns = fileNamePatterns;
    loggerParam.logDir = FLAGS_log_dir;
    loggerParam.alsoLog2Stderr = FLAGS_alsologtostderr;
    loggerParam.stderrLogLevel = GetLogSeverityName(FLAGS_stderrthreshold);
    loggerParam.logLevel = GetLogSeverityName(FLAGS_minloglevel);
    loggerParam.logAsync = FLAGS_log_async;
    loggerParam.maxSize = MaxLogSize();
    // Disable spdlog's auto-deletion of old log files, only use its file splitting (by size/time).
    // Log rotation is managed externally by the log manager.
    loggerParam.maxFiles = HIGHEST_SPDLOG_MAX_FILE_NUM;  // Set max log file limit to spdlog's MaxFiles.

    // Create LoggerProvider
    GlobalLogParam globalLogParam;
    globalLogParam.logBufSecs = FLAGS_logbufsecs;
    globalLogParam.maxAsyncQueueSize = FLAGS_log_async_queue_size;
    globalLogParam.asyncThreadCount = 2;  // 2 thread
    auto lp = std::make_shared<LoggerProvider>(globalLogParam);

    // Set LoggerProvider to ensure global singleton
    Provider::Instance().SetLoggerProvider(lp);

    auto logger = lp->InitDsLogger(loggerParam);
    if (logger == nullptr) {
        return false;
    }

    if (loggerParam.logAsync) {
        LOG(INFO) << "Async logging buffer duration: " << globalLogParam.logBufSecs << " s";
    }

    // Start log manager.
    logManager_ = std::make_unique<LogManager>(logProcessInterval);
    auto status = logManager_->Start();
    if (status.IsError()) {
        LOG(ERROR) << "Failed to start log manager:" << status.ToString() << std::endl;
        return false;
    }

    accessRecorderManagerInstance_ = std::make_unique<AccessRecorderManager>();
    auto rc = accessRecorderManagerInstance_->Init(isClient_);
    if (rc.IsError()) {
        return false;
    }

    return true;
}

void Logging::ShutdownLoggingWrapper()
{
    // Log has been shutdown, just return quiet.
    if (!IsLoggingInitialized()) {
        return;
    }

    // Stop log manager.
    logManager_->Stop();
    if (isClient_ && !FLAGS_logtostderr) {
        auto *redirectStdErr = freopen("/dev/null", "w", stderr);
        if (redirectStdErr == nullptr) {
            LOG(WARNING) << "Failed to redirect stderr to /dev/null, errno: " << errno
                         << ", error: " << strerror(errno);
        }
    }

    auto lp = Provider::Instance().GetLoggerProvider();
    if (lp) {
        lp->DropDsLogger();
    }

    Provider::Instance().SetLoggerProvider(nullptr);
    SetLoggingInitialized(false);
}

void Logging::InitClientBasicConfig()
{
    std::string errMsg;
    if (FLAGS_log_dir.empty()) {
        auto val = GetStringFromEnv(LOG_DIR_ENV.c_str(), "");
        if (Validator::ValidatePathString("log_dir", val)) {
            FLAGS_log_dir = val;
        }
    }

    if (FLAGS_max_log_size == DEFAULT_MAX_LOG_SIZE_MB) {
        FLAGS_max_log_size = GetUint32FromEnv(MAX_LOG_SIZE_ENV.c_str(), DEFAULT_CLIENT_MAX_LOG_SIZE_MB);
    }

    if (FLAGS_max_log_file_num == DEFAULT_MAX_LOG_FILE_NUM) {
        FLAGS_max_log_file_num = GetUint32FromEnv(MAX_LOG_FILE_NUM_ENV.c_str(), DEFAULT_MAX_LOG_FILE_NUM);
    }

    if (FLAGS_log_compress == DEFAULT_LOG_COMPRESS) {
        FLAGS_log_compress = GetBoolFromEnv(LOG_COMPRESS_ENV.c_str(), DEFAULT_LOG_COMPRESS);
    }

    if (FLAGS_v == DEFAULT_LOG_LEVEL) {
        uint32_t value = GetUint32FromEnv(LOG_V.c_str(), DEFAULT_LOG_LEVEL);
        FLAGS_v = static_cast<int32_t>(value > INT32_MAX ? 0 : value);
    }

    if (FLAGS_minloglevel == DEFAULT_LOG_LEVEL) {
        uint32_t value = GetUint32FromEnv(MIN_LOG_LEVEL.c_str(), DEFAULT_LOG_LEVEL);
        FLAGS_minloglevel = static_cast<int32_t>(value > INT32_MAX ? 0 : value);
    }
}

void Logging::InitClientAdvancedConfig()
{
    std::string errMsg;
    if (FLAGS_log_retention_day == DEFAULT_LOG_RETENTION_DAY) {
        FLAGS_log_retention_day = GetUint32FromEnv(LOG_RETENTION_DAY_ENV.c_str(), DEFAULT_LOG_RETENTION_DAY);
    }

    // Keep client don't print log to stderror.
    // Upstream can change FLAGS_ variable directly in fL#shorttype# namespace(high priority) to change log behavior.
    if (FLAGS_logtostderr == DEFAULT_LOG_TO_STDERR) {
        // If FLAGS_ variable is default value, can use "DATASYSTEM_xx" environment variable when to change behavior.
        FLAGS_logtostderr = GetBoolFromEnv(LOG_TO_STDERR_ENV.c_str(), DEFAULT_LOG_TO_STDERR);
    }

    if (FLAGS_alsologtostderr == DEFAULT_ALSO_LOG_TO_STDERR) {
        FLAGS_alsologtostderr = GetBoolFromEnv(ALSO_LOG_TO_STDERR_ENV.c_str(), DEFAULT_ALSO_LOG_TO_STDERR);
    }

    if (FLAGS_stderrthreshold == DEFAULT_STDERRTHRESHOLD) {
        // Change stderrthreshold default value to HIGHEST_STDERRTHRESHOLD to avoid error message log to stderr.
        FLAGS_stderrthreshold = GetUint32FromEnv(STDERR_THRESHOLD_ENV.c_str(), HIGHEST_STDERRTHRESHOLD);
    }

    if (FLAGS_log_async == DEFAULT_LOG_ASYNC_FLAG) {
        FLAGS_log_async = GetBoolFromEnv(LOG_ASYNC_ENABLE.c_str(), DEFAULT_LOG_ASYNC_FLAG);
    }

    if (FLAGS_log_async_queue_size == DEFAULT_LOG_ASYNC_QUEUE_SIZE) {
        FLAGS_log_async_queue_size =
            GetUint32FromEnv(LOG_ASYNC_QUEUE_SIZE.c_str(), DEFAULT_CLIENT_LOG_ASYNC_QUEUE_SIZE);
    }

    FLAGS_log_monitor = GetBoolFromEnv(LOG_MONITOR_ENABLE.c_str(), DEFAULT_CLIENT_LOG_MONITOR);
}

void Logging::InitClientConfig()
{
    InitClientBasicConfig();

    InitClientAdvancedConfig();
}

Logging::Logging() : init_(false)
{
}

Logging::~Logging()
{
    if (init_) {
        ShutdownLoggingWrapper();
    }
}

void Logging::Start(const std::string logFilename, bool isClient, uint32_t logProcessInterval)
{
    WriteLock lock(&mux_);
    if (IsLoggingInitialized()) {
        return;
    }

    isClient_ = isClient;

    std::string clientLogName;
    if (isClient_) {
        GetInstance()->InitClientConfig();

        clientLogName = logFilename + "_" + std::to_string(getpid());

        // Allow overriding client log filename via environment variable
        std::string logName = GetStringFromEnv(LOG_NAME_ENV.c_str(), "");
        if (ValidateLogName(logName)) {
            clientLogName = std::move(logName);
        }
    } else {
        clientLogName = logFilename;
    }

    std::string errMsg;
    (void)SetCommandLineOption("log_filename", clientLogName, errMsg);
    if (GetInstance()->InitLoggingWrapper(logProcessInterval)) {
        GetInstance()->init_ = true;
        SetLoggingInitialized(true);
        LOG(INFO) << "InitLoggingWrapper success.";
        if (isClient_) {
            LOG(INFO) << "The client log config is: log_dir: " << FLAGS_log_dir
                      << ", max_log_size: " << FLAGS_max_log_size << ", max_log_file_num: " << FLAGS_max_log_file_num
                      << ", log_compress: " << FLAGS_log_compress << ", log_retention_day: " << FLAGS_log_retention_day
                      << ", log_async: " << FLAGS_log_async << ", log_async_queue_size: " << FLAGS_log_async_queue_size
                      << ", log_v: " << FLAGS_v << std::endl;
        }
    } else {
        FLAGS_log_monitor = false;
    }
}

Status Logging::WriteLogToFile(int lineOfCode, const std::string &fileNameOfCode, const std::string &logFileName,
                               char level, const std::string &message)
{
    LogTime logTime;
    std::stringstream ss;
    auto pos = fileNameOfCode.find_last_of('/');
    std::string name = pos == std::string::npos ? fileNameOfCode : fileNameOfCode.substr(pos + 1);
    ConstructLogPrefix(ss, logTime.getTm(), logTime.getUsec(), name.c_str(), lineOfCode, podName_.c_str(), level,
                       FLAGS_cluster_name);
    ss << message;
    if (message.empty() || message[message.size() - 1] != '\n') {
        ss << '\n';
    }

    std::string output = ss.str();
    if (level != 'I') {
        std::cerr << output;
    }

    const int fileMode = 0640;
    int fd = open(logFileName.c_str(), O_WRONLY | O_CREAT | O_APPEND, fileMode);
    if (fd < 0) {
        RETURN_STATUS(K_RUNTIME_ERROR, "open file failed with " + StrErr(errno));
    }

    ssize_t sz = write(fd, output.c_str(), output.size());
    if (sz < 0) {
        std::string errMsg = "write file failed with " + StrErr(errno);
        RETRY_ON_EINTR(close(fd));
        RETURN_STATUS(K_RUNTIME_ERROR, errMsg);
    }

    RETRY_ON_EINTR(close(fd));
    return Status::OK();
}

bool Logging::ValidateLogName(const std::string &logName)
{
    if (logName.empty()) {
        return false;
    }

    // Only allow: a-z, A-Z, 0-9, _
    static const re2::RE2 re("^[a-zA-Z0-9_]*$");
    return re2::RE2::FullMatch(logName, re);
}
}  // namespace datasystem
