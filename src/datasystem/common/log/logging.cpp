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
#include <unistd.h>

#include "re2/re2.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_manager.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/log/log_time.h"
#include "datasystem/common/log/spdlog/log_message_impl.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/spdlog/provider.h"
#include "datasystem/common/log/spdlog/log_severity.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/uuid_generator.h"
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
constexpr bool DEFAULT_CLIENT_LOG_WITHOUT_PID = false;
constexpr bool DEFAULT_LOG_ASYNC_FLAG = true;
constexpr bool DEFAULT_LOG_COMPRESS = false;
constexpr bool DEFAULT_CLIENT_LOG_COMPRESS = false;
constexpr bool DEFAULT_LOG_ONLY_WRITE_INFO_FILE = true;
constexpr bool DEFAULT_LOG_TO_STDERR = false;
constexpr bool DEFAULT_ENABLE_PERF_TRACE_LOG = false;
constexpr int DEFAULT_STDERRTHRESHOLD = LogSeverity::ERROR;  // By default, errors always log to stderr.
constexpr int HIGHEST_STDERRTHRESHOLD = LogSeverity::FATAL;  // The errors log won't print to stderr.
constexpr std::size_t HIGHEST_SPDLOG_MAX_FILE_NUM = 200000;  // Maximum allowed by spdlog's rotating_file_sink.
constexpr uint32_t DEFAULT_LOG_ASYNC_THREAD_COUNT = 2;

DS_DEFINE_bool(log_async, DEFAULT_LOG_ASYNC_FLAG, "Enable asynchronous writing to log files.");
DS_DEFINE_uint32_dynamic(
    max_log_file_num, DEFAULT_MAX_LOG_FILE_NUM,
    "Maximum number of log files to retain per severity level. And every log file size is limited by max_log_size."
    "If set to 0, the log num is not limited.");
DS_DEFINE_bool_dynamic(log_compress, DEFAULT_LOG_COMPRESS,
                       "Compress old log files in .gz format. This parameter takes effect only when "
                       "the size of the generated log is greater than max log size.");
DS_DEFINE_uint32(log_retention_day, DEFAULT_LOG_RETENTION_DAY,
                 "If log_retention_day is greater than 0, any log file from your project whose last modified time is "
                 "greater than log_retention_day days will be "
                 "unlink()ed. If log_retention_day is equal 0, will not unlink log file by time.");
DS_DEFINE_int32(logbufsecs, DEFAULT_LOG_BUF_SECS, "Buffer log messages for at most this many seconds.");
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
DS_DEFINE_int32_dynamic(minloglevel, 0, "Messages below this level are not logged.");

static bool ValidateMinLogLevel(const char *flagName, int32_t value)
{
    if (value < 0 || value >= NUM_SEVERITIES) {
        LOG(ERROR) << FormatString("The value of %s flag is %d, which must be between 0 and %d (INFO=0, WARNING=1, "
                                   "ERROR=2, FATAL=3).",
                                   flagName, value, NUM_SEVERITIES - 1);
        return false;
    }
    return true;
}

DS_DEFINE_validator(minloglevel, &ValidateMinLogLevel);
DS_DEFINE_uint32(log_async_queue_size, DEFAULT_LOG_ASYNC_QUEUE_SIZE, "Size of async logger's message queue.");
DS_DEFINE_bool(log_only_write_info_file, DEFAULT_LOG_ONLY_WRITE_INFO_FILE,
               "The INFO log file always receives all severities. When true, do not create additional WARNING/ERROR "
               "log files.");
DS_DEFINE_bool(enable_perf_trace_log, GetBoolFromEnv(PERF_TRACE_LOG_ENV.c_str(), DEFAULT_ENABLE_PERF_TRACE_LOG),
               "Enable perf log output, When true always output perf related log.");

DS_DEFINE_double_dynamic(request_sample_rate, 1.0,
                         "Request log sample rate per trace [0.0-1.0]. "
                         "1.0=all retained; sampled-in request also forces access/diagnostic output.");
DS_DEFINE_double_dynamic(access_sample_rate, 1.0,
                         "Supplement access log sample rate [0.0-1.0]. "
                         "Only applies when request is NOT sampled-in; "
                         "request sampled-in always outputs access logs. "
                         "Final retention can exceed this rate. "
                         "1.0=not-sampled-in requests fully retained.");
DS_DEFINE_double_dynamic(diagnostic_sample_rate, 1.0,
                         "Supplement diagnostic log sample rate [0.0-1.0]. "
                         "Only applies when request is NOT sampled-in for "
                         "ERROR/WARNING/SLOW_LOG; request sampled-in always outputs "
                         "diagnostics. FATAL/CHECK always retained. "
                         "1.0=not-sampled-in requests fully retained.");

DS_DECLARE_bool(log_monitor);
DS_DECLARE_bool(log_only_write_info_file);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);

using namespace std::chrono;

namespace datasystem {
namespace {
std::mutex g_clientLogConfigMutex;
bool g_hasClientLogWithoutPidConfig = false;
bool g_clientLogWithoutPidConfig = false;
bool g_hasClientAccessLogNameConfig = false;
std::string g_clientAccessLogNameConfig;
}  // namespace

std::string &Logging::podName_ = Provider::GetPodName();

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

bool Logging::InitLogSampler()
{
    LogSampler::Instance().Init();

    LogSampleUserConfig cfg;
    cfg.requestSampleRate = FLAGS_request_sample_rate;
    cfg.accessSampleRate = FLAGS_access_sample_rate;
    cfg.diagnosticSampleRate = FLAGS_diagnostic_sample_rate;
    std::vector<FlagInfo> allFlags;
    GetAllFlags(allFlags);
    for (const auto &fi : allFlags) {
        if (fi.name == "request_sample_rate")
            cfg.requestSampleRateExplicit = fi.wasSpecified;
        if (fi.name == "access_sample_rate")
            cfg.accessSampleRateExplicit = fi.wasSpecified;
        if (fi.name == "diagnostic_sample_rate")
            cfg.diagnosticSampleRateExplicit = fi.wasSpecified;
    }
    if (!LogSampler::Instance().UpdateConfigFromFlags(cfg)) {
        LOG(FATAL) << "Illegal log sampler config at startup: "
                   << "request_sample_rate=" << FLAGS_request_sample_rate
                   << " access_sample_rate=" << FLAGS_access_sample_rate
                   << " diagnostic_sample_rate=" << FLAGS_diagnostic_sample_rate;
        return false;
    }
    return true;
}

namespace {

void EnsureLogFilename()
{
    if (!FLAGS_log_filename.empty()) {
        return;
    }
    // if flag log_filename is empty, it may be caused by two reasons as follows:
    // - 1. flags is not initialized yet, google::ProgramInvocationShortName() is 'UNKNOWN'.
    // - 2. flag log_filename is empty, we just set it as google::ProgramInvocationShortName().
    std::string programName = ProgramInvocationShortName();
    CHECK_STRNE(programName.c_str(), "UNKNOWN") << ": must initialize flags before logging";
    FLAGS_log_filename = std::move(programName);
}

LogParam BuildLoggerParam(uint32_t maxLogSize)
{
    std::vector<std::string> fileNamePatterns = { FLAGS_log_filename + ".INFO" };
    if (!FLAGS_log_only_write_info_file) {
        fileNamePatterns.emplace_back(FLAGS_log_filename + ".WARNING");
        fileNamePatterns.emplace_back(FLAGS_log_filename + ".ERROR");
    }
    LogParam loggerParam;
    loggerParam.fileNamePatterns = fileNamePatterns;
    loggerParam.logDir = FLAGS_log_dir;
    loggerParam.alsoLog2Stderr = FLAGS_alsologtostderr;
    loggerParam.stderrLogLevel = GetLogSeverityName(FLAGS_stderrthreshold);
    loggerParam.logLevel = GetLogSeverityName(FLAGS_minloglevel);
    loggerParam.logAsync = FLAGS_log_async;
    loggerParam.maxSize = maxLogSize;
    // Disable spdlog's auto-deletion of old log files, only use its file splitting (by size/time).
    // Log rotation is managed externally by the log manager.
    loggerParam.maxFiles = HIGHEST_SPDLOG_MAX_FILE_NUM;
    return loggerParam;
}

std::shared_ptr<ds_spdlog::logger> SetupLoggerProvider(const LogParam &loggerParam, GlobalLogParam &globalLogParam)
{
    globalLogParam.logBufSecs = FLAGS_logbufsecs;
    globalLogParam.maxAsyncQueueSize = FLAGS_log_async_queue_size;
    globalLogParam.asyncThreadCount = DEFAULT_LOG_ASYNC_THREAD_COUNT;
    auto lp = std::make_shared<LoggerProvider>(globalLogParam);
    Provider::Instance().SetLoggerProvider(lp);
    return lp->InitDsLogger(loggerParam);
}

}  // namespace

bool Logging::InitLoggingWrapper(uint32_t logProcessInterval)
{
    if (!CreateLogDir()) {
        return false;
    }
    EnsureLogFilename();
    const LogParam loggerParam = BuildLoggerParam(MaxLogSize());
    GlobalLogParam globalLogParam;
    auto logger = SetupLoggerProvider(loggerParam, globalLogParam);
    if (logger == nullptr) {
        return false;
    }
    if (!InitLogSampler()) {
        return false;
    }
    if (loggerParam.logAsync) {
        LOG(INFO) << "Async logging buffer duration: " << globalLogParam.logBufSecs << " s";
    }
    logManager_ = std::make_unique<LogManager>(logProcessInterval);
    auto status = logManager_->Start();
    if (status.IsError()) {
        LOG(ERROR) << "Failed to start log manager:" << status.ToString() << std::endl;
        return false;
    }
    accessRecorderManagerInstance_ = std::make_unique<AccessRecorderManager>();
    auto rc = accessRecorderManagerInstance_->Init(isClient_, isEmbeddedClient_);
    if (rc.IsError()) {
        return false;
    }
    (void)OperationLogger::Instance().Init(isClient_ ? "client" : "worker");
    return true;
}

void Logging::ShutdownLoggingWrapper()
{
    // Log has been shutdown, just return quiet.
    if (!IsLoggingInitialized()) {
        return;
    }

    Trace::Instance().SetTraceNewID("Shutdown;" + GetStringUuid(), true);

    OperationLogger::Instance().Shutdown();

    // Stop log manager.
    logManager_->Stop();

    // Shutdown LogSampler.
    LogSampler::Instance().Shutdown();

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

    if (!WasCommandLineFlagSpecified("max_log_size")) {
        FLAGS_max_log_size = GetUint32FromEnv(MAX_LOG_SIZE_ENV.c_str(), DEFAULT_CLIENT_MAX_LOG_SIZE_MB);
    }

    if (!WasCommandLineFlagSpecified("max_log_file_num")) {
        FLAGS_max_log_file_num = GetUint32FromEnv(MAX_LOG_FILE_NUM_ENV.c_str(), DEFAULT_MAX_LOG_FILE_NUM);
    }

    if (!WasCommandLineFlagSpecified("log_compress")) {
        FLAGS_log_compress = GetBoolFromEnv(LOG_COMPRESS_ENV.c_str(), DEFAULT_CLIENT_LOG_COMPRESS);
    }

    if (!WasCommandLineFlagSpecified("v")) {
        uint32_t value = GetUint32FromEnv(LOG_V.c_str(), DEFAULT_LOG_LEVEL);
        FLAGS_v = static_cast<int32_t>(value > INT32_MAX ? 0 : value);
    }

    if (!WasCommandLineFlagSpecified("minloglevel")) {
        uint32_t value = GetUint32FromEnv(MIN_LOG_LEVEL.c_str(), DEFAULT_LOG_LEVEL);
        FLAGS_minloglevel = static_cast<int32_t>(value > INT32_MAX ? 0 : value);
    }
}

void Logging::InitClientAdvancedConfig()
{
    std::string errMsg;
    if (!WasCommandLineFlagSpecified("log_retention_day")) {
        FLAGS_log_retention_day = GetUint32FromEnv(LOG_RETENTION_DAY_ENV.c_str(), DEFAULT_LOG_RETENTION_DAY);
    }

    // Keep client don't print log to stderror.
    // Upstream can change FLAGS_ variable directly in fL#shorttype# namespace(high priority) to change log behavior.
    if (!WasCommandLineFlagSpecified("logtostderr")) {
        // If FLAGS_ variable is default value, can use "DATASYSTEM_xx" environment variable when to change behavior.
        FLAGS_logtostderr = GetBoolFromEnv(LOG_TO_STDERR_ENV.c_str(), DEFAULT_LOG_TO_STDERR);
    }

    if (!WasCommandLineFlagSpecified("alsologtostderr")) {
        FLAGS_alsologtostderr = GetBoolFromEnv(ALSO_LOG_TO_STDERR_ENV.c_str(), DEFAULT_ALSO_LOG_TO_STDERR);
    }

    if (!WasCommandLineFlagSpecified("stderrthreshold")) {
        // Change stderrthreshold default value to HIGHEST_STDERRTHRESHOLD to avoid error message log to stderr.
        FLAGS_stderrthreshold = GetUint32FromEnv(STDERR_THRESHOLD_ENV.c_str(), HIGHEST_STDERRTHRESHOLD);
    }

    if (!WasCommandLineFlagSpecified("log_async")) {
        FLAGS_log_async = GetBoolFromEnv(LOG_ASYNC_ENABLE.c_str(), DEFAULT_LOG_ASYNC_FLAG);
    }

    if (!WasCommandLineFlagSpecified("log_async_queue_size")) {
        FLAGS_log_async_queue_size =
            GetUint32FromEnv(LOG_ASYNC_QUEUE_SIZE.c_str(), DEFAULT_CLIENT_LOG_ASYNC_QUEUE_SIZE);
    }

    if (!WasCommandLineFlagSpecified("log_monitor")) {
        FLAGS_log_monitor = GetBoolFromEnv(LOG_MONITOR_ENABLE.c_str(), DEFAULT_CLIENT_LOG_MONITOR);
    }

    if (!WasCommandLineFlagSpecified("zmq_client_io_thread")) {
        FLAGS_zmq_client_io_thread = GetInt32FromEnv("DATASYSTEM_ZMQ_CLIENT_IO_THREAD", 1);
    }

    if (!WasCommandLineFlagSpecified("log_only_write_info_file")) {
        FLAGS_log_only_write_info_file =
            GetBoolFromEnv(LOG_ONLY_WRITE_INFO_FILE_ENV.c_str(), DEFAULT_LOG_ONLY_WRITE_INFO_FILE);
    }
}

void Logging::InitClientConfig()
{
    InitClientBasicConfig();

    InitClientAdvancedConfig();
}

std::string Logging::GetClientLogName(const std::string &baseName, int pid)
{
    if (IsClientLogWithoutPidEnabled()) {
        return baseName;
    }
    return FormatString("%s_%d", baseName, pid);
}

bool Logging::IsClientLogWithoutPidEnabled()
{
    std::lock_guard<std::mutex> lock(g_clientLogConfigMutex);
    if (g_hasClientLogWithoutPidConfig) {
        return g_clientLogWithoutPidConfig;
    }
    return GetBoolFromEnv(CLIENT_LOG_WITHOUT_PID_ENV.c_str(), DEFAULT_CLIENT_LOG_WITHOUT_PID);
}

void Logging::SetClientLogWithoutPid(bool enabled)
{
    std::lock_guard<std::mutex> lock(g_clientLogConfigMutex);
    g_hasClientLogWithoutPidConfig = true;
    g_clientLogWithoutPidConfig = enabled;
}

void Logging::SetClientAccessLogName(const std::string &logName)
{
    std::lock_guard<std::mutex> lock(g_clientLogConfigMutex);
    g_hasClientAccessLogNameConfig = true;
    g_clientAccessLogNameConfig = logName;
}

std::string Logging::GetClientAccessLogName()
{
    std::lock_guard<std::mutex> lock(g_clientLogConfigMutex);
    if (g_hasClientAccessLogNameConfig) {
        return g_clientAccessLogNameConfig;
    }
    return GetStringFromEnv(ACCESS_LOG_NAME_ENV.c_str(), "");
}

#ifdef WITH_TESTS
void Logging::ResetClientLogConfigForTest()
{
    std::lock_guard<std::mutex> lock(g_clientLogConfigMutex);
    g_hasClientLogWithoutPidConfig = false;
    g_clientLogWithoutPidConfig = false;
    g_hasClientAccessLogNameConfig = false;
    g_clientAccessLogNameConfig.clear();
}
#endif

Logging::Logging() : init_(false)
{
    // Ensure spdlog is initialized early to avoid static initialization order issues.
    LoggerContext::GetDefaultLogger();
}

Logging::~Logging()
{
    if (init_) {
        ShutdownLoggingWrapper();
    }
}

void Logging::Start(const std::string logFilename, bool isClient, uint32_t logProcessInterval, bool isEmbeddedClient)
{
    WriteLock lock(&mux_);
    if (IsLoggingInitialized()) {
        return;
    }

    isClient_ = isClient;
    isEmbeddedClient_ = isEmbeddedClient;
    std::string clientLogName;
    if (isClient_) {
        GetInstance()->InitClientConfig();

        if (!FLAGS_log_filename.empty()) {
            clientLogName = FLAGS_log_filename;
        } else {
            clientLogName = GetClientLogName(logFilename, getpid());

            // Allow overriding client log filename via environment variable
            std::string logName = GetStringFromEnv(LOG_NAME_ENV.c_str(), "");
            if (ValidateLogName(logName)) {
                clientLogName = std::move(logName);
            }
        }
    } else {
        clientLogName = logFilename;
    }
    std::string errMsg;
    (void)SetCommandLineOption("log_filename", clientLogName, errMsg);
    if (GetInstance()->InitLoggingWrapper(logProcessInterval)) {
        GetInstance()->init_ = true;
        SetLoggingInitialized(true);
        podName_ = GetStringFromEnvOrFile("POD_IP", GetWorkerEnvFilePath(FLAGS_log_dir), WORKER_ENV_POD_IP_KEY,
                                          Provider::GetPodName());

        LOG(INFO) << "InitLoggingWrapper success.";
        if (isClient_) {
            LOG(INFO) << "The client log config is: log_dir: " << FLAGS_log_dir
                      << ", max_log_size: " << FLAGS_max_log_size << ", max_log_file_num: " << FLAGS_max_log_file_num
                      << ", log_compress: " << FLAGS_log_compress << ", log_retention_day: " << FLAGS_log_retention_day
                      << ", log_async: " << FLAGS_log_async << ", log_async_queue_size: " << FLAGS_log_async_queue_size
                      << ", log_v: " << FLAGS_v << ", log_only_write_info_file: " << FLAGS_log_only_write_info_file
                      << std::endl;
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
