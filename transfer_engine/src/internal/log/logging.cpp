#include "internal/log/logging.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <pthread.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <spdlog/logger.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_sinks.h>

namespace datasystem {
namespace internal {
namespace {

constexpr char K_LOG_LEVEL_ENV[] = "TRANSFER_ENGINE_LOG_LEVEL";
constexpr char K_VLOG_LEVEL_ENV[] = "TRANSFER_ENGINE_VLOG_LEVEL";
constexpr char K_VMODULE_ENV[] = "TRANSFER_ENGINE_VMODULE";
constexpr char K_LOG_DIR_ENV[] = "TRANSFER_ENGINE_LOG_DIR";
constexpr char K_LOG_TO_STDERR_ENV[] = "TRANSFER_ENGINE_LOG_TO_STDERR";
constexpr char K_ALSO_LOG_TO_STDERR_ENV[] = "TRANSFER_ENGINE_ALSO_LOG_TO_STDERR";
constexpr char K_LOG_TO_STDOUT_ENV[] = "TRANSFER_ENGINE_LOG_TO_STDOUT";
constexpr char K_STDERR_THRESHOLD_ENV[] = "TRANSFER_ENGINE_STDERR_THRESHOLD";
constexpr char K_LOG_BUFFER_LEVEL_ENV[] = "TRANSFER_ENGINE_LOG_BUFFER_LEVEL";
constexpr char K_LOG_BUFFER_SECONDS_ENV[] = "TRANSFER_ENGINE_LOG_BUFFER_SECONDS";
constexpr char K_MAX_LOG_SIZE_MB_ENV[] = "TRANSFER_ENGINE_MAX_LOG_SIZE_MB";
constexpr char K_LOG_FILE_MODE_ENV[] = "TRANSFER_ENGINE_LOG_FILE_MODE";
constexpr char K_TIMESTAMP_IN_LOG_FILE_NAME_ENV[] = "TRANSFER_ENGINE_TIMESTAMP_IN_LOG_FILE_NAME";
constexpr char K_LOG_FILE_HEADER_ENV[] = "TRANSFER_ENGINE_LOG_FILE_HEADER";
constexpr char K_LOG_PREFIX_ENV[] = "TRANSFER_ENGINE_LOG_PREFIX";
constexpr char K_LOG_YEAR_IN_PREFIX_ENV[] = "TRANSFER_ENGINE_LOG_YEAR_IN_PREFIX";
constexpr char K_LOG_UTC_TIME_ENV[] = "TRANSFER_ENGINE_LOG_UTC_TIME";
constexpr char K_PROGRAM_NAME[] = "transfer_engine";
constexpr char K_LOGGER_NAME[] = "yuanrong_transfer_engine";
constexpr size_t K_SEVERITY_FILE_COUNT = 4;
constexpr size_t K_MEGABYTE = 1024U * 1024U;
constexpr size_t K_MAX_ROTATED_FILES = 200000;
constexpr int K_INFO_SEVERITY_RANK = 0;
constexpr int K_WARNING_SEVERITY_RANK = 1;
constexpr int K_ERROR_SEVERITY_RANK = 2;
constexpr int K_FATAL_SEVERITY_RANK = 3;
constexpr int K_OFF_SEVERITY_RANK = 4;
constexpr int K_DEFAULT_STDERR_THRESHOLD = 2;
constexpr int K_DEFAULT_LOG_BUFFER_SECONDS = 30;
constexpr long K_DEFAULT_MAX_LOG_SIZE_MB = 1800;
constexpr long K_MAX_LOG_SIZE_MB_LIMIT = 4096;
constexpr long K_MIN_LOG_SIZE_MB = 1;
constexpr mode_t K_DEFAULT_LOG_FILE_MODE = 0664;
constexpr mode_t K_MAX_LOG_FILE_MODE = 0777;
constexpr int K_LOG_YEAR_BASE = 1900;
constexpr int K_YEAR_FIELD_WIDTH = 4;
constexpr int K_TIME_FIELD_WIDTH = 2;
constexpr int K_MICROSECOND_FIELD_WIDTH = 6;
constexpr int K_MONTH_BASE = 1;
constexpr int K_MICROSECONDS_PER_SECOND = 1000000;
constexpr size_t K_HOSTNAME_BUFFER_SIZE = 256;
constexpr size_t K_USERNAME_BUFFER_SIZE = 1024;
constexpr size_t K_FALLBACK_PREFIX_BUFFER_SIZE = 512;

struct VModuleRule {
    std::string pattern;
    int level = 0;
};

struct LogConfig {
    LogSeverity minSeverity = LogSeverity::INFO;
    int vlogLevel = 0;
    std::vector<VModuleRule> vmoduleRules;
    bool logToStderr = false;
    bool alsoLogToStderr = false;
    bool logToStdout = false;
    int stderrThreshold = K_DEFAULT_STDERR_THRESHOLD;
    int logBufLevel = 0;
    int logBufSecs = K_DEFAULT_LOG_BUFFER_SECONDS;
    uint32_t maxLogSizeMb = K_DEFAULT_MAX_LOG_SIZE_MB;
    mode_t logfileMode = K_DEFAULT_LOG_FILE_MODE;
    bool timestampInFilename = true;
    bool writeFileHeader = true;
    bool logPrefix = true;
    bool logYearInPrefix = true;
    bool useUtc = false;
    std::string logDir;
    bool invalidLogLevel = false;
    bool invalidVlogLevel = false;
};

bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) noexcept
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.size(); ++index) {
        const auto lhsChar = static_cast<unsigned char>(lhs[index]);
        const auto rhsChar = static_cast<unsigned char>(rhs[index]);
        if (std::toupper(lhsChar) != std::toupper(rhsChar)) {
            return false;
        }
    }
    return true;
}

const char *GetEnv(const char *name) noexcept
{
    const char *value = std::getenv(name);
    return value != nullptr && value[0] != '\0' ? value : nullptr;
}

bool ParseBool(const char *value, bool defaultValue) noexcept
{
    if (value == nullptr) {
        return defaultValue;
    }
    return value[0] == 't' || value[0] == 'T' || value[0] == 'y' || value[0] == 'Y' || value[0] == '1';
}

long ParseLong(const char *value, long defaultValue, bool *valid = nullptr) noexcept
{
    if (value == nullptr || value[0] == '\0') {
        if (valid != nullptr) {
            *valid = true;
        }
        return defaultValue;
    }
    long parsed = 0;
    const std::string_view text(value);
    const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
    const bool ok = result.ec == std::errc() && result.ptr == text.data() + text.size();
    if (valid != nullptr) {
        *valid = ok;
    }
    return ok ? parsed : defaultValue;
}

mode_t ParseFileMode(const char *value) noexcept
{
    if (value == nullptr || value[0] == '\0') {
        return K_DEFAULT_LOG_FILE_MODE;
    }
    long parsed = 0;
    const std::string_view text(value);
    const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed, 8);
    if (result.ec == std::errc() && result.ptr == text.data() + text.size() && parsed >= 0 &&
        parsed <= K_MAX_LOG_FILE_MODE) {
        return static_cast<mode_t>(parsed);
    }
    return K_DEFAULT_LOG_FILE_MODE;
}

LogSeverity ParseLogSeverity(const char *value, bool &valid) noexcept
{
    if (value == nullptr || *value == '\0' || EqualsIgnoreCase(value, "INFO")) {
        valid = true;
        return LogSeverity::INFO;
    }
    if (EqualsIgnoreCase(value, "DEBUG")) {
        valid = true;
        return LogSeverity::DEBUG;
    }
    if (EqualsIgnoreCase(value, "WARNING") || EqualsIgnoreCase(value, "WARN")) {
        valid = true;
        return LogSeverity::WARNING;
    }
    if (EqualsIgnoreCase(value, "ERROR")) {
        valid = true;
        return LogSeverity::ERROR;
    }
    if (EqualsIgnoreCase(value, "FATAL")) {
        valid = true;
        return LogSeverity::FATAL;
    }
    if (EqualsIgnoreCase(value, "OFF")) {
        valid = true;
        return LogSeverity::OFF;
    }
    valid = false;
    return LogSeverity::INFO;
}

int ParseVlogLevel(const char *value, bool &valid) noexcept
{
    const long parsed = ParseLong(value, 0, &valid);
    if (!valid || parsed < 0 || parsed > std::numeric_limits<int>::max()) {
        valid = false;
        return 0;
    }
    return static_cast<int>(parsed);
}

std::vector<VModuleRule> ParseVModuleRules(const char *value)
{
    std::vector<VModuleRule> rules;
    if (value == nullptr) {
        return rules;
    }
    std::string text(value);
    size_t begin = 0;
    while (begin < text.size()) {
        const size_t end = text.find(',', begin);
        const std::string entry = text.substr(begin, end == std::string::npos ? std::string::npos : end - begin);
        const size_t separator = entry.find('=');
        if (separator != std::string::npos && separator > 0) {
            bool valid = true;
            const long level = ParseLong(entry.c_str() + separator + 1, 0, &valid);
            if (valid && level >= 0 && level <= std::numeric_limits<int>::max()) {
                rules.push_back({ entry.substr(0, separator), static_cast<int>(level) });
            }
        }
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    return rules;
}

std::string ResolveConfiguredLogDir()
{
    if (const char *value = GetEnv(K_LOG_DIR_ENV); value != nullptr) {
        return value;
    }
    return "";
}

LogConfig LoadLogConfig() noexcept
{
    LogConfig config;
    bool valid = true;
    config.minSeverity = ParseLogSeverity(GetEnv(K_LOG_LEVEL_ENV), valid);
    config.invalidLogLevel = !valid;
    config.vlogLevel = ParseVlogLevel(GetEnv(K_VLOG_LEVEL_ENV), valid);
    config.invalidVlogLevel = !valid;

    try {
        config.vmoduleRules = ParseVModuleRules(GetEnv(K_VMODULE_ENV));
        config.logDir = ResolveConfiguredLogDir();
    } catch (...) {
        // Allocation or string parsing failures leave logging on default routing.
        config.vmoduleRules.clear();
        config.logDir.clear();
    }

    config.logToStderr = ParseBool(GetEnv(K_LOG_TO_STDERR_ENV), false);
    config.alsoLogToStderr = ParseBool(GetEnv(K_ALSO_LOG_TO_STDERR_ENV), false);
    config.logToStdout = ParseBool(GetEnv(K_LOG_TO_STDOUT_ENV), false);
    config.stderrThreshold = static_cast<int>(ParseLong(GetEnv(K_STDERR_THRESHOLD_ENV), K_DEFAULT_STDERR_THRESHOLD));
    config.logBufLevel = static_cast<int>(ParseLong(GetEnv(K_LOG_BUFFER_LEVEL_ENV), 0));
    config.logBufSecs = static_cast<int>(ParseLong(GetEnv(K_LOG_BUFFER_SECONDS_ENV), K_DEFAULT_LOG_BUFFER_SECONDS));
    const long maxLogSize = ParseLong(GetEnv(K_MAX_LOG_SIZE_MB_ENV), K_DEFAULT_MAX_LOG_SIZE_MB);
    if (maxLogSize > 0 && maxLogSize < K_MAX_LOG_SIZE_MB_LIMIT) {
        config.maxLogSizeMb = static_cast<uint32_t>(maxLogSize);
    } else {
        config.maxLogSizeMb = K_MIN_LOG_SIZE_MB;
    }
    config.logfileMode = ParseFileMode(GetEnv(K_LOG_FILE_MODE_ENV));
    config.timestampInFilename = ParseBool(GetEnv(K_TIMESTAMP_IN_LOG_FILE_NAME_ENV), true);
    config.writeFileHeader = ParseBool(GetEnv(K_LOG_FILE_HEADER_ENV), true);
    config.logPrefix = ParseBool(GetEnv(K_LOG_PREFIX_ENV), true);
    config.logYearInPrefix = ParseBool(GetEnv(K_LOG_YEAR_IN_PREFIX_ENV), true);
    config.useUtc = ParseBool(GetEnv(K_LOG_UTC_TIME_ENV), false);
    return config;
}

const LogConfig &GetLogConfig() noexcept
{
    static const LogConfig config = LoadLogConfig();
    return config;
}

int SeverityRank(LogSeverity severity) noexcept
{
    switch (severity) {
        case LogSeverity::DEBUG:
        case LogSeverity::INFO:
            return K_INFO_SEVERITY_RANK;
        case LogSeverity::WARNING:
            return K_WARNING_SEVERITY_RANK;
        case LogSeverity::ERROR:
            return K_ERROR_SEVERITY_RANK;
        case LogSeverity::FATAL:
            return K_FATAL_SEVERITY_RANK;
        case LogSeverity::OFF:
        default:
            return K_OFF_SEVERITY_RANK;
    }
}

ds_spdlog::level::level_enum ToSpdlogLevel(LogSeverity severity) noexcept
{
    switch (severity) {
        case LogSeverity::DEBUG:
        case LogSeverity::INFO:
            return ds_spdlog::level::info;
        case LogSeverity::WARNING:
            return ds_spdlog::level::warn;
        case LogSeverity::ERROR:
            return ds_spdlog::level::err;
        case LogSeverity::FATAL:
            return ds_spdlog::level::critical;
        case LogSeverity::OFF:
        default:
            return ds_spdlog::level::off;
    }
}

const char *SeverityName(LogSeverity severity) noexcept
{
    switch (severity) {
        case LogSeverity::DEBUG:
        case LogSeverity::INFO:
            return "INFO";
        case LogSeverity::WARNING:
            return "WARNING";
        case LogSeverity::ERROR:
            return "ERROR";
        case LogSeverity::FATAL:
            return "FATAL";
        case LogSeverity::OFF:
        default:
            return "OFF";
    }
}

const char *FileSeverityName(size_t index) noexcept
{
    static constexpr std::array<const char *, K_SEVERITY_FILE_COUNT> names = { "INFO", "WARNING", "ERROR", "FATAL" };
    return index < names.size() ? names[index] : "UNKNOWN";
}

char SeverityCharacter(LogSeverity severity) noexcept
{
    switch (severity) {
        case LogSeverity::WARNING:
            return 'W';
        case LogSeverity::ERROR:
            return 'E';
        case LogSeverity::FATAL:
            return 'F';
        case LogSeverity::DEBUG:
        case LogSeverity::INFO:
        case LogSeverity::OFF:
        default:
            return 'I';
    }
}

std::string_view Basename(const char *file) noexcept
{
    if (file == nullptr) {
        return "unknown";
    }
    std::string_view name(file);
    const size_t slash = name.find_last_of("/\\");
    return slash == std::string_view::npos ? name : name.substr(slash + 1);
}

std::string FormatLogLine(LogSeverity severity, const char *file, int line, std::string_view message,
                          const LogConfig &config)
{
    if (!config.logPrefix) {
        return std::string(message);
    }

    const auto now = std::chrono::system_clock::now();
    const std::time_t time = std::chrono::system_clock::to_time_t(now);
    std::tm brokenDown {};
    if (config.useUtc) {
        gmtime_r(&time, &brokenDown);
    } else {
        localtime_r(&time, &brokenDown);
    }
    const auto microseconds =
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() %
        K_MICROSECONDS_PER_SECOND;
    const auto threadId = static_cast<long>(syscall(SYS_gettid));

    std::ostringstream formatted;
    formatted.fill('0');
    formatted << SeverityCharacter(severity);
    if (config.logYearInPrefix) {
        formatted << std::setw(K_YEAR_FIELD_WIDTH) << (brokenDown.tm_year + K_LOG_YEAR_BASE);
    }
    formatted << std::setw(K_TIME_FIELD_WIDTH) << (brokenDown.tm_mon + K_MONTH_BASE) <<
        std::setw(K_TIME_FIELD_WIDTH) << brokenDown.tm_mday << ' ' << std::setw(K_TIME_FIELD_WIDTH) <<
        brokenDown.tm_hour << ':' << std::setw(K_TIME_FIELD_WIDTH) << brokenDown.tm_min << ':' <<
        std::setw(K_TIME_FIELD_WIDTH) << brokenDown.tm_sec << '.' << std::setw(K_MICROSECOND_FIELD_WIDTH) <<
        microseconds << ' ';
    formatted.fill(' ');
    formatted << getpid() << ':' << threadId << ' ' << Basename(file) << ':' << line << "] " << message;
    return formatted.str();
}

std::string GetHostname()
{
    std::array<char, K_HOSTNAME_BUFFER_SIZE> hostname{};
    if (gethostname(hostname.data(), hostname.size() - 1) != 0) {
        return "(unknown)";
    }
    hostname.back() = '\0';
    return hostname.data();
}

std::string GetUsername()
{
    if (const char *user = GetEnv("USER"); user != nullptr) {
        return user;
    }
    struct passwd pwd {};
    struct passwd *result = nullptr;
    std::array<char, K_USERNAME_BUFFER_SIZE> buffer{};
    if (getpwuid_r(geteuid(), &pwd, buffer.data(), buffer.size(), &result) == 0 && result != nullptr &&
        pwd.pw_name != nullptr) {
        return pwd.pw_name;
    }
    return "invalid-user";
}

bool DirectoryExists(const std::string &path) noexcept
{
    struct stat info {};
    return stat(path.c_str(), &info) == 0 && S_ISDIR(info.st_mode);
}

std::vector<std::string> CandidateLogDirectories(const LogConfig &config)
{
    if (!config.logDir.empty()) {
        return { config.logDir };
    }
    std::vector<std::string> candidates;
    for (const char *name : { "TEST_TMPDIR", "TMPDIR", "TMP" }) {
        if (const char *value = GetEnv(name); value != nullptr) {
            candidates.emplace_back(value);
            if (DirectoryExists(value)) {
                return candidates;
            }
        }
    }
    candidates.emplace_back("/tmp");
    if (DirectoryExists("/tmp")) {
        return candidates;
    }
    candidates.emplace_back(".");
    return candidates;
}

std::tm CurrentTime(bool useUtc)
{
    const std::time_t now = std::time(nullptr);
    std::tm value {};
    if (useUtc) {
        gmtime_r(&now, &value);
    } else {
        localtime_r(&now, &value);
    }
    return value;
}

std::string BuildLogFilename(const std::string &directory, const LogConfig &config, size_t severityIndex)
{
    std::ostringstream path;
    path << directory;
    if (!directory.empty() && directory.back() != '/') {
        path << '/';
    }
    path << K_PROGRAM_NAME << '.' << GetHostname() << '.' << GetUsername() << ".log." <<
        FileSeverityName(severityIndex);
    if (config.timestampInFilename) {
        const std::tm now = CurrentTime(config.useUtc);
        path << '.' << std::put_time(&now, "%Y%m%d-%H%M%S") << '.' << getpid();
    }
    return path.str();
}

void WriteFileHeader(std::FILE *stream, bool useUtc)
{
    if (stream == nullptr) {
        return;
    }
    const std::tm now = CurrentTime(useUtc);
    const std::string hostname = GetHostname();
    std::fprintf(stream, "Log file created at: %04d/%02d/%02d %02d:%02d:%02d%s\n",
                 now.tm_year + K_LOG_YEAR_BASE, now.tm_mon + K_MONTH_BASE, now.tm_mday, now.tm_hour, now.tm_min,
                 now.tm_sec, useUtc ? " UTC" : "");
    std::fprintf(stream, "Running on machine: %s\n", hostname.c_str());
    std::fprintf(stream, "Running duration (h:mm:ss): 0:00:00\n");
    std::fprintf(stream, "Log line format: [IWEF]yyyymmdd hh:mm:ss.uuuuuu pid:tid file:line] msg\n");
}

void UpdateSeveritySymlink(const std::string &filename, size_t severityIndex)
{
    const size_t slash = filename.find_last_of('/');
    const std::string directory = slash == std::string::npos ? "" : filename.substr(0, slash + 1);
    const std::string target = slash == std::string::npos ? filename : filename.substr(slash + 1);
    const std::string link = directory + K_PROGRAM_NAME + "." + FileSeverityName(severityIndex);
    if (unlink(link.c_str()) != 0) {
        const int unlinkErrno = errno;
        if (unlinkErrno != ENOENT) {
            return;
        }
    }
    if (symlink(target.c_str(), link.c_str()) != 0) {
        return;
    }
}

std::shared_ptr<ds_spdlog::logger> CreateConsoleLogger(const std::string &name, bool stdoutSink)
{
    ds_spdlog::sink_ptr sink;
    if (stdoutSink) {
        sink = std::make_shared<ds_spdlog::sinks::stdout_sink_mt>();
    } else {
        sink = std::make_shared<ds_spdlog::sinks::stderr_sink_mt>();
    }
    auto logger = std::make_shared<ds_spdlog::logger>(name, std::move(sink));
    logger->set_pattern("%v");
    logger->set_level(ds_spdlog::level::trace);
    return logger;
}

struct LoggerState {
    explicit LoggerState(const LogConfig &loadedConfig) : config(loadedConfig)
    {
        stderrLogger = CreateConsoleLogger(std::string(K_LOGGER_NAME) + "_stderr", false);
        stdoutLogger = CreateConsoleLogger(std::string(K_LOGGER_NAME) + "_stdout", true);
        nextFlush = std::chrono::steady_clock::now() + std::chrono::seconds(std::max(config.logBufSecs, 0));
    }

    std::shared_ptr<ds_spdlog::logger> CreateFileLogger(size_t severityIndex)
    {
        for (const auto &directory : CandidateLogDirectories(config)) {
            try {
                const std::string filename = BuildLogFilename(directory, config, severityIndex);
                ds_spdlog::file_event_handlers handlers;
                const mode_t mode = config.logfileMode;
                const bool writeHeader = config.writeFileHeader;
                const bool useUtc = config.useUtc;
                handlers.after_open = [mode, writeHeader, useUtc](const ds_spdlog::filename_t &, std::FILE *stream) {
                    if (stream != nullptr) {
                        (void)fchmod(fileno(stream), mode);
                        if (writeHeader) {
                            WriteFileHeader(stream, useUtc);
                        }
                    }
                };
                auto sink = std::make_shared<ds_spdlog::sinks::rotating_file_sink_mt>(
                    filename, static_cast<size_t>(config.maxLogSizeMb) * K_MEGABYTE, K_MAX_ROTATED_FILES, false,
                    handlers);
                auto logger = std::make_shared<ds_spdlog::logger>(
                    std::string(K_LOGGER_NAME) + "_" + FileSeverityName(severityIndex), std::move(sink));
                logger->set_pattern("%v");
                logger->set_level(ds_spdlog::level::trace);
                UpdateSeveritySymlink(filename, severityIndex);
                return logger;
            } catch (...) {
                // Try the next candidate directory if this sink cannot be opened.
            }
        }
        return nullptr;
    }

    std::shared_ptr<ds_spdlog::logger> GetFileLogger(size_t severityIndex)
    {
        if (severityIndex >= fileLoggers.size()) {
            return nullptr;
        }
        if (fileLoggers[severityIndex] == nullptr) {
            fileLoggers[severityIndex] = CreateFileLogger(severityIndex);
        }
        return fileLoggers[severityIndex];
    }

    void FlushUnlocked() noexcept
    {
        for (const auto &logger : fileLoggers) {
            if (logger != nullptr) {
                try {
                    logger->flush();
                } catch (...) {
                    // Flush is best effort during logging paths and process shutdown.
                }
            }
        }
        for (const auto &logger : { stderrLogger, stdoutLogger }) {
            if (logger != nullptr) {
                try {
                    logger->flush();
                } catch (...) {
                    // Console flush is best effort during logging paths and process shutdown.
                }
            }
        }
        nextFlush = std::chrono::steady_clock::now() + std::chrono::seconds(std::max(config.logBufSecs, 0));
    }

    bool LogToConsole(const std::shared_ptr<ds_spdlog::logger> &logger, ds_spdlog::level::level_enum level,
                      ds_spdlog::string_view_t text)
    {
        if (logger == nullptr) {
            return false;
        }
        logger->log(level, text);
        return true;
    }

    bool LogToFileSinks(int severityRank, ds_spdlog::level::level_enum level, ds_spdlog::string_view_t text)
    {
        bool emitted = false;
        const size_t maxFileIndex =
            static_cast<size_t>(std::min(severityRank, static_cast<int>(K_SEVERITY_FILE_COUNT - 1)));
        for (size_t index = 0; index <= maxFileIndex; ++index) {
            emitted = LogToConsole(GetFileLogger(index), level, text) || emitted;
        }
        return emitted;
    }

    bool LogToConfiguredSinks(int severityRank, ds_spdlog::level::level_enum level, ds_spdlog::string_view_t text)
    {
        if (config.logToStdout) {
            auto logger = severityRank >= config.stderrThreshold ? stderrLogger : stdoutLogger;
            return LogToConsole(logger, level, text);
        }
        if (config.logToStderr) {
            return LogToConsole(stderrLogger, level, text);
        }

        bool emitted = LogToFileSinks(severityRank, level, text);
        if (config.alsoLogToStderr || severityRank >= config.stderrThreshold) {
            emitted = LogToConsole(stderrLogger, level, text) || emitted;
        }
        return emitted;
    }

    bool Log(LogSeverity severity, const char *file, int line, std::string_view message) noexcept
    {
        std::lock_guard<std::mutex> lock(mutex);
        bool emitted = false;
        const int severityRank = SeverityRank(severity);
        const auto level = ToSpdlogLevel(severity);

        try {
            const std::string formatted = FormatLogLine(severity, file, line, message, config);
            const ds_spdlog::string_view_t text{ formatted.data(), formatted.size() };
            emitted = LogToConfiguredSinks(severityRank, level, text);
        } catch (...) {
            // Logging failures fall through to the fallback sink at the call site.
        }

        const auto now = std::chrono::steady_clock::now();
        if (severityRank > config.logBufLevel || config.logBufSecs <= 0 || now >= nextFlush) {
            FlushUnlocked();
        }
        return emitted;
    }

    LogConfig config;
    std::mutex mutex;
    std::array<std::shared_ptr<ds_spdlog::logger>, K_SEVERITY_FILE_COUNT> fileLoggers;
    std::shared_ptr<ds_spdlog::logger> stderrLogger;
    std::shared_ptr<ds_spdlog::logger> stdoutLogger;
    std::chrono::steady_clock::time_point nextFlush;
};

std::ostream &GetNullStream() noexcept
{
    static std::ostream stream(nullptr);
    return stream;
}

LoggerState* GetLoggerState() noexcept
{
    static LoggerState* state = []() noexcept -> LoggerState* {
        try {
            return new LoggerState(GetLogConfig());
        } catch (...) {
            // Fallback stderr logging remains available if logger construction fails.
            return nullptr;
        }
    }();
    return state;
}

std::string_view ModuleName(const char *file) noexcept
{
    if (file == nullptr) {
        return {};
    }
    std::string_view name(file);
    const size_t slash = name.find_last_of("/\\");
    if (slash != std::string_view::npos) {
        name.remove_prefix(slash + 1);
    }
    for (const std::string_view suffix : { std::string_view("-inl.h"), std::string_view(".cpp"),
                                           std::string_view(".cc"), std::string_view(".cxx"),
                                           std::string_view(".c"), std::string_view(".h"),
                                           std::string_view(".hpp") }) {
        if (name.size() >= suffix.size() && name.substr(name.size() - suffix.size()) == suffix) {
            name.remove_suffix(suffix.size());
            break;
        }
    }
    return name;
}

bool GlobMatches(std::string_view pattern, std::string_view value) noexcept
{
    size_t patternIndex = 0;
    size_t valueIndex = 0;
    size_t starIndex = std::string_view::npos;
    size_t starValueIndex = 0;
    while (valueIndex < value.size()) {
        if (patternIndex < pattern.size() &&
            (pattern[patternIndex] == '?' || pattern[patternIndex] == value[valueIndex])) {
            ++patternIndex;
            ++valueIndex;
            continue;
        }
        if (patternIndex < pattern.size() && pattern[patternIndex] == '*') {
            starIndex = patternIndex++;
            starValueIndex = valueIndex;
            continue;
        }
        if (starIndex != std::string_view::npos) {
            patternIndex = starIndex + 1;
            valueIndex = ++starValueIndex;
            continue;
        }
        return false;
    }
    while (patternIndex < pattern.size() && pattern[patternIndex] == '*') {
        ++patternIndex;
    }
    return patternIndex == pattern.size();
}

int EffectiveVlogLevel(const LogConfig &config, const char *file) noexcept
{
    const std::string_view module = ModuleName(file);
    for (const auto &rule : config.vmoduleRules) {
        if (GlobMatches(rule.pattern, module)) {
            return rule.level;
        }
    }
    return config.vlogLevel;
}

void WriteAll(int fd, const char *data, size_t size) noexcept
{
    while (size > 0) {
        const ssize_t written = write(fd, data, size);
        if (written > 0) {
            data += written;
            size -= static_cast<size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

void WriteFallback(LogSeverity severity, const char *file, int line, std::string_view message) noexcept
{
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    const bool locked = pthread_mutex_lock(&mutex) == 0;

    char prefix[K_FALLBACK_PREFIX_BUFFER_SIZE] = {};
    const int prefixSize = std::snprintf(prefix, sizeof(prefix), "[TransferEngine][%s] %s:%d] ",
                                         SeverityName(severity), file == nullptr ? "unknown" : file, line);
    if (prefixSize > 0) {
        WriteAll(STDERR_FILENO, prefix, std::min(static_cast<size_t>(prefixSize), sizeof(prefix) - 1));
    }
    WriteAll(STDERR_FILENO, message.data(), message.size());
    WriteAll(STDERR_FILENO, "\n", 1);
    if (locked) {
        (void)pthread_mutex_unlock(&mutex);
    }
}

void EmitLog(LogSeverity severity, const char *file, int line, std::string_view message) noexcept
{
    LoggerState* state = GetLoggerState();
    const bool emitted = state != nullptr && state->Log(severity, file, line, message);
    if (!emitted) {
        WriteFallback(severity, file, line, message);
    }

    if (severity == LogSeverity::FATAL) {
        FlushLogs();
        std::abort();
    }
}

void LogInvalidConfigOnce() noexcept
{
    static std::atomic<bool> logged{ false };
    if (logged.exchange(true, std::memory_order_relaxed)) {
        return;
    }
    const LogConfig &config = GetLogConfig();
    if (config.invalidLogLevel) {
        EmitLog(LogSeverity::WARNING, __FILE__, __LINE__,
                "invalid TRANSFER_ENGINE_LOG_LEVEL; falling back to INFO");
    }
    if (config.invalidVlogLevel) {
        EmitLog(LogSeverity::WARNING, __FILE__, __LINE__,
                "invalid TRANSFER_ENGINE_VLOG_LEVEL; falling back to 0");
    }
}

}  // namespace

bool ShouldLog(LogSeverity severity) noexcept
{
    const LogConfig &config = GetLogConfig();
    if (severity == LogSeverity::FATAL) {
        return true;
    }
    return config.minSeverity != LogSeverity::OFF && SeverityRank(severity) >= SeverityRank(config.minSeverity);
}

bool ShouldVLog(int level, const char *file) noexcept
{
    const LogConfig &config = GetLogConfig();
    return config.minSeverity != LogSeverity::OFF && SeverityRank(config.minSeverity) <= K_INFO_SEVERITY_RANK &&
           level >= 0 && level <= EffectiveVlogLevel(config, file);
}

void InitializeLogging() noexcept
{
    (void)GetLoggerState();
    LogInvalidConfigOnce();
}

void FlushLogs() noexcept
{
    LoggerState* state = GetLoggerState();
    if (state == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mutex);
    state->FlushUnlocked();
}

void EmitExternalLog(LogSeverity severity, int vlogLevel, const char *file, int line,
                     std::string_view message) noexcept
{
    if (vlogLevel >= 0) {
        if (!ShouldVLog(vlogLevel, file)) {
            return;
        }
    } else if (!ShouldLog(severity)) {
        return;
    }
    EmitLog(severity, file, line, message);
}

LogMessage::LogMessage(LogSeverity severity, const char *file, int line) noexcept
    : severity_(severity), file_(file), line_(line)
{
    try {
        stream_ = std::make_unique<std::ostringstream>();
    } catch (...) {
        // Allocation failure is handled by Stream() returning a null sink.
    }
}

LogMessage::~LogMessage() noexcept
{
    if (stream_ == nullptr) {
        if (severity_ == LogSeverity::FATAL) {
            EmitLog(severity_, file_, line_, "failed to allocate fatal log message");
        }
        return;
    }
    try {
        const std::string message = stream_->str();
        EmitLog(severity_, file_, line_, message);
    } catch (...) {
        WriteFallback(severity_, file_, line_, "failed to format log message");
        if (severity_ == LogSeverity::FATAL) {
            FlushLogs();
            std::abort();
        }
    }
}

std::ostream &LogMessage::Stream() noexcept
{
    return stream_ == nullptr ? GetNullStream() : *stream_;
}

}  // namespace internal
}  // namespace datasystem
