#ifndef TRANSFER_ENGINE_INTERNAL_LOG_LOGGING_H
#define TRANSFER_ENGINE_INTERNAL_LOG_LOGGING_H

#include <memory>
#include <optional>
#include <ostream>
#include <sstream>
#include <string_view>

namespace datasystem {
namespace internal {

enum class LogSeverity {
    DEBUG = 0,
    INFO = 1,
    WARNING = 2,
    ERROR = 3,
    FATAL = 4,
    OFF = 5,
};

bool ShouldLog(LogSeverity severity) noexcept;
bool ShouldVLog(int level, const char *file) noexcept;
void InitializeLogging() noexcept;
void FlushLogs() noexcept;
void EmitExternalLog(LogSeverity severity, int vlogLevel, const char *file, int line,
                     std::string_view message) noexcept;

class LogMessage {
public:
    LogMessage(LogSeverity severity, const char *file, int line) noexcept;
    ~LogMessage() noexcept;

    std::ostream &Stream() noexcept;

private:
    LogSeverity severity_;
    const char *file_;
    int line_;
    std::unique_ptr<std::ostringstream> stream_;
};

class LogMessageGuard {
public:
    LogMessageGuard(LogSeverity severity, bool enabled, const char *file, int line) noexcept
    {
        if (enabled) {
            message_.emplace(severity, file, line);
        }
    }

    ~LogMessageGuard() noexcept = default;

    bool Enabled() const noexcept
    {
        return message_.has_value();
    }

    void Disable() noexcept
    {
        message_.reset();
    }

    std::ostream &Stream() noexcept
    {
        return message_->Stream();
    }

private:
    std::optional<LogMessage> message_;
};

}  // namespace internal
}  // namespace datasystem

#define TE_LOG_DEBUG                                                                                         \
    for (::datasystem::internal::LogMessageGuard teLogDebugGuard(                                             \
             ::datasystem::internal::LogSeverity::DEBUG,                                                      \
             ::datasystem::internal::ShouldLog(::datasystem::internal::LogSeverity::DEBUG), __FILE__,         \
             __LINE__);                                                                                      \
         teLogDebugGuard.Enabled(); teLogDebugGuard.Disable())                                                \
    teLogDebugGuard.Stream()

#define TE_LOG_INFO                                                                                          \
    for (::datasystem::internal::LogMessageGuard teLogInfoGuard(                                              \
             ::datasystem::internal::LogSeverity::INFO,                                                       \
             ::datasystem::internal::ShouldLog(::datasystem::internal::LogSeverity::INFO), __FILE__,          \
             __LINE__);                                                                                      \
         teLogInfoGuard.Enabled(); teLogInfoGuard.Disable())                                                  \
    teLogInfoGuard.Stream()

#define TE_LOG_WARNING                                                                                       \
    for (::datasystem::internal::LogMessageGuard teLogWarningGuard(                                           \
             ::datasystem::internal::LogSeverity::WARNING,                                                    \
             ::datasystem::internal::ShouldLog(::datasystem::internal::LogSeverity::WARNING), __FILE__,       \
             __LINE__);                                                                                      \
         teLogWarningGuard.Enabled(); teLogWarningGuard.Disable())                                            \
    teLogWarningGuard.Stream()

#define TE_LOG_ERROR                                                                                         \
    for (::datasystem::internal::LogMessageGuard teLogErrorGuard(                                             \
             ::datasystem::internal::LogSeverity::ERROR,                                                      \
             ::datasystem::internal::ShouldLog(::datasystem::internal::LogSeverity::ERROR), __FILE__,         \
             __LINE__);                                                                                      \
         teLogErrorGuard.Enabled(); teLogErrorGuard.Disable())                                                \
    teLogErrorGuard.Stream()

#define TE_LOG_FATAL                                                                                         \
    for (::datasystem::internal::LogMessageGuard teLogFatalGuard(                                             \
             ::datasystem::internal::LogSeverity::FATAL,                                                      \
             ::datasystem::internal::ShouldLog(::datasystem::internal::LogSeverity::FATAL), __FILE__,         \
             __LINE__);                                                                                      \
         teLogFatalGuard.Enabled(); teLogFatalGuard.Disable())                                                \
    teLogFatalGuard.Stream()

#define TE_VLOG_1                                                                                            \
    for (::datasystem::internal::LogMessageGuard teVLog1Guard(                                                \
             ::datasystem::internal::LogSeverity::DEBUG, ::datasystem::internal::ShouldVLog(1, __FILE__),     \
             __FILE__, __LINE__);                                                                            \
         teVLog1Guard.Enabled(); teVLog1Guard.Disable())                                                      \
    teVLog1Guard.Stream()

#endif  // TRANSFER_ENGINE_INTERNAL_LOG_LOGGING_H
