#include "internal/log/logging.h"

#include <iomanip>
#include <mutex>
#include <unistd.h>

#include <glog/logging.h>

namespace datasystem {
namespace internal {
namespace {

void CustomPrefixFormatter(std::ostream &stream, const google::LogMessage &message, void *)
{
    const auto &t = message.time();
    const auto &tm = t.tm();
    char sev = 'I';
    switch (message.severity()) {
        case google::GLOG_INFO:
            sev = 'I';
            break;
        case google::GLOG_WARNING:
            sev = 'W';
            break;
        case google::GLOG_ERROR:
            sev = 'E';
            break;
        case google::GLOG_FATAL:
            sev = 'F';
            break;
        default:
            sev = 'I';
            break;
    }

    stream.fill('0');
    stream << sev
           << std::setw(4) << 1900 + tm.tm_year
           << std::setw(2) << 1 + tm.tm_mon
           << std::setw(2) << tm.tm_mday << ' '
           << std::setw(2) << tm.tm_hour << ':'
           << std::setw(2) << tm.tm_min << ':'
           << std::setw(2) << tm.tm_sec << '.'
           << std::setw(6) << t.usec() << ' ';
    stream.fill(' ');
#if defined(__linux__)
    #include <unistd.h>
    #include <sys/syscall.h>

    const auto linuxTid = static_cast<long>(syscall(SYS_gettid));
    stream << getpid() << ':' << linuxTid << ' ';
#else
    stream << getpid() << ':' << message.thread_id() << ' ';
#endif
    stream << message.basename() << ':' << message.line() << "]";
}

}  // namespace

void EnsureGlogInitialized(const char *programName)
{
    static std::mutex initMutex;
    std::lock_guard<std::mutex> lock(initMutex);
    if (google::IsGoogleLoggingInitialized()) {
        google::InstallPrefixFormatter(&CustomPrefixFormatter, nullptr);
        return;
    }
    google::InitGoogleLogging(programName == nullptr ? "transfer_engine" : programName);
    google::InstallPrefixFormatter(&CustomPrefixFormatter, nullptr);
}

}  // namespace internal
}  // namespace datasystem
