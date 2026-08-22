#pragma once
#include <iostream>
#include <sstream>
#include <mutex>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <string>

// Simple logging macros that avoid spdlog entirely.
// The SDK initializes its own spdlog internally; using spdlog from our code
// causes symbol conflicts with libds-spdlog.so.
// Thread-safe via static mutex; ostringstream captures the line before locking.
// Each line is prefixed with a wall-clock timestamp (YYYY-MM-DD HH:MM:SS.mmm
// in local time) so logs in metrics_*/run.log are correlatable across threads
// and across peers without cross-referencing the directory timestamp.

namespace slog_detail {
inline std::mutex &LogMutex()
{
    static std::mutex m;
    return m;
}

// Returns local-time wall clock as "YYYY-MM-DD HH:MM:SS.mmm".
// Stack-only; no heap allocation on the hot path. Computed before the
// log mutex is taken so timestamp formatting never contends across
// threads. localtime_r is POSIX; matches the pattern in metrics.cpp.
inline std::string Timestamp()
{
    using namespace std::chrono;
    auto now = system_clock::now();
    auto nowT = system_clock::to_time_t(now);
    std::tm tmBuf;
    localtime_r(&nowT, &tmBuf);
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03d", tmBuf.tm_year + 1900, tmBuf.tm_mon + 1,
                  tmBuf.tm_mday, tmBuf.tm_hour, tmBuf.tm_min, tmBuf.tm_sec, static_cast<int>(ms.count()));
    return std::string(buf);
}
}  // namespace slog_detail

#define SLOG_INFO(msg)                                                 \
    do {                                                               \
        std::ostringstream _slog_ss;                                   \
        _slog_ss << slog_detail::Timestamp() << " [INFO] " << msg;     \
        std::lock_guard<std::mutex> _slog_lk(slog_detail::LogMutex()); \
        std::cout << _slog_ss.str() << std::endl;                      \
    } while (0)

#define SLOG_WARN(msg)                                                 \
    do {                                                               \
        std::ostringstream _slog_ss;                                   \
        _slog_ss << slog_detail::Timestamp() << " [WARN] " << msg;     \
        std::lock_guard<std::mutex> _slog_lk(slog_detail::LogMutex()); \
        std::cerr << _slog_ss.str() << std::endl;                      \
    } while (0)

#define SLOG_ERROR(msg)                                                \
    do {                                                               \
        std::ostringstream _slog_ss;                                   \
        _slog_ss << slog_detail::Timestamp() << " [ERROR] " << msg;    \
        std::lock_guard<std::mutex> _slog_lk(slog_detail::LogMutex()); \
        std::cerr << _slog_ss.str() << std::endl;                      \
    } while (0)
