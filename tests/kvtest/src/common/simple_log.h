#pragma once
#include <iostream>
#include <sstream>
#include <mutex>

// Simple logging macros that avoid spdlog entirely.
// The SDK initializes its own spdlog internally; using spdlog from our code
// causes symbol conflicts with libds-spdlog.so.
// Thread-safe via static mutex; ostringstream captures the line before locking.

namespace slog_detail {
    inline std::mutex &LogMutex() {
        static std::mutex m;
        return m;
    }
}

#define SLOG_INFO(msg) do { \
    std::ostringstream _slog_ss; \
    _slog_ss << "[INFO] " << msg; \
    std::lock_guard<std::mutex> _slog_lk(slog_detail::LogMutex()); \
    std::cout << _slog_ss.str() << std::endl; \
} while(0)

#define SLOG_WARN(msg) do { \
    std::ostringstream _slog_ss; \
    _slog_ss << "[WARN] " << msg; \
    std::lock_guard<std::mutex> _slog_lk(slog_detail::LogMutex()); \
    std::cerr << _slog_ss.str() << std::endl; \
} while(0)

#define SLOG_ERROR(msg) do { \
    std::ostringstream _slog_ss; \
    _slog_ss << "[ERROR] " << msg; \
    std::lock_guard<std::mutex> _slog_lk(slog_detail::LogMutex()); \
    std::cerr << _slog_ss.str() << std::endl; \
} while(0)
