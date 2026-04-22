// Stub spdlog that uses std::cerr/std::cout to avoid conflicting with SDK's libds-spdlog.so
#pragma once
#include <iostream>
#include <string>

namespace ds_spdlog {

enum class level { trace, debug, info, warn, err, critical, off };

// Minimal stubs
template<typename... Args>
inline void info(const std::string& fmt, Args&&... args) {
    std::cout << "[INFO] " << fmt << std::endl;
}

template<typename... Args>
inline void warn(const std::string& fmt, Args&&... args) {
    std::cerr << "[WARN] " << fmt << std::endl;
}

template<typename... Args>
inline void error(const std::string& fmt, Args&&... args) {
    std::cerr << "[ERROR] " << fmt << std::endl;
}

inline void set_level(level) {}
inline void set_pattern(const std::string&) {}

} // namespace ds_spdlog
