#pragma once
#include <iostream>
#include <sstream>

// Simple logging macros that avoid spdlog entirely.
// The SDK initializes its own spdlog internally; using spdlog from our code
// causes symbol conflicts with libds-spdlog.so.

#define SLOG_INFO(msg) std::cout << "[INFO] " << msg << std::endl
#define SLOG_WARN(msg) std::cerr << "[WARN] " << msg << std::endl
#define SLOG_ERROR(msg) std::cerr << "[ERROR] " << msg << std::endl
