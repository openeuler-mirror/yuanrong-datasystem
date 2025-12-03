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
#ifndef DATASYSTEM_COMMON_LOG_LOG_H
#define DATASYSTEM_COMMON_LOG_LOG_H

#include <chrono>
#include <cstring>

#include "datasystem/common/log/spdlog/log_message.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/spdlog/log_param.h"

DS_DECLARE_int32(v);
DS_DECLARE_int32(minloglevel);

namespace datasystem {
#define DS_LOGS_LEVEL_INFO datasystem::LogSeverity::INFO
#define DS_LOGS_LEVEL_WARNING datasystem::LogSeverity::WARNING
#define DS_LOGS_LEVEL_ERROR datasystem::LogSeverity::ERROR
#define DS_LOGS_LEVEL_FATAL datasystem::LogSeverity::FATAL

static constexpr int32_t HEARTBEAT_LEVEL = 3;  // Heartbeat log level

// Basic Logging Macros Impl
#define LOG_IMPL(severity) datasystem::LogMessage(DS_LOGS_LEVEL_##severity, __FILE__, __LINE__).Stream()

// Conditional Logging Macros
#define LOG_IF(severity, condition) \
    if (condition)                  \
    LOG_IMPL(severity)

// Basic Logging Macros
#define LOG(severity) LOG_IF(severity, FLAGS_minloglevel <= DS_LOGS_LEVEL_##severity)

// Frequency-Controlled Logging Macros
#define LOG_EVERY_N(severity, n)                     \
    static int LOG_EVERY_N_COUNTER_##__LINE__ = 0;   \
    if (++LOG_EVERY_N_COUNTER_##__LINE__ % (n) == 0) \
    LOG(severity)

#define LOG_EVERY_T(severity, seconds)                                                                       \
    static auto LOG_EVERY_T_LAST_TIME_##__LINE__ = std::chrono::steady_clock::now();                         \
    auto LOG_EVERY_T_NOW_##__LINE__ = std::chrono::steady_clock::now();                                      \
    auto LOG_EVERY_T_ELAPSED_##__LINE__ = std::chrono::duration_cast<std::chrono::milliseconds>(             \
                                              LOG_EVERY_T_NOW_##__LINE__ - LOG_EVERY_T_LAST_TIME_##__LINE__) \
                                              .count();                                                      \
    if (LOG_EVERY_T_ELAPSED_##__LINE__ >= (seconds)*1000                                                     \
        && (LOG_EVERY_T_LAST_TIME_##__LINE__ = LOG_EVERY_T_NOW_##__LINE__, true))                            \
    LOG(severity)

#define LOG_FIRST_N(severity, n)                   \
    static int LOG_FIRST_N_COUNTER_##__LINE__ = 0; \
    if (LOG_FIRST_N_COUNTER_##__LINE__++ < (n))    \
    LOG(severity)

#define LOG_IF_EVERY_N(severity, condition, n)              \
    static int LOG_IF_EVERY_N_COUNTER_##__LINE__ = 0;       \
    if (condition)                                          \
        if (++LOG_IF_EVERY_N_COUNTER_##__LINE__ % (n) == 0) \
    LOG(severity)

// Verbose Logging Macros
#define VLOG_IS_ON(verboselevel) (2 >= verboselevel)

#define VLOG(verboselevel)       \
    if (FLAGS_v >= verboselevel) \
    LOG(INFO)

#define VLOG_IF(verboselevel, condition)        \
    if (FLAGS_v >= verboselevel && (condition)) \
    LOG(INFO)

#define VLOG_EVERY_N(verboselevel, n)                     \
    static int VLOG_EVERY_N_COUNTER_##__LINE__ = 0;       \
    if (FLAGS_v >= verboselevel)                          \
        if (++VLOG_EVERY_N_COUNTER_##__LINE__ % (n) == 0) \
    LOG(INFO)

// Assertion Macros
#define DS_CHECK_OP(op, val1, val2)                                                                                  \
    do {                                                                                                             \
        if (!((val1)op(val2))) {                                                                                     \
            LOG(FATAL) << "Check failed: " << #val1 " " #op " " #val2 << " (" << (val1) << " vs. " << (val2) << ")"; \
        }                                                                                                            \
    } while (0)

#define CHECK_EQ(a, b) DS_CHECK_OP(==, a, b)
#define CHECK_NE(a, b) DS_CHECK_OP(!=, a, b)
#define CHECK_LT(a, b) DS_CHECK_OP(<, a, b)
#define CHECK_LE(a, b) DS_CHECK_OP(<=, a, b)
#define CHECK_GT(a, b) DS_CHECK_OP(>, a, b)
#define CHECK_GE(a, b) DS_CHECK_OP(>=, a, b)

#define CHECK(condition) LOG_IF(FATAL, !(condition)) << "Check failed: " #condition " "

#define CHECK_NOTNULL(ptr)                                   \
    do {                                                     \
        if ((ptr) == nullptr) {                              \
            LOG(FATAL) << "Check failed: " #ptr " is NULL."; \
        }                                                    \
    } while (0)

inline bool SafeStringEqual(const char *s1, const char *s2)
{
    if (s1 == s2)
        return true;
    if (s1 == nullptr || s2 == nullptr)
        return false;
    return strcmp(s1, s2) == 0;
}

inline const char *SafeStringOutput(const char *s)
{
    return s ? s : "(null)";
}

#define CHECK_STRNE(s1, s2)                                                                                            \
    LOG_IF(FATAL, SafeStringEqual(s1, s2)) << "Check failed: " << #s1 << " != " << #s2 << " (" << SafeStringOutput(s1) \
                                           << " vs " << SafeStringOutput(s2) << ")"

#if defined(NDEBUG) && !defined(DCHECK_ALWAYS_ON)
#define DCHECK_IS_ON() 1
#else
#define DCHECK_IS_ON() 0
#endif

#if DCHECK_IS_ON()

#define DLOG(severity) LOG(severity)
#define DCHECK(condition) CHECK(condition)
#else

#define DLOG(severity) \
    while (false)      \
    LOG(severity)
#define DCHECK(condition) \
    while (false)         \
    CHECK(condition)
#endif

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_LOG_H
