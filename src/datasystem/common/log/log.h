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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/spdlog/log_message.h"
#include "datasystem/common/log/spdlog/log_param.h"

DS_DECLARE_int32(v);
DS_DECLARE_int32(minloglevel);
DS_DECLARE_int32(log_monitor_interval_ms);

namespace datasystem {

// Default when FLAGS_log_monitor_interval_ms <= 0 (must match gflag default in res_metric_collector.cpp).
static constexpr int DEFAULT_LOG_MONITOR_INTERVAL_MS = 10000;

// Thread-safe throttle for LOG_FIRST_EVERY_N: first log after each monitor interval,
// then every n hits (relaxed atomics; ordering is best-effort for log spam control).
inline bool LogFirstEveryNShouldEmit(int n, int intervalMs, std::atomic<int64_t> &lastNs,
    std::atomic<int> &counter)
{
    if (n < 1) {
        return false;
    }
    namespace ch = std::chrono;
    const auto now = ch::steady_clock::now();
    const int64_t nowNs = ch::duration_cast<ch::nanoseconds>(now.time_since_epoch()).count();

    int64_t last = lastNs.load(std::memory_order_relaxed);
    if (last == 0) {
        lastNs.store(nowNs, std::memory_order_relaxed);
        last = nowNs;
    }

    const int iv = intervalMs > 0 ? intervalMs : DEFAULT_LOG_MONITOR_INTERVAL_MS;
    const int64_t intervalNs = static_cast<int64_t>(iv) * 1000000LL;

    // Single CAS attempt: on contention, do not spin; fall through to fetch_add (best-effort throttle).
    last = lastNs.load(std::memory_order_relaxed);
    if (nowNs - last >= intervalNs) {
        if (lastNs.compare_exchange_weak(last, nowNs, std::memory_order_release, std::memory_order_relaxed)) {
            counter.store(n - 1, std::memory_order_relaxed);
            return true;
        }
    }

    const int prev = counter.fetch_add(1, std::memory_order_relaxed);
    return ((prev + 1) % n == 0);
}

#define DS_LOGS_LEVEL_INFO datasystem::LogSeverity::INFO
#define DS_LOGS_LEVEL_WARNING datasystem::LogSeverity::WARNING
#define DS_LOGS_LEVEL_ERROR datasystem::LogSeverity::ERROR
#define DS_LOGS_LEVEL_FATAL datasystem::LogSeverity::FATAL

static constexpr int32_t HEARTBEAT_LEVEL = 3;  // Heartbeat log level

// `X_##__LINE__` does not expand __LINE__; use DS_LOG_PP_CAT(X_, __LINE__) for per-line unique names.
#define DS_LOG_PP_CAT_I(a, b) a##b
#define DS_LOG_PP_CAT(a, b) DS_LOG_PP_CAT_I(a, b)

inline bool ShouldLogFirstAndEveryN(uint32_t n, std::atomic<uint64_t> &counter)
{
    const uint64_t current = counter.fetch_add(1, std::memory_order_relaxed) + 1;
    return (current == 1) || (n > 0 && (current % n == 0));
}

// Basic Logging Macros Impl
#define LOG_IMPL(severity) datasystem::LogMessage(DS_LOGS_LEVEL_##severity, __FILE__, __LINE__).Stream()

// Conditional Logging Macros
#define LOG_IF(severity, condition)                                                         \
    if ((condition) && datasystem::ShouldCreateLogMessage(DS_LOGS_LEVEL_##severity))         \
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

#define LOG_FIRST_AND_EVERY_N(severity, n)                                                                  \
    static std::atomic<uint64_t> DS_LOG_PP_CAT(LOG_FIRST_AND_EVERY_N_COUNTER_, __LINE__){ 0 };             \
    if (datasystem::ShouldLogFirstAndEveryN((n), DS_LOG_PP_CAT(LOG_FIRST_AND_EVERY_N_COUNTER_, __LINE__))) \
    LOG(severity)

// First log each monitor interval, then every N calls in that interval (see LogFirstEveryNShouldEmit).
// Style matches LOG_IF_EVERY_N: static state per __LINE__, then if (...) LOG(...).
#define LOG_FIRST_EVERY_N(severity, n)                                                                      \
    static std::atomic<int64_t> DS_LOG_FTE_LAST_NS_##__LINE__{ 0 };                                          \
    static std::atomic<int> DS_LOG_FTE_CTR_##__LINE__{ (n) - 1 };                                            \
    if (datasystem::LogFirstEveryNShouldEmit(                                                                \
            (n), FLAGS_log_monitor_interval_ms, DS_LOG_FTE_LAST_NS_##__LINE__, DS_LOG_FTE_CTR_##__LINE__))   \
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
