/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Lock helper for lock congestion diagnostic
 */

#ifndef DATASYSTEM_LOCK_HELPER_H
#define DATASYSTEM_LOCK_HELPER_H

#include <algorithm>
#include <mutex>
#include <shared_mutex>
#include <type_traits>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/timer.h"

#define LOCK_ARGS(_mutex) \
    (_mutex), [funName = __FUNCTION__] { return FormatString("%s, %s:%s", #_mutex, funName, __LINE__); }

#define LOCK_ARGS_MSG(_mutex, _msg) \
    (_mutex), [funName = __FUNCTION__, &_msg] { return FormatString("%s %s, %s:%s", _msg, #_mutex, funName, __LINE__); }

#define LOCK_ARGS_MSG_FN(_mutex, _msgFn) \
    (_mutex),                            \
        [this, funName = __FUNCTION__] { return FormatString("%s %s, %s:%s", _msgFn(), #_mutex, funName, __LINE__); }

#define DEFER_LOCK_ARGS(_mutex)                                                                           \
    (_mutex), [funName = __FUNCTION__] { return FormatString("%s, %s:%s", #_mutex, funName, __LINE__); }, \
        std::defer_lock

#define DEFER_LOCK_ARGS_MSG(_mutex, _msg)                                                                           \
    (_mutex),                                                                                                       \
        [funName = __FUNCTION__, &_msg] { return FormatString("%s %s, %s:%s", _msg, #_mutex, funName, __LINE__); }, \
        std::defer_lock

#define DEFER_LOCK_ARGS_MSG_FN(_mutex, _msgFn)                                                                         \
    (_mutex),                                                                                                          \
        [this, funName = __FUNCTION__] { return FormatString("%s %s, %s:%s", _msgFn(), #_mutex, funName, __LINE__); }, \
        std::defer_lock

namespace datasystem {
static constexpr int LOCK_NORMAL_LOG_LEVEL = 1;     // Normal output log level
static constexpr int LOCK_INTERNAL_LOG_LEVEL = 2;   // Internal output log level
static constexpr int LOCK_DEBUG_LOG_LEVEL = 3;      // Debug output log level
static constexpr int LOCK_WAIT_TIMEOUT_LIMIT = 10;  // 10s
static constexpr int LOCK_HOLD_TIMEOUT_LIMIT = 5;   // 5s

template <typename F, typename R = typename std::result_of<F()>::type,
          typename std::enable_if<std::is_same<R, std::string>::value>::type * = nullptr>
class TryLockHelper {
public:
    TryLockHelper(F &&f) : func_(std::move(f))
    {
    }

    ~TryLockHelper()
    {
        auto elapsed = t_.ElapsedSecond();
        if (elapsed > LOCK_HOLD_TIMEOUT_LIMIT || VLOG_IS_ON(LOCK_DEBUG_LOG_LEVEL)) {
            LOG(WARNING) << FormatString("[%s] Wait and hold lock duration [%.6lf]s", func_(), elapsed);
        }
    }

    template <typename T>
    void AcquireLock(T &lock)
    {
        bool isTimeout = false;
        while (!lock.try_lock_for(std::chrono::seconds(LOCK_WAIT_TIMEOUT_LIMIT))) {
            if (!isTimeout) {
                LOG(WARNING) << FormatString("[%s] Acquire lock takes longer than %d s.", func_(),
                                             LOCK_WAIT_TIMEOUT_LIMIT);
                isTimeout = true;
            }
        }
        if (isTimeout || VLOG_IS_ON(LOCK_DEBUG_LOG_LEVEL)) {
            LOG(WARNING) << FormatString("[%s] Acquire lock takes [%.6lf]s", func_(), t_.ElapsedSecond());
        }
    }

    template <typename T>
    bool TryLock(T &lock, const int timeoutSec = LOCK_WAIT_TIMEOUT_LIMIT)
    {
        bool acquired = lock.try_lock_for(std::chrono::seconds(timeoutSec));
        if (t_.ElapsedSecond() > LOCK_WAIT_TIMEOUT_LIMIT || VLOG_IS_ON(LOCK_DEBUG_LOG_LEVEL)) {
            LOG(WARNING) << FormatString("[%s] Try lock takes [%.6lf]s", func_(), t_.ElapsedSecond());
        }
        return acquired;
    }

private:
    Timer t_;
    F func_;
};

template <typename T, typename F>
class ReadLockHelper : public std::shared_lock<T>, public TryLockHelper<F> {
public:
    ReadLockHelper(T &mux, F &&f) : std::shared_lock<T>(mux, std::defer_lock), TryLockHelper<F>(std::move(f))
    {
        AcquireLock();
    }
    ~ReadLockHelper() = default;

    ReadLockHelper(T &mux, F &&f, std::defer_lock_t)
        : std::shared_lock<T>(mux, std::defer_lock), TryLockHelper<F>(std::move(f))
    {
    }

    void AcquireLock()
    {
        TryLockHelper<F>::AcquireLock(*this);
    }
};

template <typename T, typename F>
class WriteLockHelper : public std::unique_lock<T>, public TryLockHelper<F> {
public:
    WriteLockHelper(T &mux, F &&f) : std::unique_lock<T>(mux, std::defer_lock), TryLockHelper<F>(std::move(f))
    {
        AcquireLock();
    }

    WriteLockHelper(T &mux, F &&f, std::defer_lock_t)
        : std::unique_lock<T>(mux, std::defer_lock), TryLockHelper<F>(std::move(f))
    {
    }

    ~WriteLockHelper() = default;

    void AcquireLock()
    {
        TryLockHelper<F>::AcquireLock(*this);
    }

    bool TryLock(const int timeoutSec = LOCK_WAIT_TIMEOUT_LIMIT)
    {
        return TryLockHelper<F>::TryLock(*this, timeoutSec);
    }
};
}  // namespace datasystem
#endif  // DATASYSTEM_LOCK_HELPER_H
