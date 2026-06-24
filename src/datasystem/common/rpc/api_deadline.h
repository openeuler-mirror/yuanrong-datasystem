/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Thread-local API-level deadline budget shared across the request call chain.
 */
#ifndef DATASYSTEM_COMMON_RPC_API_DEADLINE_H
#define DATASYSTEM_COMMON_RPC_API_DEADLINE_H

#include <chrono>
#include <vector>
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

/**
 * @brief ApiDeadline singleton class tracking the per-thread API deadline budget
 * shared by the client, worker, and master along one request call chain.
 */
class ApiDeadline {
public:
    /** @brief Get the thread-local singleton ApiDeadline instance. @return ApiDeadline instance. */
    static ApiDeadline &Instance();

    /**
     * @brief Initialize the deadline with a millisecond budget.
     * @param[in] requestTimeoutMs Deadline duration, in milliseconds.
     */
    void Init(int64_t requestTimeoutMs);

    /**
     * @brief Initialize the deadline with a microsecond budget.
     * @param[in] requestTimeoutUs Deadline duration, in microseconds.
     */
    void InitUs(int64_t requestTimeoutUs);

    /**
     * @brief Get the remaining deadline budget.
     * @return Remaining duration in microseconds; RPC_TIMEOUT*1000 when uninitialized.
     */
    int64_t ApiRemainingUs() const;

    /** @brief Check whether the deadline has been initialized. @return True if initialized. */
    bool IsInitialized() const;

    /**
     * @brief Verify the deadline has not been exceeded.
     * @return K_OK if time remains; K_RPC_DEADLINE_EXCEEDED otherwise.
     */
    Status CheckApiDeadline() const;

    /** @brief Save the current deadline state onto an internal stack for nested calls. */
    void Push();

    /** @brief Restore the most recently pushed deadline state. */
    void Pop();

    /** @brief Clear the deadline and the saved-state stack. */
    void Reset();

    ApiDeadline(const ApiDeadline&) = delete;
    ApiDeadline& operator=(const ApiDeadline&) = delete;
    ApiDeadline(ApiDeadline&&) = delete;
    ApiDeadline& operator=(ApiDeadline&&) = delete;

private:
    struct SaveState {
        std::chrono::steady_clock::time_point deadline;
        uint8_t initialized;
    };

    ApiDeadline() = default;
    ~ApiDeadline() = default;

    bool initialized_ = false;
    std::chrono::steady_clock::time_point deadline_;
    std::vector<SaveState> savedStates_;
};

/** @brief Tag type to select the microsecond overload of ApiDeadlineGuard. */
struct InUs {};

/**
 * @brief RAII guard that pushes the current ApiDeadline, installs a new budget,
 * and restores the previous one on destruction.
 */
class ApiDeadlineGuard {
public:
    /**
     * @brief Push the current deadline and initialize with a millisecond budget.
     * @param[in] timeoutMs Deadline duration, in milliseconds.
     */
    explicit ApiDeadlineGuard(int64_t timeoutMs)
    {
        if (timeoutMs <= 0) {
            LOG(WARNING) << FormatString(
                "ApiDeadlineGuard: timeoutMs=%ld <= 0, fallback to RPC_TIMEOUT=%dms.", timeoutMs, RPC_TIMEOUT);
            timeoutMs = RPC_TIMEOUT;
        }
        ApiDeadline::Instance().Push();
        ApiDeadline::Instance().Init(timeoutMs);
    }

    /**
     * @brief Push the current deadline and initialize with a microsecond budget.
     * @param[in] timeoutUs Deadline duration, in microseconds.
     * @param[in] InUs Disambiguating tag to select the microsecond overload.
     */
    ApiDeadlineGuard(int64_t timeoutUs, InUs)
    {
        ApiDeadline::Instance().Push();
        ApiDeadline::Instance().InitUs(timeoutUs);
    }

    /** @brief Restore the previously pushed deadline state. */
    ~ApiDeadlineGuard()
    {
        ApiDeadline::Instance().Pop();
    }

    ApiDeadlineGuard(const ApiDeadlineGuard&) = delete;
    ApiDeadlineGuard& operator=(const ApiDeadlineGuard&) = delete;
    ApiDeadlineGuard(ApiDeadlineGuard&&) = delete;
    ApiDeadlineGuard& operator=(ApiDeadlineGuard&&) = delete;
};

/**
 * @brief Re-initialize the per-request deadlines after a thread dispatch, deducting
 * the time spent waiting in the dispatch queue.
 *
 * Initializes both ApiDeadline and reqTimeoutDuration (from thread_local.h) with the
 * remaining budget. Callers that later read reqTimeoutDuration do not need a separate
 * Init call. The implementation includes thread_local.h for this purpose.
 * @param[in] capturedRemainingUs Remaining budget captured at dispatch time, in microseconds.
 * @param[in] dispatchTime Steady-clock time point when the request was dispatched.
 * @return K_OK if budget remains after queue wait; K_RPC_DEADLINE_EXCEEDED otherwise.
 */
Status InitTimeoutsFromDispatch(int64_t capturedRemainingUs, std::chrono::steady_clock::time_point dispatchTime);

/**
 * @brief Get the remaining budget for wire-level meta propagation.
 * Returns ApiDeadline's remaining if initialized (clamped to minimum 1us for expired),
 * or falls back to reqTimeoutDuration's real remaining if ApiDeadline is uninitialized.
 * @return Remaining duration in microseconds; minimum 1us if expired but initialized.
 */
int64_t GetRemainingUsForMeta();

}  // namespace datasystem
#endif
