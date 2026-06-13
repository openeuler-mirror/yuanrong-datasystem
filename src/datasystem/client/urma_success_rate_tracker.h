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
 * Description: Request-driven URMA data-plane success-rate tracker.
 */
#ifndef DATASYSTEM_CLIENT_URMA_SUCCESS_RATE_TRACKER_H
#define DATASYSTEM_CLIENT_URMA_SUCCESS_RATE_TRACKER_H

#include <atomic>
#include <cstdint>
#include <limits>

namespace datasystem {
namespace client {
class UrmaSuccessRateTracker {
public:
    struct WindowStats {
        uint64_t successCount{ 0 };
        uint64_t failureCount{ 0 };
        uint64_t totalCount{ 0 };
    };

    UrmaSuccessRateTracker() = default;
    ~UrmaSuccessRateTracker() = default;

    /**
     * @brief Record a URMA data-plane request result and evaluate failover trigger.
     * @param[in] success Whether the URMA request succeeded.
     * @param[in] windowLengthMs Sliding window duration in milliseconds.
     * @param[in] nowMs Current timestamp in milliseconds.
     * @param[out] windowStats Optional statistics for the settled window.
     * @return True if a worker switch should be triggered; false otherwise.
     */
    bool RecordUrmaResult(bool success, uint64_t windowLengthMs, uint64_t nowMs, WindowStats *windowStats = nullptr);

    /**
     * @brief Mark the current switch attempt as finished and update backoff state.
     * @param[in] success Whether the worker switch succeeded.
     * @param[in] nowMs Current timestamp in milliseconds.
     */
    void FinishSwitchAttempt(bool success, uint64_t nowMs);

    /**
     * @brief Reset all window counters and backoff state to initial values.
     */
    void ResetWindow();

    /**
     * @brief Check whether a worker switch is currently in progress.
     * @return True if a switch attempt is ongoing.
     */
    bool IsSwitchInProgress() const;

    /**
     * @brief Get the timestamp after which a new worker switch may be attempted.
     * @return Next allowed switch time in milliseconds; 0 if no backoff is active.
     */
    uint64_t GetNextAllowedSwitchTimeMs() const;

    /**
     * @brief Get the current backoff level for failed switch attempts.
     * @return Backoff level (0 means no backoff; higher levels mean longer intervals).
     */
    uint32_t GetBackoffLevel() const;

    /**
     * @brief Get total sample count in current window (test only).
     * @return The sample count.
     */
    uint64_t GetCurrentSampleCountForTest() const;

private:
    struct WindowSnapshot {
        uint64_t successCount;
        uint64_t failureCount;
    };

    /**
     * @brief Decide whether an expired window snapshot should trigger a switch.
     * @param[in] snapshot The expired window's success/failure counts.
     * @param[in] nowMs Current timestamp in milliseconds.
     * @return True if the switch is triggered and atomically claimed.
     */
    bool ShouldTriggerSwitch(const WindowSnapshot &snapshot, uint64_t nowMs);

    /**
     * @brief Fill an optional WindowStats output from a settled window snapshot.
     * @param[out] windowStats Destination pointer; skipped if nullptr.
     * @param[in] snapshot The settled window's success/failure counts.
     */
    static void SetWindowStats(WindowStats *windowStats, const WindowSnapshot &snapshot);

    /**
     * @brief Increment success or failure counter in the current window.
     * @param[in] success Whether the request succeeded.
     */
    void RecordInCurrentWindow(bool success);

    /**
     * @brief Check whether the window success rate is below the threshold.
     * @param[in] snapshot Window success/failure counts.
     * @param[in] successRateRatio Failover trigger threshold as a success-rate ratio (0.0-1.0).
     * @return True if the success rate is below the threshold.
     */
    static bool IsUnhealthy(const WindowSnapshot &snapshot, double successRateRatio);
    static constexpr uint64_t WINDOW_NOT_STARTED = std::numeric_limits<uint64_t>::max();

    std::atomic<uint64_t> successCount_{ 0 };
    std::atomic<uint64_t> failureCount_{ 0 };
    std::atomic<uint64_t> windowStartTimeMs_{ WINDOW_NOT_STARTED };
    std::atomic<bool> switchInProgress_{ false };
    std::atomic<uint64_t> nextAllowedSwitchTimeMs_{ 0 };
    std::atomic<uint32_t> backoffLevel_{ 0 };
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_URMA_SUCCESS_RATE_TRACKER_H
