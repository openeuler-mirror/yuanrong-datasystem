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
#include "datasystem/client/urma_success_rate_tracker.h"

#include "datasystem/common/flags/flags.h"

DS_DECLARE_double(urma_failover_success_rate_ratio);
DS_DECLARE_uint32(urma_failover_min_sample_count);

namespace datasystem {
namespace client {
namespace {
constexpr uint64_t BACKOFF_INTERVALS_MS[] = { 1000, 2000, 4000, 8000, 10000 };
constexpr uint32_t MAX_BACKOFF_LEVEL = sizeof(BACKOFF_INTERVALS_MS) / sizeof(BACKOFF_INTERVALS_MS[0]) - 1;
}  // namespace

bool UrmaSuccessRateTracker::RecordUrmaResult(bool success, uint64_t windowLengthMs, uint64_t nowMs,
                                              WindowStats *windowStats)
{
    if (FLAGS_urma_failover_success_rate_ratio <= 0.0 || windowLengthMs == 0) {
        ResetWindow();
        return false;
    }
    while (true) {
        uint64_t start = windowStartTimeMs_.load(std::memory_order_acquire);
        if (start == WINDOW_NOT_STARTED) {
            if (!windowStartTimeMs_.compare_exchange_strong(start, nowMs, std::memory_order_acq_rel)) {
                continue;
            }
            RecordInCurrentWindow(success);
            return false;
        }
        if (nowMs - start < windowLengthMs) {
            RecordInCurrentWindow(success);
            return false;
        }
        if (!windowStartTimeMs_.compare_exchange_strong(start, nowMs, std::memory_order_acq_rel)) {
            continue;
        }
        WindowSnapshot snapshot{ successCount_.exchange(0, std::memory_order_acq_rel),
                                 failureCount_.exchange(0, std::memory_order_acq_rel) };
        RecordInCurrentWindow(success);
        SetWindowStats(windowStats, snapshot);
        return ShouldTriggerSwitch(snapshot, nowMs);
    }
}

void UrmaSuccessRateTracker::FinishSwitchAttempt(bool success, uint64_t nowMs)
{
    if (!success) {
        uint32_t level = std::min(backoffLevel_.load(std::memory_order_relaxed), MAX_BACKOFF_LEVEL);
        nextAllowedSwitchTimeMs_.store(nowMs + BACKOFF_INTERVALS_MS[level], std::memory_order_release);
        backoffLevel_.store(std::min(level + 1, MAX_BACKOFF_LEVEL), std::memory_order_release);
    }
    switchInProgress_.store(false, std::memory_order_release);
}

void UrmaSuccessRateTracker::ResetWindow()
{
    successCount_.store(0, std::memory_order_release);
    failureCount_.store(0, std::memory_order_release);
    windowStartTimeMs_.store(WINDOW_NOT_STARTED, std::memory_order_release);
    switchInProgress_.store(false, std::memory_order_release);
    nextAllowedSwitchTimeMs_.store(0, std::memory_order_release);
    backoffLevel_.store(0, std::memory_order_release);
}

bool UrmaSuccessRateTracker::IsSwitchInProgress() const
{
    return switchInProgress_.load(std::memory_order_acquire);
}

uint64_t UrmaSuccessRateTracker::GetNextAllowedSwitchTimeMs() const
{
    return nextAllowedSwitchTimeMs_.load(std::memory_order_acquire);
}

uint32_t UrmaSuccessRateTracker::GetBackoffLevel() const
{
    return backoffLevel_.load(std::memory_order_acquire);
}

uint64_t UrmaSuccessRateTracker::GetCurrentSampleCountForTest() const
{
    return successCount_.load(std::memory_order_acquire) + failureCount_.load(std::memory_order_acquire);
}

bool UrmaSuccessRateTracker::ShouldTriggerSwitch(const WindowSnapshot &snapshot, uint64_t nowMs)
{
    uint64_t total = snapshot.successCount + snapshot.failureCount;
    if (total < FLAGS_urma_failover_min_sample_count) {
        return false;
    }
    if (!IsUnhealthy(snapshot, FLAGS_urma_failover_success_rate_ratio)) {
        backoffLevel_.store(0, std::memory_order_release);
        nextAllowedSwitchTimeMs_.store(0, std::memory_order_release);
        return false;
    }
    if (nowMs < nextAllowedSwitchTimeMs_.load(std::memory_order_acquire)) {
        return false;
    }
    bool expected = false;
    return switchInProgress_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}

void UrmaSuccessRateTracker::SetWindowStats(WindowStats *windowStats, const WindowSnapshot &snapshot)
{
    if (windowStats == nullptr) {
        return;
    }
    windowStats->successCount = snapshot.successCount;
    windowStats->failureCount = snapshot.failureCount;
    windowStats->totalCount = snapshot.successCount + snapshot.failureCount;
}

void UrmaSuccessRateTracker::RecordInCurrentWindow(bool success)
{
    if (success) {
        successCount_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    failureCount_.fetch_add(1, std::memory_order_relaxed);
}

bool UrmaSuccessRateTracker::IsUnhealthy(const WindowSnapshot &snapshot, double successRateRatio)
{
    uint64_t total = snapshot.successCount + snapshot.failureCount;
    return static_cast<double>(snapshot.successCount) < static_cast<double>(total) * successRateRatio;
}
}  // namespace client
}  // namespace datasystem
