/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/shared_memory/delayed_release_shm_manager.h"

#include <algorithm>

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace {
constexpr auto DELAYED_RELEASE_REPORT_INTERVAL = std::chrono::seconds(10);
}  // namespace

DelayedReleaseShmManager::DelayedReleaseShmManager()
    : releaseThread_(&DelayedReleaseShmManager::Run, this)
{
}

DelayedReleaseShmManager::~DelayedReleaseShmManager()
{
    Stop();
}

DelayedReleaseShmManager &DelayedReleaseShmManager::Instance()
{
    static DelayedReleaseShmManager instance;
    return instance;
}

void DelayedReleaseShmManager::Add(const std::shared_ptr<ShmUnit> &shmUnit, std::chrono::milliseconds delay)
{
    if (shmUnit == nullptr) {
        return;
    }
    const auto size = shmUnit->size;
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) {
        return;
    }
    delayReleaseQueue_.push({ std::chrono::steady_clock::now() + delay, shmUnit });
    pendingBytes_ += size;
    lock.unlock();
    cv_.notify_one();
}

void DelayedReleaseShmManager::Run()
{
    auto nextReportTime = std::chrono::steady_clock::now() + DELAYED_RELEASE_REPORT_INTERVAL;
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_ || !delayReleaseQueue_.empty()) {
        if (delayReleaseQueue_.empty()) {
            cv_.wait(lock, [this] { return stopping_ || !delayReleaseQueue_.empty(); });
            nextReportTime = std::chrono::steady_clock::now() + DELAYED_RELEASE_REPORT_INTERVAL;
            continue;
        }

        const auto nextReleaseTime = delayReleaseQueue_.top().releaseTime;
        const auto wakeTime = std::min(nextReleaseTime, nextReportTime);
        cv_.wait_until(lock, wakeTime);

        const auto now = std::chrono::steady_clock::now();
        if (now >= nextReportTime) {
            const auto pendingCount = delayReleaseQueue_.size();
            const auto pendingBytes = pendingBytes_;
            nextReportTime = now + DELAYED_RELEASE_REPORT_INTERVAL;
            lock.unlock();
            LOG(INFO) << "[DELAYED_RELEASE_SHM_STATUS] pendingCount=" << pendingCount
                      << ", pendingBytes=" << pendingBytes;
            lock.lock();
            continue;
        }
        if (delayReleaseQueue_.empty() || delayReleaseQueue_.top().releaseTime > now) {
            continue;
        }

        auto shmUnit = delayReleaseQueue_.top().shmUnit;
        delayReleaseQueue_.pop();
        pendingBytes_ -= shmUnit->size;
        const auto pendingCount = delayReleaseQueue_.size();
        const auto pendingBytes = pendingBytes_;
        lock.unlock();
        LOG_EVERY_T(WARNING, DELAY_RELEASE_LOG_INTERVAL_SEC)
            << "[DELAY_RELEASE_DONE] id=" << shmUnit->id
            << ", identity=" << shmUnit->GetIdentity() << ", bytes=" << shmUnit->size
            << ", pendingCount=" << pendingCount << ", pendingBytes=" << pendingBytes;
        shmUnit.reset();
        lock.lock();
    }
}

void DelayedReleaseShmManager::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    cv_.notify_all();
    if (releaseThread_.joinable()) {
        releaseThread_.join();
    }
}

}  // namespace datasystem
