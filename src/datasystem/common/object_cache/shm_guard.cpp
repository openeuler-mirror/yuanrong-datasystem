/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Implementation of ShmGuard.
 */

#include "datasystem/common/object_cache/shm_guard.h"

#include <atomic>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace {
constexpr int64_t SHM_GUARD_SLOW_FREE_THRESHOLD_MS = 1000;
constexpr uint32_t SHM_GUARD_SLOW_FREE_STREAK_THRESHOLD = 10;
constexpr int64_t SHM_GUARD_SLOW_FREE_REJECT_MS = 15000;

std::atomic<uint32_t> g_slowShmGuardFreeStreak{ 0 };
std::atomic<int64_t> g_rejectTcpPayloadUntilMs{ 0 };

int64_t NowSteadyMs()
{
    return static_cast<int64_t>(GetSteadyClockTimeStampMs());
}

void UpdateSlowFreeCircuit(int64_t holdMs)
{
    if (holdMs <= SHM_GUARD_SLOW_FREE_THRESHOLD_MS) {
        g_slowShmGuardFreeStreak.store(0);
        return;
    }

    auto streak = g_slowShmGuardFreeStreak.fetch_add(1) + 1;
    if (streak < SHM_GUARD_SLOW_FREE_STREAK_THRESHOLD) {
        return;
    }

    g_rejectTcpPayloadUntilMs.store(NowSteadyMs() + SHM_GUARD_SLOW_FREE_REJECT_MS);
    g_slowShmGuardFreeStreak.store(0);
    VLOG(1) << "[ShmGuard::Free] Open slow-free circuit breaker, slowStreak=" << streak
            << ", rejectMs=" << SHM_GUARD_SLOW_FREE_REJECT_MS;
}
}  // namespace

ShmGuard::ShmGuard(std::shared_ptr<ShmUnit> shmUnit, size_t dataSize, size_t metaSize)
    : impl_(std::make_shared<Impl>(std::move(shmUnit))), dataSize_(dataSize), metaSize_(metaSize)
{
}

void ShmGuard::EnableSlowFreeObserve()
{
    if (impl_ == nullptr) {
        return;
    }
    impl_->trackSlowFree.store(true);
}

Status ShmGuard::TryRLatch(bool retry)
{
    RETURN_RUNTIME_ERROR_IF_NULL(impl_);
    RETURN_RUNTIME_ERROR_IF_NULL(impl_->shmUnit);
    auto lockFrame = reinterpret_cast<uint32_t *>(impl_->shmUnit->GetPointer());
    auto tmpLock = std::make_shared<object_cache::ShmLock>(lockFrame, metaSize_, 0);
    RETURN_IF_NOT_OK(tmpLock->Init());

    static const int maxRetry = 20;
    static const int sleepTimeMs = 10;
    bool locked = false;
    for (int i = 0; i < maxRetry; i++) {
        locked = tmpLock->TryRLatch();
        if (locked || !retry) {
            break;
        }
        LOG(WARNING) << "Try read latch failed, try again...";
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(locked, K_RUNTIME_ERROR, "Try read latch failed");
    impl_->lock = std::move(tmpLock);
    return Status::OK();
}

#ifndef DISABLE_RPC
Status ShmGuard::TransferTo(std::vector<RpcMessage> &messages, const uint64_t offset, const uint64_t size)
{
    RETURN_RUNTIME_ERROR_IF_NULL(impl_);
    RETURN_RUNTIME_ERROR_IF_NULL(impl_->shmUnit);
    CHECK_FAIL_RETURN_STATUS(
        size < UINT64_MAX - offset && offset + size <= dataSize_, K_RUNTIME_ERROR,
        FormatString("invalid read offset %zu and size %zu, data size %zu", offset, size, dataSize_));
    const size_t maxInt = std::numeric_limits<int32_t>::max();
    auto remaining = size == 0 ? dataSize_ - offset : size;
    auto ffn = ShmGuard::Free;
    auto ptr = static_cast<uint8_t *>(impl_->shmUnit->GetPointer()) + metaSize_ + offset;
    // We measure one hold time for the whole transfer: from the first frame handoff until the last frame releases.
    impl_->transferStartTimeMs.store(NowSteadyMs());
    impl_->remainingFrames.store(0);
    while (remaining > 0) {
        int32_t bufSize = std::min(remaining, maxInt);
        auto hint = std::make_unique<std::shared_ptr<Impl>>(impl_);
        VLOG(1) << "[ShmGuard::TransferTo] shmId=" << impl_->shmUnit->GetId()
                << ", shmPtr=" << static_cast<const void *>(impl_->shmUnit.get()) << ", offset=" << offset
                << ", size=" << size << ", frameSize=" << bufSize << ", dataSize=" << dataSize_
                << ", metaSize=" << metaSize_;
        messages.emplace_back();
        impl_->remainingFrames.fetch_add(1);
        Status rc = messages.back().TransferOwnership(const_cast<uint8_t *>(ptr), bufSize, ffn, hint.get());
        if (rc.IsError()) {
            impl_->trackSlowFree.store(false);
            impl_->remainingFrames.fetch_sub(1);
            return rc;
        }
        (void)hint.release();
        remaining -= bufSize;
        ptr += bufSize;
    }
    impl_ = nullptr;
    return Status::OK();
}
#endif

void ShmGuard::Free(void *data, void *hint)
{
    (void)data;
    auto implHolder = reinterpret_cast<std::shared_ptr<Impl> *>(hint);
    if (implHolder == nullptr || *implHolder == nullptr) {
        delete implHolder;
        return;
    }

    auto &impl = *implHolder;
    if (impl->shmUnit == nullptr) {
        delete implHolder;
        return;
    }
    // Only the last frame closing the transfer should evaluate the total ShmUnit hold time.
    if (impl->IsLastFrameOnRelease() && impl->trackSlowFree.load()) {
        const auto holdMs = NowSteadyMs() - impl->transferStartTimeMs.load();
        UpdateSlowFreeCircuit(holdMs);
        VLOG(1) << "[ShmGuard::Free] shmId=" << impl->shmUnit->GetId()
                << ", shmPtr=" << static_cast<const void *>(impl->shmUnit.get())
                << ", useCount=" << impl->shmUnit.use_count() << ", holdMs=" << holdMs
                << ", tid=" << std::this_thread::get_id();
    }
    delete implHolder;
}

bool ShmGuard::IsSlowFreeCircuitOpen(int64_t &remainingMs)
{
    const auto nowMs = NowSteadyMs();
    const auto rejectUntilMs = g_rejectTcpPayloadUntilMs.load();
    remainingMs = rejectUntilMs > nowMs ? rejectUntilMs - nowMs : 0;
    return remainingMs > 0;
}

ShmGuard::Impl::Impl(std::shared_ptr<ShmUnit> shm) : shmUnit(std::move(shm)), lock(nullptr), tid(std::this_thread::get_id())
{
}

bool ShmGuard::Impl::IsLastFrameOnRelease()
{
    uint32_t prevRemainingFrames = remainingFrames.load();
    while (prevRemainingFrames > 0
           && !remainingFrames.compare_exchange_weak(prevRemainingFrames, prevRemainingFrames - 1)) {
    }
    return prevRemainingFrames == 1;
}

ShmGuard::Impl::~Impl()
{
    if (lock != nullptr) {
        lock->UnRLatch(tid);
    }
}
}  // namespace datasystem
