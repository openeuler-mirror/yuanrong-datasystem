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

namespace datasystem {
ShmGuard::ShmGuard(std::shared_ptr<ShmUnit> shmUnit, size_t dataSize, size_t metaSize)
    : impl_(std::make_shared<Impl>(std::move(shmUnit))), dataSize_(dataSize), metaSize_(metaSize)
{
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
    while (remaining > 0) {
        auto hint = std::make_unique<std::shared_ptr<Impl>>(impl_);
        int32_t bufSize = std::min(remaining, maxInt);
        messages.emplace_back();
        RETURN_IF_NOT_OK(messages.back().TransferOwnership(const_cast<uint8_t *>(ptr), bufSize, ffn, hint.get()));
        (void)hint.release();
        remaining -= bufSize;
        ptr += bufSize;
    }
    impl_ = nullptr;
    return Status::OK();
}

void ShmGuard::Free(void *data, void *hint)
{
    (void)data;
    auto impl = reinterpret_cast<std::shared_ptr<Impl> *>(hint);
    delete impl;
}

ShmGuard::Impl::Impl(std::shared_ptr<ShmUnit> shm)
    : shmUnit(std::move(shm)), lock(nullptr), tid(std::this_thread::get_id())
{
}

ShmGuard::Impl::~Impl()
{
    if (lock != nullptr) {
        lock->UnRLatch(tid);
    }
}
}  // namespace datasystem
