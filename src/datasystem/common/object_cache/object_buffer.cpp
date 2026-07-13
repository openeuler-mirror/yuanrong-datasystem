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

/** Description: Implements ObjectBuffer — a lightweight transport-layer-owned object data carrier. */

#include "datasystem/object/object_buffer.h"

#include <cstdlib>
#include <limits>
#include <utility>

#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {

template <typename LockType>
Status InitLatch(std::shared_ptr<void> &storage)
{
    std::shared_ptr<object_cache::Lock> latch = std::make_shared<LockType>();
    storage = latch;
    return latch->Init();
}

object_cache::Lock *GetLatch(const std::shared_ptr<void> &storage)
{
    return static_cast<object_cache::Lock *>(storage.get());
}

const ObjectBufferInfo &GetInfo(const std::shared_ptr<void> &state)
{
    return *static_cast<const ObjectBufferInfo *>(state.get());
}

ObjectBufferInfo &GetMutableInfo(std::shared_ptr<void> &state)
{
    return *static_cast<ObjectBufferInfo *>(state.get());
}

}  // namespace

ObjectBuffer::ObjectBuffer(std::shared_ptr<void> state)
    : state_(std::move(state)), isShm_(false)
{
}

Status ObjectBuffer::Create(std::shared_ptr<void> state, std::shared_ptr<ObjectBuffer> &out)
{
    CHECK_FAIL_RETURN_STATUS(state != nullptr, K_INVALID, "ObjectBuffer state must not be null.");
    struct MakeSharedEnabler : public ObjectBuffer {
        explicit MakeSharedEnabler(std::shared_ptr<void> state) : ObjectBuffer(std::move(state))
        {
        }
    };
    auto buf = std::make_shared<MakeSharedEnabler>(std::move(state));
    RETURN_IF_NOT_OK(buf->Init());
    out = std::move(buf);
    return Status::OK();
}

ObjectBuffer::~ObjectBuffer()
{
    if (state_ != nullptr) {
        auto &info = GetMutableInfo(state_);
        // Release malloc'd memory: owned when NOT shm and NOT UB pool handle
        if (!isShm_ && info.ubGetBufferHandle == nullptr && info.pointer != nullptr) {
            free(info.pointer);
            info.pointer = nullptr;
        }
        // UB pool handle (ubGetBufferHandle) and mmap entry are released by shared_ptr destructor
    }
    latch_.reset();
    state_.reset();
}

ObjectBuffer::ObjectBuffer(ObjectBuffer &&other) noexcept
    : state_(std::move(other.state_)), latch_(std::move(other.latch_)), isShm_(other.isShm_)
{
    other.isShm_ = false;
}

ObjectBuffer &ObjectBuffer::operator=(ObjectBuffer &&other) noexcept
{
    if (this != &other) {
        ObjectBuffer moved(std::move(other));
        std::swap(state_, moved.state_);
        std::swap(latch_, moved.latch_);
        std::swap(isShm_, moved.isShm_);
    }
    return *this;
}

Status ObjectBuffer::Init()
{
    auto &info = GetMutableInfo(state_);
    // Step 1: UB pool pre-allocation (pointer set, no local SHM)
    bool ubPoolPreAlloc = (info.ubUrmaDataInfo != nullptr && info.pointer != nullptr);
    if (ubPoolPreAlloc) {
        isShm_ = false;
        return InitLatch<object_cache::CommonLock>(latch_);
    }
    if (info.remoteHostInfo != nullptr) {
        info.pointer = nullptr;
        isShm_ = false;
        return InitLatch<object_cache::CommonLock>(latch_);
    }
    // Step 2: allocate if still no pointer
    if (info.pointer == nullptr) {
        RETURN_IF_NOT_OK(MallocBufferHelper());
    }
    // Step 3: determine latch type based on shmId
    if (!info.shmId.Empty()) {
        isShm_ = true;
        // Transport-layer SHM buffers do not carry a lock ID, so local latching remains disabled.
        return InitLatch<object_cache::DisabledLock>(latch_);
    }
    isShm_ = false;
    return InitLatch<object_cache::CommonLock>(latch_);
}

Status ObjectBuffer::MallocBufferHelper()
{
    auto &info = GetMutableInfo(state_);
    constexpr uint64_t maxBufferSize = std::numeric_limits<uint64_t>::max();
    CHECK_FAIL_RETURN_STATUS(
        info.dataSize < maxBufferSize && info.metadataSize <= maxBufferSize - info.dataSize - 1,
        K_RUNTIME_ERROR,
        FormatString("Buffer allocation size overflow, data size: %llu, metadata size: %llu", info.dataSize,
                     info.metadataSize));
    const uint64_t mallocSize = info.metadataSize + info.dataSize + 1;
    CHECK_FAIL_RETURN_STATUS(
        mallocSize <= std::numeric_limits<size_t>::max(), K_RUNTIME_ERROR,
        FormatString("Buffer allocation size %llu exceeds size_t max", mallocSize));
    auto memPtr = static_cast<uint8_t *>(malloc(static_cast<size_t>(mallocSize)));
    if (memPtr == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Memory allocation failed");
    }
    Status rc = HugeMemset(memPtr, mallocSize, '\0', mallocSize);
    if (rc.IsError()) {
        free(memPtr);
        LOG(ERROR) << "Buffer memset failed. Detail: " << rc.ToString();
        return rc;
    }
    info.pointer = memPtr;
    return Status::OK();
}

Status ObjectBuffer::MemoryCopy(const void *data, uint64_t length)
{
    if (state_ == nullptr) {
        RETURN_STATUS(StatusCode::K_INVALID, "ObjectBuffer is not initialized.");
    }
    auto &info = GetMutableInfo(state_);
    uint64_t dataSize = info.dataSize;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data != nullptr, K_INVALID, "Can't put null pointer.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(info.pointer != nullptr, K_INVALID, "Buffer data pointer is null.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(length > 0 && length <= dataSize, K_INVALID,
                                         "Data length must be in (0, buffer_size].");
    uint8_t *dstData = info.pointer + info.metadataSize;
    return HugeMemoryCopy(dstData, dataSize, static_cast<const uint8_t *>(data), length);
}

void *ObjectBuffer::MutableData()
{
    if (state_ == nullptr) {
        return nullptr;
    }
    auto &info = GetMutableInfo(state_);
    return info.pointer == nullptr ? nullptr : info.pointer + info.metadataSize;
}

const void *ObjectBuffer::ImmutableData() const
{
    if (state_ == nullptr) {
        return nullptr;
    }
    const auto &info = GetInfo(state_);
    return info.pointer == nullptr ? nullptr : info.pointer + info.metadataSize;
}

int64_t ObjectBuffer::GetSize() const
{
    if (state_ == nullptr) {
        return 0;
    }
    return static_cast<int64_t>(GetInfo(state_).dataSize);
}

Status ObjectBuffer::WLatch(uint64_t timeoutSec)
{
    if (latch_ != nullptr) {
        return GetLatch(latch_)->WLatch(timeoutSec);
    }
    return Status::OK();
}

Status ObjectBuffer::RLatch(uint64_t timeoutSec)
{
    if (latch_ != nullptr) {
        return GetLatch(latch_)->RLatch(timeoutSec);
    }
    return Status::OK();
}

Status ObjectBuffer::UnRLatch()
{
    if (latch_ != nullptr) {
        GetLatch(latch_)->UnRLatch();
    }
    return Status::OK();
}

Status ObjectBuffer::UnWLatch()
{
    if (latch_ != nullptr) {
        GetLatch(latch_)->UnWLatch();
    }
    return Status::OK();
}

}  // namespace datasystem
