/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: This file is used to read and write data and publish data to the server.
 */

#include "datasystem/object/buffer.h"

#include <securec.h>

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"

static constexpr int DEBUG_LOG_LEVEL = 2;

namespace datasystem {
Buffer::Buffer(std::shared_ptr<ObjectBufferInfo> bufferInfo,
               const std::shared_ptr<object_cache::ObjectClientImpl> &clientImpl)
    : bufferInfo_(std::move(bufferInfo)), clientImpl_(clientImpl->weak_from_this()), isShm_(false)
{
    clientId_ = clientImpl->GetClientId();
}

Status Buffer::Init()
{
    auto clientImpl = clientImpl_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(clientImpl);
    RETURN_IF_NOT_OK(CheckDeprecated());

    // Special check for Remote H2D. If the remote host info exists,
    // then the data is neither in local shared memory nor in payload, but rather still on remote worker.
    if (bufferInfo_->remoteHostInfo != nullptr) {
        bufferInfo_->pointer = nullptr;
        isShm_ = false;
        latch_ = std::make_shared<object_cache::CommonLock>();
    } else if (bufferInfo_->pointer == nullptr
               && bufferInfo_->payloadPointer == nullptr) {  // non-shared memory Create or Put
        auto mallocSize = bufferInfo_->dataSize + 1;
        auto memPtr = static_cast<uint8_t *>(malloc(mallocSize));
        if (memPtr == nullptr) {
            RETURN_STATUS(K_RUNTIME_ERROR, "Memory allocation failed");
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(HugeMemset(memPtr, mallocSize, '\0', mallocSize),
                                         FormatString("Buffer memset failed"));
        bufferInfo_->pointer = memPtr;
        latch_ = std::make_shared<object_cache::CommonLock>();
    } else if (bufferInfo_->pointer == nullptr && bufferInfo_->payloadPointer != nullptr) {  // non-shared memory Get.
        bufferInfo_->pointer = static_cast<uint8_t *>(bufferInfo_->payloadPointer->Data());
        latch_ = std::make_shared<object_cache::CommonLock>();
    } else {
        isShm_ = true;
        auto *lockFrame = reinterpret_cast<uint32_t *>(bufferInfo_->pointer);
        latch_ = std::make_shared<object_cache::ShmLock>(lockFrame, bufferInfo_->metadataSize, clientImpl->GetLockId());
    }
    INJECT_POINT("buffer.init");
    return latch_->Init();
}

Status Buffer::CreateBuffer(std::shared_ptr<ObjectBufferInfo> bufferInfo,
                            std::shared_ptr<object_cache::ObjectClientImpl> clientImpl, std::shared_ptr<Buffer> &buffer)
{
    struct ConcreteBuffer : public Buffer {
        ConcreteBuffer(std::shared_ptr<ObjectBufferInfo> bufferInfo,
                       const std::shared_ptr<object_cache::ObjectClientImpl> &clientImpl)
            : Buffer(std::move(bufferInfo), clientImpl)
        {
            clientId_ = clientImpl->GetClientId();
        }
    };
    buffer = std::make_shared<ConcreteBuffer>(std::move(bufferInfo), std::move(clientImpl));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(buffer->Init(), "Create buffer failed");
    return Status::OK();
}

Buffer::Buffer(Buffer &&other) noexcept
    : bufferInfo_(std::move(other.bufferInfo_)),
      clientImpl_(std::move(other.clientImpl_)),
      latch_(std::move(other.latch_)),
      isShm_(other.isShm_),
      clientId_(std::move(other.clientId_))
{
    other.Reset();
}

Buffer &Buffer::operator=(Buffer &&other) noexcept
{
    if (this != &other) {
        Release();
        bufferInfo_ = other.bufferInfo_;
        latch_ = other.latch_;
        clientImpl_ = other.clientImpl_;
        isShm_ = other.isShm_;
        other.Reset();
        clientId_ = other.clientId_;
    }
    return *this;
}

void Buffer::Reset()
{
    bufferInfo_ = nullptr;
    clientImpl_.reset();
    latch_ = nullptr;
    isShm_ = false;
    clientId_ = "";
}

void Buffer::Release()
{
    // At the condition of "non-shared memory Create or Put", free memory after destructor.
    if (bufferInfo_ != nullptr) {
        if (!isShm_ && bufferInfo_->payloadPointer == nullptr && bufferInfo_->pointer) {
            free(bufferInfo_->pointer);
            bufferInfo_->pointer = nullptr;
        }
    }

    // for ut test
    INJECT_POINT("buffer.release", [this]() { isShm_ = false; });
    auto clientImpl = clientImpl_.lock();
    if (clientImpl != nullptr && isShm_ && !isReleased_) {
        clientImpl->DecreaseReferenceCnt(bufferInfo_->shmId, isShm_, bufferInfo_->version);
    }
    bufferInfo_.reset();
    clientImpl_.reset();
    latch_.reset();
    clientId_ = "";
}

Buffer::~Buffer()
{
    Release();
}

Status Buffer::MemoryCopy(const void *data, uint64_t length)
{
    auto clientImpl = clientImpl_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(clientImpl);
    VLOG(DEBUG_LOG_LEVEL) << "Begin to MemoryCopy, clientId: " << clientId_ << ", data length: " << length;
    PerfPoint point(PerfKey::BUFFER_MEMORY_COPY);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    uint8_t *dstData = bufferInfo_->pointer + bufferInfo_->metadataSize;
    uint64_t dataSize = GetSize();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data != nullptr, K_INVALID, "Can't put null pointer.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(length > 0 && length <= dataSize, K_INVALID,
                                         "Data length must be in (0, buffer_size].");
    Status status = ::datasystem::MemoryCopy(dstData, dataSize, static_cast<const uint8_t *>(data), length,
                                             clientImpl->memoryCopyThreadPool_, clientImpl->memcpyParallelThreshold_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(status.IsOk(), K_RUNTIME_ERROR,
                                         FormatString("Copy data to buffer failed, err: %s", status.ToString()));
    return Status::OK();
}

int64_t Buffer::GetSize() const
{
    return static_cast<int64_t>(bufferInfo_->dataSize);
}

Status Buffer::Publish(const std::unordered_set<std::string> &nestedKeys)
{
    auto clientImplSharedPtr = clientImpl_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(clientImplSharedPtr);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");

    Status status = clientImplSharedPtr->Publish(bufferInfo_, nestedKeys, isShm_);
    if (isShm_) {
        SetVisibility(status.IsOk());
    }
    return status;
}

Status Buffer::Seal(const std::unordered_set<std::string> &nestedKeys)
{
    auto clientImpl = clientImpl_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(clientImpl);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");
    Status status = clientImpl->Seal(bufferInfo_, nestedKeys, isShm_);
    if (isShm_) {
        SetVisibility(status.IsOk());
    }
    if (status.IsOk()) {
        bufferInfo_->isSeal = true;
    }
    return status;
}

Status Buffer::WLatch(uint64_t timeoutSec)
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to WLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    RETURN_IF_NOT_OK(CheckVisible());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");
    CHECK_FAIL_RETURN_STATUS(timeoutSec > 0, K_INVALID, "timeout value should be positive.");
    return latch_->WLatch(timeoutSec);
}

Status Buffer::RLatch(uint64_t timeoutSec)
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to RLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    RETURN_IF_NOT_OK(CheckVisible());
    CHECK_FAIL_RETURN_STATUS(timeoutSec > 0, K_INVALID, "timeout value should be positive.");
    return latch_->RLatch(timeoutSec);
}

Status Buffer::UnRLatch()
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to UnRLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    latch_->UnRLatch();
    return Status::OK();
}

Status Buffer::UnWLatch()
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to UnWLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    latch_->UnWLatch();
    return Status::OK();
}

void *Buffer::MutableData()
{
    return static_cast<void *>(bufferInfo_->pointer + bufferInfo_->metadataSize);
}

const void *Buffer::ImmutableData()
{
    return static_cast<const void *>(MutableData());
}

Status Buffer::InvalidateBuffer()
{
    auto clientImpl = clientImpl_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(clientImpl);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    RETURN_IF_NOT_OK(clientImpl->InvalidateBuffer(bufferInfo_->objectKey));
    return Status::OK();
}

RemoteH2DHostInfo *Buffer::GetRemoteHostInfo()
{
    return bufferInfo_->remoteHostInfo.get();
}

Status Buffer::CheckDeprecated()
{
    auto clientImpl = clientImpl_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(clientImpl);
    RETURN_OK_IF_TRUE(!isShm_);

    // In the shared memory scenario, the worker may have released the memory when the network is unavailable.
    Status status = clientImpl->CheckConnection();
    if (status.IsError()) {
        return status;
    }
    if (bufferInfo_->version != clientImpl->GetWorkerVersion()
        || clientImpl->GetState() != (uint16_t)ClientState::INITIALIZED) {
        RETURN_STATUS(K_RUNTIME_ERROR, "The buffer is useless, please destruct it!");
    }
    return Status::OK();
}

uint8_t *Buffer::GetVisiblePointer()
{
    return static_cast<uint8_t *>(MutableData()) - sizeof(uint8_t);
}

void Buffer::SetVisibility(bool visible)
{
    uint8_t val = visible ? 0 : 1;
    uint8_t *pointer = GetVisiblePointer();
    *pointer = val;
}

Status Buffer::CheckVisible()
{
    if (!isShm_) {
        return Status::OK();
    }
    uint8_t *val = GetVisiblePointer();
    if (*val != 0) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Buffer publish/seal failed, unable to visit");
    }
    return Status::OK();
}
}  // namespace datasystem
