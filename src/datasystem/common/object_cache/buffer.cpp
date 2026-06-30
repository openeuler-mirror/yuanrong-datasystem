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

#include "datasystem/client/object_cache/object_client_impl.h"
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/status.h"

static constexpr int DEBUG_LOG_LEVEL = 2;

namespace datasystem {

Buffer::Buffer() = default;

Buffer::Buffer(std::shared_ptr<ObjectBufferInfo> bufferInfo,
               const std::shared_ptr<object_cache::ObjectClientImpl> &clientImpl)
    : bufferInfo_(std::move(bufferInfo)), clientImpl_(clientImpl->weak_from_this()), isShm_(false)
{
    clientId_ = clientImpl->GetClientId();
}

Status Buffer::Init()
{
    auto clientImpl = clientImpl_.lock();
    if (clientImpl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Client already destroyed or Shutdown() invoked, buffer invalidated.");
    }
    RETURN_IF_NOT_OK(CheckDeprecated());

    // Step 1: resolve pointer from payload if not already set by caller
    if (bufferInfo_->pointer == nullptr && bufferInfo_->payloadPointer != nullptr) {
        bufferInfo_->pointer = static_cast<uint8_t *>(bufferInfo_->payloadPointer->Data());
    }
    // Step 2: UB pool pre-allocation (pointer set, no local SHM) or lazy remote
    bool ubPoolPreAlloc = (bufferInfo_->ubUrmaDataInfo != nullptr && bufferInfo_->pointer != nullptr);
    if (ubPoolPreAlloc) {
        isShm_ = false;
        latch_ = std::make_shared<object_cache::CommonLock>();
        return latch_->Init();
    }
    if (bufferInfo_->remoteHostInfo != nullptr || bufferInfo_->ubUrmaDataInfo != nullptr) {
        bufferInfo_->pointer = nullptr;
        isShm_ = false;
        latch_ = std::make_shared<object_cache::CommonLock>();
        return latch_->Init();
    }
    // Step 3: allocate if still no pointer (Put/Create)
    if (bufferInfo_->pointer == nullptr) {
        RETURN_IF_NOT_OK(MallocBufferHelper());
    }
    // Step 4: determine latch type based on shmId
    if (!bufferInfo_->shmId.Empty()) {
        isShm_ = true;
        if (bufferInfo_->metadataSize == 0) {
            latch_ = std::make_shared<object_cache::DisabledLock>();
        } else {
            auto *lockFrame = reinterpret_cast<uint32_t *>(bufferInfo_->pointer);
            latch_ = std::make_shared<object_cache::ShmLock>(lockFrame, bufferInfo_->metadataSize,
                                                             clientImpl->GetLockId());
        }
    } else {
        isShm_ = false;
        latch_ = std::make_shared<object_cache::CommonLock>();
    }
#ifdef WITH_TESTS
    INJECT_POINT("buffer.init");
#endif
    return latch_->Init();
}

Status Buffer::MallocBufferHelper()
{
    auto mallocSize = bufferInfo_->metadataSize + bufferInfo_->dataSize + 1;
    auto memPtr = static_cast<uint8_t *>(malloc(mallocSize));
    if (memPtr == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Memory allocation failed");
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(HugeMemset(memPtr, mallocSize, '\0', mallocSize),
                                     FormatString("Buffer memset failed"));
    bufferInfo_->pointer = memPtr;
    return Status::OK();
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
      isReleased_(other.isReleased_),
      clientId_(std::move(other.clientId_))
{
    other.Reset();
}

Buffer &Buffer::operator=(Buffer &&other) noexcept
{
    if (this != &other) {
        Release();
        bufferInfo_ = std::move(other.bufferInfo_);
        latch_ = std::move(other.latch_);
        clientImpl_ = std::move(other.clientImpl_);
        isShm_ = other.isShm_;
        isReleased_ = other.isReleased_;
        clientId_ = std::move(other.clientId_);
        other.Reset();
    }
    return *this;
}

void Buffer::Reset()
{
    bufferInfo_ = nullptr;
    clientImpl_.reset();
    latch_ = nullptr;
    isShm_ = false;
    isReleased_ = false;
    clientId_ = "";
}

void Buffer::Release(object_cache::ObjectClientImpl *clientPtr)
{
    if (bufferInfo_ != nullptr) {
        if (!isShm_ && bufferInfo_->payloadPointer == nullptr && bufferInfo_->pointer
            && bufferInfo_->ubGetBufferHandle == nullptr) {
            free(bufferInfo_->pointer);
            bufferInfo_->pointer = nullptr;
        }
    }
#ifdef WITH_TESTS
    // for ut test
    INJECT_POINT("buffer.release", [this]() { isShm_ = false; });
#endif
    do {
        if (isReleased_) {
            break;
        }
        if (clientPtr) {
            clientPtr->DecreaseReferenceCnt(bufferInfo_->shmId, isShm_, bufferInfo_->version);
            break;
        }
        auto clientImpl = clientImpl_.lock();
        if (clientImpl != nullptr) {
            clientImpl->DecreaseReferenceCnt(bufferInfo_->shmId, isShm_, bufferInfo_->version);
        }
    } while (false);
    bufferInfo_.reset();
    clientImpl_.reset();
    latch_.reset();
    clientId_ = "";
    isReleased_ = true;
}

Buffer::~Buffer()
{
    Release();
}

Status Buffer::MemoryCopy(const void *data, uint64_t length)
{
    return MemoryCopyWithTransport(data, length, nullptr);
}

Status Buffer::MemoryCopyWithTransport(const void *data, uint64_t length, uint8_t *actualTransportKind)
{
    if (actualTransportKind != nullptr) {
        *actualTransportKind = static_cast<uint8_t>(AccessTransportKind::SHM);
    }
    if (bufferInfo_ == nullptr) {
        RETURN_STATUS(StatusCode::K_INVALID,
                      "Buffer is not initialized. Key may already exist with NX option.");
    }
    auto clientImpl = clientImpl_.lock();
    if (clientImpl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Client already destroyed or Shutdown() invoked, buffer invalidated.");
    }
    VLOG(DEBUG_LOG_LEVEL) << "Begin to MemoryCopy, clientId: " << clientId_ << ", data length: " << length;
    PerfPoint point(PerfKey::BUFFER_MEMORY_COPY);
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    const bool traceEnabled = ShouldCollectLatencyTrace(GetClientLatencyTraceConfig());
    RETURN_IF_NOT_OK(CheckDeprecated());
    uint64_t dataSize = GetSize();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data != nullptr, K_INVALID, "Can't put null pointer.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(length > 0 && length <= dataSize, K_INVALID,
                                         "Data length must be in (0, buffer_size].");
    if (bufferInfo_->ubUrmaDataInfo) {
        Status ubStatus;
        if (bufferInfo_->ubGetBufferHandle && bufferInfo_->pointer != nullptr) {
            ubStatus = clientImpl->SendBufferViaUbFromPool(bufferInfo_, data, length, traceEnabled);
        } else {
            ubStatus = clientImpl->SendBufferViaUb(bufferInfo_, data, length, traceEnabled);
        }
        if (ubStatus.IsOk()) {
            if (actualTransportKind != nullptr) {
                *actualTransportKind = static_cast<uint8_t>(AccessTransportKind::UB);
            }
            AccessTransportTracker::Record(AccessTransportKind::UB);
            return Status::OK();
        }
        if (actualTransportKind != nullptr) {
            *actualTransportKind = static_cast<uint8_t>(AccessTransportKind::TCP);
        }
        AccessTransportTracker::Record(AccessTransportKind::TCP);
        // fallback to TCP if UB send fails, allocate buffer for that purpose.
        bufferInfo_->ubGetBufferHandle.reset();
        RETURN_IF_NOT_OK(MallocBufferHelper());
    }
    uint8_t *dstData = bufferInfo_->pointer + bufferInfo_->metadataSize;
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
    if (clientImplSharedPtr == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Client already destroyed or Shutdown() invoked, buffer invalidated.");
    }
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    const bool traceEnabled = ShouldCollectLatencyTrace(GetClientLatencyTraceConfig());
    RETURN_IF_NOT_OK(CheckDeprecated());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");

    if (bufferInfo_->ubUrmaDataInfo && !bufferInfo_->ubDataSentByMemoryCopy) {
        uint64_t dataSize = GetSize();
        const void *dataPtr = ImmutableData();
        if (dataPtr != nullptr && dataSize > 0) {
            Status ubStatus;
            if (bufferInfo_->ubGetBufferHandle && bufferInfo_->pointer != nullptr) {
                ubStatus = clientImplSharedPtr->SendBufferViaUbFromPool(bufferInfo_, dataPtr, dataSize, traceEnabled);
            } else {
                ubStatus = clientImplSharedPtr->SendBufferViaUb(bufferInfo_, dataPtr, dataSize, traceEnabled);
            }
            if (ubStatus.IsOk()) {
                AccessTransportTracker::Record(AccessTransportKind::UB);
            } else {
                LOG(ERROR) << "Try to publish via UB but failed! object key: " << bufferInfo_->objectKey
                           << ", ub send status: " << ubStatus.ToString();
                AccessTransportTracker::Record(AccessTransportKind::TCP);
            }
        }
    }

    Status status = clientImplSharedPtr->Publish(bufferInfo_, nestedKeys, isShm_);
    if (isShm_) {
        SetVisibility(status.IsOk());
    } else {
        // worker already release shmUnit for this case.
        isReleased_ = !bufferInfo_->shmId.Empty() && status.IsOk();
    }
    return status;
}

Status Buffer::Seal(const std::unordered_set<std::string> &nestedKeys)
{
    auto clientImpl = clientImpl_.lock();
    if (clientImpl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Client already destroyed or Shutdown() invoked, buffer invalidated.");
    }
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
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
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    RETURN_IF_NOT_OK(CheckVisible());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo_->isSeal, K_OC_ALREADY_SEALED, "Client object is already sealed");
    CHECK_FAIL_RETURN_STATUS(timeoutSec > 0, K_INVALID, "timeout value should be positive.");
    return latch_->WLatch(timeoutSec);
}

Status Buffer::RLatch(uint64_t timeoutSec)
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to RLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    RETURN_IF_NOT_OK(CheckVisible());
    CHECK_FAIL_RETURN_STATUS(timeoutSec > 0, K_INVALID, "timeout value should be positive.");
    return latch_->RLatch(timeoutSec);
}

Status Buffer::UnRLatch()
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to UnRLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    latch_->UnRLatch();
    return Status::OK();
}

Status Buffer::UnWLatch()
{
    VLOG(DEBUG_LOG_LEVEL) << "Begin to UnWLatch, clientId: " << clientId_ << ", isShm: " << isShm_;
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    latch_->UnWLatch();
    return Status::OK();
}

Status Buffer::CopyDataWithRLatch(const std::function<Status()> &copyFn)
{
    if (copyFn == nullptr) {
        RETURN_STATUS(StatusCode::K_INVALID, "CopyDataWithRLatch: copy callback is null.");
    }
    if (latch_ == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "CopyDataWithRLatch: buffer is not initialized.");
    }

    // When the buffer was created with oc_metadata_header disabled, the shm layout
    // has no lock frame and read latching is a no-op; we still need the same
    // CheckDeprecated/CheckVisible preconditions that Buffer::RLatch() enforces so
    // that callers do not read stale or deprecated buffers.
    const bool latchSupported = latch_->IsSupported();
    if (!latchSupported) {
        RETURN_IF_NOT_OK(CheckDeprecated());
        RETURN_IF_NOT_OK(CheckVisible());
    } else {
        RETURN_IF_NOT_OK(RLatch());
    }

    Status copyStatus = copyFn();

    if (latchSupported) {
        Status unlatchStatus = UnRLatch();
        if (copyStatus.IsError()) {
            return copyStatus;
        }
        return unlatchStatus;
    }
    return copyStatus;
}

void *Buffer::MutableData()
{
    if (bufferInfo_->pointer == nullptr && bufferInfo_->ubUrmaDataInfo != nullptr) {
        bufferInfo_->ubDataSentByMemoryCopy = false;
        Status status = MallocBufferHelper();
        if (status.IsError()) {
            LOG(ERROR) << FormatString("Malloc buffer for object %s failed, err: %s", bufferInfo_->objectKey,
                                       status.ToString());
            return nullptr;
        }
    }
    if (bufferInfo_->ubGetBufferHandle && bufferInfo_->pointer != nullptr) {
        // UB pool pre-allocated: reset flag so Publish() will re-send via RDMA
        bufferInfo_->ubDataSentByMemoryCopy = false;
    }
    return static_cast<void *>(bufferInfo_->pointer + bufferInfo_->metadataSize);
}

const void *Buffer::ImmutableData()
{
    return static_cast<const void *>(MutableData());
}

Status Buffer::InvalidateBuffer()
{
    auto clientImpl = clientImpl_.lock();
    if (clientImpl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Client already destroyed or Shutdown() invoked, buffer invalidated.");
    }
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    RETURN_IF_NOT_OK(CheckDeprecated());
    RETURN_IF_NOT_OK(clientImpl->InvalidateBuffer(bufferInfo_->objectKey));
    return Status::OK();
}

RemoteH2DHostInfoPb *Buffer::GetRemoteHostInfo()
{
    return bufferInfo_->remoteHostInfo.get();
}

Status Buffer::CheckDeprecated()
{
    auto clientImpl = clientImpl_.lock();
    if (clientImpl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      "Client already destroyed or Shutdown() invoked, buffer invalidated.");
    }
    RETURN_OK_IF_TRUE(!isShm_);

    // In the shared memory scenario, the worker may have released the memory when the network is unavailable.
    if (clientId_ != clientImpl->GetClientId()) {
        RETURN_STATUS(K_BUFFER_DEPRECATED, "The buffer is deprecated, please destruct it!");
    }
    Status status = clientImpl->CheckConnection();
    if (status.IsError()) {
        return status;
    }
    if (bufferInfo_->version != clientImpl->GetWorkerVersion()
        || clientImpl->GetState() != (uint16_t)ClientState::INITIALIZED) {
        RETURN_STATUS(K_BUFFER_DEPRECATED, "The buffer is deprecated, please destruct it!");
    }
    return Status::OK();
}

uint8_t *Buffer::GetVisiblePointer()
{
    if (bufferInfo_ == nullptr || bufferInfo_->metadataSize == 0) {
        return nullptr;
    }
    return static_cast<uint8_t *>(MutableData()) - sizeof(uint8_t);
}

void Buffer::SetVisibility(bool visible)
{
    if (!isShm_ || bufferInfo_ == nullptr || bufferInfo_->metadataSize == 0) {
        return;
    }
    uint8_t val = visible ? 0 : 1;
    uint8_t *pointer = GetVisiblePointer();
    if (pointer == nullptr) {
        return;
    }
    *pointer = val;
}

Status Buffer::CheckVisible()
{
    if (!isShm_ || bufferInfo_ == nullptr || bufferInfo_->metadataSize == 0) {
        return Status::OK();
    }
    uint8_t *val = GetVisiblePointer();
    if (val == nullptr) {
        return Status::OK();
    }
    if (*val != 0) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Buffer publish/seal failed, unable to visit");
    }
    return Status::OK();
}
}  // namespace datasystem
