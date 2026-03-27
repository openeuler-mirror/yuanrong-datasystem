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

#include "datasystem/common/device/nvidia/cuda_resource_manager.h"

#include <algorithm>
#include <utility>

#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
constexpr size_t DEFAULT_GPU_PIPELINE_DEPTH = 4;
constexpr size_t MIN_GPU_PIPELINE_DEPTH = 2;
constexpr size_t MAX_GPU_PIPELINE_DEPTH = 8;
}  // namespace

CudaResourceManager::CudaResourceManager()
{
    // CUDA does not support FFTS/HUGE_FFTS (NPU-specific), force DIRECT policy.
    policyD2H = MemcopyPolicy::DIRECT;
    policyH2D = MemcopyPolicy::DIRECT;
    devManager_ = DeviceManagerFactory::GetDeviceManager();

    uint64_t pipelineDepth = DEFAULT_GPU_PIPELINE_DEPTH;
    LOG_IF_ERROR(GetNumberFromEnv("DS_GPU_H2D_PIPELINE_DEPTH", pipelineDepth), "GetNumberFromEnv failed");
    h2dPipelineDepth_ = std::clamp(static_cast<size_t>(pipelineDepth), MIN_GPU_PIPELINE_DEPTH, MAX_GPU_PIPELINE_DEPTH);

    pipelineDepth = DEFAULT_GPU_PIPELINE_DEPTH;
    LOG_IF_ERROR(GetNumberFromEnv("DS_GPU_D2H_PIPELINE_DEPTH", pipelineDepth), "GetNumberFromEnv failed");
    d2hPipelineDepth_ = std::clamp(static_cast<size_t>(pipelineDepth), MIN_GPU_PIPELINE_DEPTH, MAX_GPU_PIPELINE_DEPTH);
}

CudaResourceManager::~CudaResourceManager()
{
    ShutdownSessionPools();
}

Status CudaResourceManager::MemcpyBatchD2H(const std::vector<DeviceBlobList> &devBlobList,
                                           std::vector<Buffer *> &bufferList)
{
    return CudaMemcpyBatch(devBlobList, bufferList, MemcpyKind::DEVICE_TO_HOST);
}

Status CudaResourceManager::MemcpyBatchH2D(const std::vector<DeviceBlobList> &devBlobList,
                                           std::vector<Buffer *> &bufferList)
{
    return CudaMemcpyBatch(devBlobList, bufferList, MemcpyKind::HOST_TO_DEVICE);
}

void CudaResourceManager::SetPolicyByHugeTlb(bool enableHugeTlb)
{
    // CUDA does not support FFTS/HUGE_FFTS, no policy change needed.
    (void)enableHugeTlb;
}

Status CudaResourceManager::CudaMemcpyBatch(const std::vector<DeviceBlobList> &devBlobList,
                                            std::vector<Buffer *> &bufferList, MemcpyKind copyKind)
{
    CHECK_FAIL_RETURN_STATUS(!devBlobList.empty(), K_INVALID, "The devBlobList is empty.");
    CHECK_FAIL_RETURN_STATUS(!bufferList.empty(), K_INVALID, "The bufferList is empty.");
    CHECK_FAIL_RETURN_STATUS(devBlobList.size() == bufferList.size(), K_INVALID,
                             FormatString("The devBlobList size %zu is not equal to bufferList size %zu",
                                          devBlobList.size(), bufferList.size()));
    CHECK_FAIL_RETURN_STATUS(devManager_ != nullptr, K_RUNTIME_ERROR, "Failed to get device manager.");

    auto deviceId = devBlobList[0].deviceIdx;
    RETURN_IF_NOT_OK(devManager_->SetDevice(deviceId));
    for (size_t i = 0; i < devBlobList.size(); i++) {
        CHECK_FAIL_RETURN_STATUS(devBlobList[i].deviceIdx == deviceId, K_INVALID,
                                 FormatString("Device index mismatch in batch: expect %d, actual %d, index %zu",
                                              deviceId, devBlobList[i].deviceIdx, i));
    }

    DeviceBatchCopyHelper helper;
    RETURN_IF_NOT_OK(helper.Prepare(devBlobList, bufferList, copyKind));
    RETURN_OK_IF_TRUE(helper.batchSize == 0);

    if (copyKind == MemcpyKind::HOST_TO_DEVICE) {
        return SubmitBatchH2D(helper, deviceId);
    }
    return SubmitBatchD2H(helper, deviceId);
}

Status CudaResourceManager::CudaMemcpyBatchSync(DeviceBatchCopyHelper &helper, MemcpyKind copyKind)
{
    Status lastErr = Status::OK();
    for (size_t index = 0; index < helper.batchSize; index++) {
        auto *dst = helper.dstList[index];
        auto *src = helper.srcList[index];
        auto size = helper.dataSizeList[index];
        Status rc;
        if (copyKind == MemcpyKind::DEVICE_TO_HOST) {
            rc = devManager_->MemCopyD2H(dst, size, src, size);
        } else {
            rc = devManager_->MemCopyH2D(dst, size, src, size);
        }
        if (rc.IsError()) {
            lastErr = rc;
            LOG(ERROR) << FormatString("CudaMemCopy failed for flat index %zu: %s", index, rc.ToString().c_str());
        }
    }
    return lastErr.IsError() ? lastErr : Status::OK();
}

bool CudaResourceManager::NeedPooledSession(const DeviceBatchCopyHelper &helper) const
{
    for (const auto &meta : helper.bufferMetas) {
        if (meta.blobCount > 1) {
            return true;
        }
    }
    return false;
}

Status CudaResourceManager::SubmitBatchH2D(DeviceBatchCopyHelper &helper, int32_t deviceId)
{
    if (!NeedPooledSession(helper)) {
        return SubmitBatchDirectAsync(helper, deviceId, MemcpyKind::HOST_TO_DEVICE);
    }

    RETURN_IF_NOT_OK(EnsureInitialized());
    std::unique_ptr<GpuH2DSession> session;
    RETURN_IF_NOT_OK(AcquireH2DSession(deviceId, session));

    Status submitRc = Status::OK();
    for (size_t objectIndex = 0; objectIndex < helper.bufferMetas.size(); objectIndex++) {
        const auto &meta = helper.bufferMetas[objectIndex];
        auto rc = (meta.blobCount <= 1)
                      ? SubmitObjectDirectAsync(helper, objectIndex, session->directStream, MemcpyKind::HOST_TO_DEVICE)
                      : SubmitObjectWithH2DPipeline(helper, objectIndex, *session);
        if (rc.IsError()) {
            submitRc = rc;
            break;
        }
    }

    auto tailRc = TailWaitH2D(*session);
    bool reusable = submitRc.IsOk() && tailRc.IsOk();
    ReleaseH2DSession(std::move(session), reusable);
    return submitRc.IsError() ? submitRc : tailRc;
}

Status CudaResourceManager::SubmitBatchD2H(DeviceBatchCopyHelper &helper, int32_t deviceId)
{
    if (!NeedPooledSession(helper)) {
        return SubmitBatchDirectAsync(helper, deviceId, MemcpyKind::DEVICE_TO_HOST);
    }

    RETURN_IF_NOT_OK(EnsureInitialized());
    std::unique_ptr<GpuD2HSession> session;
    RETURN_IF_NOT_OK(AcquireD2HSession(deviceId, session));

    Status submitRc = Status::OK();
    for (size_t objectIndex = 0; objectIndex < helper.bufferMetas.size(); objectIndex++) {
        const auto &meta = helper.bufferMetas[objectIndex];
        auto rc = (meta.blobCount <= 1)
                      ? SubmitObjectDirectAsync(helper, objectIndex, session->directStream, MemcpyKind::DEVICE_TO_HOST)
                      : SubmitObjectWithD2HPipeline(helper, objectIndex, *session);
        if (rc.IsError()) {
            submitRc = rc;
            break;
        }
    }

    auto tailRc = TailWaitD2H(helper, *session);
    bool reusable = submitRc.IsOk() && tailRc.IsOk();
    ReleaseD2HSession(std::move(session), reusable);
    return submitRc.IsError() ? submitRc : tailRc;
}

Status CudaResourceManager::SubmitBatchDirectAsync(DeviceBatchCopyHelper &helper, int32_t deviceId, MemcpyKind copyKind)
{
    (void)deviceId;
    void *stream = nullptr;
    RETURN_IF_NOT_OK(devManager_->CreateStream(&stream));

    Status lastRc = Status::OK();
    for (size_t objectIndex = 0; objectIndex < helper.bufferMetas.size(); objectIndex++) {
        auto rc = SubmitObjectDirectAsync(helper, objectIndex, stream, copyKind);
        if (rc.IsError()) {
            lastRc = rc;
            break;
        }
    }

    auto syncRc = devManager_->SynchronizeStream(stream);
    LOG_IF_ERROR(devManager_->DestroyStream(stream), "Destroy direct stream failed.");
    return lastRc.IsError() ? lastRc : syncRc;
}

Status CudaResourceManager::SubmitObjectDirectAsync(const DeviceBatchCopyHelper &helper, size_t objectIndex,
                                                    void *stream, MemcpyKind copyKind)
{
    CHECK_FAIL_RETURN_STATUS(objectIndex < helper.bufferMetas.size(), K_INVALID,
                             FormatString("Invalid object index %zu", objectIndex));
    const auto &meta = helper.bufferMetas[objectIndex];
    if (copyKind == MemcpyKind::HOST_TO_DEVICE) {
        auto &srcBuffer = helper.srcBuffers[objectIndex];
        size_t offset = 0;
        for (size_t blobIndex = 0; blobIndex < meta.blobCount; blobIndex++) {
            size_t flatBlobIndex = meta.firstBlobOffset + blobIndex;
            CHECK_FAIL_RETURN_STATUS(
                flatBlobIndex < helper.dstBuffers.size(), K_RUNTIME_ERROR,
                FormatString("Invalid flat blob index %zu for object %zu", flatBlobIndex, objectIndex));
            auto &dstBuffer = helper.dstBuffers[flatBlobIndex];
            auto rc = devManager_->MemcpyAsync(static_cast<void *>(dstBuffer.ptr), dstBuffer.size,
                                               static_cast<void *>(static_cast<uint8_t *>(srcBuffer.ptr) + offset),
                                               dstBuffer.size, copyKind, stream);
            if (rc.IsError()) {
                LOG(ERROR) << FormatString("DirectAsync H2D submit failed for object %zu, blob %zu: %s", objectIndex,
                                           blobIndex, rc.ToString().c_str());
                return rc;
            }
            offset += dstBuffer.size;
        }
        return Status::OK();
    }

    CHECK_FAIL_RETURN_STATUS(copyKind == MemcpyKind::DEVICE_TO_HOST, K_INVALID, "Invalid memcpy kind");
    CHECK_FAIL_RETURN_STATUS(objectIndex < helper.dstBuffers.size(), K_RUNTIME_ERROR,
                             FormatString("Invalid destination object index %zu", objectIndex));
    auto &dstBuffer = helper.dstBuffers[objectIndex];
    size_t offset = 0;
    for (size_t blobIndex = 0; blobIndex < meta.blobCount; blobIndex++) {
        size_t flatBlobIndex = meta.firstBlobOffset + blobIndex;
        CHECK_FAIL_RETURN_STATUS(
            flatBlobIndex < helper.srcBuffers.size(), K_RUNTIME_ERROR,
            FormatString("Invalid flat blob index %zu for object %zu", flatBlobIndex, objectIndex));
        auto &srcBuffer = helper.srcBuffers[flatBlobIndex];
        CHECK_FAIL_RETURN_STATUS(
            offset + srcBuffer.size <= dstBuffer.size, K_RUNTIME_ERROR,
            FormatString("DirectAsync D2H offset overflow for object %zu, blob %zu", objectIndex, blobIndex));
        auto *dstPtr = static_cast<void *>(static_cast<uint8_t *>(dstBuffer.ptr) + offset);
        auto rc = devManager_->MemcpyAsync(dstPtr, srcBuffer.size, srcBuffer.ptr, srcBuffer.size, copyKind, stream);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("DirectAsync D2H submit failed for object %zu, blob %zu: %s", objectIndex,
                                       blobIndex, rc.ToString().c_str());
            return rc;
        }
        offset += srcBuffer.size;
    }
    return Status::OK();
}

Status CudaResourceManager::CreateH2DSession(int32_t deviceId, std::unique_ptr<GpuH2DSession> &session)
{
    auto newSession = std::make_unique<GpuH2DSession>();
    newSession->deviceId = deviceId;
    newSession->slots.resize(GetH2DPipelineDepth());

    auto rc = devManager_->SetDevice(deviceId);
    if (rc.IsError()) {
        return rc;
    }

    rc = devManager_->CreateStream(&newSession->directStream);
    if (rc.IsError()) {
        return rc;
    }

    for (auto &slot : newSession->slots) {
        rc = devManager_->CreateStream(&slot.stream);
        if (rc.IsError()) {
            DestroyH2DSession(std::move(newSession));
            return rc;
        }
        rc = devManager_->CreateEvent(&slot.doneEvent);
        if (rc.IsError()) {
            DestroyH2DSession(std::move(newSession));
            return rc;
        }
    }

    session = std::move(newSession);
    return Status::OK();
}

Status CudaResourceManager::CreateD2HSession(int32_t deviceId, std::unique_ptr<GpuD2HSession> &session)
{
    auto newSession = std::make_unique<GpuD2HSession>();
    newSession->deviceId = deviceId;
    newSession->slots.resize(GetD2HPipelineDepth());

    auto rc = devManager_->SetDevice(deviceId);
    if (rc.IsError()) {
        return rc;
    }

    rc = devManager_->CreateStream(&newSession->directStream);
    if (rc.IsError()) {
        return rc;
    }

    for (auto &slot : newSession->slots) {
        rc = devManager_->CreateStream(&slot.stream);
        if (rc.IsError()) {
            DestroyD2HSession(std::move(newSession));
            return rc;
        }
        rc = devManager_->CreateEvent(&slot.doneEvent);
        if (rc.IsError()) {
            DestroyD2HSession(std::move(newSession));
            return rc;
        }
    }

    session = std::move(newSession);
    return Status::OK();
}

void CudaResourceManager::DestroyH2DSession(std::unique_ptr<GpuH2DSession> session)
{
    if (session == nullptr) {
        return;
    }
    if (devManager_ == nullptr) {
        return;
    }
    auto rc = devManager_->SetDevice(session->deviceId);
    if (rc.IsError()) {
        LOG(ERROR) << "Set device for H2D session destroy failed: " << rc;
        return;
    }

    if (session->directStream != nullptr) {
        LOG_IF_ERROR(devManager_->SynchronizeStream(session->directStream), "Synchronize direct stream failed.");
        LOG_IF_ERROR(devManager_->DestroyStream(session->directStream), "Destroy direct stream failed.");
        session->directStream = nullptr;
    }

    for (auto &slot : session->slots) {
        if (slot.inFlight && slot.doneEvent != nullptr) {
            LOG_IF_ERROR(devManager_->SynchronizeEvent(slot.doneEvent), "Synchronize H2D slot event failed.");
            slot.inFlight = false;
        }
        if (slot.doneEvent != nullptr) {
            LOG_IF_ERROR(devManager_->DestroyEvent(slot.doneEvent), "Destroy H2D slot event failed.");
            slot.doneEvent = nullptr;
        }
        if (slot.stream != nullptr) {
            LOG_IF_ERROR(devManager_->DestroyStream(slot.stream), "Destroy H2D slot stream failed.");
            slot.stream = nullptr;
        }
        if (Host() != nullptr) {
            LOG_IF_ERROR(Host()->Free(slot.hostPinnedPool), "Free H2D slot host pinned buffer failed.");
        }
        slot.capacity = 0;
        slot.lastObjectIndex = 0;
    }
}

void CudaResourceManager::DestroyD2HSession(std::unique_ptr<GpuD2HSession> session)
{
    if (session == nullptr) {
        return;
    }
    if (devManager_ == nullptr) {
        return;
    }
    auto rc = devManager_->SetDevice(session->deviceId);
    if (rc.IsError()) {
        LOG(ERROR) << "Set device for D2H session destroy failed: " << rc;
        return;
    }

    if (session->directStream != nullptr) {
        LOG_IF_ERROR(devManager_->SynchronizeStream(session->directStream), "Synchronize direct stream failed.");
        LOG_IF_ERROR(devManager_->DestroyStream(session->directStream), "Destroy direct stream failed.");
        session->directStream = nullptr;
    }

    for (auto &slot : session->slots) {
        if (slot.phase == GpuD2HSlotPhase::IN_FLIGHT_D2H && slot.doneEvent != nullptr) {
            LOG_IF_ERROR(devManager_->SynchronizeEvent(slot.doneEvent), "Synchronize D2H slot event failed.");
        }
        if (slot.doneEvent != nullptr) {
            LOG_IF_ERROR(devManager_->DestroyEvent(slot.doneEvent), "Destroy D2H slot event failed.");
            slot.doneEvent = nullptr;
        }
        if (slot.stream != nullptr) {
            LOG_IF_ERROR(devManager_->DestroyStream(slot.stream), "Destroy D2H slot stream failed.");
            slot.stream = nullptr;
        }
        if (Host() != nullptr) {
            LOG_IF_ERROR(Host()->Free(slot.hostPinnedPool), "Free D2H slot host pinned buffer failed.");
        }
        slot.capacity = 0;
        slot.lastObjectIndex = 0;
        slot.phase = GpuD2HSlotPhase::AVAILABLE;
    }
}

CudaResourceManager::SessionPool<CudaResourceManager::GpuH2DSession> *CudaResourceManager::GetOrCreateH2DPool(
    int32_t deviceId)
{
    std::lock_guard<std::mutex> lock(h2dPoolsMutex_);
    auto &pool = h2dPools_[deviceId];
    if (pool == nullptr) {
        pool = std::make_unique<SessionPool<GpuH2DSession>>();
    }
    return pool.get();
}

CudaResourceManager::SessionPool<CudaResourceManager::GpuD2HSession> *CudaResourceManager::GetOrCreateD2HPool(
    int32_t deviceId)
{
    std::lock_guard<std::mutex> lock(d2hPoolsMutex_);
    auto &pool = d2hPools_[deviceId];
    if (pool == nullptr) {
        pool = std::make_unique<SessionPool<GpuD2HSession>>();
    }
    return pool.get();
}

CudaResourceManager::SessionPool<CudaResourceManager::GpuH2DSession> *CudaResourceManager::GetH2DPool(int32_t deviceId)
{
    std::lock_guard<std::mutex> lock(h2dPoolsMutex_);
    auto iter = h2dPools_.find(deviceId);
    return iter == h2dPools_.end() ? nullptr : iter->second.get();
}

CudaResourceManager::SessionPool<CudaResourceManager::GpuD2HSession> *CudaResourceManager::GetD2HPool(int32_t deviceId)
{
    std::lock_guard<std::mutex> lock(d2hPoolsMutex_);
    auto iter = d2hPools_.find(deviceId);
    return iter == d2hPools_.end() ? nullptr : iter->second.get();
}

Status CudaResourceManager::AcquireH2DSession(int32_t deviceId, std::unique_ptr<GpuH2DSession> &session)
{
    auto *pool = GetOrCreateH2DPool(deviceId);
    bool needCreateSession = false;
    while (!needCreateSession) {
        std::unique_lock<std::mutex> lock(pool->mutex);
        if (pool->closed) {
            return Status(K_RUNTIME_ERROR, "CUDA H2D session pool is closed.");
        }
        if (!pool->idleSessions.empty()) {
            session = std::move(pool->idleSessions.front());
            pool->idleSessions.pop_front();
            ++pool->borrowedSessions;
            return Status::OK();
        }
        if (pool->totalSessions < pool->maxSessions) {
            ++pool->totalSessions;
            ++pool->borrowedSessions;
            needCreateSession = true;
            continue;
        }
        pool->cv.wait(lock, [pool]() { return pool->closed || !pool->idleSessions.empty(); });
    }

    auto rc = CreateH2DSession(deviceId, session);
    if (rc.IsOk()) {
        return Status::OK();
    }

    std::lock_guard<std::mutex> lock(pool->mutex);
    --pool->totalSessions;
    --pool->borrowedSessions;
    pool->cv.notify_all();
    return rc;
}

Status CudaResourceManager::AcquireD2HSession(int32_t deviceId, std::unique_ptr<GpuD2HSession> &session)
{
    auto *pool = GetOrCreateD2HPool(deviceId);
    bool needCreateSession = false;
    while (!needCreateSession) {
        std::unique_lock<std::mutex> lock(pool->mutex);
        if (pool->closed) {
            return Status(K_RUNTIME_ERROR, "CUDA D2H session pool is closed.");
        }
        if (!pool->idleSessions.empty()) {
            session = std::move(pool->idleSessions.front());
            pool->idleSessions.pop_front();
            ++pool->borrowedSessions;
            return Status::OK();
        }
        if (pool->totalSessions < pool->maxSessions) {
            ++pool->totalSessions;
            ++pool->borrowedSessions;
            needCreateSession = true;
            continue;
        }
        pool->cv.wait(lock, [pool]() { return pool->closed || !pool->idleSessions.empty(); });
    }

    auto rc = CreateD2HSession(deviceId, session);
    if (rc.IsOk()) {
        return Status::OK();
    }

    std::lock_guard<std::mutex> lock(pool->mutex);
    --pool->totalSessions;
    --pool->borrowedSessions;
    pool->cv.notify_all();
    return rc;
}

void CudaResourceManager::ReleaseH2DSession(std::unique_ptr<GpuH2DSession> session, bool reusable)
{
    auto *pool = GetH2DPool(session == nullptr ? -1 : session->deviceId);
    if (pool == nullptr) {
        DestroyH2DSession(std::move(session));
        return;
    }
    if (reusable) {
        ResetH2DSession(*session);
    }

    std::unique_ptr<GpuH2DSession> sessionToDestroy;
    bool notifyAll = false;
    {
        std::lock_guard<std::mutex> lock(pool->mutex);
        if (reusable && !pool->closed) {
            pool->idleSessions.emplace_back(std::move(session));
        } else {
            sessionToDestroy = std::move(session);
            --pool->totalSessions;
        }
        --pool->borrowedSessions;
        notifyAll = pool->closed;
    }

    if (sessionToDestroy != nullptr) {
        DestroyH2DSession(std::move(sessionToDestroy));
    }
    if (notifyAll) {
        pool->cv.notify_all();
    } else {
        pool->cv.notify_one();
    }
}

void CudaResourceManager::ReleaseD2HSession(std::unique_ptr<GpuD2HSession> session, bool reusable)
{
    auto *pool = GetD2HPool(session == nullptr ? -1 : session->deviceId);
    if (pool == nullptr) {
        DestroyD2HSession(std::move(session));
        return;
    }
    if (reusable) {
        ResetD2HSession(*session);
    }

    std::unique_ptr<GpuD2HSession> sessionToDestroy;
    bool notifyAll = false;
    {
        std::lock_guard<std::mutex> lock(pool->mutex);
        if (reusable && !pool->closed) {
            pool->idleSessions.emplace_back(std::move(session));
        } else {
            sessionToDestroy = std::move(session);
            --pool->totalSessions;
        }
        --pool->borrowedSessions;
        notifyAll = pool->closed;
    }

    if (sessionToDestroy != nullptr) {
        DestroyD2HSession(std::move(sessionToDestroy));
    }
    if (notifyAll) {
        pool->cv.notify_all();
    } else {
        pool->cv.notify_one();
    }
}

template <typename SessionT, typename DestroyFn>
void CudaResourceManager::ShutdownPools(std::mutex &poolsMutex,
                                        std::unordered_map<int32_t, std::unique_ptr<SessionPool<SessionT>>> &pools,
                                        DestroyFn destroyFn)
{
    std::vector<SessionPool<SessionT> *> poolList;
    {
        std::lock_guard<std::mutex> lock(poolsMutex);
        poolList.reserve(pools.size());
        for (auto &item : pools) {
            poolList.emplace_back(item.second.get());
        }
    }

    for (auto *pool : poolList) {
        std::deque<std::unique_ptr<SessionT>> sessions;
        {
            std::unique_lock<std::mutex> lock(pool->mutex);
            pool->closed = true;
            pool->cv.notify_all();
            pool->cv.wait(lock, [pool]() { return pool->borrowedSessions == 0; });
            sessions = std::move(pool->idleSessions);
            pool->totalSessions = 0;
        }
        for (auto &session : sessions) {
            destroyFn(std::move(session));
        }
    }

    std::lock_guard<std::mutex> lock(poolsMutex);
    pools.clear();
}

void CudaResourceManager::ShutdownSessionPools()
{
    ShutdownPools(h2dPoolsMutex_, h2dPools_,
                  [this](std::unique_ptr<GpuH2DSession> session) { DestroyH2DSession(std::move(session)); });
    ShutdownPools(d2hPoolsMutex_, d2hPools_,
                  [this](std::unique_ptr<GpuD2HSession> session) { DestroyD2HSession(std::move(session)); });
}

Status CudaResourceManager::EnsurePinnedSlotCapacity(GpuPinnedSlotBase &slot, uint64_t objectSize)
{
    if (objectSize <= slot.capacity) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(Host() != nullptr, K_RUNTIME_ERROR,
                             "HostMemMgr is not initialized for GPU pinned pipeline");
    if (slot.capacity > 0) {
        RETURN_IF_NOT_OK(Host()->Free(slot.hostPinnedPool));
        slot.capacity = 0;
    }

    std::vector<BufferMetaInfo> bufferMetas{ BufferMetaInfo{
        .blobCount = 1, .firstBlobOffset = 0, .size = objectSize } };
    RETURN_IF_NOT_OK(Host()->Allocate(bufferMetas, slot.hostPinnedPool, true));
    slot.capacity = slot.hostPinnedPool[0].size;
    return Status::OK();
}

void CudaResourceManager::TrimPinnedSlot(GpuPinnedSlotBase &slot)
{
    if (slot.capacity <= MAX_RETAINED_PINNED_BUFFER_BYTES || Host() == nullptr) {
        return;
    }
    LOG_IF_ERROR(Host()->Free(slot.hostPinnedPool), "Trim host pinned buffer failed.");
    slot.capacity = 0;
}

void CudaResourceManager::ResetH2DSession(GpuH2DSession &session)
{
    session.nextVictim = 0;
    for (auto &slot : session.slots) {
        slot.inFlight = false;
        slot.lastObjectIndex = 0;
        TrimPinnedSlot(slot);
    }
}

void CudaResourceManager::ResetD2HSession(GpuD2HSession &session)
{
    session.nextVictim = 0;
    for (auto &slot : session.slots) {
        slot.phase = GpuD2HSlotPhase::AVAILABLE;
        slot.lastObjectIndex = 0;
        TrimPinnedSlot(slot);
    }
}

Status CudaResourceManager::TailWaitH2D(GpuH2DSession &session)
{
    RETURN_IF_NOT_OK(devManager_->SynchronizeStream(session.directStream));
    for (auto &slot : session.slots) {
        if (!slot.inFlight) {
            continue;
        }
        RETURN_IF_NOT_OK(devManager_->SynchronizeEvent(slot.doneEvent));
        slot.inFlight = false;
    }
    return Status::OK();
}

Status CudaResourceManager::AcquireH2DSlot(GpuH2DSession &session, GpuH2DSlot *&slot)
{
    CHECK_FAIL_RETURN_STATUS(!session.slots.empty(), K_RUNTIME_ERROR, "GPU H2D pipeline slots are empty.");
    for (size_t attempt = 0; attempt < session.slots.size(); attempt++) {
        size_t index = (session.nextVictim + attempt) % session.slots.size();
        auto &candidate = session.slots[index];
        if (!candidate.inFlight) {
            session.nextVictim = (index + 1) % session.slots.size();
            slot = &candidate;
            return Status::OK();
        }
        if (devManager_->QueryEventStatus(candidate.doneEvent).IsOk()) {
            candidate.inFlight = false;
            session.nextVictim = (index + 1) % session.slots.size();
            slot = &candidate;
            return Status::OK();
        }
    }

    auto &candidate = session.slots[session.nextVictim];
    RETURN_IF_NOT_OK(devManager_->SynchronizeEvent(candidate.doneEvent));
    candidate.inFlight = false;
    session.nextVictim = (session.nextVictim + 1) % session.slots.size();
    slot = &candidate;
    return Status::OK();
}

Status CudaResourceManager::SubmitObjectWithH2DPipeline(const DeviceBatchCopyHelper &helper, size_t objectIndex,
                                                        GpuH2DSession &session)
{
    CHECK_FAIL_RETURN_STATUS(objectIndex < helper.bufferMetas.size(), K_INVALID,
                             FormatString("Invalid object index %zu", objectIndex));
    GpuH2DSlot *slot = nullptr;
    RETURN_IF_NOT_OK(AcquireH2DSlot(session, slot));

    const auto &meta = helper.bufferMetas[objectIndex];
    auto &srcBuffer = helper.srcBuffers[objectIndex];
    RETURN_IF_NOT_OK(EnsurePinnedSlotCapacity(*slot, meta.size));

    auto *hostPinnedPtr = slot->hostPinnedPool[0].pointer;
    RETURN_IF_NOT_OK(Host()->HostMemoryCopy(hostPinnedPtr, meta.size, srcBuffer.ptr, srcBuffer.size));

    size_t offset = 0;
    for (size_t blobIndex = 0; blobIndex < meta.blobCount; blobIndex++) {
        size_t flatBlobIndex = meta.firstBlobOffset + blobIndex;
        CHECK_FAIL_RETURN_STATUS(
            flatBlobIndex < helper.dstBuffers.size(), K_RUNTIME_ERROR,
            FormatString("Invalid flat blob index %zu for object %zu", flatBlobIndex, objectIndex));
        auto &dstBuffer = helper.dstBuffers[flatBlobIndex];
        auto rc = devManager_->MemcpyAsync(dstBuffer.ptr, dstBuffer.size,
                                           static_cast<void *>(static_cast<uint8_t *>(hostPinnedPtr) + offset),
                                           dstBuffer.size, MemcpyKind::HOST_TO_DEVICE, slot->stream);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Pipeline H2D submit failed for object %zu, blob %zu: %s", objectIndex,
                                       blobIndex, rc.ToString().c_str());
            return rc;
        }
        offset += dstBuffer.size;
    }

    RETURN_IF_NOT_OK(devManager_->RecordEvent(slot->doneEvent, slot->stream));
    slot->inFlight = true;
    slot->lastObjectIndex = objectIndex;
    return Status::OK();
}

Status CudaResourceManager::DrainD2HSlotToHost(const DeviceBatchCopyHelper &helper, GpuD2HSlot &slot)
{
    CHECK_FAIL_RETURN_STATUS(Host() != nullptr, K_RUNTIME_ERROR, "HostMemMgr is not initialized for GPU D2H pipeline");
    CHECK_FAIL_RETURN_STATUS(slot.lastObjectIndex < helper.bufferMetas.size(), K_RUNTIME_ERROR,
                             FormatString("Invalid D2H slot object index %zu", slot.lastObjectIndex));
    CHECK_FAIL_RETURN_STATUS(slot.lastObjectIndex < helper.dstBuffers.size(), K_RUNTIME_ERROR,
                             FormatString("Invalid D2H destination object index %zu", slot.lastObjectIndex));

    const auto &meta = helper.bufferMetas[slot.lastObjectIndex];
    const auto &dstBuffer = helper.dstBuffers[slot.lastObjectIndex];
    auto *hostPinnedPtr = slot.hostPinnedPool[0].pointer;
    CHECK_FAIL_RETURN_STATUS(hostPinnedPtr != nullptr, K_RUNTIME_ERROR, "D2H pinned host buffer is null");
    CHECK_FAIL_RETURN_STATUS(meta.size <= slot.capacity, K_RUNTIME_ERROR,
                             FormatString("D2H slot capacity %llu too small for object size %zu",
                                          static_cast<unsigned long long>(slot.capacity), meta.size));
    CHECK_FAIL_RETURN_STATUS(
        meta.size <= dstBuffer.size, K_RUNTIME_ERROR,
        FormatString("D2H destination size %zu too small for object size %zu", dstBuffer.size, meta.size));
    RETURN_IF_NOT_OK(Host()->HostMemoryCopy(dstBuffer.ptr, dstBuffer.size, hostPinnedPtr, meta.size));
    return Status::OK();
}

Status CudaResourceManager::AcquireD2HSlot(const DeviceBatchCopyHelper &helper, GpuD2HSession &session,
                                           GpuD2HSlot *&slot)
{
    CHECK_FAIL_RETURN_STATUS(!session.slots.empty(), K_RUNTIME_ERROR, "GPU D2H pipeline slots are empty.");
    for (size_t attempt = 0; attempt < session.slots.size(); attempt++) {
        size_t index = (session.nextVictim + attempt) % session.slots.size();
        auto &candidate = session.slots[index];
        if (candidate.phase == GpuD2HSlotPhase::AVAILABLE) {
            session.nextVictim = (index + 1) % session.slots.size();
            slot = &candidate;
            return Status::OK();
        }
        if (candidate.phase == GpuD2HSlotPhase::IN_FLIGHT_D2H
            && devManager_->QueryEventStatus(candidate.doneEvent).IsOk()) {
            candidate.phase = GpuD2HSlotPhase::READY_FOR_DRAIN;
        }
        if (candidate.phase == GpuD2HSlotPhase::READY_FOR_DRAIN) {
            RETURN_IF_NOT_OK(DrainD2HSlotToHost(helper, candidate));
            candidate.phase = GpuD2HSlotPhase::AVAILABLE;
            session.nextVictim = (index + 1) % session.slots.size();
            slot = &candidate;
            return Status::OK();
        }
    }

    auto &candidate = session.slots[session.nextVictim];
    if (candidate.phase == GpuD2HSlotPhase::IN_FLIGHT_D2H) {
        RETURN_IF_NOT_OK(devManager_->SynchronizeEvent(candidate.doneEvent));
        candidate.phase = GpuD2HSlotPhase::READY_FOR_DRAIN;
    }
    if (candidate.phase == GpuD2HSlotPhase::READY_FOR_DRAIN) {
        RETURN_IF_NOT_OK(DrainD2HSlotToHost(helper, candidate));
        candidate.phase = GpuD2HSlotPhase::AVAILABLE;
    }
    session.nextVictim = (session.nextVictim + 1) % session.slots.size();
    slot = &candidate;
    return Status::OK();
}

Status CudaResourceManager::SubmitObjectWithD2HPipeline(const DeviceBatchCopyHelper &helper, size_t objectIndex,
                                                        GpuD2HSession &session)
{
    CHECK_FAIL_RETURN_STATUS(objectIndex < helper.bufferMetas.size(), K_INVALID,
                             FormatString("Invalid object index %zu", objectIndex));
    GpuD2HSlot *slot = nullptr;
    RETURN_IF_NOT_OK(AcquireD2HSlot(helper, session, slot));

    const auto &meta = helper.bufferMetas[objectIndex];
    RETURN_IF_NOT_OK(EnsurePinnedSlotCapacity(*slot, meta.size));

    auto *hostPinnedPtr = slot->hostPinnedPool[0].pointer;
    CHECK_FAIL_RETURN_STATUS(hostPinnedPtr != nullptr, K_RUNTIME_ERROR, "D2H pinned host buffer is null");

    size_t offset = 0;
    for (size_t blobIndex = 0; blobIndex < meta.blobCount; blobIndex++) {
        size_t flatBlobIndex = meta.firstBlobOffset + blobIndex;
        CHECK_FAIL_RETURN_STATUS(
            flatBlobIndex < helper.srcBuffers.size(), K_RUNTIME_ERROR,
            FormatString("Invalid flat blob index %zu for object %zu", flatBlobIndex, objectIndex));
        const auto &srcBuffer = helper.srcBuffers[flatBlobIndex];
        CHECK_FAIL_RETURN_STATUS(
            offset + srcBuffer.size <= slot->capacity, K_RUNTIME_ERROR,
            FormatString("Pipeline D2H offset overflow for object %zu, blob %zu", objectIndex, blobIndex));
        auto *dstPtr = static_cast<void *>(static_cast<uint8_t *>(hostPinnedPtr) + offset);
        auto rc = devManager_->MemcpyAsync(dstPtr, slot->capacity - offset, srcBuffer.ptr, srcBuffer.size,
                                           MemcpyKind::DEVICE_TO_HOST, slot->stream);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Pipeline D2H submit failed for object %zu, blob %zu: %s", objectIndex,
                                       blobIndex, rc.ToString().c_str());
            return rc;
        }
        offset += srcBuffer.size;
    }

    RETURN_IF_NOT_OK(devManager_->RecordEvent(slot->doneEvent, slot->stream));
    slot->phase = GpuD2HSlotPhase::IN_FLIGHT_D2H;
    slot->lastObjectIndex = objectIndex;
    return Status::OK();
}

Status CudaResourceManager::TailWaitD2H(const DeviceBatchCopyHelper &helper, GpuD2HSession &session)
{
    RETURN_IF_NOT_OK(devManager_->SynchronizeStream(session.directStream));
    for (auto &slot : session.slots) {
        if (slot.phase == GpuD2HSlotPhase::IN_FLIGHT_D2H) {
            RETURN_IF_NOT_OK(devManager_->SynchronizeEvent(slot.doneEvent));
            slot.phase = GpuD2HSlotPhase::READY_FOR_DRAIN;
        }
        if (slot.phase == GpuD2HSlotPhase::READY_FOR_DRAIN) {
            RETURN_IF_NOT_OK(DrainD2HSlotToHost(helper, slot));
            slot.phase = GpuD2HSlotPhase::AVAILABLE;
        }
    }
    return Status::OK();
}

size_t CudaResourceManager::GetH2DPipelineDepth() const
{
    return h2dPipelineDepth_;
}

size_t CudaResourceManager::GetD2HPipelineDepth() const
{
    return d2hPipelineDepth_;
}

}  // namespace datasystem
