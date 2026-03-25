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
 * Description: CUDA resource manager for GPU support.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_RESOURCE_MANAGER_H
#define DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_RESOURCE_MANAGER_H

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "datasystem/common/device/device_batch_copy_helper.h"
#include "datasystem/common/device/device_resource_manager.h"

namespace datasystem {

class CudaResourceManager : public DeviceResourceManager {
public:
    CudaResourceManager();
    ~CudaResourceManager() override;

    Status MemcpyBatchD2H(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList) override;
    Status MemcpyBatchH2D(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList) override;
    void SetPolicyByHugeTlb(bool enableHugeTlb) override;

private:
    static constexpr size_t MAX_SESSIONS_PER_DEVICE = 4;
    static constexpr uint64_t MAX_RETAINED_PINNED_BUFFER_BYTES = 64ULL * 1024ULL * 1024ULL;

    Status CudaMemcpyBatch(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList,
                           MemcpyKind copyKind);
    Status CudaMemcpyBatchSync(DeviceBatchCopyHelper &helper, MemcpyKind copyKind);
    Status SubmitBatchH2D(DeviceBatchCopyHelper &helper, int32_t deviceId);
    Status SubmitBatchD2H(DeviceBatchCopyHelper &helper, int32_t deviceId);
    Status SubmitBatchDirectAsync(DeviceBatchCopyHelper &helper, int32_t deviceId, MemcpyKind copyKind);
    Status SubmitObjectDirectAsync(const DeviceBatchCopyHelper &helper, size_t objectIndex, void *stream,
                                   MemcpyKind copyKind);
    bool NeedPooledSession(const DeviceBatchCopyHelper &helper) const;

    struct GpuPinnedSlotBase {
        GpuPinnedSlotBase() : hostPinnedPool(1)
        {
        }

        std::vector<ShmUnit> hostPinnedPool;
        void *stream = nullptr;
        void *doneEvent = nullptr;
        uint64_t capacity = 0;
        size_t lastObjectIndex = 0;
    };

    struct GpuH2DSlot : GpuPinnedSlotBase {
        bool inFlight = false;
    };

    enum class GpuD2HSlotPhase {
        AVAILABLE,
        IN_FLIGHT_D2H,
        READY_FOR_DRAIN,
    };

    struct GpuD2HSlot : GpuPinnedSlotBase {
        GpuD2HSlotPhase phase = GpuD2HSlotPhase::AVAILABLE;
    };

    struct GpuH2DSession {
        int32_t deviceId = -1;
        void *directStream = nullptr;
        size_t nextVictim = 0;
        std::vector<GpuH2DSlot> slots;
    };

    struct GpuD2HSession {
        int32_t deviceId = -1;
        void *directStream = nullptr;
        size_t nextVictim = 0;
        std::vector<GpuD2HSlot> slots;
    };

    template <typename SessionT>
    struct SessionPool {
        size_t maxSessions = MAX_SESSIONS_PER_DEVICE;
        bool closed = false;
        size_t totalSessions = 0;
        size_t borrowedSessions = 0;
        std::mutex mutex;
        std::condition_variable cv;
        std::deque<std::unique_ptr<SessionT>> idleSessions;
    };

    Status CreateH2DSession(int32_t deviceId, std::unique_ptr<GpuH2DSession> &session);
    Status CreateD2HSession(int32_t deviceId, std::unique_ptr<GpuD2HSession> &session);
    void DestroyH2DSession(std::unique_ptr<GpuH2DSession> session);
    void DestroyD2HSession(std::unique_ptr<GpuD2HSession> session);
    Status AcquireH2DSession(int32_t deviceId, std::unique_ptr<GpuH2DSession> &session);
    Status AcquireD2HSession(int32_t deviceId, std::unique_ptr<GpuD2HSession> &session);
    void ReleaseH2DSession(std::unique_ptr<GpuH2DSession> session, bool reusable);
    void ReleaseD2HSession(std::unique_ptr<GpuD2HSession> session, bool reusable);
    SessionPool<GpuH2DSession> *GetOrCreateH2DPool(int32_t deviceId);
    SessionPool<GpuD2HSession> *GetOrCreateD2HPool(int32_t deviceId);
    SessionPool<GpuH2DSession> *GetH2DPool(int32_t deviceId);
    SessionPool<GpuD2HSession> *GetD2HPool(int32_t deviceId);
    template <typename SessionT, typename DestroyFn>
    void ShutdownPools(std::mutex &poolsMutex,
                       std::unordered_map<int32_t, std::unique_ptr<SessionPool<SessionT>>> &pools,
                       DestroyFn destroyFn);
    void ShutdownSessionPools();

    Status EnsurePinnedSlotCapacity(GpuPinnedSlotBase &slot, uint64_t objectSize);
    void TrimPinnedSlot(GpuPinnedSlotBase &slot);
    void ResetH2DSession(GpuH2DSession &session);
    void ResetD2HSession(GpuD2HSession &session);

    Status TailWaitH2D(GpuH2DSession &session);
    Status TailWaitD2H(const DeviceBatchCopyHelper &helper, GpuD2HSession &session);
    Status AcquireH2DSlot(GpuH2DSession &session, GpuH2DSlot *&slot);
    Status AcquireD2HSlot(const DeviceBatchCopyHelper &helper, GpuD2HSession &session, GpuD2HSlot *&slot);
    Status DrainD2HSlotToHost(const DeviceBatchCopyHelper &helper, GpuD2HSlot &slot);
    Status SubmitObjectWithH2DPipeline(const DeviceBatchCopyHelper &helper, size_t objectIndex,
                                       GpuH2DSession &session);
    Status SubmitObjectWithD2HPipeline(const DeviceBatchCopyHelper &helper, size_t objectIndex,
                                       GpuD2HSession &session);
    size_t GetH2DPipelineDepth() const;
    size_t GetD2HPipelineDepth() const;

    DeviceManagerBase *devManager_ = nullptr;
    size_t h2dPipelineDepth_ = 4;
    size_t d2hPipelineDepth_ = 4;
    std::mutex h2dPoolsMutex_;
    std::unordered_map<int32_t, std::unique_ptr<SessionPool<GpuH2DSession>>> h2dPools_;
    std::mutex d2hPoolsMutex_;
    std::unordered_map<int32_t, std::unique_ptr<SessionPool<GpuD2HSession>>> d2hPools_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_RESOURCE_MANAGER_H
