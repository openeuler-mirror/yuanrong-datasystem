/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Data system AclMemMgr.
 */

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_ACL_MEM_MGR_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_ACL_MEM_MGR_H

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/ascend/acl_parallel_direct_executor.h"
#include "datasystem/common/device/ascend/acl_parallel_ffts_executor.h"
#include "datasystem/common/device/ascend/callback_thread.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/ffts_dispatcher.h"
#include "datasystem/common/device/device_batch_copy_helper.h"
#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const size_t FFTS_PIPELINE = 2;
const size_t MAX_DEVICE_COUNT = 64;
struct DataMetaInfo {
    size_t blobCount;
    size_t firstBlobOffset;
    void *ptr;
    size_t size;
};

struct AclResource {
    void *primaryStream = nullptr;
    void *secondaryStream = nullptr;
    void *toDestDone[FFTS_PIPELINE]{};
    void *toPinDone[FFTS_PIPELINE]{};
    bool subscribeReport = false;
};

struct FftsResourceBundle {
    AclResource resource;
    std::unique_ptr<ffts::FftsDispatcher> dispatcher;
};

class AclResourceManager;
class FftsPipelineCopierBase;

class AclMemCopyPool {
public:
    AclMemCopyPool(AclResourceManager *resourceMgr);
    /**
     * @brief Perform a batch memory copy operation on the NPU.
     *
     * @param[in] copyKind   Type of memory copy.
     * @param[in] helper     Helper object that holds the batch copy tasks.
     * @param[in] deviceId   Target device ID.
     * @return Status K_OK on success; an error code otherwise.
     */
    Status MemcpyBatchD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcopyPolicy policy);

    Status MemcpyBatchH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcopyPolicy policy);

    ~AclMemCopyPool();

private:
    bool ShouldFallbackToDirectForH2D(const DeviceBatchCopyHelper &helper, MemcopyPolicy policy);
    bool ShouldUseParallelDirect(const DeviceBatchCopyHelper &helper, const ParallelH2DConfig &config) const;
    bool ShouldUseParallelDirect(const DeviceBatchCopyHelper &helper, const ParallelD2HConfig &config) const;
    bool ShouldUseParallelFfts(const DeviceBatchCopyHelper &helper, const ParallelFftsH2DConfig &config) const;
    bool ShouldUseParallelFfts(const DeviceBatchCopyHelper &helper, const ParallelFftsD2HConfig &config) const;
    Status MemcpyFftsH2DSerial(uint32_t deviceId, DeviceBatchCopyHelper &helper);
    Status MemcpyFftsD2HSerial(uint32_t deviceId, DeviceBatchCopyHelper &helper);
    Status ExecuteFftsH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper, bool parallelShard,
                          ThreadPool *deviceSubmitPool);
    Status ExecuteFftsD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper, bool parallelShard,
                          ThreadPool *deviceSubmitPool);
    Status MemcpyBatchDirect(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind kind, PerfPoint &point);
    Status MemcpyBatchFftsD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper, PerfPoint &point);
    Status MemcpyBatchFftsH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcopyPolicy policy, PerfPoint &point);
    Status RunFftsD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper);
    Status RunFftsH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper);
    Status HandleFftsResult(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind kind, Status fftsRc);
    Status GetOrCreateParallelFftsExecutor(MemcpyKind kind, AclParallelFftsExecutor *&executor);
    Status GetOrCreateParallelDirectExecutor(uint32_t deviceId, MemcpyKind kind,
                                             std::shared_ptr<AclParallelDirectExecutor> &executor);
    Status AclMemcpyBatch(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind copyKind);
    std::unique_ptr<ThreadPool> copyPool_;
    std::unique_ptr<ThreadPool> h2hCopyPool_;
    std::unique_ptr<ThreadPool> fftsCopyPool_;
    std::unique_ptr<AclParallelFftsExecutor> parallelFftsH2DExecutor_;
    std::unique_ptr<AclParallelFftsExecutor> parallelFftsD2HExecutor_;
    std::vector<void *> copyStreams_;
    int32_t deviceNow_ = -1;
    DeviceManagerBase *devInterImpl_;
    AclResourceManager *resourceMgr_;
    std::mutex parallelExecutorMutex_;
    // Bound outstanding FFTS work to one call and at most workerNum parallel tasks.
    std::mutex parallelFftsCallMutex_;
    std::unordered_map<uint32_t, std::shared_ptr<AclParallelDirectExecutor>> parallelH2DExecutors_;
    std::unordered_map<uint32_t, std::shared_ptr<AclParallelDirectExecutor>> parallelD2HExecutors_;
};

class AclResourceManager : public DeviceResourceManager {
public:
    AclResourceManager();
    ~AclResourceManager() = default;

    Status MemcpyBatchD2H(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList) override;
    Status MemcpyBatchH2D(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList) override;
    Status MemcpyBatchH2D(const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                          const std::vector<Buffer *> &bufferList) override;
    Status MemcpyBatchD2H(const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                          const std::vector<Buffer *> &bufferList) override;

    const ParallelH2DConfig &GetParallelH2DConfig() const
    {
        return parallelH2DConfig_;
    }

    const Status &GetParallelH2DConfigStatus() const
    {
        return parallelH2DConfigStatus_;
    }

    const ParallelD2HConfig &GetParallelD2HConfig() const
    {
        return parallelD2HConfig_;
    }

    const Status &GetParallelD2HConfigStatus() const
    {
        return parallelD2HConfigStatus_;
    }

    const ParallelFftsH2DConfig &GetParallelFftsH2DConfig() const
    {
        return parallelFftsH2DConfig_;
    }

    const Status &GetParallelFftsH2DConfigStatus() const
    {
        return parallelFftsH2DConfigStatus_;
    }

    const ParallelFftsD2HConfig &GetParallelFftsD2HConfig() const
    {
        return parallelFftsD2HConfig_;
    }

    const Status &GetParallelFftsD2HConfigStatus() const
    {
        return parallelFftsD2HConfigStatus_;
    }

    void SetPolicyByHugeTlb(bool enableHugeTlb) override
    {
        if (enableHugeTlb && policyD2H == MemcopyPolicy::FFTS) {
            policyD2H = MemcopyPolicy::HUGE_FFTS;
        }
        if (enableHugeTlb && policyH2D == MemcopyPolicy::FFTS) {
            policyH2D = MemcopyPolicy::HUGE_FFTS;
        }
        // FftsPipelineCopierBase skips the host staging pool only when both directions use HUGE_FFTS.
        if (enableHugeTlb && policyD2H == MemcopyPolicy::HUGE_FFTS && policyH2D == MemcopyPolicy::HUGE_FFTS) {
            hostMemSize = 0;
        }
    }

    class DeviceResource {
    public:
        DeviceResource(uint32_t deviceId) : deviceId_(deviceId)
        {
        }
        Status CreateAclRtStream(bool subscribeReport, aclrtStream &stream);
        Status FreeAclRtStream(bool subscribeReport, aclrtStream stream);
        Status CreateRtNotify(rtNotify_t &notify);
        Status FreeRtNotify(rtNotify_t notify);
        Status CreateFftsDispatcher(std::unique_ptr<ffts::FftsDispatcher> &dispatcher);
        void FreeFftsDispatcher(std::unique_ptr<ffts::FftsDispatcher> dispatcher);
        Status CreateFftsResourceBundle(bool subscribeReport, std::unique_ptr<FftsResourceBundle> &bundle);
        void FreeFftsResourceBundle(std::unique_ptr<FftsResourceBundle> bundle, size_t cacheSize);
        std::unique_ptr<std::vector<ShmUnit>> TakeFftsDeviceStaging(
            size_t minSize, std::unique_ptr<std::vector<ShmUnit>> &undersizedPool);
        std::unique_ptr<std::vector<ShmUnit>> CacheFftsDeviceStaging(
            std::unique_ptr<std::vector<ShmUnit>> memoryPool, size_t cacheSize, uint64_t maxCacheBytes);

    private:
        Status InitCallbackThread();
        const size_t CACHE_SIZE = 8;
        uint32_t deviceId_;
        // Protects callback setup plus the legacy individual-resource and capacity-aware staging caches.
        std::shared_timed_mutex mutex_;
        // Complete control bundles never nest this lock with mutex_, so hot-path acquire/release cannot contend with
        // staging-cache lookups or individual-resource cold-path cleanup.
        std::mutex fftsResourceBundleMutex_;
        std::unique_ptr<acl::CallbackThread> callbackThread_;
        std::deque<std::unique_ptr<ffts::FftsDispatcher>> fftsDispatcherQueue_;
        std::deque<std::unique_ptr<FftsResourceBundle>> fftsResourceBundleQueue_;
        std::deque<std::unique_ptr<FftsResourceBundle>> subscribeReportFftsResourceBundleQueue_;
        std::deque<std::unique_ptr<std::vector<ShmUnit>>> fftsDeviceStagingQueue_;
        std::deque<aclrtStream> streamQueue_;
        std::deque<aclrtStream> subscribeReportStreamQueue_;
        std::deque<rtNotify_t> notifyQueue_;
    };

private:
    friend class FftsPipelineCopierBase;

    Status CreateAclRtStream(uint32_t deviceId, aclrtStream &stream, bool subscribeReport);
    Status FreeAclRtStream(uint32_t deviceId, aclrtStream stream, bool subscribeReport);
    Status CreateRtNotify(uint32_t deviceIdx, rtNotify_t &notify);
    Status FreeRtNotify(uint32_t deviceId, rtNotify_t notify);
    Status CreateFftsDispatcher(uint32_t deviceId, std::unique_ptr<ffts::FftsDispatcher> &dispatcher);
    void FreeFftsDispatcher(uint32_t deviceId, std::unique_ptr<ffts::FftsDispatcher> dispatcher);
    Status CreateFftsResourceBundle(uint32_t deviceId, bool subscribeReport,
                                    std::unique_ptr<FftsResourceBundle> &bundle);
    void FreeFftsResourceBundle(uint32_t deviceId, std::unique_ptr<FftsResourceBundle> bundle);
    Status AcquireFftsDeviceStaging(uint32_t deviceId, const std::vector<BufferMetaInfo> &bufferMetas,
                                    std::unique_ptr<std::vector<ShmUnit>> &memoryPool);
    Status ReleaseFftsDeviceStaging(uint32_t deviceId, std::unique_ptr<std::vector<ShmUnit>> memoryPool);

    ParallelH2DConfig parallelH2DConfig_;
    Status parallelH2DConfigStatus_;
    ParallelD2HConfig parallelD2HConfig_;
    Status parallelD2HConfigStatus_;
    ParallelFftsH2DConfig parallelFftsH2DConfig_;
    Status parallelFftsH2DConfigStatus_;
    ParallelFftsD2HConfig parallelFftsD2HConfig_;
    Status parallelFftsD2HConfigStatus_;
    std::vector<std::unique_ptr<DeviceResource>> deviceResources_;
    std::unique_ptr<AclMemCopyPool> swapOutPool_;
    std::unique_ptr<AclMemCopyPool> swapInPool_;
};

struct PipelineH2DTasks {
    std::vector<BufferView> srcBuffers;
    std::vector<BufferView> destBuffers;
    std::vector<BufferMetaInfo> bufferMetas;
    bool IsEmpty()
    {
        return srcBuffers.empty();
    }
};

struct PipelineH2HTasks {
    std::vector<size_t> indexes;
    bool IsEmpty()
    {
        return indexes.empty();
    }
};

struct D2HCallbackState {
    explicit D2HCallbackState(size_t expectedCount) : expectedCount(expectedCount)
    {
        // The C callback must not let an allocation exception cross the runtime callback boundary.
        tasks.indexes.reserve(expectedCount);
    }

    void Complete(size_t index)
    {
        std::unique_lock<std::mutex> locker(mutex);
        if (!forced) {
            tasks.indexes.emplace_back(index);
        }
        finishCount++;
        cv.notify_one();
    }

    void ForceFinish()
    {
        std::unique_lock<std::mutex> locker(mutex);
        forced = true;
        finishCount = expectedCount;
        tasks.indexes.clear();
        cv.notify_one();
    }

    std::mutex mutex;
    std::condition_variable cv;
    PipelineH2HTasks tasks;
    size_t finishCount = 0;
    bool forced = false;
    const size_t expectedCount;
};

struct D2HCallbackDataStorage;

class FftsPipelineCopierBase {
public:
    FftsPipelineCopierBase(int32_t deviceId, AclResourceManager *aclResourceMgr,
                           const std::vector<BufferMetaInfo> &bufferMetas, ThreadPool *h2hCopyPool,
                           ThreadPool *fftsCopyPool);
    ~FftsPipelineCopierBase();

    FftsPipelineCopierBase(const FftsPipelineCopierBase &) = delete;
    FftsPipelineCopierBase &operator=(const FftsPipelineCopierBase &) = delete;

protected:
    Status GetBufferViews(size_t count, const std::vector<ShmUnit> &memoryPool, std::vector<BufferView> &buffers);

    Status AllocAndInitTransferBuffers(const std::vector<BufferView> &hostBuffer);

    bool IsFinish()
    {
        return finishCount_ >= bufferMetas_.size();
    }

    Status InitAclResource(bool subscribeReport);

    Status NotifyStart();
    Status WaitFinish();

    acl::AclDeviceManager *aclDeviceManager_;
    AclResourceManager *aclResourceMgr_;
    bool skipH2HMemcpy_ = false;
    const int32_t deviceId_;
    const std::vector<BufferMetaInfo> &bufferMetas_;
    AclResource resource_;
    std::unique_ptr<ffts::FftsDispatcher> fftsDispatcher_;
    std::unique_ptr<FftsResourceBundle> resourceBundle_;
    ThreadPool *h2hCopyPool_;
    ThreadPool *fftsCopyPool_;
    bool primaryStreamSynchronized_ = false;

    std::vector<BufferView> transferHostBuffers_;
    std::vector<BufferView> transferDeviceBuffers_;
    std::unique_ptr<std::vector<ShmUnit>> transferHostPool_;
    std::unique_ptr<std::vector<ShmUnit>> transferDevicePool_;
    bool cacheDeviceStaging_ = false;

    // Only the D2H copier populates these. Kept on the base so the abandon path in ~FftsPipelineCopierBase can
    // break the state <-> callback-data cycle when stream synchronization fails; H2D leaves them null.
    std::shared_ptr<D2HCallbackState> callbackState_;
    std::shared_ptr<D2HCallbackDataStorage> callbackDataStorage_;

    std::mutex mutex_;
    std::condition_variable cv_;
    size_t finishCount_;
};

class FftsPipelineH2DCopier : public FftsPipelineCopierBase {
public:
    FftsPipelineH2DCopier(int32_t deviceId, AclResourceManager *aclResourceMgr,
                          const std::vector<BufferMetaInfo> &bufferMetas, ThreadPool *h2hCopyPool,
                          ThreadPool *fftsCopyPool);
    ~FftsPipelineH2DCopier() = default;
    FftsPipelineH2DCopier(const FftsPipelineH2DCopier &) = delete;
    FftsPipelineH2DCopier &operator=(const FftsPipelineH2DCopier &) = delete;

    Status ExecuteMemcpy(const std::vector<BufferView> &deviceBuffers, const std::vector<BufferView> &hostBuffers);

    // Used by the bounded outer FFTS executor; running inline avoids serializing again on fftsCopyPool_.
    Status ExecuteMemcpyInlineFfts(const std::vector<BufferView> &deviceBuffers,
                                   const std::vector<BufferView> &hostBuffers);

    Status AddFftsNotifyTask(size_t index, const std::vector<BufferView> &deviceBuffers, bool addTask = true);

private:
    Status ExecuteMemcpyImpl(const std::vector<BufferView> &deviceBuffers, const std::vector<BufferView> &hostBuffers,
                             bool inlineFfts);
    Status RunFfts();
    void AddTask(size_t index, const std::vector<BufferView> &deviceBuffers);
    Status SubmitToStream(const std::vector<BufferView> &srcBuffers, const std::vector<BufferView> &destBuffers,
                          const std::vector<BufferMetaInfo> &bufferMetas);

    PipelineH2DTasks tasks_;
    size_t blobOffset_;
    std::atomic<size_t> submitCount_;
};

struct NotifyH2HCallbackData {
    // The runtime retains this preallocated record until it invokes the callback. Keep only a weak state reference
    // so an abandoned stream can release its state without creating a state <-> callback-data ownership cycle.
    std::weak_ptr<D2HCallbackState> state;
    D2HCallbackDataStorage *storage = nullptr;
    size_t index = 0;
};

struct D2HCallbackDataStorage {
    explicit D2HCallbackDataStorage(size_t expectedCount) : callbackDatas(expectedCount)
    {
        for (size_t index = 0; index < expectedCount; ++index) {
            callbackDatas[index].storage = this;
            callbackDatas[index].index = index;
        }
    }

    void Acquire()
    {
        refs.fetch_add(1, std::memory_order_relaxed);
    }

    void Release()
    {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

    NotifyH2HCallbackData *At(size_t index)
    {
        return &callbackDatas[index];
    }

private:
    std::atomic<size_t> refs{ 1 };
    std::vector<NotifyH2HCallbackData> callbackDatas;
};

class FftsPipelineD2HCopier : public FftsPipelineCopierBase {
public:
    FftsPipelineD2HCopier(int32_t deviceId, AclResourceManager *aclResourceMgr,
                          const std::vector<BufferMetaInfo> &bufferMetas, ThreadPool *h2hCopyPool,
                          ThreadPool *fftsCopyPool);
    ~FftsPipelineD2HCopier() = default;
    FftsPipelineD2HCopier(const FftsPipelineH2DCopier &) = delete;
    FftsPipelineD2HCopier &operator=(const FftsPipelineH2DCopier &) = delete;

    Status ExecuteMemcpy(const std::vector<BufferView> &hostBuffers, const std::vector<BufferView> &deviceBuffers);

private:
    Status SubmitToStream(const std::vector<BufferView> &srcBuffers, const std::vector<BufferView> &transferBuffers,
                          const std::vector<BufferView> &destBuffers, const std::vector<BufferMetaInfo> &bufferMetas);
    static void NotifyH2HCallback(void *userData);
    void ForceFinish();
};
}  // namespace datasystem
#endif
