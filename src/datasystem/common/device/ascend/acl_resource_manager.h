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

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/ascend/callback_thread.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/ffts_dispatcher.h"
#include "datasystem/common/device/device_batch_copy_helper.h"
#include "datasystem/common/device/device_resource_manager.h"
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

class AclResourceManager;
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
    Status AclMemcpyBatch(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind copyKind);
    std::unique_ptr<ThreadPool> copyPool_;
    std::unique_ptr<ThreadPool> h2hCopyPool_;
    std::unique_ptr<ThreadPool> fftsCopyPool_;
    std::vector<void *> copyStreams_;
    int32_t deviceNow_ = -1;
    DeviceManagerBase *devInterImpl_;
    AclResourceManager *resourceMgr_;
};

class AclResourceManager : public DeviceResourceManager {
public:
    AclResourceManager()
    {
        deviceResources_.reserve(MAX_DEVICE_COUNT);
        for (size_t deviceId = 0; deviceId < MAX_DEVICE_COUNT; deviceId++) {
            deviceResources_.emplace_back(std::make_unique<DeviceResource>(deviceId));
        }
        swapOutPool_ = std::make_unique<AclMemCopyPool>(this);
        swapInPool_ = std::make_unique<AclMemCopyPool>(this);
    };
    ~AclResourceManager() = default;

    Status MemcpyBatchD2H(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList) override;
    Status MemcpyBatchH2D(const std::vector<DeviceBlobList> &devBlobList, std::vector<Buffer *> &bufferList) override;

    Status CreateAclRtStream(uint32_t deviceId, aclrtStream &stream, bool subscribeReport);
    Status FreeAclRtStream(uint32_t deviceId, aclrtStream stream, bool subscribeReport);

    Status CreateRtNotify(uint32_t deviceIdx, rtNotify_t &notify);
    Status FreeRtNotify(uint32_t deviceId, rtNotify_t notify);

    void SetPolicyByHugeTlb(bool enableHugeTlb) override
    {
        if (enableHugeTlb && policyD2H == MemcopyPolicy::FFTS) {
            policyD2H = MemcopyPolicy::HUGE_FFTS;
            hostMemSize = 0;
        }
        if (enableHugeTlb && policyH2D == MemcopyPolicy::FFTS) {
            policyH2D = MemcopyPolicy::HUGE_FFTS;
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

    private:
        Status InitCallbackThread();
        Status InitFftsDispatcher();
        const size_t CACHE_SIZE = 8;
        uint32_t deviceId_;
        std::shared_timed_mutex mutex_;
        std::unique_ptr<ffts::FftsDispatcher> fftsDispatcher_;
        std::unique_ptr<acl::CallbackThread> callbackThread_;
        std::deque<aclrtStream> streamQueue_;
        std::deque<aclrtStream> subscribeReportStreamQueue_;
        std::deque<rtNotify_t> notifyQueue_;
    };

private:
    std::vector<std::unique_ptr<DeviceResource>> deviceResources_;
    std::unique_ptr<AclMemCopyPool> swapOutPool_;
    std::unique_ptr<AclMemCopyPool> swapInPool_;
};

struct AclResource {
    void *primaryStream;
    void *secondaryStream;
    void *toDestDone[FFTS_PIPELINE];
    void *toPinDone[FFTS_PIPELINE];
    bool subscribeReport;
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
    ThreadPool *h2hCopyPool_;
    ThreadPool *fftsCopyPool_;

    std::vector<BufferView> transferHostBuffers_;
    std::vector<BufferView> transferDeviceBuffers_;
    std::vector<ShmUnit> transferHostPool_;
    std::vector<ShmUnit> transferDevicePool_;

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

    Status AddFftsNotifyTask(size_t index, const std::vector<BufferView> &deviceBuffers, bool addTask = true);

private:
    void AddTask(size_t index, const std::vector<BufferView> &deviceBuffers);
    Status SubmitToStream(const std::vector<BufferView> &srcBuffers, const std::vector<BufferView> &destBuffers,
                          const std::vector<BufferMetaInfo> &bufferMetas);

    PipelineH2DTasks tasks_;
    size_t blobOffset_;
    std::atomic<size_t> submitCount_;
};

class FftsPipelineD2HCopier;
struct NotifyH2HCallbackData {
    FftsPipelineD2HCopier *copier;
    size_t index;
};

struct PipelineH2HTasks {
    std::vector<size_t> indexes;
    bool IsEmpty()
    {
        return indexes.empty();
    }
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
                          const std::vector<BufferView> &destBuffers, const std::vector<BufferMetaInfo> &bufferMetas,
                          std::vector<NotifyH2HCallbackData> &callbackDatas);
    static void NotifyH2HCallback(void *userData);
    void ForceFinish();

    PipelineH2HTasks tasks_;
};
}  // namespace datasystem
#endif
