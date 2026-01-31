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
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena_group_key.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const size_t FFTS_PIPELINE = 2;

struct DataMetaInfo {
    size_t blobCount;
    size_t firstBlobOffset;
    void *ptr;
    size_t size;
};

struct BufferView {
    void *ptr;
    size_t size;
};

struct BufferMetaInfo {
    size_t blobCount;
    size_t firstBlobOffset;
    size_t size;
};

enum class MemcopyPolicy : int {
    DIRECT,
    FFTS,
    HUGE_FFTS,
};

struct MemcopyConfig {
    void Init();
    std::string ToString();
    Status GetNumberFromEnv(const char *key, uint64_t &value);
    Status GetPolicyFromEnv(const char *key, MemcopyPolicy &policy);

    const uint64_t defaultHostMemSize = 2684354560;   // 2.5G
    const uint64_t defaultDeviceMemSize = 104857600;  // 100MB
    const uint64_t defaultBlockSize = 10485760;       // 10MB
    MemcopyPolicy policyD2H = MemcopyPolicy::FFTS;
    MemcopyPolicy policyH2D = MemcopyPolicy::FFTS;
    uint64_t deviceMemSize = defaultDeviceMemSize;
    uint64_t hostMemSize = defaultHostMemSize;
};

using datasystem::memory::Allocator;
using datasystem::memory::DevMemFuncRegister;
using AllocateType = datasystem::memory::CacheType;

class AclMemMgrBase {
public:
    AclMemMgrBase(Allocator *allocator) : allocator_(allocator)
    {
    }

    virtual ~AclMemMgrBase() = default;

    Status Init();

    Status Allocate(const std::vector<BufferMetaInfo> &bMeta, std::vector<ShmUnit> &memoryPool, bool skipRetry = false);

    Status Free(std::vector<ShmUnit> &memoryPool);

    static size_t GetPiplineNums()
    {
        return pipeLineNums;
    }

protected:
    std::mutex memPoolLock_;
    static const size_t pipeLineNums = 2;
    void *ptr_ = nullptr;
    Allocator *allocator_ = nullptr;
    const std::string DEFAULT_TENANTID = "";
    AllocateType type_ = AllocateType::DEV_HOST;
    std::unique_ptr<WaitPost> waitPost_{ nullptr };  // wait for some second to check memory is free
};

class AclHostMemMgr : public AclMemMgrBase {
public:
    AclHostMemMgr(Allocator *allocator);
    ~AclHostMemMgr() = default;

    Status HostMemoryCopy(void *dstData, uint64_t dstLength, void *srcData, uint64_t srcLength);

protected:
    std::shared_ptr<ThreadPool> memoryCopyThreadPool_;
};

class AclDeviceMemMgr : public AclMemMgrBase {
public:
    AclDeviceMemMgr(Allocator *allocator) : AclMemMgrBase(allocator)
    {
        type_ = AllocateType::DEV_DEVICE;
    }
    ~AclDeviceMemMgr() = default;
};

class AclResourceManager {
public:
    AclResourceManager();
    ~AclResourceManager() = default;
    Status Init();
    std::shared_ptr<AclHostMemMgr> &Host()
    {
        std::shared_lock<std::shared_timed_mutex> rlocker(mutex_);
        return aclHostMemMgr_;
    }
    std::shared_ptr<AclDeviceMemMgr> &Device()
    {
        std::shared_lock<std::shared_timed_mutex> rlocker(mutex_);
        return aclDeviceMemMgr_;
    }

    Status CreateAclRtStream(uint32_t deviceId, aclrtStream &stream, bool subscribeReport);
    Status FreeAclRtStream(uint32_t deviceId, aclrtStream stream, bool subscribeReport);

    Status CreateRtNotify(uint32_t deviceIdx, rtNotify_t &notify);
    Status FreeRtNotify(uint32_t deviceId, rtNotify_t notify);

    void SetPolicyDirect()
    {
        config.policyD2H = MemcopyPolicy::DIRECT;
        config.policyH2D = MemcopyPolicy::DIRECT;
    }

    void SetD2HPolicyByHugeTlb(bool enableHugeTlb)
    {
        if (enableHugeTlb && config.policyD2H == MemcopyPolicy::FFTS) {
            config.policyD2H = MemcopyPolicy::HUGE_FFTS;
            config.hostMemSize = 0;
        }
        if (enableHugeTlb && config.policyH2D == MemcopyPolicy::FFTS) {
            config.policyH2D = MemcopyPolicy::HUGE_FFTS;
            config.hostMemSize = 0;
        }
    }

    MemcopyPolicy GetD2HPolicy()
    {
        return config.policyD2H;
    }

    MemcopyPolicy GetH2DPolicy()
    {
        return config.policyH2D;
    }

private:
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

    MemcopyConfig config;
    const size_t CACHE_SIZE = 8;
    std::shared_timed_mutex mutex_;
    std::shared_ptr<AclHostMemMgr> aclHostMemMgr_;
    std::shared_ptr<AclDeviceMemMgr> aclDeviceMemMgr_;

    const size_t MAX_DEVICE_COUNT = 64;
    std::vector<std::unique_ptr<DeviceResource>> deviceResources_;
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
    bool skipH2HMemcpy_ =  false;
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
