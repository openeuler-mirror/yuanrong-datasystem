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

#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_RESOURCE_MGR_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_RESOURCE_MGR_H

#include <vector>
#include <shared_mutex>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/buffer.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/hetero/device_common.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/device/device_manager_factory.h"
namespace datasystem {

struct BufferMetaInfo {
    size_t blobCount;
    size_t firstBlobOffset;
    size_t size;
};
using datasystem::memory::Allocator;
using datasystem::memory::DevMemFuncRegister;
using AllocateType = datasystem::memory::CacheType;

class MemMgrBase {
public:
    MemMgrBase(Allocator *allocator) : allocator_(allocator)
    {
    }

    virtual ~MemMgrBase() = default;

    Status Init();

    Status Allocate(const std::vector<BufferMetaInfo> &bMeta, std::vector<ShmUnit> &memoryPool, bool skipRetry = false);

    Status Free(std::vector<ShmUnit> &memoryPool);

protected:
    std::mutex memPoolLock_;

    void *ptr_ = nullptr;
    Allocator *allocator_ = nullptr;
    const std::string DEFAULT_TENANTID = "";
    AllocateType type_ = AllocateType::DEV_HOST;
    std::unique_ptr<WaitPost> waitPost_{ nullptr };  // wait for some second to check memory is free
};

class HostMemMgr : public MemMgrBase {
public:
    HostMemMgr(Allocator *allocator);
    ~HostMemMgr() = default;

    Status HostMemoryCopy(void *dstData, uint64_t dstLength, void *srcData, uint64_t srcLength);

protected:
    std::shared_ptr<ThreadPool> memoryCopyThreadPool_;
};

class DeviceMemMgr : public MemMgrBase {
public:
    DeviceMemMgr(Allocator *allocator) : MemMgrBase(allocator)
    {
        type_ = AllocateType::DEV_DEVICE;
    }
    ~DeviceMemMgr() = default;
};

class DeviceResourceManager {
public:
    DeviceResourceManager()
    {
        LOG_IF_ERROR(GetNumberFromEnv("DS_DEVICE_ACL_SIZE", deviceMemSize), "GetNumberFromEnv failed");
        LOG_IF_ERROR(GetNumberFromEnv("DS_HOST_ACL_SIZE", hostMemSize), "GetNumberFromEnv failed");
        LOG_IF_ERROR(GetPolicyFromEnv("DS_D2H_MEMCPY_POLICY", policyD2H), "GetPolicyFromEnv failed");
        LOG_IF_ERROR(GetPolicyFromEnv("DS_H2D_MEMCPY_POLICY", policyH2D), "GetPolicyFromEnv failed");
    }
    virtual ~DeviceResourceManager() = default;

    virtual Status MemcpyBatchD2H(const std::vector<DeviceBlobList> &devBlobList,
                                  std::vector<Buffer *> &bufferList) = 0;
    virtual Status MemcpyBatchH2D(const std::vector<DeviceBlobList> &devBlobList,
                                  std::vector<Buffer *> &bufferList) = 0;
    virtual void SetPolicyByHugeTlb(bool enableHugeTlb) = 0;

    Status EnsureInitialized()
    {
        std::call_once(initFlag_, [this]() { initStatus_ = DoInit(); });
        return initStatus_;
    }
    Status DoInit();

    // Should be called after Init, to make sure the mem manager is ready.
    HostMemMgr *Host()
    {
        return hostMemMgr_.get();
    }
    DeviceMemMgr *Device()
    {
        return deviceMemMgr_.get();
    }

    MemcopyPolicy GetD2HPolicy()
    {
        return policyD2H;
    }

    MemcopyPolicy GetH2DPolicy()
    {
        return policyH2D;
    }

    uint64_t GetDeviceMemSize() const
    {
        return deviceMemSize;
    }

    uint64_t GetHostMemSize() const
    {
        return hostMemSize;
    }

    void SetPolicyDirect()
    {
        policyD2H = MemcopyPolicy::DIRECT;
        policyH2D = MemcopyPolicy::DIRECT;
    }

    std::string PrintMemConfig()
    {
        std::stringstream ss;
        ss << "MemcopyConfig { policyD2H:" << static_cast<int>(policyD2H);
        ss << ", policyH2D:" << static_cast<int>(policyH2D);
        ss << ", deviceMemSize:" << deviceMemSize;
        ss << ", hostMemSize:" << hostMemSize;
        ss << "}";
        return ss.str();
    }

protected:
    MemcopyPolicy policyD2H = MemcopyPolicy::FFTS;
    MemcopyPolicy policyH2D = MemcopyPolicy::FFTS;
    uint64_t deviceMemSize = 104857600;  // 100MB
    uint64_t hostMemSize = 2684354560;   // 2.5G
private:
    std::once_flag initFlag_;
    Status initStatus_;
    std::unique_ptr<HostMemMgr> hostMemMgr_;
    std::unique_ptr<DeviceMemMgr> deviceMemMgr_;
};
}  // namespace datasystem
#endif
