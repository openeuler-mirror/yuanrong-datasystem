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
 * Description: define cuda ipc driver.
 */

#ifndef OS_XPRT_PIPLN_CUDA_IPC
#define OS_XPRT_PIPLN_CUDA_IPC

#include <mutex>
#include "datasystem/common/os_transport_pipeline/chunk_manager.h"

#ifndef PIPLN_USE_MOCK
#include <cuda.h>
#include <cuda_runtime.h>
#include <mutex>

namespace OsXprtPipln {

class CudaRH2DDriver : public BaseRH2DDriver {
public:
    CudaRH2DDriver(const DevShmInfo &devInfo, bool isClient) : BaseRH2DDriver(devInfo, isClient)
    {
    }
    virtual ~CudaRH2DDriver()
    {
        Release();
    }

    Status SubmitIO(void *srcData, size_t srcSize, size_t destOffset) override;
    Status Init() override;
    Status WaitIO() override;
    Status Release() override;

    static Status RegisterHostMemory(void *ptr, size_t size);
    static void UnRegisterHostMemory(void *ptr);

private:
    bool UseExternalStream() const;
    cudaEvent_t GetSelfEvent(bool createIfNotExists);
    void RemoveSelfEvent();
    void RemoveSelfStream();

    cudaStream_t stream_ = nullptr;
    cudaEvent_t event_ = nullptr;
};
}  // namespace OsXprtPipln

#else

namespace OsXprtPipln {
class CudaRH2DDriver : public BaseRH2DDriver {
public:
    CudaRH2DDriver(const DevShmInfo &devInfo, bool isClient) : BaseRH2DDriver(devInfo, isClient)
    {
    }
    virtual ~CudaRH2DDriver()
    {
        Release();
    }
    Status SubmitIO(void *srcData, size_t srcSize, size_t destOffset) override
    {
        (void)srcData;
        (void)srcSize;
        (void)destOffset;
        return Status::OK();
    }
    Status Init() override
    {
        return Status::OK();
    }
    Status WaitIO() override
    {
        return Status::OK();
    }
    Status Release() override
    {
        return Status::OK();
    }
    static Status RegisterHostMemory(void *ptr, size_t size)
    {
        (void)ptr;
        (void)size;
        return Status::OK();
    }
    static void UnRegisterHostMemory(void *ptr)
    {
        (void)ptr;
    }
};
}  // namespace OsXprtPipln

#endif

#endif
