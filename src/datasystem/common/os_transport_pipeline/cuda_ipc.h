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

#include <cuda.h>
#include <cuda_runtime.h>

#include "datasystem/common/os_transport_pipeline/chunk_manager.h"

namespace OsXprtPipln {

class CudaIPC : public IpcDriver {
public:
    CudaIPC(const DevShmInfo &devInfo, const std::string &targetHandle, bool isClient)
        : IpcDriver(devInfo, targetHandle, isClient)
    {
    }
    virtual ~CudaIPC()
    {
    }

    Status SubmitIO(void *srcData, size_t srcSize, size_t destOffset) override;
    Status EncodeDriver() override;
    Status DecodeDriver() override;
    Status WaitIO() override;
    Status Release() override;

    void ExportHandles(cudaStream_t &stream, cudaEvent_t &event);

private:
    cudaEvent_t event_ = nullptr;
    static inline std::unordered_map<int, cudaStream_t> streams_;
};
}  // namespace OsXprtPipln

#endif