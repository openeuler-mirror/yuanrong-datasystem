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
 * Description: cuda driver for IPC and local h2d.
 */

#include "datasystem/common/os_transport_pipeline/cuda_ipc.h"
#include "datasystem/common/os_transport_pipeline/dlopen_util.h"

#include <cstring>
#include <string>

#define CUDA_RETURN_IF_NOT_OK(funcName, failHook, ...)                                            \
    do {                                                                                          \
        cudaError_t ret;                                                                          \
        CALL_CUDA_RT_FUNC(ret, funcName, __VA_ARGS__)                                             \
        if (ret != cudaSuccess) {                                                                 \
            failHook;                                                                             \
            return Status(StatusCode::K_IO_ERROR,                                                 \
                          std::string(__func__) + " failed: " + CudaErrToString(ret, #funcName)); \
        }                                                                                         \
    } while (0)

namespace OsXprtPipln {

static inline std::string CudaErrToString(cudaError_t code, const std::string &symbol)
{
    if (IS_VALID_DYNFUNC(CudaRTLibLoader, cudaGetErrorString)) {
        return CudaRTLibLoader::Instance()->cudaGetErrorStringFunc_(code);
    } else {
        return std::string("symbol ") + symbol + " is not loaded";
    }
}
#define CudeIpcCloseMemHandle(handle)                                        \
    do {                                                                     \
        if (IS_VALID_DYNFUNC(CudaRTLibLoader, cudaGetErrorString))           \
            CudaRTLibLoader::Instance()->cudaIpcCloseMemHandleFunc_(handle); \
    } while (0)

Status CudaIPC::EncodeDriver()
{
    if (!isClient)
        return Status(StatusCode::K_NOT_SUPPORTED, "EncodeDriver should be called in client");

    if (!targetAddr)
        return Status(StatusCode::K_INVALID, "targetAddr is nullptr");

    cudaIpcMemHandle_t *handle = (cudaIpcMemHandle_t *)GetEncodeHandle(sizeof(type));
    CUDA_RETURN_IF_NOT_OK(cudaSetDevice, {}, devId);
    CUDA_RETURN_IF_NOT_OK(cudaIpcGetMemHandle, {}, handle, targetAddr);
    return Status::OK();
}

Status CudaIPC::DecodeDriver()
{
    if (isClient)
        return Status(StatusCode::K_NOT_SUPPORTED, "DecodeDriver should be called in server");

    // decode handle
    cudaIpcMemHandle_t *handle = (cudaIpcMemHandle_t *)GetDecodeHandle(sizeof(type));
    if (!handle)
        return Status(StatusCode::K_RUNTIME_ERROR, "handle is not set");

    CUDA_RETURN_IF_NOT_OK(cudaSetDevice, {}, devId);
    CUDA_RETURN_IF_NOT_OK(cudaIpcOpenMemHandle, {}, &targetAddr, *handle, cudaIpcMemLazyEnablePeerAccess);

    // create stream
    if (streams_.find(devId) == streams_.end()) {
        cudaStream_t *stream = &streams_[devId];
        CUDA_RETURN_IF_NOT_OK(
            cudaStreamCreateWithFlags,
            {
                streams_.erase(devId);
                void *tmp = targetAddr;
                targetAddr = nullptr;
                CudeIpcCloseMemHandle(tmp);
            },
            stream, cudaStreamNonBlocking);
    }

    // create event
    CUDA_RETURN_IF_NOT_OK(
        cudaEventCreate,
        {
            void *tmp = targetAddr;
            targetAddr = nullptr;
            CudeIpcCloseMemHandle(tmp);
        },
        &event_, (cudaEventDisableTiming | cudaEventInterprocess));

    return Status::OK();
}

Status CudaIPC::SubmitIO(void *srcData, size_t srcSize, size_t destOffset)
{
    if (isClient)
        return Status(StatusCode::K_NOT_SUPPORTED, "client should not call submitIO");

    if (event_) {
        size_t destAddr = reinterpret_cast<size_t>(targetAddr) + destOffset;
        CUDA_RETURN_IF_NOT_OK(cudaSetDevice, {}, devId);
        CUDA_RETURN_IF_NOT_OK(cudaMemcpyAsync, {}, (void *)destAddr, srcData, srcSize, cudaMemcpyHostToDevice,
                              streams_[devId]);
        CUDA_RETURN_IF_NOT_OK(cudaEventRecord, {}, event_, streams_[devId]);
    } else {
        return Status(StatusCode::K_RUNTIME_ERROR, "no stream for Submit");
    }
    return Status::OK();
}

Status CudaIPC::WaitIO()
{
    if (!isClient && event_) {
        CUDA_RETURN_IF_NOT_OK(cudaSetDevice, {}, devId);
        CUDA_RETURN_IF_NOT_OK(cudaStreamWaitEvent, {}, streams_[devId], event_, 0);
        VLOG(1) << "RH2D:cudaStreamWaitEvent end";
    }
    return Status::OK();
}

Status CudaIPC::Release()
{
    if (isClient && targetAddr) {
    } else {
        if (event_) {
            cudaEvent_t tmp = event_;
            event_ = nullptr;
            CUDA_RETURN_IF_NOT_OK(cudaSetDevice, {}, devId);
            CUDA_RETURN_IF_NOT_OK(cudaEventDestroy, {}, tmp);
        }
        if (targetAddr) {
            void *tmp = targetAddr;
            targetAddr = nullptr;
            CUDA_RETURN_IF_NOT_OK(cudaSetDevice, {}, devId);
            CUDA_RETURN_IF_NOT_OK(cudaIpcCloseMemHandle, {}, tmp);
        }
    }
    return Status::OK();
}

void CudaIPC::ExportHandles(cudaStream_t &stream, cudaEvent_t &event)
{
    if (streams_.find(devId) == streams_.end()) {
        stream = nullptr;
        event = nullptr;
    } else {
        stream = streams_[devId];
        event = event_;
    }
}
}  // namespace OsXprtPipln
