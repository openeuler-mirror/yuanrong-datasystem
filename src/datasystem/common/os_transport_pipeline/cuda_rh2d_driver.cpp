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
#include "datasystem/common/os_transport_pipeline/cuda_rh2d_driver.h"

#ifndef PIPLN_USE_MOCK
#include <cstdint>
#include <chrono>
#include <cstring>
#include <string>
#include <cstdio>

#include "datasystem/common/os_transport_pipeline/dlopen_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace OsXprtPipln {

static inline std::string CudaErrToString(cudaError_t code, const std::string &symbol)
{
    if (IS_VALID_DYNFUNC(CudaRTLibLoader, cudaGetErrorString)) {
        return CudaRTLibLoader::Instance()->cudaGetErrorStringFunc_(code);
    } else {
        return std::string("symbol ") + symbol + " is not loaded";
    }
}

static Status CallCudaRTHook(const std::function<cudaError_t(void)> &hook, const std::string &funcName)
{
    cudaError_t ret = hook();
    if (ret != cudaSuccess)
        return Status(StatusCode::K_IO_ERROR, funcName + " failed: " + CudaErrToString(ret, funcName));
    return Status::OK();
}
#define CUDA_CALL_WRAPPER(funcName, ...)                   \
    CallCudaRTHook(                                        \
        [&]() {                                            \
            cudaError_t ret;                               \
            CALL_CUDA_RT_FUNC(ret, funcName, __VA_ARGS__); \
            return ret;                                    \
        },                                                 \
        #funcName)

#define CUDA_RETURN_IF_NOT_OK(funcName, ...)                                               \
    do {                                                                                   \
        cudaError_t ret;                                                                   \
        CALL_CUDA_RT_FUNC(ret, funcName, __VA_ARGS__);                                     \
        CHECK_FAIL_RETURN_STATUS(ret == cudaSuccess, StatusCode::K_IO_ERROR,               \
                                 #funcName " failed: " + CudaErrToString(ret, #funcName)); \
    } while (0)

bool CudaRH2DDriver::UseExternalStream() const
{
    return h2dStream != nullptr;
}

Status CudaRH2DDriver::Init()
{
    if (!isClient) {
        return Status::OK();
    }

    if (stream_ == nullptr) {
        CUDA_RETURN_IF_NOT_OK(cudaStreamCreateWithFlags, &stream_, cudaStreamNonBlocking);
    }

    RETURN_RUNTIME_ERROR_IF_NULL(GetSelfEvent(true /* createIfNotExists */));

    // Temporary verification log. In the old implementation, different drivers printed the same
    // stream address. With this implementation, different CudaRH2DDriver instances should usually
    // print different stream addresses.
    VLOG(1) << PIPLN_LOG_PREFIX "stream verify(init): driver=" << this << " stream=" << stream_ << " event=" << event_;
    return Status::OK();
}

cudaEvent_t CudaRH2DDriver::GetSelfEvent(bool createIfNotExists)
{
    if (event_ != nullptr || !createIfNotExists) {
        return event_;
    }

    cudaEvent_t e = nullptr;
    Status rc = CUDA_CALL_WRAPPER(cudaEventCreate, &e, cudaEventDisableTiming);
    if (rc.IsOk()) {
        event_ = e;
        return event_;
    }
    return nullptr;
}

void CudaRH2DDriver::RemoveSelfEvent()
{
    if (event_ != nullptr) {
        CUDA_CALL_WRAPPER(cudaEventDestroy, event_);
        event_ = nullptr;
    }
}

void CudaRH2DDriver::RemoveSelfStream()
{
    if (stream_ != nullptr) {
        CUDA_CALL_WRAPPER(cudaStreamDestroy, stream_);
        stream_ = nullptr;
    }
}

Status CudaRH2DDriver::RegisterHostMemory(void *ptr, size_t size)
{
    if (!IS_VALID_DYNFUNC(CudaRTLibLoader, cudaHostRegister)) {
        DO_LOAD_DYNLIB(CudaRTLibLoader);
    }
    cudaError_t cudaRet;
    CALL_CUDA_RT_FUNC(cudaRet, cudaHostRegister, ptr, size, cudaHostRegisterPortable);
    if (cudaRet != cudaSuccess && cudaRet != cudaErrorHostMemoryAlreadyRegistered) {
        return Status(StatusCode::K_RUNTIME_ERROR,
                      "cudaHostRegister failed: " + CudaErrToString(cudaRet, "cudaHostRegister"));
    }
    return Status::OK();
}

void CudaRH2DDriver::UnRegisterHostMemory(void *ptr)
{
    if (ptr) {
        CUDA_CALL_WRAPPER(cudaHostUnregister, ptr);
    }
}

Status CudaRH2DDriver::SubmitIO(void *srcData, size_t srcSize, size_t destOffset)
{
    if (!isClient) {
        return Status(StatusCode::K_NOT_SUPPORTED, "worker should not call submitIO");
    }
    if (stream_ == nullptr) {
        return Status(StatusCode::K_RUNTIME_ERROR, "no stream for Submit");
    }

    size_t destAddr = reinterpret_cast<size_t>(targetAddr) + destOffset;
    CUDA_RETURN_IF_NOT_OK(cudaMemcpyAsync, (void *)destAddr, srcData, srcSize, cudaMemcpyHostToDevice, stream_);

    if (!UseExternalStream()) {
        cudaEvent_t event = GetSelfEvent(false /* createIfNotExists */);
        if (event == nullptr) {
            return Status(StatusCode::K_RUNTIME_ERROR, "no event for internal stream Submit");
        }
        CUDA_RETURN_IF_NOT_OK(cudaEventRecord, event, stream_);
        VLOG(2) << PIPLN_LOG_PREFIX "RH2D submit internal stream: driver=" << this << " stream=" << stream_
                << " event=" << event << " srcSize=" << srcSize << " destOffset=" << destOffset;
    } else {
        VLOG(2) << PIPLN_LOG_PREFIX "RH2D submit external stream: driver=" << this << " stream=" << stream_
                << " srcSize=" << srcSize << " destOffset=" << destOffset;
    }
    return Status::OK();
}

Status CudaRH2DDriver::WaitIO()
{
    if (!isClient || UseExternalStream()) {
        return Status::OK();
    }
    cudaEvent_t event = GetSelfEvent(false /* createIfNotExists */);
    if (event != nullptr) {
        CUDA_RETURN_IF_NOT_OK(cudaEventSynchronize, event);
        VLOG(1) << PIPLN_LOG_PREFIX "cudaEventSynchronize end, driver=" << this << " stream=" << stream_
                << " event=" << event;
    }
    return Status::OK();
}

Status CudaRH2DDriver::Release()
{
    if (isClient && !UseExternalStream()) {
        RemoveSelfEvent();
        RemoveSelfStream();
    }

    return Status::OK();
}

}  // namespace OsXprtPipln

#endif
