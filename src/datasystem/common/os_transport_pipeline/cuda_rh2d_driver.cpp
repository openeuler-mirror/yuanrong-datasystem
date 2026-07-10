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

static constexpr int MAX_POPEN_LINE_LENGTH = 256;

namespace OsXprtPipln {

std::unordered_map<std::string, int32_t> CudaRH2DDriver::uuidToDevIdMap_;

static inline std::string CudaErrToString(cudaError_t code, const std::string &symbol)
{
    if (IS_VALID_DYNFUNC(CudaRTLibLoader, cudaGetErrorString)) {
        return CudaRTLibLoader::Instance()->cudaGetErrorStringFunc_(code);
    } else {
        return std::string("symbol ") + symbol + " is not loaded";
    }
}

Status CudaRH2DDriver::CallCudaRTHook(const std::function<cudaError_t(void)> &hook, const std::string &funcName)
{
    cudaError_t ret = hook();
    switch (ret) {
        case cudaSuccess:
            return Status::OK();
        case cudaErrorLaunchFailure:
        case cudaErrorIllegalAddress:
        case cudaErrorHardwareStackError:
        case cudaErrorUnknown:
            break;
        default:
            return Status(StatusCode::K_IO_ERROR, funcName + " failed: " + CudaErrToString(ret, funcName));
    }
    // meet fatal error, reset context and try again
    RETURN_IF_NOT_OK(ResetContext());
    CHECK_FAIL_RETURN_STATUS(hook() == cudaSuccess, StatusCode::K_IO_ERROR,
                             funcName + " failed: " + CudaErrToString(ret, funcName));
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

#define CUDA_RETURN_IF_NOT_OK(funcName, ...)                        \
    do {                                                            \
        RETURN_IF_NOT_OK(CUDA_CALL_WRAPPER(funcName, __VA_ARGS__)); \
    } while (0)

bool CudaRH2DDriver::UseExternalStream() const
{
    return h2dStream != nullptr;
}

Status CudaRH2DDriver::ResetContext()
{
    if (currentDevId_ == -1) {
        return Status(StatusCode::K_RUNTIME_ERROR, "set cuda device failed, cudaId is null");
    }

    std::unique_lock<std::shared_mutex> l(initMutex_);

    CUDA_RETURN_IF_NOT_OK(cudaSetDevice, currentDevId_);
    inited_ = true;

    if (UseExternalStream()) {
        // The stream is owned by caller. Do not reset CUDA context, create event, or destroy it.
        stream_ = reinterpret_cast<cudaStream_t>(h2dStream);
        event_ = nullptr;
        return Status::OK();
    }

    // Keep the original internal-stream behavior for compatibility when caller does not pass a stream.
    CUDA_RETURN_IF_NOT_OK(cudaDeviceReset);
    stream_ = nullptr;
    event_ = nullptr;

    if (isClient) {
        CUDA_RETURN_IF_NOT_OK(cudaStreamCreateWithFlags, &stream_, cudaStreamNonBlocking);
        CUDA_RETURN_IF_NOT_OK(cudaEventCreate, &event_, cudaEventDisableTiming);
        VLOG(1) << "RH2D internal stream created(reset): driver=" << this << " stream=" << stream_
                << " event=" << event_;
    }

    return Status::OK();
}

Status CudaRH2DDriver::Init()
{
    if (!isClient) {
        return Status::OK();
    }

    if (UseExternalStream()) {
        if (currentDevId_ != -1) {
            CUDA_RETURN_IF_NOT_OK(cudaSetDevice, currentDevId_);
            inited_ = true;
        }
        stream_ = reinterpret_cast<cudaStream_t>(h2dStream);
        CHECK_FAIL_RETURN_STATUS(stream_ != nullptr, StatusCode::K_RUNTIME_ERROR, "external h2d stream is null");
        event_ = nullptr;
        VLOG(1) << "RH2D use external stream: driver=" << this << " stream=" << stream_;
        return Status::OK();
    }

    if (!inited_) {
        RETURN_IF_NOT_OK(ResetContext());
    } else if (currentDevId_ != -1) {
        CUDA_RETURN_IF_NOT_OK(cudaSetDevice, currentDevId_);
    }

    if (stream_ == nullptr) {
        CUDA_RETURN_IF_NOT_OK(cudaStreamCreateWithFlags, &stream_, cudaStreamNonBlocking);
    }

    RETURN_RUNTIME_ERROR_IF_NULL(GetSelfEvent(true /* createIfNotExists */));

    VLOG(1) << "RH2D use internal stream: driver=" << this << " stream=" << stream_ << " event=" << event_;
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
        cudaError_t ret;
        CALL_CUDA_RT_FUNC(ret, cudaHostUnregister, ptr);
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
        VLOG(1) << "RH2D submit internal stream: driver=" << this << " stream=" << stream_ << " event=" << event
                << " srcSize=" << srcSize << " destOffset=" << destOffset;
    } else {
        VLOG(1) << "RH2D submit external stream: driver=" << this << " stream=" << stream_
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
        VLOG(1) << "RH2D internal cudaEventSynchronize end, driver=" << this << " stream=" << stream_
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

Status CudaRH2DDriver::LoadGpuIds()
{
    char buf[MAX_POPEN_LINE_LENGTH];
    FILE *f = popen("nvidia-smi -L", "r");
    if (!f) {
        return Status(StatusCode::K_RUNTIME_ERROR, "LoadGpuIds failed: failed to popen(\"nvidia-smi -L\")");
    }
    uuidToDevIdMap_.clear();
    while (fgets(buf, MAX_POPEN_LINE_LENGTH, f)) {
        std::string line = buf;
        // gpu id
        const std::string gpu_prefix = "GPU ";
        size_t pos_gpu = line.find(gpu_prefix);
        if (pos_gpu == std::string::npos) {
            continue;
        }
        size_t start = pos_gpu + gpu_prefix.size();
        size_t end = start;
        while (line[end] >= '0' && line[end] <= '9' && end < line.size())
            end++;
        if (start == end)
            continue;
        int devId = std::stoi(line.substr(start, end - start));

        // gpu uuid
        const std::string uuid_prefix = "UUID: GPU-";
        size_t pos_uuid = line.find(uuid_prefix);
        if (pos_uuid == std::string::npos)
            continue;
        start = pos_uuid + uuid_prefix.size();
        end = line.find(')', start);
        if (start == end)
            continue;
        std::string gpuUUID = line.substr(start, end - start);

        // save it
        uuidToDevIdMap_[gpuUUID] = devId;
    }
    int status = pclose(f);
    if (!WIFEXITED(status) || (WEXITSTATUS(status) != 0)) {
        return Status(StatusCode::K_RUNTIME_ERROR, "LoadGpuIds failed: popen return " + std::to_string(status));
    }
    return Status::OK();
}

void CudaRH2DDriver::SwitchToAndGetGpuId(const std::string &uuid)
{
    if (uuid.empty())
        return;
    int32_t ret = -1;
    try {
        // is num
        ret = std::stoi(uuid);
    } catch (...) {
        // is uuid
        auto it = uuidToDevIdMap_.find(uuid);
        if (it != uuidToDevIdMap_.end()) {
            ret = it->second;
        } else {
            // is GPU-uuid
            auto it2 = uuidToDevIdMap_.find(uuid.substr(4));
            if (it2 != uuidToDevIdMap_.end())
                ret = it2->second;
        }
    }
    currentDevId_ = ret;
    if (ret == -1)
        return;

    cudaError_t cudaRet;
    CALL_CUDA_RT_FUNC(cudaRet, cudaSetDevice, currentDevId_);
    if (cudaRet != cudaSuccess) {
        LOG(WARNING) << "set cuda device failed for " << std::to_string(currentDevId_) << ":"
                     << CudaErrToString(cudaRet, "cudaSetDevice");
    } else {
        inited_ = true;
    }
    // Do not create a global static stream here. Each CudaRH2DDriver creates its own stream in Init().
}

std::unordered_map<std::string, int32_t> &CudaRH2DDriver::GetDevIdMap()
{
    return uuidToDevIdMap_;
}
}  // namespace OsXprtPipln

#endif
