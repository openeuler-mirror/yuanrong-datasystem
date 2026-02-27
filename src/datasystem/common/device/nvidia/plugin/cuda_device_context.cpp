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
 * Description: The CUDA device context plugin.
 */

#include "datasystem/common/device/nvidia/plugin/cuda_device_context.h"

#include <cuda_runtime_api.h>
#include <chrono>
#include <thread>

int DSCudaInit()
{
    return cudaFree(0);
}

int DSCudaFinalize()
{
    return 0;
}

int MallocDeviceMemory(size_t dataSize, void *&deviceData)
{
    return cudaMalloc(&deviceData, dataSize);
}

int FreeDeviceMemory(void *deviceData)
{
    if (deviceData != nullptr) {
        return cudaFree(deviceData);
    }
    return cudaSuccess;
}

int DSCudartMalloc(void **devPtr, size_t size)
{
    return cudaMalloc(devPtr, size);
}

int DSCudartFree(void *devPtr)
{
    return cudaFree(devPtr);
}

int DSCudartMallocHost(void **devPtr, size_t size)
{
    return cudaMallocHost(devPtr, size);
}

int DSCudartFreeHost(void *devPtr)
{
    return cudaFreeHost(devPtr);
}

int MemCopyD2D(void *dst, const void *src, size_t srcSize)
{
    return cudaMemcpy(dst, src, srcSize, cudaMemcpyDeviceToDevice);
}

int MemCopyD2H(void *dst, const void *src, size_t srcSize)
{
    return cudaMemcpy(dst, src, srcSize, cudaMemcpyDeviceToHost);
}

int MemCopyH2D(void *dst, const void *src, size_t srcSize)
{
    return cudaMemcpy(dst, src, srcSize, cudaMemcpyHostToDevice);
}

int DSCudartMemcpyAsync(void *dst, const void *src, size_t count, cudaMemcpyKind kind,
                        cudaStream_t stream)
{
    (void)kind;
    return cudaMemcpyAsync(dst, src, count, cudaMemcpyDeviceToDevice, stream);
}

int GetDeviceIdx(int32_t &deviceIdx)
{
    return cudaGetDevice(&deviceIdx);
}

int SetDeviceIdx(int32_t deviceIdx)
{
    return cudaSetDevice(deviceIdx);
}

int DSCudartGetDeviceCount(int *count)
{
    return cudaGetDeviceCount(count);
}

int DSCudartQueryDeviceStatus(uint32_t deviceId, int32_t *deviceStatus)
{
    int originDevice = 0;
    cudaGetDevice(&originDevice);
    int ret = 0;
    if (cudaSetDevice(deviceId) != cudaSuccess) {
        *deviceStatus = 1;
        ret = 1;
    } else if (cudaFree(0) == cudaSuccess) {
        *deviceStatus = 0;
        ret = 0;
    } else {
        *deviceStatus = 1;
        ret = 1;
    }
    if (cudaSetDevice(originDevice) != cudaSuccess) {
        return 1;
    }
    return ret;
}

int DSCudartSetDevice(int deviceId)
{
    return cudaSetDevice(deviceId);
}

int DSCudartResetDevice(int deviceId)
{   
    int originDevice;
    cudaGetDevice(&originDevice);
    cudaSetDevice(deviceId);
    int ret = cudaDeviceReset();
    cudaSetDevice(originDevice);
    return ret;
}

int RtCreateStream(cudaStream_t *stream)
{
    return cudaStreamCreate(stream);
}

int RtSynchronizeStream(cudaStream_t stream)
{
    return cudaStreamSynchronize(stream);
}

int RtSynchronizeStreamWithTimeout(cudaStream_t stream, int32_t timeoutMs)
{
    if (timeoutMs < 0) {
        return cudaStreamSynchronize(stream);
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        cudaError_t status = cudaStreamQuery(stream);
        if (status == cudaSuccess) {
            return cudaSuccess;
        }
        if (status != cudaErrorNotReady) {
            return status;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    return cudaErrorTimeout;
}

int RtDestroyStream(cudaStream_t stream)
{
    return cudaStreamDestroy(stream);
}

int RtDestroyStreamForce(cudaStream_t stream)
{
    return cudaStreamDestroy(stream);
}

int DSNcclGetRootInfo(ncclUniqueId *rootInfo)
{
    return ncclGetUniqueId(rootInfo);
}

int DSNcclCommInitRootInfo(int nRanks, const ncclUniqueId *rootInfo, int rank, ncclComm_t *comm)
{
    return ncclCommInitRank(comm, nRanks, *rootInfo, rank);
}

int DSNcclSend(void *sendBuf, size_t count, ncclDataType_t dataType, int destRank, ncclComm_t comm,
               cudaStream_t stream)
{
    return ncclSend(sendBuf, count, dataType, destRank, comm, stream);
}

int DSNcclRecv(void *recvBuf, size_t count, ncclDataType_t dataType, int srcRank, ncclComm_t comm,
               cudaStream_t stream)
{
    return ncclRecv(recvBuf, count, dataType, srcRank, comm, stream);
}

int DSNcclGetCommAsyncError(ncclComm_t comm, ncclResult_t *asyncError)
{
    return ncclCommGetAsyncError(comm, asyncError);
}

int DSNcclCommDestroy(ncclComm_t comm)
{
    return ncclCommDestroy(comm);
}

int DSCudartCreateEvent(cudaEvent_t *event)
{
    return cudaEventCreate(event);
}

int DSCudartRecordEvent(cudaEvent_t event, cudaStream_t stream)
{
    return cudaEventRecord(event, stream);
}

int DSCudartSynchronizeEvent(cudaEvent_t event)
{
    return cudaEventSynchronize(event);
}

int DSCudartSynchronizeEventWithTimeout(cudaEvent_t event, int32_t timeoutMs)
{
    if (timeoutMs < 0) {
        return cudaEventSynchronize(event);
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        cudaError_t status = cudaEventQuery(event);
        if (status == cudaSuccess) {
            return cudaSuccess;
        }
        if (status != cudaErrorNotReady) {
            return status;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    return cudaErrorTimeout;
}

int DSCudartDestroyEvent(cudaEvent_t event)
{
    return cudaEventDestroy(event);
}

int DSCudartQueryEventStatus(cudaEvent_t event, cudaEventRecordedStatus *status)
{
    int query = cudaEventQuery(event);
    if (query == cudaSuccess) {
        *status = CUDA_EVENT_RECORDED_STATUS_COMPLETE;
    } else {
        *status = CUDA_EVENT_RECORDED_STATUS_NOT_READY;
    }

    return cudaSuccess;
}

