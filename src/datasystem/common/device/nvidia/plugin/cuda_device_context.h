/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_DEVICE_CUDA_DEVICE_CONTEXT_H
#define DATASYSTEM_COMMON_DEVICE_CUDA_DEVICE_CONTEXT_H

#include <cstddef>
#include <cstdint>

#include <cuda_runtime.h>
#include <nccl.h>
#include "datasystem/common/device/nvidia/cuda_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    CUDA_EVENT_RECORDED_STATUS_NOT_READY = 0,
    CUDA_EVENT_RECORDED_STATUS_COMPLETE = 1
} cudaEventRecordedStatus;

// lifecycle management
int DSCudaInit();
int DSCudaFinalize();

// memory management
int MallocDeviceMemory(size_t dataSize, void *&deviceData);
int FreeDeviceMemory(void *deviceData);

int DSCudartMalloc(void **devPtr, size_t size);
int DSCudartFree(void *devPtr);
int DSCudartMallocHost(void **devPtr, size_t size);
int DSCudartFreeHost(void *devPtr);

// memory copy
int MemCopyD2D(void *dst, const void *src, size_t srcSize);
int MemCopyD2H(void *dst, const void *src, size_t srcSize);
int MemCopyH2D(void *dst, const void *src, size_t srcSize);

int DSCudartMemcpyAsync(void *dst, const void *src, size_t count, cudaMemcpyKind kind,
                        cudaStream_t stream);

// device management
int GetDeviceIdx(int32_t &deviceIdx);
int SetDeviceIdx(int32_t deviceIdx);

int DSCudartGetDeviceCount(int *count);
int DSCudartQueryDeviceStatus(uint32_t deviceId, int32_t *deviceStatus);
int DSCudartSetDevice(int deviceId);
int DSCudartResetDevice(int deviceId);

// stream management
int RtCreateStream(cudaStream_t *stream);
int RtSynchronizeStream(cudaStream_t stream);
int RtSynchronizeStreamWithTimeout(cudaStream_t stream, int32_t timeoutMs);
int RtDestroyStream(cudaStream_t stream);
int RtDestroyStreamForce(cudaStream_t stream);

// nccl management
int DSNcclGetRootInfo(ncclUniqueId *rootInfo);
int DSNcclCommInitRootInfo(int nRanks, const ncclUniqueId *rootInfo, int rank, ncclComm_t *comm);
int DSNcclSend(void *sendBuf, size_t count, ncclDataType_t dataType, int destRank, ncclComm_t comm,
               cudaStream_t stream);
int DSNcclRecv(void *recvBuf, size_t count, ncclDataType_t dataType, int srcRank, ncclComm_t comm,
               cudaStream_t stream);
int DSNcclGetCommAsyncError(ncclComm_t comm, ncclResult_t *asyncError);
int DSNcclCommDestroy(ncclComm_t comm);

// event management
int DSCudartCreateEvent(cudaEvent_t *event);
int DSCudartRecordEvent(cudaEvent_t event, cudaStream_t stream);
int DSCudartSynchronizeEvent(cudaEvent_t event);
int DSCudartSynchronizeEventWithTimeout(cudaEvent_t event, int32_t timeoutMs);
int DSCudartDestroyEvent(cudaEvent_t event);
int DSCudartQueryEventStatus(cudaEvent_t event, cudaEventRecordedStatus *status);

#ifdef __cplusplus
};
#endif

#endif
