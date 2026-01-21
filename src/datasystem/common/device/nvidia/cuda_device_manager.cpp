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
 * Description: CUDA device manager implementation for GPU support.
 */
#include "datasystem/common/device/nvidia/cuda_device_manager.h"

namespace datasystem {
namespace cuda {

std::once_flag CudaDeviceManager::init_;
std::unique_ptr<CudaDeviceManager> CudaDeviceManager::instance_ = nullptr;

namespace {
constexpr const char *K_NOT_SUPPORTED_MSG = "CUDA device manager is not yet implemented";
}  // namespace

CudaDeviceManager::CudaDeviceManager() = default;

CudaDeviceManager::~CudaDeviceManager()
{
    Shutdown();
}

CudaDeviceManager *CudaDeviceManager::Instance()
{
    std::call_once(init_, []() { instance_ = std::make_unique<CudaDeviceManager>(); });
    return instance_.get();
}

// ==================== Lifecycle Management ====================

void CudaDeviceManager::Shutdown()
{
}

Status CudaDeviceManager::CheckPluginOk()
{
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::Init(const char *configPath)
{
    (void)configPath;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::Finalize()
{
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== Device Management ====================

Status CudaDeviceManager::GetDeviceCount(uint32_t *count)
{
    (void)count;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::QueryDeviceStatus(uint32_t deviceId)
{
    (void)deviceId;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::VerifyDeviceId(std::vector<uint32_t> deviceId)
{
    (void)deviceId;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::SetDevice(int32_t deviceId)
{
    (void)deviceId;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::GetDeviceIdx(int32_t &deviceIdx)
{
    (void)deviceIdx;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::ResetDevice(int32_t deviceId)
{
    (void)deviceId;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== Memory Management ====================

Status CudaDeviceManager::Malloc(void **devPtr, size_t size, MemMallocPolicy policy)
{
    (void)devPtr;
    (void)size;
    (void)policy;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::Free(void *devPtr)
{
    (void)devPtr;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::MallocHost(void **hostPtr, size_t size)
{
    (void)hostPtr;
    (void)size;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::FreeHost(void *hostPtr)
{
    (void)hostPtr;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::MallocDeviceMemory(size_t dataSize, void *&deviceData)
{
    (void)dataSize;
    (void)deviceData;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::FreeDeviceMemory(void *deviceData)
{
    (void)deviceData;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== Memory Copy ====================

Status CudaDeviceManager::MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize)
{
    (void)hostDst;
    (void)dstMaxSize;
    (void)devSrc;
    (void)srcSize;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize)
{
    (void)devDst;
    (void)dstMaxSize;
    (void)hostSrc;
    (void)srcSize;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize)
{
    (void)dst;
    (void)dstMaxSize;
    (void)src;
    (void)srcSize;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                                      MemcpyKind kind, void *stream)
{
    (void)dst;
    (void)dstMaxSize;
    (void)src;
    (void)count;
    (void)kind;
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== Stream Management ====================

Status CudaDeviceManager::CreateStream(void **stream)
{
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::SynchronizeStream(void *stream)
{
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs)
{
    (void)stream;
    (void)timeoutMs;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::DestroyStream(void *stream)
{
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::DestroyStreamForce(void *stream)
{
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== Event Management ====================

Status CudaDeviceManager::CreateEvent(void **event)
{
    (void)event;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::RecordEvent(void *event, void *stream)
{
    (void)event;
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::SynchronizeEvent(void *event)
{
    (void)event;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::SynchronizeEventWithTimeout(void *event, int32_t timeoutMs)
{
    (void)event;
    (void)timeoutMs;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::DestroyEvent(void *event)
{
    (void)event;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::QueryEventStatus(void *event)
{
    (void)event;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== Collective Communication (NCCL) ====================

Status CudaDeviceManager::CommGetRootInfo(CommRootInfo *rootInfo)
{
    (void)rootInfo;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::CommInitRootInfo(uint32_t nRanks, const CommRootInfo *rootInfo, uint32_t rank, void **comm)
{
    (void)nRanks;
    (void)rootInfo;
    (void)rank;
    (void)comm;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::CommSend(void *sendBuf, uint64_t count, CommDataType dataType, uint32_t destRank,
                                   void *comm, void *stream)
{
    (void)sendBuf;
    (void)count;
    (void)dataType;
    (void)destRank;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::CommRecv(void *recvBuf, uint64_t count, CommDataType dataType, uint32_t srcRank,
                                   void *comm, void *stream)
{
    (void)recvBuf;
    (void)count;
    (void)dataType;
    (void)srcRank;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::CommDestroy(void *comm)
{
    (void)comm;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::CommGetAsyncError(void *comm)
{
    (void)comm;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== P2P Communication ====================

Status CudaDeviceManager::P2PGetRootInfo(CommRootInfo *rootInfo)
{
    (void)rootInfo;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::P2PCommInitRootInfo(const CommRootInfo *rootInfo, P2pKindBase kind, P2pLinkBase link,
                                              void **comm)
{
    (void)rootInfo;
    (void)kind;
    (void)link;
    (void)comm;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::P2PCommDestroy(void *comm)
{
    (void)comm;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::P2PSend(void *sendBuf, uint64_t count, CommDataType dataType, void *comm, void *stream)
{
    (void)sendBuf;
    (void)count;
    (void)dataType;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::P2PRecv(void *recvBuf, uint64_t count, CommDataType dataType, void *comm, void *stream)
{
    (void)recvBuf;
    (void)count;
    (void)dataType;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

Status CudaDeviceManager::P2PGetCommAsyncError(void *comm)
{
    (void)comm;
    return Status(K_NOT_SUPPORTED, K_NOT_SUPPORTED_MSG);
}

// ==================== NPU-Specific Interfaces (Not Supported on CUDA) ====================

Status CudaDeviceManager::NotifyCreate(int32_t deviceId, void **notify)
{
    (void)deviceId;
    (void)notify;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::NotifyDestroy(void *notify)
{
    (void)notify;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::NotifyRecord(void *notify, void *stream)
{
    (void)notify;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::NotifyWait(void *notify, void *stream)
{
    (void)notify;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "Notify operations are NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::LaunchCallback(StreamCallback fn, void *userData, CallbackBlockType blockType, void *stream)
{
    (void)fn;
    (void)userData;
    (void)blockType;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "LaunchCallback is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::ProcessReport(int32_t timeout)
{
    (void)timeout;
    return Status(K_NOT_SUPPORTED, "ProcessReport is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::SubscribeReport(uint64_t threadId, void *stream)
{
    (void)threadId;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "SubscribeReport is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::UnSubscribeReport(uint64_t threadId, void *stream)
{
    (void)threadId;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "UnSubscribeReport is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::GeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type)
{
    (void)ctrl;
    (void)num;
    (void)type;
    return Status(K_NOT_SUPPORTED, "GeneralCtrl is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::GetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val)
{
    (void)deviceId;
    (void)moduleType;
    (void)infoType;
    (void)val;
    return Status(K_NOT_SUPPORTED, "GetDeviceInfo is NPU-specific and not supported on CUDA");
}

// ==================== P2P Host Memory Registration (Not Supported on CUDA) ====================

Status CudaDeviceManager::P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentBase *segmentInfo,
                                             P2pSegmentPermBase permissions)
{
    (void)hostBuf;
    (void)size;
    (void)segmentInfo;
    (void)permissions;
    return Status(K_NOT_SUPPORTED, "P2PRegisterHostMem is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::P2PImportHostSegment(P2pSegmentBase segmentInfo)
{
    (void)segmentInfo;
    return Status(K_NOT_SUPPORTED, "P2PImportHostSegment is NPU-specific and not supported on CUDA");
}

Status CudaDeviceManager::P2PScatterBatchFromRemoteHostMem(P2pScatterBase *entries, uint32_t batchSize,
                                                           void *comm, void *stream)
{
    (void)entries;
    (void)batchSize;
    (void)comm;
    (void)stream;
    return Status(K_NOT_SUPPORTED, "P2PScatterBatchFromRemoteHostMem is NPU-specific and not supported on CUDA");
}

}  // namespace cuda
}  // namespace datasystem
