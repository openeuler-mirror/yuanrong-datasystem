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
 * Description: CUDA device manager for GPU support.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_DEVICE_MANAGER_H
#define DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_DEVICE_MANAGER_H

#include <memory>
#include <mutex>

#include "datasystem/common/device/device_manager_base.h"

namespace datasystem {
namespace cuda {

/**
 * @brief CUDA Device Manager implementation
 * This class provides GPU (CUDA) support for the datasystem.
 * Currently returns K_NOT_SUPPORTED for all operations - implementation pending.
 */
class CudaDeviceManager : public DeviceManagerBase {
public:
    /**
     * @brief Get singleton instance of CudaDeviceManager
     * @return Pointer to the singleton instance
     */
    static CudaDeviceManager *Instance();

    ~CudaDeviceManager() override;

    // Disable copy and move
    CudaDeviceManager(const CudaDeviceManager &) = delete;
    CudaDeviceManager &operator=(const CudaDeviceManager &) = delete;
    CudaDeviceManager(CudaDeviceManager &&) = delete;
    CudaDeviceManager &operator=(CudaDeviceManager &&) = delete;

    // ==================== Lifecycle Management ====================
    void Shutdown() override;
    Status CheckPluginOk() override;
    Status Init(const char *configPath) override;
    Status Finalize() override;

    // ==================== Device Management ====================
    Status GetDeviceCount(uint32_t *count) override;
    Status QueryDeviceStatus(uint32_t deviceId) override;
    Status VerifyDeviceId(std::vector<uint32_t> deviceId) override;
    Status SetDevice(int32_t deviceId) override;
    Status GetDeviceIdx(int32_t &deviceIdx) override;
    Status ResetDevice(int32_t deviceId) override;

    // ==================== Memory Management ====================
    Status Malloc(void **devPtr, size_t size, MemMallocPolicy policy) override;
    Status Free(void *devPtr) override;
    Status MallocHost(void **hostPtr, size_t size) override;
    Status FreeHost(void *hostPtr) override;
    Status MallocDeviceMemory(size_t dataSize, void *&deviceData) override;
    Status FreeDeviceMemory(void *deviceData) override;

    // ==================== Memory Copy ====================
    Status MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize) override;
    Status MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize) override;
    Status MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize) override;
    Status MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                       MemcpyKind kind, void *stream) override;

    // ==================== Stream Management ====================
    Status CreateStream(void **stream) override;
    Status SynchronizeStream(void *stream) override;
    Status SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs) override;
    Status DestroyStream(void *stream) override;
    Status DestroyStreamForce(void *stream) override;

    // ==================== Event Management ====================
    Status CreateEvent(void **event) override;
    Status RecordEvent(void *event, void *stream) override;
    Status SynchronizeEvent(void *event) override;
    Status SynchronizeEventWithTimeout(void *event, int32_t timeoutMs) override;
    Status DestroyEvent(void *event) override;
    Status QueryEventStatus(void *event) override;

    // ==================== Collective Communication (NCCL) ====================
    Status CommGetRootInfo(CommRootInfo *rootInfo) override;
    Status CommInitRootInfo(uint32_t nRanks, const CommRootInfo *rootInfo, uint32_t rank, void **comm) override;
    Status CommSend(void *sendBuf, uint64_t count, CommDataType dataType, uint32_t destRank,
                    void *comm, void *stream) override;
    Status CommRecv(void *recvBuf, uint64_t count, CommDataType dataType, uint32_t srcRank,
                    void *comm, void *stream) override;
    Status CommDestroy(void *comm) override;
    Status CommGetAsyncError(void *comm) override;

    // ==================== P2P Communication ====================
    Status P2PGetRootInfo(CommRootInfo *rootInfo) override;
    Status P2PCommInitRootInfo(const CommRootInfo *rootInfo, P2pKindBase kind, P2pLinkBase link,
                               void **comm) override;
    Status P2PCommDestroy(void *comm) override;
    Status P2PSend(void *sendBuf, uint64_t count, CommDataType dataType, void *comm, void *stream) override;
    Status P2PRecv(void *recvBuf, uint64_t count, CommDataType dataType, void *comm, void *stream) override;
    Status P2PGetCommAsyncError(void *comm) override;

    // ==================== NPU-Specific Interfaces (Not Supported on CUDA) ====================
    Status NotifyCreate(int32_t deviceId, void **notify) override;
    Status NotifyDestroy(void *notify) override;
    Status NotifyRecord(void *notify, void *stream) override;
    Status NotifyWait(void *notify, void *stream) override;
    Status LaunchCallback(StreamCallback fn, void *userData, CallbackBlockType blockType, void *stream) override;
    Status ProcessReport(int32_t timeout) override;
    Status SubscribeReport(uint64_t threadId, void *stream) override;
    Status UnSubscribeReport(uint64_t threadId, void *stream) override;
    Status GeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type) override;
    Status GetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val) override;

    // ==================== P2P Host Memory Registration (Not Supported on CUDA) ====================
    Status P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentBase *segmentInfo,
                              P2pSegmentPermBase permissions) override;
    Status P2PImportHostSegment(P2pSegmentBase segmentInfo) override;
    Status P2PScatterBatchFromRemoteHostMem(P2pScatterBase *entries, uint32_t batchSize,
                                            void *comm, void *stream) override;

private:
    CudaDeviceManager();

    static std::once_flag init_;
    static std::unique_ptr<CudaDeviceManager> instance_;
};

}  // namespace cuda
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_DEVICE_MANAGER_H
