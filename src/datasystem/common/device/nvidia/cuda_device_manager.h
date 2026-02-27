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

#include <dlfcn.h>
#include <functional>
#include <memory>
#include <mutex>

#include "datasystem/common/log/log.h"
#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/common/device/nvidia/cuda_types.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/util/dlutils.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/utils/status.h"

#define RETURN_CUDA_RESULT(cudaRet, interType)                                                                         \
    do {                                                                                                               \
        int _cudaRet = (cudaRet);                                                                                      \
        if (_cudaRet != 0) {                                                                                           \
            return Status(StatusCode::K_CUDA_ERROR, __LINE__, __FILE__, FormatString("%s api failed with error code %d"\
            ", please refer to %s documentation for detailed error information. ", interType, cudaRet, interType));    \
        }                                                                                                              \
        return Status::OK();                                                                                           \
    } while (false)

#define RETURN_CUDART_RESULT(cudaRet)          \
    do {                                       \
        RETURN_CUDA_RESULT(cudaRet, "CUDA");   \
    } while (false)

#define RETURN_NCCL_RESULT(ncclRet)            \
    do {                                       \
        RETURN_CUDA_RESULT(ncclRet, "NCCL");   \
    } while (false)

typedef enum {
    CUDA_EVENT_RECORDED_STATUS_NOT_READY = 0,
    CUDA_EVENT_RECORDED_STATUS_COMPLETE = 1
} cudaEventRecordedStatus;

namespace datasystem {

namespace cuda {

/**
 * @brief CUDA Device Manager implementation
 * This class provides GPU (CUDA) support for the datasystem.
 */
class CudaDeviceManager : public DeviceManagerBase {
public:
    CudaDeviceManager();
    virtual ~CudaDeviceManager();
    CudaDeviceManager(CudaDeviceManager &&) = delete;                  // Move construct
    CudaDeviceManager(const CudaDeviceManager &) = delete;             // Copy construct
    CudaDeviceManager &operator=(const CudaDeviceManager &) = delete;  // Copy assign
    CudaDeviceManager &operator=(CudaDeviceManager &&) = delete;       // Move assign

    /**
     * @brief Get singleton instance of CudaDeviceManager
     * @return Pointer to the singleton instance
     */
    static CudaDeviceManager *Instance();

    // ==================== Lifecycle Management ====================
    /**
     * @brief Shutdown manager.
     */
    virtual void Shutdown();

    /**
     * @brief Check the plugin is loaded ok.
     * @return OK if plugin is ready.
     */
    virtual Status CheckPluginOk();

    /**
     * @brief Verify the integrity of a file by using the hash value.
     * @param[in] cudaPluginPath Path of the cudaPlugin so file.
     * @return OK if the file passes the integrity check.
     */
    virtual Status VerifyingSha256(const std::string &cudaPluginPath);

    // ==================== Memory Management ====================
    /**
     * @brief Malloc device memory
     * @param[in] dataSize The data size.
     * @param[out] deviceData The device memory pointer
     * @return K_OK if malloc success.
     */
    virtual Status MallocDeviceMemory(size_t dataSize, void *&deviceData);

    /**
     * @brief Free device memory
     * @param[in] deviceData The device memory pointer
     * @return K_OK if free success.
     */
    virtual Status FreeDeviceMemory(void *deviceData);

    // ==================== Device Management ====================
    /**
     * @brief Get the device index this thread belong to.
     * @param[out] deviceIdx The device index.
     * @return K_OK if get success.
     */
    virtual Status GetDeviceIdx(int32_t &deviceIdx);

    /**
     * @brief Verify if devices ID is valid
     * @param[in] deviceId Device ID to verify
     * @return Status of the operation
     */
    virtual Status VerifyDeviceId(std::vector<uint32_t> deviceId);

    // ==================== Memory Copy ====================
    /**
     * @brief Copy device memory from device to host.
     * @param[out] hostDst The destination host memory pointer.
     * @param[in] dstMaxSize The max size of destination pointer.
     * @param[in] devSrc The source device memory pointer.
     * @param[in] srcSize The size of source pointer.
     * @return K_OK if copy success.
     */
    virtual Status MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize);

    /**
     * @brief Copy device memory from host to device.
     * @param[out] devDst The destination device memory pointer.
     * @param[in] dstMaxSize The max size of destination pointer.
     * @param[in] hostSrc The source host memory pointer.
     * @param[in] srcSize The size of source pointer.
     * @return K_OK if copy success.
     */
    virtual Status MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize);

    /**
     * @brief Copy device memory from device to device.
     * @param[out] dst The destination device memory pointer.
     * @param[in] dstMaxSize The max size of destination pointer.
     * @param[in] src The source device memory pointer.
     * @param[in] srcSize The size of source pointer.
     * @return K_OK if copy success.
     */
    virtual Status MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize);

    /**
     * @brief Set the Device Idx object
     * @param[in] deviceId The device index.
     * @return K_OK if successful.
     */
    virtual Status SetDeviceIdx(int32_t deviceId);

    // ==================== Stream Management ====================
    /**
     * @brief Create the CUDA runtime stream.
     * @param[out] stream The CUDA runtime stream.
     * @return K_OK if successful.
     */
    virtual Status RtCreateStream(cudaStream_t *stream);

    /**
     * @brief Synchronize the stream between cpu and gpu.
     * @param[in] stream The CUDA runtime stream.
     * @return K_OK if successful.
     */
    virtual Status RtSynchronizeStream(cudaStream_t stream);

    /**
     * @brief Synchronize the stream between cpu and gpu within the timeout period.
     * @param[in] stream The CUDA runtime stream.
     * @param[in] timeoutMs Timeout interval of an interface.
     * @return K_OK if successful.
     */
    virtual Status RtSynchronizeStreamWithTimeout(cudaStream_t stream, int32_t timeoutMs);

    /**
     * @brief Destroy the CUDA runtime stream.
     * @param[in] stream The CUDA runtime stream.
     * @return K_OK if successful.
     */
    virtual Status RtDestroyStream(cudaStream_t stream);

    /**
     * @brief Force destroy the CUDA runtime stream.
     * @param[in] stream The CUDA runtime stream.
     * @return K_OK if successful.
     */
    virtual Status RtDestroyStreamForce(cudaStream_t stream);

    // ==================== NCCL Communication ====================
    /**
     * @brief NCCL get the unique id (root info).
     * @param[out] rootInfo The NCCL unique id.
     * @return K_OK if successful.
     */
    virtual Status DSNcclGetRootInfo(ncclUniqueId *rootInfo);

    /**
     * @brief Init the NCCL communicator by root info.
     * @param[in] nRanks The number of ranks.
     * @param[in] rootInfo The root info (NCCL unique id).
     * @param[in] rank The rank in local.
     * @param[out] comm The NCCL communicator.
     * @return K_OK if successful.
     */
    virtual Status DSNcclCommInitRootInfo(int nRanks, const ncclUniqueId *rootInfo, int rank, ncclComm_t *comm);

    /**
     * @brief NCCL send the data to the receiving side.
     * @param[in] sendBuf The pointer of data ready to send.
     * @param[in] count The count of item in the buffer.
     * @param[in] dataType The data type of item in the buffer.
     * @param[in] destRank The rank of destination.
     * @param[in] comm The NCCL communicator.
     * @param[in] stream The CUDA runtime stream.
     * @return Status of the call.
     */
    virtual Status DSNcclSend(void *sendBuf, size_t count, ncclDataType_t dataType, int destRank, ncclComm_t comm,
                              cudaStream_t stream);

    /**
     * @brief NCCL recv the data from the sending side.
     * @param[in] recvBuf The pointer of data ready to receive.
     * @param[in] count The count of item in the buffer.
     * @param[in] dataType The data type of item in the buffer.
     * @param[in] srcRank The rank of source.
     * @param[in] comm The NCCL communicator.
     * @param[in] stream The CUDA runtime stream.
     * @return K_OK if successful.
     */
    virtual Status DSNcclRecv(void *recvBuf, size_t count, ncclDataType_t dataType, int srcRank, ncclComm_t comm,
                              cudaStream_t stream);

    /**
     * @brief Destroy the NCCL communicator.
     * @param[in] comm The NCCL communicator.
     * @return K_OK if successful.
     */
    virtual Status DSNcclCommDestroy(ncclComm_t comm);

    // ==================== Event Management ====================
    /**
     * @brief Create an event, which is used in the stream synchronization waiting scenario.
     * @param[out] event Pointer to an event.
     * @return K_OK if successful.
     */
    virtual Status DSCudartCreateEvent(cudaEvent_t *event);

    /**
     * @brief Record an event in the stream.
     * @param[in] event Event to be recorded.
     * @param[in] stream Records a specified event in a specified stream.
     * @return K_OK if successful.
     */
    virtual Status DSCudartRecordEvent(cudaEvent_t event, cudaStream_t stream);

    /**
     * @brief Blocks application running and waits for the event to complete.
     * @param[in] event Event to be waited.
     * @return K_OK if successful.
     */
    virtual Status DSCudartSynchronizeEvent(cudaEvent_t event);

    /**
     * @brief Synchronize event with timeout.
     * @param[in] event Event to be waited.
     * @param[in] timeoutMs Timeout interval of an interface.
     * @return K_OK if successful.
     */
    virtual Status DSCudartSynchronizeEventWithTimeout(cudaEvent_t event, int32_t timeoutMs);

    /**
     * @brief Destroy an event.
     * @param[in] event Event to be destroyed.
     * @return K_OK if successful.
     */
    virtual Status DSCudartDestroyEvent(cudaEvent_t event);

    /**
     * @brief Queries whether the events recorded by cudaEventRecord have completed.
     * @param[in] event Event to query.
     * @return K_OK if successful.
     */
    virtual Status DSCudartQueryEventStatus(cudaEvent_t event);

    /**
     * @brief Queries whether an error occurs in the communication domain.
     * @param[in] comm The NCCL communicator.
     * @return K_OK if successful.
     */
    virtual Status DSNcclGetCommAsyncError(ncclComm_t comm);

    // ==================== CUDA Lifecycle ====================
    virtual Status cudaInit();
    virtual Status cudartSetDevice(int deviceId);
    virtual Status cudaFinalize();
    virtual Status cudartGetDeviceCount(int *count);
    virtual Status cudartQueryDeviceStatus(uint32_t deviceId);
    virtual Status cudartMemcpyAsync(void *dst, const void *src, size_t count,
                                    cudaMemcpyKind kind, cudaStream_t stream);
    virtual Status cudartResetDevice(int deviceId);
    virtual Status cudartMalloc(void **devPtr, size_t size);
    virtual Status cudartFree(void *devPtr);
    virtual Status cudartMallocHost(void **devPtr, size_t size);
    virtual Status cudartFreeHost(void *devPtr);

    // ==================== DeviceManagerBase Interface Implementation ====================
    Status Init(const char *configPath) override;
    Status Finalize() override;

    Status GetDeviceCount(uint32_t *count) override;
    Status QueryDeviceStatus(uint32_t deviceId) override;
    Status SetDevice(int32_t deviceId) override;
    Status ResetDevice(int32_t deviceId) override;

    Status Malloc(void **devPtr, size_t size, MemMallocPolicy policy) override;
    Status Free(void *devPtr) override;
    Status MallocHost(void **hostPtr, size_t size) override;
    Status FreeHost(void *hostPtr) override;

    Status MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                       MemcpyKind kind, void *stream) override;
    Status MemcpyBatch(void **dsts, size_t *destMax, void **srcs, size_t *sizes, size_t numBatches, MemcpyKind kind,
                       uint32_t deviceIdx, size_t *failIndex) override;

    Status CreateStream(void **stream) override;
    Status SynchronizeStream(void *stream) override;
    Status SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs) override;
    Status DestroyStream(void *stream) override;
    Status DestroyStreamForce(void *stream) override;

    Status CreateEvent(void **event) override;
    Status RecordEvent(void *event, void *stream) override;
    Status SynchronizeEvent(void *event) override;
    Status SynchronizeEventWithTimeout(void *event, int32_t timeoutMs) override;
    Status DestroyEvent(void *event) override;
    Status QueryEventStatus(void *event) override;

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
    /**
     * @brief Initialize the CUDA device manager.
     */
    void Init();

    /**
     * @brief Load the CUDA plugin.
     * @return OK if plugin is loaded.
     */
    Status LoadPlugin();

    /**
     * @brief Load function symbols from the plugin.
     */
    void DlsymFuncObj();

    /**
     * @brief Handle CUDA result and convert to Status.
     * @param[in] cudaResult The CUDA result code.
     * @return Status of the operation.
     */
    Status HandleCudaResult(int cudaResult);

    /**
     * @brief Handle NCCL result and convert to Status.
     * @param[in] ncclResult The NCCL result code.
     * @return Status of the operation.
     */
    Status HandleNcclResult(int ncclResult);

    // Register plugin function as function pointer in class member.
    REG_METHOD(MallocDeviceMemory, int, size_t, void *&);
    REG_METHOD(FreeDeviceMemory, int, void *);
    REG_METHOD(GetDeviceIdx, int, int32_t &);
    REG_METHOD(SetDeviceIdx, int, int32_t);
    REG_METHOD(MemCopyD2D, int, void *, const void *, size_t);
    REG_METHOD(MemCopyD2H, int, void *, const void *, size_t);
    REG_METHOD(MemCopyH2D, int, void *, const void *, size_t);

    REG_METHOD(RtCreateStream, int, cudaStream_t *);
    REG_METHOD(RtSynchronizeStream, int, cudaStream_t);
    REG_METHOD(RtSynchronizeStreamWithTimeout, int, cudaStream_t, int32_t);
    REG_METHOD(RtDestroyStream, int, cudaStream_t);
    REG_METHOD(RtDestroyStreamForce, int, cudaStream_t);

    REG_METHOD(DSNcclGetRootInfo, int, ncclUniqueId *);
    REG_METHOD(DSNcclCommInitRootInfo, int, int, const ncclUniqueId *, int, ncclComm_t *);
    REG_METHOD(DSNcclSend, int, void *, size_t, ncclDataType_t, int, ncclComm_t, cudaStream_t);
    REG_METHOD(DSNcclRecv, int, void *, size_t, ncclDataType_t, int, ncclComm_t, cudaStream_t);
    REG_METHOD(DSNcclCommDestroy, int, ncclComm_t);
    REG_METHOD(DSNcclGetCommAsyncError, int, ncclComm_t, ncclResult_t *);

    REG_METHOD(DSCudartCreateEvent, int, cudaEvent_t *);
    REG_METHOD(DSCudartRecordEvent, int, cudaEvent_t, cudaStream_t);
    REG_METHOD(DSCudartSynchronizeEvent, int, cudaEvent_t);
    REG_METHOD(DSCudartSynchronizeEventWithTimeout, int, cudaEvent_t, int32_t);
    REG_METHOD(DSCudartDestroyEvent, int, cudaEvent_t);
    REG_METHOD(DSCudartQueryEventStatus, int, cudaEvent_t, cudaEventRecordedStatus *);

    REG_METHOD(DSCudaInit, int);
    REG_METHOD(DSCudaFinalize, int);
    REG_METHOD(DSCudartSetDevice, int, int);
    REG_METHOD(DSCudartGetDeviceCount, int, int *);
    REG_METHOD(DSCudartQueryDeviceStatus, int, uint32_t, int32_t *);
    REG_METHOD(DSCudartMemcpyAsync, int, void *, const void *, size_t, cudaMemcpyKind, cudaStream_t);
    REG_METHOD(DSCudartResetDevice, int, int);
    REG_METHOD(DSCudartMalloc, int, void **, size_t);
    REG_METHOD(DSCudartFree, int, void *);
    REG_METHOD(DSCudartMallocHost, int, void **, size_t);
    REG_METHOD(DSCudartFreeHost, int, void *);

    static std::once_flag init_;
    static std::once_flag hasLoadPlugin_;
    static std::unique_ptr<CudaDeviceManager> instance_;

    void *pluginHandle_{ nullptr };
    std::unique_ptr<WaitPost> waitPost_;
    std::unique_ptr<Thread> loadPluginThread_{ nullptr };
};

}  // namespace cuda
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_NVIDIA_CUDA_DEVICE_MANAGER_H
