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
 * Description: Abstract base class for device manager, supporting both NPU (Ascend) and GPU (CUDA).
 */
#ifndef DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_BASE_H
#define DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_BASE_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>

#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Memory copy direction enumeration (device-agnostic)
 */
enum class MemcpyKind {
    HOST_TO_HOST = 0,
    HOST_TO_DEVICE = 1,
    DEVICE_TO_HOST = 2,
    DEVICE_TO_DEVICE = 3,
};

/**
 * @brief Memory allocation policy enumeration (device-agnostic)
 */
enum class MemMallocPolicy {
    HUGE_FIRST = 0,       // Allocate huge page memory first
    HUGE_ONLY = 1,        // Only allocate huge page memory
    NORMAL_ONLY = 2,      // Only allocate normal page memory
};

/**
 * @brief Callback block type enumeration (device-agnostic)
 */
enum class CallbackBlockType {
    BLOCK = 0,            // Blocking callback
    NO_BLOCK = 1,         // Non-blocking callback
};

/**
 * @brief Communication data type enumeration (device-agnostic)
 */
enum class CommDataType {
    INT8 = 0,
    INT16 = 1,
    INT32 = 2,
    INT64 = 3,
    UINT8 = 4,
    UINT16 = 5,
    UINT32 = 6,
    UINT64 = 7,
    FLOAT16 = 8,
    FLOAT32 = 9,
    FLOAT64 = 10,
    BFLOAT16 = 11,
};

/**
 * @brief Root info structure for collective communication initialization
 * @note Size is 4108 bytes to accommodate both HCCL (4108) and NCCL (128) requirements
 */
struct CommRootInfo {
    char data[4108];
};

// ==================== P2P Communication Types (Device-Agnostic) ====================

/**
 * @brief P2P communication kind enumeration
 */
enum class P2pKindBase {
    RECEIVER = 0,         // P2P receiver
    SENDER = 1,           // P2P sender
    BIDIRECTIONAL = 2,    // Bidirectional (currently unsupported)
};

/**
 * @brief P2P link type enumeration
 */
enum class P2pLinkBase {
    HCCS = 0,             // HCCS link (same node)
    ROCE = 1,             // RoCE link (cross node)
    AUTO = 2,             // Auto detect
};

/**
 * @brief P2P segment permissions enumeration
 */
enum class P2pSegmentPermBase {
    READ_WRITE = 0,       // Read and write
    READ_ONLY = 1,        // Read only
    WRITE_ONLY = 2,       // Write only
};

const uint32_t P2P_SEGMENT_INFO_SIZE = 48;

/**
 * @brief P2P segment info structure (device-agnostic)
 */
struct P2pSegmentBase {
    char data[P2P_SEGMENT_INFO_SIZE];
};

/**
 * @brief P2P scatter entry structure (device-agnostic)
 */
struct P2pScatterBase {
    void *srcBuf;           // Source buffer (DDR)
    void **dstBufs;         // Destination buffers array
    uint64_t *counts;       // Counts per destination
    CommDataType dataType;  // Data type
    uint32_t numEntries;    // Number of entries
};

/**
 * @brief Callback function type for stream callbacks
 */
using StreamCallback = void (*)(void *userData);

/**
 * @brief Abstract base class for device management
 * This class provides a unified interface for both NPU (Ascend/ACL) and GPU (CUDA) devices.
 */
class DeviceManagerBase {
public:
    DeviceManagerBase() = default;
    virtual ~DeviceManagerBase() = default;

    // Disable copy and move
    DeviceManagerBase(const DeviceManagerBase &) = delete;
    DeviceManagerBase &operator=(const DeviceManagerBase &) = delete;
    DeviceManagerBase(DeviceManagerBase &&) = delete;
    DeviceManagerBase &operator=(DeviceManagerBase &&) = delete;

    // ==================== Lifecycle Management ====================

    /**
     * @brief Shutdown the device manager and release all resources
     */
    virtual void Shutdown() = 0;

    /**
     * @brief Check if the device plugin/driver is loaded and ready
     * @return Status::OK() if ready, error status otherwise
     */
    virtual Status CheckPluginOk() = 0;

    /**
     * @brief Initialize the device runtime
     * @param[in] configPath Optional configuration file path (can be nullptr)
     * @return Status of the operation
     */
    virtual Status Init(const char *configPath) = 0;

    /**
     * @brief Finalize the device runtime
     * @return Status of the operation
     */
    virtual Status Finalize() = 0;

    // ==================== Device Management ====================

    /**
     * @brief Get the number of available devices
     * @param[out] count Number of devices
     * @return Status of the operation
     */
    virtual Status GetDeviceCount(uint32_t *count) = 0;

    /**
     * @brief Query the status of a specific device
     * @param[in] deviceId Device ID to query
     * @return Status of the operation
     */
    virtual Status QueryDeviceStatus(uint32_t deviceId) = 0;

    /**
     * @brief Verify if devices ID is valid
     * @param[in] deviceId Device ID to verify
     * @return Status of the operation
     */
    virtual Status VerifyDeviceId(std::vector<uint32_t> deviceId) = 0;

    /**
     * @brief Set the current device for this thread
     * @param[in] deviceId Device ID to set
     * @return Status of the operation
     */
    virtual Status SetDevice(int32_t deviceId) = 0;

    /**
     * @brief Get the current device index for this thread
     * @param[out] deviceIdx Current device index
     * @return Status of the operation
     */
    virtual Status GetDeviceIdx(int32_t &deviceIdx) = 0;

    /**
     * @brief Reset a device (release all resources on device)
     * @param[in] deviceId Device ID to reset
     * @return Status of the operation
     */
    virtual Status ResetDevice(int32_t deviceId) = 0;

    // ==================== Memory Management ====================

    /**
     * @brief Allocate device memory
     * @param[out] devPtr Pointer to allocated memory
     * @param[in] size Size in bytes to allocate
     * @param[in] policy Allocation policy
     * @return Status of the operation
     */
    virtual Status Malloc(void **devPtr, size_t size, MemMallocPolicy policy) = 0;

    /**
     * @brief Free device memory
     * @param[in] devPtr Pointer to memory to free
     * @return Status of the operation
     */
    virtual Status Free(void *devPtr) = 0;

    /**
     * @brief Allocate pinned host memory
     * @param[out] hostPtr Pointer to allocated memory
     * @param[in] size Size in bytes to allocate
     * @return Status of the operation
     */
    virtual Status MallocHost(void **hostPtr, size_t size) = 0;

    /**
     * @brief Free pinned host memory
     * @param[in] hostPtr Pointer to memory to free
     * @return Status of the operation
     */
    virtual Status FreeHost(void *hostPtr) = 0;

    /**
     * @brief Allocate device memory (legacy interface)
     * @param[in] dataSize Size in bytes
     * @param[out] deviceData Pointer to allocated memory
     * @return Status of the operation
     */
    virtual Status MallocDeviceMemory(size_t dataSize, void *&deviceData) = 0;

    /**
     * @brief Free device memory (legacy interface)
     * @param[in] deviceData Pointer to memory to free
     * @return Status of the operation
     */
    virtual Status FreeDeviceMemory(void *deviceData) = 0;

    // ==================== Memory Copy ====================

    /**
     * @brief Synchronous memory copy from device to host
     * @param[out] hostDst Destination host pointer
     * @param[in] dstMaxSize Maximum size of destination buffer
     * @param[in] devSrc Source device pointer
     * @param[in] srcSize Size to copy
     * @return Status of the operation
     */
    virtual Status MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize) = 0;

    /**
     * @brief Synchronous memory copy from host to device
     * @param[out] devDst Destination device pointer
     * @param[in] dstMaxSize Maximum size of destination buffer
     * @param[in] hostSrc Source host pointer
     * @param[in] srcSize Size to copy
     * @return Status of the operation
     */
    virtual Status MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize) = 0;

    /**
     * @brief Synchronous memory copy from device to device
     * @param[out] dst Destination device pointer
     * @param[in] dstMaxSize Maximum size of destination buffer
     * @param[in] src Source device pointer
     * @param[in] srcSize Size to copy
     * @return Status of the operation
     */
    virtual Status MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize) = 0;

    /**
     * @brief Asynchronous memory copy
     * @param[out] dst Destination pointer
     * @param[in] dstMaxSize Maximum size of destination buffer
     * @param[in] src Source pointer
     * @param[in] count Size to copy
     * @param[in] kind Direction of copy
     * @param[in] stream Stream to use for async operation (void*)
     * @return Status of the operation
     */
    virtual Status MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                               MemcpyKind kind, void *stream) = 0;

    // ==================== Stream Management ====================

    /**
     * @brief Create a stream
     * @param[out] stream Pointer to created stream (void**)
     * @return Status of the operation
     */
    virtual Status CreateStream(void **stream) = 0;

    /**
     * @brief Synchronize a stream (wait for all operations to complete)
     * @param[in] stream Stream to synchronize (void*)
     * @return Status of the operation
     */
    virtual Status SynchronizeStream(void *stream) = 0;

    /**
     * @brief Synchronize a stream with timeout
     * @param[in] stream Stream to synchronize (void*)
     * @param[in] timeoutMs Timeout in milliseconds
     * @return Status of the operation
     */
    virtual Status SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs) = 0;

    /**
     * @brief Destroy a stream
     * @param[in] stream Stream to destroy (void*)
     * @return Status of the operation
     */
    virtual Status DestroyStream(void *stream) = 0;

    /**
     * @brief Force destroy a stream
     * @param[in] stream Stream to destroy (void*)
     * @return Status of the operation
     */
    virtual Status DestroyStreamForce(void *stream) = 0;

    // ==================== Event Management ====================

    /**
     * @brief Create an event
     * @param[out] event Pointer to created event (void**)
     * @return Status of the operation
     */
    virtual Status CreateEvent(void **event) = 0;

    /**
     * @brief Record an event in a stream
     * @param[in] event Event to record (void*)
     * @param[in] stream Stream to record in (void*)
     * @return Status of the operation
     */
    virtual Status RecordEvent(void *event, void *stream) = 0;

    /**
     * @brief Synchronize on an event (wait for event to complete)
     * @param[in] event Event to wait for (void*)
     * @return Status of the operation
     */
    virtual Status SynchronizeEvent(void *event) = 0;

    /**
     * @brief Synchronize on an event with timeout
     * @param[in] event Event to wait for (void*)
     * @param[in] timeoutMs Timeout in milliseconds
     * @return Status of the operation
     */
    virtual Status SynchronizeEventWithTimeout(void *event, int32_t timeoutMs) = 0;

    /**
     * @brief Destroy an event
     * @param[in] event Event to destroy (void*)
     * @return Status of the operation
     */
    virtual Status DestroyEvent(void *event) = 0;

    /**
     * @brief Query if an event has completed
     * @param[in] event Event to query (void*)
     * @return Status of the operation (OK if completed)
     */
    virtual Status QueryEventStatus(void *event) = 0;

    // ==================== Collective Communication ====================

    /**
     * @brief Get root info for collective communication initialization
     * @param[out] rootInfo Root info structure
     * @return Status of the operation
     */
    virtual Status CommGetRootInfo(CommRootInfo *rootInfo) = 0;

    /**
     * @brief Initialize a communicator using root info
     * @param[in] nRanks Total number of ranks
     * @param[in] rootInfo Root info from rank 0
     * @param[in] rank This rank's ID
     * @param[out] comm Pointer to created communicator (void**)
     * @return Status of the operation
     */
    virtual Status CommInitRootInfo(uint32_t nRanks, const CommRootInfo *rootInfo, uint32_t rank, void **comm) = 0;

    /**
     * @brief Send data to a specific rank
     * @param[in] sendBuf Data to send
     * @param[in] count Number of elements
     * @param[in] dataType Data type
     * @param[in] destRank Destination rank
     * @param[in] comm Communicator (void*)
     * @param[in] stream Stream for async operation (void*)
     * @return Status of the operation
     */
    virtual Status CommSend(void *sendBuf, uint64_t count, CommDataType dataType, uint32_t destRank,
                            void *comm, void *stream) = 0;

    /**
     * @brief Receive data from a specific rank
     * @param[out] recvBuf Buffer to receive into
     * @param[in] count Number of elements
     * @param[in] dataType Data type
     * @param[in] srcRank Source rank
     * @param[in] comm Communicator (void*)
     * @param[in] stream Stream for async operation (void*)
     * @return Status of the operation
     */
    virtual Status CommRecv(void *recvBuf, uint64_t count, CommDataType dataType, uint32_t srcRank,
                            void *comm, void *stream) = 0;

    /**
     * @brief Destroy a communicator
     * @param[in] comm Communicator to destroy (void*)
     * @return Status of the operation
     */
    virtual Status CommDestroy(void *comm) = 0;

    /**
     * @brief Query async error status of a communicator
     * @param[in] comm Communicator to query (void*)
     * @return Status of the operation
     */
    virtual Status CommGetAsyncError(void *comm) = 0;

    // ==================== P2P Communication ====================

    /**
     * @brief Get root info for P2P communication
     * @param[out] rootInfo Root info structure
     * @return Status of the operation
     */
    virtual Status P2PGetRootInfo(CommRootInfo *rootInfo) = 0;

    /**
     * @brief Initialize P2P communicator
     * @param[in] rootInfo Root info structure
     * @param[in] kind Sender or receiver (P2pKindBase)
     * @param[in] link Link type (P2pLinkBase)
     * @param[out] comm Pointer to created communicator (void**)
     * @return Status of the operation
     */
    virtual Status P2PCommInitRootInfo(const CommRootInfo *rootInfo, P2pKindBase kind, P2pLinkBase link,
                                       void **comm) = 0;

    /**
     * @brief Destroy P2P communicator
     * @param[in] comm Communicator to destroy (void*)
     * @return Status of the operation
     */
    virtual Status P2PCommDestroy(void *comm) = 0;

    /**
     * @brief P2P send data
     * @param[in] sendBuf Data to send
     * @param[in] count Number of elements
     * @param[in] dataType Data type
     * @param[in] comm P2P communicator (void*)
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status P2PSend(void *sendBuf, uint64_t count, CommDataType dataType, void *comm, void *stream) = 0;

    /**
     * @brief P2P receive data
     * @param[out] recvBuf Buffer to receive into
     * @param[in] count Number of elements
     * @param[in] dataType Data type
     * @param[in] comm P2P communicator (void*)
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status P2PRecv(void *recvBuf, uint64_t count, CommDataType dataType, void *comm, void *stream) = 0;

    /**
     * @brief Query P2P communicator async error
     * @param[in] comm Communicator (void*)
     * @return Status of the operation
     */
    virtual Status P2PGetCommAsyncError(void *comm) = 0;

    // ==================== NPU-Specific Interfaces ====================
    // These interfaces are only used when EnableMerge=true (NPU FFTS/Pipeline path)
    // CUDA implements these to return K_NOT_SUPPORTED

    /**
     * @brief Create a notify object (NPU FFTS synchronization)
     * @param[in] deviceId Device ID
     * @param[out] notify Pointer to created notify (void**)
     * @return Status of the operation
     */
    virtual Status NotifyCreate(int32_t deviceId, void **notify) = 0;

    /**
     * @brief Destroy a notify object
     * @param[in] notify Notify to destroy (void*)
     * @return Status of the operation
     */
    virtual Status NotifyDestroy(void *notify) = 0;

    /**
     * @brief Record a notify in a stream
     * @param[in] notify Notify to record (void*)
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status NotifyRecord(void *notify, void *stream) = 0;

    /**
     * @brief Wait for a notify in a stream
     * @param[in] notify Notify to wait for (void*)
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status NotifyWait(void *notify, void *stream) = 0;

    /**
     * @brief Launch a callback function on stream completion
     * @param[in] fn Callback function
     * @param[in] userData User data passed to callback
     * @param[in] blockType Blocking type
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status LaunchCallback(StreamCallback fn, void *userData, CallbackBlockType blockType, void *stream) = 0;

    /**
     * @brief Process callback reports (NPU callback thread)
     * @param[in] timeout Timeout in milliseconds
     * @return Status of the operation
     */
    virtual Status ProcessReport(int32_t timeout) = 0;

    /**
     * @brief Subscribe a thread to receive stream reports
     * @param[in] threadId Thread ID
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status SubscribeReport(uint64_t threadId, void *stream) = 0;

    /**
     * @brief Unsubscribe a thread from stream reports
     * @param[in] threadId Thread ID
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status UnSubscribeReport(uint64_t threadId, void *stream) = 0;

    /**
     * @brief General device control (NPU specific)
     * @param[in] ctrl Control data
     * @param[in] num Number of control entries
     * @param[in] type Control type
     * @return Status of the operation
     */
    virtual Status GeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type) = 0;

    /**
     * @brief Get device information (NPU specific)
     * @param[in] deviceId Device ID
     * @param[in] moduleType Module type
     * @param[in] infoType Information type
     * @param[out] val Output value
     * @return Status of the operation
     */
    virtual Status GetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val) = 0;

    // ==================== P2P Host Memory Registration (NPU P2P-Transfer specific) ====================

    /**
     * @brief Register host memory for P2P transfer
     * @param[in] hostBuf Host buffer
     * @param[in] size Size in bytes
     * @param[out] segmentInfo Segment info output (P2pSegmentBase*)
     * @param[in] permissions Access permissions (P2pSegmentPermBase)
     * @return Status of the operation
     */
    virtual Status P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentBase *segmentInfo,
                                      P2pSegmentPermBase permissions) = 0;

    /**
     * @brief Import a host segment for P2P transfer
     * @param[in] segmentInfo Segment info (P2pSegmentBase)
     * @return Status of the operation
     */
    virtual Status P2PImportHostSegment(P2pSegmentBase segmentInfo) = 0;

    /**
     * @brief Batch scatter from remote host memory
     * @param[in] entries Scatter entries (P2pScatterBase*)
     * @param[in] batchSize Number of entries
     * @param[in] comm P2P communicator (void*)
     * @param[in] stream Stream (void*)
     * @return Status of the operation
     */
    virtual Status P2PScatterBatchFromRemoteHostMem(P2pScatterBase *entries, uint32_t batchSize,
                                                    void *comm, void *stream) = 0;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_DEVICE_DEVICE_MANAGER_BASE_H