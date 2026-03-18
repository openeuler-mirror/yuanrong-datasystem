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
 * Description: The AscendCL device context plugin.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_ACL_DEVICE_CONTEXT_H
#define DATASYSTEM_COMMON_DEVICE_ACL_DEVICE_CONTEXT_H

#include <cstddef>
#include <cstdint>
#include <functional>

#include <acl/acl.h>
#include <acl/acl_rt.h>
#include <hccl/hccl.h>
#include <p2p.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Malloc device memory
 * @param[in] dataSize The data size.
 * @param[out] deviceData The device memory pointer
 * @return 0 if malloc success.
 */
int MallocDeviceMemory(size_t dataSize, void *&deviceData);

/**
 * @brief Free device memory
 * @param[in] deviceData The device memory pointer
 * @return 0 if free success.
 */
int FreeDeviceMemory(void *deviceData);

/**
 * @brief Get the device index this thread belong to.
 * @param[out] deviceIdx The device index.
 * @return 0 if get success.
 */
int GetDeviceIdx(int32_t &deviceIdx);

/**
 * @brief Copy device memory from device to host.
 * @param[out] hostDst The destination host memory pointer.
 * @param[in] dstMaxSize The max size of destination pointer.
 * @param[in] devSrc The source device memory pointer.
 * @param[in] srcSize The size of source pointer.
 * @return 0 if copy success.
 */
int MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize);

/**
 * @brief Copy device memory from device to host.
 * @param[out] devDst The destination device memory pointer.
 * @param[in] dstMaxSize The max size of destination pointer.
 * @param[in] hostSrc The source host memory pointer.
 * @param[in] srcSize The size of source pointer.
 * @return 0 if copy success.
 */
int MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize);

/**
 * @brief Copy device memory from device to host.
 * @param[out] dst The destination device memory pointer.
 * @param[in] dstMaxSize The max size of destination pointer.
 * @param[in] src The source device memory pointer.
 * @param[in] srcSize The size of source pointer.
 * @return 0 if copy success.
 */
int MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize);

/**
 * @brief Set the Device Idx object
 * @param[in] deviceId The device index.
 * @return 0 if successful.
 */
int SetDeviceIdx(int32_t deviceIdx);

/**
 * @brief Create the acl runtime stream.
 * @param[in] stream The acl runtime stream.
 * @return 0 if successful.
 */
int RtCreateStream(aclrtStream *stream);

/**
 * @brief Synchronize the stream between cpu and npu.
 * @param[in] stream  The acl runtime stream.
 * @return 0 if successful.
 */
int RtSynchronizeStream(aclrtStream stream);

/**
 * @brief Synchronize the stream between cpu and npu within the timeout period.
 * @param[in] stream  The acl runtime stream.
 * @param[in] timeoutMs Timeout interval of an interface.
 * @return 0 if successful.
 */
int RtSynchronizeStreamWithTimeout(aclrtStream stream, int32_t timeoutMs);

/**
 * @brief Destroy the acl runtime stream.
 * @param[in] stream The acl runtime stream.
 * @return 0 if successful.
 */
int RtDestroyStream(aclrtStream stream);

/**
 * @brief Force destroy the acl runtime stream.
 * @param[in] stream The acl runtime stream.
 * @return 0 if successful.
 */
int RtDestroyStreamForce(aclrtStream stream);

/**
 * @brief Hccl get the root info.
 * @param[out] rootInfo
 * @return 0 if successful.
 */
int DSHcclGetRootInfo(HcclRootInfo *rootInfo);

/**
 * @brief Init the hccl communicator by root info.
 * @param[in] nRanks The number of ranks.
 * @param[in] rootInfo The root info.
 * @param[in] rank The rank in local.
 * @param[out] comm The hccl communicator.
 * @return 0 if successful.
 */
int DSHcclCommInitRootInfo(uint32_t nRanks, const HcclRootInfo *rootInfo, uint32_t rank, HcclComm *comm);

/**
 * @brief HCCL send the data to the receiving side.
 * @param[in] sendBuf The pointer of data ready to send.
 * @param[in] count The count of item in the buffer.
 * @param[in] dataType The data type of item in the buffer.
 * @param[in] destRank The rank of destination.
 * @param[in] comm The hccl communicator.
 * @param[in] stream The acl runtime stream.
 * @return Status of the call.
 */
int DSHcclSend(void *sendBuf, uint64_t count, HcclDataType dataType, uint32_t destRank, HcclComm comm,
               aclrtStream stream);

/**
 * @brief HCCL recv the data from the sending side.
 * @param[in] recvBuf The pointer of data ready to receive.
 * @param[in] count The count of item in the buffer.
 * @param[in] dataType The data type of item in the buffer.
 * @param[in] srcRank The rank of source.
 * @param[in] comm The hccl communicator.
 * @param[in] stream The acl runtime stream.
 * @return 0 if successful.
 */
int DSHcclRecv(void *recvBuf, uint64_t count, HcclDataType dataType, uint32_t srcRank, HcclComm comm,
               aclrtStream stream);

/**
 * @brief Destroy
 * @param[in] comm the hccl communicator.
 * @return 0 if successful.
 */
int DSHcclCommDestroy(HcclComm comm);

/**
 * @brief Create an event, which is used in the stream synchronization waiting scenario.
 * @param[in] event Pointer to an event.
 * @return 0 if successful.
 */
int DSAclrtCreateEvent(aclrtEvent *event);

/**
 * @brief Record an event in the stream.
 * @param[in] event Event to be recorded.
 * @param[in] stream Records a specified event in a specified stream.
 * @return 0 if successful.
 */
int DSAclrtRecordEvent(aclrtEvent event, aclrtStream stream);

/**
 * @brief Blocks application running and waits for the event to complete.
 * @param[in] event Event to be recorded.
 * @return 0 if successful.
 */
int DSAclrtSynchronizeEvent(aclrtEvent event);

/**
 * @brief Record an event in the stream
 * @param[in] event Event to be waited.
 * @param[in] timeoutMs Timeout interval of an interface.
 * @return 0 if successful.
 */
int DSAclrtSynchronizeEventWithTimeout(aclrtEvent event, int32_t timeoutMs);

/**
 * @brief Destroy an event.
 * @param[in] event Event to be waited.
 * @return 0 if successful.
 */
int DSAclrtDestroyEvent(aclrtEvent event);

/**
 * @brief Queries whether the events recorded by aclrtRecordEvent
 * @param[in] event Event to query.
 * @param[out] status Pointer to the event status.
 * @return 0 if successful.
 */
int DSAclrtQueryEventStatus(aclrtEvent event, aclrtEventRecordedStatus *status);

/**
 * @brief Queries whether an error occurs in the communication domain.
 * In the current version, only the HCCL_E_REMOTE error type is returned.
 * @param[in] comm the hccl communicator.
 * @param[out] asyncError If the result is 0, no error occurs in the communication domain.
 * For details about other return values, see HcclResult Type.
 * @return 0 if successful.
 */
int DSHcclGetCommAsyncError(HcclComm comm, HcclResult *asyncError);

/**
 * @ingroup AscendCL
 * @brief acl initialize
 * @par Restriction
 * The aclInit interface can be called only once in a process
 * @param configPath [IN]    the config path,it can be NULL
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclInit(const char *configPath);

/**
 * @ingroup AscendCL
 * @brief Specify the device to use for the operation
 * implicitly create the default context and the default stream
 * @param  deviceId [IN]  the device id
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclrtSetDevice(const int deviceId);

/**
 * @brief acl finalize
 * @par Restriction
 * Need to call aclFinalize before the process exits.
 * After calling aclFinalize,the services cannot continue to be used normally.
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclFinalize();

/**
 * @brief get total device number.
 * @param count [OUT]    the device number
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclrtGetDeviceCount(uint32_t *count);

/**
 * @brief Check the device status.
 * @param deviceId [in]    the device number
 * @param deviceStatus [OUT]    the device status
 * @retval ACL_SUCCESS and ACL_RT_DEVICE_STATUS_NORMAL The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclrtQueryDeviceStatus(uint32_t deviceId, int32_t *deviceStatus);

/**
 * @brief  Asynchronous memory replication between Host and Device
 *
 * @par Function
 *  After calling this interface,
 *  be sure to call the aclrtSynchronizeStream interface to ensure that
 *  the task of memory replication has been completed
 *
 * @par Restriction
 * @li For on-chip Device-to-Device memory copy,
 *     both the source and destination addresses must be 64-byte aligned
 *
 * @param dst [IN]     destination address pointer
 * @param destMax [IN] Max length of destination address memory
 * @param src [IN]     source address pointer
 * @param count [IN]   the number of byte to copy
 * @param kind [IN]    memcpy type
 * @param stream [IN]  asynchronized task stream
 *
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclrtMemcpyAsync(void *dst, size_t destMax, const void *src, size_t count, aclrtMemcpyKind kind,
                       aclrtStream stream);

/**
 * @brief  Batched memory copy between Host and Device.
 * @param[in]  dsts           Array of destination address pointers.
 * @param[in]  destMax        Array of maximum lengths for each destination memory region, in bytes.
 * @param[in]  srcs           Array of source address pointers.
 * @param[in]  sizes          Array of copy sizes for each batch, in bytes.
 * @param[in]  numBatches     Number of batches; length of `dsts`, `srcs`, and `sizes` arrays.
 * @param[in]  attrs          Array of memory copy attributes.
 * @param[in]  attrsIndexes   Array of attribute indexes specifying the range of batches each attribute applies to.
 * @param[in]  numAttrs       Length of `attrs` and `attrsIndexes` arrays.
 * @param[out] failIndex      Index of the batch where an error occurred (only supports validation of memory attributes
 * and copy direction). If the error is not associated with a specific batch, this will be set to `SIZE_MAX`.
 *
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure.
 */

int DSAclrtMemcpyBatch(void **dsts, size_t *destMax, void **srcs, size_t *sizes, size_t numBatches,
                       aclrtMemcpyBatchAttr *attrs, size_t *attrsIndexes, size_t numAttrs, size_t *failIndex);

/**
 * @brief Reset the current operating Device and free resources on the device,
 * including the default context, the default stream,
 * and all streams created under the default context,
 * and synchronizes the interface.
 * If the task under the default context or stream has not been completed,
 * the system will wait for the task to complete before releasing it.
 *
 * @par Restriction
 * @li The Context, Stream, and Event that are explicitly created
 * on the device to be reset. Before resetting,
 * it is recommended to follow the following interface calling sequence,
 * otherwise business abnormalities may be caused.
 * @li Interface calling sequence:
 * call aclrtDestroyEvent interface to release Event or
 * call aclrtDestroyStream interface to release explicitly created Stream->
 * call aclrtDestroyContext to release explicitly created Context->
 * call aclrtResetDevice interface
 *
 * @param  deviceId [IN]   the device id
 *
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclrtResetDevice(int32_t deviceId);

/**
 * @brief alloc memory on device, real alloc size is aligned to 32 bytes and padded with 32 bytes
 *
 * @par Function
 *  alloc for size linear memory on device
 *  and return a pointer to allocated memory by *devPtr
 *
 * @par Restriction
 * @li The memory requested by the aclrtMalloc interface needs to be released
 * through the aclrtFree interface.
 * @li Before calling the media data processing interface,
 * if you need to apply memory on the device to store input or output data,
 * you need to call acldvppMalloc to apply for memory.
 *
 * @param devPtr [OUT]  pointer to pointer to allocated memory on device
 * @param size [IN]     alloc memory size
 * @param policy [IN]   memory alloc policy
 *
 * @retval ACL_SUCCESS The function is successfully executed.
 * @retval OtherValues Failure
 */
int DSAclrtMalloc(void **devPtr, size_t size, aclrtMemMallocPolicy policy);

int DSAclrtFree(void *devPtr);

int DSAclrtMallocHost(void **devPtr, size_t size);

int DSAclrtFreeHost(void *devPtr);

/**
 * @brief Get P2P root info.
 *
 * @param rootInfo A pointer identifying the P2P root info.
 * @return HcclResult
 */
int DSP2PGetRootInfo(HcclRootInfo *rootInfo);

/**
 * @brief Initialize P2P with root info.
 *
 * @param rootInfo A struct identifying the p2p root info.
 * @param kind Identifies whether the current device is a sender and receiver.
 * @param link Identifies the communication channel type.
 * @param comm A pointer identifying the initialized communication resource.
 * @param p2pCommInitOptions Additional options for p2pcomm
 * @return HcclResult
 * @see HcclCommDestroy()
 */
int DSP2PCommInitRootInfo(const HcclRootInfo *rootInfo, P2pKind kind, P2pLink link, P2PComm *comm,
                          P2PCommInitOptions *p2pCommInitOptions = nullptr);

/**
 * @brief Destroy P2P comm
 *
 * @param comm A pointer identifying the communication resource targeting
 * @return HcclResult
 * @see P2PCommInitRootInfo()
 */
int DSP2PCommDestroy(P2PComm comm);

/**
 * @brief Send operator.
 *
 * @param sendBuff A pointer identifying the input data address of the operator.
 * @param count An integer(u64) identifying the number of the send data.
 * @param dataType The data type of the operator, must be one of the following types: int8, int16, int32, int64,
uint8, uint16, uint32, uint64, float16, float32, float64, bfloat16.
 * @param comm A pointer identifying the communication resource based on.
 * @param stream A pointer identifying the stream information.
 * @return HcclResult
 */
int DSP2PSend(void *sendBuf, uint64_t count, HcclDataType dataType, P2PComm comm, aclrtStream stream);

/**
 * @brief Recv operator.
 *
 * @param recvBuff A pointer identifying the output data address of the operator.
 * @param count An integer(u64) identifying the number of the receive data.
 * @param dataType The data type of the operator, must be one of the following types: int8, int16, int32, int64,
uint8, uint16, uint32, uint64, float16, float32, float64, bfloat16.
 * @param srcRank An integer identifying the source rank.
 * @param comm A pointer identifying the communication resource based on.
 * @param stream A pointer identifying the stream information.
 * @return HcclResult
 */
int DSP2PRecv(void *recvBuf, uint64_t count, HcclDataType dataType, HcclComm comm, aclrtStream stream);

/**
 * @brief Queries whether an error occurs in the communication domain.
 * In the current version, only the HCCL_E_REMOTE error type is returned.
 * @param[in] comm the hccl communicator.
 * @param[out] asyncError If the result is 0, no error occurs in the communication domain.
 * For details about other return values, see HcclResult Type.
 * @return 0 if successful.
 */
int DSP2PGetCommAsyncError(P2PComm comm, HcclResult *asyncError);

int DSRtNotifyCreate(int32_t deviceId, void **notify);
int DSRtNotifyDestroy(void *notify);

int DSRtNotifyRecord(void *notify, void *stream);
int DSRtNotifyWait(void *notify, void *stream);

int DSRtGeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type);
int DSRtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val);

int DSAclrtLaunchCallback(aclrtCallback fn, void *userData, aclrtCallbackBlockType blockType, aclrtStream stream);
int DSAclrtProcessReport(int32_t timeout);
int DSAclrtSubscribeReport(uint64_t threadId, aclrtStream stream);
int DSAclrtUnSubscribeReport(uint64_t threadId, aclrtStream stream);

int DSP2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentInfo *segmentInfo, P2pSegmentPermissions permissions);
int DSP2PImportHostSegment(P2pSegmentInfo segmentInfo);
int DSP2PScatterBatchFromRemoteHostMem(P2pScatterEntry *entries, uint32_t batchSize, P2PComm comm, aclrtStream stream);

#ifdef __cplusplus
};
#endif

#endif
