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
 * Description: Defines the ascend device manager.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_ACL_DEVICE_MANAGER_H
#define DATASYSTEM_COMMON_DEVICE_ACL_DEVICE_MANAGER_H

#include <dlfcn.h>
#include <functional>
#include <memory>
#include <mutex>

#include "datasystem/common/log/log.h"
#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/util/dlutils.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/utils/status.h"

#define RETURN_CANN_RESULT(aclRet, interType)                                                                          \
    do {                                                                                                               \
        int _aclRet = (aclRet);                                                                                        \
        if (_aclRet != 0) {                                                                                            \
            return Status(StatusCode::K_ACL_ERROR, __LINE__, __FILE__, FormatString("%s api failed with error code %d" \
            ", please refer to %s documentation for detailed error information. ", interType, aclRet, interType));     \
        }                                                                                                              \
        return Status::OK();                                                                                           \
    } while (false)

#define RETURN_ACL_RESULT(aclRet)          \
    do {                                   \
        RETURN_CANN_RESULT(aclRet, "ACL"); \
    } while (false)

#define RETURN_HCCL_RESULT(aclRet)          \
    do {                                    \
        RETURN_CANN_RESULT(aclRet, "HCCL"); \
    } while (false)

namespace datasystem {

namespace acl {

class AclDeviceManager : public DeviceManagerBase {
public:
    AclDeviceManager();
    virtual ~AclDeviceManager();
    AclDeviceManager(AclDeviceManager &&) = delete;                  // Move construct
    AclDeviceManager(const AclDeviceManager &) = delete;             // Copy construct
    AclDeviceManager &operator=(const AclDeviceManager &) = delete;  // Copy assign
    AclDeviceManager &operator=(AclDeviceManager &&) = delete;       // Move assign

    static AclDeviceManager *Instance();

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
     * @param[in] aclPluginPath Path of the aclPlugin so file.
     * @return OK if the file passes the integrity check.
     */
    virtual Status VerifyingSha256(const std::string &aclPluginPath);

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
     * @return Status of the call.
     */
    virtual Status SetDeviceIdx(int32_t deviceId);

    /**
     * @brief Create the acl runtime stream.
     * @param[in] stream The acl runtime stream.
     * @return Status of the call.
     */
    virtual Status RtCreateStream(aclrtStream *stream);

    /**
     * @brief Synchronize the stream between cpu and npu.
     * @param[in] stream  The acl runtime stream.
     * @return Status of the call.
     */
    virtual Status RtSynchronizeStream(aclrtStream stream);

    /**
     * @brief Synchronize the stream between cpu and npu within the timeout period.
     * @param[in] stream  The acl runtime stream.
     * @param[in] timeoutMs Timeout interval of an interface.
     * @return Status of the call.
     */
    virtual Status RtSynchronizeStreamWithTimeout(aclrtStream stream, int32_t timeoutMs);

    /**
     * @brief Destroy the acl runtime stream.
     * @param[in] stream The acl runtime stream.
     * @return Status of the call.
     */
    virtual Status RtDestroyStream(aclrtStream stream);

    /**
     * @brief Force destroy the acl runtime stream.
     * @param[in] stream The acl runtime stream.
     * @return Status of the call.
     */
    virtual Status RtDestroyStreamForce(aclrtStream stream);

    /**
     * @brief Hccl get the root info.
     * @param[out] rootInfo
     * @return Status of the call.
     */
    virtual Status DSHcclGetRootInfo(HcclRootInfo *rootInfo);

    /**
     * @brief Init the hccl communicator by root info.
     * @param[in] nRanks The number of ranks.
     * @param[in] rootInfo The root info.
     * @param[in] rank The rank in local.
     * @param[out] comm The hccl communicator.
     * @return Status of the call.
     */
    virtual Status DSHcclCommInitRootInfo(uint32_t nRanks, const HcclRootInfo *rootInfo, uint32_t rank, HcclComm *comm);

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
    virtual Status DSHcclSend(void *sendBuf, uint64_t count, HcclDataType dataType, uint32_t destRank, HcclComm comm,
                              aclrtStream stream);

    /**
     * @brief HCCL recv the data from the sending side.
     * @param[in] recvBuf The pointer of data ready to receive.
     * @param[in] count The count of item in the buffer.
     * @param[in] dataType The data type of item in the buffer.
     * @param[in] srcRank The rank of source.
     * @param[in] comm The hccl communicator.
     * @param[in] stream The acl runtime stream.
     * @return Status of the call.
     */
    virtual Status DSHcclRecv(void *recvBuf, uint64_t count, HcclDataType dataType, uint32_t srcRank, HcclComm comm,
                              aclrtStream stream);

    /**
     * @brief Destroy
     * @param[in] comm the hccl communicator.
     * @return Status of the call.
     */
    Status DSHcclCommDestroy(HcclComm comm);

    /**
     * @brief Create an event, which is used in the stream synchronization waiting scenario.
     * @param[in] event Pointer to an event.
     * @return Status of the call.
     */
    virtual Status DSAclrtCreateEvent(aclrtEvent *event);

    /**
     * @brief Record an event in the stream.
     * @param[in] event Event to be recorded.
     * @param[in] stream Records a specified event in a specified stream.
     * @return Status of the call.
     */
    virtual Status DSAclrtRecordEvent(aclrtEvent event, aclrtStream stream);

    /**
     * @brief Blocks application running and waits for the event to complete.
     * @param[in] event Event to be recorded.
     * @return Status of the call.
     */
    virtual Status DSAclrtSynchronizeEvent(aclrtEvent event);

    /**
     * @brief Record an event in the stream.
     * @param[in] event Event to be waited.
     * @param[in] timeoutMs Timeout interval of an interface.
     * @return Status of the call.
     */
    virtual Status DSAclrtSynchronizeEventWithTimeout(aclrtEvent event, int32_t timeoutMs);

    /**
     * @brief Destroy an event.
     * @param[in] event Event to be waited.
     * @return Status of the call.
     */
    virtual Status DSAclrtDestroyEvent(aclrtEvent event);

    /**
     * @brief Queries whether the events recorded by aclrtRecordEvent
     * @param[in] event Event to query.
     * @return Status of the call.
     */
    virtual Status DSAclrtQueryEventStatus(aclrtEvent event);

    /**
     * @brief Queries whether an error occurs in the communication domain.
     * In the current version, only the HCCL_E_REMOTE error type is returned.
     * @param[in] comm the hccl communicator.
     * @param[out] asyncError If the result is 0, no error occurs in the communication domain.
     * For details about other return values, see HcclResult Type.
     * @return Status of the call.
     */
    virtual Status DSHcclGetCommAsyncError(HcclComm comm);

    virtual Status aclInit(const char *configPath);

    virtual Status aclrtSetDevice(int deviceId);

    virtual Status aclFinalize();

    virtual Status aclrtGetDeviceCount(uint32_t *count);

    virtual Status aclrtQueryDeviceStatus(uint32_t deviceId);

    virtual Status aclrtMemcpyAsync(void *dst, size_t destMax, const void *src, size_t count, aclrtMemcpyKind kind,
                                    aclrtStream stream);

    virtual Status aclrtResetDevice(int32_t deviceId);

    virtual Status aclrtMalloc(void **devPtr, size_t size, aclrtMemMallocPolicy policy);

    virtual Status aclrtFree(void *devPtr);

    virtual Status aclrtMallocHost(void **devPtr, size_t size);

    virtual Status aclrtFreeHost(void *devPtr);

    /**
     * @brief Get P2P root info.
     *
     * @param rootInfo A pointer identifying the P2P root info.
     * @return HcclResult
     */
    virtual Status DSP2PGetRootInfo(HcclRootInfo *rootInfo);

    /**
     * @brief Initialize P2P with root info.
     *
     * @param rootInfo A struct identifying the p2p root info.
     * @param kind Identifies whether the current device is a sender and receiver.
     * @param link Identifies the communication channel type.
     * @param comm A pointer identifying the initialized communication resource.
     * @return HcclResult
     * @see HcclCommDestroy()
     */
    virtual Status DSP2PCommInitRootInfo(const HcclRootInfo *rootInfo, P2pKind kind, P2pLink link, P2PComm *comm);

    /**
     * @brief Destroy P2P comm
     *
     * @param comm A pointer identifying the communication resource targeting
     * @return HcclResult
     * @see P2PCommInitRootInfo()
     */
    virtual Status DSP2PCommDestroy(P2PComm comm);

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
    virtual Status DSP2PSend(void *sendBuf, uint64_t count, HcclDataType dataType, P2PComm comm, aclrtStream stream);

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
    virtual Status DSP2PRecv(void *recvBuf, uint64_t count, HcclDataType dataType, P2PComm comm, aclrtStream stream);

    /**
     * @brief Queries whether an error occurs in the communication domain.
     * In the current version, only the HCCL_E_REMOTE error type is returned.
     * @param[in] comm the hccl communicator.
     * @param[out] asyncError If the result is 0, no error occurs in the communication domain.
     * For details about other return values, see HcclResult Type.
     * @return Status of the call.
     */
    virtual Status DSP2PGetCommAsyncError(P2PComm comm);

    virtual Status RtNotifyCreate(int32_t deviceId, void **notify);
    virtual Status RtNotifyDestroy(void *notify);

    virtual Status RtNotifyRecord(void *notify, void *stream);
    virtual Status RtNotifyWait(void *notify, void *stream);

    virtual Status RtGeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type);
    virtual Status RtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val);

    virtual Status AclrtLaunchCallback(aclrtCallback fn, void *userData, aclrtCallbackBlockType blockType,
                                       aclrtStream stream);
    virtual Status AclrtProcessReport(int32_t timeout);
    virtual Status AclrtSubscribeReport(uint64_t threadId, aclrtStream stream);
    virtual Status AclrtUnSubscribeReport(uint64_t threadId, aclrtStream stream);

    virtual Status DSP2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentInfo *segmentInfo,
                                        P2pSegmentPermissions permissions);
    virtual Status DSP2PImportHostSegment(P2pSegmentInfo segmentInfo);
    virtual Status DSP2PScatterBatchFromRemoteHostMem(P2pScatterEntry* entries, uint32_t batchSize, P2PComm comm,
                                                      aclrtStream stream);

    // ==================== DeviceManagerBase Interface Implementation ====================
    // Lifecycle Management
    Status Init(const char *configPath) override;
    Status Finalize() override;

    // Device Management
    Status GetDeviceCount(uint32_t *count) override;
    Status QueryDeviceStatus(uint32_t deviceId) override;
    Status SetDevice(int32_t deviceId) override;
    Status ResetDevice(int32_t deviceId) override;

    // Memory Management
    Status Malloc(void **devPtr, size_t size, MemMallocPolicy policy) override;
    Status Free(void *devPtr) override;
    Status MallocHost(void **hostPtr, size_t size) override;
    Status FreeHost(void *hostPtr) override;

    // Memory Copy
    Status MemcpyAsync(void *dst, size_t dstMaxSize, const void *src, size_t count,
                       MemcpyKind kind, void *stream) override;

    // Stream Management
    Status CreateStream(void **stream) override;
    Status SynchronizeStream(void *stream) override;
    Status SynchronizeStreamWithTimeout(void *stream, int32_t timeoutMs) override;
    Status DestroyStream(void *stream) override;
    Status DestroyStreamForce(void *stream) override;

    // Event Management
    Status CreateEvent(void **event) override;
    Status RecordEvent(void *event, void *stream) override;
    Status SynchronizeEvent(void *event) override;
    Status SynchronizeEventWithTimeout(void *event, int32_t timeoutMs) override;
    Status DestroyEvent(void *event) override;
    Status QueryEventStatus(void *event) override;

    // Communication
    Status CommGetRootInfo(CommRootInfo *rootInfo) override;
    Status CommInitRootInfo(uint32_t nRanks, const CommRootInfo *rootInfo, uint32_t rank, void **comm) override;
    Status CommSend(void *sendBuf, uint64_t count, CommDataType dataType, uint32_t destRank,
                    void *comm, void *stream) override;
    Status CommRecv(void *recvBuf, uint64_t count, CommDataType dataType, uint32_t srcRank,
                    void *comm, void *stream) override;
    Status CommDestroy(void *comm) override;
    Status CommGetAsyncError(void *comm) override;

    // P2P
    Status P2PGetRootInfo(CommRootInfo *rootInfo) override;
    Status P2PCommInitRootInfo(const CommRootInfo *rootInfo, P2pKindBase kind, P2pLinkBase link,
                               void **comm) override;
    Status P2PCommDestroy(void *comm) override;
    Status P2PSend(void *sendBuf, uint64_t count, CommDataType dataType, void *comm, void *stream) override;
    Status P2PRecv(void *recvBuf, uint64_t count, CommDataType dataType, void *comm, void *stream) override;
    Status P2PGetCommAsyncError(void *comm) override;
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
    Status P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentBase *segmentInfo,
                              P2pSegmentPermBase permissions) override;
    Status P2PImportHostSegment(P2pSegmentBase segmentInfo) override;
    Status P2PScatterBatchFromRemoteHostMem(P2pScatterBase *entries, uint32_t batchSize,
                                            void *comm, void *stream) override;

private:
    /**
     * @brief Init the acl device manager and load ascend plugin.
     */
    void Init();

    /**
     * @brief Load ascend plugin library.
     * @return OK if load library success.
     */
    Status LoadPlugin();

    /**
     * @brief dlsym the class member function pointer.
     */
    void DlsymFuncObj();

    void LoadResearchPlugin();

    /**
     * @brief Handle HCCL operation result and provide detailed error information
     * @param hcclResult The HCCL operation result code to be checked
     * @return Status Returns Status with detailed message if result is HCCL_E_SUSPENDING,
     *                otherwise returns the original HCCL result conversion
     * @note Special handling for HCCL_E_SUSPENDING (22): Provides troubleshooting guidance for communicator suspension
     *       caused by device state reset or communication domain destruction
     */
    Status HandleHcclResult(int hcclResult);

    // Register plugin function as function pointer in class member.
    REG_METHOD(MallocDeviceMemory, int, size_t, void *&);
    REG_METHOD(FreeDeviceMemory, int, void *);
    REG_METHOD(GetDeviceIdx, int, int32_t &);
    REG_METHOD(MemCopyD2H, int, void *, size_t, const void *, size_t);
    REG_METHOD(MemCopyH2D, int, void *, size_t, const void *, size_t);
    REG_METHOD(MemCopyD2D, int, void *, size_t, const void *, size_t);
    REG_METHOD(SetDeviceIdx, int, int32_t);

    REG_METHOD(RtCreateStream, int, aclrtStream *);
    REG_METHOD(RtSynchronizeStream, int, aclrtStream);
    REG_METHOD(RtSynchronizeStreamWithTimeout, int, aclrtStream, int32_t);
    REG_METHOD(RtDestroyStream, int, aclrtStream);
    REG_METHOD(RtDestroyStreamForce, int, aclrtStream);

    REG_METHOD(DSHcclGetRootInfo, int, HcclRootInfo *);
    REG_METHOD(DSHcclCommInitRootInfo, int, uint32_t, const HcclRootInfo *, uint32_t, HcclComm *);
    REG_METHOD(DSHcclSend, int, void *, uint64_t, HcclDataType, uint32_t, HcclComm, aclrtStream);
    REG_METHOD(DSHcclRecv, int, void *, uint64_t, HcclDataType, uint32_t, HcclComm, aclrtStream);
    REG_METHOD(DSHcclCommDestroy, int, HcclComm);

    REG_METHOD(DSAclrtCreateEvent, int, aclrtEvent *);
    REG_METHOD(DSAclrtRecordEvent, int, aclrtEvent, aclrtStream);
    REG_METHOD(DSAclrtSynchronizeEvent, int, aclrtEvent);
    REG_METHOD(DSAclrtSynchronizeEventWithTimeout, int, aclrtEvent, int32_t);
    REG_METHOD(DSAclrtDestroyEvent, int, aclrtEvent);
    REG_METHOD(DSAclrtQueryEventStatus, int, aclrtEvent, aclrtEventRecordedStatus *);
    REG_METHOD(DSHcclGetCommAsyncError, int, HcclComm, HcclResult *);
    REG_METHOD(DSAclInit, int, const char *);
    REG_METHOD(DSAclrtSetDevice, int, int);
    REG_METHOD(DSAclFinalize, int);
    REG_METHOD(DSAclrtGetDeviceCount, int, uint32_t *);
    REG_METHOD(DSAclrtQueryDeviceStatus, int, uint32_t, int32_t *);
    REG_METHOD(DSAclrtMemcpyAsync, int, void *, size_t, const void *, size_t, aclrtMemcpyKind, aclrtStream);
    REG_METHOD(DSAclrtResetDevice, int, int32_t);
    REG_METHOD(DSAclrtMalloc, int, void **, size_t, aclrtMemMallocPolicy);
    REG_METHOD(DSAclrtFree, int, void *);
    REG_METHOD(DSAclrtMallocHost, int, void **, size_t);
    REG_METHOD(DSAclrtFreeHost, int, void *);

    REG_METHOD(DSP2PGetRootInfo, int, HcclRootInfo *);
    REG_METHOD(DSP2PCommInitRootInfo, int, const HcclRootInfo *, P2pKind, P2pLink, P2PComm *);
    REG_METHOD(DSP2PCommDestroy, int, P2PComm);
    REG_METHOD(DSP2PSend, int, void *, uint64_t, HcclDataType, P2PComm, aclrtStream);
    REG_METHOD(DSP2PRecv, int, void *, uint64_t, HcclDataType, P2PComm, aclrtStream);
    REG_METHOD(DSP2PGetCommAsyncError, int, P2PComm, HcclResult *);

    REG_METHOD(DSRtNotifyCreate, int, int32_t, void **);
    REG_METHOD(DSRtNotifyDestroy, int, void *);
    REG_METHOD(DSRtNotifyRecord, int, void *, void *);
    REG_METHOD(DSRtNotifyWait, int, void *, void *);

    REG_METHOD(DSRtGeneralCtrl, int, uintptr_t *ctrl, uint32_t num, uint32_t type);
    REG_METHOD(DSRtGetDeviceInfo, int, uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val);
    REG_METHOD(DSAclrtLaunchCallback, int, aclrtCallback fn, void *userData, aclrtCallbackBlockType blockType,
               aclrtStream stream);
    REG_METHOD(DSAclrtProcessReport, int, int32_t timeout);
    REG_METHOD(DSAclrtSubscribeReport, int, uint64_t threadId, aclrtStream stream);
    REG_METHOD(DSAclrtUnSubscribeReport, int, uint64_t threadId, aclrtStream stream);

    REG_METHOD(DSP2PRegisterHostMem, int, void *, uint64_t, P2pSegmentInfo *, P2pSegmentPermissions);
    REG_METHOD(DSP2PImportHostSegment, int, P2pSegmentInfo);
    REG_METHOD(DSP2PScatterBatchFromRemoteHostMem, int, P2pScatterEntry*, uint32_t, P2PComm, aclrtStream);

    static std::once_flag init_;
    static std::once_flag hasLoadPlugin_;
    static std::unique_ptr<AclDeviceManager> instance_;

    void *pluginHandle_{ nullptr };
    std::unique_ptr<WaitPost> waitPost_;
    std::unique_ptr<Thread> loadPluginThread_{ nullptr };
};
}  // namespace acl
}  // namespace datasystem
#endif
