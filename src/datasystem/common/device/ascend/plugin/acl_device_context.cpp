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

#include "datasystem/common/device/ascend/plugin/acl_device_context.h"

#include <runtime/dev.h>
#include <runtime/event.h>
#include <runtime/rt_stars.h>


const int DEFAULT_HCCL_BUFFSIZE = 10;  //  Config the buff size of hccl comm

int MallocDeviceMemory(size_t dataSize, void *&deviceData)
{
    return aclrtMalloc(&deviceData, dataSize, ACL_MEM_MALLOC_HUGE_FIRST);
}

int FreeDeviceMemory(void *deviceData)
{
    if (deviceData != nullptr) {
        return aclrtFree(deviceData);
    }
    return ACL_SUCCESS;
}

int GetDeviceIdx(int32_t &deviceIdx)
{
    return aclrtGetDevice(&deviceIdx);
}

int SetDeviceIdx(int32_t deviceIdx)
{
    return aclrtSetDevice(deviceIdx);
}

int MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize)
{
    return aclrtMemcpy(hostDst, dstMaxSize, devSrc, srcSize, ACL_MEMCPY_DEVICE_TO_HOST);
}

int MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize)
{
    return aclrtMemcpy(devDst, dstMaxSize, hostSrc, srcSize, ACL_MEMCPY_HOST_TO_DEVICE);
}

int MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize)
{
    return aclrtMemcpy(dst, dstMaxSize, src, srcSize, ACL_MEMCPY_DEVICE_TO_DEVICE);
}

int RtCreateStream(aclrtStream *stream)
{
    return aclrtCreateStream(stream);
}

int RtSynchronizeStream(aclrtStream stream)
{
    return aclrtSynchronizeStream(stream);
}

int RtSynchronizeStreamWithTimeout(aclrtStream stream, int32_t timeoutMs)
{
    return aclrtSynchronizeStreamWithTimeout(stream, timeoutMs);
}

int RtDestroyStream(aclrtStream stream)
{
    return aclrtDestroyStream(stream);
}

int RtDestroyStreamForce(aclrtStream stream)
{
    return aclrtDestroyStreamForce(stream);
}

int DSHcclGetRootInfo(HcclRootInfo *rootInfo)
{
    return HcclGetRootInfo(rootInfo);
}

int DSHcclCommInitRootInfo(uint32_t nRanks, const HcclRootInfo *rootInfo, uint32_t rank, HcclComm *comm)
{
    HcclCommConfig config;
    HcclCommConfigInit(&config);
    config.hcclBufferSize = DEFAULT_HCCL_BUFFSIZE;
    config.hcclOpExpansionMode = 1;  // Reduce the streams consumption of comm
    return HcclCommInitRootInfoConfig(nRanks, rootInfo, rank, &config, comm);
}

int DSHcclSend(void *sendBuf, uint64_t count, HcclDataType dataType, uint32_t destRank, HcclComm comm,
               aclrtStream stream)
{
    return HcclSend(sendBuf, count, dataType, destRank, comm, stream);
}

int DSHcclRecv(void *recvBuf, uint64_t count, HcclDataType dataType, uint32_t srcRank, HcclComm comm,
               aclrtStream stream)
{
    return HcclRecv(recvBuf, count, dataType, srcRank, comm, stream);
}

int DSHcclCommDestroy(HcclComm comm)
{
    return HcclCommDestroy(comm);
}

int DSAclrtCreateEvent(aclrtEvent *event)
{
    return aclrtCreateEvent(event);
}

int DSAclrtRecordEvent(aclrtEvent event, aclrtStream stream)
{
    return aclrtRecordEvent(event, stream);
}

int DSAclrtSynchronizeEvent(aclrtEvent event)
{
    return aclrtSynchronizeEvent(event);
}

int DSAclrtSynchronizeEventWithTimeout(aclrtEvent event, int32_t timeoutMs)
{
    return aclrtSynchronizeEventWithTimeout(event, timeoutMs);
}

int DSAclrtDestroyEvent(aclrtEvent event)
{
    return aclrtDestroyEvent(event);
}

int DSAclrtQueryEventStatus(aclrtEvent event, aclrtEventRecordedStatus *status)
{
    return aclrtQueryEventStatus(event, status);
}

int DSHcclGetCommAsyncError(HcclComm comm, HcclResult *asyncError)
{
    return HcclGetCommAsyncError(comm, asyncError);
}

int DSAclInit(const char *configPath)
{
    return aclInit(configPath);
}

int DSAclrtSetDevice(int deviceId)
{
    return aclrtSetDevice(deviceId);
}

int DSAclFinalize()
{
    return aclFinalize();
}

int DSAclrtGetDeviceCount(uint32_t *count)
{
    return aclrtGetDeviceCount(count);
}

int DSAclrtQueryDeviceStatus(uint32_t deviceId, int32_t *deviceStatus)
{
    aclrtDeviceStatus status;
    int ret = aclrtQueryDeviceStatus(deviceId, &status);
    *deviceStatus = status == ACL_RT_DEVICE_STATUS_NORMAL ? 0 : 1;
    return ret;
}

int DSAclrtMemcpyAsync(void *dst, size_t destMax, const void *src, size_t count, aclrtMemcpyKind kind,
                       aclrtStream stream)
{
    return aclrtMemcpyAsync(dst, destMax, src, count, kind, stream);
}

int DSAclrtResetDevice(int32_t deviceId)
{
    return aclrtResetDevice(deviceId);
}

int DSAclrtMalloc(void **devPtr, size_t size, aclrtMemMallocPolicy policy)
{
    return aclrtMalloc(devPtr, size, policy);
}

int DSAclrtFree(void *devPtr)
{
    return aclrtFree(devPtr);
}

int DSAclrtMallocHost(void **devPtr, size_t size)
{
    return aclrtMallocHost(devPtr, size);
}

int DSAclrtFreeHost(void *devPtr)
{
    return aclrtFreeHost(devPtr);
}

int DSP2PGetRootInfo(HcclRootInfo *rootInfo)
{
    return P2PGetRootInfo(rootInfo);
}

int DSP2PCommInitRootInfo(const HcclRootInfo *rootInfo, P2pKind kind, P2pLink link, P2PComm *comm)
{
    return P2PCommInitRootInfo(rootInfo, kind, link, comm);
}

int DSP2PCommDestroy(P2PComm comm)
{
    return P2PCommDestroy(comm);
}

int DSP2PSend(void *sendBuf, uint64_t count, HcclDataType dataType, P2PComm comm, aclrtStream stream)
{
    return P2PSend(sendBuf, count, dataType, comm, stream);
}

int DSP2PRecv(void *recvBuf, uint64_t count, HcclDataType dataType, HcclComm comm, aclrtStream stream)
{
    return P2PRecv(recvBuf, count, dataType, comm, stream);
}

int DSP2PGetCommAsyncError(P2PComm comm, HcclResult *asyncError)
{
    (void)comm;
    *asyncError = HcclResult::HCCL_SUCCESS;
    return 0;
}

int DSRtNotifyCreate(int32_t deviceId, void **notify)
{
    return rtNotifyCreate(deviceId, notify);
}

int DSRtNotifyDestroy(void *notify)
{
    return rtNotifyDestroy(notify);
}

int DSRtNotifyRecord(void *notify, void *stream)
{
    return rtNotifyRecord(notify, stream);
}

int DSRtNotifyWait(void *notify, void *stream)
{
    return rtNotifyWait(notify, stream);
}

int DSRtGeneralCtrl(uintptr_t *ctrl, uint32_t num, uint32_t type)
{
    return rtGeneralCtrl(ctrl, num, type);
}

int DSRtGetDeviceInfo(uint32_t deviceId, int32_t moduleType, int32_t infoType, int64_t *val)
{
    return rtGetDeviceInfo(deviceId, moduleType, infoType, val);
}

int DSAclrtLaunchCallback(aclrtCallback fn, void *userData, aclrtCallbackBlockType blockType, aclrtStream stream)
{
    return aclrtLaunchCallback(fn, userData, blockType, stream);
}

int DSAclrtProcessReport(int32_t timeout)
{
    return aclrtProcessReport(timeout);
}

int DSAclrtSubscribeReport(uint64_t threadId, aclrtStream stream)
{
    return aclrtSubscribeReport(threadId, stream);
}

int DSAclrtUnSubscribeReport(uint64_t threadId, aclrtStream stream)
{
    return aclrtUnSubscribeReport(threadId, stream);
}
