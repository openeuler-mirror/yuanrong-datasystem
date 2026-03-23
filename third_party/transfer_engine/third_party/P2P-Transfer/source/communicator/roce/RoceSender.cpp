/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#include "communicator/roce/RoceSender.h"
#include "tools/npu-error.h"
#include "tools/tools.h"
#include "tools/env.h"
#include "tools/logging.h"
#include "securec.h"
#include "runtime/dev.h"
#include <acl/acl.h>
#include <string>
#include "npu/P2PStream.h"

RoceSender::RoceSender(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes,
                       uint32_t nSendBuffs, uint32_t qpNum)
    : sendDeviceId(deviceId),
      isRoot(isRoot),
      blockSizeBytes(blockSizeBytes),
      chunkSizeBytes(chunkSizeBytes),
      nSendBuffs(nSendBuffs),
      qpNum(qpNum)
{
    nChunksPerBuff = blockSizeBytes / chunkSizeBytes;  // Should be divisible
}

Status RoceSender::Initialize(TCPObjectClient *client, TCPObjectServer *server)
{
    uint32_t logicDevIndex = static_cast<uint32_t>(sendDeviceId);
    uint32_t phyId = 0;

    rtError_t phyRc = rtGetDevicePhyIdByIndex(logicDevIndex, &phyId);
    p2p::LogInfo(std::string("RoceSender::Initialize phy query #1, send_device_id=") +
                 std::to_string(sendDeviceId) + ", logic_device_id=" + std::to_string(logicDevIndex) +
                 ", rc=" + std::to_string(static_cast<int>(phyRc)));
    if (phyRc != RT_ERROR_NONE) {
        if (!TryMapVisibleToLogicDeviceId(static_cast<uint32_t>(sendDeviceId), logicDevIndex)) {
            ACL_CHECK_STATUS(rtGetDeviceIndexByPhyId(static_cast<uint32_t>(sendDeviceId), &logicDevIndex));
            p2p::LogInfo(std::string("RoceSender::Initialize fallback rtGetDeviceIndexByPhyId success, input=") +
                         std::to_string(sendDeviceId) + ", logic_device_id=" + std::to_string(logicDevIndex));
        } else {
            p2p::LogInfo(std::string("RoceSender::Initialize fallback env map success, input=") +
                         std::to_string(sendDeviceId) + ", logic_device_id=" + std::to_string(logicDevIndex));
        }
        ACL_CHECK_STATUS(rtGetDevicePhyIdByIndex(logicDevIndex, &phyId));
        p2p::LogInfo(std::string("RoceSender::Initialize phy query #2 success, logic_device_id=") +
                     std::to_string(logicDevIndex) + ", phy_id=" + std::to_string(phyId));
    } else {
        p2p::LogInfo(std::string("RoceSender::Initialize phy query #1 success, phy_id=") + std::to_string(phyId));
    }

    CHECK_STATUS(RdmaDev::GetInstance(phyId, rdmaDev));
    union hccp_ip_addr ipv4Addr;
    CHECK_STATUS(rdmaDev->getIpv4(&ipv4Addr));

    rdmaSocket = std::make_unique<RdmaSocket>(phyId, ipv4Addr, CLIENT);
    CHECK_STATUS(rdmaSocket->init());

    SenderData senderData;
    senderData.set_sendnpuipv4(in_addr_to_uint32(ipv4Addr.addr));

    if (isRoot) {
        CHECK_STATUS(server->SendObject(senderData));
    } else {
        CHECK_STATUS(client->SendObject(senderData));
    }

    ReceiverData receiverData;
    if (isRoot) {
        CHECK_STATUS(server->ReceiveObject(receiverData));
    } else {
        CHECK_STATUS(client->ReceiveObject(receiverData));
    }

    union hccp_ip_addr remoteIp {
    };
    uint32_to_in_addr(receiverData.recvnpuipv4(), remoteIp.addr);
    CHECK_STATUS(rdmaSocket->connect(remoteIp, receiverData.recvlistenport(), receiverData.tag()));
    CHECK_STATUS(rdmaSocket->waitReady(0));

    void *rdmaHandle;
    void *fdHandle;
    CHECK_STATUS(rdmaDev->getRdmaHandle(&rdmaHandle));
    CHECK_STATUS(rdmaSocket->getFdHandle(&fdHandle));

    valueMem = std::make_unique<NotifyValueMem>();
    CHECK_STATUS(valueMem->alloc());
    CHECK_STATUS(valueMem->get(&notifySrcValAddr, &notifySize));

    for (int i = 0; i < nSendBuffs; i++) {
        std::unique_ptr<P2PMem> mem = std::make_unique<P2PMem>();
        CHECK_STATUS(mem->alloc(blockSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST_P2P));
        sendBuffs.push_back(std::move(mem));
    }

    for (int q = 0; q < qpNum; q++) {
        std::unique_ptr<RdmaQp> qp = std::make_unique<RdmaQp>();
        CHECK_STATUS(qp->create(rdmaHandle));

        CHECK_STATUS(qp->registerMemoryRegion(notifySrcValAddr, notifySize));

        for (int i = 0; i < nSendBuffs; i++) {
            CHECK_STATUS(qp->registerMemoryRegion(sendBuffs[i]->get(), blockSizeBytes));
        }

        CHECK_STATUS(qp->connect(fdHandle));
        CHECK_STATUS(qp->waitReady(0));
        qps.push_back(std::move(qp));
    }

    CHECK_STATUS(rdmaSocket->close());

    for (int i = 0; i < nSendBuffs; i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->create(sendDeviceId));
        recvReadyNotifies.push_back(std::move(notify));
    }

    recvCompleteNotify = std::make_unique<RdmaNotify>();
    CHECK_STATUS(recvCompleteNotify->create(sendDeviceId));

    for (int i = 0; i < nSendBuffs * nChunksPerBuff; i++) {
        std::unique_ptr<LocalNotify> notify = std::make_unique<LocalNotify>();
        CHECK_STATUS(notify->create(sendDeviceId));
        sendReadyNotifies.push_back(std::move(notify));
    }

    ReceiverNpuResources receiverResources;
    if (isRoot) {
        CHECK_STATUS(server->ReceiveObject(receiverResources));
    } else {
        CHECK_STATUS(client->ReceiveObject(receiverResources));
    }

    for (int i = 0; i < receiverResources.recvbuffaddrs_size(); i++) {
        remoteRecvBuffAddrs.push_back(receiverResources.recvbuffaddrs(i));
    }

    for (int i = 0; i < qpNum; i++) {
        qpNotifyRemoteBaseVas.push_back(receiverResources.qpbaseaddrs(i));
    }

    // Get notify addresses
    SenderNpuResources senderResources;
    for (int q = 0; q < qpNum; q++) {
        unsigned long long notifyBaseVa;
        CHECK_STATUS(qps[q]->getNotifyBaseAddress(&notifyBaseVa));
        senderResources.add_qpbaseaddrs(static_cast<uint64_t>(notifyBaseVa));
    }

    for (auto &notify : recvReadyNotifies) {
        uint64_t recvReadyNotifyAddrOffset;
        CHECK_STATUS(notify->getAddrOffset(&recvReadyNotifyAddrOffset));
        senderResources.add_recvreadynotifyaddroffsets(recvReadyNotifyAddrOffset);
    }

    uint64_t recvCompleteNotifyAddrOffset;
    CHECK_STATUS(recvCompleteNotify->getAddrOffset(&recvCompleteNotifyAddrOffset));
    senderResources.set_recvcompletenotifyaddroffset(recvCompleteNotifyAddrOffset);

    senderResources.set_notifysrcvaladdr(reinterpret_cast<uint64_t>(notifySrcValAddr));

    if (isRoot) {
        CHECK_STATUS(server->SendObject(senderResources));
    } else {
        CHECK_STATUS(client->SendObject(senderResources));
    }

    for (int i = 0; i < receiverResources.senddonenotifyaddroffsets_size(); i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->open(receiverResources.senddonenotifyaddroffsets(i)));
        sendDoneNotifies.push_back(std::move(notify));
    }

    state = RoceSenderStatus::ROCE_SENDER_INITIALIZED;

    fftsDispatcher = std::make_unique<p2p::DispatcherFFTS>(sendDeviceId);
    NPU_ERROR(fftsDispatcher->Init());
    NPU_ERROR(fftsDispatcher->CreateFftsCtxs(1));
    NPU_ERROR(fftsDispatcher->SetFftsCtx(0));

    return Status::Success();
}

constexpr size_t PAGE_SIZE_BYTES = 4 * 1024;  // 4 KiB

#define USE_FFTS

#ifdef USE_FFTS
Status RoceSender::SendChunk(void **srcPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream, uint32_t &lastTaskId,
                             bool isLast)
{
    uint32_t recvReadyTaskId = 0;
    if (curChunk == 0) {
        uint32_t notifyId;
        CHECK_STATUS(recvReadyNotifies[curBuffer]->getId(&notifyId));
        fftsDispatcher->SignalWaitCrossChip(recvReadyNotifies[curBuffer]->get(), &recvReadyTaskId, notifyId);
        if (lastTaskId > 0) {
            fftsDispatcher->AddTaskDependency(lastTaskId, recvReadyTaskId);
        }
    }

    uint32_t numLastMemcpyTaskIds = std::min(SENDER_MAX_PARALLEL_TASKS, count);
    uint32_t lastMemcpyTaskIds[numLastMemcpyTaskIds];
    uint64_t curChunkOffset = 0;

    void *chunkMid =
        static_cast<void *>(static_cast<unsigned char *>(sendBuffs[curBuffer]->get()) + curChunk * chunkSizeBytes);
    uint64_t chunkDst = remoteRecvBuffAddrs[curBuffer] + curChunk * chunkSizeBytes;

    for (int i = 0; i < count; i++) {
        void *chunkSrc = srcPtrs[i];
        void *chunkMidOffset = static_cast<void *>(static_cast<unsigned char *>(chunkMid) + curChunkOffset);
        size_t copySize = sizes[i];

        uint32_t memcpyTaskId = 0;
        fftsDispatcher->MemcpyAsync(chunkMidOffset, chunkSrc, copySize, &memcpyTaskId);

        if (i < SENDER_MAX_PARALLEL_TASKS) {
            if (i < SENDER_MAX_PARALLEL_TASKS && curChunk == 0) {
                fftsDispatcher->AddTaskDependency(recvReadyTaskId, memcpyTaskId);
            } else if (lastTaskId > 0) {
                fftsDispatcher->AddTaskDependency(lastTaskId, memcpyTaskId);
            }
        } else {
            fftsDispatcher->AddTaskDependency(lastMemcpyTaskIds[i % SENDER_MAX_PARALLEL_TASKS], memcpyTaskId);
        }
        curChunkOffset += RoundUp(copySize, PAGE_SIZE_BYTES);
        lastMemcpyTaskIds[i % SENDER_MAX_PARALLEL_TASKS] = memcpyTaskId;
    }

    uint64_t notifyDstAddr;
    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getRecordInfo(&notifyDstAddr));
    notifyDstAddr += qpNotifyRemoteBaseVas[0];
    uint32_t rdmaWriteTaskId = 0;

    CHECK_STATUS(qps[0]->dispatchRdmaOpFfts(fftsDispatcher.get(), reinterpret_cast<uint64_t>(chunkMid), chunkDst,
                                            curChunkOffset, RA_OP_WRITE, RA_SEND_SIGNALED, &rdmaWriteTaskId));

    for (int i = 0; i < numLastMemcpyTaskIds; i++) {
        fftsDispatcher->AddTaskDependency(lastMemcpyTaskIds[i], rdmaWriteTaskId);
    }

    uint32_t rdmaNotifyTaskId = 0;
    CHECK_STATUS(qps[0]->dispatchRdmaOpFfts(fftsDispatcher.get(), reinterpret_cast<uint64_t>(notifySrcValAddr),
                                            notifyDstAddr, notifySize, RA_OP_WRITE, RA_SEND_SIGNALED | RA_SEND_FENCE,
                                            &rdmaNotifyTaskId));
    fftsDispatcher->AddTaskDependency(rdmaWriteTaskId, rdmaNotifyTaskId);

    if (isLast) {
        uint32_t recvCompleteTaskId;
        uint32_t notifyId;
        CHECK_STATUS(recvCompleteNotify->getId(&notifyId));
        fftsDispatcher->SignalWaitCrossChip(recvCompleteNotify->get(), &recvCompleteTaskId, notifyId);
        fftsDispatcher->AddTaskDependency(rdmaNotifyTaskId, recvCompleteTaskId);
        lastTaskId = recvCompleteTaskId;
    } else {
        lastTaskId = rdmaNotifyTaskId;
    }

    if (curChunk == nChunksPerBuff - 1) {
        curBuffer = (curBuffer + 1) % nSendBuffs;
    }
    curChunk = (curChunk + 1) % nChunksPerBuff;

    return Status::Success();
}
#else
Status RoceSender::SendChunk(void **srcPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream, uint32_t &lastTaskId,
                             bool isLast)
{
    if (curChunk == 0) {
        CHECK_STATUS(recvReadyNotifies[curBuffer]->wait(stream));
    }

    uint64_t curChunkOffset = 0;

    void *chunkMid =
        static_cast<void *>(static_cast<unsigned char *>(sendBuffs[curBuffer]->get()) + curChunk * chunkSizeBytes);
    uint64_t chunkDst = remoteRecvBuffAddrs[curBuffer] + curChunk * chunkSizeBytes;

    for (int i = 0; i < count; i++) {
        void *chunkSrc = srcPtrs[i];
        void *chunkMidOffset = static_cast<void *>(static_cast<unsigned char *>(chunkMid) + curChunkOffset);
        size_t copySize = sizes[i];
        ACL_CHECK_STATUS(
            aclrtMemcpyAsync(chunkMidOffset, copySize, chunkSrc, copySize, ACL_MEMCPY_DEVICE_TO_DEVICE, stream));
        curChunkOffset += RoundUp(copySize, PAGE_SIZE_BYTES);
    }

    const uint32_t kRoceVectorSize = 2;
    std::vector<uint64_t> roceSrcAddrs(kRoceVectorSize);
    std::vector<uint64_t> roceDstAddrs(kRoceVectorSize);
    std::vector<uint32_t> roceLengths(kRoceVectorSize);

    roceSrcAddrs[0] = reinterpret_cast<uint64_t>(chunkMid);
    roceDstAddrs[0] = chunkDst;
    roceLengths[0] = curChunkOffset;

    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getRecordInfo(&roceDstAddrs[1]));
    roceDstAddrs[1] += qpNotifyRemoteBaseVas[0];
    roceSrcAddrs[1] = reinterpret_cast<uint64_t>(notifySrcValAddr);
    roceLengths[1] = notifySize;

    CHECK_STATUS(qps[0]->execRdmaOp(roceSrcAddrs, roceDstAddrs, roceLengths, RA_OP_WRITE, RA_SEND_SIGNALED, stream));

    if (isLast) {
        CHECK_STATUS(recvCompleteNotify->wait(stream));
    }

    if (curChunk == nChunksPerBuff - 1) {
        curBuffer = (curBuffer + 1) % nSendBuffs;
    }
    curChunk = (curChunk + 1) % nChunksPerBuff;

    return Status::Success();
}
#endif

Status RoceSender::Send(void **srcPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream)
{
    if (state != RoceSenderStatus::ROCE_SENDER_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Sender has not been initialized yet");
    }

    uint32_t numTasks = 1;

    uint32_t lastTaskId = 0;
    uint32_t srcIdx = 0;
    void *chunkSrcPtrs[chunkSizeBytes / PAGE_SIZE_BYTES];
    size_t chunkCopySizes[chunkSizeBytes / PAGE_SIZE_BYTES];

    while (srcIdx < count) {
        uint64_t currentSize = sizes[srcIdx];  // size

        // Transfer is larger than a chunk and must be split across multiple chunks
        if (currentSize >= chunkSizeBytes) {
            uint32_t numChunks = (currentSize + chunkSizeBytes - 1) / chunkSizeBytes;
            for (uint64_t i = 0; i < numChunks; i++) {
                chunkSrcPtrs[0] =
                    static_cast<void *>(static_cast<unsigned char *>(srcPtrs[srcIdx]) + i * chunkSizeBytes);
                chunkCopySizes[0] = (i < numChunks - 1) ? chunkSizeBytes : (currentSize - i * chunkSizeBytes);
                bool isLast = (srcIdx == count - 1) && i == (numChunks - 1);
                CHECK_STATUS(SendChunk(chunkSrcPtrs, chunkCopySizes, 1, stream, lastTaskId, isLast));
            }
            srcIdx++;
        } else {
            // Transfer fits in a chunk, try to fit as many subsequent chunks in transfer as possible
            uint64_t chunkSize = 0;
            uint32_t numMerged = 0;

            for (uint32_t i = srcIdx; i < count; ++i) {
                if (sizes[i] > chunkSizeBytes) {
                    break;
                }

                uint64_t alignedSize = RoundUp(sizes[i], PAGE_SIZE_BYTES);
                if (chunkSize + alignedSize <= chunkSizeBytes) {
                    chunkSrcPtrs[i - srcIdx] = srcPtrs[i];
                    chunkCopySizes[i - srcIdx] = sizes[i];
                    chunkSize += alignedSize;
                    numMerged++;
                } else {
                    break;
                }
            }
            bool isLast = (srcIdx + numMerged == count);
            if (srcIdx == 0 && curChunk != 0) {
                numTasks = std::min(SENDER_MAX_PARALLEL_TASKS, numMerged);
            }
            CHECK_STATUS(SendChunk(chunkSrcPtrs, chunkCopySizes, numMerged, stream, lastTaskId, isLast));
            srcIdx += numMerged;
        }
    }

// might want to split more depending on amount of chunks
#ifdef USE_FFTS
    fftsDispatcher->LaunchFftsTask(stream, numTasks, 0);
    fftsDispatcher->ReuseCtx(0);
#endif
    return Status::Success();
}
