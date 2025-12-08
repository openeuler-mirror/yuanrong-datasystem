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
#include "communicator/roce/RoceReceiver.h"
#include "tools/npu-error.h"
#include "tools/tools.h"
#include "securec.h"
#include "runtime/dev.h"
#include "tools/env.h"

RoceReceiver::RoceReceiver(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes,
                           uint32_t nRecvBuffs, uint32_t qpNum)
    : recvDeviceId(deviceId),
      isRoot(isRoot),
      blockSizeBytes(blockSizeBytes),
      chunkSizeBytes(chunkSizeBytes),
      nRecvBuffs(nRecvBuffs),
      qpNum(qpNum)
{
    nChunksPerBuff = blockSizeBytes / chunkSizeBytes;  // Should be divisible
    oneSidedBuffLkeys.resize(qpNum);
}

Status RoceReceiver::Initialize(TCPObjectClient *client, TCPObjectServer *server)
{
    int32_t visibleDevId = 0;
    uint32_t phyId = 0;
    uint32_t listenPortRangeStart = 0;
    uint32_t listenPortRangeEnd = 0;
    CHECK_STATUS(GetRocePortRange(listenPortRangeStart, listenPortRangeEnd));

    const int tagLength = 30;
    tag = std::string(tagLength, ' ');
    FillRandom(tag);

    ACL_CHECK_STATUS(rtGetVisibleDeviceIdByLogicDeviceId(recvDeviceId, &visibleDevId));
    ACL_CHECK_STATUS(rtGetDevicePhyIdByIndex(recvDeviceId, &phyId));

    CHECK_STATUS(RdmaDev::GetInstance(phyId, rdmaDev));
    union hccp_ip_addr ipv4Addr;
    CHECK_STATUS(rdmaDev->getIpv4(&ipv4Addr));

    rdmaSocket = std::make_unique<RdmaSocket>(phyId, ipv4Addr, SERVER);
    CHECK_STATUS(rdmaSocket->init());
    CHECK_STATUS(rdmaSocket->listenFirstAvailable(listenPortRangeStart, listenPortRangeEnd));
    SenderData senderData;
    if (isRoot) {
        CHECK_STATUS(server->ReceiveObject(senderData));
    } else {
        CHECK_STATUS(client->ReceiveObject(senderData));
    }

    uint32_to_in_addr(senderData.sendnpuipv4(), remoteIp.addr);

    CHECK_STATUS(rdmaSocket->addWhitelist(remoteIp, tag));

    unsigned int listenPort;
    CHECK_STATUS(rdmaSocket->getListenPort(listenPort));

    ReceiverData receiverData;
    receiverData.set_recvnpuipv4(in_addr_to_uint32(ipv4Addr.addr));
    receiverData.set_recvlistenport(listenPort);
    receiverData.set_tag(tag);

    if (isRoot) {
        CHECK_STATUS(server->SendObject(receiverData));
    } else {
        CHECK_STATUS(client->SendObject(receiverData));
    }

    CHECK_STATUS(rdmaSocket->waitReady(0));

    void *rdmaHandle;
    void *fdHandle;
    CHECK_STATUS(rdmaDev->getRdmaHandle(&rdmaHandle));
    CHECK_STATUS(rdmaSocket->getFdHandle(&fdHandle));

    valueMem = std::make_unique<NotifyValueMem>();
    CHECK_STATUS(valueMem->alloc());
    CHECK_STATUS(valueMem->get(&notifySrcValAddr, &notifySize));

    for (int i = 0; i < nRecvBuffs; i++) {
        std::unique_ptr<P2PMem> mem = std::make_unique<P2PMem>();
        CHECK_STATUS(mem->alloc(blockSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST_P2P));
        recvBuffs.push_back(std::move(mem));
    }

    for (int i = 0; i < nRecvBuffs * nChunksPerBuff; i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->create(recvDeviceId));
        sendDoneNotifies.push_back(std::move(notify));
    }

    for (int q = 0; q < qpNum; q++) {
        std::unique_ptr<RdmaQp> qp = std::make_unique<RdmaQp>();
        CHECK_STATUS(qp->create(rdmaHandle));

        CHECK_STATUS(qp->registerMemoryRegion(notifySrcValAddr, notifySize));

        for (int i = 0; i < nRecvBuffs; i++) {
            CHECK_STATUS(qp->registerMemoryRegion(recvBuffs[i]->get(), blockSizeBytes));
        }

        CHECK_STATUS(qp->connect(fdHandle));
        CHECK_STATUS(qp->waitReady(0));
        qps.push_back(std::move(qp));
    }

    CHECK_STATUS(rdmaSocket->close());

    ReceiverNpuResources receiverResources;
    for (int q = 0; q < qpNum; q++) {
        unsigned long long notifyBaseVa;
        CHECK_STATUS(qps[q]->getNotifyBaseAddress(&notifyBaseVa));
        receiverResources.add_qpbaseaddrs(static_cast<uint64_t>(notifyBaseVa));
        qpNotifyBaseVas.push_back(notifyBaseVa);
    }

    for (auto &notify : sendDoneNotifies) {
        uint64_t sendDoneNotifyAddrOffset;
        CHECK_STATUS(notify->getAddrOffset(&sendDoneNotifyAddrOffset));
        receiverResources.add_senddonenotifyaddroffsets(sendDoneNotifyAddrOffset);
    }

    for (auto &mem : recvBuffs) {
        receiverResources.add_recvbuffaddrs(reinterpret_cast<uint64_t>(mem->get()));
    }

    if (isRoot) {
        CHECK_STATUS(server->SendObject(receiverResources));
    } else {
        CHECK_STATUS(client->SendObject(receiverResources));
    }

    SenderNpuResources senderResources;
    if (isRoot) {
        CHECK_STATUS(server->ReceiveObject(senderResources));
    } else {
        CHECK_STATUS(client->ReceiveObject(senderResources));
    }

    for (int i = 0; i < qpNum; i++) {
        qpNotifyRemoteBaseVas.push_back(senderResources.qpbaseaddrs(i));
    }

    for (int i = 0; i < senderResources.recvreadynotifyaddroffsets_size(); i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->open(senderResources.recvreadynotifyaddroffsets(i)));
        recvReadyNotifies.push_back(std::move(notify));
    }

    recvCompleteNotify = std::make_unique<RdmaNotify>();
    CHECK_STATUS(recvCompleteNotify->open(senderResources.recvcompletenotifyaddroffset()));

    // One sided comm
    remotenotifySrcValAddr = senderResources.notifysrcvaladdr();
    for (int i = 0; i < nRecvBuffs; i++) {
        std::unique_ptr<P2PMem> mem = std::make_unique<P2PMem>();
        CHECK_STATUS(mem->alloc(blockSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST_P2P));
        for (int q = 0; q < qpNum; q++) {
            CHECK_STATUS(qps[q]->registerMemoryRegion(mem->get(), blockSizeBytes));

            struct mr_info mrInfo {};
            CHECK_STATUS(qps[q]->getMemoryRegionInfo(mem->get(), &mrInfo));
            oneSidedBuffLkeys[q].push_back(mrInfo.lkey);
        }

        oneSidedBuffs.push_back(std::move(mem));
    }

    for (int i = 0; i < nRecvBuffs * nChunksPerBuff; i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->create(recvDeviceId));
        uint64_t addrOffset;
        CHECK_STATUS(notify->getAddrOffset(&addrOffset));
        readChunkDoneNotifyAddrOffsets.push_back(addrOffset);
        readChunkDoneNotifies.push_back(std::move(notify));
    }

    for (int i = 0; i < nRecvBuffs; i++) {
        std::unique_ptr<P2PNotify> notify = std::make_unique<P2PNotify>();
        CHECK_STATUS(notify->create(recvDeviceId));
        uint64_t addr;
        CHECK_STATUS(notify->getAddr(&addr));
        blockAvailableNotifyAddr.push_back(addr);
        blockAvailableNotifies.push_back(std::move(notify));
    }

    writeOpFinishNotify = std::make_unique<RdmaNotify>();
    CHECK_STATUS(writeOpFinishNotify->create(recvDeviceId));

    state = RoceReceiverStatus::ROCE_RECEIVER_INITIALIZED;

    fftsDispatcher = std::make_unique<p2p::DispatcherFFTS>(recvDeviceId);
    NPU_ERROR(fftsDispatcher->Init());
    NPU_ERROR(fftsDispatcher->CreateFftsCtxs(1));
    NPU_ERROR(fftsDispatcher->SetFftsCtx(0));

    return Status::Success();
}

constexpr size_t PAGE_SIZE_BYTES = 4 * 1024;  // 4 KiB

#define USE_FFTS

#ifdef USE_FFTS
// Later check mask amount of commands in FFTS task and whether can get exceeded
Status RoceReceiver::ReceiveChunk(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream,
                                  uint32_t *lastTaskIds, uint32_t &lastTaskCount, bool isLast)
{
    uint32_t sendDoneTaskId = 0;
    uint32_t notifyId;
    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getId(&notifyId));
    fftsDispatcher->SignalWaitCrossChip(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->get(), &sendDoneTaskId,
                                        notifyId);

    for (int i = 0; i < lastTaskCount; i++) {
        fftsDispatcher->AddTaskDependency(lastTaskIds[i], sendDoneTaskId);
    }

    uint32_t recvCompleteTaskId;
    if (isLast) {
        uint64_t notifyDstAddr;
        CHECK_STATUS(recvCompleteNotify->getRecordInfo(&notifyDstAddr));
        notifyDstAddr += qpNotifyRemoteBaseVas[0];

        CHECK_STATUS(qps[0]->dispatchRdmaOpFfts(fftsDispatcher.get(), reinterpret_cast<uint64_t>(notifySrcValAddr),
                                                notifyDstAddr, notifySize, RA_OP_WRITE,
                                                RA_SEND_SIGNALED | RA_SEND_FENCE, &recvCompleteTaskId));
        fftsDispatcher->AddTaskDependency(sendDoneTaskId, recvCompleteTaskId);
    }

    uint32_t numLastMemcpyTaskIds = std::min(RECEIVER_MAX_PARALLEL_TASKS, count);
    uint32_t lastMemcpyTaskIds[numLastMemcpyTaskIds];
    uint64_t curChunkOffset = 0;
    for (int i = 0; i < count; i++) {
        void *chunkSrc = static_cast<void *>(static_cast<unsigned char *>(recvBuffs[curBuffer]->get())
                                             + curChunk * chunkSizeBytes + curChunkOffset);
        void *chunkDst = dstPtrs[i];
        size_t copySize = sizes[i];

        uint32_t memcpyTaskId = 0;
        fftsDispatcher->MemcpyAsync(chunkDst, chunkSrc, copySize, &memcpyTaskId);

        if (i < RECEIVER_MAX_PARALLEL_TASKS) {
            if (isLast) {
                fftsDispatcher->AddTaskDependency(recvCompleteTaskId, memcpyTaskId);
            } else {
                fftsDispatcher->AddTaskDependency(sendDoneTaskId, memcpyTaskId);
            }
        } else {
            fftsDispatcher->AddTaskDependency(lastMemcpyTaskIds[i % RECEIVER_MAX_PARALLEL_TASKS], memcpyTaskId);
        }

        curChunkOffset += RoundUp(copySize, PAGE_SIZE_BYTES);
        lastMemcpyTaskIds[i % RECEIVER_MAX_PARALLEL_TASKS] = memcpyTaskId;
    }

    if (curChunk == nChunksPerBuff - 1) {
        uint64_t notifyDstAddr;
        CHECK_STATUS(recvReadyNotifies[curBuffer]->getRecordInfo(&notifyDstAddr));
        notifyDstAddr += qpNotifyRemoteBaseVas[0];

        uint32_t rdmaNotifyTaskId = 0;
        CHECK_STATUS(qps[0]->dispatchRdmaOpFfts(fftsDispatcher.get(), reinterpret_cast<uint64_t>(notifySrcValAddr),
                                                notifyDstAddr, notifySize, RA_OP_WRITE,
                                                RA_SEND_SIGNALED | RA_SEND_FENCE, &rdmaNotifyTaskId));
        for (int i = 0; i < numLastMemcpyTaskIds; i++) {
            fftsDispatcher->AddTaskDependency(lastMemcpyTaskIds[i], rdmaNotifyTaskId);
        }

        lastTaskIds[0] = rdmaNotifyTaskId;
        lastTaskCount = 1;
        curBuffer = (curBuffer + 1) % nRecvBuffs;
    } else {
        for (int i = 0; i < numLastMemcpyTaskIds; i++) {
            lastTaskIds[i] = lastMemcpyTaskIds[i];
        }
        lastTaskCount = numLastMemcpyTaskIds;
    }

    curChunk = (curChunk + 1) % nChunksPerBuff;

    return Status::Success();
}
#else
Status RoceReceiver::ReceiveChunk(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream,
                                  uint32_t &lastTaskId, bool isLast)
{
    unsigned long long notifyBaseVa;
    CHECK_STATUS(qps[0]->getNotifyBaseAddress(&notifyBaseVa));
    uint64_t sendDoneNotifyAddrOffset;
    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getAddrOffset(&sendDoneNotifyAddrOffset));
    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->wait(stream));

    if (isLast) {
        CHECK_STATUS(recvCompleteNotify->record(qpNotifyRemoteBaseVas[0], reinterpret_cast<uint64_t>(notifySrcValAddr),
                                                notifySize, stream, qps[0].get()));
    }

    uint64_t curChunkOffset = 0;
    for (int i = 0; i < count; i++) {
        void *chunkSrc = static_cast<void *>(static_cast<unsigned char *>(recvBuffs[curBuffer]->get())
                                             + curChunk * chunkSizeBytes + curChunkOffset);
        void *chunkDst = dstPtrs[i];
        size_t copySize = sizes[i];

        ACL_CHECK_STATUS(aclrtMemcpyAsync(chunkDst, copySize, chunkSrc, copySize, ACL_MEMCPY_DEVICE_TO_DEVICE, stream));

        curChunkOffset += RoundUp(copySize, PAGE_SIZE_BYTES);
    }

    if (curChunk == nChunksPerBuff - 1) {
        CHECK_STATUS(recvReadyNotifies[curBuffer]->record(stream, qps[0].get()));
        curBuffer = (curBuffer + 1) % nRecvBuffs;
    }

    curChunk = (curChunk + 1) % nChunksPerBuff;

    return Status::Success();
}
#endif

Status RoceReceiver::Receive(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream)
{
    if (state != RoceReceiverStatus::ROCE_RECEIVER_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Receiver has not been initialized yet");
    }

    if (!started) {
        started = true;
        for (auto &notify : recvReadyNotifies) {
            CHECK_STATUS(notify->record(qpNotifyRemoteBaseVas[0], notifySrcValAddr, notifySize, stream, qps[0].get()));
        }
    }

    uint32_t lastTaskIds[RECEIVER_MAX_PARALLEL_TASKS];
    uint32_t lastTaskCount = 0;
    uint32_t dstIdx = 0;

    void *chunkDstPtrs[chunkSizeBytes / PAGE_SIZE_BYTES];
    size_t chunkCopySizes[chunkSizeBytes / PAGE_SIZE_BYTES];

    while (dstIdx < count) {
        uint64_t currentSize = sizes[dstIdx];  // size

        // Transfer is larger than a chunk and must be split across multiple chunks
        if (currentSize >= chunkSizeBytes) {
            uint32_t numChunks = (currentSize + chunkSizeBytes - 1) / chunkSizeBytes;
            for (uint64_t i = 0; i < numChunks; i++) {
                chunkDstPtrs[0] =
                    static_cast<void *>(static_cast<unsigned char *>(dstPtrs[dstIdx]) + i * chunkSizeBytes);
                chunkCopySizes[0] = (i < numChunks - 1) ? chunkSizeBytes : (currentSize - i * chunkSizeBytes);
                bool isLast = (dstIdx == count - 1) && i == (numChunks - 1);
                CHECK_STATUS(ReceiveChunk(chunkDstPtrs, chunkCopySizes, 1, stream, lastTaskIds, lastTaskCount, isLast));
            }
            dstIdx++;
        } else {
            // Transfer fits in a chunk, try to fit as many subsequent chunks in transfer as possible
            uint64_t chunkSize = 0;
            uint32_t numMerged = 0;

            for (uint32_t i = dstIdx; i < count; ++i) {
                if (sizes[i] > chunkSizeBytes) {
                    break;
                }

                uint64_t alignedSize = RoundUp(sizes[i], PAGE_SIZE_BYTES);
                if (chunkSize + alignedSize <= chunkSizeBytes) {
                    chunkDstPtrs[i - dstIdx] = dstPtrs[i];
                    chunkCopySizes[i - dstIdx] = sizes[i];
                    chunkSize += alignedSize;
                    numMerged++;
                } else {
                    break;
                }
            }
            bool isLast = (dstIdx + numMerged == count);
            CHECK_STATUS(
                ReceiveChunk(chunkDstPtrs, chunkCopySizes, numMerged, stream, lastTaskIds, lastTaskCount, isLast));
            dstIdx += numMerged;
        }
    }

// Might want to split more depending on amount of chunks
#ifdef USE_FFTS
    fftsDispatcher->LaunchFftsTask(stream, 1, 0);
    fftsDispatcher->ReuseCtx(0);
#endif

    return Status::Success();
}

// Note1: task IDs can be 0, add lastRdmaTaskCount
// Note2: currently assumes srcptr always same rkey
Status RoceReceiver::ReadChunk(void *srcPtr, uint64_t srcSize, void **dstPtrs, uint64_t *dstSizes, uint32_t count,
                               aclrtStream stream, uint32_t &lastRdmaTaskId, uint32_t &lastRdmaTaskCount,
                               uint32_t *lastSdmaTaskIds, uint32_t &lastSdmaTaskCount, uint32_t srcRkey, bool isLast)
{
    // Wait for block to become available
    uint32_t blockAvailableTaskId = 0;
    if (curOneSidedChunk == 0) {
        uint32_t notifyId;
        CHECK_STATUS(blockAvailableNotifies[curOneSidedBuffer]->getId(&notifyId));
        fftsDispatcher->SignalWaitCrossChip(blockAvailableNotifies[curOneSidedBuffer]->get(), &blockAvailableTaskId,
                                            notifyId);
        if (lastRdmaTaskCount > 0) {
            fftsDispatcher->AddTaskDependency(lastRdmaTaskId, blockAvailableTaskId);
        }
    }

    // Read remote memory and ring notify to know when read finished
    void *chunkMid = static_cast<void *>(static_cast<unsigned char *>(oneSidedBuffs[curOneSidedBuffer]->get())
                                         + curOneSidedChunk * chunkSizeBytes);

    uint32_t rdmaReadTaskId = 0;
    CHECK_STATUS(qps[curQp]->dispatchTypicalRdmaOpFfts(
        fftsDispatcher.get(), reinterpret_cast<uint64_t>(chunkMid), reinterpret_cast<uint64_t>(srcPtr), srcSize,
        RA_OP_READ, RA_SEND_SIGNALED, oneSidedBuffLkeys[curQp][curOneSidedBuffer], srcRkey, &rdmaReadTaskId));
    if (curOneSidedChunk == 0) {
        fftsDispatcher->AddTaskDependency(blockAvailableTaskId, rdmaReadTaskId);
    } else if (lastRdmaTaskCount > 0) {
        fftsDispatcher->AddTaskDependency(lastRdmaTaskId, rdmaReadTaskId);
    }

    uint32_t readChunkDoneNotifyTaskId = 0;
    CHECK_STATUS(qps[curQp]->dispatchRdmaOpFfts(
        fftsDispatcher.get(),
        qpNotifyBaseVas[curQp] + readChunkDoneNotifyAddrOffsets[curOneSidedBuffer * nChunksPerBuff + curOneSidedChunk],
        remotenotifySrcValAddr, notifySize, RA_OP_READ, RA_SEND_SIGNALED | RA_SEND_FENCE, &readChunkDoneNotifyTaskId));
    fftsDispatcher->AddTaskDependency(rdmaReadTaskId, readChunkDoneNotifyTaskId);

    uint32_t waitReadDoneTaskId = 0;
    uint32_t waitReadDoneNotifyId;
    CHECK_STATUS(
        readChunkDoneNotifies[curOneSidedBuffer * nChunksPerBuff + curOneSidedChunk]->getId(&waitReadDoneNotifyId));
    fftsDispatcher->SignalWaitCrossChip(
        readChunkDoneNotifies[curOneSidedBuffer * nChunksPerBuff + curOneSidedChunk]->get(), &waitReadDoneTaskId,
        waitReadDoneNotifyId);
    fftsDispatcher->AddTaskDependency(readChunkDoneNotifyTaskId, waitReadDoneTaskId);
    for (int i = 0; i < lastSdmaTaskCount; i++) {
        fftsDispatcher->AddTaskDependency(lastSdmaTaskIds[i], waitReadDoneTaskId);
    }

    uint32_t numLastMemcpyTaskIds = std::min(RECEIVER_MAX_PARALLEL_TASKS, count);
    uint32_t lastMemcpyTaskIds[numLastMemcpyTaskIds];
    uint64_t curChunkOffset = 0;
    for (int i = 0; i < count; i++) {
        void *chunkSrc = static_cast<void *>(static_cast<unsigned char *>(oneSidedBuffs[curOneSidedBuffer]->get())
                                             + curOneSidedChunk * chunkSizeBytes + curChunkOffset);
        void *chunkDst = dstPtrs[i];
        size_t copySize = dstSizes[i];

        uint32_t memcpyTaskId = 0;
        fftsDispatcher->MemcpyAsync(chunkDst, chunkSrc, copySize, &memcpyTaskId);

        if (i < RECEIVER_MAX_PARALLEL_TASKS) {
            fftsDispatcher->AddTaskDependency(waitReadDoneTaskId, memcpyTaskId);
        } else {
            fftsDispatcher->AddTaskDependency(lastMemcpyTaskIds[i % RECEIVER_MAX_PARALLEL_TASKS], memcpyTaskId);
        }

        curChunkOffset += copySize;
        lastMemcpyTaskIds[i % RECEIVER_MAX_PARALLEL_TASKS] = memcpyTaskId;
    }

    if (curOneSidedChunk == nChunksPerBuff - 1) {
        uint32_t blockAvailableNotifyTaskId = 0;
        ACL_CHECK_STATUS(fftsDispatcher->SignalRecordCrossChip(blockAvailableNotifies[curOneSidedBuffer]->get(),
                                                               &blockAvailableNotifyTaskId,
                                                               blockAvailableNotifyAddr[curOneSidedBuffer]));
        for (int i = 0; i < numLastMemcpyTaskIds; i++) {
            fftsDispatcher->AddTaskDependency(lastMemcpyTaskIds[i], blockAvailableNotifyTaskId);
        }
        lastSdmaTaskIds[0] = blockAvailableNotifyTaskId;
        lastSdmaTaskCount = 1;
        curOneSidedBuffer = (curOneSidedBuffer + 1) % nRecvBuffs;
    } else {
        for (int i = 0; i < numLastMemcpyTaskIds; i++) {
            lastSdmaTaskIds[i] = lastMemcpyTaskIds[i];
        }
        lastSdmaTaskCount = numLastMemcpyTaskIds;
    }

    curOneSidedChunk = (curOneSidedChunk + 1) % nChunksPerBuff;
    curQp = (curQp + 1) % qpNum;

    lastRdmaTaskId = readChunkDoneNotifyTaskId;
    lastRdmaTaskCount = 1;
    return Status::Success();
}

// Status RoceReceiver::Read(void *srcPtr, void **dstPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream)
Status RoceReceiver::Read(P2PIScatterEntry *entries, uint32_t batchSize, aclrtStream stream)
{
    if (state != RoceReceiverStatus::ROCE_RECEIVER_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Receiver has not been initialized yet");
    }

    if (!onesidedStarted) {
        onesidedStarted = true;
        for (auto &notify : blockAvailableNotifies) {
            CHECK_STATUS(notify->record(stream));
        }
    }

    uint32_t lastRdmaTaskId = 0;
    uint32_t lastRdmaTaskCount = 0;
    uint32_t lastSdmaTaskIds[RECEIVER_MAX_PARALLEL_TASKS];
    uint32_t lastSdmaTaskCount = 0;

    void *chunkDstPtrs[chunkSizeBytes / PAGE_SIZE_BYTES];
    size_t chunkCopySizes[chunkSizeBytes / PAGE_SIZE_BYTES];

    for (int b = 0; b < batchSize; b++) {
        void *srcPtr = entries[b].ddrBuf;
        void **dstPtrs = entries[b].dstBufs;
        uint64_t *sizes = entries[b].sizes;
        uint32_t count = entries[b].numEl;

        // Note: currently assumes srcPtr one rkey, which seems reasonable
        struct P2PSegmentHandle segmentHandle {};
        std::shared_ptr<RdmaDev> rdmaDev;
        CHECK_STATUS(RdmaDev::GetInstance(recvDeviceId, rdmaDev));
        CHECK_STATUS(rdmaDev->getRemoteSegment(srcPtr, remoteIp, segmentHandle));
        void *srcDevPtr = reinterpret_cast<void *>(
            reinterpret_cast<char *>(segmentHandle.devPtr)
            + (reinterpret_cast<char *>(srcPtr) - reinterpret_cast<char *>(segmentHandle.ddrPtr)));

        uint32_t dstIdx = 0;

        uint64_t curSrcOffset = 0;

        while (dstIdx < count) {
            uint64_t currentSize = sizes[dstIdx];  // size

            // Transfer is larger than a chunk and must be split across multiple chunks
            if (currentSize >= chunkSizeBytes) {
                uint32_t numChunks = (currentSize + chunkSizeBytes - 1) / chunkSizeBytes;
                for (uint64_t i = 0; i < numChunks; i++) {
                    void *chunkSrcPtr = static_cast<void *>(static_cast<unsigned char *>(srcDevPtr) + curSrcOffset);
                    chunkDstPtrs[0] =
                        static_cast<void *>(static_cast<unsigned char *>(dstPtrs[dstIdx]) + i * chunkSizeBytes);
                    uint64_t chunkCopySize = (i < numChunks - 1) ? chunkSizeBytes : (currentSize - i * chunkSizeBytes);
                    ;
                    chunkCopySizes[0] = chunkCopySize;
                    bool isLast = (dstIdx == count - 1) && i == (numChunks - 1);
                    CHECK_STATUS(ReadChunk(chunkSrcPtr, chunkCopySize, chunkDstPtrs, chunkCopySizes, 1, stream,
                                           lastRdmaTaskId, lastRdmaTaskCount, lastSdmaTaskIds, lastSdmaTaskCount,
                                           segmentHandle.rKey, isLast));
                    curSrcOffset += chunkCopySize;
                }
                dstIdx++;
            } else {
                // Transfer fits in a chunk, try to fit as many subsequent chunks in transfer as possible
                uint64_t chunkSize = 0;
                uint32_t numMerged = 0;

                for (uint32_t i = dstIdx; i < count; ++i) {
                    if (sizes[i] > chunkSizeBytes) {
                        break;
                    }

                    uint64_t size = sizes[i];

                    if (chunkSize + size <= chunkSizeBytes) {
                        chunkDstPtrs[i - dstIdx] = dstPtrs[i];
                        chunkCopySizes[i - dstIdx] = sizes[i];
                        chunkSize += size;
                        numMerged++;
                    } else {
                        break;
                    }
                }
                bool isLast = (dstIdx + numMerged == count);
                void *chunkSrcPtr = static_cast<void *>(static_cast<unsigned char *>(srcDevPtr) + curSrcOffset);
                CHECK_STATUS(ReadChunk(chunkSrcPtr, chunkSize, chunkDstPtrs, chunkCopySizes, numMerged, stream,
                                       lastRdmaTaskId, lastRdmaTaskCount, lastSdmaTaskIds, lastSdmaTaskCount,
                                       segmentHandle.rKey, isLast));
                curSrcOffset += chunkSize;  // NOTE: only correct if all copies are larger than PAGE_SIZE_BYTES
                dstIdx += numMerged;
            }
        }
    }

// Might want to split more depending on amount of chunks
#ifdef USE_FFTS
    ACL_CHECK_STATUS(fftsDispatcher->LaunchFftsTask(stream, 1, 0));
    ACL_CHECK_STATUS(fftsDispatcher->ReuseCtx(0));
#endif

    return Status::Success();
}
