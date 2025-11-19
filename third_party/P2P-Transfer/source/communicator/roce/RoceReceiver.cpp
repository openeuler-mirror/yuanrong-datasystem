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
                           uint32_t nRecvBuffs)
    : recvDeviceId(deviceId),
      isRoot(isRoot),
      blockSizeBytes(blockSizeBytes),
      chunkSizeBytes(chunkSizeBytes),
      nRecvBuffs(nRecvBuffs)
{
    nChunksPerBuff = blockSizeBytes / chunkSizeBytes;  // Should be divisible
}

Status RoceReceiver::Initialize(TCPObjectClient *client, TCPObjectServer *server)
{
    int32_t visibleDevId = 0;
    uint32_t phyId = 0;
    uint32_t listenPortRangeStart = 0;
    uint32_t listenPortRangeEnd = 0;
    CHECK_STATUS(GetRocePortRange(listenPortRangeStart, listenPortRangeEnd));

    tag = "2DG0E8/W*1HjhYj};LZ-2FtcYreCri";

    ACL_CHECK_STATUS(rtGetVisibleDeviceIdByLogicDeviceId(recvDeviceId, &visibleDevId));
    ACL_CHECK_STATUS(rtGetDevicePhyIdByIndex(recvDeviceId, &phyId));

    hccp = std::make_unique<Hccp>(visibleDevId);
    CHECK_STATUS(hccp->start());

    rdmaAgent = std::make_unique<RdmaAgent>(phyId);
    CHECK_STATUS(rdmaAgent->init());

    union hccp_ip_addr ipv4Addr;
    CHECK_STATUS(rdmaAgent->getDeviceIpv4(&ipv4Addr));

    rdmaSocket = std::make_unique<RdmaSocket>(phyId, ipv4Addr, SERVER);
    CHECK_STATUS(rdmaSocket->init());
    CHECK_STATUS(rdmaSocket->listenFirstAvailable(listenPortRangeStart, listenPortRangeEnd));
    SenderData senderData;
    if (isRoot) {
        CHECK_STATUS(server->ReceiveObject(senderData));
    } else {
        CHECK_STATUS(client->ReceiveObject(senderData));
    }

    union hccp_ip_addr remoteIp {
    };
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
    CHECK_STATUS(rdmaSocket->getRdmaHandle(&rdmaHandle));
    CHECK_STATUS(rdmaSocket->getFdHandle(&fdHandle));

    qp = std::make_unique<RdmaQp>();
    CHECK_STATUS(qp->create(rdmaHandle));

    for (int i = 0; i < nRecvBuffs; i++) {
        std::unique_ptr<P2PMem> mem = std::make_unique<P2PMem>();
        CHECK_STATUS(mem->alloc(blockSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST_P2P));
        qp->registerMemoryRegion(mem->get(), blockSizeBytes);
        recvBuffs.push_back(std::move(mem));
    }

    for (int i = 0; i < nRecvBuffs * nChunksPerBuff; i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->create(recvDeviceId));
        sendDoneNotifies.push_back(std::move(notify));
    }

    CHECK_STATUS(qp->connect(fdHandle));
    CHECK_STATUS(qp->waitReady(0));
    CHECK_STATUS(rdmaSocket->close());

    // Get notify addresses
    unsigned long long notifyBaseVa;
    CHECK_STATUS(qp->getNotifyBaseAddress(&notifyBaseVa));

    ReceiverNpuResources receiverResources;
    for (auto &notify : sendDoneNotifies) {
        uint64_t sendDoneNotifyAddrOffset;
        CHECK_STATUS(notify->getAddrOffset(&sendDoneNotifyAddrOffset));
        receiverResources.add_senddonenotifyaddrs(static_cast<uint64_t>(notifyBaseVa) + sendDoneNotifyAddrOffset);
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

    for (int i = 0; i < senderResources.recvreadynotifyaddrs_size(); i++) {
        std::unique_ptr<RdmaNotify> notify = std::make_unique<RdmaNotify>();
        CHECK_STATUS(notify->open(senderResources.recvreadynotifyaddrs(i)));
        recvReadyNotifies.push_back(std::move(notify));
    }

    recvCompleteNotify = std::make_unique<RdmaNotify>();
    CHECK_STATUS(recvCompleteNotify->open(senderResources.recvcompletenotifyaddr()));

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
        uint32_t notifyId;
        uint64_t notifySrcAddr;
        uint64_t notifyDstAddr;
        uint32_t notifyWriteLength;
        CHECK_STATUS(recvCompleteNotify->getRecordInfo(qp.get(), &notifySrcAddr, &notifyDstAddr, &notifyWriteLength));

        CHECK_STATUS(qp->rdmaWriteFfts(fftsDispatcher.get(), notifySrcAddr, notifyDstAddr, notifyWriteLength,
                                       &recvCompleteTaskId));
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
        uint64_t notifySrcAddr;
        uint64_t notifyDstAddr;
        uint32_t notifyWriteLength;
        CHECK_STATUS(
            recvReadyNotifies[curBuffer]->getRecordInfo(qp.get(), &notifySrcAddr, &notifyDstAddr, &notifyWriteLength));

        uint32_t rdmaNotifyTaskId = 0;
        CHECK_STATUS(qp->rdmaWriteFfts(fftsDispatcher.get(), notifySrcAddr, notifyDstAddr, notifyWriteLength,
                                       &rdmaNotifyTaskId));
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
    CHECK_STATUS(qp->getNotifyBaseAddress(&notifyBaseVa));
    uint64_t sendDoneNotifyAddrOffset;
    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getAddrOffset(&sendDoneNotifyAddrOffset));
    CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->wait(stream));

    if (isLast) {
        CHECK_STATUS(recvCompleteNotify->record(stream, qp.get()));
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
        CHECK_STATUS(recvReadyNotifies[curBuffer]->record(stream, qp.get()));
        curBuffer = (curBuffer + 1) % nRecvBuffs;
    }

    curChunk = (curChunk + 1) % nChunksPerBuff;

    return Status::Success();
}
#endif

uint32_t RoceReceiver::MergeReceivesIntoChunk(void **chunkDstPtrs, size_t *chunkCopySizes, void **dstPtrs,
                                              uint64_t *sizes, uint32_t dstIdx, uint32_t count)
{
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

    return numMerged;
}

Status RoceReceiver::Receive(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream)
{
    if (state != RoceReceiverStatus::ROCE_RECEIVER_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Receiver has not been initialized yet");
    }

    if (!started) {
        started = true;
        for (auto &notify : recvReadyNotifies) {
            CHECK_STATUS(notify->record(stream, qp.get()));
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
        } else {  // Transfer fits in a chunk, try to fit as many subsequent chunks in transfer as possible
            uint32_t numMerged = MergeReceivesIntoChunk(
                chunkDstPtrs, chunkCopySizes, dstPtrs, sizes, dstIdx, count
            );
            bool isLast = (dstIdx + numMerged == count);
            CHECK_STATUS(
                ReceiveChunk(chunkDstPtrs, chunkCopySizes, numMerged, stream, lastTaskIds, lastTaskCount, isLast));
            dstIdx += numMerged;
        }
    }

#ifdef USE_FFTS
    fftsDispatcher->LaunchFftsTask(stream, 1, 0);
    fftsDispatcher->ReuseCtx(0);
#endif

    return Status::Success();
}