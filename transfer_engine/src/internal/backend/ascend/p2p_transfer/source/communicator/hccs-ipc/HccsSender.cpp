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
#include "communicator/hccs-ipc/HccsSender.h"
#include "tools/npu-error.h"
#include "securec.h"
#include "runtime/dev.h"
#include <thread>

HccsSender::HccsSender(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes,
                       uint32_t nRecvBuffs)
    : sendDeviceId(deviceId),
      isRoot(isRoot),
      blockSizeBytes(blockSizeBytes),
      chunkSizeBytes(chunkSizeBytes),
      nRecvBuffs(nRecvBuffs)
{
    nChunksPerBuff = blockSizeBytes / chunkSizeBytes;  // Should be divisible
}

Status HccsSender::Initialize(TCPObjectClient *client, TCPObjectServer *server)
{
    if (isRoot) {
        this->server = server;
        CHECK_STATUS(initializeRootSender());
    } else {
        this->client = client;
        CHECK_STATUS(initializeClientSender());
    }

    fftsDispatcher = std::make_unique<p2p::DispatcherFFTS>(sendDeviceId);
    NPU_ERROR(fftsDispatcher->Init());
    NPU_ERROR(fftsDispatcher->CreateFftsCtxs(1));
    NPU_ERROR(fftsDispatcher->SetFftsCtx(0));

    return Status::Success();
}

Status HccsSender::Send(void **srcPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream)
{
    if (state != SenderStatus::SENDER_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Sender has not been initialized yet");
    }

    if (count > 1) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "HCCS currently doesn't support batch transfer");
    }

    void *src = srcPtrs[0];
    uint64_t size = sizes[0];

    uint32_t lastTaskId = 0;

    uint64_t nChunks = (size + chunkSizeBytes - 1) / chunkSizeBytes;  // ceil
    for (uint64_t i = 0; i < nChunks; i++) {
        uint32_t recvReadyTaskId = 0;
        if (curChunk == 0) {
            uint32_t notifyId;
            CHECK_STATUS(recvReadyNotifies[curBuffer]->getId(&notifyId));
            ACL_CHECK_STATUS(
                fftsDispatcher->SignalWaitCrossChip(recvReadyNotifies[curBuffer]->get(), &recvReadyTaskId, notifyId));
            if (lastTaskId > 0) {
                ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(lastTaskId, recvReadyTaskId));
            }
        }

        void *chunkSrc = static_cast<void *>(static_cast<unsigned char *>(src) + i * chunkSizeBytes);
        void *chunkDst =
            static_cast<void *>(static_cast<unsigned char *>(recvBuffs[curBuffer]->get()) + curChunk * chunkSizeBytes);

        size_t copySize = (i < nChunks - 1) ? chunkSizeBytes : (size - i * chunkSizeBytes);

        uint32_t memcpyTaskId = 0;
        ACL_CHECK_STATUS(fftsDispatcher->MemcpyAsync(chunkDst, chunkSrc, copySize, &memcpyTaskId));
        if (curChunk == 0) {
            ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(recvReadyTaskId, memcpyTaskId));
        } else if (lastTaskId > 0) {
            ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(lastTaskId, memcpyTaskId));
        }

        uint32_t notifyTaskId = 0;
        uint64_t notifyAddr;
        CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getAddr(&notifyAddr));
        ACL_CHECK_STATUS(fftsDispatcher->SignalRecordCrossChip(
            sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->get(), &notifyTaskId, notifyAddr));
        ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(memcpyTaskId, notifyTaskId));
        lastTaskId = notifyTaskId;

        if (curChunk == nChunksPerBuff - 1) {
            curBuffer = (curBuffer + 1) % nRecvBuffs;
        }
        curChunk = (curChunk + 1) % nChunksPerBuff;
    }

    ACL_CHECK_STATUS(fftsDispatcher->LaunchFftsTask(stream, 1, 0));
    ACL_CHECK_STATUS(fftsDispatcher->ReuseCtx(0));

    return Status::Success();
}

Status HccsSender::initializeRootSender()
{
    Status status;
    SendRootStage2Data stage2Data;
    SendRootStage3Data stage3Data;
    RecvClientStage2Data stage2msg;
    InitEndMsg endMsg;

    while (state != SenderStatus::SENDER_INITIALIZED) {
        switch (state) {
            case SenderStatus::SENDER_UNINITIALIZED:
                status = rootInitStage1();
                state = SenderStatus::SENDER_INIT1_DONE;
                break;
            case SenderStatus::SENDER_INIT1_DONE:
                CHECK_STATUS(server->ReceiveObject(stage2Data));
                if (stage2Data.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + stage2Data.error_msg());
                }

                // Root initialization failed at stage 1, error out after receiving stage2Data
                SEND_MSG_ERROR(server, status, "Sender root init failed at stage 1: ", stage2msg);
                status = rootInitStage2(stage2Data);
                SEND_MSG_ERROR(server, status, "Sender root init failed at stage 2: ", stage2msg);

                stage2msg.set_send_pid(sendPid);
                stage2msg.set_send_dev_id(sendDeviceId);
                for (auto &notify : recvReadyNotifies) {
                    stage2msg.add_recv_ready_names(notify->getName().data(), NOTIFY_NAME_LENGTH);
                }
                CHECK_STATUS(server->SendObject(stage2msg));
                state = SenderStatus::SENDER_INIT2_DONE;
                break;
            case SenderStatus::SENDER_INIT2_DONE:
                CHECK_STATUS(server->ReceiveObject(stage3Data));
                if (stage3Data.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + stage3Data.error_msg());
                }

                status = rootInitStage3(stage3Data);
                SEND_MSG_ERROR(server, status, "Sender root init failed at stage 3: ", endMsg);
                CHECK_STATUS(server->SendObject(endMsg));
                state = SenderStatus::SENDER_INITIALIZED;
                break;
        }
    }
    return Status::Success();
}

Status HccsSender::initializeClientSender()
{
    Status status;
    SendClientStage2Data stage2Data;
    InitEndMsg endMsg;
    RecvRootStage2Data stage2Msg;
    RecvRootStage3Data stage3Msg;

    while (state != SenderStatus::SENDER_INITIALIZED) {
        switch (state) {
            case SenderStatus::SENDER_UNINITIALIZED:
                status = clientInitStage1();
                SEND_MSG_ERROR(client, status, "Sender client init failed at stage 1: ", stage2Msg);

                stage2Msg.set_send_pid(sendPid);
                stage2Msg.set_send_dev_id(sendDeviceId);
                CHECK_STATUS(client->SendObject(stage2Msg));
                state = SenderStatus::SENDER_INIT1_DONE;
                break;
            case SenderStatus::SENDER_INIT1_DONE:
                CHECK_STATUS(client->ReceiveObject(stage2Data));
                if (stage2Data.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + stage2Data.error_msg());
                }
                status = clientInitStage2(stage2Data);
                SEND_MSG_ERROR(client, status, "Sender client init failed at stage 2: ", stage3Msg);
                for (auto &notify : recvReadyNotifies) {
                    stage3Msg.add_recv_ready_names(notify->getName().data(), NOTIFY_NAME_LENGTH);
                }
                CHECK_STATUS(client->SendObject(stage3Msg));
                state = SenderStatus::SENDER_INIT2_DONE;
                break;
            case SenderStatus::SENDER_INIT2_DONE:
                CHECK_STATUS(client->ReceiveObject(endMsg));
                if (endMsg.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + endMsg.error_msg());
                }
                CHECK_STATUS(clientInitStage3(endMsg));
                state = SenderStatus::SENDER_INITIALIZED;
                break;
        }
    }
    return Status::Success();
}

Status HccsSender::rootInitStage1()
{
    CHECK_STATUS(setPid());
    CHECK_STATUS(createRecvReadyNotifies());
    return Status::Success();
}

Status HccsSender::clientInitStage1()
{
    CHECK_STATUS(setPid());
    CHECK_STATUS(createRecvReadyNotifies());
    return Status::Success();
}

Status HccsSender::rootInitStage2(SendRootStage2Data &stage2Data)
{
    CHECK_STATUS(enablePeerAccess(stage2Data.recv_dev_id(), stage2Data.recv_pid()));
    return Status::Success();
}

Status HccsSender::clientInitStage2(SendClientStage2Data &stage2Data)
{
    CHECK_STATUS(enablePeerAccess(stage2Data.recv_dev_id(), stage2Data.recv_pid()));
    std::vector<std::array<char, MEM_NAME_LENGTH>> recvBufferNames;
    for (int i = 0; i < stage2Data.recv_buffer_names_size(); i++) {
        std::array<char, MEM_NAME_LENGTH> element;
        memcpy_s(element.data(), MEM_NAME_LENGTH, stage2Data.recv_buffer_names(i).data(), MEM_NAME_LENGTH);
        recvBufferNames.push_back(element);
    }
    CHECK_STATUS(openRecvBuffs(recvBufferNames));

    std::vector<std::array<char, MEM_NAME_LENGTH>> sendDoneNames;
    for (int i = 0; i < stage2Data.send_done_names_size(); i++) {
        std::array<char, NOTIFY_NAME_LENGTH> element;
        memcpy_s(element.data(), NOTIFY_NAME_LENGTH, stage2Data.send_done_names(i).data(), NOTIFY_NAME_LENGTH);
        sendDoneNames.push_back(element);
    }
    CHECK_STATUS(openSendDoneNotifies(sendDoneNames));
    return Status::Success();
}

Status HccsSender::rootInitStage3(SendRootStage3Data &stage3Data)
{
    std::vector<std::array<char, MEM_NAME_LENGTH>> recvBufferNames;
    for (int i = 0; i < stage3Data.recv_buffer_names_size(); i++) {
        std::array<char, MEM_NAME_LENGTH> element;
        memcpy_s(element.data(), MEM_NAME_LENGTH, stage3Data.recv_buffer_names(i).data(), MEM_NAME_LENGTH);
        recvBufferNames.push_back(element);
    }
    CHECK_STATUS(openRecvBuffs(recvBufferNames));

    std::vector<std::array<char, MEM_NAME_LENGTH>> sendDoneNames;
    for (int i = 0; i < stage3Data.send_done_names_size(); i++) {
        std::array<char, NOTIFY_NAME_LENGTH> element;
        memcpy_s(element.data(), MEM_NAME_LENGTH, stage3Data.send_done_names(i).data(), NOTIFY_NAME_LENGTH);
        sendDoneNames.push_back(element);
    }
    CHECK_STATUS(openSendDoneNotifies(sendDoneNames));
    return Status::Success();
}

Status HccsSender::clientInitStage3(InitEndMsg &endMsg)
{
    return Status::Success();
}

Status HccsSender::setPid()
{
    ACL_CHECK_STATUS(rtDeviceGetBareTgid(&sendPid));
    return Status::Success();
}

Status HccsSender::createRecvReadyNotifies()
{
    for (int i = 0; i < nRecvBuffs; i++) {
        std::unique_ptr<P2PNotify> notify = std::make_unique<P2PNotify>();
        CHECK_STATUS(notify->create(sendDeviceId));
        recvReadyNotifies.push_back(std::move(notify));
    }
    return Status::Success();
}

Status HccsSender::enablePeerAccess(uint32_t deviceId, uint32_t pid)
{
    std::unique_ptr<PeerManager> pmg = std::make_unique<PeerManager>();
    pmg->allowAccess(deviceId);
    peerManager = std::move(pmg);

    for (auto &notify : recvReadyNotifies) {
        notify->allowAccess(pid);
    }
    return Status::Success();
}

Status HccsSender::openRecvBuffs(std::vector<std::array<char, MEM_NAME_LENGTH>> memNames)
{
    for (auto memName : memNames) {
        std::unique_ptr<P2PMem> mem = std::make_unique<P2PMem>();
        LOG_STATUS(mem->open(memName));
        recvBuffs.push_back(std::move(mem));
    }
    return Status::Success();
}

Status HccsSender::openSendDoneNotifies(std::vector<std::array<char, NOTIFY_NAME_LENGTH>> notifyNames)
{
    for (auto notifyName : notifyNames) {
        std::unique_ptr<P2PNotify> notify = std::make_unique<P2PNotify>();
        CHECK_STATUS(notify->open(notifyName));
        sendDoneNotifies.push_back(std::move(notify));
    }
    return Status::Success();
}