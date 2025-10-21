/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "communicator/hccs-ipc/HccsReceiver.h"
#include "tools/npu-error.h"
#include "securec.h"
#include "runtime/dev.h"
#include <thread>

HccsReceiver::HccsReceiver(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes,
                           uint32_t nRecvBuffs)
    : recvDeviceId(deviceId),
      isRoot(isRoot),
      blockSizeBytes(blockSizeBytes),
      chunkSizeBytes(chunkSizeBytes),
      nRecvBuffs(nRecvBuffs)
{
    nChunksPerBuff = blockSizeBytes / chunkSizeBytes;  // Should be divisible
}

Status HccsReceiver::Initialize(TCPObjectClient *client, TCPObjectServer *server)
{
    if (isRoot) {
        this->server = server;
        CHECK_STATUS(initializeRootReceiver());
    } else {
        this->client = client;
        CHECK_STATUS(initializeClientReceiver());
    }

    fftsDispatcher = std::make_unique<p2p::DispatcherFFTS>(recvDeviceId);
    NPU_ERROR(fftsDispatcher->Init());
    NPU_ERROR(fftsDispatcher->CreateFftsCtxs(1));
    NPU_ERROR(fftsDispatcher->SetFftsCtx(0));

    return Status::Success();
}

Status HccsReceiver::Receive(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream)
{
    if (state != ReceiverStatus::RECEIVER_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Receiver has not been initialized yet");
    }

    if (!started) {
        started = true;
        for (auto &notify : recvReadyNotifies) {
            CHECK_STATUS(notify->record(stream));
        }
    }

    if (count > 1) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "HCCS currently doesn't support batch transfer");
    }

    void *dst = dstPtrs[0];
    uint64_t size = sizes[0];

    uint32_t lastTaskId = 0;

    uint64_t nChunks = (size + chunkSizeBytes - 1) / chunkSizeBytes;  // ceil
    for (uint64_t i = 0; i < nChunks; i++) {
        uint32_t sendDoneTaskId = 0;
        uint32_t notifyId;
        CHECK_STATUS(sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->getId(&notifyId));
        ACL_CHECK_STATUS(fftsDispatcher->SignalWaitCrossChip(
            sendDoneNotifies[curBuffer * nChunksPerBuff + curChunk]->get(), &sendDoneTaskId, notifyId));

        if (lastTaskId > 0) {
            ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(lastTaskId, sendDoneTaskId));
        }

        void *chunkSrc =
            static_cast<void *>(static_cast<unsigned char *>(recvBuffs[curBuffer]->get()) + curChunk * chunkSizeBytes);
        void *chunkDst = static_cast<void *>(static_cast<unsigned char *>(dst) + i * chunkSizeBytes);
        size_t copySize = (i < nChunks - 1) ? chunkSizeBytes : (size - i * chunkSizeBytes);

        uint32_t memcpyTaskId = 0;
        ACL_CHECK_STATUS(fftsDispatcher->MemcpyAsync(chunkDst, chunkSrc, copySize, &memcpyTaskId));
        ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(sendDoneTaskId, memcpyTaskId));

        if (curChunk == nChunksPerBuff - 1) {
            uint32_t recvReadyTaskId = 0;
            uint64_t notifyAddr;
            CHECK_STATUS(recvReadyNotifies[curBuffer]->getAddr(&notifyAddr));
            ACL_CHECK_STATUS(fftsDispatcher->SignalRecordCrossChip(recvReadyNotifies[curBuffer]->get(),
                                                                   &recvReadyTaskId, notifyAddr));
            ACL_CHECK_STATUS(fftsDispatcher->AddTaskDependency(memcpyTaskId, recvReadyTaskId));
            lastTaskId = recvReadyTaskId;
            curBuffer = (curBuffer + 1) % nRecvBuffs;
        } else {
            lastTaskId = memcpyTaskId;
        }

        curChunk = (curChunk + 1) % nChunksPerBuff;
    }

    ACL_CHECK_STATUS(fftsDispatcher->LaunchFftsTask(stream, 1, 0));
    ACL_CHECK_STATUS(fftsDispatcher->ReuseCtx(0));

    return Status::Success();
}

Status HccsReceiver::initializeRootReceiver()
{
    Status status;
    RecvRootStage2Data stage2Data;
    RecvRootStage3Data stage3Data;
    SendClientStage2Data stage2msg;
    InitEndMsg endMsg;

    while (state != ReceiverStatus::RECEIVER_INITIALIZED) {
        switch (state) {
            case ReceiverStatus::RECEIVER_UNINITIALIZED:
                status = rootInitStage1();
                state = ReceiverStatus::RECEIVER_INIT1_DONE;
                break;
            case ReceiverStatus::RECEIVER_INIT1_DONE:
                CHECK_STATUS(server->ReceiveObject(stage2Data));
                if (stage2Data.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + stage2Data.error_msg());
                }

                // Root initialization failed at stage 1, error out after receiving stage2Data
                SEND_MSG_ERROR(server, status, "Receiver client init failed at stage 1: ", stage2msg);
                status = rootInitStage2(stage2Data);
                SEND_MSG_ERROR(server, status, "Receiver client init failed at stage 2: ", stage2msg);

                stage2msg.set_recv_pid(recvPid);
                stage2msg.set_recv_dev_id(recvDeviceId);
                for (auto &notify : sendDoneNotifies) {
                    stage2msg.add_send_done_names(notify->getName().data(), NOTIFY_NAME_LENGTH);
                }
                for (auto &mem : recvBuffs) {
                    stage2msg.add_recv_buffer_names(mem->getName().data(), MEM_NAME_LENGTH);
                }
                CHECK_STATUS(server->SendObject(stage2msg));
                state = ReceiverStatus::RECEIVER_INIT2_DONE;
                break;
            case ReceiverStatus::RECEIVER_INIT2_DONE:
                CHECK_STATUS(server->ReceiveObject(stage3Data));
                if (stage3Data.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + stage3Data.error_msg());
                }

                status = rootInitStage3(stage3Data);
                SEND_MSG_ERROR(server, status, "Receiver client init failed at stage 3: ", endMsg);
                CHECK_STATUS(server->SendObject(endMsg));
                state = ReceiverStatus::RECEIVER_INITIALIZED;
                break;
        }
    }
    return Status::Success();
}

Status HccsReceiver::initializeClientReceiver()
{
    Status status;
    RecvClientStage2Data stage2Data;
    InitEndMsg endMsg;
    SendRootStage2Data stage2Msg;
    SendRootStage3Data stage3Msg;

    while (state != ReceiverStatus::RECEIVER_INITIALIZED) {
        switch (state) {
            case ReceiverStatus::RECEIVER_UNINITIALIZED:
                status = clientInitStage1();
                SEND_MSG_ERROR(client, status, "Receiver root init failed at stage 1: ", stage2Msg);

                stage2Msg.set_recv_pid(recvPid);
                stage2Msg.set_recv_dev_id(recvDeviceId);
                CHECK_STATUS(client->SendObject(stage2Msg));

                state = ReceiverStatus::RECEIVER_INIT1_DONE;
                break;
            case ReceiverStatus::RECEIVER_INIT1_DONE:
                CHECK_STATUS(client->ReceiveObject(stage2Data));
                if (stage2Data.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + stage2Data.error_msg());
                }

                status = clientInitStage2(stage2Data);
                SEND_MSG_ERROR(client, status, "Receiver root init failed at stage 2: ", stage3Msg);

                for (auto &notify : sendDoneNotifies) {
                    stage3Msg.add_send_done_names(notify->getName().data(), NOTIFY_NAME_LENGTH);
                }
                for (auto &mem : recvBuffs) {
                    stage3Msg.add_recv_buffer_names(mem->getName().data(), MEM_NAME_LENGTH);
                }
                CHECK_STATUS(client->SendObject(stage3Msg));

                state = ReceiverStatus::RECEIVER_INIT2_DONE;
                break;
            case ReceiverStatus::RECEIVER_INIT2_DONE:
                CHECK_STATUS(client->ReceiveObject(endMsg));
                if (endMsg.failed()) {
                    return Status::Error(ErrorCode::INTERNAL_ERROR, "Peer init failed " + endMsg.error_msg());
                }

                CHECK_STATUS(clientInitStage3(endMsg));
                state = ReceiverStatus::RECEIVER_INITIALIZED;
                break;
        }
    }
    return Status::Success();
}

Status HccsReceiver::rootInitStage1()
{
    CHECK_STATUS(setPid());
    CHECK_STATUS(createRecvBuffs());
    CHECK_STATUS(createSendDoneNotifies());

    return Status::Success();
}

Status HccsReceiver::clientInitStage1()
{
    CHECK_STATUS(setPid());
    CHECK_STATUS(createRecvBuffs());
    CHECK_STATUS(createSendDoneNotifies());
    return Status::Success();
}

Status HccsReceiver::rootInitStage2(RecvRootStage2Data &stage2Data)
{
    CHECK_STATUS(enablePeerAccess(stage2Data.send_dev_id(), stage2Data.send_pid()));
    return Status::Success();
}

Status HccsReceiver::clientInitStage2(RecvClientStage2Data &stage2Data)
{
    CHECK_STATUS(enablePeerAccess(stage2Data.send_dev_id(), stage2Data.send_pid()));

    std::vector<std::array<char, NOTIFY_NAME_LENGTH>> recvReadyNames;
    for (int i = 0; i < stage2Data.recv_ready_names_size(); i++) {
        std::array<char, NOTIFY_NAME_LENGTH> element;
        memcpy_s(element.data(), NOTIFY_NAME_LENGTH, stage2Data.recv_ready_names(i).data(), NOTIFY_NAME_LENGTH);
        recvReadyNames.push_back(element);
    }
    CHECK_STATUS(openRecvReadyNotifies(recvReadyNames));
    return Status::Success();
}

Status HccsReceiver::rootInitStage3(RecvRootStage3Data &stage3Data)
{
    std::vector<std::array<char, NOTIFY_NAME_LENGTH>> recvReadyNames;
    for (int i = 0; i < stage3Data.recv_ready_names_size(); i++) {
        std::array<char, NOTIFY_NAME_LENGTH> element;
        memcpy_s(element.data(), NOTIFY_NAME_LENGTH, stage3Data.recv_ready_names(i).data(), NOTIFY_NAME_LENGTH);
        recvReadyNames.push_back(element);
    }
    CHECK_STATUS(openRecvReadyNotifies(recvReadyNames));
    return Status::Success();
}

Status HccsReceiver::clientInitStage3(InitEndMsg &endMsg)
{
    return Status::Success();
}

Status HccsReceiver::setPid()
{
    ACL_CHECK_STATUS(rtDeviceGetBareTgid(&recvPid));
    return Status::Success();
}

Status HccsReceiver::createRecvBuffs()
{
    for (int i = 0; i < nRecvBuffs; i++) {
        std::unique_ptr<P2PMem> mem = std::make_unique<P2PMem>();
        CHECK_STATUS(mem->alloc(blockSizeBytes, ACL_MEM_MALLOC_HUGE_FIRST));
        recvBuffs.push_back(std::move(mem));
    }
    return Status::Success();
}

Status HccsReceiver::createSendDoneNotifies()
{
    for (int i = 0; i < nRecvBuffs * nChunksPerBuff; i++) {
        std::unique_ptr<P2PNotify> notify = std::make_unique<P2PNotify>();
        CHECK_STATUS(notify->create(recvDeviceId));
        sendDoneNotifies.push_back(std::move(notify));
    }
    return Status::Success();
}

Status HccsReceiver::enablePeerAccess(uint32_t deviceId, uint32_t pid)
{
    std::unique_ptr<PeerManager> pmg = std::make_unique<PeerManager>();
    pmg->allowAccess(deviceId);
    peerManager = std::move(pmg);

    for (auto &mem : recvBuffs) {
        mem->allowAccess(pid);
    }

    for (auto &notify : sendDoneNotifies) {
        notify->allowAccess(pid);
    }
    return Status::Success();
}

Status HccsReceiver::openRecvReadyNotifies(std::vector<std::array<char, NOTIFY_NAME_LENGTH>> notifyNames)
{
    for (auto notifyName : notifyNames) {
        std::unique_ptr<P2PNotify> notify = std::make_unique<P2PNotify>();
        CHECK_STATUS(notify->open(notifyName));
        recvReadyNotifies.push_back(std::move(notify));
    }
    return Status::Success();
}