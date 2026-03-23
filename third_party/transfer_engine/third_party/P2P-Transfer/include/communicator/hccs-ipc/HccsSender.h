/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_HCCS_SENDER_H
#define P2P_HCCS_SENDER_H

#include "tools/Status.h"
#include "tools/npu-error.h"
#include "npu/P2PMem.h"
#include "npu/P2PNotify.h"
#include "npu/PeerManager.h"
#include "include/communicator/hccs-ipc/proto/HccsIpcInitMsg.pb.h"
#include "communication/TcpServer.h"
#include "communication/TcpClient.h"
#include "communicator/CommChannel.h"
#include "npu/ffts/dispatcher_ffts.h"

enum SenderStatus {
    SENDER_UNINITIALIZED,
    SENDER_INIT1_DONE,
    SENDER_INIT2_DONE,
    SENDER_INITIALIZED,
};

class HccsSender : public SendChannel {
public:
    HccsSender(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes, uint32_t nRecvBuffs);

    Status Initialize(TCPObjectClient *client, TCPObjectServer *server);
    Status Send(void **srcPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream);

private:
    Status initializeRootSender();
    Status initializeClientSender();
    Status rootInitStage1();
    Status clientInitStage1();
    Status rootInitStage2(SendRootStage2Data &stage2Data);
    Status clientInitStage2(SendClientStage2Data &stage2Data);
    Status rootInitStage3(SendRootStage3Data &stage3Data);
    Status clientInitStage3(InitEndMsg &endMsg);

    Status setPid();
    Status createRecvReadyNotifies();
    Status enablePeerAccess(uint32_t deviceId, uint32_t pid);
    Status openRecvBuffs(std::vector<std::array<char, MEM_NAME_LENGTH>> memNames);
    Status openSendDoneNotifies(std::vector<std::array<char, NOTIFY_NAME_LENGTH>> notifyNames);
    void addRecvReadyNames(std::vector<std::array<char, NOTIFY_NAME_LENGTH>> &dest);

    TCPObjectClient *client;
    TCPObjectServer *server;

    uint32_t blockSizeBytes;  // bytes
    uint32_t chunkSizeBytes;  // bytes
    uint32_t nRecvBuffs;
    uint32_t nChunksPerBuff;

    int32_t sendDeviceId;
    uint32_t sendPid;

    std::unique_ptr<p2p::DispatcherFFTS> fftsDispatcher;

    std::unique_ptr<PeerManager> peerManager;

    std::vector<std::unique_ptr<P2PMem>> recvBuffs;
    std::vector<std::unique_ptr<P2PNotify>> sendDoneNotifies;
    std::vector<std::unique_ptr<P2PNotify>> recvReadyNotifies;

    SenderStatus state = SENDER_UNINITIALIZED;
    bool isRoot;

    uint32_t curChunk = 0;
    uint32_t curBuffer = 0;
};

#endif  // P2P_HCCS_SENDER_H