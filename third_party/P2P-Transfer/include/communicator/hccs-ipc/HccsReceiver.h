/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_HCCS_RECEIVER_H
#define P2P_HCCS_RECEIVER_H

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

enum ReceiverStatus {
    RECEIVER_UNINITIALIZED,
    RECEIVER_INIT1_DONE,
    RECEIVER_INIT2_DONE,
    RECEIVER_INITIALIZED,
};

class HccsReceiver : public ReceiveChannel {
public:
    HccsReceiver(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes, uint32_t nRecvBuffs);

    Status Initialize(TCPObjectClient *client, TCPObjectServer *server) override;
    Status Receive(void **dstPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream) override;

private:
    Status initializeRootReceiver();
    Status initializeClientReceiver();
    Status rootInitStage1();
    Status clientInitStage1();
    Status rootInitStage2(RecvRootStage2Data &stage2Data);
    Status clientInitStage2(RecvClientStage2Data &stage2Data);
    Status rootInitStage3(RecvRootStage3Data &stage3Data);
    Status clientInitStage3(InitEndMsg &endMsg);

    Status setPid();
    Status createRecvBuffs();
    Status createSendDoneNotifies();
    Status enablePeerAccess(uint32_t deviceId, uint32_t pid);
    void addSendDoneNames(std::vector<std::array<char, NOTIFY_NAME_LENGTH>> &dest);
    void addRecvBufferNames(std::vector<std::array<char, MEM_NAME_LENGTH>> &dest);
    Status openRecvReadyNotifies(std::vector<std::array<char, NOTIFY_NAME_LENGTH>> notifyNames);

    TCPObjectClient *client;
    TCPObjectServer *server;

    uint32_t blockSizeBytes;  // bytes
    uint32_t chunkSizeBytes;  // byte
    uint32_t nRecvBuffs;
    uint32_t nChunksPerBuff;

    int32_t recvDeviceId;
    uint32_t recvPid;

    std::unique_ptr<p2p::DispatcherFFTS> fftsDispatcher;

    std::unique_ptr<PeerManager> peerManager;

    std::vector<std::unique_ptr<P2PMem>> recvBuffs;
    std::vector<std::unique_ptr<P2PNotify>> sendDoneNotifies;
    std::vector<std::unique_ptr<P2PNotify>> recvReadyNotifies;

    ReceiverStatus state = RECEIVER_UNINITIALIZED;
    bool isRoot;
    bool started = false;

    uint32_t curChunk = 0;
    uint32_t curBuffer = 0;
};

#endif  // P2P_HCCS_RECEIVER_H