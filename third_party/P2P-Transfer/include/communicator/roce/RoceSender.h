/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_ROCE_SENDER_H
#define P2P_ROCE_SENDER_H

#include "tools/Status.h"
#include "tools/npu-error.h"
#include "include/communicator/roce/proto/RoceInitMsg.pb.h"
#include "communication/TcpServer.h"
#include "communication/TcpClient.h"
#include "npu/Hccp.h"
#include "npu/RdmaAgent.h"
#include "npu/RdmaSocket.h"
#include "npu/RdmaQp.h"
#include "npu/RdmaNotify.h"
#include "npu/LocalNotify.h"
#include "npu/P2PMem.h"
#include "npu/P2PStream.h"
#include "communicator/CommChannel.h"
#include "npu/ffts/dispatcher_ffts.h"

constexpr uint32_t SENDER_MAX_PARALLEL_TASKS = 8;

enum RoceSenderStatus {
    ROCE_SENDER_UNINITIALIZED,
    ROCE_SENDER_INITIALIZED,
};

class RoceSender : public SendChannel {
public:
    RoceSender(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes, uint32_t nSendBuffs);

    Status Initialize(TCPObjectClient *client, TCPObjectServer *server) override;
    Status Send(void **srcPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream) override;

private:
    uint32_t MergeTransfersIntoChunk(void **chunkSrcPtrs, size_t *chunkCopySizes, void **srcPtrs, uint64_t *sizes,
                                     uint32_t srcIdx, uint32_t count);
    Status SendChunk(void **srcPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream, uint32_t &lastTaskId,
                     bool isLast);
    TCPObjectClient *client;
    TCPObjectServer *server;

    uint32_t blockSizeBytes;  // bytes
    uint32_t chunkSizeBytes;  // bytes
    uint32_t nSendBuffs;
    uint32_t nChunksPerBuff;

    std::string tag;
    int32_t sendDeviceId;

    std::unique_ptr<Hccp> hccp;
    std::unique_ptr<RdmaAgent> rdmaAgent;
    std::unique_ptr<RdmaSocket> rdmaSocket;
    std::unique_ptr<RdmaQp> qp;
    std::unique_ptr<p2p::DispatcherFFTS> fftsDispatcher;

    std::vector<uint64_t> remoteRecvBuffAddrs;
    std::vector<std::unique_ptr<P2PMem>> sendBuffs;

    std::vector<std::unique_ptr<LocalNotify>> sendReadyNotifies;
    std::vector<std::unique_ptr<RdmaNotify>> sendDoneNotifies;
    std::vector<std::unique_ptr<RdmaNotify>> recvReadyNotifies;
    std::unique_ptr<RdmaNotify> recvCompleteNotify;

    RoceSenderStatus state = ROCE_SENDER_UNINITIALIZED;
    bool isRoot;

    uint32_t curChunk = 0;
    uint32_t curBuffer = 0;
};

#endif  // P2P_ROCE_SENDER_H