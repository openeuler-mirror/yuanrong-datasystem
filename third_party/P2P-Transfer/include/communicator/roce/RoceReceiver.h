/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_ROCE_RECEIVER_H
#define P2P_ROCE_RECEIVER_H

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
#include "npu/P2PMem.h"
#include "communicator/CommChannel.h"
#include "npu/ffts/dispatcher_ffts.h"

constexpr uint32_t RECEIVER_MAX_PARALLEL_TASKS = 8;

enum RoceReceiverStatus {
    ROCE_RECEIVER_UNINITIALIZED,
    ROCE_RECEIVER_INITIALIZED,
};

class RoceReceiver : public ReceiveChannel {
public:
    RoceReceiver(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes, uint32_t nRecvBuffs);

    Status Initialize(TCPObjectClient *client, TCPObjectServer *server) override;
    Status Receive(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream) override;

private:
    uint32_t MergeReceivesIntoChunk(void **chunkDstPtrs, size_t *chunkCopySizes, void **dstPtrs, uint64_t *sizes,
                                    uint32_t dstIdx, uint32_t count);
    Status ReceiveChunk(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream, uint32_t *lastTaskIds,
                        uint32_t &lastTaskCount, bool isLast);

    TCPObjectClient *client;
    TCPObjectServer *server;

    uint32_t blockSizeBytes;  // bytes
    uint32_t chunkSizeBytes;  // byte
    uint32_t nRecvBuffs;
    uint32_t nChunksPerBuff;

    int32_t recvDeviceId;
    std::string tag;

    std::unique_ptr<Hccp> hccp;
    std::unique_ptr<RdmaAgent> rdmaAgent;
    std::unique_ptr<RdmaSocket> rdmaSocket;
    std::unique_ptr<RdmaQp> qp;
    std::unique_ptr<p2p::DispatcherFFTS> fftsDispatcher;

    std::vector<std::unique_ptr<P2PMem>> recvBuffs;
    std::vector<std::unique_ptr<RdmaNotify>> sendDoneNotifies;
    std::vector<std::unique_ptr<RdmaNotify>> recvReadyNotifies;
    std::unique_ptr<RdmaNotify> recvCompleteNotify;

    RoceReceiverStatus state = ROCE_RECEIVER_UNINITIALIZED;
    bool isRoot;
    bool started = false;

    uint32_t curChunk = 0;
    uint32_t curBuffer = 0;
};

#endif  // P2P_ROCE_RECEIVER_H