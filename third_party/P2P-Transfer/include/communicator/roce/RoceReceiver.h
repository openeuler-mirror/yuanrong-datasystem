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
#include "npu/RdmaAgent.h"
#include "npu/RdmaDev.h"
#include "npu/RdmaSocket.h"
#include "npu/RdmaQp.h"
#include "npu/RdmaNotify.h"
#include "npu/P2PMem.h"
#include "npu/P2PNotify.h"
#include "npu/NotifyValueMem.h"
#include "communicator/P2PCommunicator.h"
#include "communicator/CommChannel.h"
#include "npu/ffts/dispatcher_ffts.h"
#include "communicator/P2PCommunicatorManager.h"

constexpr uint32_t RECEIVER_MAX_PARALLEL_TASKS = 8;

enum RoceReceiverStatus {
    ROCE_RECEIVER_UNINITIALIZED,
    ROCE_RECEIVER_INITIALIZED,
};

class RoceReceiver : public ReceiveChannel {
public:
    RoceReceiver(int32_t deviceId, bool isRoot, uint32_t blockSizeBytes, uint32_t chunkSizeBytes, uint32_t nRecvBuffs,
                 uint32_t qpNum, bool enableTwoSidedBuffer, PingpongBufferPool *pingpongPool = nullptr);

    Status Initialize(TCPObjectClient *client, TCPObjectServer *server) override;
    Status Receive(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream) override;
    Status Read(P2PIScatterEntry *entries, uint32_t batchSize, aclrtStream stream) override;

private:
    Status ReceiveChunk(void **dstPtrs, uint64_t *sizes, uint32_t count, aclrtStream stream, uint32_t *lastTaskIds,
                        uint32_t &lastTaskCount, bool isLast);
    Status ReadChunk(void *srcPtr, uint64_t srcSize, void **dstPtrs, uint64_t *dstSizes, uint32_t count,
                     aclrtStream stream, uint32_t &lastRdmaTaskId, uint32_t &lastRdmaTaskCount,
                     uint32_t *lastSdmaTaskIds, uint32_t &lastSdmaTaskCount, uint32_t srcRkey, bool isLast,
                     bool isFirst);

    TCPObjectClient *client;
    TCPObjectServer *server;

    uint32_t blockSizeBytes;  // bytes
    uint32_t chunkSizeBytes;  // byte
    uint32_t nRecvBuffs;
    uint32_t nChunksPerBuff;

    uint32_t curQp = 0;
    uint32_t qpNum;

    bool enableTwoSidedBuffer;
    PingpongBufferPool *pingpongPool;

    int32_t recvDeviceId;
    std::string tag;

    std::shared_ptr<RdmaDev> rdmaDev;
    std::unique_ptr<RdmaSocket> rdmaSocket;
    std::vector<std::unique_ptr<RdmaQp>> qps;
    std::vector<uint64_t> qpNotifyBaseVas;
    std::vector<uint64_t> qpNotifyRemoteBaseVas;
    std::unique_ptr<NotifyValueMem> valueMem;
    std::unique_ptr<p2p::DispatcherFFTS> fftsDispatcher;

    std::vector<std::unique_ptr<P2PMem>> recvBuffs;
    std::vector<std::unique_ptr<RdmaNotify>> sendDoneNotifies;
    std::vector<std::unique_ptr<RdmaNotify>> recvReadyNotifies;
    std::unique_ptr<RdmaNotify> recvCompleteNotify;

    void *notifySrcValAddr;
    uint32_t notifySize;

    // Unify implementation with Send/Recv, later reuse comm buffer across comms
    bool onesidedStarted = false;
    uint64_t remotenotifySrcValAddr;
    std::vector<std::shared_ptr<P2PMem>> oneSidedBuffs;
    std::vector<std::vector<uint32_t>> oneSidedBuffLkeys;
    std::vector<std::unique_ptr<RdmaNotify>> readChunkDoneNotifies;
    std::vector<uint64_t> readChunkDoneNotifyAddrOffsets;
    std::vector<std::unique_ptr<P2PNotify>> blockAvailableNotifies;  // record first time so usable
    std::vector<uint64_t> blockAvailableNotifyAddr;
    std::unique_ptr<RdmaNotify> writeOpFinishNotify;
    union hccp_ip_addr remoteIp;

    RoceReceiverStatus state = ROCE_RECEIVER_UNINITIALIZED;
    bool isRoot;
    bool started = false;

    uint32_t curChunk = 0;
    uint32_t curBuffer = 0;
};

#endif  // P2P_ROCE_RECEIVER_H