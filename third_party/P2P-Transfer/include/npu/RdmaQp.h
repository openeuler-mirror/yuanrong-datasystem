/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RDMA_QP_H
#define P2P_RDMA_QP_H
#include <stddef.h>
#include <array>
#include <vector>
#include <unordered_map>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include <arpa/inet.h>
#include "npu/NotifyValueMem.h"
#include "external/ra.h"
#include "npu/ffts/dispatcher_ffts.h"
#include "npu/P2PStream.h"

enum RdmaQpStatus { QP_INITIALIZED = 0, QP_CONNECTING, QP_CONNECTED, QP_UNINITIALIZED };

class RdmaQp {
public:
    RdmaQp() : status(RdmaQpStatus::QP_UNINITIALIZED)
    {
    }

    ~RdmaQp();

    RdmaQp(const RdmaQp &) = delete;
    RdmaQp &operator=(const RdmaQp &) = delete;

    Status create(void *rdmaHandle);
    Status registerMemoryRegion(void *addr, uint32_t size);
    Status getMemoryRegionInfo(void *startAddr, struct mr_info *info);
    Status connect(void *socketFdHandle);
    Status getStatus(ra_qp_status *qpStatus);
    Status waitReady(uint32_t timeOutMs);

    Status getNotifyBaseAddress(unsigned long long *notifyBaseAddr);
    // Later clean these up
    Status execRdmaOp(uint64_t srcAddr, uint64_t dstAddr, uint32_t length, uint32_t op, int32_t flag, rtStream_t stm);
    Status dispatchRdmaOpFfts(p2p::DispatcherFFTS *dispatcher, uint64_t srcAddr, uint64_t dstAddr, uint32_t length,
                              uint32_t op, int32_t flag, uint32_t *rdmaTaskId);
    Status execRdmaOps(std::vector<uint64_t> srcAddrs, std::vector<uint64_t> dstAddrs, std::vector<uint32_t> lengths,
                       std::vector<uint32_t> ops, std::vector<int32_t> flags, rtStream_t stm);
    Status dispatchRdmaOpsFfts(p2p::DispatcherFFTS *dispatcher, std::vector<uint64_t> srcAddrs,
                               std::vector<uint64_t> dstAddrs, std::vector<uint32_t> lengths, std::vector<uint32_t> ops,
                               std::vector<int32_t> flags, uint32_t *lastTaskId);
    Status execTypicalRdmaOp(uint64_t srcAddr, uint64_t dstAddr, uint32_t length, uint32_t op, int32_t flag,
                             uint32_t lkey, uint32_t rkey, rtStream_t stm);
    Status dispatchTypicalRdmaOpFfts(p2p::DispatcherFFTS *dispatcher, uint64_t srcAddr, uint64_t dstAddr,
                                     uint32_t length, uint32_t op, int32_t flag, uint32_t lkey, uint32_t rkey,
                                     uint32_t *rdmaTaskId);

private:
    RdmaQpStatus status;
    void *qpHandle;

    unsigned long long notifyBaseVa = 0;

    std::unordered_map<void *, struct mr_info> registeredMrs;
};

#endif  // P2P_RDMA_QP_H