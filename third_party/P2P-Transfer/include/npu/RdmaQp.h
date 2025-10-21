/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RDMA_QP_H
#define P2P_RDMA_QP_H
#include <stddef.h>
#include <array>
#include <vector>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include <arpa/inet.h>
#include "npu/NotifyValueMem.h"
#include "external/ra.h"
#include "npu/ffts/dispatcher_ffts.h"
#include "npu/P2PStream.h"

enum RdmaQpStatus {
    QP_INITIALIZED = 0,
    QP_CONNECTING,
    QP_CONNECTED,
    QP_UNINITIALIZED
};

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
    Status connect(void *socketFdHandle);
    Status getStatus(ra_qp_status *qpStatus);
    Status waitReady(uint32_t timeOutMs);

    Status getNotifyBaseAddress(unsigned long long *notifyBaseAddr);
    Status getSrcValInfo(void **notifySrcValAddr, uint32_t *notifySize);
    Status rdmaWrite(uint64_t srcAddr, uint64_t dstAddr, uint32_t length, rtStream_t stm);
    Status rdmaWriteFfts(p2p::DispatcherFFTS *dispatcher, uint64_t srcAddr, uint64_t dstAddr, uint32_t length,
                         uint32_t *rdmaTaskId);
    Status rdmaWriteFfts(p2p::DispatcherFFTS *dispatcher, std::vector<uint64_t> srcAddrs,
                         std::vector<uint64_t> dstAddrs, std::vector<uint32_t> lengths);
    Status rdmaWrite(std::vector<uint64_t> srcAddrs, std::vector<uint64_t> dstAddrs, std::vector<uint32_t> lengths,
                     rtStream_t stm);

private:
    RdmaQpStatus status;
    void *qpHandle;

    unsigned long long notifyBaseVa = 0;
    void *notifySrcValAddr;
    uint32_t notifySize;

    std::vector<struct mr_info> registeredMrs;
};

#endif  // P2P_RDMA_QP_H