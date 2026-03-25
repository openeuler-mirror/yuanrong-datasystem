/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RDMA_NOTIFY_H
#define P2P_RDMA_NOTIFY_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include "npu/RdmaQp.h"

enum RdmaNotifyType { LOCAL_RDMA_NOTIFY = 0, REMOTE_RDMA_NOTIFY, UNINITIALIZED_RDMA_NOTIFY };

class RdmaNotify {
public:
    RdmaNotify() : type(RdmaNotifyType::UNINITIALIZED_RDMA_NOTIFY), isIdSet(false)
    {
    }
    ~RdmaNotify();

    RdmaNotify(const RdmaNotify &) = delete;
    RdmaNotify &operator=(const RdmaNotify &) = delete;

    void *get();

    Status create(uint32_t deviceId);
    Status getAddrOffset(uint64_t *notifyAddrOffset);
    Status wait(aclrtStream stream);

    Status open(uint64_t notifyRemoteAddrOffset);
    Status record(uint64_t notifyBaseAddr, void *notifySrcValAddr, uint32_t notifySize, aclrtStream stream, RdmaQp *qp);
    Status getRecordInfo(uint64_t *dstAddr);
    Status getId(uint32_t *notifyID);

private:
    RdmaNotifyType type;
    rtNotify_t notify = nullptr;

    bool isIdSet;
    uint32_t notifyID;

    uint64_t notifyAddrOffset;
};

#endif  // P2P_RDMA_NOTIFY_H