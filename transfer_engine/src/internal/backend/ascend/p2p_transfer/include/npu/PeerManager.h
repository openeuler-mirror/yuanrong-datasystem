/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_PEER_H
#define P2P_PEER_H
#include <stddef.h>
#include <unordered_set>
#include "tools/Status.h"
#include "tools/npu-error.h"
#include "acl/acl.h"

class PeerManager {
public:
    PeerManager() {}
    ~PeerManager();

    PeerManager(const PeerManager &) = delete;
    PeerManager &operator=(const PeerManager &) = delete;

    // Allow the given device to access resources of the current device (memory, notifies, ...)
    Status allowAccess(uint32_t peerDeviceId);
    Status disableAccess(uint32_t peerDeviceId);

private:
    std::unordered_set<uint32_t> peers;
};

#endif  // P2P_PEER_H