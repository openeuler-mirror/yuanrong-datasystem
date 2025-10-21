/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/PeerManager.h"

PeerManager::~PeerManager()
{
    for (auto peerDeviceId : peers) {
        disableAccess(peerDeviceId);
    }
}

Status PeerManager::allowAccess(uint32_t peerDeviceId)
{
    if (peers.find(peerDeviceId) != peers.end()) {
        return Status::Error(ErrorCode::REPEAT_INITIALIZE, "Peer access to device already enabled");
    }

    ACL_CHECK_STATUS(aclrtDeviceEnablePeerAccess(peerDeviceId, 0));
    peers.insert(peerDeviceId);
    return Status::Success();
}

Status PeerManager::disableAccess(uint32_t peerDeviceId)
{
    if (peers.find(peerDeviceId) != peers.end()) {
        return Status::Error(ErrorCode::NOT_FOUND, "Device is not a peer");
    }

    peers.erase(peerDeviceId);
    ACL_CHECK_STATUS(aclrtDeviceDisablePeerAccess(peerDeviceId));
    return Status::Success();
}