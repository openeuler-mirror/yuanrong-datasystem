
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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