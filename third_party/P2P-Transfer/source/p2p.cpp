
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

#include <string>
#include <cstring>
#include <mutex>
#include <memory>
#include <unordered_map>
#include "communicator/P2PCommunicator.h"
#include "communicator/P2PCommunicatorManager.h"
#include "tools/hccl-convert.h"
#include "tools/npu-error.h"
#include "securec.h"
#include "p2p.h"
#include "npu/Hccp.h"
#include "runtime/dev.h"

constexpr uint32_t P2P_NUM_PINGPONG_BUFF = 2;
constexpr uint32_t P2P_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;
constexpr uint32_t P2P_CHUNK_SIZE_BYTES = 2 * 1024 * 1024;

// Manages P2PCommunicators of the current process
P2PCommunicatorManager commManager;

std::mutex hccpMut;
std::unique_ptr<Hccp> hccp(nullptr);

HcclResult P2PGetRootInfo(HcclRootInfo *rootInfo)
{
    if (rootInfo == nullptr) {
        std::cerr << "[P2P] P2PGetRootInfo: rootInfo should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    memset_s(rootInfo->internal, HCCL_ROOT_INFO_BYTES, 0, HCCL_ROOT_INFO_BYTES);

    // Create root communicator
    std::shared_ptr<P2PCommunicator> p2pComm = std::make_shared<P2PCommunicator>(true);
    CHECK_STATUS_HCCL(p2pComm->StartRoot());

    // Write connection info to rootInfo
    P2PRootHandle rootHandle;
    CHECK_STATUS_HCCL(p2pComm->GetRootHandle(rootHandle));
    memcpy_s(rootInfo->internal, sizeof(rootHandle), &rootHandle, sizeof(rootHandle));

    // Store communicator as unassociated
    std::string identifier(rootHandle.identifier, ROOTHANDLE_INDENTIFIER_MAX_LENGTH);
    commManager.addUnboundRootComm(identifier, p2pComm);

    return HCCL_SUCCESS;
}

HcclResult PrewarmHccp()
{
    {
        std::lock_guard<std::mutex> lock(hccpMut);
        if (!hccp) {
            int32_t devId = 0;
            ACL_CHECK_HCCL(rtGetDevice(&devId));
            int32_t visibleDevId = 0;
            ACL_CHECK_HCCL(rtGetVisibleDeviceIdByLogicDeviceId(devId, &visibleDevId));
            hccp = std::make_unique<Hccp>(visibleDevId);
            CHECK_STATUS_HCCL(hccp->start());
        }
    }

    return HCCL_SUCCESS;
}

HcclResult UnwarmHccp()
{
    {
        std::lock_guard<std::mutex> lock(hccpMut);
        if (!hccp) {
            hccp.reset();
        }
    }

    return HCCL_SUCCESS;
}

HcclResult P2PCommInitRootInfo(const HcclRootInfo *rootInfo, P2pKind kind, P2pLink link, P2PComm *comm)
{
    if (rootInfo == nullptr) {
        std::cerr << "[P2P] P2PCommInitRootInfo: rootInfo is empty" << std::endl;
        return HCCL_E_PARA;
    }

    if (comm == nullptr) {
        std::cerr << "[P2P] P2PCommInitRootInfo: comm should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    if (kind != P2P_RECEIVER && kind != P2P_SENDER) {
        std::cerr << "[P2P] P2PCommInitRootInfo: P2P kind not supported " << kind << std::endl;
        return HCCL_E_PARA;
    }

    P2PRootHandle rootHandle;
    memcpy_s(&rootHandle, sizeof(rootHandle), rootInfo->internal, sizeof(rootHandle));
    std::string identifier(rootHandle.identifier, ROOTHANDLE_INDENTIFIER_MAX_LENGTH);

    // Get root communicator associated with identifier. If no root communicator is found, P2PCommInitRootInfo
    // was called on the client side and we need to create a new communicator to connect to the root communicator.
    std::shared_ptr<P2PCommunicator> p2pComm = commManager.getAndRemoveUnboundCommunicator(identifier);
    if (!p2pComm) {
        p2pComm = std::make_shared<P2PCommunicator>(false);
        CHECK_STATUS_HCCL(p2pComm->StartClient(rootHandle));
    }

    int32_t deviceId;
    P2PCommRole role;
    ACL_CHECK_HCCL(aclrtGetDevice(&deviceId));
    CHECK_STATUS_HCCL(p2pKindToCommRole(kind, role));

    P2PCommArgs args = { deviceId, link, role, P2P_NUM_PINGPONG_BUFF, P2P_BLOCK_SIZE_BYTES, P2P_CHUNK_SIZE_BYTES };

    // Establish connection between root and client communicators
    CHECK_STATUS_HCCL(p2pComm->EstablishConnection(args));

    P2PComm resComm = p2pComm.get();
    commManager.addCommunicator(resComm, p2pComm);

    *comm = resComm;

    return HCCL_SUCCESS;
}

HcclResult P2PCommDestroy(P2PComm comm)
{
    if (comm == nullptr) {
        std::cerr << "[P2P] P2PCommDestroy: comm should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    if (!commManager.removeCommunicator(comm)) {
        std::cerr << "[P2P] P2PCommDestroy: comm does not exist" << std::endl;
        return HCCL_E_NOT_FOUND;
    }

    return HCCL_SUCCESS;
}

HcclResult P2PSendBatch(void **sendBufs, uint64_t *counts, HcclDataType dataType, uint32_t batchSize, P2PComm comm,
                        aclrtStream stream)
{
    if (dataType >= HCCL_DATA_TYPE_RESERVED) {
        std::cerr << "[P2P] P2PSend: dataType unrecognized data type" << dataType << std::endl;
        return HCCL_E_PARA;
    }
    size_t typeSize = GetHcclDataSizeBytes(dataType);

    uint64_t sizeBytes[batchSize];
    for (int i = 0; i < batchSize; i++) {
        if (sendBufs[i] == nullptr) {
            std::cerr << "[P2P] P2PSend: sendBuffs[" << i << "] should not be null" << std::endl;
            return HCCL_E_PARA;
        }

        if (counts[i] == 0) {
            std::cerr << "[P2P] P2PSend: counts[" << i << "] should be larger than zero" << std::endl;
            return HCCL_E_PARA;
        }

        sizeBytes[i] = counts[i] * typeSize;
    }

    if (comm == nullptr) {
        std::cerr << "[P2P] P2PSend: comm should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    std::shared_ptr<P2PCommunicator> p2pComm = commManager.getCommunicator(comm);
    if (!p2pComm) {
        std::cerr << "[P2P] P2PSend: comm does not exist" << std::endl;
        return HCCL_E_NOT_FOUND;
    }

    CHECK_STATUS_HCCL(p2pComm->Send(sendBufs, sizeBytes, batchSize, stream));

    return HCCL_SUCCESS;
}

HcclResult P2PRecvBatch(void **recvBuffs, uint64_t *counts, HcclDataType dataType, uint32_t batchSize, P2PComm comm,
                        aclrtStream stream)
{
    if (dataType >= HCCL_DATA_TYPE_RESERVED) {
        std::cerr << "[P2P] P2PRecv: dataType unrecognized data type" << dataType << std::endl;
        return HCCL_E_PARA;
    }
    size_t typeSize = GetHcclDataSizeBytes(dataType);

    uint64_t sizeBytes[batchSize];
    for (int i = 0; i < batchSize; i++) {
        if (recvBuffs[i] == nullptr) {
            std::cerr << "[P2P] P2PRecv: sendBufs[" << i << "] should not be null" << std::endl;
            return HCCL_E_PARA;
        }

        if (counts[i] == 0) {
            std::cerr << "[P2P] P2PRecv: counts[" << i << "] should be larger than zero" << std::endl;
            return HCCL_E_PARA;
        }

        sizeBytes[i] = counts[i] * typeSize;
    }

    if (comm == nullptr) {
        std::cerr << "[P2P] P2PRecv: comm should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    std::shared_ptr<P2PCommunicator> p2pComm = commManager.getCommunicator(comm);
    if (!p2pComm) {
        std::cerr << "[P2P] P2PRecv: comm does not exist" << std::endl;
        return HCCL_E_NOT_FOUND;
    }

    CHECK_STATUS_HCCL(p2pComm->Receive(recvBuffs, sizeBytes, batchSize, stream));

    return HCCL_SUCCESS;
}

HcclResult P2PSend(void *sendBuf, uint64_t count, HcclDataType dataType, P2PComm comm, aclrtStream stream)
{
    return P2PSendBatch(&sendBuf, &count, dataType, 1, comm, stream);
}

HcclResult P2PRecv(void *recvBuf, uint64_t count, HcclDataType dataType, P2PComm comm, aclrtStream stream)
{
    return P2PRecvBatch(&recvBuf, &count, dataType, 1, comm, stream);
}

HcclResult P2PGetCommAsyncError(P2PComm comm, HcclResult *asyncError)
{
    if (comm == nullptr) {
        std::cerr << "[P2P] P2PGetCommAsyncError: comm should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    std::shared_ptr<P2PCommunicator> p2pComm = commManager.getCommunicator(comm);
    if (!p2pComm) {
        std::cerr << "[P2P] P2PGetCommAsyncError: comm does not exist" << std::endl;
        return HCCL_E_NOT_FOUND;
    }

    P2PCommChannelType channelType;
    CHECK_STATUS_HCCL(p2pComm->GetChannelType(channelType));

    return HCCL_SUCCESS;
}