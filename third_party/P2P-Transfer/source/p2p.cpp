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

#include <cstdint>
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
#include "runtime/dev.h"
#include "npu/RdmaDev.h"

// Later make all configurable
constexpr uint32_t P2P_NUM_PINGPONG_BUFF = 2;
constexpr uint32_t P2P_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;
constexpr uint32_t P2P_CHUNK_SIZE_BYTES = 2 * 1024 * 1024;
constexpr uint32_t P2P_QP_NUM = 3;

// Manages P2PCommunicators of the current process
P2PCommunicatorManager commManager;

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

    int32_t deviceId;
    ACL_CHECK_HCCL(aclrtGetDevice(&deviceId));

    // Spin up hccp
    std::shared_ptr<RdmaAgent> agent;
    CHECK_STATUS_HCCL(RdmaAgent::GetInstance(deviceId, agent));

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

    P2PCommRole role;
    CHECK_STATUS_HCCL(p2pKindToCommRole(kind, role));

    P2PCommArgs args = { deviceId,  link, role, P2P_NUM_PINGPONG_BUFF, P2P_BLOCK_SIZE_BYTES, P2P_CHUNK_SIZE_BYTES,
                         P2P_QP_NUM };

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

HcclResult P2PScatterBatchFromRemoteHostMem(P2pScatterEntry *entries, uint32_t batchSize, P2PComm comm,
                                            aclrtStream stream)
{
    if (comm == nullptr) {
        std::cerr << "[P2P] P2PGet: comm should not be null" << std::endl;
        return HCCL_E_PARA;
    }

    std::shared_ptr<P2PCommunicator> p2pComm = commManager.getCommunicator(comm);
    if (!p2pComm) {
        std::cerr << "[P2P] P2PGet: comm does not exist" << std::endl;
        return HCCL_E_NOT_FOUND;
    }

    P2PIScatterEntry iscatterEntries[batchSize];
    std::vector<std::vector<uint64_t>> sizes(batchSize);

    for (int b = 0; b < batchSize; b++) {
        if (entries[b].dataType >= HCCL_DATA_TYPE_RESERVED) {
            std::cerr << "[P2P] P2PGet: dataType unrecognized data type" << entries[b].dataType << std::endl;
            return HCCL_E_PARA;
        }

        if (entries[b].ddrBuf == nullptr) {
            std::cerr << "[P2P] P2PGet: ddrBuf should not be null" << std::endl;
            return HCCL_E_PARA;
        }

        size_t typeSize = GetHcclDataSizeBytes(entries[b].dataType);
        uint32_t numEl = entries[b].numEl;

        iscatterEntries[b].ddrBuf = entries[b].ddrBuf;
        iscatterEntries[b].dstBufs = entries[b].dstBufs;
        iscatterEntries[b].numEl = numEl;
        sizes[b].resize(numEl);

        uint64_t sizeBytes[numEl];
        for (int i = 0; i < numEl; i++) {
            if (entries[b].dstBufs[i] == nullptr) {
                std::cerr << "[P2P] P2PGet: sendBufs[" << i << "] should not be null" << std::endl;
                return HCCL_E_PARA;
            }

            if (entries[b].counts[i] == 0) {
                std::cerr << "[P2P] P2PGet: counts[" << i << "] should be larger than zero" << std::endl;
                return HCCL_E_PARA;
            }

            sizes[b][i] = entries[b].counts[i] * typeSize;
        }
        iscatterEntries[b].sizes = sizes[b].data();
    }

    CHECK_STATUS_HCCL(p2pComm->Read(iscatterEntries, batchSize, stream));

    return HCCL_SUCCESS;
}

HcclResult P2PScatterFromRemoteHostMem(void *ddrBuf, void **dstBufs, uint64_t *counts, HcclDataType dataType,
                                       uint32_t numEl, P2PComm comm, aclrtStream stream)
{
    P2pScatterEntry entry;
    entry.ddrBuf = ddrBuf;
    entry.dstBufs = dstBufs;
    entry.counts = counts;
    entry.dataType = dataType;
    entry.numEl = numEl;

    return P2PScatterBatchFromRemoteHostMem(&entry, 1, comm, stream);
}

HcclResult P2PGetRemoteHostMem(void *ddrBuf, void *dstBuf, uint64_t count, HcclDataType dataType, P2PComm comm,
                               aclrtStream stream)
{
    P2pScatterEntry entry;
    entry.ddrBuf = ddrBuf;
    entry.dstBufs = &dstBuf;
    entry.counts = &count;
    entry.dataType = dataType;
    entry.numEl = 1;

    return P2PScatterBatchFromRemoteHostMem(&entry, 1, comm, stream);
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

HcclResult P2PRegisterHostMem(void *hostBuf, uint64_t size, P2pSegmentInfo *segmentInfo,
                              P2pSegmentPermissions permissions)
{
    int32_t deviceId;
    ACL_CHECK_HCCL(aclrtGetDevice(&deviceId));

    // Spin up hccp
    std::shared_ptr<RdmaAgent> agent;
    CHECK_STATUS_HCCL(RdmaAgent::GetInstance(deviceId, agent));

    std::shared_ptr<RdmaDev> rdmaDev;
    CHECK_STATUS_HCCL(RdmaDev::GetInstance(deviceId, rdmaDev));

    void *devPtr;
    ACL_CHECK_HCCL(aclrtHostRegister(hostBuf, size, ACL_HOST_REGISTER_MAPPED, &devPtr));

    int accessFlag;
    CHECK_STATUS_HCCL(p2pSegmentPermissionsToFlag(permissions, accessFlag));

    CHECK_STATUS_HCCL(rdmaDev->registerGlobalMemoryRegion(hostBuf, devPtr, size, accessFlag));

    // Write segment info to segmentInfo
    P2PSegmentHandle segmentHandle;
    CHECK_STATUS_HCCL(rdmaDev->getSegmentHandle(hostBuf, segmentHandle));
    errno_t err = memcpy_s(segmentInfo->internal, P2P_SEGMENT_INFO_BYTES, &segmentHandle, sizeof(segmentHandle));
    if (err != EOK) {
        std::cout << "memcpy_s failed" << std::endl;
    }

    return HCCL_SUCCESS;
}

HcclResult P2PImportHostSegment(P2pSegmentInfo segmentInfo)
{
    int32_t deviceId;
    ACL_CHECK_HCCL(aclrtGetDevice(&deviceId));

    // Spin up hccp
    std::shared_ptr<RdmaAgent> agent;
    CHECK_STATUS_HCCL(RdmaAgent::GetInstance(deviceId, agent));

    struct P2PSegmentHandle segmentHandle;
    memcpy_s(&segmentHandle, sizeof(segmentHandle), segmentInfo.internal, sizeof(segmentInfo));

    std::shared_ptr<RdmaDev> rdmaDev;
    CHECK_STATUS_HCCL(RdmaDev::GetInstance(deviceId, rdmaDev));
    CHECK_STATUS_HCCL(rdmaDev->addRemoteSegment(segmentHandle));

    return HCCL_SUCCESS;
}