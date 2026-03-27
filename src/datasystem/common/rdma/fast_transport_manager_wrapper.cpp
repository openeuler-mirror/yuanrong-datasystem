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

#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"

#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/util/gflag/common_gflags.h"

namespace datasystem {
Status GetClientCommUuid(std::string &commId)
{
    (void)commId;
#ifdef BUILD_HETERO
    if (IsRemoteH2DEnabled()) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetClientCommUuid(commId));
    }
#endif
    return Status::OK();
}

void SetClientRemoteH2DConfig(bool enableRemoteH2D, uint32_t devId)
{
    (void)enableRemoteH2D;
    (void)devId;
#ifdef BUILD_HETERO
    RemoteH2DManager::SetClientRemoteH2DConfig(enableRemoteH2D, devId);
#endif
}

bool IsRemoteH2DEnabled()
{
#ifdef BUILD_HETERO
    return RemoteH2DManager::IsRemoteH2DEnabled();
#else
    return false;
#endif
}

Status RegisterHostMemory(void *segAddress, const uint64_t &segSize)
{
    (void)segAddress;
    (void)segSize;
#ifdef BUILD_HETERO
    if (IsRemoteH2DEnabled() && FLAGS_urma_register_whole_arena && segAddress != nullptr) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().RegisterHostMemory(segAddress, segSize));
    }
#endif
    return Status::OK();
}

bool IsUrmaEnabled()
{
#ifdef USE_URMA
    return UrmaManager::IsUrmaEnabled();
#else
    return false;
#endif
}

void SetClientFastTransportMode(FastTransportMode fastTransportMode, uint64_t transportSize)
{
    (void)fastTransportMode;
    (void)transportSize;
#ifdef USE_URMA
    UrmaManager::SetClientUrmaConfig(fastTransportMode, transportSize);
#endif
}

bool IsUcpEnabled()
{
#ifdef USE_RDMA
    return UcpManager::IsUcpEnabled();
#else
    return false;
#endif
}

bool IsFastTransportEnabled()
{
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        return true;
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled()) {
        return true;
    }
#endif

    return false;
}

Status InitializeFastTransportManager(const HostPort &hostport)
{
    (void)hostport;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().Init(hostport));
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled()) {
        RETURN_IF_NOT_OK(UcpManager::Instance().Init());
    }
#endif
    return Status::OK();
}

Status RemoveRemoteFastTransportNode(const HostPort &remoteAddress)
{
    (void)remoteAddress;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().RemoveRemoteDevice(remoteAddress.ToString()));
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled()) {
        RETURN_IF_NOT_OK(UcpManager::Instance().RemoveEndpoint(remoteAddress));
    }
#endif
    return Status::OK();
}

Status RemoveRemoteFastTransportClient(const ClientKey &clientId)
{
    (void)clientId;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        const std::string &urmaClientId = UrmaManager::Instance().GetRemoteDevicesByClientId(clientId);
        if (!urmaClientId.empty()) {
            RETURN_IF_NOT_OK(UrmaManager::Instance().RemoveRemoteDevice(urmaClientId));
        }
    }
#endif
    return Status::OK();
}

bool NeedRegisterWholeArena()
{
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled() && UrmaManager::IsRegisterWholeArenaEnabled()) {
        return true;
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled() && UcpManager::IsRegisterWholeArenaEnabled()) {
        return true;
    }
#endif
    return false;
}

Status RegisterFastTransportMemory(void *segAddress, const uint64_t &segSize)
{
    (void)segAddress;
    (void)segSize;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled() && UrmaManager::IsRegisterWholeArenaEnabled() && segAddress != nullptr) {
        LOG(INFO) << "Doing URMA memory registration of size " << segSize;
        RETURN_IF_NOT_OK(UrmaManager::Instance().RegisterSegment(reinterpret_cast<uint64_t>(segAddress), segSize));
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled() && UcpManager::IsRegisterWholeArenaEnabled() && segAddress != nullptr) {
        LOG(INFO) << "Doing UCP memory registration of size " << segSize;
        RETURN_IF_NOT_OK(UcpManager::Instance().RegisterSegment(reinterpret_cast<uint64_t>(segAddress), segSize));
    }
#endif
    return Status::OK();
}

Status WaitFastTransportEvent(std::vector<uint64_t> &keys, std::function<int64_t(void)> remainingTime,
                              std::function<Status(Status &)> errorHandler)
{
    (void)keys;
    (void)remainingTime;
    (void)errorHandler;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UrmaManager::Instance().WaitToFinish(key, remainingTime());
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(errorHandler(rc), "Failed to wait for URMA event.");
        }
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled()) {
        for (auto key : keys) {
            // Wait for the event until timeout
            Status rc = UcpManager::Instance().WaitToFinish(key, remainingTime());
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(errorHandler(rc), "Failed to wait for RDMA event.");
        }
    }
#endif
    return Status::OK();
}

void GetSegmentInfoFromShmUnit(std::shared_ptr<ShmUnit> shmUnit, uint64_t memoryAddress, uint64_t &segAddress,
                               uint64_t &segSize)
{
    // If we registered the whole arena to RDMA device,
    // then the segment address is the arena address,
    // and the segment size would be the mmaped size.
    // Otherwise the segment is for per object memory.
    bool is_register_whole_arena = FLAGS_urma_register_whole_arena;

#if defined(USE_URMA)
    is_register_whole_arena = UrmaManager::IsRegisterWholeArenaEnabled();
#elif defined(USE_RDMA)
    is_register_whole_arena = UcpManager::IsRegisterWholeArenaEnabled();
#endif
    if (is_register_whole_arena) {
        segAddress = memoryAddress - shmUnit->GetOffset();
        segSize = shmUnit->GetMmapSize();
    } else {
        segAddress = memoryAddress;
        segSize = shmUnit->GetSize();
    }
}

Status UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress, const uint64_t &localSegSize,
                        const uint64_t &localObjectAddress, const uint64_t &readOffset, const uint64_t &readSize,
                        const uint64_t &metaDataSize, bool blocking, std::vector<uint64_t> &eventKeys,
                        std::shared_ptr<EventWaiter> waiter)
{
    (void)urmaInfo;
    (void)localSegAddress;
    (void)localSegSize;
    (void)localObjectAddress;
    (void)readOffset;
    (void)readSize;
    (void)metaDataSize;
    (void)blocking;
    (void)eventKeys;
    (void)waiter;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaWritePayload(urmaInfo, localSegAddress, localSegSize,
                                                              localObjectAddress, readOffset, readSize, metaDataSize,
                                                              blocking, eventKeys, waiter));
#endif
    return Status::OK();
}

Status UrmaRead(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress, const uint64_t &localSegSize,
                const uint64_t &localObjectAddress, const uint64_t &dataSize, const uint64_t &metaSize,
                std::vector<uint64_t> &keys)
{
    (void)urmaInfo;
    (void)localSegAddress;
    (void)localSegSize;
    (void)localObjectAddress;
    (void)dataSize;
    (void)metaSize;
    (void)keys;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaRead(urmaInfo, localSegAddress, localSegSize, localObjectAddress,
                                                      dataSize, metaSize, keys));
#endif
    return Status::OK();
}

Status UrmaGatherWrite(const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                       std::vector<uint64_t> &eventKeys)
{
    (void)remoteInfo;
    (void)objInfos;
    (void)blocking;
    (void)eventKeys;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaGatherWrite(remoteInfo, objInfos, blocking, eventKeys));
#endif
    return Status::OK();
}

Status FillUcpInfo(uint64_t segAddress, uint64_t dataOffset, const std::string &srcIpAddr, UcpRemoteInfoPb &ucpInfo)
{
    (void)segAddress;
    (void)dataOffset;
    (void)srcIpAddr;
    (void)ucpInfo;
#ifdef USE_RDMA
    RETURN_IF_NOT_OK(UcpManager::Instance().FillUcpInfoImpl(segAddress, dataOffset, srcIpAddr, ucpInfo));
#endif
    return Status::OK();
}

Status UcpPutPayload(const UcpRemoteInfoPb &ucpInfo, const uint64_t &localObjectAddress, const uint64_t &readOffset,
                     const uint64_t &readSize, const uint64_t &metaDataSize, bool blocking,
                     std::vector<uint64_t> &eventKeys)
{
    (void)ucpInfo;
    (void)localObjectAddress;
    (void)readOffset;
    (void)readSize;
    (void)metaDataSize;
    (void)blocking;
    (void)eventKeys;
#ifdef USE_RDMA
    LOG(INFO) << FormatString("[FastTransportWrapper] Doing Ucp Put Payload (Size = %d)", readSize);
    RETURN_IF_NOT_OK(UcpManager::Instance().UcpPutPayload(ucpInfo, localObjectAddress, readOffset, readSize,
                                                          metaDataSize, blocking, eventKeys));
#endif
    return Status::OK();
}

Status ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    (void)req;
    (void)rsp;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        LOG(INFO) << "[FastTransportWrapper] Doing URMA connect info exchange";
        RETURN_IF_NOT_OK(UrmaManager::Instance().ExchangeJfr(req, rsp));
    }
#endif
    return Status::OK();
}

Status DoExchangeUrmaConnectInfo(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp)
{
    RETURN_IF_NOT_OK(ExchangeJfr(req, rsp));
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled() && !rsp.has_hand_shake()) {
        UrmaManager::Instance().GetLocalUrmaInfo().ToProto(*rsp.mutable_hand_shake());
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetSegmentInfo(rsp));
    }
#endif
    return Status::OK();
}

Status UcpGatherPut(const UcpRemoteInfoPb &ucpInfo, uint64_t metaDataSize, const std::vector<LocalSgeInfo> &objInfos,
                    bool blocking, std::vector<uint64_t> &eventKeys)
{
    (void)ucpInfo;
    (void)metaDataSize;
    (void)objInfos;
    (void)blocking;
    (void)eventKeys;
#ifdef USE_RDMA
    LOG(INFO) << "[FastTransportWrapper] Doing Ucp Gather Put Payload";
    RETURN_IF_NOT_OK(UcpManager::Instance().UcpGatherPut(ucpInfo, metaDataSize, objInfos, blocking, eventKeys));
#endif
    return Status::OK();
}

Status CheckTransportConnectionStable(const std::string &hostAddress, const std::string &instanceId)
{
    (void)hostAddress;
    (void)instanceId;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().CheckUrmaConnectionStable(hostAddress, instanceId));
    }
#elif defined(USE_RDMA)
    if (UcpManager::IsUcpEnabled()) {
        RETURN_IF_NOT_OK(UcpManager::Instance().CheckUcpConnectionStable(hostAddress, instanceId));
    }
#endif
    return Status::OK();
}

Status GetLocalTransportInstanceId(std::string &instanceId)
{
    (void)instanceId;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        UrmaManager::Instance().GetLocalInstanceId(instanceId);
        return Status::OK();
    }
#elif defined(USE_RDMA)
    if (UcpManager::IsUcpEnabled()) {
        UcpManager::Instance().GetLocalInstanceId(instanceId);
        return Status::OK();
    }
#endif
    RETURN_STATUS(K_URMA_ERROR, "Disabled fast transport, cannot get local instance id");
}

Status ConstructHandshakePb(const std::string &senderAddr, UrmaHandshakeReqPb &req, const std::string &clientEntityId)
{
    (void)senderAddr;
    (void)req;
    (void)clientEntityId;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        uint32_t jfrIndex = UrmaManager::Instance().GetJfrIndex(senderAddr);
        UrmaManager::Instance().GetLocalUrmaInfo().ToProto(req, jfrIndex);
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetSegmentInfo(req));
        if (!UrmaManager::Instance().GetClientId().empty()) {
            req.set_client_id(UrmaManager::Instance().GetClientId());
        }
        if (!clientEntityId.empty()) {
            req.set_client_entity_id(clientEntityId);
        }
    }
#endif
    return Status::OK();
}
}  // namespace datasystem
