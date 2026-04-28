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

#include <chrono>

#include "datasystem/common/inject/inject_point.h"
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

uint64_t GenerateReqId()
{
#ifdef USE_URMA
    return UrmaManager::Instance().GenerateReqId();
#else
    static std::atomic<uint64_t> startReqId = 0;
    return startReqId.fetch_add(1);
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

Status InitializeFastTransportManager(const HostPort &hostport)
{
    (void)hostport;
    INJECT_POINT("FastTransportManager.Initialize", [](int delayMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        return Status(K_URMA_ERROR, "Inject fast transport init failed");
    });
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
        RETURN_IF_NOT_OK(UrmaManager::Instance().RemoveRemoteClient(clientId));
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
                        const uint64_t &metaDataSize, uint8_t srcChipId, uint8_t dstChipId, bool blocking,
                        std::vector<uint64_t> &eventKeys,
                        std::shared_ptr<EventWaiter> waiter)
{
    (void)urmaInfo;
    (void)localSegAddress;
    (void)localSegSize;
    (void)localObjectAddress;
    (void)readOffset;
    (void)readSize;
    (void)metaDataSize;
    (void)srcChipId;
    (void)dstChipId;
    (void)blocking;
    (void)eventKeys;
    (void)waiter;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaWritePayload(urmaInfo, localSegAddress, localSegSize,
                                                              localObjectAddress, readOffset, readSize, metaDataSize,
                                                              srcChipId, dstChipId, blocking, eventKeys, waiter));
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
        auto &mgr = UrmaManager::Instance();
        // Get or create a local Jetty for this target node (reused across reconnections).
        uint32_t jettyId = 0;
        RETURN_IF_NOT_OK(mgr.GetOrCreateLocalJetty(senderAddr, jettyId, JettyType::RECV));
        auto localInfo = mgr.GetLocalUrmaInfo();
        localInfo.jfrId = jettyId;
        localInfo.ToProto(req);
        if (!mgr.GetClientId().empty()) {
            req.set_client_id(mgr.GetClientId());
        }
        if (!clientEntityId.empty()) {
            req.set_client_entity_id(clientEntityId);
        }
        RETURN_IF_NOT_OK(mgr.GetSegmentInfo(req));
    }
#endif
    return Status::OK();
}

Status FinalizeOutboundConnection(const UrmaHandshakeRspPb &rsp)
{
    (void)rsp;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().FinalizeOutboundConnection(rsp));
    }
#endif
    return Status::OK();
}
}  // namespace datasystem
