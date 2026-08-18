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

#include <algorithm>
#include <chrono>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/flags/common_flags.h"
#ifdef USE_NPU
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif

namespace datasystem {
Status GetClientCommUuid(std::string &commId)
{
    (void)commId;
#ifdef USE_NPU
    if (IsRemoteH2DEnabled()) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetClientCommUuid(commId));
    }
#endif
    return Status::OK();
}

Status SetClientRemoteH2DConfig(bool enableRemoteH2D, uint32_t devId, const std::string &localIp)
{
    (void)enableRemoteH2D;
    (void)devId;
    (void)localIp;
#ifdef USE_NPU
    RETURN_IF_NOT_OK(RemoteH2DManager::SetClientRemoteH2DConfig(enableRemoteH2D, devId, localIp));
#endif
    return Status::OK();
}

Status SetRH2DLocalEndpointIp(const std::string &localIp)
{
    (void)localIp;
#ifdef USE_NPU
    RETURN_IF_NOT_OK(RemoteH2DManager::SetRH2DLocalEndpointIp(localIp));
#endif
    return Status::OK();
}

Status InitializeRemoteH2DManager()
{
#ifdef USE_NPU
    if (RemoteH2DManager::IsRemoteH2DEnabled()) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetInitStatus());
    }
#endif
    return Status::OK();
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

void SetClientFastTransportMode(FastTransportMode fastTransportMode, uint64_t transportSize, bool enablePipelineH2D)
{
    (void)fastTransportMode;
    (void)transportSize;
    (void)enablePipelineH2D;
#ifdef USE_URMA
    UrmaManager::SetClientUrmaConfig(fastTransportMode, transportSize, enablePipelineH2D);
#endif
}

void SetClientUbNumaConfig(bool affinityEnabled, uint32_t rrType, uint32_t inflightWrDiffThreshold,
                           const std::string &configSource)
{
    (void)affinityEnabled;
    (void)rrType;
    (void)inflightWrDiffThreshold;
    (void)configSource;
#ifdef USE_URMA
    UrmaManager::SetClientUbNumaConfig(affinityEnabled, rrType, inflightWrDiffThreshold, configSource);
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
    if (IsUrmaRuntimeConfigured()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().Init(hostport));
        PublishClientUrmaRuntimeReady();
    }
#endif

#ifdef USE_RDMA
    if (UcpManager::IsUcpEnabled()) {
        RETURN_IF_NOT_OK(UcpManager::Instance().Init());
    }
#endif
    return Status::OK();
}

Status ProbeUbDataPlane(const UrmaHandshakeRspPb &response, UrmaWriteFailure *failure)
{
#ifdef USE_URMA
    CHECK_FAIL_RETURN_STATUS(response.has_recovery_probe_addr(), K_NOT_SUPPORTED,
                             "Worker handshake has no dedicated URMA WRITE recovery probe address");
    constexpr uint64_t probeSize = 1;
    uint64_t segmentAddress = 0;
    uint64_t segmentSize = 0;
    uint64_t dataAddress = 0;
    RETURN_IF_NOT_OK(UrmaManager::Instance().GetRecoveryProbeSourceInfo(segmentAddress, segmentSize, dataAddress));
    std::vector<uint64_t> eventKeys;
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaWritePayload(
        response.recovery_probe_addr(), segmentAddress, segmentSize, dataAddress, 0, probeSize, 0, INVALID_CHIP_ID,
        INVALID_CHIP_ID, false, eventKeys, nullptr, failure));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    auto remainingTime = [deadline]() {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        return std::max<int64_t>(0, remaining.count());
    };
    auto preserveError = [](Status &status) { return status; };
    RETURN_IF_NOT_OK(WaitFastTransportEventWithFailure(eventKeys, remainingTime, preserveError, failure));
    INJECT_POINT("DataPlaneManager.ProbeUbDataPlane.AfterCompletion");
    INJECT_POINT("FastTransportManager.ProbeUbDataPlane.AfterCompletion");
    return Status::OK();
#else
    (void)response;
    (void)failure;
    return Status(K_NOT_SUPPORTED, "URMA recovery probe is unavailable in this build");
#endif
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
                        std::shared_ptr<EventWaiter> waiter, UrmaWriteFailure *failure,
                        std::optional<UrmaLateCompletionContext> lateCompletionContext)
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
    (void)lateCompletionContext;
    if (failure != nullptr) {
        *failure = {};
    }
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaWritePayload(urmaInfo, localSegAddress, localSegSize,
                                                              localObjectAddress, readOffset, readSize, metaDataSize,
                                                              srcChipId, dstChipId, blocking, eventKeys, waiter,
                                                              failure, std::move(lateCompletionContext)));
#endif
    return Status::OK();
}

Status AcquireUrmaSendLane(const UrmaRemoteAddrPb &urmaInfo, std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    (void)urmaInfo;
    laneLease.reset();
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().AcquireSendLane(urmaInfo, laneLease));
#endif
    return Status::OK();
}

Status SealUrmaSendLaneLease(const std::shared_ptr<UrmaSendLaneLease> &laneLease)
{
    (void)laneLease;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().SealSendLaneLease(laneLease));
#endif
    return Status::OK();
}

Status UrmaWritePayloadWithLane(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress,
                                const uint64_t &localSegSize, const uint64_t &localObjectAddress,
                                const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                uint8_t srcChipId, uint8_t dstChipId, bool blocking,
                                std::vector<uint64_t> &eventKeys,
                                const std::shared_ptr<UrmaSendLaneLease> &laneLease,
                                std::shared_ptr<EventWaiter> waiter, UrmaWriteFailure *failure,
                                std::optional<UrmaLateCompletionContext> lateCompletionContext)
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
    (void)laneLease;
    (void)waiter;
    (void)lateCompletionContext;
    if (failure != nullptr) {
        *failure = {};
    }
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaWritePayloadWithLane(
        urmaInfo, localSegAddress, localSegSize, localObjectAddress, readOffset, readSize, metaDataSize, srcChipId,
        dstChipId, blocking, eventKeys, laneLease, waiter, failure, std::move(lateCompletionContext)));
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
                       std::vector<uint64_t> &eventKeys,
                       std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    (void)remoteInfo;
    (void)objInfos;
    (void)blocking;
    (void)eventKeys;
    (void)lateCompletionContext;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaGatherWrite(remoteInfo, objInfos, blocking, eventKeys,
                                                             std::move(lateCompletionContext)));
#endif
    return Status::OK();
}

Status UrmaGatherWriteWithLane(const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos,
                               bool blocking, std::vector<uint64_t> &eventKeys,
                               const std::shared_ptr<UrmaSendLaneLease> &laneLease,
                               std::optional<UrmaLateCompletionContext> lateCompletionContext)
{
    (void)remoteInfo;
    (void)objInfos;
    (void)blocking;
    (void)eventKeys;
    (void)laneLease;
    (void)lateCompletionContext;
#ifdef USE_URMA
    RETURN_IF_NOT_OK(UrmaManager::Instance().UrmaGatherWriteWithLane(remoteInfo, objInfos, blocking, eventKeys,
                                                                     laneLease,
                                                                     std::move(lateCompletionContext)));
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

Status ImportRecoveryProbeHandshake(const UrmaHandshakeReqPb &req)
{
    (void)req;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().ImportRecoveryProbeHandshake(req));
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

namespace {
Status ConstructHandshakeIdentityPb(const std::string &senderAddr, UrmaHandshakeReqPb &req,
                                    const std::string &clientEntityId)
{
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        auto &mgr = UrmaManager::Instance();
        uint32_t jettyId = 0;
        RETURN_IF_NOT_OK(mgr.GetOrCreateLocalJetty(senderAddr, jettyId, JettyType::RECV));
        std::shared_ptr<UrmaJetty> localRecvJetty;
        RETURN_IF_NOT_OK(mgr.GetLocalJetty(senderAddr, localRecvJetty, JettyType::RECV));
        auto localInfo = mgr.GetLocalUrmaInfo();
        localInfo.jfrId = jettyId;

        urma_rjetty_t *rjetty = nullptr;
        uint32_t rjettyLen = 0;
        urma_status_t urmaStatus = ds_urma_get_rjetty(localRecvJetty->Raw(), &rjetty, &rjettyLen);
        if (rjetty != nullptr) {
            localInfo.rjettyBuf.assign(reinterpret_cast<const char *>(rjetty), rjettyLen);
            ds_urma_put_rjetty(rjetty);
            LOG(INFO) << "[URMA_CONNECT] Got delegated rjetty context, length=" << rjettyLen;
        } else {
            LOG(WARNING) << "[URMA_CONNECT] Failed to get delegated rjetty context, status=" << urmaStatus
                         << ", fallback to legacy handshake";
        }

        localInfo.ToProto(req);
        if (!mgr.GetClientId().empty()) {
            req.set_client_id(mgr.GetClientId());
        }
        if (!clientEntityId.empty()) {
            req.set_client_entity_id(clientEntityId);
        }
    }
#else
    (void)senderAddr;
    (void)req;
    (void)clientEntityId;
#endif
    return Status::OK();
}
}  // namespace

Status ConstructHandshakePb(const std::string &senderAddr, UrmaHandshakeReqPb &req, const std::string &clientEntityId)
{
    RETURN_IF_NOT_OK(ConstructHandshakeIdentityPb(senderAddr, req, clientEntityId));
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(UrmaManager::Instance().GetSegmentInfo(req));
    }
#endif
    return Status::OK();
}

Status ConstructRecoveryProbeHandshakePb(const std::string &senderAddr, UrmaHandshakeReqPb &req,
                                         UrmaRemoteAddrPb &recoveryProbeAddr)
{
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        auto &manager = UrmaManager::Instance();
        uint64_t segmentAddress = 0;
        uint64_t dataOffset = 0;
        RETURN_IF_NOT_OK(manager.GetRecoveryProbeSegmentInfo(segmentAddress, dataOffset));
        RETURN_IF_NOT_OK(ConstructHandshakeIdentityPb(senderAddr, req, ""));
        RETURN_IF_NOT_OK(manager.GetSegmentInfo(segmentAddress, *req.add_seg_infos()));
        recoveryProbeAddr.set_seg_va(segmentAddress);
        recoveryProbeAddr.set_seg_data_offset(dataOffset);
        recoveryProbeAddr.mutable_request_address()->CopyFrom(req.address());
        recoveryProbeAddr.set_client_id(req.client_id());
    }
#else
    (void)senderAddr;
    (void)req;
    (void)recoveryProbeAddr;
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
