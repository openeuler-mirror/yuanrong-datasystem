/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Implement the object cache remote services on the master.
 */

#include "datasystem/master/object_cache/master_oc_service_impl.h"

#include <chrono>
#include <memory>
#include <type_traits>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/trace.h"

#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/master/object_cache/device/master_dev_oc_manager.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"

namespace datasystem {
namespace master {
static constexpr int ASYNC_MIN_THREAD_NUM = 1;
static constexpr int ASYNC_MAX_THREAD_NUM = 4;

static constexpr double US_PER_MS = 1000.0;

namespace {
Status ValidateMigrationRequest(const cluster::MembershipEndpointView *membership, const std::string &localAddress,
                                uint64_t topologyVersion, uint64_t batchEpoch, const std::string &sourceMemberId,
                                const std::string &targetMemberId, const std::string &sourceAddress)
{
    const bool hasFence = topologyVersion != 0 || batchEpoch != 0 || !sourceMemberId.empty() || !targetMemberId.empty();
    if (!hasFence) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(topologyVersion > 0 && batchEpoch > 0 && !sourceMemberId.empty()
                                 && !targetMemberId.empty() && !sourceAddress.empty(),
                             K_INVALID, "Incomplete topology migration RPC fence");
    CHECK_FAIL_RETURN_STATUS(membership != nullptr && !localAddress.empty(), K_NOT_READY,
                             "Topology membership is unavailable for migration fence validation");
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership->GetSnapshot(snapshot));
    cluster::TopologyMigrationFence fence{ topologyVersion, batchEpoch,
                                           { sourceMemberId, sourceAddress },
                                           { targetMemberId, localAddress } };
    return snapshot->ValidateMigrationFence(fence);
}
}  // namespace

MasterOCServiceImpl::MasterOCServiceImpl(HostPort serverAddress, std::shared_ptr<PersistenceApi> persistApi,
                                         std::shared_ptr<AkSkManager> akSkManager,
                                         MetadataManagerHolder *metadataManagerHolder, ResourceManager *resourceManager,
                                         const cluster::MembershipEndpointView &topologyMembership,
                                         std::string localAddress)
    : MasterOCService(serverAddress),
      masterAddress_(std::move(serverAddress)),
      persistenceApi_(persistApi),
      topologyMembership_(&topologyMembership),
      localAddress_(std::move(localAddress)),
      akSkManager_(akSkManager),
      metadataManagerHolder_(metadataManagerHolder),
      resourceManager_(resourceManager)
{
}

MasterOCServiceImpl::~MasterOCServiceImpl()
{
}

void MasterOCServiceImpl::Shutdown()
{
    LOG(INFO) << "MasterOCServiceImpl exit";
    OCMigrateMetadataManager::Instance().Shutdown();
}

Status MasterOCServiceImpl::Init()
{
    RETURN_RUNTIME_ERROR_IF_NULL(topologyMembership_);
    reconciliationAsyncPool_ =
        std::make_unique<ThreadPool>(ASYNC_MIN_THREAD_NUM, ASYNC_MAX_THREAD_NUM, "Reconciliation");
    RETURN_IF_NOT_OK(OCMigrateMetadataManager::Instance().Init(masterAddress_, akSkManager_, topologyMembership_,
                                                               metadataManagerHolder_));
    return Status::OK();
}

Status MasterOCServiceImpl::GIncNestedRef(const GIncNestedRefReqPb &req, GIncNestedRefRspPb &resp)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master recv GIncNestedRef req: %s", LogHelper::IgnoreSensitive(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->IncreaseNestedRefCnt(req, resp),
                                     "Inc global nested refs failed with a error");
    GetMasterTimeCost().Append("Total GIncNestedRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GIncNestedRef %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::GDecNestedRef(const GDecNestedRefReqPb &req, GDecNestedRefRspPb &resp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master recv GDecNestedRef req: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->DecreaseNestedRefCnt(req, resp),
                                     "Dec global nested refs failed with error");
    GetMasterTimeCost().Append("Total GDecNestedRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GDecNestedRef %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::CreateMeta(const CreateMetaReqPb &req, CreateMetaRspPb &rsp)
{
    INJECT_POINT("master.CreateMeta.begin");
    ScopedRequestContext ctx;
    Timer timer;
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    PerfPoint point(PerfKey::MASTER_CREATE_META);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::META_CREATE_META_START);
    }
    const std::string localAddr = GetLocalAddr().ToString();
    LOG_FIRST_AND_EVERY_N(INFO, 1000) << FormatString("Processing CreateMetaReq, redirect: %d", req.redirect())
                                      << AppendSrcDstForLog(req.address(), localAddr);

    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    // Call MetadataManager to create the object meta.
    Status status = ocMetadataManager->CreateMeta(req, rsp);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::META_CREATE_META_END);
    }
    if (!status.IsOk()) {
        LOG(ERROR) << FormatString("[ObjectKey %s] Create object failed with error: %s", req.meta().object_key(),
                                   status.ToString());
    } else {
        INJECT_POINT("MasterOCServiceImpl.CreateMeta.idempotence");
    }
    point.Record();
    const auto totalUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    if (ShouldPrintLatencySummary(totalUs, config)) {
        PhaseDurationResult result =
            ComputePhaseDurations(Trace::Instance().GetLatencyTicks(), Trace::Instance().GetLatencyTickCount(),
                                  Trace::Instance().GetLatencyTickDroppedCount());
        bool hasDownstream = Trace::Instance().GetDownstreamPhases().count > 0;
        MergeDownstreamPhases(result);
        bool gateHit = CheckPhaseGate(result, config);
        if (gateHit || hasDownstream) {
            EncodePhaseProto(rsp, result);
        }
    }
    const double totalMs = static_cast<double>(totalUs) / US_PER_MS;
    GetMasterTimeCost().Append("Total CreateMeta", totalMs);
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && totalUs >= config.processSlowerThanUs, 1,
                        FormatString("CreateMeta done, cost: %.3fms, %s", totalMs, GetMasterTimeCost().GetInfo()));
    return status;
}

Status MasterOCServiceImpl::CreateMultiMeta(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp)
{
    INJECT_POINT("master.CreateMultiMeta.begin");
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    Status status = ocMetadataManager->CreateMultiMeta(req, rsp);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("CreateMultiMeta objects failed with error: %s", status.ToString());
    } else {
        VLOG(1) << FormatString("Master %s CreateMultiMeta rsp: %s", GetLocalAddr().ToString(),
                                LogHelper::IgnoreSensitive(rsp));
    }
    GetMasterTimeCost().Append("Total CreateMultiMeta", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master CreateMultiMeta %s", GetMasterTimeCost().GetInfo());
    return status;
}

Status MasterOCServiceImpl::CreateCopyMeta(const CreateCopyMetaReqPb &req, CreateCopyMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    PerfPoint point(PerfKey::MASTER_CREATE_COPY_META);
    const std::string localAddr = GetLocalAddr().ToString();
    LOG_FIRST_AND_EVERY_N(INFO, 1000) << FormatString("Processing CreateCopyMetaReq, redirect: %d", req.redirect())
                                      << AppendSrcDstForLog(req.address(), localAddr);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    // Call MetadataManager to create the object meta.
    Status status = ocMetadataManager->CreateCopyMeta(req, rsp);
    if (!status.IsOk()) {
        if (status.GetCode() == K_NOT_FOUND) {
            VLOG(1) << "meta already deleted";
        } else {
            LOG(ERROR) << FormatString("CreateCopyMeta failed: %s", status.ToString());
        }
    } else {
        LOG_FIRST_AND_EVERY_N(INFO, 1000) << "CreateCopyMeta success";
        INJECT_POINT("MasterOCServiceImpl.CreateCopyMeta.idempotence");
    }
    VLOG(1) << FormatString("Master %s CreateCopyMeta rsp: %s", GetLocalAddr().ToString(),
                            LogHelper::IgnoreSensitive(rsp));
    point.Record();
    auto totalMs = timer.ElapsedMilliSecond();
    GetMasterTimeCost().Append("Total CreateCopyMeta", totalMs);
    auto vlogLevel = (totalMs > 1 || status.IsError()) ? 0 : 1;
    VLOG(vlogLevel) << FormatString("CreateCopyMeta done, cost: %.1fms, %s", totalMs, GetMasterTimeCost().GetInfo());
    return status;
}

Status MasterOCServiceImpl::CreateMultiCopyMeta(const CreateMultiCopyMetaReqPb &req, CreateMultiCopyMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    PerfPoint point(PerfKey::MASTER_CREATE_MULTI_COPY_META);
    const std::string localAddr = GetLocalAddr().ToString();
    VLOG(1) << FormatString("Processing CreateMultiCopyMeta, req: ") << LogHelper::IgnoreSensitive(req)
            << AppendSrcDstForLog(req.address(), localAddr);

    INJECT_POINT("master.CreateMultiCopyMeta.beforePersist");
    Status status = CreateMultiCopyMetaImpl(req, rsp);
    point.Record();
    auto elapsedMs = timer.ElapsedMilliSecond();
    auto vlogLevel = elapsedMs > 1 ? 0 : 1;
    GetMasterTimeCost().Append("Total CreateMultiCopyMeta", elapsedMs);
    VLOG(vlogLevel) << FormatString("Process CreateMultiCopyMeta cost: %d ms, req: ", elapsedMs)
                    << LogHelper::IgnoreSensitive(req) << AppendSrcDstForLog(req.address(), localAddr) << " "
                    << GetMasterTimeCost().GetInfo();
    return status;
}

Status MasterOCServiceImpl::CreateMultiCopyMetaImpl(const CreateMultiCopyMetaReqPb &req, CreateMultiCopyMetaRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    Status status = ocMetadataManager->CreateMultiCopyMeta(req, rsp);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("CreateMultiCopyMeta objects failed with error: %s", status.ToString());
    }
    return status;
}

Status MasterOCServiceImpl::QueryMeta(const QueryMetaReqPb &req, QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    PerfPoint point(PerfKey::MASTER_QUERY_META);
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::META_QUERYMETA_START);
    }
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    INJECT_POINT("MasterOCServiceImpl.QueryMeta.busy");
    VLOG(1) << "Processing QueryMetaReq";
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    Status status;
    // Call MetadataManager to query object meta.
    status = ocMetadataManager->QueryMeta(req, rsp, payloads);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::META_QUERYMETA_END);
    }
    const auto totalUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    if (ShouldPrintLatencySummary(totalUs, config)) {
        PhaseDurationResult result =
            ComputePhaseDurations(Trace::Instance().GetLatencyTicks(), Trace::Instance().GetLatencyTickCount(),
                                  Trace::Instance().GetLatencyTickDroppedCount());
        bool hasDownstream = Trace::Instance().GetDownstreamPhases().count > 0;
        MergeDownstreamPhases(result);
        bool gateHit = CheckPhaseGate(result, config);
        if (gateHit || hasDownstream) {
            EncodePhaseProto(rsp, result);
        }
    }
    const double totalMs = static_cast<double>(totalUs) / US_PER_MS;
    GetMasterTimeCost().Append("Total QueryMeta", totalMs);
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && totalUs >= config.processSlowerThanUs, 1,
                        FormatString("QueryMeta done, target num %d, success num %d, cost: %.3fms, %s",
                                     req.ids().size(), rsp.query_metas_size(), totalMs, GetMasterTimeCost().GetInfo()));
    return Status::OK();
}

Status MasterOCServiceImpl::GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->GetMetaInfo(req, rsp);
}

Status MasterOCServiceImpl::RemoveMeta(const RemoveMetaReqPb &req, RemoveMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });

    PerfPoint point(PerfKey::MASTER_REMOVE_META);
    INJECT_POINT("master.remove_meta");
    VLOG(1) << FormatString("Master received RemoveMeta req: %s", LogHelper::IgnoreSensitive(req));
    Status status = RemoveMetaImpl(req, rsp);
    point.Record();
    auto elapsedMs = timer.ElapsedMilliSecond();
    GetMasterTimeCost().Append("Total RemoveMeta", elapsedMs);
    LOG(INFO) << FormatString(
        "RemoveMeta finished cost %d ms, receive id size: %d, success size: %d, need wait size: %d, need data size: "
        "%d, failed size: %d, outdated size: %d, req: %s %s",
        elapsedMs, req.ids_size() + req.id_with_version_size(), rsp.success_ids_size(), rsp.need_wait_ids_size(),
        rsp.need_data_ids_size(), rsp.failed_ids_size(), rsp.outdated_ids_size(), LogHelper::IgnoreSensitive(req),
        GetMasterTimeCost().GetInfo());
    return status;
}

Status MasterOCServiceImpl::RemoveMetaImpl(const RemoveMetaReqPb &req, RemoveMetaRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    // Call MetadataManager to remove object meta.
    Status status = ocMetadataManager->RemoveMeta(req, rsp);
    LOG_IF_ERROR(status, "RemoveMeta failed");
    return status;
}

Status MasterOCServiceImpl::UpdateMeta(const UpdateMetaReqPb &req, UpdateMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::META_UPDATE_META_START);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed in UpdateMeta");
    const std::string localAddr = GetLocalAddr().ToString();
    LOG(INFO) << FormatString("Processing UpdateMetaReq, redirect: %d", req.redirect())
              << AppendSrcDstForLog(req.address(), localAddr);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });

    // Call MetadataManager to update object meta.
    Status status = ocMetadataManager->UpdateMeta(req, rsp);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::META_UPDATE_META_END);
    }
    if (!status.IsOk()) {
        LOG(ERROR) << "UpdateMeta failed with error: " << status.ToString();
    }
    const auto totalUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    if (ShouldPrintLatencySummary(totalUs, config)) {
        PhaseDurationResult result =
            ComputePhaseDurations(Trace::Instance().GetLatencyTicks(), Trace::Instance().GetLatencyTickCount(),
                                  Trace::Instance().GetLatencyTickDroppedCount());
        bool hasDownstream = Trace::Instance().GetDownstreamPhases().count > 0;
        MergeDownstreamPhases(result);
        bool gateHit = CheckPhaseGate(result, config);
        if (gateHit || hasDownstream) {
            EncodePhaseProto(rsp, result);
        }
    }
    VLOG(1) << FormatString("Master %s UpdateMeta rsp: %s", GetLocalAddr().ToString(), LogHelper::IgnoreSensitive(rsp));
    INJECT_POINT("master.update_meta_failure");
    const double totalMs = static_cast<double>(totalUs) / US_PER_MS;
    GetMasterTimeCost().Append("Total UpdateMeta", totalMs);
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && totalUs >= config.processSlowerThanUs, 1,
                        FormatString("UpdateMeta done, cost: %.3fms, %s", totalMs, GetMasterTimeCost().GetInfo()));
    return status;
}

Status MasterOCServiceImpl::GetObjectLocations(const GetObjectLocationsReqPb &req, GetObjectLocationsRspPb &resp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    VLOG(1) << "Master " << GetLocalAddr().ToString()
            << " received GetObjectLocations req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->GetObjectLocations(req, resp),
                                     "Master get object locations failed");
    return Status::OK();
}

Status MasterOCServiceImpl::QueryAndGet(const QueryAndGetReqPb &req, QueryAndGetRspPb &resp,
                                        std::vector<RpcMessage> &payloads)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->QueryAndGet(req, resp, payloads);
}

Status MasterOCServiceImpl::DeleteAllCopyMeta(
    std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> serverApi)
{
    DeleteAllCopyMetaReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    if (req.are_device_objects()) {
        return ocMetadataManager->GetDeviceOcManager()->DeleteDevObjects(req, serverApi);
    }

    Timer timer;
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    RETURN_IF_NOT_OK(ocMetadataManager->DeleteAllCopyMetaWithServerApi(req, serverApi));
    auto totalMs = timer.ElapsedMilliSecond();
    auto vlogLevel = (totalMs > 1) ? 0 : 1;
    VLOG(vlogLevel) << FormatString("DeleteAllCopyMeta done, object count: %d, cost: %.1fms", req.object_keys_size(),
                                    totalMs);
    return Status::OK();
}

Status MasterOCServiceImpl::DeleteAllCopyMeta(const DeleteAllCopyMetaReqPb &req, DeleteAllCopyMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    VLOG(1) << FormatString("DeleteAllCopyMeta, object count: %d", req.object_keys_size());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    if (req.are_device_objects()) {
        ocMetadataManager->GetDeviceOcManager()->DeleteDevObjectsImpl(req, rsp);
        return Status::OK();
    }
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    RETURN_IF_NOT_OK(ocMetadataManager->DeleteAllCopyMeta(req, rsp));
    auto totalMs = timer.ElapsedMilliSecond();
    GetMasterTimeCost().Append("Total DeleteAllCopyMeta", totalMs);
    auto vlogLevel = (totalMs > 1) ? 0 : 1;
    VLOG(vlogLevel) << FormatString("DeleteAllCopyMeta done, object count: %d, cost: %.1fms, %s",
                                    req.object_keys_size(), totalMs, GetMasterTimeCost().GetInfo());
    return Status::OK();
}

std::unordered_map<std::string, QueryGlobalRefNumReqPb> MasterOCServiceImpl::QueryWorkerGRefReqPbGen(
    const std::unordered_set<std::string> &objectKeys, std::shared_ptr<master::OCMetadataManager> &ocMetadataManager)
{
    // Generate the pb used by each worker, with pb containing the required object_keys.
    std::unordered_map<std::string, QueryGlobalRefNumReqPb> queryTarget;  // ip-port string - pb.
    std::unordered_map<std::string, std::unordered_set<std::string>> refTable;
    ocMetadataManager->globalRefTable_->GetAllRef(refTable);
    for (const auto &objKey : objectKeys) {  // For each object_key in referrer set,
        auto iter = refTable.find(objKey);   // get the set of ref addr of this obj.
        if (iter == refTable.end()) {
            continue;
        }
        for (const auto &addr : iter->second) {      // For each ref addr,
            auto targetPb = queryTarget.find(addr);  // check if this addr is in queryTarget.
            if (targetPb == queryTarget.end()) {     // If not, add new the pb req with the obj.
                QueryGlobalRefNumReqPb req;
                req.add_object_keys(objKey);
                queryTarget[addr] = req;
            } else {  // If yes, add this obj to the req pb.
                targetPb->second.add_object_keys(objKey);
            }
            LOG(INFO) << "[GRef] Generate the targetPb with key:" << addr
                      << ", val: " << LogHelper::IgnoreSensitive(queryTarget.at(addr));
        }
    }
    return queryTarget;
}

Status MasterOCServiceImpl::SendQueryGRefReq(const HostPort &addr, QueryGlobalRefNumReqPb req,
                                             QueryGlobalRefNumRspPb &rsp)
{
    LOG(INFO) << "Master send gRef query to worker: " << addr.ToString();
    auto masterWorkerApi =
        MasterWorkerOCApi::CreateMasterWorkerOCApi(addr, masterAddress_, akSkManager_, masterWorkerOCService_);
    RETURN_IF_NOT_OK(masterWorkerApi->Init());
    RETURN_IF_NOT_OK(masterWorkerApi->QueryGlobalRefNumOnWorker(req, rsp));
    LOG(INFO) << "Master receive gRef query rsp from worker: " << addr.ToString();
    return Status::OK();
}

Status MasterOCServiceImpl::QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received QueryGlobalRefNum req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    GetRequestContext()->timeoutDuration.InitWithPositiveTime(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });

    // Iterate all workers concurrently.
    std::vector<HostPort> allWorkers;
    RETURN_RUNTIME_ERROR_IF_NULL(topologyMembership_);
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(topologyMembership_->GetSnapshot(snapshot),
                                     "Failed to load the cluster topology Snapshot");
    allWorkers.reserve(snapshot->CommittedMembers().size());
    for (const auto *member : snapshot->CommittedMembers()) {
        HostPort address;
        RETURN_IF_NOT_OK(address.ParseString(member->identity.address));
        allWorkers.emplace_back(std::move(address));
    }

    std::vector<std::string> objectKeys(req.object_keys().begin(), req.object_keys().end());
    RETURN_IF_NOT_OK(ocMetadataManager->RedirectObjRefs(rsp, req.redirect(), objectKeys));
    if (rsp.ref_is_moving()) {
        return Status::OK();
    }
    if (req.object_keys_size() > 0) {
        std::unordered_map<std::string, QueryGlobalRefNumReqPb> queryTarget = QueryWorkerGRefReqPbGen(
            std::unordered_set<std::string>(objectKeys.begin(), objectKeys.end()), ocMetadataManager);
        for (const auto &targetWorker : queryTarget) {
            QueryGlobalRefNumRspPb rspWorker;
            HostPort addr;
            LOG(INFO) << "[GRef] Master find the worker: " << targetWorker.first
                      << "has the following objs to check: " << LogHelper::IgnoreSensitive(targetWorker.second);
            addr.ParseString(targetWorker.first);
            auto queryAllReq = targetWorker.second;
            Status rc = SendQueryGRefReq(addr, queryAllReq, rspWorker);
            if (rc.IsOk()) {
                rsp.add_objs_glb_refs()->CopyFrom(rspWorker);
            } else {
                LOG(ERROR) << "Query the Objects Global References on Worker " << addr.ToString() << " Failed.\n"
                           << rc.GetMsg();
            }
        }
        return Status::OK();
    }
    // For query-all scenario.
    for (const auto &worker : allWorkers) {
        QueryGlobalRefNumRspPb rspWorker;
        Status rc = SendQueryGRefReq(worker, req, rspWorker);
        if (rc.IsOk()) {
            rsp.add_objs_glb_refs()->CopyFrom(rspWorker);
        } else {
            LOG(ERROR) << "Query the Objects Global References on Worker " << worker.ToString() << " Failed.\n"
                       << rc.GetMsg();
        }
    }
    return Status::OK();
}

Status MasterOCServiceImpl::GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GIncreaseMasterAppRef request, src:" << req.address()
              << ", remoteClientId:" << req.remote_client_id();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    Status status = ocMetadataManager->GIncreaseMasterAppRef(req, resp);
    if (!status.IsOk()) {
        LOG(ERROR) << "[Ref] Increase global reference: failed with error: " << status.ToString();
    } else {
        LOG(INFO) << "[Reference Counting] Increase Master App reference success";
    }
    resp.mutable_last_rc()->set_error_code(status.GetCode());
    resp.mutable_last_rc()->set_error_msg(status.GetMsg());
    return Status::OK();
}

Status MasterOCServiceImpl::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GIncreaseRef request, address:" << req.address()
              << ", objects:" << VectorToString(req.object_keys());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    INJECT_POINT("master.GIncrease_ref_failure");
    Status status = ocMetadataManager->GIncreaseRef(req, resp);
    if (!status.IsOk()) {
        LOG(ERROR) << "[Ref] Increase global reference: failed with error: " << status.ToString();
    } else {
        LOG(INFO) << "[Reference Counting] Increase global reference success";
        INJECT_POINT("master.GIncrease_ref_Idempotence");
    }
    resp.mutable_last_rc()->set_error_code(status.GetCode());
    resp.mutable_last_rc()->set_error_msg(status.GetMsg());
    GetMasterTimeCost().Append("Total GIncreaseRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GIncreaseRef %s", GetMasterTimeCost().GetInfo());

    return Status::OK();
}

Status MasterOCServiceImpl::GDecreaseRef(
    std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    GDecreaseReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GDecreaseRef request, address:" << req.address()
              << ", objects:" << VectorToString(req.object_keys());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    INJECT_POINT("master.GDecreaseRef.before");
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    RETURN_IF_NOT_OK(ocMetadataManager->GDecreaseRefWithServerApi(req, serverApi));
    LOG(INFO) << FormatString("The operations of master GDecreaseRef %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[Ref] Master received GDecreaseRef request, address:" << req.address()
              << ", objects:" << VectorToString(req.object_keys());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    INJECT_POINT("master.GDecreaseRef.before");
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    RETURN_IF_NOT_OK(ocMetadataManager->GDecreaseRef(req, resp));
    GetMasterTimeCost().Append("Total GDecreaseRef", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master GDecreaseRef %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("[Ref] Master received ReleaseGRefs req: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    ocMetadataManager->ReleaseGRefs(req, resp);
    return Status::OK();
}

Status MasterOCServiceImpl::ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("[Ref] Master recv ReleaseGRefsOfRemoteClientId: %s", LogHelper::IgnoreSensitive(req));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    ocMetadataManager->ReleaseGRefsOfRemoteClientId(req, resp);
    return Status::OK();
}

Status MasterOCServiceImpl::PushMetaToMaster(const PushMetaToMasterReqPb &req, PushMetaToMasterRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received PushMetaToMaster req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->ProcessWorkerPushMeta(req, rsp),
                                     "Master process PushMetaToMaster failed");
    GetMasterTimeCost().Append("Total PushMetaToMaster", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master PushMetaToMaster %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::RollbackSeal(const RollbackSealReqPb &req, RollbackSealRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocMetadataManager->RollbackSeal(req, rsp), "Master process RollbackSeal failed");
    INJECT_POINT("MasterOCServiceImpl.RollbackSeal.idempotence");
    GetMasterTimeCost().Append("Total RollbackSeal", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master PushMetaToMaster %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::IfNeedTriggerReconciliationImpl(const ReconciliationQueryPb &req, ReconciliationRspPb &rsp)
{
    (void)rsp;
    ScopedRequestContext ctx;
    Timer timer;
    // This is called by worker when centralized master is used. Do sync reconciliation.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    HostPort workerAddr;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerAddr.ParseString(req.hostport()), "workeradd parse failed");
    LOG(INFO) << "The master receives a reconciliation request from the worker on " << req.hostport() << ".";
    CHECK_FAIL_RETURN_STATUS(topologyMembership_ != nullptr, StatusCode::K_NOT_READY,
                             "Topology membership view is not available.");
    // TopologyController continuously reconciles membership; consume its current immutable Snapshot here.
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ocMetadataManager->ProcessWorkerRestart(workerAddr.ToString(), req.event_timestamp(), true),
        "Master process worker restart failed");
    GetMasterTimeCost().Append("Total ReconcileMembershipChange", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master ReconcileMembershipChange %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::ReconcileMembershipChange(
    std::shared_ptr<ServerUnaryWriterReader<ReconciliationRspPb, ReconciliationQueryPb>> serverApi)
{
    (void)reconciliationAsyncPool_->Submit([this, serverApi] {
        ReconciliationQueryPb req;
        ReconciliationRspPb rsp;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "ReconcileMembershipChange read request failed");
        RETURN_IF_NOT_OK(IfNeedTriggerReconciliationImpl(req, rsp));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "ReconcileMembershipChange write failed");
        return Status::OK();
    });
    return Status::OK();
}

void MasterOCServiceImpl::AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *masterWorkerService)
{
    masterWorkerOCService_ = masterWorkerService;  // save a copy of the ptr for the SendQueryGRefReq
}

bool MasterOCServiceImpl::HaveAsyncMetaRequest()
{
    return metadataManagerHolder_->HaveAsyncMetaRequest();
}

std::string MasterOCServiceImpl::GetETCDAsyncQueueUsage()
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    Status rc = metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager);
    if (rc.IsError()) {
        return "";
    }
    return ocMetadataManager->GetETCDAsyncQueueUsage();
}

std::string MasterOCServiceImpl::GetMasterAsyncPoolUsage(int64_t intervalMs)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    Status rc = metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager);
    if (rc.IsError()) {
        return "";
    }
    return ocMetadataManager->GetMasterAsyncPoolUsage(intervalMs);
}

Status MasterOCServiceImpl::MigrateMetadata(const MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    RETURN_IF_NOT_OK(ValidateMigrationRequest(topologyMembership_, localAddress_, req.topology_version(),
                                              req.batch_epoch(), req.source_member_id(), req.target_member_id(),
                                              req.source_addr()));
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    LOG(INFO) << GetWorkerId() << " save migrate data";
    if (!ocMetadataManager->GetDeviceOcManager()->CheckDeviceMetasMigrateInfoIsEmpty(req)) {
        LOG(ERROR) << "Receive device meta, start to save.";
        ocMetadataManager->GetDeviceOcManager()->SaveMigrationDeviceMeta(req);
    }
    RETURN_IF_NOT_OK(ocMetadataManager->SaveMigrationMetadata(req, rsp));
    if (!req.client_id_refs().empty()) {
        ocMetadataManager->SaveMigrationRemoteClientRefData(req);
    }
    if (!req.sub_metas().empty()) {
        ocMetadataManager->SaveSubscribeData(req);
    }
    GetMasterTimeCost().Append("Total MigrateMetadata", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master MigrateMetadata %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::ReportResource(const ResourceReportReqPb &req, ResourceReportRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    RETURN_IF_NOT_OK(resourceManager_->ReportResource(req, rsp));
    GetMasterTimeCost().Append("Total ReportResource", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of master ReportResource %s", GetMasterTimeCost().GetInfo());
    return Status::OK();
}

Status MasterOCServiceImpl::ReportRebalanceResult(const ReportRebalanceResultReqPb &req,
                                                  ReportRebalanceResultRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    INJECT_POINT("MasterOCServiceImpl.ReportRebalanceResult");
    return resourceManager_->ReportRebalanceResult(req, rsp);
}

Status MasterOCServiceImpl::PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");

    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->PutP2PMetaImpl(req, resp);
}

Status MasterOCServiceImpl::ReplacePrimary(const ReplacePrimaryReqPb &req, ReplacePrimaryRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master received ReplacePrimary req: %s, size: %ld", req.new_primary_addr(),
                              req.object_infos_size());
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->ReplacePrimary(req, rsp);
}

Status MasterOCServiceImpl::PureQueryMeta(const PureQueryMetaReqPb &req, PureQueryMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received PureQueryMeta request, object size: " << req.object_keys_size();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->PureQueryMeta(req, rsp);
}

Status MasterOCServiceImpl::CheckObjectDataLocation(const CheckObjectDataLocationReqPb &req,
                                                    CheckObjectDataLocationRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received CheckObjectDataLocation request, object size: " << req.object_versions_size()
              << ", address: " << req.address();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->CheckObjectDataLocation(req, rsp);
}

Status MasterOCServiceImpl::SubscribeReceiveEvent(
    std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    SubscribeReceiveEventReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received SubscribeReceiveEvent Event req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessSubscribeReceiveEventRequest(req, serverApi),
                                     "Process SubscribeReceiveEvent failed");
    return Status::OK();
}

Status MasterOCServiceImpl::GetP2PMeta(
    std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    GetP2PMetaReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessGetP2PMetaRequest(req, serverApi),
                                     "Process GetP2PMeta failed");
    return Status::OK();
}

Status MasterOCServiceImpl::SendRootInfo(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received Send RootInfo req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->SendRootInfoImpl(req, resp);
}

Status MasterOCServiceImpl::RecvRootInfo(
    std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    RecvRootInfoReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received Recv RootInfo req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessRecvRootInfoRequest(req, serverApi),
                                     "Process GetP2PMeta failed");
    return Status::OK();
}

Status MasterOCServiceImpl::AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received AckRecvFinish req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->AckRecvFinish(req, resp);
}

Status MasterOCServiceImpl::RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->RemoveP2PLocation(req, resp);
}

std::string MasterOCServiceImpl::GetWorkerId()
{
    return metadataManagerHolder_->GetCurrentWorkerUuid();
}

Status MasterOCServiceImpl::GetDataInfo(
    std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    GetDataInfoReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received GetDataInfo req: " << LogHelper::IgnoreSensitive(req);
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterDevOcManager->ProcessGetDataInfoRequest(req, serverApi),
                                     "Process GetDataInfo failed");
    return Status::OK();
}

Status MasterOCServiceImpl::ReleaseMetaData(const ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::MasterDevOcManager> masterDevOcManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetDeviceOcManager(masterDevOcManager),
                                     "GetOcMetadataManager failed");
    return masterDevOcManager->DeviceReleaseMetaDataImpl(req, resp);
}

Status MasterOCServiceImpl::RollbackMultiMeta(const RollbackMultiMetaReqPb &req, RollbackMultiMetaRspPb &rsp)
{
    ScopedRequestContext ctx;
    INJECT_POINT("master.RollbackMultiMeta.begin");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");

    GetRequestContext()->timeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { GetRequestContext()->timeoutDuration.Reset(); });
    LOG(INFO) << FormatString("Processing RollbackMultiMeta, address: %s objectKeys: %s", req.address(),
                              VectorToString(req.object_keys()));
    return ocMetadataManager->RollbackMultiMeta(req, rsp);
}

Status MasterOCServiceImpl::Expire(const ExpireReqPb &req, ExpireRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Master received ExpireMeta request, object size: " << req.object_keys_size();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    return ocMetadataManager->Expire(req, rsp);
}

Status MasterOCServiceImpl::LivenessCheck(const LivenessCheckReqPb &req, LivenessCheckRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->GetOcMetadataManager(ocMetadataManager),
                                     "GetOcMetadataManager failed");
    (void)rsp;
    return ocMetadataManager->LivenessCheck();
}
}  // namespace master
}  // namespace datasystem
