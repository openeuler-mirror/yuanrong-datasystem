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
 * Description: Defines the worker worker service processing main class.
 */
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"

#include <algorithm>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>


#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/utils/status.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#ifdef USE_NPU
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/worker/object_cache/worker_worker_oc_gather_layout.h"
#include "datasystem/worker/object_cache/worker_worker_peer_state_codec.h"

DS_DECLARE_string(worker_address);
DS_DECLARE_int32(oc_worker_worker_direct_port);
DS_DECLARE_int32(oc_worker_worker_parallel_nums);
DS_DECLARE_int32(oc_worker_worker_parallel_min);
DS_DECLARE_uint64(oc_worker_aggregate_single_max);
DS_DECLARE_uint64(oc_worker_aggregate_merge_size);

namespace datasystem {
namespace {
constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
constexpr char URMA_WARMUP_KEY_PREFIX[] = "_urma_";
constexpr uint64_t URMA_WARMUP_OBJECT_SIZE = 1;

constexpr double US_PER_MS = 1000.0;

bool IsUrmaWarmupRequest(const GetObjectRemoteReqPb &req)
{
    return req.has_urma_info() && req.object_key().rfind(URMA_WARMUP_KEY_PREFIX, 0) == 0 && req.read_offset() == 0
           && req.read_size() == URMA_WARMUP_OBJECT_SIZE && req.data_size() == URMA_WARMUP_OBJECT_SIZE;
}

}  // namespace

inline std::ostream &operator<<(std::ostream &os, const GetObjectRemoteReqPb &req)
{
    os << "(";
    os << req.object_key() << ",";
    os << req.request_id() << ",";
    os << req.read_offset() << ",";
    os << req.read_size();
    os << ")";
    return os;
}
namespace object_cache {
namespace {
void MovePayload(std::vector<RpcMessage> &src, std::vector<RpcMessage> &dst)
{
    dst.insert(dst.end(), std::make_move_iterator(src.begin()), std::make_move_iterator(src.end()));
}

std::string GetRemoteAddressForLog(const GetObjectRemoteReqPb &req)
{
    if (req.has_urma_info()) {
        return FormatString("%s:%d", req.urma_info().request_address().host(),
                            req.urma_info().request_address().port());
    }
    if (req.has_ucp_info()) {
        return FormatString("%s:%d", req.ucp_info().remote_ip_addr().host(), req.ucp_info().remote_ip_addr().port());
    }
    return "";
}

Status GetRemoteAddressFromBatchGetReq(const BatchGetObjectRemoteReqPb &req, HostPort &requestAddress)
{
    CHECK_FAIL_RETURN_STATUS(req.requests_size() > 0, K_INVALID, "BatchGetObjectRemote request is empty");
    const auto &firstReq = req.requests(0);
    std::string host;
    int port = -1;
    if (firstReq.has_urma_info()) {
        host = firstReq.urma_info().request_address().host();
        port = firstReq.urma_info().request_address().port();
    } else if (firstReq.has_ucp_info()) {
        host = firstReq.ucp_info().remote_ip_addr().host();
        port = firstReq.ucp_info().remote_ip_addr().port();
    } else {
        RETURN_STATUS(K_INVALID, "BatchGetObjectRemote request has no remote address");
    }
    requestAddress = HostPort(host, port);
    return Status::OK();
}

void LogBatchGetObjectRemotePrepareFailed(const BatchGetObjectRemoteReqPb &req, const std::string &callerAddress,
                                          const std::string &firstObjectKey, const Status &status)
{
    VLOG(1) << "[REMOTE_GET_CONNECTION_CHECK_FAILED] method=BatchGetObjectRemote"
            << ", count=" << req.requests_size() << ", firstObjectKey=" << firstObjectKey
            << ", src=" << callerAddress << ", dst=" << FLAGS_worker_address
            << ", status=" << status.ToString() << ", willReturnViaBrpcSetFailed=true";
}

void LogBatchGetObjectRemoteFinish(const LatencyTraceConfig &config, const BatchGetObjectRemoteReqPb &req,
                                   const std::vector<RpcMessage> &payload, const std::string &firstObjectKey,
                                   uint64_t realRemainingTime, const std::string &callerAddress, const Timer &timer)
{
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    // Per-phase breakdown: RemoteLockEntry / RemoteTryRLatch / RemoteWriteFastTransport.
    SLOW_LOG_IF_OR_VLOG(
        INFO, config.processSlowerThanUs > 0 && elapsedUs >= config.processSlowerThanUs, 1,
        AppendSrcDstForLog(
            FormatString("[Get/RemotePull] finish, count: %d, firstObjectKey: %s, payload size: %zu, start "
                         "remainingTime: %zu, cost: %.3fms, breakdown: %s",
                         req.requests_size(), firstObjectKey, payload.size(), realRemainingTime, elapsedMs,
                         GetWorkerTimeCost().GetInfo()),
            callerAddress, FLAGS_worker_address));
}
}  // namespace

WorkerWorkerOCServiceImpl::WorkerWorkerOCServiceImpl(
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc, std::shared_ptr<AkSkManager> akSkManager,
    const cluster::MembershipEndpointView &membership, CoordinationAvailabilityProvider coordinationAvailable,
    BackendObservationProvider backendObservationProvider)
    : WorkerWorkerOCService(clientSvc == nullptr ? HostPort() : clientSvc->GetLocalAddr()),
      ocClientWorkerSvc_(std::move(clientSvc)),
      akSkManager_(std::move(akSkManager)),
      membership_(membership),
      coordinationAvailable_(std::move(coordinationAvailable)),
      backendObservationProvider_(std::move(backendObservationProvider))
{
}

WorkerWorkerOCServiceImpl::~WorkerWorkerOCServiceImpl()
{
    communicatorThreadPool_.reset();
    LOG(INFO) << "WorkerWorkerOCServiceImpl exit";
}

Status WorkerWorkerOCServiceImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS(ocClientWorkerSvc_ != nullptr, StatusCode::K_NOT_READY,
                             "ClientWorkerService must be initialized before WorkerWorkerService construction");
    RETURN_IF_EXCEPTION_OCCURS(communicatorThreadPool_ = std::make_shared<ThreadPool>(0, 4, "CommInit"));
    return WorkerWorkerOCService::Init();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetObjectRemoteRspPb, GetObjectRemoteReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    Timer timer;
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY);
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    GetObjectRemoteReqPb req;
    GetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::DATA_REMOTEGET_START);
    }
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    INJECT_POINT("worker.GetObjectRemote.afterRead");
    auto connectionRc = CheckConnectionStable(req);
    if (connectionRc.IsError()) {
        VLOG(1) << "[REMOTE_GET_CONNECTION_CHECK_FAILED] method=GetObjectRemote"
                << ", objectKey=" << req.object_key() << ", src=" << GetRemoteAddressForLog(req)
                << ", dst=" << FLAGS_worker_address << ", status=" << connectionRc.ToString()
                << ", willReturnViaBrpcSetFailed=true";
        return connectionRc;
    }
    // K_OC_REMOTE_GET_NOT_ENOUGH error happens only when URMA is used for RDMA and size of the object
    // is different from the request. A provider-local UB write failure must also cross the RPC boundary so the
    // requester can quarantine this source before its next read.
    auto getRc = GetObjectRemote(req, rsp, payload);
    const bool providerUbFailureEncoded = TryEncodeProviderUbFailureResponse(getRc, rsp);
    if (!providerUbFailureEncoded) {
        RETURN_IF_NOT_OK_EXCEPT(getRc, StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
    }
    TryEncodeRemoteGetLatencySummary(config, traceEnabled, rsp);
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SendGetObjectRemotePayload(serverApi, rsp, payload),
                                     "GetObjectRemote send payload error");
    pointImpl.Record();
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    const char *requestTransport =
        req.has_urma_info() ? "UB" : (req.has_ucp_info() ? "RDMA" : "RPC_PAYLOAD");
    SLOW_LOG_IF_OR_VLOG(
        INFO, config.processSlowerThanUs > 0 && elapsedUs >= config.processSlowerThanUs, 1,
        AppendSrcDstForLog(
            FormatString("[GetObjectRemote] finish, objectKey: %s, requestTransport: %s, dataSource: %d, "
                         "payloadCount: %zu, cost: %.3fms",
                         req.object_key(), requestTransport, static_cast<int>(rsp.data_source()), payload.size(),
                         elapsedMs),
            GetRemoteAddressForLog(req), FLAGS_worker_address));
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                  std::vector<RpcMessage> &payload, bool isQueryAndGet)
{
    // Inherit the SDK traceID from the worker thread's thread_local Trace (set by
    // WorkerEntryImpl's SetTraceContextFromMeta) into the per-request context so
    // that any access recorder or log emitted inside the pull handler scope carries
    // the same traceID as the original SDK request. Without this, the pull handler
    // runs without an active RequestContext and nested recorders fall back to a
    // detached UUID that does not correlate with the SDK request.
    ScopedRequestContext ctx;
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY);
    Timer slowLogTimer;
    if (isQueryAndGet) {
        RETURN_IF_NOT_OK(CheckConnectionStable(req));
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const std::string callerAddress = GetRemoteAddressForLog(req);
    std::vector<uint64_t> eventKeys;
    RETURN_IF_NOT_OK(GetObjectRemoteHandler(req, rsp, payload, true, eventKeys, nullptr, nullptr, nullptr,
                                            nullptr, isQueryAndGet));
    auto config = GetServerLatencyTraceConfig();
    uint64_t elapsedUs = static_cast<uint64_t>(slowLogTimer.ElapsedMicroSecond());
    SLOW_LOG_IF_OR_VLOG(
        INFO,
        (config.processSlowerThanUs > 0 && elapsedUs >= config.processSlowerThanUs) || FLAGS_enable_perf_trace_log, 1,
        AppendSrcDstForLog(
            FormatString("Processing pull object[%s] offset[%ld] size[%ld], expectedDataSize[%ld], version[%ld], "
                         "hasUrmaInfo[%d], cost: %.3fms",
                         req.object_key(), req.read_offset(), req.read_size(), req.data_size(), req.version(),
                         req.has_urma_info(), static_cast<double>(elapsedUs) / US_PER_MS),
            callerAddress, FLAGS_worker_address));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::SendGetObjectRemotePayload(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetObjectRemoteRspPb, GetObjectRemoteReqPb>> serverApi,
    const GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payload)
{
    const bool tagPayload = FLAGS_oc_worker_worker_direct_port > 0;
    if (rsp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED
        || rsp.data_source() == DataTransferSource::DATA_DELAY_TRANSFER
        || rsp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED_MEMSET_META) {
        return serverApi->SendAndTagPayload({}, tagPayload);
    }
    if (rsp.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
        return serverApi->SendAndTagPayload(payload, tagPayload);
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteBatchWrite(uint32_t paraIndex, const GetObjectRemoteReqPb &subReq,
                                                            BatchGetObjectRemoteRspPb &rsp,
                                                            std::vector<ParallelRes> &parallelRes,
                                                            std::shared_ptr<AggregateMemory> batchPtr,
                                                            BatchRh2dContext *batchRh2dContext)
{
    PerfPoint point(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_BATCH_WRITE);
    GetObjectRemoteRspPb &subRsp = parallelRes[paraIndex].respPbs.emplace_back();
    uint64_t &subIndex = parallelRes[paraIndex].subIndex;

    std::vector<RpcMessage> subPayload;
    std::vector<uint64_t> eventKeys;
    auto isGatherWrite = IsFastTransportEnabled() && batchPtr != nullptr;
    const std::string callerAddress = GetRemoteAddressForLog(subReq);
    VLOG(1) << AppendSrcDstForLog(FormatString("Processing pull object[%s] offset[%ld] size[%ld]", subReq.object_key(),
                                               subReq.read_offset(), subReq.read_size()),
                                  callerAddress, FLAGS_worker_address);
    Status fallbackStatus;
    auto status =
        GetObjectRemoteHandler(subReq, subRsp, subPayload, false, eventKeys, batchPtr, rsp.mutable_root_info(),
                               isGatherWrite ? nullptr : &fallbackStatus, batchRh2dContext);
    // payload means need to FallbackTcp/NormalTcp transport
    if ((status.IsError() && subPayload.empty()) || (status.IsError() && !FLAGS_enable_transport_fallback)) {
        subRsp.mutable_error()->set_error_code(status.GetCode());
        subRsp.mutable_error()->set_error_msg(status.GetMsg());
    }

    if (isGatherWrite) {
        // pre save subPayload to fallbackPayloads, for fallback when GatherWrite failed
        batchPtr->fallbackPayloads.insert(batchPtr->fallbackPayloads.end(), std::make_move_iterator(subPayload.begin()),
                                          std::make_move_iterator(subPayload.end()));
        return Status::OK();
    }

    // empty requestIds means failed fastTransport or tcp mode.
    // Note: Pipeline RH2D sub-requests are completed by MLCacheDirect pipeline sender and must not be waited by
    // normal fast-transport event logic.
    std::vector<uint64_t> requestIds;
    if (!(subReq.has_urma_info() && OsXprtPipln::IsPiplnH2DRequest(subReq.urma_info()))) {
        requestIds = std::move(eventKeys);
    }

    parallelRes[paraIndex].kps.emplace_back(subIndex, std::make_pair(std::move(requestIds), std::move(subPayload)));
    parallelRes[paraIndex].fallbackStatuses.emplace_back(fallbackStatus);
    subIndex++;
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::PrepareAggregateMemory(BatchGetObjectRemoteReqPb &req, AggregateInfo &info)
{
    info.canBatchHandler = false;
    info.batchReqSize.clear();
    info.batchStartIndex.clear();
    info.batchSizes.clear();

    const uint64_t batchLimitKeys = 1024;  // must same as obj_cache_shm_unit in req side.
    std::vector<object_cache::AggregateGatherSubgroup> subgroups;
    if (!object_cache::ShouldUseAggregateGather(
            req, OsXprtPipln::IsPiplnH2DRequest(req), ocClientWorkerSvc_->GetMetadataSize(),
            FLAGS_oc_worker_aggregate_single_max, FLAGS_oc_worker_aggregate_merge_size, batchLimitKeys, subgroups)) {
        return Status::OK();
    }
    for (const auto &subgroup : subgroups) {
        info.batchStartIndex.emplace_back(subgroup.startIndex);
        info.batchSizes.emplace_back(subgroup.byteSize);
        info.batchReqSize.emplace_back(subgroup.requestCount);
    }
    info.canBatchHandler = true;
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GatherWrite(uint64_t subIndex, AggregateInfo &info,
                                              std::shared_ptr<AggregateMemory> aggregatedMem,
                                              std::vector<ParallelRes> &parallelRes, BatchGetObjectRemoteReqPb &req,
                                              const std::shared_ptr<UrmaSendLaneLease> &sendLaneLease)
{
    if (!info.canBatchHandler) {
        return Status::OK();
    }
    auto startPos = info.batchStartIndex[subIndex];
    auto *subReq = req.mutable_requests(startPos);
    ParallelRes &loc = parallelRes[subIndex];

    Status rc = Status::OK();
    if (IsUrmaEnabled() && subReq->has_urma_info()) {
        auto &urmaInfo = subReq->urma_info();
        CHECK_FAIL_RETURN_STATUS(urmaInfo.seg_data_offset() >= ocClientWorkerSvc_->GetMetadataSize(), K_RUNTIME_ERROR,
                                 "Aggregate URMA target has no metadata headroom");
        RemoteSegInfo remoteSegInfo{
            .segAddr = urmaInfo.seg_va(),
            .segOffset = urmaInfo.seg_data_offset() - ocClientWorkerSvc_->GetMetadataSize(),
            .host = urmaInfo.request_address().host(),
            .port = urmaInfo.request_address().port(),
            .dstChipId = urmaInfo.has_chip_id() ? static_cast<uint8_t>(urmaInfo.chip_id()) : INVALID_CHIP_ID,
        };
        if (sendLaneLease != nullptr) {
            rc = UrmaGatherWriteWithLane(remoteSegInfo, aggregatedMem->localSgeInfos, false, loc.eventKeys,
                                         sendLaneLease);
        } else {
            rc = UrmaGatherWrite(remoteSegInfo, aggregatedMem->localSgeInfos, false, loc.eventKeys);
        }
    } else if (IsUcpEnabled() && subReq->has_ucp_info()) {
        CHECK_FAIL_RETURN_STATUS(subReq->ucp_info().remote_buf() >= ocClientWorkerSvc_->GetMetadataSize(),
                                 K_RUNTIME_ERROR, "Aggregate UCP target has no metadata headroom");
        rc = UcpGatherPut(subReq->ucp_info(), ocClientWorkerSvc_->GetMetadataSize(), aggregatedMem->localSgeInfos,
                          false, loc.eventKeys);
    }

    loc.fallbackPayloads = std::move(aggregatedMem->fallbackPayloads);

    std::vector<uint64_t> subKeys;
    // if error, set fallback flag
    if (rc.IsError()) {
        LOG_IF_ERROR(rc, "GatherWrite failed, all objects will fallback to payload");
        // Set empty subkeys and empty payload, and get failed payload in objectFallbackPayload
        loc.kps.emplace_back(loc.subIndex, std::make_pair(std::move(subKeys), std::vector<RpcMessage>()));
        for (uint64_t i = 0; i < info.batchReqSize[subIndex]; ++i) {
            loc.respPbs[i].set_data_source(datasystem::DataTransferSource::DATA_IN_PAYLOAD);
            if (!FLAGS_enable_transport_fallback) {
                loc.respPbs[i].mutable_error()->set_error_code(rc.GetCode());
                loc.respPbs[i].mutable_error()->set_error_msg(rc.GetMsg());
            }
        }
        return Status::OK();
    }

    subKeys = std::move(loc.eventKeys);
    loc.kps.emplace_back(loc.subIndex, std::make_pair(std::move(subKeys), std::vector<RpcMessage>()));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::PrepareBatchRh2dContext(const GetObjectRemoteReqPb &req,
                                                          BatchRh2dContext &batchRh2dContext)
{
#ifdef USE_NPU
    if (!IsRemoteH2DEnabled() || req.comm_id().empty()) {
        return Status::OK();
    }

    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetDevIdForComm(req.comm_id(), batchRh2dContext.devId));
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().SetDeviceIdx(batchRh2dContext.devId));
    batchRh2dContext.prepared = true;
#else
    (void)req;
    (void)batchRh2dContext;
#endif
    return Status::OK();
}

bool WorkerWorkerOCServiceImpl::TryCompleteMissingUrmaWarmup(const GetObjectRemoteReqPb &req,
                                                             GetObjectRemoteRspPb &rsp)
{
    if (!IsUrmaWarmupRequest(req)) {
        return false;
    }
    auto status = ocClientWorkerSvc_->objectTable_->Contains(req.object_key());
    if (status.GetCode() != StatusCode::K_NOT_FOUND) {
        return false;
    }
    rsp.mutable_error()->set_error_code(K_OK);
    rsp.set_data_source(DataTransferSource::DATA_ALREADY_TRANSFERRED);
    return true;
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteHandler(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                         std::vector<RpcMessage> &payload, bool blocking,
                                                         std::vector<uint64_t> &eventKeys,
                                                         std::shared_ptr<AggregateMemory> batchPtr,
                                                         RemoteH2DRootInfoPb *batchRootInfo, Status *fallbackStatus,
                                                         BatchRh2dContext *batchRh2dContext, bool isQueryAndGet)
{
    PerfPoint point(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_HANDLER);
    const std::string &objectKey = req.object_key();
    const std::string &requestId = req.request_id();
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "objectKey is empty.");
    if (TryCompleteMissingUrmaWarmup(req, rsp)) {
        return Status::OK();
    }
    if (!IsUrmaWarmupRequest(req)) {
        INJECT_POINT("worker.worker_worker_remote_get_sleep");
        INJECT_POINT("worker.worker_worker_remote_get_failure");
    }
    Status status = GetObjectRemoteImpl(req, rsp, payload, blocking, eventKeys, batchPtr, batchRootInfo,
                                        fallbackStatus, batchRh2dContext, isQueryAndGet);
    if (status.GetCode() == K_INVALID || status.GetCode() == K_NOT_FOUND) {
        status = Status(K_WORKER_PULL_OBJECT_NOT_FOUND, status.GetMsg());
    }
    if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND && IsUrmaWarmupRequest(req)) {
        rsp.mutable_error()->set_error_code(K_OK);
        rsp.set_data_source(DataTransferSource::DATA_ALREADY_TRANSFERRED);
        return Status::OK();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        status, FormatString("[ObjectKey %s] Get object remote failed, requestId: %s, workerAddr: %s", objectKey,
                             requestId, localAddress_.ToString()));
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetSafeObjectEntry(const std::string &objectKey, bool tryLock, uint64_t version,
                                                     std::shared_ptr<SafeObjType> &safeEntry)
{
    bool insert = false;
    auto func = [this, &objectKey, &safeEntry, &insert]() {
        return ocClientWorkerSvc_->objectTable_->ReserveGetAndLock(objectKey, safeEntry, insert, false, false);
    };
    RETURN_IF_NOT_OK(RetryWhenDeadlock(func));
    if (insert) {
        Raii innerUnlock([&safeEntry]() { safeEntry->WUnlock(); });
        if (!tryLock) {
            // get data from L2 cache if worker is primary copy
            return ocClientWorkerSvc_->GetDataFromL2CacheForPrimaryCopy(objectKey, version, safeEntry);
        }
        // tryLock callers must not load a missing entry from L2; the remote-get handler maps not-found for retry.
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object not found");
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::EstablishConnAndFillSeg(const std::string &commId, const uint64_t &localSegAddress,
                                                          const uint64_t &localSegSize,
                                                          std::shared_ptr<ShmUnit> shmUnit, uint64_t metadataSize,
                                                          GetObjectRemoteRspPb &rsp, RemoteH2DRootInfoPb *batchRootInfo,
                                                          BatchRh2dContext *batchRh2dContext)
{
    (void)commId;
    (void)localSegAddress;
    (void)localSegSize;
    (void)shmUnit;
    (void)metadataSize;
    (void)rsp;
    (void)batchRootInfo;
    (void)batchRh2dContext;
#ifdef USE_NPU
    PerfPoint point(PerfKey::WORKER_REMOTE_GET_PREPARE_RH2D_HOST_INFO);

    int32_t devId = -1;
    if (batchRh2dContext != nullptr && batchRh2dContext->prepared) {
        devId = batchRh2dContext->devId;
    } else {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetDevIdForComm(commId, devId));
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().SetDeviceIdx(devId));
    }

    // Send root info to client
    auto *rootInfo = batchRootInfo ? batchRootInfo : rsp.mutable_host_info()->mutable_root_info();
    if (rootInfo->internal().empty()) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().P2PGetRootInfo(commId, rootInfo));
    }

    // Initialize communicator connection (accept client).
    std::shared_ptr<RemoteH2DContext> p2pComm;
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().P2PCommInitRootInfo(commId, *rootInfo, P2P_SENDER, p2pComm, devId,
                                                                      communicatorThreadPool_));
    // Send segment info to client
    auto *segmentPb = rsp.mutable_host_info()->mutable_remote_host_segment();
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().FillSegmentInfo(localSegSize, shmUnit->GetOffset() + metadataSize,
                                                                  localSegAddress, *segmentPb, devId));

    // Send offset info to client
    uint64_t *dataPtr = reinterpret_cast<uint64_t *>(static_cast<uint8_t *>(shmUnit->GetPointer()) + metadataSize);
    auto *dataInfoPb = rsp.mutable_host_info()->mutable_data_info();
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().FillDataInfo(dataPtr, *dataInfoPb));
    point.Record();
#endif
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::LoadPayloadAndFillResponse(
    const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, SafeObjType &entry, std::vector<RpcMessage> &outPayload,
    const std::string &objectKey, uint64_t offset, uint64_t size, bool blocking, std::vector<uint64_t> &eventKeys,
    const std::shared_ptr<AggregateMemory> &batchPtr, RemoteH2DRootInfoPb *batchRootInfo,
    BatchRh2dContext *batchRh2dContext, Status *fallbackStatus, bool isFastTransportEnabled, bool isUrmaFastTransport,
    bool isPipelineH2DRequest, PerfPoint &batchImplPoint, bool isQueryAndGet)
{
    PerfPoint loadDataPoint(PerfKey::WORKER_LOAD_OBJECT_DATA);
    PerfPoint pointImpl(PerfKey::WORKER_REMOTE_GET_READ_KEY);
    ReadObjectKV objKv(ReadKey(objectKey, offset, size), entry);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objKv.CheckReadOffset(), "Read offset verify failed");
    if (entry->IsSpilled() && entry->GetShmUnit() == nullptr) {
        RETURN_IF_NOT_OK(LoadSpilledObjectData(objectKey, outPayload, objKv, pointImpl, isQueryAndGet));
    } else {
        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_PAYLOAD_SHM_UNIT);
        ShmGuard shmGuard(entry->GetShmUnit(), entry->GetDataSize(), entry->GetMetadataSize());
        if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
            // Timing: SHM read-latch contention → breakdown "RemoteTryRLatch: Nms".
            Timer latchTimer;
            RETURN_IF_NOT_OK(shmGuard.TryRLatch());
            GetWorkerTimeCost().Append("RemoteTryRLatch", latchTimer.ElapsedMilliSecond());
        }
        INJECT_POINT("worker.LoadObjectData.AddPayload");
        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_PAYLOAD);

        auto shmUnit = entry->GetShmUnit();
        uint64_t localSegAddress;
        uint64_t localSegSize;
        GetSegmentInfoFromShmUnit(shmUnit, reinterpret_cast<uint64_t>(shmUnit->GetPointer()), localSegAddress,
                                  localSegSize);
        Status fastTransportStatus = Status::OK();
        std::string fastTransportName;

        // Timing: URMA/UCP transport latency → breakdown "RemoteWriteFastTransport: Nms".
        {
            Timer writeTimer;
            RETURN_IF_NOT_OK(WriteViaFastTransport(req, rsp, entry, shmUnit, localSegAddress, localSegSize, offset,
                                                   size, blocking, eventKeys, batchPtr, isFastTransportEnabled,
                                                   isPipelineH2DRequest, batchRh2dContext, fastTransportStatus,
                                                   fastTransportName));
            GetWorkerTimeCost().Append("RemoteWriteFastTransport", writeTimer.ElapsedMilliSecond());
        }
        if (isQueryAndGet && req.has_urma_info()) {
            CHECK_FAIL_RETURN_STATUS(isUrmaFastTransport, K_NOT_SUPPORTED,
                                     "QueryAndGet UB transport is unavailable");
            RETURN_IF_NOT_OK(fastTransportStatus);
        } else {
            RETURN_IF_NOT_OK(HandlePayloadFallback(
                req, rsp, entry, outPayload, shmGuard, shmUnit, fastTransportStatus, fastTransportName, objectKey,
                isUrmaFastTransport, isPipelineH2DRequest, blocking, batchPtr, fallbackStatus, batchRootInfo,
                batchRh2dContext, objKv, localSegAddress, localSegSize));
        }
        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_RESP);
    }

    FillGetObjectRemoteResponse(rsp, entry, loadDataPoint, batchImplPoint);
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::LoadSpilledObjectData(const std::string &objectKey,
                                                        std::vector<RpcMessage> &outPayload,
                                                        const ReadObjectKV &objKv, PerfPoint &point,
                                                        bool isQueryAndGet)
{
    if (isQueryAndGet) {
        RETURN_STATUS(K_NOT_SUPPORTED, "QueryAndGet fast path only reads resident data");
    }
    point.RecordAndReset(PerfKey::WORKER_REMOTE_GET_PAYLOAD_FROM_DISK);
    RETURN_IF_NOT_OK(
        WorkerOcSpill::Instance()->Get(objectKey, outPayload, objKv.GetReadSize(), objKv.GetReadOffset()));
    point.RecordAndReset(PerfKey::WORKER_REMOTE_GET_RESP);
    return Status::OK();
}

void WorkerWorkerOCServiceImpl::FillGetObjectRemoteResponse(GetObjectRemoteRspPb &rsp, const SafeObjType &entry,
                                                            PerfPoint &loadDataPoint, PerfPoint &batchImplPoint)
{
    PerfPoint fillRspPoint(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_FILL_RESPONSE);
    rsp.mutable_error()->set_error_code(StatusCode::K_OK);
    rsp.set_data_size(static_cast<int64_t>(entry->GetDataSize()));
    rsp.set_create_time(static_cast<int64_t>(entry->GetCreateTime()));
    rsp.set_life_state(static_cast<uint32_t>(entry->GetLifeState()));
    fillRspPoint.Record();
    loadDataPoint.Record();
    batchImplPoint.Record();
}

Status WorkerWorkerOCServiceImpl::LockEntryForRemoteGet(const std::string &objectKey, bool tryLock, uint64_t version,
                                                        std::shared_ptr<SafeObjType> &safeEntry)
{
    RETURN_IF_NOT_OK(GetSafeObjectEntry(objectKey, tryLock, version, safeEntry));
    if (tryLock) {
        int maxRetryCount = 5;
        Status s;
        do {
            s = safeEntry->TryRLock();
            if (s.IsOk()) {
                break;
            }
            --maxRetryCount;
        } while (maxRetryCount > 0 && s.GetCode() == StatusCode::K_TRY_AGAIN);
        RETURN_IF_NOT_OK(s);
    } else {
        RETURN_IF_NOT_OK(safeEntry->RLock());
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::CheckFastTransportSize(const SafeObjType &entry, uint64_t expectedDataSize,
                                                         const std::string &objectKey, bool isFastTransportEnabled,
                                                         GetObjectRemoteRspPb &rsp)
{
    if (isFastTransportEnabled && entry->GetDataSize() != expectedDataSize) {
        rsp.mutable_error()->set_error_code(StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
        rsp.set_data_size(static_cast<int64_t>(entry->GetDataSize()));
        INJECT_POINT("WorkerWorkerOCServiceImpl.GetObjectRemoteImpl.changeDataSize", [&rsp](int64_t size) {
            rsp.set_data_size(size);
            return Status::OK();
        });
        rsp.set_create_time(static_cast<int64_t>(entry->GetCreateTime()));
        rsp.set_life_state(static_cast<uint32_t>(entry->GetLifeState()));
        RETURN_STATUS_LOG_ERROR(K_OC_REMOTE_GET_NOT_ENOUGH,
                                FormatString("[ObjectKey %s] data size mismatch, actual = %zu, expected = %zu",
                                             objectKey, entry->GetDataSize(), expectedDataSize));
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::WriteViaFastTransport(
    const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, SafeObjType &entry, std::shared_ptr<ShmUnit> shmUnit,
    uint64_t localSegAddress, uint64_t localSegSize, uint64_t offset, uint64_t size, bool blocking,
    std::vector<uint64_t> &eventKeys, const std::shared_ptr<AggregateMemory> &batchPtr, bool isFastTransportEnabled,
    bool isPipelineH2DRequest, BatchRh2dContext *batchRh2dContext, Status &fastTransportStatus,
    std::string &fastTransportName)
{
    auto markFastTransferResult = [&rsp, isPipelineH2DRequest](const Status &status) {
        if (status.IsError()) {
            if (isPipelineH2DRequest) {
                return status;
            }
            CHECK_FAIL_RETURN_STATUS(FLAGS_enable_transport_fallback, status.GetCode(), status.GetMsg());
            return Status::OK();
        }
        rsp.set_data_source(datasystem::DataTransferSource::DATA_ALREADY_TRANSFERRED);
        return Status::OK();
    };

    if (isFastTransportEnabled) {
        if (batchRh2dContext != nullptr && batchRh2dContext->IsUrmaTcpFallback()) {
            fastTransportStatus = batchRh2dContext->urmaAcquireStatus;
            fastTransportName = "UrmaSendLaneAcquire";
            RETURN_IF_NOT_OK(markFastTransferResult(fastTransportStatus));
        } else if (batchPtr) {
            batchPtr->localSgeInfos.emplace_back(
                LocalSgeInfo{ .segAddr = localSegAddress,
                              .segSize = localSegSize,
                              .sgeAddr = uintptr_t(shmUnit->GetPointer()),
                              .readOffset = req.read_offset(),
                              .writeSize = Align4BitsCeiling(entry->GetDataSize() + entry->GetMetadataSize()),
                              .metaDataSize = 0,
                              .srcChipId = NumaIdToChipId(shmUnit->GetNumaId()) });
            rsp.set_data_source(datasystem::DataTransferSource::DATA_ALREADY_TRANSFERRED_MEMSET_META);
        } else if (IsUrmaEnabled()) {
            const uint8_t srcChipId = NumaIdToChipId(shmUnit->GetNumaId());
            const uint8_t dstChipId =
                req.urma_info().has_chip_id() ? static_cast<uint8_t>(req.urma_info().chip_id()) : INVALID_CHIP_ID;
            Status rc;
            UrmaWriteFailure failure;
            if (batchRh2dContext != nullptr && batchRh2dContext->sendLaneLease != nullptr) {
                rc = UrmaWritePayloadWithLane(
                    req.urma_info(), localSegAddress, localSegSize,
                    reinterpret_cast<uint64_t>(shmUnit->GetPointer()), offset, size, entry->GetMetadataSize(),
                    srcChipId, dstChipId, blocking, eventKeys, batchRh2dContext->sendLaneLease, nullptr, &failure);
            } else {
                rc = UrmaWritePayload(req.urma_info(), localSegAddress, localSegSize,
                                      reinterpret_cast<uint64_t>(shmUnit->GetPointer()), offset, size,
                                      entry->GetMetadataSize(), srcChipId, dstChipId, blocking, eventKeys, nullptr,
                                      &failure);
            }
            fastTransportStatus = rc;
            fastTransportName = "UrmaWrite";
            if (rc.IsError()) {
                RecordProviderUbWriteFailure(req, rc, localAddress_, rsp, &failure,
                                             ocClientWorkerSvc_->GetUbAdmission());
            }
            RETURN_IF_NOT_OK(markFastTransferResult(rc));
        } else if (IsUcpEnabled()) {
            auto rc = UcpPutPayload(req.ucp_info(), reinterpret_cast<uint64_t>(shmUnit->GetPointer()), offset, size,
                                    entry->GetMetadataSize(), blocking, eventKeys);
            fastTransportStatus = rc;
            fastTransportName = "UcpWrite";
            RETURN_IF_NOT_OK(markFastTransferResult(rc));
        }
    }
    return Status::OK();
}

void WorkerWorkerOCServiceImpl::RecordProviderUbWriteFailure(const GetObjectRemoteReqPb &req, const Status &status,
                                                             const HostPort &operatorWorker, GetObjectRemoteRspPb &rsp,
                                                             const UrmaWriteFailure *failure,
                                                             PeerUbAdmission *ubAdmission)
{
    const auto &address = req.urma_info().request_address();
    HostPort failedEndpoint(address.host(), address.port());
    const std::string failedEndpointIdentity =
        failedEndpoint.Empty() && !req.urma_info().client_id().empty()
            ? "client_id=" + req.urma_info().client_id()
            : failedEndpoint.ToString();
    auto &detail = *rsp.mutable_provider_ub_failure_detail();
    if (failure != nullptr) {
        FillProviderUbFailureDetail(status, failedEndpointIdentity, operatorWorker.ToString(),
                                    failure->providerStatus, failure->cqeStatus, detail);
    } else {
        FillProviderUbFailureDetail(status, failedEndpointIdentity, operatorWorker.ToString(), std::nullopt,
                                    std::nullopt, detail);
    }
    ReportProviderLocalUbWriteFailure(
        ubAdmission, operatorWorker, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK, status,
        failure == nullptr ? std::nullopt : failure->providerStatus,
        failure == nullptr ? std::nullopt : failure->cqeStatus);
}

bool WorkerWorkerOCServiceImpl::TryEncodeProviderUbFailureResponse(const Status &status, GetObjectRemoteRspPb &rsp)
{
    if (status.IsOk() || !rsp.has_provider_ub_failure_detail()) {
        return false;
    }
    rsp.mutable_error()->set_error_code(status.GetCode());
    rsp.mutable_error()->set_error_msg(status.GetMsg());
    return true;
}

Status WorkerWorkerOCServiceImpl::ProcessFallbackTrackError(const Status &rc, const Status &fastTransportStatus,
                                                            bool blocking, Status *fallbackStatus,
                                                            bool &canPrepareFallbackPayload,
                                                            const std::string &objectKey)
{
    if (!rc.IsError()) {
        return Status::OK();
    }
    bool nonBlockingOk = fastTransportStatus.IsOk() && !blocking;
    if (nonBlockingOk && fallbackStatus != nullptr) {
        *fallbackStatus = rc;
    }
    if (nonBlockingOk) {
        canPrepareFallbackPayload = false;
        return Status::OK();
    }
    LOG(WARNING) << FormatString("Worker-to-worker TCP fallback payload rejected for object %s: %s", objectKey,
                                 rc.ToString());
    return rc;
}

Status WorkerWorkerOCServiceImpl::HandlePayloadFallback(
    const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, SafeObjType &entry, std::vector<RpcMessage> &outPayload,
    ShmGuard &shmGuard, std::shared_ptr<ShmUnit> shmUnit, Status &fastTransportStatus,
    const std::string &fastTransportName, const std::string &objectKey, bool isUrmaFastTransport,
    bool isPipelineH2DRequest, bool blocking, const std::shared_ptr<AggregateMemory> &batchPtr, Status *fallbackStatus,
    RemoteH2DRootInfoPb *batchRootInfo, BatchRh2dContext *batchRh2dContext, const ReadObjectKV &objKv,
    uint64_t localSegAddress, uint64_t localSegSize)
{
    if (!isPipelineH2DRequest && IsRemoteH2DEnabled() && !req.comm_id().empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            EstablishConnAndFillSeg(req.comm_id(), localSegAddress, localSegSize, shmUnit, entry->GetMetadataSize(),
                                    rsp, batchRootInfo, batchRh2dContext),
            "");
        rsp.set_data_source(datasystem::DataTransferSource::DATA_DELAY_TRANSFER);
    }

    const bool skipTcpPayload = isPipelineH2DRequest || (IsRemoteH2DEnabled() && !req.comm_id().empty());
    if ((!IsFastTransportEnabled() || !blocking) && !skipTcpPayload) {
        bool canPrepareFallbackPayload = true;
        if (FLAGS_enable_transport_fallback && (fastTransportStatus.IsError() || (!blocking && batchPtr == nullptr))
            && isUrmaFastTransport) {
            auto trackStatus = fastTransportStatus.IsOk()
                                   ? Status(StatusCode::K_URMA_ERROR, "URMA wait fallback payload precheck")
                                   : fastTransportStatus;
            auto rc = shmGuard.TrackUrmaFallbackTcp(objKv.GetReadSize(), trackStatus, "worker->worker");
            if (rc.IsError() && rsp.has_provider_ub_failure_detail()) {
                UpdateProviderUbFailureDetailForWrappedStatus(trackStatus, rc,
                                                              *rsp.mutable_provider_ub_failure_detail());
            }
            RETURN_IF_NOT_OK(ProcessFallbackTrackError(rc, fastTransportStatus, blocking, fallbackStatus,
                                                       canPrepareFallbackPayload, objectKey));
        }
        if (canPrepareFallbackPayload) {
            // Per-object-in-batch fallback diagnostic. Under a sustained URMA fault every object in every
            // remote-get batch falls back here. The fast-transport failure root cause is already recorded in
            // the response's provider_ub_failure_detail above (UpdateProviderUbFailureDetailForWrappedStatus),
            // so this WARN is only the fallback notice -- throttle it like the batch-level fallback log,
            // not a root-cause signal.
            LOG_IF_EVERY_N(WARNING, fastTransportStatus.IsError(), K_URMA_WARNING_LOG_EVERY_N)
                << FormatString("%s[%s] fallback to tcp, rc = %s", fastTransportName, objectKey,
                                fastTransportStatus.ToString());
            RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayload, objKv.GetReadOffset(), objKv.GetReadSize()));
        }
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteImpl(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                      std::vector<RpcMessage> &outPayload, bool blocking,
                                                      std::vector<uint64_t> &eventKeys,
                                                      std::shared_ptr<AggregateMemory> batchPtr,
                                                      RemoteH2DRootInfoPb *batchRootInfo, Status *fallbackStatus,
                                                      BatchRh2dContext *batchRh2dContext, bool isQueryAndGet)
{
    // Outer scope (BatchGetObjectRemote/GetObjectRemote) already created ScopedRequestContext.
    // No nested scope here: workerTimeCost is a value member not inherited by inner scopes.
    PerfPoint batchImplPoint(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_IMPL);
    (void)eventKeys;
    (void)blocking;
    const std::string &objectKey = req.object_key();
    const bool tryLock = req.try_lock();
    const uint64_t version = req.version();
    const uint64_t offset = req.read_offset();
    const uint64_t size = req.read_size();
    const uint64_t expectedDataSize = req.data_size();
    std::shared_ptr<SafeObjType> safeEntry;

    Status rc = Status::OK();
    INJECT_POINT("worker.batch_get_failure_for_keys", [&objectKey, &rc]() {
        if (objectKey == "key2") {
            rc = Status(K_RUNTIME_ERROR, "Injected K_RUNTIME_ERROR");
        } else if (objectKey == "key3") {
            rc = Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "Injected K_WORKER_PULL_OBJECT_NOT_FOUND");
        } else if (objectKey == "key0") {
            rc = Status(K_OUT_OF_MEMORY, "Injected K_OUT_OF_MEMORY");
        }
        return Status::OK();
    });
    RETURN_IF_NOT_OK(rc);

    // Timing: SafeObject RLock blocked by drain WLock → breakdown "RemoteLockEntry: Nms".
    {
        Timer lockEntryTimer;
        RETURN_IF_NOT_OK(LockEntryForRemoteGet(objectKey, tryLock, version, safeEntry));
        GetWorkerTimeCost().Append("RemoteLockEntry", lockEntryTimer.ElapsedMilliSecond());
    }
    Raii raii([safeEntry]() { safeEntry->RUnlock(); });
    auto &entry = *safeEntry;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!entry->stateInfo.IsCacheInvalid() && !entry->IsInvalid(), K_INVALID,
                                         FormatString("[ObjectKey %s] is invalid", objectKey));
    LOG_IF(WARNING, entry->GetCreateTime() != version) << FormatString(
        "[ObjectKey %s] Version: %ld, require version: %ld", objectKey, entry->GetCreateTime(), version);

    const bool isUrmaFastTransport = IsUrmaEnabled() && req.has_urma_info();
    const bool isPipelineH2DRequest = isUrmaFastTransport && OsXprtPipln::IsPiplnH2DRequest(req.urma_info());
    bool isFastTransportEnabled = isUrmaFastTransport || (IsUcpEnabled() && req.has_ucp_info());
    RETURN_IF_NOT_OK(CheckFastTransportSize(entry, expectedDataSize, objectKey, isFastTransportEnabled, rsp));

    return LoadPayloadAndFillResponse(req, rsp, entry, outPayload, objectKey, offset, size, blocking, eventKeys,
                                      batchPtr, batchRootInfo, batchRh2dContext, fallbackStatus, isFastTransportEnabled,
                                      isUrmaFastTransport, isPipelineH2DRequest, batchImplPoint, isQueryAndGet);
}

Status WorkerWorkerOCServiceImpl::CheckCoordinatorState(const CheckCoordinatorStateReqPb &req,
                                                        CheckCoordinatorStateRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(coordinationAvailable_), K_NOT_READY,
                             "Coordination availability provider is not initialized.");
    bool isCoordinationAvailable = coordinationAvailable_();
    rsp.set_available(isCoordinationAvailable);
    LOG_IF(INFO, isCoordinationAvailable) << "Coordination backend is available";
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetClusterState(const GetClusterStateReqPb &req, GetClusterStateRspPb &rsp)
{
    ScopedRequestContext ctx;
    INJECT_POINT("WorkerWorkerOCServiceImpl.GetClusterState.returnError");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(backendObservationProvider_), K_NOT_READY,
                             "Control-backend observation provider is not initialized.");
    RETURN_IF_NOT_OK(FillGetClusterStateRspPbFromControlBackendObservation(backendObservationProvider_(), rsp));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetPeerHashRing(const GetHashRingReqPb &req, GetHashRingRspPb &rsp)
{
    RETURN_RUNTIME_ERROR_IF_NULL(ocClientWorkerSvc_);
    return ocClientWorkerSvc_->GetHashRing(req, rsp);
}

Status WorkerWorkerOCServiceImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                              std::vector<::datasystem::RpcMessage> payloads)
{
    ScopedRequestContext ctx;
    return ocClientWorkerSvc_->MigrateData(req, rsp, std::move(payloads));
}

Status WorkerWorkerOCServiceImpl::MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    ScopedRequestContext ctx;
    return ocClientWorkerSvc_->MigrateDataDirect(req, rsp);
}

Status WorkerWorkerOCServiceImpl::CheckConnectionStable(const GetObjectRemoteReqPb &req)
{
    const bool isUrmaRequest = IsUrmaEnabled() && req.has_urma_info();
    const bool isUcpRequest = IsUcpEnabled() && req.has_ucp_info();
    const bool isFastTransportEnabled = isUrmaRequest || isUcpRequest;
    if (!isFastTransportEnabled) {
        return Status::OK();
    }
    std::string host;
    int port = 0;
    if (req.has_urma_info()) {
        host = req.urma_info().request_address().host();
        port = req.urma_info().request_address().port();
    }
    if (req.has_ucp_info()) {
        host = req.ucp_info().remote_ip_addr().host();
        port = req.ucp_info().remote_ip_addr().port();
    }
    const HostPort requestAddress(host, port);
    const std::string requestAddressStr = requestAddress.ToString();
    const bool isClientUrmaRequest = isUrmaRequest && !req.urma_info().client_id().empty();
    const std::string &remoteConnectionId =
        isClientUrmaRequest ? req.urma_info().client_id() : requestAddressStr;
    auto rc = CheckTransportConnectionStable(remoteConnectionId, req.urma_instance_id());
    if (rc.IsError() && rc.GetCode() == K_URMA_NEED_CONNECT) {
        std::string remoteWorkerId = "UNKNOWN";
        cluster::MemberEndpoint remoteEndpoint;
        if (!isClientUrmaRequest && membership_.ResolveByAddress(requestAddressStr, remoteEndpoint).IsOk()
            && !remoteEndpoint.identity.id.empty()) {
            remoteWorkerId = remoteEndpoint.identity.id;
        }
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_NEED_CONNECT] CheckConnectionStable failed, remoteAddress=" << requestAddressStr
            << ", remoteWorkerId=" << remoteWorkerId
            << ", remoteInstanceId=" << (req.urma_instance_id().empty() ? "UNKNOWN" : req.urma_instance_id())
            << ", rc=" << rc.ToString();
    }
    return rc;
}

Status WorkerWorkerOCServiceImpl::BatchGetObjectRemote(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<BatchGetObjectRemoteRspPb, BatchGetObjectRemoteReqPb>>
        serverApi)
{
    ScopedRequestContext ctx;
    Timer timer;
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY);
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    BatchGetObjectRemoteReqPb req;
    BatchGetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    INJECT_POINT("worker.BatchGetObjectRemote.afterRead");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    HostPort requestAddress;
    const std::string callerAddress =
        GetRemoteAddressFromBatchGetReq(req, requestAddress).IsOk() ? requestAddress.ToString() : "";
    const auto &firstObjectKey = req.requests_size() > 0 ? req.requests(0).object_key() : "";
    auto realRemainingTime = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
    VLOG(1) << AppendSrcDstForLog(
        FormatString("[Get/RemotePull] Receive, count: %d, remainingTime: %zu", req.requests_size(), realRemainingTime),
        callerAddress, FLAGS_worker_address);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    auto prepareRc = PrepareBatchGetObjectRemoteReq(req);
    if (prepareRc.IsError()) {
        LogBatchGetObjectRemotePrepareFailed(req, callerAddress, firstObjectKey, prepareRc);
        return prepareRc;
    }
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::DATA_REMOTEGET_START);
    }
    RETURN_IF_NOT_OK(BatchGetObjectRemoteImpl(req, rsp, payload));
    TryEncodeRemoteGetLatencySummary(config, traceEnabled, rsp);
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                     "GetObjectRemote send payload error");
    pointImpl.Record();
    LogBatchGetObjectRemoteFinish(config, req, payload, firstObjectKey, realRemainingTime, callerAddress, timer);
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::PrepareBatchGetObjectRemoteReq(BatchGetObjectRemoteReqPb &req)
{
    CHECK_FAIL_RETURN_STATUS(req.requests_size() > 0, K_INVALID, "BatchGetObjectRemote request is empty");
    auto *singleReq = req.mutable_requests(0);
    *(singleReq->mutable_urma_instance_id()) = req.urma_instance_id();
    return CheckConnectionStable(*singleReq);
}

Status WorkerWorkerOCServiceImpl::BatchGetObjectRemoteImpl(BatchGetObjectRemoteReqPb &req,
                                                           BatchGetObjectRemoteRspPb &rsp,
                                                           std::vector<RpcMessage> &payload)
{
    PerfPoint point(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE);
    BatchRh2dContext batchTransportContext;
    if (IsUrmaEnabled()) {
        for (const auto &subReq : req.requests()) {
            if (subReq.has_urma_info()) {
                // Acquire before any sub-request starts. This is the only
                // pool acquisition for the whole worker-to-worker Batch Get RPC.
                auto acquireRc = AcquireUrmaSendLane(subReq.urma_info(), batchTransportContext.sendLaneLease);
                if (acquireRc.IsOk()) {
                    batchTransportContext.urmaTransportMode = BatchRh2dContext::UrmaTransportMode::SHARED_LANE;
                    // Test synchronization point after the RPC owns its lane and before any sub-request can post a WR.
                    INJECT_POINT("WorkerWorkerOCServiceImpl.BatchGetAfterAcquireSendLane");
                } else if (FLAGS_enable_transport_fallback) {
                    // Pin the whole RPC to TCP before object processing begins. Propagate the original acquire status
                    // through HandlePayloadFallback so existing fallback accounting and admission control still apply.
                    batchTransportContext.urmaTransportMode = BatchRh2dContext::UrmaTransportMode::TCP_FALLBACK;
                    batchTransportContext.urmaAcquireStatus = acquireRc;
                } else {
                    return acquireRc;
                }
                break;
            }
        }
    }
    bool sendLaneSealed = false;
    auto sealBatchLane = [&]() {
        if (sendLaneSealed || batchTransportContext.sendLaneLease == nullptr) {
            return Status::OK();
        }
        sendLaneSealed = true;
        return SealUrmaSendLaneLease(batchTransportContext.sendLaneLease);
    };
    Raii sealOnExit([&sealBatchLane]() {
        LOG_IF_ERROR(sealBatchLane(), "Failed to seal worker-to-worker Batch Get URMA lane");
    });

    std::vector<ParallelRes> parallelRes;
    const bool isPipelineH2DRequest = OsXprtPipln::IsPiplnH2DRequest(req);
    const bool useParallelBatchGet = !isPipelineH2DRequest
                                     && req.requests_size() > FLAGS_oc_worker_worker_parallel_min
                                     && IsFastTransportEnabled();
    if (useParallelBatchGet) {
        RETURN_IF_NOT_OK(ParallelBatchGetObject(req, rsp, parallelRes, batchTransportContext));
    } else {
        uint32_t parallelSize = 1;
        parallelRes.resize(parallelSize);
        PerfPoint loopPoint(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_IMPL_LOOP_SERIAL);
        BatchRh2dContext batchRh2dContext = batchTransportContext;
        if (req.requests_size() > 0) {
            auto *firstReq = req.mutable_requests(0);
            *(firstReq->mutable_comm_id()) = req.comm_id();
            RETURN_IF_NOT_OK(PrepareBatchRh2dContext(*firstReq, batchRh2dContext));
        }
        for (int i = 0; i < req.requests_size(); i++) {
            auto *subReq = req.mutable_requests(i);
            *(subReq->mutable_comm_id()) = req.comm_id();
            (void)GetObjectRemoteBatchWrite(parallelSize - 1, *subReq, rsp, parallelRes, nullptr, &batchRh2dContext);
        }
        loopPoint.Record();
    }

    // No new WR can be created after the batch processing loops return. Keep
    // completion ownership in the events, and let the shared lease settle only
    // after this single Seal call. Object-level WR failures intentionally do
    // not request retirement; their cleanup follows release semantics.
    RETURN_IF_NOT_OK(sealBatchLane());
    return MergeParallelBatchGetResult(req, parallelRes, rsp, payload);
}

Status WorkerWorkerOCServiceImpl::MergeParallelBatchGetResult(const BatchGetObjectRemoteReqPb &req,
                                                              std::vector<ParallelRes> &parallelRes,
                                                              BatchGetObjectRemoteRspPb &rsp,
                                                              std::vector<RpcMessage> &payload)
{
    uint64_t index = 0;
    for (auto &loc : parallelRes) {
        for (auto &resp : loc.respPbs) {
            rsp.add_responses()->Swap(&resp);
        }

        for (size_t kpIndex = 0; kpIndex < loc.kps.size(); ++kpIndex) {
            auto &kp = loc.kps[kpIndex];
            Status fallbackStatus = kpIndex < loc.fallbackStatuses.size() ? loc.fallbackStatuses[kpIndex] : Status();
            const bool isSingleBatchKp = (loc.kps.size() == 1 && loc.respPbs.size() > 1);
            const uint64_t coveredRespNum = isSingleBatchKp ? static_cast<uint64_t>(loc.respPbs.size()) : 1;
            const bool singleFallback = kp.second.first.empty() && !kp.second.second.empty();
            if (singleFallback) {
                MovePayload(kp.second.second, payload);
                index++;
                continue;
            }

            const bool batchFallback = kp.second.first.empty() && kp.second.second.empty();
            if (batchFallback) {
                if (fallbackStatus.IsError()) {
                    // fallbackStatus is populated only when transport fallback is enabled and the limiter rejects it.
                    LOG(WARNING) << "Worker-to-worker TCP fallback payload rejected: " << fallbackStatus.ToString();
                    rsp.mutable_responses()->at(index).mutable_error()->set_error_code(fallbackStatus.GetCode());
                    rsp.mutable_responses()->at(index).mutable_error()->set_error_msg(fallbackStatus.GetMsg());
                    index += coveredRespNum;
                    continue;
                }
                MovePayload(loc.fallbackPayloads, payload);
                index += coveredRespNum;
                continue;
            }

            BatchWaitContext context{ req, loc, kp, rsp, payload, index, coveredRespNum, fallbackStatus };
            RETURN_IF_NOT_OK(WaitFastTransportAndFallback(context));
        }

        loc.kps.clear();
        loc.fallbackPayloads.clear();
        loc.fallbackStatuses.clear();
    }
    return Status::OK();
}

void WorkerWorkerOCServiceImpl::SetBatchResponseError(BatchGetObjectRemoteRspPb &rsp, uint64_t begin, uint64_t count,
                                                      const Status &status)
{
    const uint64_t responseCount = static_cast<uint64_t>(rsp.responses_size());
    if (begin >= responseCount) {
        return;
    }
    const uint64_t affectedCount = std::min(count, responseCount - begin);
    for (uint64_t offset = 0; offset < affectedCount; ++offset) {
        auto *error = rsp.mutable_responses(static_cast<int>(begin + offset))->mutable_error();
        error->set_error_code(status.GetCode());
        error->set_error_msg(status.GetMsg());
    }
}

void WorkerWorkerOCServiceImpl::RecordBatchProviderFailure(BatchWaitContext &context, const Status &status,
                                                           const UrmaWriteFailure &failure)
{
    const auto &req = context.req;
    const uint64_t index = context.responseIndex;
    if (index >= static_cast<uint64_t>(req.requests_size()) || !req.requests(index).has_urma_info()) {
        return;
    }
    const uint64_t detailCount = std::min(context.coveredResponseCount,
                                          static_cast<uint64_t>(req.requests_size()) - index);
    for (uint64_t detailIndex = 0; detailIndex < detailCount; ++detailIndex) {
        RecordProviderUbWriteFailure(
            req.requests(index + detailIndex), status, localAddress_,
            *context.rsp.mutable_responses()->Mutable(static_cast<int>(index + detailIndex)), &failure,
            detailIndex == 0 ? ocClientWorkerSvc_->GetUbAdmission() : nullptr);
    }
}

void WorkerWorkerOCServiceImpl::MoveBatchFallbackPayload(BatchWaitContext &context)
{
    auto &eventPayloads = context.eventPayload.second.second;
    if (eventPayloads.empty()) {
        MovePayload(context.parallelResult.fallbackPayloads, context.payload);
        for (uint64_t offset = 0; offset < context.coveredResponseCount; ++offset) {
            context.rsp.mutable_responses()
                ->at(context.responseIndex + offset)
                .set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
        }
        context.parallelResult.fallbackPayloads.clear();
        return;
    }
    MovePayload(eventPayloads, context.payload);
    context.rsp.mutable_responses()
        ->at(context.responseIndex)
        .set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
    eventPayloads.clear();
}

Status WorkerWorkerOCServiceImpl::HandleBatchWaitFailure(BatchWaitContext &context, Status &status,
                                                         const UrmaWriteFailure &failure)
{
    RecordBatchProviderFailure(context, status, failure);
    if (context.fallbackStatus.IsError()) {
        HostPort requestAddress;
        LOG_IF_ERROR(GetRemoteAddressFromBatchGetReq(context.req, requestAddress),
                     "GetRemoteAddressFromBatchGetReq failed");
        LOG(WARNING) << FormatString(
            "Worker-to-worker TCP fallback payload rejected, srcAddress = %s, targetAddress = %s, "
            "wait rc = %s, fallback rc = %s",
            localAddress_.ToString(), requestAddress.ToString(), status.ToString(),
            context.fallbackStatus.ToString());
        auto wrappedStatus = context.fallbackStatus;
        wrappedStatus.AppendMsg(status.GetMsg());
        SetBatchResponseError(context.rsp, context.responseIndex, context.coveredResponseCount, wrappedStatus);
        return wrappedStatus;
    }
    if (!FLAGS_enable_transport_fallback) {
        SetBatchResponseError(context.rsp, context.responseIndex, context.coveredResponseCount, status);
        return status;
    }

    MoveBatchFallbackPayload(context);
    HostPort requestAddress;
    LOG_IF_ERROR(GetRemoteAddressFromBatchGetReq(context.req, requestAddress),
                 "GetRemoteAddressFromBatchGetReq failed");
    // Throttle the per-batch fallback diagnostic: under a sustained URMA provider fault every
    // BatchGetObjectRemote RPC falls back to TCP here, emitting one WARN per batch. Reuse the file's
    // K_URMA_WARNING_LOG_EVERY_N convention (also used at the URMA_NEED_CONNECT site above).
    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << FormatString("fallback to tcp, srcAddress = %s, targetAddress = %s, rc = %s",
                        localAddress_.ToString(), requestAddress.ToString(), status.ToString());
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::WaitFastTransportAndFallback(BatchWaitContext &context)
{
    auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
    UrmaWriteFailure failure;
    auto errorHandler = [&context, &failure, this](Status &status) {
        return status.IsError() ? HandleBatchWaitFailure(context, status, failure) : status;
    };
    (void)WaitFastTransportEventWithFailure(context.eventPayload.second.first, remainingTime, errorHandler, &failure);
    context.responseIndex += context.coveredResponseCount;
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::ParallelBatchGetObject(
    BatchGetObjectRemoteReqPb &req, BatchGetObjectRemoteRspPb &rsp, std::vector<ParallelRes> &parallelRes,
    const BatchRh2dContext &batchTransportContext)
{
    LOG(INFO) << PIPLN_LOG_PREFIX "Enter ParallelBatchGetObject: requestCount=" << req.requests_size()
              << ", parallelNums=" << FLAGS_oc_worker_worker_parallel_nums;
    tbb::task_arena limited;
    if (FLAGS_oc_worker_worker_parallel_nums > 0) {
        limited.initialize(FLAGS_oc_worker_worker_parallel_nums);
    }

    AggregateInfo info;
    CHECK_FAIL_RETURN_STATUS(PrepareAggregateMemory(req, info), K_RUNTIME_ERROR, "Prepare Memory failed");
    // An acquire failure selects TCP for the whole RPC. Disable aggregate SGE construction as well as GatherWrite;
    // otherwise the aggregate path would either post URMA or lose the per-object TCP payloads.
    if (batchTransportContext.IsUrmaTcpFallback()) {
        info.canBatchHandler = false;
    }
    uint64_t parallelSize = info.canBatchHandler ? info.batchReqSize.size() : req.requests_size();

    parallelRes.resize(parallelSize);
    limited.execute([&] {
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, parallelSize), [&](const tbb::blocked_range<uint64_t> &r) {
            PerfPoint totalTime(PerfKey::WORKER_PARALLEL_BATCH_ASYNC_SEND);
            for (uint64_t i = r.begin(); i != r.end(); ++i) {
                uint64_t startPos = info.canBatchHandler ? info.batchStartIndex[i] : i;
                uint64_t endPos = info.canBatchHandler ? startPos + info.batchReqSize[i] : startPos + 1;
                std::shared_ptr<AggregateMemory> batchPtr = nullptr;
                if (info.canBatchHandler) {
                    batchPtr = std::make_shared<AggregateMemory>();
                    batchPtr->localSgeInfos.reserve(endPos - startPos);
                }
                BatchRh2dContext batchRh2dContext = batchTransportContext;
                auto *firstReq = req.mutable_requests(startPos);
                *(firstReq->mutable_comm_id()) = req.comm_id();
                LOG_IF_ERROR(PrepareBatchRh2dContext(*firstReq, batchRh2dContext), "PrepareBatchRh2dContext failed");
                PerfPoint loopPoint(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_IMPL_LOOP_PARALLEL);
                for (uint64_t j = startPos; j < endPos; ++j) {
                    auto *subReq = req.mutable_requests(j);
                    *(subReq->mutable_comm_id()) = req.comm_id();
                    GetObjectRemoteBatchWrite(i, *subReq, rsp, parallelRes, batchPtr, &batchRh2dContext);
                }
                loopPoint.Record();
                PerfPoint pDo(PerfKey::URMA_GATHER_WRITE_DO);
                LOG_IF_ERROR(GatherWrite(i, info, batchPtr, parallelRes, req, batchTransportContext.sendLaneLease),
                             "gather write error!");
            }
        });
    });

    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::NotifyRemoteGet(const NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp)
{
    ScopedRequestContext ctx;
    LOG(INFO) << PIPLN_LOG_PREFIX "NotifyRemoteGet request: object_count=" << req.object_keys_size();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->NotifyRemoteGet(req, rsp), "NotifyRemoteGet failed");
    LOG(INFO) << PIPLN_LOG_PREFIX "NotifyRemoteGet success";
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
