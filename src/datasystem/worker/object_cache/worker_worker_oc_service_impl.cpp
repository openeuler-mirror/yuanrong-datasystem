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

#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>

#include "datasystem/worker/worker_topology_references.h"

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
    return req.has_urma_info() && req.object_key().rfind(URMA_WARMUP_KEY_PREFIX, 0) == 0 && !req.try_lock()
           && req.version() == 0 && req.read_offset() == 0 && req.read_size() == URMA_WARMUP_OBJECT_SIZE
           && req.data_size() == URMA_WARMUP_OBJECT_SIZE && req.comm_id().empty();
}

bool IsCoordinationBackendAvailable(EtcdStore *etcdStore, cluster::ICoordinationBackend *coordinationBackend)
{
    if (coordinationBackend != nullptr) {
        return !coordinationBackend->IsKeepAliveTimeout();
    }
    if (etcdStore != nullptr) {
        return !etcdStore->IsKeepAliveTimeout();
    }
    LOG(WARNING) << "Coordination backend is unavailable: both EtcdStore and CoordinationBackend are not initialized.";
    return false;
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
    SLOW_LOG_IF_OR_VLOG(
        INFO, config.processSlowerThanUs > 0 && elapsedUs >= config.processSlowerThanUs, 1,
        AppendSrcDstForLog(
            FormatString("[Get/RemotePull] finish, count: %d, firstObjectKey: %s, payload size: %zu, start "
                         "remainingTime: %zu, cost: %.3fms",
                         req.requests_size(), firstObjectKey, payload.size(), realRemainingTime, elapsedMs),
            callerAddress, FLAGS_worker_address));
}
}  // namespace

WorkerWorkerOCServiceImpl::WorkerWorkerOCServiceImpl(
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc, std::shared_ptr<AkSkManager> akSkManager,
    EtcdStore *etcdStore, cluster::ICoordinationBackend *coordinationBackend,
    worker::WorkerTopologyReferences *topologyEngine, BackendObservationProvider backendObservationProvider)
    : ocClientWorkerSvc_(std::move(clientSvc)),
      akSkManager_(std::move(akSkManager)),
      etcdStore_(etcdStore),
      coordinationBackend_(coordinationBackend),
      topologyEngine_(topologyEngine),
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
    // is different from the request
    RETURN_IF_NOT_OK_EXCEPT(GetObjectRemote(req, rsp, payload), StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
    TryEncodeRemoteGetLatencySummary(config, traceEnabled, rsp);
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);

    if (rsp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED
        || rsp.data_source() == DataTransferSource::DATA_DELAY_TRANSFER
        || rsp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED_MEMSET_META) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload({}, FLAGS_oc_worker_worker_direct_port > 0),
                                         "GetObjectRemote send payload error");
    } else if (rsp.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                         "GetObjectRemote send payload error");
    }

    pointImpl.Record();
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    SLOW_LOG_IF_OR_VLOG(
        INFO, config.processSlowerThanUs > 0 && elapsedUs >= config.processSlowerThanUs, 1,
        AppendSrcDstForLog(FormatString("[GetObjectRemote] finish, objectKey: %s, payload size: %zu, cost: %.3fms",
                                        req.object_key(), payload.size(), elapsedMs),
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
    if (isQueryAndGet) {
        RETURN_IF_NOT_OK(CheckConnectionStable(req));
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const std::string callerAddress = GetRemoteAddressForLog(req);
    LOG(INFO) << AppendSrcDstForLog(FormatString("Processing pull object[%s] offset[%ld] size[%ld]", req.object_key(),
                                                 req.read_offset(), req.read_size()),
                                    callerAddress, FLAGS_worker_address);
    std::vector<uint64_t> eventKeys;
    RETURN_IF_NOT_OK(GetObjectRemoteHandler(req, rsp, payload, true, eventKeys, nullptr, nullptr, nullptr,
                                            nullptr, isQueryAndGet));
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
    uint64_t reqSize = req.requests_size();

    info.canBatchHandler = true;
    // ceil data;
    info.batchReqSize.clear();
    info.batchStartIndex.clear();
    info.batchSizes.clear();

    uint64_t batchReqSize = 0;
    uint64_t batchCap = 0;
    uint64_t batchStartIndex = 0;
    const uint64_t batchLimitKeys = 1024;  // must same as obj_cache_shm_unit in req side.
    uint64_t metadataSize = ocClientWorkerSvc_->GetMetadataSize();

    if (OsXprtPipln::IsPiplnH2DRequest(req)) {
        // pipeline rh2d doesn't support aggregate memory now
        info.canBatchHandler = false;
        return Status::OK();
    }

    for (uint64_t i = 0; i < reqSize; i++) {
        uint64_t dataSize = req.requests(i).data_size();
        if (dataSize > FLAGS_oc_worker_aggregate_single_max) {
            info.canBatchHandler = false;
            return Status::OK();
        }
        uint64_t needSize = dataSize + metadataSize;
        uint64_t ceilingSize = Align4BitsCeiling(needSize);
        if (batchCap + ceilingSize > FLAGS_oc_worker_aggregate_merge_size || batchReqSize >= batchLimitKeys) {
            info.batchStartIndex.emplace_back(batchStartIndex);
            info.batchSizes.emplace_back(batchCap);
            info.batchReqSize.emplace_back(batchReqSize);

            batchCap = 0;
            batchReqSize = 0;
            batchStartIndex = i;
        }

        batchReqSize++;
        batchCap += ceilingSize;
    }

    if (batchReqSize > 0) {
        info.batchStartIndex.emplace_back(batchStartIndex);
        info.batchSizes.emplace_back(batchCap);
        info.batchReqSize.emplace_back(batchReqSize);
    }

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
        for (int64_t i = startPos; i < startPos + info.batchReqSize[subIndex]; ++i) {
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
    INJECT_POINT("worker.worker_worker_remote_get_sleep");
    INJECT_POINT("worker.worker_worker_remote_get_failure");
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "objectKey is empty.");
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
            RETURN_IF_NOT_OK(shmGuard.TryRLatch());
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

        RETURN_IF_NOT_OK(WriteViaFastTransport(req, rsp, entry, shmUnit, localSegAddress, localSegSize, offset, size,
                                               blocking, eventKeys, batchPtr, isFastTransportEnabled,
                                               isPipelineH2DRequest, batchRh2dContext, fastTransportStatus,
                                               fastTransportName));
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
        if (batchPtr) {
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
            if (batchRh2dContext != nullptr && batchRh2dContext->sendLaneLease != nullptr) {
                rc = UrmaWritePayloadWithLane(
                    req.urma_info(), localSegAddress, localSegSize,
                    reinterpret_cast<uint64_t>(shmUnit->GetPointer()), offset, size, entry->GetMetadataSize(), srcChipId,
                    dstChipId, blocking, eventKeys, batchRh2dContext->sendLaneLease);
            } else {
                rc = UrmaWritePayload(req.urma_info(), localSegAddress, localSegSize,
                                      reinterpret_cast<uint64_t>(shmUnit->GetPointer()), offset, size,
                                      entry->GetMetadataSize(), srcChipId, dstChipId, blocking, eventKeys);
            }
            fastTransportStatus = rc;
            fastTransportName = "UrmaWrite";
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
            RETURN_IF_NOT_OK(ProcessFallbackTrackError(rc, fastTransportStatus, blocking, fallbackStatus,
                                                       canPrepareFallbackPayload, objectKey));
        }
        if (canPrepareFallbackPayload) {
            LOG_IF(WARNING, fastTransportStatus.IsError()) << FormatString(
                "%s[%s] fallback to tcp, rc = %s", fastTransportName, objectKey, fastTransportStatus.ToString());
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
    ScopedRequestContext ctx;
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

    RETURN_IF_NOT_OK(LockEntryForRemoteGet(objectKey, tryLock, version, safeEntry));
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
    bool isCoordinationAvailable = IsCoordinationBackendAvailable(etcdStore_, coordinationBackend_);
    rsp.set_available(isCoordinationAvailable);
    LOG_IF(INFO, isCoordinationAvailable) << "Coordination backend is available";
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetClusterState(const GetClusterStateReqPb &req, GetClusterStateRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    bool isCoordinationAvailable = IsCoordinationBackendAvailable(etcdStore_, coordinationBackend_);
    rsp.set_coordinator_available(isCoordinationAvailable);
    RETURN_RUNTIME_ERROR_IF_NULL(topologyEngine_);
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(backendObservationProvider_), K_NOT_READY,
                             "Control-backend observation provider is not initialized.");
    RETURN_IF_NOT_OK(FillGetClusterStateRspPbFromControlBackendObservation(backendObservationProvider_(), rsp));
    LOG_IF(INFO, isCoordinationAvailable) << "Coordination backend is available";
    return Status::OK();
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
        if (!isClientUrmaRequest && topologyEngine_ != nullptr && topologyEngine_->membership != nullptr
            && topologyEngine_->membership->ResolveByAddress(requestAddressStr, remoteEndpoint).IsOk()
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
    std::shared_ptr<UrmaSendLaneLease> sendLaneLease;
    if (IsUrmaEnabled()) {
        for (const auto &subReq : req.requests()) {
            if (subReq.has_urma_info()) {
                // Acquire before any sub-request starts. This is the only
                // pool acquisition for the whole worker-to-worker Batch Get RPC.
                RETURN_IF_NOT_OK(AcquireUrmaSendLane(subReq.urma_info(), sendLaneLease));
                break;
            }
        }
    }
    bool sendLaneSealed = false;
    auto sealBatchLane = [&]() {
        if (sendLaneSealed || sendLaneLease == nullptr) {
            return Status::OK();
        }
        sendLaneSealed = true;
        return SealUrmaSendLaneLease(sendLaneLease);
    };
    Raii sealOnExit([&sealBatchLane]() {
        LOG_IF_ERROR(sealBatchLane(), "Failed to seal worker-to-worker Batch Get URMA lane");
    });

    std::vector<ParallelRes> parallelRes;
    if (req.requests_size() > FLAGS_oc_worker_worker_parallel_min && IsFastTransportEnabled()) {
        RETURN_IF_NOT_OK(ParallelBatchGetObject(req, rsp, parallelRes, sendLaneLease));
    } else {
        uint32_t parallelSize = 1;
        parallelRes.resize(parallelSize);
        PerfPoint loopPoint(PerfKey::WORKER_SERVER_BATCH_GET_REMOTE_IMPL_LOOP_SERIAL);
        BatchRh2dContext batchRh2dContext;
        batchRh2dContext.sendLaneLease = sendLaneLease;
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

            RETURN_IF_NOT_OK(
                WaitFastTransportAndFallback(req, loc, kp, rsp, payload, index, coveredRespNum, fallbackStatus));
        }

        loc.kps.clear();
        loc.fallbackPayloads.clear();
        loc.fallbackStatuses.clear();
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::WaitFastTransportAndFallback(
    const BatchGetObjectRemoteReqPb &req, ParallelRes &loc,
    std::pair<uint64_t, std::pair<std::vector<uint64_t>, std::vector<RpcMessage>>> &kp, BatchGetObjectRemoteRspPb &rsp,
    std::vector<RpcMessage> &payload, uint64_t &index, uint64_t coveredRespNum, const Status &fallbackStatus)
{
    auto remainingTime = []() { return GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(); };
    const auto srcAddress = localAddress_.ToString();
    auto errorHandler = [&index, &rsp, &loc, &kp, &payload, coveredRespNum, &fallbackStatus, &srcAddress,
                         &req](Status &status) {
        const bool waitFailed = status.IsError();
        if (waitFailed) {
            if (fallbackStatus.IsError()) {
                // fallbackStatus is populated only when transport fallback is enabled and the limiter rejects it.
                HostPort requestAddress;
                LOG_IF_ERROR(GetRemoteAddressFromBatchGetReq(req, requestAddress),
                             "GetRemoteAddressFromBatchGetReq failed");
                const auto targetAddress = requestAddress.ToString();
                LOG(WARNING) << FormatString(
                    "Worker-to-worker TCP fallback payload rejected, srcAddress = %s, targetAddress = %s, "
                    "wait rc = %s, fallback rc = %s",
                    srcAddress, targetAddress, status.ToString(), fallbackStatus.ToString());
                auto newStatus = fallbackStatus;
                newStatus.AppendMsg(status.GetMsg());
                rsp.mutable_responses()->at(index).mutable_error()->set_error_code(newStatus.GetCode());
                rsp.mutable_responses()->at(index).mutable_error()->set_error_msg(newStatus.GetMsg());
                return newStatus;
            }
            const bool batchWaitFailed = kp.second.second.empty();
            if (batchWaitFailed) {
                MovePayload(loc.fallbackPayloads, payload);
                for (uint64_t batchIndex = 0; batchIndex < coveredRespNum; ++batchIndex) {
                    rsp.mutable_responses()
                        ->at(index + batchIndex)
                        .set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
                }
                loc.fallbackPayloads.clear();
            } else {
                MovePayload(kp.second.second, payload);
                rsp.mutable_responses()->at(index).set_data_source(DataTransferSource::DATA_IN_PAYLOAD);
                kp.second.second.clear();
            }

            if (FLAGS_enable_transport_fallback) {
                HostPort requestAddress;
                LOG_IF_ERROR(GetRemoteAddressFromBatchGetReq(req, requestAddress),
                             "GetRemoteAddressFromBatchGetReq failed");
                const auto targetAddress = requestAddress.ToString();
                LOG(WARNING) << FormatString("fallback to tcp, srcAddress = %s, targetAddress = %s, rc = %s",
                                             srcAddress, targetAddress, status.ToString());
                return Status::OK();
            }
        }

        rsp.mutable_responses()->at(index).mutable_error()->set_error_code(status.GetCode());
        rsp.mutable_responses()->at(index).mutable_error()->set_error_msg(status.GetMsg());
        return status;
    };

    (void)WaitFastTransportEvent(kp.second.first, remainingTime, errorHandler);
    index += coveredRespNum;
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::ParallelBatchGetObject(
    BatchGetObjectRemoteReqPb &req, BatchGetObjectRemoteRspPb &rsp, std::vector<ParallelRes> &parallelRes,
    const std::shared_ptr<UrmaSendLaneLease> &sendLaneLease)
{
    tbb::task_arena limited;
    if (FLAGS_oc_worker_worker_parallel_nums > 0) {
        limited.initialize(FLAGS_oc_worker_worker_parallel_nums);
    }

    AggregateInfo info;
    CHECK_FAIL_RETURN_STATUS(PrepareAggregateMemory(req, info), K_RUNTIME_ERROR, "Prepare Memory failed");
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
                BatchRh2dContext batchRh2dContext;
                batchRh2dContext.sendLaneLease = sendLaneLease;
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
                LOG_IF_ERROR(GatherWrite(i, info, batchPtr, parallelRes, req, sendLaneLease),
                             "gather write error!");
            }
        });
    });

    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::NotifyRemoteGet(const NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp)
{
    ScopedRequestContext ctx;
    LOG(INFO) << PIPLN_LOG_PREFIX" NotifyRemoteGet request: object_count=" << req.object_keys_size();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->NotifyRemoteGet(req, rsp), "NotifyRemoteGet failed");
    LOG(INFO) << PIPLN_LOG_PREFIX" NotifyRemoteGet success";
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
