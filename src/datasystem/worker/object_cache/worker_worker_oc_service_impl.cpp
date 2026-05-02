/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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

/**
 * Description: Defines the worker worker service processing main class.
 */
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"

#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>

#include "datasystem/common/util/thread_local.h"
#include "datasystem/utils/status.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

DS_DECLARE_int32(oc_worker_worker_direct_port);
DS_DECLARE_int32(oc_worker_worker_parallel_nums);
DS_DECLARE_int32(oc_worker_worker_parallel_min);
DS_DECLARE_uint64(oc_worker_aggregate_single_max);
DS_DECLARE_uint64(oc_worker_aggregate_merge_size);

namespace datasystem {
namespace {
constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
}

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
}  // namespace

WorkerWorkerOCServiceImpl::WorkerWorkerOCServiceImpl(
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc, std::shared_ptr<AkSkManager> akSkManager,
    EtcdStore *etcdStore, EtcdClusterManager *etcdCm)
    : ocClientWorkerSvc_(std::move(clientSvc)),
      akSkManager_(std::move(akSkManager)),
      etcdStore_(etcdStore),
      etcdCm_(etcdCm)
{
}

WorkerWorkerOCServiceImpl::~WorkerWorkerOCServiceImpl()
{
    LOG(INFO) << "WorkerWorkerOCServiceImpl exit";
}

Status WorkerWorkerOCServiceImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS(ocClientWorkerSvc_ != nullptr, StatusCode::K_NOT_READY,
                             "ClientWorkerService must be initialized before WorkerWorkerService construction");
    constexpr uint32_t commThrdNum = 4;
    RETURN_IF_EXCEPTION_OCCURS(communicatorThreadPool_ = std::make_shared<ThreadPool>(0, commThrdNum, "CommInit"));
    return WorkerWorkerOCService::Init();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetObjectRemoteRspPb, GetObjectRemoteReqPb>> serverApi)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY);
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    GetObjectRemoteReqPb req;
    GetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    INJECT_POINT("worker.GetObjectRemote.afterRead");
    // K_OC_REMOTE_GET_NOT_ENOUGH error happens only when URMA is used for RDMA and size of the object
    // is different from the request
    RETURN_IF_NOT_OK(CheckConnectionStable(req));
    RETURN_IF_NOT_OK_EXCEPT(GetObjectRemote(req, rsp, payload), StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
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
    LOG(INFO) << "send data success";
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                  std::vector<RpcMessage> &payload)
{
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::string callerAddress;
    if (req.has_urma_info()) {
        callerAddress = FormatString("%s:%d", req.urma_info().request_address().host(),
                                     req.urma_info().request_address().port());
    } else if (req.has_ucp_info()) {
        callerAddress = FormatString("%s:%d", req.ucp_info().remote_ip_addr().host(),
                                     req.ucp_info().remote_ip_addr().port());
    }
    LOG(INFO) << FormatString("Processing pull object[%s] request[%s] src[%s] dst[%s] offset[%ld] size[%ld]",
                              req.object_key(), req.request_id(), callerAddress, localAddress_.ToString(),
                              req.read_offset(), req.read_size());
    std::vector<uint64_t> eventKeys;
    RETURN_IF_NOT_OK(GetObjectRemoteHandler(req, rsp, payload, true, eventKeys));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteBatchWrite(uint32_t paraIndex, const GetObjectRemoteReqPb &subReq,
                                                            BatchGetObjectRemoteRspPb &rsp,
                                                            std::vector<ParallelRes> &parallelRes,
                                                            std::shared_ptr<AggregateMemory> batchPtr)
{
    GetObjectRemoteRspPb &subRsp = parallelRes[paraIndex].respPbs.emplace_back();
    uint64_t &subIndex = parallelRes[paraIndex].subIndex;

    std::vector<RpcMessage> subPayload;
    std::vector<uint64_t> eventKeys;
    auto isGatherWrite = IsFastTransportEnabled() && batchPtr != nullptr;
    std::string callerAddress;
    if (subReq.has_urma_info()) {
        callerAddress = FormatString("%s:%d", subReq.urma_info().request_address().host(),
                                     subReq.urma_info().request_address().port());
    } else if (subReq.has_ucp_info()) {
        callerAddress = FormatString("%s:%d", subReq.ucp_info().remote_ip_addr().host(),
                                     subReq.ucp_info().remote_ip_addr().port());
    }
    LOG(INFO) << FormatString("Processing pull object[%s] request[%s] src[%s] dst[%s] offset[%ld] size[%ld]",
                              subReq.object_key(), subReq.request_id(), callerAddress, localAddress_.ToString(),
                              subReq.read_offset(), subReq.read_size());
    Status fallbackStatus;
    auto status = GetObjectRemoteHandler(subReq, subRsp, subPayload, false, eventKeys, batchPtr,
                                         rsp.mutable_root_info(), isGatherWrite ? nullptr : &fallbackStatus);
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

    // empty requestIds means failed fastTransport or tcp mode
    std::vector<uint64_t> requestIds;
    requestIds = std::move(eventKeys);

    parallelRes[paraIndex].kps.emplace_back(subIndex, std::make_pair(std::move(requestIds), std::move(subPayload)));
    parallelRes[paraIndex].fallbackStatuses.emplace_back(fallbackStatus);
    subIndex++;
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
                                              std::vector<ParallelRes> &parallelRes, BatchGetObjectRemoteReqPb &req)
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
        rc = UrmaGatherWrite(remoteSegInfo, aggregatedMem->localSgeInfos, false, loc.eventKeys);
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

Status WorkerWorkerOCServiceImpl::GetObjectRemoteHandler(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                         std::vector<RpcMessage> &payload, bool blocking,
                                                         std::vector<uint64_t> &eventKeys,
                                                         std::shared_ptr<AggregateMemory> batchPtr,
                                                         RemoteH2DRootInfoPb *batchRootInfo, Status *fallbackStatus)
{
    const std::string &objectKey = req.object_key();
    const std::string &requestId = req.request_id();
    INJECT_POINT("worker.worker_worker_remote_get_sleep");
    INJECT_POINT("worker.worker_worker_remote_get_failure");
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "objectKey is empty.");
    Status status =
        GetObjectRemoteImpl(req, rsp, payload, blocking, eventKeys, batchPtr, batchRootInfo, fallbackStatus);
    if (status.GetCode() == K_INVALID || status.GetCode() == K_NOT_FOUND) {
        status = Status(K_WORKER_PULL_OBJECT_NOT_FOUND, status.GetMsg());
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        status, FormatString("[ObjectKey %s] Get object remote failed, requestId: %s, workerAddr: %s", objectKey,
                             requestId, localAddress_.ToString()));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetSafeObjectEntry(const std::string &objectKey, bool tryLock, uint64_t version,
                                                     std::shared_ptr<SafeObjType> &safeEntry)
{
    if (ocClientWorkerSvc_->IsInRollbackProgress(objectKey)) {
        LOG(INFO) << FormatString("[ObjectKey %s] Data is in rolling back, now can not be get", objectKey);
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object not found");
    }
    bool insert = false;
    auto func = [this, &objectKey, &safeEntry, &insert]() {
        return ocClientWorkerSvc_->objectTable_->ReserveGetAndLock(objectKey, safeEntry, insert, false, false);
    };
    RETURN_IF_NOT_OK(RetryWhenDeadlock(func));
    if (insert) {
        Raii innerUnlock([&safeEntry]() { safeEntry->WUnlock(); });
        StatusCode code;
        if (ocClientWorkerSvc_->IsInRollbackProgress(objectKey)) {
            LOG(INFO) << FormatString("[ObjectKey %s] Data is in rolling back, now can not be get", objectKey);
            code = StatusCode::K_NOT_FOUND;
        } else if (!tryLock) {
            // get data from L2 cache if worker is primary copy
            return ocClientWorkerSvc_->GetDataFromL2CacheForPrimaryCopy(objectKey, version, safeEntry);
        } else {
            // tryLock it is the local master call, we need to avoid deadlock.
            code = StatusCode::K_UNKNOWN_ERROR;
        }
        RETURN_STATUS(code, "Object not found");
    }
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::EstablishConnAndFillSeg(const std::string &commId, const uint64_t &localSegAddress,
                                                          const uint64_t &localSegSize,
                                                          std::shared_ptr<ShmUnit> shmUnit, uint64_t metadataSize,
                                                          GetObjectRemoteRspPb &rsp, RemoteH2DRootInfoPb *batchRootInfo)
{
    (void)commId;
    (void)localSegAddress;
    (void)localSegSize;
    (void)shmUnit;
    (void)metadataSize;
    (void)rsp;
    (void)batchRootInfo;
#ifdef BUILD_HETERO
    PerfPoint point(PerfKey::WORKER_REMOTE_GET_RH2D);

    int32_t devId;
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetDevIdForComm(commId, devId));

    PerfPoint pointImpl(PerfKey::WORKER_REMOTE_GET_P2P_SET_DEV);
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().SetDeviceIdx(devId));

    // Send root info to client
    pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_P2P_GET_ROOT);
    auto *rootInfo = batchRootInfo ? batchRootInfo : rsp.mutable_host_info()->mutable_root_info();
    if (rootInfo->internal().empty()) {
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().P2PGetRootInfo(commId, rootInfo));
    }

    // Initialize communicator connection (accept client).
    // Fixme: error handling
    pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_P2P_COMM_INIT);
    std::shared_ptr<RemoteH2DContext> p2pComm;
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().P2PCommInitRootInfo(commId, *rootInfo, P2P_SENDER, p2pComm, devId,
                                                                      communicatorThreadPool_));

    // Send segment info to client
    pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_P2P_FILL_SEG);
    auto *segmentPb = rsp.mutable_host_info()->mutable_remote_host_segment();
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().FillSegmentInfo(localSegSize, shmUnit->GetOffset() + metadataSize,
                                                                  localSegAddress, *segmentPb, devId));

    // Send offset info to client
    pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_P2P_FILL_DATA);
    auto *dataInfoPb = rsp.mutable_host_info()->mutable_data_info();
    uint64_t *dataPtr = reinterpret_cast<uint64_t *>(static_cast<uint8_t *>(shmUnit->GetPointer()) + metadataSize);
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().FillDataInfo(dataPtr, *dataInfoPb));
#endif
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteImpl(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                      std::vector<RpcMessage> &outPayload, bool blocking,
                                                      std::vector<uint64_t> &eventKeys,
                                                      std::shared_ptr<AggregateMemory> batchPtr,
                                                      RemoteH2DRootInfoPb *batchRootInfo, Status *fallbackStatus)
{
    (void)eventKeys;
    (void)blocking;
    const std::string &objectKey = req.object_key();
    const bool tryLock = req.try_lock();
    const uint64_t version = req.version();
    const uint64_t offset = req.read_offset();
    const uint64_t size = req.read_size();
    const uint64_t expectedDataSize = req.data_size();
    std::shared_ptr<SafeObjType> safeEntry;
    // If entry insert failed, it would not be locked.

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

    RETURN_IF_NOT_OK(GetSafeObjectEntry(objectKey, tryLock, version, safeEntry));

    // not need WLock
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
    Raii raii([safeEntry]() { safeEntry->RUnlock(); });
    auto &entry = *safeEntry;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!entry->stateInfo.IsCacheInvalid() && !entry->IsInvalid(), K_INVALID,
                                         FormatString("[ObjectKey %s] is invalid", objectKey));
    LOG_IF(WARNING, entry->GetCreateTime() != version) << FormatString(
        "[ObjectKey %s] Version: %ld, require version: %ld", objectKey, entry->GetCreateTime(), version);
    const bool isUrmaFastTransport = IsUrmaEnabled() && req.has_urma_info();
    bool isFastTransportEnabled = isUrmaFastTransport || (IsUcpEnabled() && req.has_ucp_info());
    if (isFastTransportEnabled && entry->GetDataSize() != expectedDataSize) {
        // Return error with changed size, so the request can be retried.
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
    PerfPoint point(PerfKey::WORKER_LOAD_OBJECT_DATA);
    PerfPoint pointImpl(PerfKey::WORKER_REMOTE_GET_READ_KEY);
    ReadObjectKV objKv(ReadKey(objectKey, offset, size), entry);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objKv.CheckReadOffset(), "Read offset verify failed");
    if (entry->IsSpilled() && entry->GetShmUnit() == nullptr) {  // At this step, the local memory may already exist.
        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_PAYLOAD_FROM_DISK);
        RETURN_IF_NOT_OK(
            WorkerOcSpill::Instance()->Get(objectKey, outPayload, objKv.GetReadSize(), objKv.GetReadOffset()));
    } else {
        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_PAYLOAD_SHM_UNIT);
        ShmGuard shmGuard(entry->GetShmUnit(), entry->GetDataSize(), entry->GetMetadataSize());
        if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
            RETURN_IF_NOT_OK(shmGuard.TryRLatch());
        }
        INJECT_POINT("worker.LoadObjectData.AddPayload");

        pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_PAYLOAD);

        auto shmUnit = entry->GetShmUnit();
        const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
        uint64_t localSegAddress;
        uint64_t localSegSize;
        GetSegmentInfoFromShmUnit(shmUnit, localObjectAddress, localSegAddress, localSegSize);
        Status fastTransportStatus = Status::OK();
        std::string fastTransportName;
        auto markFastTransferResult = [&rsp](const Status &status) {
            if (status.IsError()) {
                CHECK_FAIL_RETURN_STATUS(FLAGS_enable_transport_fallback, status.GetCode(), status.GetMsg());
                return Status::OK();
            }
            rsp.set_data_source(datasystem::DataTransferSource::DATA_ALREADY_TRANSFERRED);
            return Status::OK();
        };

        // Support send payload exceed 2GB
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
                // later add a check on data size and read size.
                const uint8_t srcChipId = NumaIdToChipId(shmUnit->GetNumaId());
                const uint8_t dstChipId =
                    req.urma_info().has_chip_id() ? static_cast<uint8_t>(req.urma_info().chip_id()) : INVALID_CHIP_ID;
                auto rc = UrmaWritePayload(req.urma_info(), localSegAddress, localSegSize, localObjectAddress, offset,
                                           size, entry->GetMetadataSize(), srcChipId, dstChipId, blocking, eventKeys);
                fastTransportStatus = rc;
                fastTransportName = "UrmaWrite";
                RETURN_IF_NOT_OK(markFastTransferResult(rc));
            } else if (IsUcpEnabled()) {
                // later add a check on data size and read size.
                auto rc = UcpPutPayload(req.ucp_info(), localObjectAddress, offset, size, entry->GetMetadataSize(),
                                        blocking, eventKeys);
                fastTransportStatus = rc;
                fastTransportName = "UcpWrite";
                RETURN_IF_NOT_OK(markFastTransferResult(rc));
            }
        }

        // For compatibility, only trigger RH2D if this client request both supports and enables RH2D.
        if (IsRemoteH2DEnabled() && !req.comm_id().empty()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                EstablishConnAndFillSeg(req.comm_id(), localSegAddress, localSegSize, shmUnit, entry->GetMetadataSize(),
                                        rsp, batchRootInfo),
                "");
            rsp.set_data_source(datasystem::DataTransferSource::DATA_DELAY_TRANSFER);
        }

        // We need to extend the ShmGuard lifecycle if we perform parallel urma_write/ucp_put_nbx.
        if ((!IsFastTransportEnabled() || !blocking) && !(IsRemoteH2DEnabled() && !req.comm_id().empty())) {
            bool canPrepareFallbackPayload = true;
            if (FLAGS_enable_transport_fallback && (fastTransportStatus.IsError() || (!blocking && batchPtr == nullptr))
                && isUrmaFastTransport) {
                auto trackStatus = fastTransportStatus.IsOk()
                                       ? Status(StatusCode::K_URMA_ERROR, "URMA wait fallback payload precheck")
                                       : fastTransportStatus;
                auto rc = shmGuard.TrackUrmaFallbackTcp(objKv.GetReadSize(), trackStatus, "worker->worker");
                if (rc.IsError()) {
                    if (fastTransportStatus.IsOk() && !blocking) {
                        if (fallbackStatus != nullptr) {
                            *fallbackStatus = rc;
                        }
                        canPrepareFallbackPayload = false;
                    } else {
                        LOG(WARNING) << FormatString("Worker-to-worker TCP fallback payload rejected for object %s: %s",
                                                     objectKey, rc.ToString());
                        RETURN_IF_NOT_OK(rc);
                    }
                }
            }
            if (canPrepareFallbackPayload) {
                LOG_IF(WARNING, fastTransportStatus.IsError()) << FormatString(
                    "%s[%s] fallback to tcp, rc = %s", fastTransportName, objectKey, fastTransportStatus.ToString());
                RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayload, objKv.GetReadOffset(), objKv.GetReadSize()));
            }
        }
    }

    pointImpl.RecordAndReset(PerfKey::WORKER_REMOTE_GET_RESP);
    rsp.mutable_error()->set_error_code(StatusCode::K_OK);  // No size change
    rsp.set_data_size(static_cast<int64_t>(entry->GetDataSize()));
    rsp.set_create_time(static_cast<int64_t>(entry->GetCreateTime()));
    rsp.set_life_state(static_cast<uint32_t>(entry->GetLifeState()));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::CheckEtcdState(const CheckEtcdStateReqPb &req, CheckEtcdStateRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    bool isEtcdAvailable = etcdStore_->Writable().IsOk();
    rsp.set_available(isEtcdAvailable);
    LOG_IF(INFO, isEtcdAvailable) << "Etcd is available";
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetClusterState(const GetClusterStateReqPb &req, GetClusterStateRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    bool isEtcdAvailable = etcdStore_->Writable().IsOk();
    rsp.set_etcd_available(isEtcdAvailable);
    RETURN_RUNTIME_ERROR_IF_NULL(etcdCm_);
    *rsp.mutable_hash_ring() = etcdCm_->GetHashRing()->GetHashRingPb();
    LOG_IF(INFO, isEtcdAvailable) << "Etcd is available";
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                              std::vector<::datasystem::RpcMessage> payloads)
{
    return ocClientWorkerSvc_->MigrateData(req, rsp, std::move(payloads));
}

Status WorkerWorkerOCServiceImpl::MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    return ocClientWorkerSvc_->MigrateDataDirect(req, rsp);
}

Status WorkerWorkerOCServiceImpl::CheckConnectionStable(const GetObjectRemoteReqPb &req)
{
    bool isFastTransportEnabled = (IsUrmaEnabled() && req.has_urma_info()) || (IsUcpEnabled() && req.has_ucp_info());
    if (!isFastTransportEnabled) {
        return Status::OK();
    }
    std::string host;
    int port;
    if (req.has_urma_info()) {
        host = req.urma_info().request_address().host();
        port = req.urma_info().request_address().port();
    }
    if (req.has_ucp_info()) {
        host = req.ucp_info().remote_ip_addr().host();
        port = req.ucp_info().remote_ip_addr().port();
    }
    const HostPort requestAddress(host, port);
    auto rc = CheckTransportConnectionStable(requestAddress.ToString(), req.urma_instance_id());
    if (rc.IsError() && rc.GetCode() == K_URMA_NEED_CONNECT) {
        std::string remoteWorkerId = "UNKNOWN";
        if (etcdCm_ != nullptr) {
            auto workerId = etcdCm_->GetWorkerIdByWorkerAddr(requestAddress.ToString());
            if (!workerId.empty()) {
                remoteWorkerId = workerId;
            }
        }
        LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
            << "[URMA_NEED_CONNECT] CheckConnectionStable failed, remoteAddress=" << requestAddress.ToString()
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
    METRIC_TIMER(metrics::KvMetricId::WORKER_RPC_REMOTE_GET_INBOUND_LATENCY);
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    BatchGetObjectRemoteReqPb req;
    BatchGetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    LOG(INFO) << "BatchGetObjectRemote request (objectKey, requestId, readOffset, readSize): "
              << VectorToString(req.requests()) << " remainingTime:" << reqTimeoutDuration.CalcRealRemainingTime()
              << "ms";
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    RETURN_IF_NOT_OK(PrepareBatchGetObjectRemoteReq(req));
    RETURN_IF_NOT_OK(BatchGetObjectRemoteImpl(req, rsp, payload));
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                     "GetObjectRemote send payload error");
    pointImpl.Record();
    auto vlogLevel = payload.empty() ? 1 : 0;
    VLOG(vlogLevel) << "send data success with payload size: " << payload.size();
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
    std::vector<ParallelRes> parallelRes;
    if (req.requests_size() > FLAGS_oc_worker_worker_parallel_min && IsFastTransportEnabled()) {
        RETURN_IF_NOT_OK(ParallelBatchGetObject(req, rsp, parallelRes));
    } else {
        uint32_t parallelSize = 1;
        parallelRes.resize(parallelSize);
        for (int i = 0; i < req.requests_size(); i++) {
            auto *subReq = req.mutable_requests(i);
            *(subReq->mutable_comm_id()) = req.comm_id();
            (void)GetObjectRemoteBatchWrite(parallelSize - 1, *subReq, rsp, parallelRes, nullptr);
        }
    }

    // pipeline h2d don't need wait and retry, just fill in response
    if (OsXprtPipln::IsPiplnH2DRequest(req)) {
        for (auto &loc : parallelRes) {
            for (auto &resp : loc.respPbs) {
                rsp.add_responses()->Swap(&resp);
            }
        }
        // no try again
        return Status::OK();
    }

    point.RecordAndReset(PerfKey::FAST_TRANSPORT_TOTAL_EVENT_WAIT);
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
    auto remainingTime = []() { return reqTimeoutDuration.CalcRemainingTime(); };
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
                rsp.mutable_responses()->at(index).mutable_error()->set_error_code(fallbackStatus.GetCode());
                rsp.mutable_responses()->at(index).mutable_error()->set_error_msg(fallbackStatus.GetMsg());
                return fallbackStatus;
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

Status WorkerWorkerOCServiceImpl::ParallelBatchGetObject(BatchGetObjectRemoteReqPb &req, BatchGetObjectRemoteRspPb &rsp,
                                                         std::vector<ParallelRes> &parallelRes)
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
                for (uint64_t j = startPos; j < endPos; ++j) {
                    auto *subReq = req.mutable_requests(j);
                    GetObjectRemoteBatchWrite(i, *subReq, rsp, parallelRes, batchPtr);
                }
                PerfPoint pDo(PerfKey::URMA_GATHER_WRITE_DO);
                LOG_IF_ERROR(GatherWrite(i, info, batchPtr, parallelRes, req), "gather write error!");
            }
        });
    });

    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::NotifyRemoteGet(const NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp)
{
    LOG(INFO) << FormatString("NotifyRemoteGet request, object size: %d", req.object_keys_size());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->NotifyRemoteGet(req, rsp), "NotifyRemoteGet failed");
    LOG(INFO) << "NotifyRemoteGet success";
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
