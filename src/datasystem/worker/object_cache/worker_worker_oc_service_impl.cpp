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

#include "datasystem/utils/status.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/common/perf/perf_manager.h"

DS_DECLARE_int32(oc_worker_worker_direct_port);
DS_DECLARE_int32(oc_worker_worker_parallel_nums);
DS_DECLARE_int32(oc_worker_worker_parallel_min);
DS_DECLARE_uint64(oc_worker_aggregate_single_max);
DS_DECLARE_uint64(oc_worker_aggregate_merge_size);

namespace datasystem {
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
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    GetObjectRemoteReqPb req;
    GetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    PerfPoint pointRead(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    pointRead.Record();
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    INJECT_POINT("worker.GetObjectRemote.afterRead");
    // K_OC_REMOTE_GET_NOT_ENOUGH error happens only when URMA is used for RDMA and size of the object
    // is different from the request
    RETURN_IF_NOT_OK(CheckConnectionStable(req));
    RETURN_IF_NOT_OK_EXCEPT(GetObjectRemote(req, rsp, payload), StatusCode::K_OC_REMOTE_GET_NOT_ENOUGH);
    pointImpl.Record();
    PerfPoint pointWrite(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointWrite.Record();
    PerfPoint pointSendPayload(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);

    if (rsp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED
        || rsp.data_source() == DataTransferSource::DATA_DELAY_TRANSFER
        || rsp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED_MEMSET_META) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload({}, FLAGS_oc_worker_worker_direct_port > 0),
                                         "GetObjectRemote send payload error");
    } else if (rsp.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                         "GetObjectRemote send payload error");
    }

    pointSendPayload.Record();
    LOG(INFO) << FormatString("pull success");
    point.Record();
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                  std::vector<RpcMessage> &payload)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Processing pull object[%s] request[%s] offset[%ld] size[%ld]", req.object_key(),
                              req.request_id(), req.read_offset(), req.read_size());
    std::vector<uint64_t> dummyKeys;
    RETURN_IF_NOT_OK(GetObjectRemoteHandler(req, rsp, payload, true, dummyKeys));
    return Status::OK();
}

void WorkerWorkerOCServiceImpl::GetObjectRemoteBatchWrite(
    uint32_t paraIndex, const GetObjectRemoteReqPb &subReq, BatchGetObjectRemoteRspPb &rsp,
    std::vector<RpcMessage> &payload,
    std::map<uint64_t, std::pair<std::vector<uint64_t>, std::vector<RpcMessage>>> &keys,
    std::vector<ParallelRes> &parallelRes, std::shared_ptr<AggregateMemory> batchPtr)
{
    bool disabledParrallel = parallelRes.empty();

    GetObjectRemoteRspPb &subRsp =
        disabledParrallel ? *(rsp.add_responses()) : parallelRes[paraIndex].respPbs.emplace_back();

    std::vector<RpcMessage> subPayload;
    std::vector<uint64_t> fastTransportEventKeys;

    auto status = GetObjectRemoteHandler(subReq, subRsp, subPayload, false, fastTransportEventKeys, batchPtr);
    if (status.IsError()) {
        subRsp.mutable_error()->set_error_code(status.GetCode());
        subRsp.mutable_error()->set_error_msg(status.GetMsg());
        return;
    }
    auto isGatherWrite = IsFastTransportEnabled() && batchPtr != nullptr;
    if (isGatherWrite) {
        return;
    }
    // If keys are empty, we 1) get payload from spill or 2) urma is not enabled.
    // In both cases we send payload as part of the response.
    // Otherwise we extend the lifecycle of payload only until urma_write is done.
    if (fastTransportEventKeys.empty()) {
        auto &localPayload = disabledParrallel ? payload : parallelRes[paraIndex].pays;
        localPayload.insert(localPayload.end(), std::make_move_iterator(subPayload.begin()),
                            std::make_move_iterator(subPayload.end()));
    } else if (batchPtr == nullptr) {
        auto &localKps = disabledParrallel ? keys : parallelRes[paraIndex].kps;
        localKps.emplace(paraIndex, std::make_pair(std::move(fastTransportEventKeys), std::move(subPayload)));
    }
}

Status WorkerWorkerOCServiceImpl::AllocateAggregateMemory(uint64_t parallelIndex, AggregateInfo &info,
                                                          std::shared_ptr<AggregateMemory> &batchPtr)
{
    if (!info.canBatchHandler) {
        return Status::OK();
    }
    PerfPoint totalTime(PerfKey::WORKER_AGGREGATE_MEM_ALLOC);
    batchPtr = std::make_shared<AggregateMemory>();
    batchPtr->batchShmUnit = std::make_shared<ShmUnit>();
    Status rc = batchPtr->batchShmUnit->AllocateMemory("", info.batchSizes[parallelIndex], false, ServiceType::OBJECT,
                                                       static_cast<memory::CacheType>(0));
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Failed to allocate memory for batch get, size: %d", info.batchSizes[parallelIndex]);
    }
    auto ret = memset_s(batchPtr->batchShmUnit->GetPointer(), info.batchSizes[parallelIndex], 0,
                        info.batchSizes[parallelIndex]);
    if (ret != EOK) {
        batchPtr->batchShmUnit->SetHardFreeMemory();
        batchPtr->batchShmUnit->FreeMemory();
        LOG(ERROR) << FormatString("[Aggregated memory] Memset failed, errno: %d", ret);
    }
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

Status WorkerWorkerOCServiceImpl::AggregaedMemorySend(uint64_t subIndex, AggregateInfo &info,
                                                      std::shared_ptr<AggregateMemory> aggregatedMem,
                                                      std::vector<ParallelRes> &parallelRes,
                                                      BatchGetObjectRemoteReqPb &req)
{
    if (!info.canBatchHandler) {
        return Status::OK();
    }
    RETURN_RUNTIME_ERROR_IF_NULL(aggregatedMem);

    std::vector<uint64_t> subKeys;
    std::vector<RpcMessage> subPayload;
    auto startPos = info.batchStartIndex[subIndex];
    auto *subReq = req.mutable_requests(startPos);

    const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(aggregatedMem->batchShmUnit->GetPointer());
    uint64_t localSegAddress = 0;
    uint64_t localSegSize;
    if (IsUrmaEnabled()) {
        GetSegmentInfoFromShmUnit(aggregatedMem->batchShmUnit, localObjectAddress, localSegAddress, localSegSize);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            UrmaWritePayload(subReq->urma_info(), localSegAddress, localSegSize, localObjectAddress, 0,
                             info.batchSizes[subIndex], ocClientWorkerSvc_->GetMetadataSize(), false, subKeys),
            "Failed in aggregate memory urma write");
    } else if (IsUcpEnabled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            UcpPutPayload(subReq->ucp_info(), localObjectAddress, 0, info.batchSizes[subIndex],
                          ocClientWorkerSvc_->GetMetadataSize(), false, subKeys),
            "Failed in aggregate memory ucp put");
    }

    ShmGuard shmGuard(aggregatedMem->batchShmUnit, info.batchSizes[subIndex], 0);
    RETURN_IF_NOT_OK(shmGuard.TransferTo(subPayload, 0, info.batchSizes[subIndex]));
    ParallelRes &loc = parallelRes[subIndex];
    loc.kps.emplace(startPos, std::make_pair(std::move(subKeys), std::move(subPayload)));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GatherWrite(uint64_t subIndex, AggregateInfo &info,
                                              std::shared_ptr<AggregateMemory> aggregatedMem,
                                              std::vector<ParallelRes> &parallelRes, BatchGetObjectRemoteReqPb &req)
{
    if (!info.canBatchHandler) {
        return Status::OK();
    }

    std::vector<uint64_t> subKeys;
    std::vector<RpcMessage> subPayload;
    auto startPos = info.batchStartIndex[subIndex];
    auto *subReq = req.mutable_requests(startPos);
    if (IsUrmaEnabled() && subReq->has_urma_info()) {
        auto &urmaInfo = subReq->urma_info();
        RemoteSegInfo remoteSegInfo{
            .segAddr = urmaInfo.seg_va(),
            .segOffset = urmaInfo.seg_data_offset() - ocClientWorkerSvc_->GetMetadataSize(),
            .host = urmaInfo.request_address().host(),
            .port = urmaInfo.request_address().port(),
        };
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            UrmaGatherWrite(remoteSegInfo, aggregatedMem->localSgeInfos, false, subKeys),
            "Failed in aggregate memory urma write");
    } else if (IsUcpEnabled() && subReq->has_ucp_info()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            UcpGatherPut(subReq->ucp_info(), ocClientWorkerSvc_->GetMetadataSize(), aggregatedMem->localSgeInfos,
                         false, subKeys),
            "Failed in aggregate memory ucp gather put");
    }
    ParallelRes &loc = parallelRes[subIndex];
    loc.kps.emplace(startPos, std::make_pair(std::move(subKeys), std::move(subPayload)));
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteHandler(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                         std::vector<RpcMessage> &payload, bool blocking,
                                                         std::vector<uint64_t> &keys,
                                                         std::shared_ptr<AggregateMemory> batchPtr)
{
    const std::string &objectKey = req.object_key();
    const std::string &requestId = req.request_id();
    INJECT_POINT("worker.worker_worker_remote_get_sleep");
    INJECT_POINT("worker.worker_worker_remote_get_failure");
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "objectKey is empty.");
    Status status = GetObjectRemoteImpl(req, rsp, payload, blocking, keys, batchPtr);
    INJECT_POINT("worker.batch_get_failure_for_keys", [&objectKey]() {
        if (objectKey == "key2") {
            return Status(K_RUNTIME_ERROR, "Injected K_RUNTIME_ERROR");
        } else if (objectKey == "key3") {
            return Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "Injected K_WORKER_PULL_OBJECT_NOT_FOUND");
        } else if (objectKey == "key0") {
            return Status(K_OUT_OF_MEMORY, "Injected K_OUT_OF_MEMORY");
        }
        return Status::OK();
    });
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
                                                          GetObjectRemoteRspPb &rsp)
{
    (void)commId;
    (void)localSegAddress;
    (void)localSegSize;
    (void)shmUnit;
    (void)metadataSize;
    (void)rsp;
#ifdef BUILD_HETERO
    tbb::concurrent_hash_map<std::string, int32_t>::accessor acc;
    // Assign new commId a new devId
    if (!commDevIdMap_.find(acc, commId)) {
        std::vector<int32_t> devIds;
        RETURN_IF_NOT_OK(RemoteH2DManager::Instance().GetWorkerDeviceIds(&devIds));
        commDevIdMap_.insert(acc, commId);
        acc->second = devIds[nextDevIdIndex_.fetch_add(1) % devIds.size()];
    }
    int32_t devId = acc->second;

    // Send root info to client
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().P2PGetRootInfo(commId, rsp.mutable_root_info(), devId));

    // Initialize communicator connection (accept client).
    // Fixme: error handling
    auto traceId = Trace::Instance().GetTraceID();
    communicatorThreadPool_->Execute([rootInfo = rsp.root_info(), commId = commId, traceId, devId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        std::shared_ptr<RemoteH2DContext> p2pComm;
        LOG_IF_ERROR(RemoteH2DManager::Instance().P2PCommInitRootInfo(commId, rootInfo, P2P_SENDER, p2pComm, devId),
                     "P2PCommInitRootInfo failed.");
    });

    // Send segment info to client
    RETURN_IF_NOT_OK(RemoteH2DManager::Instance().FillSegmentInfo(
        localSegSize, shmUnit->GetOffset() + metadataSize, localSegAddress, *rsp.mutable_remote_host_segment(), devId));

    // Send offset info to client
    auto *dataInfoPb = rsp.mutable_data_info();
    uint64_t *dataPtr = reinterpret_cast<uint64_t *>(static_cast<uint8_t *>(shmUnit->GetPointer()) + metadataSize);

    // Get number of data sizes
    uint64_t sz = *dataPtr;

    // Get the first offset
    dataPtr++;
    uint64_t firstOffset = *dataPtr;
    dataInfoPb->set_offset(firstOffset);

    // Calculate sizes of data
    for (uint64_t i = 0; i < sz; i++) {
        uint64_t offsetX = *dataPtr;
        dataPtr++;
        uint64_t offsetY = *dataPtr;
        dataInfoPb->add_sizes(offsetY - offsetX);
    }
#endif
    return Status::OK();
}

Status WorkerWorkerOCServiceImpl::GetObjectRemoteImpl(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                                      std::vector<RpcMessage> &outPayload, bool blocking,
                                                      std::vector<uint64_t> &keys,
                                                      std::shared_ptr<AggregateMemory> batchPtr)
{
    (void)keys;
    (void)blocking;
    const std::string &objectKey = req.object_key();
    const bool tryLock = req.try_lock();
    const uint64_t version = req.version();
    const uint64_t offset = req.read_offset();
    const uint64_t size = req.read_size();
    const uint64_t expectedDataSize = req.data_size();
    std::shared_ptr<SafeObjType> safeEntry;
    // If entry insert failed, it would not be locked.
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
    bool isFastTransportEnabled = (IsUrmaEnabled() && req.has_urma_info()) || (IsUcpEnabled() && req.has_ucp_info());
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
    ReadObjectKV objKv(ReadKey(objectKey, offset, size), entry);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objKv.CheckReadOffset(), "Read offset verify failed");
    if (entry->IsSpilled() && entry->GetShmUnit() == nullptr) {  // At this step, the local memory may already exist.
        PerfPoint p(PerfKey::WORKER_REMOTE_GET_PAYLOAD_FROM_DISK);
        RETURN_IF_NOT_OK(
            WorkerOcSpill::Instance()->Get(objectKey, outPayload, objKv.GetReadSize(), objKv.GetReadOffset()));
        p.Record();
    } else {
        ShmGuard shmGuard(entry->GetShmUnit(), entry->GetDataSize(), entry->GetMetadataSize());
        if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
            RETURN_IF_NOT_OK(shmGuard.TryRLatch());
        }
        INJECT_POINT("worker.LoadObjectData.AddPayload");

        PerfPoint p(PerfKey::WORKER_REMOTE_GET_PAYLOAD);

        auto shmUnit = entry->GetShmUnit();
        const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
        uint64_t localSegAddress;
        uint64_t localSegSize;
        GetSegmentInfoFromShmUnit(shmUnit, localObjectAddress, localSegAddress, localSegSize);

        // Support send payload exceed 2GB
        if (isFastTransportEnabled) {
            if (batchPtr) {
                batchPtr->localSgeInfos.emplace_back(
                    LocalSgeInfo{ .segAddr = localSegAddress,
                                  .segSize = localSegSize,
                                  .sgeAddr = uintptr_t(shmUnit->GetPointer()),
                                  .readOffset = req.read_offset(),
                                  .writeSize = Align4BitsCeiling(entry->GetDataSize() + entry->GetMetadataSize()),
                                  .metaDataSize = 0 });
                rsp.set_data_source(datasystem::DataTransferSource::DATA_ALREADY_TRANSFERRED_MEMSET_META);
            } else if (IsUrmaEnabled()) {
                // later add a check on data size and read size.
                auto shmUnit = entry->GetShmUnit();
                const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
                uint64_t localSegAddress;
                uint64_t localSegSize;
                GetSegmentInfoFromShmUnit(shmUnit, localObjectAddress, localSegAddress, localSegSize);
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                    UrmaWritePayload(req.urma_info(), localSegAddress, localSegSize, localObjectAddress, offset, size,
                                     entry->GetMetadataSize(), blocking, keys),
                    "Failed in single data urma write");
                rsp.set_data_source(datasystem::DataTransferSource::DATA_ALREADY_TRANSFERRED);
            } else if (IsUcpEnabled()) {
                // later add a check on data size and read size.
                auto shmUnit = entry->GetShmUnit();
                const uint64_t localObjectAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(UcpPutPayload(req.ucp_info(), localObjectAddress, offset, size,
                                                               entry->GetMetadataSize(), blocking, keys),
                                                 "Failed in single data ucp put");
                rsp.set_data_source(datasystem::DataTransferSource::DATA_ALREADY_TRANSFERRED);
            }
        }

        // For compatibility, only trigger RH2D if this client request both supports and enables RH2D.
        if (IsRemoteH2DEnabled() && !req.comm_id().empty()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(EstablishConnAndFillSeg(req.comm_id(), localSegAddress, localSegSize,
                                                                     shmUnit, entry->GetMetadataSize(), rsp),
                                             "");
            rsp.set_data_source(datasystem::DataTransferSource::DATA_DELAY_TRANSFER);
        }

        // We need to extend the ShmGuard lifecycle if we perform parallel urma_write/ucp_put_nbx.
        if (!IsFastTransportEnabled() || !blocking) {
            RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayload, objKv.GetReadOffset(), objKv.GetReadSize()));
        }
    }

    PerfPoint rspPoint(PerfKey::WORKER_REMOTE_GET_RESP);
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
    if (rc.GetCode() == K_RDMA_NEED_CONNECT) {
        // Clear remote ucp connection and reconnect in write payload.
        LOG(INFO) << "Rdma receive get request form restart worker " << requestAddress.ToString();
        return RemoveRemoteFastTransportNode(requestAddress);
    }
    return rc;
}

Status WorkerWorkerOCServiceImpl::BatchGetObjectRemote(
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<BatchGetObjectRemoteRspPb, BatchGetObjectRemoteReqPb>>
        serverApi)
{
    PerfPoint point(PerfKey::WORKER_SERVER_GET_REMOTE);
    BatchGetObjectRemoteReqPb req;
    BatchGetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payload;
    PerfPoint pointImpl(PerfKey::WORKER_SERVER_GET_REMOTE_READ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "GetObjectRemote read error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_IMPL);
    std::map<uint64_t, std::pair<std::vector<uint64_t>, std::vector<RpcMessage>>> keys;
    std::vector<GetObjectRemoteRspPb> getObjRemoteSubRsp;
    LOG(INFO) << "BatchGetObjectRemote request (objectKey, reqeustId, readOffset, readSize): "
              << VectorToString(req.requests());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    auto *signleReq = req.mutable_requests(0);
    *(signleReq->mutable_urma_instance_id()) = req.urma_instance_id();
    RETURN_IF_NOT_OK(CheckConnectionStable(*signleReq));
    if (req.requests_size() > FLAGS_oc_worker_worker_parallel_min && IsFastTransportEnabled()) {
        tbb::task_arena limited;
        if (FLAGS_oc_worker_worker_parallel_nums > 0) {
            limited.initialize(FLAGS_oc_worker_worker_parallel_nums);
        }
        std::vector<ParallelRes> parallelRes;

        AggregateInfo info;
        CHECK_FAIL_RETURN_STATUS(PrepareAggregateMemory(req, info), K_RUNTIME_ERROR, "Prepare Memory failed");
        uint64_t parallelSize = info.canBatchHandler ? info.batchReqSize.size() : req.requests_size();

        parallelRes.resize(parallelSize);
        limited.execute([&] {
            tbb::parallel_for(
                tbb::blocked_range<uint64_t>(0, parallelSize), [&](const tbb::blocked_range<uint64_t> &r) {
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
                            GetObjectRemoteBatchWrite(i, *subReq, rsp, payload, keys, parallelRes, batchPtr);
                        }
                        PerfPoint pDo(PerfKey::URMA_GATHER_WRITE_DO);
                        LOG_IF_ERROR(GatherWrite(i, info, batchPtr, parallelRes, req), "gather write error!");
                    }
                });
        });

        for (ParallelRes &loc : parallelRes) {
            for (auto &resp : loc.respPbs) {
                rsp.add_responses()->Swap(&resp);
            }

            payload.insert(payload.end(), std::make_move_iterator(loc.pays.begin()),
                           std::make_move_iterator(loc.pays.end()));

            for (auto &[idx, kp] : loc.kps) {
                keys.emplace(idx, std::move(kp));
            }
        }
    } else {
        for (int i = 0; i < req.requests_size(); i++) {
            std::vector<ParallelRes> emptyRes = {};
            auto *subReq = req.mutable_requests(i);
            *(subReq->mutable_comm_id()) = req.comm_id();
            GetObjectRemoteBatchWrite(i, *subReq, rsp, payload, keys, emptyRes);
        }
    }
    // Wait for fast transport events if the events are created and not already waited.

    pointImpl.RecordAndReset(PerfKey::FAST_TRANSPORT_TOTAL_EVENT_WAIT);
    for (auto &pair : keys) {
        int index = pair.first;
        auto remainingTime = []() { return reqTimeoutDuration.CalcRealRemainingTime(); };
        auto errorHandler = [index, &rsp](Status &status) {
            rsp.mutable_responses()->at(index).mutable_error()->set_error_code(status.GetCode());
            rsp.mutable_responses()->at(index).mutable_error()->set_error_msg(status.GetMsg());
            return status;
        };
        (void)WaitFastTransportEvent(pair.second.first, remainingTime, errorHandler);
        // Early release of ShmGuard.
        pair.second.second.clear();
    }

    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_WRITE);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Write(rsp), "GetObjectRemote write error");
    pointImpl.RecordAndReset(PerfKey::WORKER_SERVER_GET_REMOTE_SENDPAYLOAD);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->SendAndTagPayload(payload, FLAGS_oc_worker_worker_direct_port > 0),
                                     "GetObjectRemote send payload error");
    pointImpl.Record();
    VLOG(1) << FormatString("pull success");
    point.Record();
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
