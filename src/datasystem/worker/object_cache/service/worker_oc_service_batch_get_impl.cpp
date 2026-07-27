/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Defines the worker service Get process, for batch get purposes.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <numeric>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/async_update_location_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/service_execution_policy.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_int64(batch_get_threshold_mb, 100, "The payload threshold to batch get objects");
DS_DEFINE_validator(batch_get_threshold_mb, &Validator::ValidateBatchGetThreshold);
DS_DECLARE_bool(enable_data_replication);
DS_DECLARE_int32(oc_worker_worker_parallel_min);

using namespace datasystem::master;
namespace datasystem {
namespace object_cache {
namespace {
constexpr int64_t MIGRATE_DATA_TIMEOUT_MS = 200;



constexpr double US_PER_MS = 1000.0;

void LogInflightRemoteGetRequestIfNeeded(const metrics::Gauge &inflightGauge)
{
    constexpr int logInterval = 10;
    constexpr int64_t logLimit = 16;
    const int64_t inflightCount = inflightGauge.Get();
    if (inflightCount > logLimit) {
        LOG_EVERY_T(INFO, logInterval) << "inflight remote get request: " << inflightCount;
    }
}

constexpr size_t MAX_REMOTE_H2D_BATCH_GET_OBJECTS = 1024;

size_t RemoteH2DBatchGetChunkSize()
{
    // Reuse oc_worker_worker_parallel_min as a lower bound for each remote H2D chunk size.
    // A larger value here means more objects per task and therefore fewer parallel tasks.
    const auto parallelMin = FLAGS_oc_worker_worker_parallel_min > 0 ? FLAGS_oc_worker_worker_parallel_min : 0;
    return std::max<size_t>(MAX_REMOTE_H2D_BATCH_GET_OBJECTS, static_cast<size_t>(parallelMin));
}

}  // namespace

namespace {

bool NeedCleanupWorkerWorkerOcRpcChannel(const Status &rc)
{
    if (rc.IsOk()) {
        return false;
    }
    switch (rc.GetCode()) {
        case StatusCode::K_RPC_UNAVAILABLE:
        case StatusCode::K_RPC_DEADLINE_EXCEEDED:
        case StatusCode::K_RPC_CANCELLED:
        case StatusCode::K_URMA_CONNECT_FAILED:
        case StatusCode::K_URMA_WAIT_TIMEOUT:
        case StatusCode::K_TRY_AGAIN:
            return true;
        default:
            return false;
    }
}

void CleanupWorkerWorkerOcRpcChannel(
    const HostPort &hostAddr, const std::string &address,
    std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> &clientApi,
    std::shared_ptr<WorkerRemoteWorkerOCApi> &workerStub, Status rc, const std::string &stage)
{
    LOG(WARNING) << PIPLN_LOG_PREFIX "Remote get " << stage << " failed: address=" << address
                 << ", error=" << rc.ToString() << ". Clean RPC channel.";

    // Drop current per-call stream and shared stub before removing the cached stub.
    // Otherwise RpcStubCacheMgr::Remove may fail because the cached stub is still held by this request.
    clientApi.reset();
    workerStub.reset();

    auto removeRc = RpcStubCacheMgr::Instance().Remove(hostAddr, StubType::WORKER_WORKER_OC_SVC);
    if (removeRc.IsError()) {
        LOG(WARNING) << PIPLN_LOG_PREFIX "Remove RPC stub failed: address=" << address
                     << ", error=" << removeRc.ToString();
    } else {
        LOG(WARNING) << PIPLN_LOG_PREFIX "Remove RPC stub success: address=" << address;
    }
}

}  // namespace

bool WorkerOcServiceGetImpl::CanUpdateCopyMeta(const std::map<ReadKey, LockedEntity> &entries,
                                               const std::unordered_set<std::string> &skipKeys,
                                               const std::string &objectKey)
{
    if (skipKeys.find(objectKey) != skipKeys.end()) {
        return false;
    }
    auto entryIt = entries.find(ReadKey(objectKey));
    return entryIt != entries.end() && entryIt->second.safeObj != nullptr && entryIt->second.safeObj->Get() != nullptr
           && entryIt->second.safeObj->Get()->GetShmUnit() != nullptr;
}

Status WorkerOcServiceGetImpl::BatchGetRetrieveRemotePayload(uint64_t completeDataSize, ReadObjectKV &objectKV,
                                                             std::vector<RpcMessage> &payloads, uint64_t &payloadIndex)
{
    PerfPoint retrieveRemotePayloadPoint(PerfKey::WORKER_RETRIEVE_REMOTE_PAYLOAD);
    auto &entry = objectKV.GetObjEntry();
    const auto &objectKey = objectKV.GetObjKey();
    uint64_t offset = objectKV.GetReadOffset();
    auto needReceiveSz = objectKV.GetReadSize();
    auto metaSz = entry->GetMetadataSize();
    uint64_t cap = completeDataSize + metaSz;
    bool szChanged = (entry->GetShmUnit() == nullptr) || (entry->GetShmUnit()->size != cap);
    // Only create new shm if size changed or not exist.
    if (szChanged) {
        auto shmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK(AllocateMemoryForObject(objectKey, completeDataSize, metaSz, false, evictionManager_, *shmUnit,
                                                 entry->modeInfo.GetCacheType()));
        shmUnit->id = ShmKey::Intern(GetStringUuid());
        entry->SetShmUnit(shmUnit);
    }
    PerfPoint copyPoint(PerfKey::WORKER_MEMORY_COPY);

    // Pick up payload data until the expected size is fulfilled.
    std::vector<std::pair<const uint8_t *, uint64_t>> payloadData;
    size_t payloadLen;
    for (payloadLen = 0; payloadLen < needReceiveSz && payloadIndex < payloads.size(); payloadIndex++) {
        auto &payload = payloads[payloadIndex];
        payloadData.emplace_back(reinterpret_cast<const uint8_t *>(payload.Data()), payload.Size());
        payloadLen += payload.Size();
    }
    CHECK_FAIL_RETURN_STATUS(!(payloads.empty() || payloadLen == 0), K_INVALID,
                             "Payload is null or no bytes to write.");
    CHECK_FAIL_RETURN_STATUS(needReceiveSz == payloadLen, K_RUNTIME_ERROR, "Data size does not match.");
    auto memStatus = entry->GetShmUnit()->MemoryCopy(payloadData, memCpyThreadPool_, metaSz + offset);
    if (memStatus.IsError()) {
        // On MemoryCopy failure, free the ShmUnit and drop it from the entry so a retry reallocates
        // instead of reusing a freed unit whose pointer is null. Mirrors the #783 fix applied to
        // RetrieveRemotePayload and SaveBinaryObjectToMemory. See issues #783/#803.
        entry->GetShmUnit()->SetHardFreeMemory();
        entry->GetShmUnit()->FreeMemory();
        entry->SetShmUnit(nullptr);
        return memStatus;
    }
    return Status::OK();
}

void WorkerOcServiceGetImpl::HandleGetFailureHelper(const std::string &objectKey, uint64_t version,
                                                    std::shared_ptr<SafeObjType> &entry, bool isInsert)
{
    (void)isInsert;
    LOG(WARNING) << "Get object from remote failed, start to remove location from master";
    (void)RemoveLocation(objectKey, version);
    CleanupGetFailureOnLock(entry);
}

void WorkerOcServiceGetImpl::CleanupGetFailureOnLock(std::shared_ptr<SafeObjType> &entry)
{
    auto obj = entry->Get();
    if (obj == nullptr) {
        return;
    }
    if (obj->GetShmUnit() != nullptr) {
        obj->GetShmUnit()->SetHardFreeMemory();
    }
    LOG_IF_ERROR(obj->FreeResources(), "Free resources after remote get failure failed");
    obj->SetLifeState(ObjectLifeState::OBJECT_INVALID);
    obj->stateInfo.SetCacheInvalid(true);
}

Status WorkerOcServiceGetImpl::GetObjectsFromAnywhereBatched(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                             const std::shared_ptr<GetRequest> &request,
                                                             std::vector<RpcMessage> &payloads,
                                                             std::map<ReadKey, LockedEntity> &lockedEntries,
                                                             std::unordered_set<std::string> &failedIds,
                                                             std::set<ReadKey> &needRetryIds)
{
    PerfPoint point(PerfKey::WORKER_GET_BATCH_GROUPBY_DATA_NODE);
    RETURN_RUNTIME_ERROR_IF_NULL(workerBatchRemoteGetThreadPool_);
    Status lastRc = Status::OK();
    std::vector<std::string> successIds;
    successIds.reserve(queryMetas.size());
    // First group queried meta by address, take out the ones that are problematic.
    // And also take out the ones that got their payload from QueryMeta already.
    std::unordered_map<std::string, std::list<std::pair<std::list<GetObjectInfo>, uint64_t>>> groupedQueryMetas;
    groupedQueryMetas.reserve(queryMetas.size());
    std::vector<master::QueryMetaInfoPb> payloadIndexMetas;
    payloadIndexMetas.reserve(queryMetas.size());
    for (auto &queryMeta : queryMetas) {
        const auto &meta = queryMeta.meta();
        const auto &objectKey = meta.object_key();
        const auto dataFormat = static_cast<DataFormat>(meta.config().data_format());
        if (dataFormat != DataFormat::BINARY && dataFormat != DataFormat::HETERO) {
            lastRc = Status(K_INVALID, "object data format not match.");
            failedIds.emplace(meta.object_key());
            LOG(ERROR) << lastRc;
            continue;
        }
        auto iter = lockedEntries.find(ReadKey(objectKey));
        if (iter == lockedEntries.end()) {
            LOG(ERROR) << FormatString("[ObjectKey %s] QueryMeta exist but lock entry absent, should not happen",
                                       objectKey);
            lastRc = Status(K_UNKNOWN_ERROR, "QueryMeta exist but lock entry absent, should not happen");
            continue;
        }
        auto &safeObj = iter->second.safeObj;
        if (queryMeta.payload_indexs_size() != 0) {
            payloadIndexMetas.emplace_back(queryMeta);
        } else {
            GetObjectInfo info;
            info.entry = &(iter->second);
            info.readKey = &(iter->first);
            info.queryMeta = &queryMeta;
            GroupQueryMeta(info, groupedQueryMetas);
            SetObjectEntryAccordingToMeta(meta, GetMetadataSize(), *safeObj);
        }
    }

    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_HANDLE_DATA_IN_META);
    // For the ones that already got their payload from queried meta, fallback to existing logic.
    lastRc =
        GetObjectsFromAnywhereSerially(payloadIndexMetas, request, payloads, lockedEntries, failedIds, needRetryIds);
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_BEFORE_RUN);
    // And then deal with the requests that can be batched.
    struct BatchRemoteGetTask {
        const std::string *address;
        std::list<GetObjectInfo> *infos;
    };
    std::vector<BatchRemoteGetTask> tasks;
    for (auto &groupedQueryMeta : groupedQueryMetas) {
        for (auto &infoPair : groupedQueryMeta.second) {
            tasks.emplace_back(BatchRemoteGetTask{ &groupedQueryMeta.first, &infoPair.first });
        }
    }

    std::vector<std::future<Status>> futures;
    futures.reserve(tasks.size());
    std::list<GetObjectInfo> failedMetas;
    std::vector<std::list<GetObjectInfo>> tempFailedMetas(tasks.size());
    std::vector<std::vector<std::string>> tempSuccessIds(tasks.size());
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(tasks.size());
    std::vector<std::unordered_set<std::string>> tempFailedIds(tasks.size());
    auto traceContext = Trace::Instance().GetContext();
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_RUN);
    std::mutex lastRcMutex;
    auto mergeLastRc = [&lastRc, &lastRcMutex](const Status &rc) {
        if (rc.IsError()) {
            std::lock_guard<std::mutex> lock(lastRcMutex);
            lastRc = rc;
        }
    };
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(remainingUs > 0, K_RPC_DEADLINE_EXCEEDED, "RPC deadline exceeded");
    auto dispatchTime = std::chrono::steady_clock::now();
    const bool useThreadPoolFanout = ShouldUseServiceThreadPoolFanout(FLAGS_use_brpc);
    for (size_t index = 0; index < tasks.size(); ++index) {
        auto *infos = tasks[index].infos;
        const auto address = *tasks[index].address;
        auto func = [this, address, infos, &request, &tempSuccessIds, &tempNeedRetryIds, &tempFailedIds,
                     &tempFailedMetas, index, traceContext, remainingUs, dispatchTime] {
            RETURN_IF_NOT_OK(InitTimeoutsFromDispatch(remainingUs, dispatchTime));
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext, true);
            return BatchGetObjectFromRemoteOnLock(address, *infos, request, tempSuccessIds[index],
                                                 tempNeedRetryIds[index], tempFailedIds[index], tempFailedMetas[index]);
        };
        if (!useThreadPoolFanout || index + 1 == tasks.size()) {
            auto rc = func();
            LOG_IF_ERROR(rc, "BatchGetObjectFromRemoteOnLock failed");
            mergeLastRc(rc);
        } else {
            futures.emplace_back(workerBatchRemoteGetThreadPool_->Submit(std::move(func)));
        }
    }
    for (auto &fut : futures) {
        auto rc = fut.get();
        LOG_IF_ERROR(rc, "BatchGetObjectFromRemoteOnLock failed");
        mergeLastRc(rc);
    }
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_WAIT_REMOTE_TASKS);

    for (uint64_t i = 0; i < tasks.size(); ++i) {
        if (!tempSuccessIds[i].empty()) {
            successIds.insert(successIds.end(), std::make_move_iterator(tempSuccessIds[i].begin()),
                              std::make_move_iterator(tempSuccessIds[i].end()));
        }
        if (!tempNeedRetryIds[i].empty()) {
            needRetryIds.insert(std::make_move_iterator(tempNeedRetryIds[i].begin()),
                                std::make_move_iterator(tempNeedRetryIds[i].end()));
        }
        if (!tempFailedIds[i].empty()) {
            failedIds.insert(tempFailedIds[i].begin(), tempFailedIds[i].end());
        }
        if (!tempFailedMetas[i].empty()) {
            failedMetas.splice(failedMetas.end(), tempFailedMetas[i]);
        }
    }
    auto infoIter = failedMetas.begin();
    while (infoIter != failedMetas.end()) {
        auto &objectKey = infoIter->queryMeta->meta().object_key();
        auto &pair = infoIter->entry;
        auto &entry = pair->safeObj;
        bool isInsert = pair->insert;
        HandleGetFailureHelper(objectKey, infoIter->queryMeta->meta().version(), entry, isInsert);
        infoIter++;
    }
    if (successIds.size() != (queryMetas.size() - payloadIndexMetas.size())) {
        LOG(ERROR) << "Failed to get object data from remote. " << successIds.size() << " objects pulled success: ["
                   << VectorToString(successIds) << "], meta data num: " << queryMetas.size()
                   << " lastRc: " << lastRc.ToString();
    }
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_UPDATE_LOCATION);
    BatchUpdateLocationHelper(successIds, queryMetas, lockedEntries);
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_OTHER);
    return lastRc;
}

void WorkerOcServiceGetImpl::MarkNeedDeleteForDisconnectedMasters(
    const std::vector<std::string> &successIds, std::map<ReadKey, LockedEntity> &entries,
    std::unordered_set<std::string> &needSetDeleteObjectKeys)
{
    // If the master is disconnected before updating the location, we have to give up retaining the replica, because the
    // master will only manage the replica through the location.
    auto grouped = metadataRouteResolver_->GroupOwners(successIds);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    needSetDeleteObjectKeys.reserve(successIds.size());
    for (const auto &item : objKeysGrpByMaster) {
        if (endpointPolicy_ == nullptr || endpointPolicy_->CheckEndpoint(item.first, false).IsError()) {
            needSetDeleteObjectKeys.insert(item.second.begin(), item.second.end());
        }
    }
    for (const auto &objectKey : needSetDeleteObjectKeys) {
        auto it = entries.find(ReadKey(objectKey));
        if (it != entries.end()) {
            auto rc = TryLockWithRetry(objectKey, it->second.safeObj);
            if (rc.IsError()) {
                continue;
            }
            it->second.safeObj->Get()->stateInfo.SetNeedToDelete(true);
            it->second.safeObj->WUnlock();
        }
    }
}

void WorkerOcServiceGetImpl::SubmitAsyncUpdateLocation(std::vector<UpdateLocationParam> &params)
{
    if (params.empty()) {
        return;
    }
    UpdateLocationTask asyncUpdateTask = UpdateLocationTask(params);
    asyncUpdateLocationManager_->AddTask(std::move(asyncUpdateTask));
}

void WorkerOcServiceGetImpl::BatchUpdateLocationHelper(const std::vector<std::string> &successIds,
                                                       const std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                       std::map<ReadKey, LockedEntity> &entries)
{
    if (!FLAGS_enable_data_replication) {
        return;
    }
    std::unordered_set<std::string> needSetDeleteObjectKeys;
    MarkNeedDeleteForDisconnectedMasters(successIds, entries, needSetDeleteObjectKeys);

    std::vector<UpdateLocationParam> asyncUpdateLocationParams;
    // Capture trace ID once on the handler thread so the async update location
    // worker can re-establish it for correlated logging and the downstream
    // CreateCopyMeta RPC.
    const std::string traceID = Trace::Instance().GetTraceID();
    if (successIds.size() == queryMetas.size()) {
        for (auto &queryMeta : queryMetas) {
            const auto &objectKey = queryMeta.meta().object_key();
            if (!CanUpdateCopyMeta(entries, needSetDeleteObjectKeys, objectKey)) {
                continue;
            }
            asyncUpdateLocationParams.emplace_back(
                UpdateLocationParam{ objectKey, queryMeta.meta().version(),
                                     static_cast<uint32_t>(queryMeta.meta().config().data_format()), traceID });
        }
    } else {
        std::unordered_set<std::string> successIdSet;
        successIdSet.reserve(successIds.size());
        successIdSet.insert(successIds.begin(), successIds.end());
        for (auto &queryMeta : queryMetas) {
            const auto &meta = queryMeta.meta();
            const auto &objectKey = meta.object_key();
            if (successIdSet.find(objectKey) != successIdSet.end()
                && CanUpdateCopyMeta(entries, needSetDeleteObjectKeys, objectKey)) {
                asyncUpdateLocationParams.emplace_back(UpdateLocationParam{
                    objectKey, meta.version(), static_cast<uint32_t>(meta.config().data_format()), traceID });
            }
        }
    }
    SubmitAsyncUpdateLocation(asyncUpdateLocationParams);
}

void WorkerOcServiceGetImpl::BatchUpdateLocationHelper(const std::vector<std::string> &successIds,
                                                       const QueryMetaMap &queryMetas,
                                                       std::map<ReadKey, LockedEntity> &entries)
{
    if (!FLAGS_enable_data_replication) {
        return;
    }
    std::unordered_set<std::string> needSetDeleteObjectKeys;
    MarkNeedDeleteForDisconnectedMasters(successIds, entries, needSetDeleteObjectKeys);

    std::vector<UpdateLocationParam> asyncUpdateLocationParams;
    // Capture trace ID once on the handler thread so the async update location
    // worker can re-establish it for correlated logging and the downstream
    // CreateCopyMeta RPC.
    const std::string traceID = Trace::Instance().GetTraceID();
    for (const auto &objectKey : successIds) {
        if (!CanUpdateCopyMeta(entries, needSetDeleteObjectKeys, objectKey)) {
            continue;
        }
        const auto &meta = queryMetas.at(objectKey);
        asyncUpdateLocationParams.emplace_back(
            UpdateLocationParam{ meta.meta().object_key(), meta.meta().version(),
                                 static_cast<uint32_t>(meta.meta().config().data_format()), traceID });
    }
    SubmitAsyncUpdateLocation(asyncUpdateLocationParams);
}

void WorkerOcServiceGetImpl::GroupQueryMeta(
    GetObjectInfo &info,
    std::unordered_map<std::string, std::list<std::pair<std::list<GetObjectInfo>, uint64_t>>> &groupedQueryMetas)
{
    const static uint64_t maxPayloadSize = FLAGS_batch_get_threshold_mb * 1024 * 1024;
    const auto &meta = info.queryMeta->meta();
    auto &splitList = groupedQueryMetas[info.queryMeta->address()];
    if (splitList.empty()) {
        splitList.emplace_back(std::make_pair(std::list<GetObjectInfo>{}, 0));
    }
    if (IsRemoteH2DEnabled() && splitList.back().first.size() >= RemoteH2DBatchGetChunkSize()) {
        splitList.emplace_back(std::make_pair(std::list<GetObjectInfo>{}, 0));
    }
    if (!IsFastTransportEnabled() && !IsRemoteH2DEnabled() && (FLAGS_batch_get_threshold_mb != 0)) {
        auto payloadSize = meta.data_size() < UINT64_MAX - splitList.back().second
                               ? splitList.back().second + meta.data_size()
                               : UINT64_MAX;
        if (splitList.back().second > 0 && payloadSize > maxPayloadSize) {
            splitList.emplace_back(std::make_pair(std::list<GetObjectInfo>{}, 0));
        }
    }
    splitList.back().first.emplace_back(info);
    splitList.back().second += meta.data_size();
}

void WorkerOcServiceGetImpl::BatchGetObjectHandleIndividualStatus(Status &status, const ReadKey &readKey,
                                                                  std::vector<std::string> &successIds,
                                                                  std::vector<ReadKey> &needRetryIds,
                                                                  std::unordered_set<std::string> &failedIds)
{
    if (status.IsOk()) {
        successIds.emplace_back(readKey.objectKey);
        return;
    }
    if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
        LOG(INFO) << FormatString("[ObjectKey %s] Object not found in remote worker.", readKey.objectKey);
        status = Status::OK();
        needRetryIds.emplace_back(readKey);
    } else if (status.GetCode() == K_OC_REMOTE_GET_NOT_ENOUGH) {
        // Note that it gets retried at BatchGetObjectFromRemoteWorker, so do not need to add to needRetryIds.
        LOG(INFO) << FormatString("[ObjectKey %s] Object size changed, needs retry.", readKey.objectKey);
    } else if (status.GetCode() == K_OUT_OF_MEMORY) {
        LOG(INFO) << FormatString("[ObjectKey %s] Out of memory, get remote abort.", readKey.objectKey);
    } else {
        LOG(ERROR) << FormatString("[ObjectKey %s] Get from remote failed: %s.", readKey.objectKey, status.ToString());
    }
    failedIds.emplace(readKey.objectKey);
}

Status WorkerOcServiceGetImpl::HandleBatchSubResponse(const GetObjectRemoteRspPb &subResp,
                                                      const RemoteH2DRootInfoPb &batchRootInfo, ObjectMetaPb *meta,
                                                      ReadObjectKV &objectKV, std::vector<RpcMessage> &payloads,
                                                      uint64_t &payloadIndex, bool &tryGetFromElsewhere,
                                                      bool &dataSizeChange)
{
    Status subRc = Status(static_cast<StatusCode>(subResp.error().error_code()), subResp.error().error_msg());
    if (subRc.GetCode() == K_OC_REMOTE_GET_NOT_ENOUGH) {
        // If this error happens, remote worker should also sent the changed data size.
        if (subResp.data_size() == 0) {
            subRc = Status(K_INVALID, "object size should be greater than 0");
        } else {
            // Update the data size for the next round.
            dataSizeChange = true;
            meta->set_data_size(subResp.data_size());
        }
        tryGetFromElsewhere = false;
    } else if (subRc.IsOk()) {
        // Successful logics
        // Payload data handling
        if (subResp.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
            // At this point, we haven't materialized the payload which is still sitting in the tcp/ip buffers.
            // We either receive payload directly into shared memory or fall back to the old behavior to save
            // the payload in ZMQ private memory
            subRc = BatchGetRetrieveRemotePayload(subResp.data_size(), objectKV, payloads, payloadIndex);
        }

        if (IsRemoteH2DEnabled() && (subResp.data_source() == DataTransferSource::DATA_DELAY_TRANSFER)
            && subResp.has_host_info()) {
            auto hostInfo = std::make_shared<RemoteH2DHostInfoPb>();
            *hostInfo = std::move(subResp.host_info());
            *(hostInfo->mutable_root_info()) = batchRootInfo;
#ifdef BUILD_HETERO
            objectKV.GetObjEntry()->SetRemoteHostInfo(*objectKV.commId_, hostInfo);
#endif
        }
        if (subResp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED_MEMSET_META) {
            PerfPoint pMemSet(PerfKey::URMA_GATHER_MEMSET);
            auto metaSize = objectKV.GetObjEntry()->GetMetadataSize();
            memset_s(objectKV.GetObjEntry()->GetShmUnit()->pointer, metaSize, 0, metaSize);
        }
    }
    return subRc;
}

void WorkerOcServiceGetImpl::HandleBatchSubResponsePart2(Status &subRc, const std::string &address, ObjectMetaPb *meta,
                                                         ReadObjectKV &objectKV, const Status &checkConnectStatus,
                                                         bool &tryGetFromElsewhere,
                                                         std::vector<std::string> &needEvictIds)
{
    auto &objectKey = objectKV.GetObjKey();
    auto &entry = objectKV.GetObjEntry();
    // Same retry logic as in GetObjectFromRemoteWorkerWithoutDump,
    // this would fallback to non-batched version as the primary address would be different.
    const auto &primaryAddress = meta->primary_address();
    if (subRc.IsError() && checkConnectStatus.IsOk() && !address.empty() && !primaryAddress.empty()
        && primaryAddress != address && primaryAddress != localAddress_.ToString()) {
        // Remote get may fail if provider can't acquire read latch too many times.
        LOG(INFO) << FormatString("[ObjectKey %s] Object may not exist in %s, try to get from primary copy %s, %s",
                                  objectKey, address, primaryAddress, subRc.ToString());
        subRc = PullObjectDataFromRemoteWorker(primaryAddress, meta->data_size(), objectKV);
    }
    // Second round of error handling, as retrive payload might fail, and it might fallback to
    // PullObjectDataFromRemoteWorker.
    if (subRc.IsOk()) {
        PerfPoint point(PerfKey::WORKER_HANDLE_BATCH_SUB_ADD_EVICTION);
        needEvictIds.emplace_back(objectKey);
        entry->stateInfo.SetNeedToDelete(true);
        point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_SYNC_META);
        if (FLAGS_enable_data_replication) {
            subRc = ProcessObjectEntryAndSyncMetadata(objectKV, true);
        }
        point.Record();
    }
    // Handle error as in GetObjectFromRemoteOnLock code path, move on to the next request.
    if (subRc.GetCode() == K_OUT_OF_MEMORY || IsRpcTimeoutOrTryAgain(subRc)) {
        tryGetFromElsewhere = false;
    } else if (checkConnectStatus.IsOk() && !address.empty() && entry.Get() == nullptr) {
        subRc = Status(K_NOT_FOUND, FormatString("Get from remote worker failed, object(%s) not exist in "
                                                 "worker, maybe the object has been deleted.",
                                                 objectKey));
        tryGetFromElsewhere = false;
    }
}

Status WorkerOcServiceGetImpl::ProcessBatchResponse(
    const std::string &address, Status &checkConnectStatus, std::list<GetObjectInfo> &infos,
    const std::shared_ptr<GetRequest> &request, const Status &status, BatchGetObjectRemoteRspPb &rspPb,
    std::vector<RpcMessage> &payloads, std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
    std::unordered_set<std::string> &failedIds, std::list<GetObjectInfo> &failedInfos, bool &dataSizeChange)
{
    PerfPoint all(PerfKey::WORKER_HANDLE_BATCH_GET_RESPONSE);
    Status lastRc = status;
    uint64_t payloadIndex = 0;
    auto iter = infos.begin();
    std::vector<std::string> needEvictObjs;
    needEvictObjs.reserve(infos.size());
    std::shared_ptr<std::string> commId = nullptr;
    if (IsRemoteH2DEnabled() && request != nullptr) {
        commId = std::make_shared<std::string>(request->GetClientCommUuid());
    }
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    for (int i = 0; iter != infos.end(); i++) {
        bool isFromL2 = false;
        auto metaIter = iter->queryMeta->mutable_meta();
        PerfPoint point(PerfKey::WORKER_HANDLE_BATCH_SUB_PRE);
        auto &objectKey = iter->queryMeta->meta().object_key();
        auto lockEntry = iter->entry;
        if (lockEntry == nullptr) {
            continue;
        }
        auto entry = lockEntry->safeObj;
        const auto &readKey = iter->readKey;
        ReadObjectKV objectKV(*readKey, *entry, commId);
        Status subRc = status;
        bool tryGetFromElsewhere = true;
        bool dataSizeChanged = false;
        if (subRc.IsOk()) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_PAYLOAD);
            auto &subResp = rspPb.responses(i);
            subRc = HandleBatchSubResponse(subResp, rspPb.root_info(), metaIter, objectKV, payloads, payloadIndex,
                                           tryGetFromElsewhere, dataSizeChanged);
            if (subRc.IsOk() && request && subResp.data_source() == DataTransferSource::DATA_ALREADY_TRANSFERRED) {
                OsXprtPipln::MarkPipelineStep1Ok(request->GetH2DChunkManager(), objectKV.GetObjKey());
            }
        }
        if (tryGetFromElsewhere) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_RESP_PT_2);
        }
        HandleBatchSubResponsePart2(subRc, address, metaIter, objectKV, checkConnectStatus, tryGetFromElsewhere,
                                    needEvictObjs);
        if (subRc.IsError() && tryGetFromElsewhere) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_L2);
            Timer timer;
            bool ifWorkerConnected = checkConnectStatus.IsOk();
            TryGetFromL2CacheWhenNotFoundInWorker(*metaIter, address, ifWorkerConnected, objectKV, subRc, traceEnabled);
            LOG(INFO) << "Query from L2 cache use " << timer.ElapsedMilliSecond()
                      << " millisecond, address: " << address << ", ifWorkerConnected: " << ifWorkerConnected;
            CheckAndReturnPullNotFoundForRetry(*metaIter, address, *entry, checkConnectStatus, subRc);
            isFromL2 = true;
        }
        if (subRc.IsOk() && entry->Get() == nullptr) {
            subRc = Status(K_NOT_FOUND, FormatString("Get from remote worker failed, object(%s) not exist in "
                                                     "worker, maybe the object has been deleted.",
                                                     objectKey));
        }
        if (subRc.IsOk()) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_UPDATA_REQUEST);
            subRc = UpdateRequestForSuccess(objectKV, request);
        }
        const bool needFailureCleanup = !dataSizeChanged && subRc.IsError();
        point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_STATUS);
        BatchGetObjectHandleIndividualStatus(subRc, *readKey, successIds, needRetryIds, failedIds);
        if (!dataSizeChanged) {
            auto current = iter++;
            if (needFailureCleanup) {
                failedInfos.splice(failedInfos.end(), infos, current);
            } else {
                infos.erase(current);
            }
        } else {
            dataSizeChange = true;
            iter++;
        }
        lastRc = subRc;
        if (subRc.IsOk()) {
            isFromL2 ? CacheHitInfo::Instance().IncL2Hit(1) : CacheHitInfo::Instance().IncRemoteHit(1);
        }
        point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_OTHER);
    }
    SubmitAsyncAddEvictTask(std::move(needEvictObjs));
    return lastRc;
}

#define CLEAN_RPC_AND_RETURN_WHEN_ERROR(func, hint)                                                   \
    do {                                                                                              \
        auto ret = func;                                                                              \
        if (ret.IsError()) {                                                                          \
            ret = WithRpcDiag(ret, "BatchGetObjectRemote", localAddress_, address);                   \
            /* pipeline chunk-spliting may introduce more urma request                                \
             * and make urma wait error happen more frequently */                                     \
            if (request && OsXprtPipln::IsPiplnH2DRequest(request->GetH2DChunkManager())              \
                && NeedCleanupWorkerWorkerOcRpcChannel(ret)) {                                        \
                CleanupWorkerWorkerOcRpcChannel(hostAddr, address, clientApi, workerStub, ret, hint); \
            }                                                                                         \
            return ret;                                                                               \
        }                                                                                             \
    } while (0)

Status WorkerOcServiceGetImpl::PrepareBatchGetRemoteRequest(
    const std::string &address, std::list<GetObjectInfo> &infos, const std::shared_ptr<GetRequest> &request,
    std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
    std::unordered_set<std::string> &failedIds, PerfPoint &point, HostPort &hostAddr,
    Status &checkConnectStatus, BatchGetObjectRemoteReqPb &reqPb)
{
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_RUNTIME_ERROR,
                             "Fail to get objects from remote worker, no object copy exists.");
    CHECK_FAIL_RETURN_STATUS(address != localAddress_.ToString(), K_RUNTIME_ERROR,
                             "Remote getting from self address is invalid");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hostAddr.ParseString(address),
                                     FormatString("Parse object address %s failed", address));
    CHECK_FAIL_RETURN_STATUS(endpointPolicy_ != nullptr, K_NOT_READY, "Object endpoint policy is unavailable");
    checkConnectStatus = endpointPolicy_->CheckEndpoint(hostAddr, false);
    CHECK_FAIL_RETURN_STATUS(checkConnectStatus.IsOk(), K_RUNTIME_ERROR,
                             "Fail to get objects from remote worker, no object copy exists.");
    INJECT_POINT("worker.before_GetObjectFromRemoteWorkerAndDump");
    point.RecordAndReset(PerfKey::WORKER_BATCH_GET_CONSTRUCT_GET_REQUEST);
    RETURN_IF_NOT_OK(ConstructBatchGetRequest(address, infos, request, successIds, needRetryIds, failedIds, reqPb));
    VLOG(1) << AppendSrcDstForLog(FormatString("[Get] Remote pull, count: %d, path: %s", reqPb.requests_size(),
                                               IsUrmaEnabled() ? "UB" : (IsUcpEnabled() ? "RDMA" : "TCP")),
                                      localAddress_.ToString(), address);
    INJECT_POINT("worker.remote_get_failed");
    point.RecordAndReset(PerfKey::WORKER_BATCH_GET_CREATE_REMOTE_API);
    return Status::OK();
}

Status WorkerOcServiceGetImpl::SendBatchGetRemoteRequest(
    const std::string &address, const HostPort &hostAddr, const std::shared_ptr<GetRequest> &request,
    int64_t migrateDataTimeoutMs, uint64_t rpcSlowerThanUs, PerfPoint &point,
    BatchGetObjectRemoteReqPb &reqPb, BatchGetObjectRemoteRspPb &rspPb, std::vector<RpcMessage> &payloads)
{
    std::shared_ptr<WorkerRemoteWorkerOCApi> workerStub;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(address, localAddress_, akSkManager_, workerStub),
                                     "Create remote worker api failed.");
    std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> clientApi;
    int64_t timeoutMs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
    timeoutMs = request != nullptr ? timeoutMs : std::min(timeoutMs, migrateDataTimeoutMs);
    point.RecordAndReset(PerfKey::WORKER_BATCH_GET_SEND_AND_RECV);
    auto inflightGauge =
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_INFLIGHT_REMOTE_GET_REQUEST));
    inflightGauge.Inc();
    LogInflightRemoteGetRequestIfNeeded(inflightGauge);
    Raii inflightGuard([inflightGauge]() { inflightGauge.Dec(); });
    Timer timer;
    constexpr int32_t minRetryOnceRpcMs = 1;  // The first level of retryIntervalsMs.
    auto rc = RetryOnErrorRepent(
        timeoutMs,
        [this, &workerStub, &reqPb, &rspPb, &clientApi, &address, &payloads, &hostAddr, &request](int32_t) {
            PerfPoint rpcPoint(PerfKey::WORKER_BATCH_REMOTE_GET_RPC);
            if (workerStub == nullptr) {
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                    CreateRemoteWorkerApi(address, localAddress_, akSkManager_, workerStub),
                    "Recreate remote worker api failed.");
            }
            CLEAN_RPC_AND_RETURN_WHEN_ERROR(workerStub->BatchGetObjectRemote(&clientApi), "open stream");
            CLEAN_RPC_AND_RETURN_WHEN_ERROR(workerStub->BatchGetObjectRemoteWrite(clientApi, reqPb), "write request");
            auto readRc = clientApi->Read(rspPb);
            CLEAN_RPC_AND_RETURN_WHEN_ERROR(TryReconnectRemoteWorker(address, readRc), "read response/reconnect");
            // Multiple spilled objects can share the payload, so keep compatibility with down-level clients here.
            CLEAN_RPC_AND_RETURN_WHEN_ERROR(clientApi->ReceivePayload(payloads), "receive payload");
            return Status::OK();
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_URMA_CONNECT_FAILED, StatusCode::K_URMA_WAIT_TIMEOUT },
        minRetryOnceRpcMs);
    const auto elapsedUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    const double elapsedMs = static_cast<double>(elapsedUs) / US_PER_MS;
    SLOW_LOG_IF_OR_VLOG(
        INFO, rpcSlowerThanUs > 0 && elapsedUs >= rpcSlowerThanUs, 1,
        AppendSrcDstForLog(
            FormatString("[Get] Remote done, count: %d, path: %s, cost: %.3fms", reqPb.requests_size(),
                         IsUrmaEnabled() ? "UB" : (IsUcpEnabled() ? "RDMA" : "TCP"), elapsedMs),
            localAddress_.ToString(), address));
    return rc;
}

Status WorkerOcServiceGetImpl::BatchGetObjectFromRemoteWorker(
    const std::string &address, std::list<GetObjectInfo> &infos, const std::shared_ptr<GetRequest> &request,
    std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
    std::unordered_set<std::string> &failedIds, std::list<GetObjectInfo> &failedMetas)
{
    const auto traceConfig = GetServerLatencyTraceConfig();
    successIds.reserve(successIds.size() + infos.size());
    needRetryIds.reserve(needRetryIds.size() + infos.size());
    failedIds.reserve(failedIds.size() + infos.size());
    bool dataSizeChange;
    Status lastRc;
    Status checkConnectStatus;
    do {
        dataSizeChange = false;
        BatchGetObjectRemoteReqPb reqPb;
        BatchGetObjectRemoteRspPb rspPb;
        std::vector<RpcMessage> payloads;
        if (!infos.empty()) {
            reqPb.mutable_requests()->Reserve(static_cast<int>(infos.size()));
        }
        PerfPoint point(PerfKey::WORKER_BATCH_GET_CONSTRUCT_AND_SEND);
        Status rc;
        {
            PerfPoint detailPoint(PerfKey::WORKER_BATCH_GET_CONSTRUCT_AND_SEND_PRE);
            HostPort hostAddr;
            rc = PrepareBatchGetRemoteRequest(address, infos, request, successIds, needRetryIds, failedIds,
                                              detailPoint, hostAddr, checkConnectStatus, reqPb);
            if (rc.IsOk()) {
                rc = SendBatchGetRemoteRequest(address, hostAddr, request, MIGRATE_DATA_TIMEOUT_MS,
                                               traceConfig.rpcSlowerThanUs, detailPoint, reqPb, rspPb, payloads);
            }
        }
        point.Record();
        point.RecordAndReset(PerfKey::WORKER_BATCH_GET_HANDLE_RESPONSE);
        lastRc = ProcessBatchResponse(address, checkConnectStatus, infos, request, rc, rspPb, payloads, successIds,
                                      needRetryIds, failedIds, failedMetas, dataSizeChange);
    } while (dataSizeChange);
    return lastRc;
}

Status WorkerOcServiceGetImpl::BatchGetObjectFromRemoteOnLock(
    const std::string &address, std::list<GetObjectInfo> &infos, const std::shared_ptr<GetRequest> &request,
    std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
    std::unordered_set<std::string> &failedIds, std::list<GetObjectInfo> &failedMetas)
{
    PerfPoint point(PerfKey::WORKER_PULL_REMOTE_DATA);
    // Construct and send request for batch remote get.
    return BatchGetObjectFromRemoteWorker(address, infos, request, successIds, needRetryIds, failedIds, failedMetas);
}

Status WorkerOcServiceGetImpl::AggregateAllocateHelper(std::list<GetObjectInfo> &infos,
                                                       std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                       std::vector<uint32_t> &shmIndexMapping)
{
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> traversalHelper =
        [&infos](std::function<void(uint64_t, uint64_t, uint32_t)> collector, bool &needAggregate) {
            needAggregate = infos.size() > 1;
            uint32_t objectIndex = 0;
            for (auto iter = infos.begin(); iter != infos.end() && needAggregate; iter++, objectIndex++) {
                const auto &meta = iter->queryMeta->meta();
                auto dataSz = meta.data_size();
                auto &entry = *(iter->entry->safeObj);
                auto metaSz = entry->GetMetadataSize();
                uint64_t shmSize = dataSz + metaSz;

                auto shmUnit = entry->GetShmUnit();
                // Skip the aggregation if allocation is not needed for the object.
                bool szChanged = (shmUnit == nullptr) || (shmUnit->size != shmSize);
                if (!szChanged) {
                    continue;
                }
                collector(dataSz, shmSize, objectIndex);
            }
        };
    auto firstObjectKey = infos.front().queryMeta->meta().object_key();
    RETURN_IF_NOT_OK(AggregateAllocate(firstObjectKey, traversalHelper, evictionManager_, shmOwners, shmIndexMapping));
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
