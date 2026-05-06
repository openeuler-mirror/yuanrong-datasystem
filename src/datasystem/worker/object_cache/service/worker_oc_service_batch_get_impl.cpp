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

/**
 * Description: Defines the worker service Get process, for batch get purposes.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/async_update_location_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_int64(batch_get_threshold_mb, 100, "The payload threshold to batch get objects");
DS_DEFINE_validator(batch_get_threshold_mb, &Validator::ValidateBatchGetThreshold);
DS_DECLARE_bool(enable_data_replication);

using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

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
    RETURN_IF_NOT_OK(entry->GetShmUnit()->MemoryCopy(payloadData, memCpyThreadPool_, metaSz + offset));
    return Status::OK();
}

void WorkerOcServiceGetImpl::HandleGetFailureHelper(const std::string &objectKey, uint64_t version,
                                                    std::shared_ptr<SafeObjType> &entry, bool isInsert)
{
    (void)isInsert;
    LOG(WARNING) << "Get object from remote failed, start to remove location from master";
    (void)RemoveLocation(objectKey, version);
    auto obj = entry->Get();
    if (obj == nullptr) {
        return;
    }
    if (obj->GetShmUnit() != nullptr) {
        obj->GetShmUnit()->SetHardFreeMemory();
    }
    obj->FreeResources();
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
    std::vector<master::QueryMetaInfoPb> payloadIndexMetas;
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
    std::vector<std::future<Status>> futures;
    std::list<GetObjectInfo> failedMetas;
    std::vector<std::list<GetObjectInfo>> tempFailedMetas(groupedQueryMetas.size());
    std::vector<std::vector<std::string>> tempSuccessIds(groupedQueryMetas.size());
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(groupedQueryMetas.size());
    std::vector<std::unordered_set<std::string>> tempFailedIds(groupedQueryMetas.size());
    size_t index = 0;
    auto traceContext = Trace::Instance().GetContext();
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_RUN);
    std::mutex lastRcMutex;
    for (auto queryMeta = groupedQueryMetas.begin(); queryMeta != groupedQueryMetas.end(); ++queryMeta, ++index) {
        auto &address = queryMeta->first;
        auto &infoList = queryMeta->second;

        auto func = [this, &lastRc, address, &infoList, &request, &tempSuccessIds, &tempNeedRetryIds, &tempFailedIds,
                     &tempFailedMetas, index, traceContext, &lastRcMutex] {
            Status tmpRc = Status::OK();
            for (auto &infoPair : infoList) {
                auto &infos = infoPair.first;
                TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext, true);
                tmpRc = BatchGetObjectFromRemoteOnLock(address, infos, request, tempSuccessIds[index],
                                                       tempNeedRetryIds[index], tempFailedIds[index],
                                                       tempFailedMetas[index]);
            }
            {
                std::lock_guard<std::mutex> lock(lastRcMutex);
                lastRc = tmpRc;
            }
            return tmpRc;
        };
        if (index + 1 == groupedQueryMetas.size()) {
            LOG_IF_ERROR(func(), "BatchGetObjectFromRemoteOnLock failed");
        } else {
            // Fixme: reqTimeoutDuration is not initialized when this function is called, so the timeout set in
            // RpcOptions inside BatchGetObjectFromRemoteOnLock may not work as expected.
            futures.emplace_back(workerBatchRemoteGetThreadPool_->Submit(std::move(func)));
        }
    }
    for (auto &fut : futures) {
        if (!fut.get().IsOk()) {
            LOG(ERROR) << "BatchGetObjectFromRemoteOnLock failed";
        }
    }
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_AFTER_RUN);

    for (uint64_t i = 0; i < groupedQueryMetas.size(); ++i) {
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
    BatchUpdateLocationHelper(successIds, queryMetas, lockedEntries);
    point.RecordAndReset(PerfKey::WORKER_GET_BATCH_OTHER);
    return lastRc;
}

void WorkerOcServiceGetImpl::BatchUpdateLocationHelper(const std::vector<std::string> &successIds,
                                                       const std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                       std::map<ReadKey, LockedEntity> &entries)
{
    if (!FLAGS_enable_data_replication) {
        return;
    }
    // If the master is disconnected before updating the location, we have to give up retaining the replica, because the
    // master will only manage the replica through the location.
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(successIds);
    std::unordered_set<std::string> needSetDeleteObjectKeys;
    needSetDeleteObjectKeys.reserve(successIds.size());
    etcdCM_->GetObjectKeysFromNotConnectedMaster(objKeysGrpByMaster, needSetDeleteObjectKeys);
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
    std::vector<UpdateLocationParam> asyncUpdateLocationParams;
    if (successIds.size() == queryMetas.size()) {
        for (auto &queryMeta : queryMetas) {
            if (needSetDeleteObjectKeys.find(queryMeta.meta().object_key()) != needSetDeleteObjectKeys.end()) {
                continue;
            }
            asyncUpdateLocationParams.emplace_back(
                UpdateLocationParam{ queryMeta.meta().object_key(), queryMeta.meta().version(),
                                     static_cast<uint32_t>(queryMeta.meta().config().data_format()) });
        }
    } else {
        for (auto &queryMeta : queryMetas) {
            if (needSetDeleteObjectKeys.find(queryMeta.meta().object_key()) != needSetDeleteObjectKeys.end()) {
                continue;
            }
            const auto &meta = queryMeta.meta();
            const auto &objectKey = meta.object_key();
            if (std::find(successIds.begin(), successIds.end(), objectKey) != successIds.end()) {
                asyncUpdateLocationParams.emplace_back(UpdateLocationParam{
                    objectKey, meta.version(), static_cast<uint32_t>(meta.config().data_format()) });
            }
        }
    }
    if (asyncUpdateLocationParams.empty()) {
        return;
    }
    UpdateLocationTask asyncUpdateTask = UpdateLocationTask(asyncUpdateLocationParams);
    asyncUpdateLocationManager_->AddTask(std::move(asyncUpdateTask));
}

void WorkerOcServiceGetImpl::BatchUpdateLocationHelper(const std::vector<std::string> &successIds,
                                                       const QueryMetaMap &queryMetas,
                                                       std::map<ReadKey, LockedEntity> &entries)
{
    if (!FLAGS_enable_data_replication) {
        return;
    }
    // If the master is disconnected before updating the location, we have to give up retaining the replica, because the
    // master will only manage the replica through the location.
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(successIds);
    std::unordered_set<std::string> needSetDeleteObjectKeys;
    needSetDeleteObjectKeys.reserve(successIds.size());
    etcdCM_->GetObjectKeysFromNotConnectedMaster(objKeysGrpByMaster, needSetDeleteObjectKeys);
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
    std::vector<UpdateLocationParam> asyncUpdateLocationParams;

    for (const auto &objectKey : successIds) {
        if (needSetDeleteObjectKeys.find(objectKey) != needSetDeleteObjectKeys.end()) {
            continue;
        }
        const auto &meta = queryMetas.at(objectKey);
        asyncUpdateLocationParams.emplace_back(
            UpdateLocationParam{ meta.meta().object_key(), meta.meta().version(),
                                 static_cast<uint32_t>(meta.meta().config().data_format()) });
    }

    if (asyncUpdateLocationParams.empty()) {
        return;
    }
    UpdateLocationTask asyncUpdateTask = UpdateLocationTask(asyncUpdateLocationParams);
    asyncUpdateLocationManager_->AddTask(std::move(asyncUpdateTask));
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
    std::shared_ptr<std::string> commId = nullptr;
    if (IsRemoteH2DEnabled() && request != nullptr) {
        commId = std::make_shared<std::string>(request->GetClientCommUuid());
    }
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
        }
        if (tryGetFromElsewhere) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_RESP_PT_2);
        }
        HandleBatchSubResponsePart2(subRc, address, metaIter, objectKV, checkConnectStatus, tryGetFromElsewhere,
                                    needEvictObjs);
        if (subRc.IsError() && tryGetFromElsewhere && !address.empty()) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_OTHER_AZ);
            HostPort hostAddr;
            hostAddr.ParseString(address);
            // Note that rc can change upon TryGetObjectFromOtherAZ.
            TryGetObjectFromOtherAZ(*metaIter, hostAddr, objectKV, subRc, true);
        }
        if (subRc.IsError() && tryGetFromElsewhere) {
            point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_L2);
            Timer timer;
            bool ifWorkerConnected = checkConnectStatus.IsOk();
            TryGetFromL2CacheWhenNotFoundInWorker(*metaIter, address, ifWorkerConnected, objectKV, subRc);
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
        point.RecordAndReset(PerfKey::WORKER_HANDLE_BATCH_SUB_FOR_STATUS);
        if (!dataSizeChanged && subRc.IsError()) {
            failedInfos.emplace_back(*iter);
        }
        BatchGetObjectHandleIndividualStatus(subRc, *readKey, successIds, needRetryIds, failedIds);
        if (!dataSizeChanged) {
            iter = infos.erase(iter);
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

Status WorkerOcServiceGetImpl::BatchGetObjectFromRemoteWorker(
    const std::string &address, std::list<GetObjectInfo> &infos, const std::shared_ptr<GetRequest> &request,
    std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
    std::unordered_set<std::string> &failedIds, std::list<GetObjectInfo> &failedMetas)
{
    bool isMigrateData = request == nullptr;
    const int64_t migrateDataTimeoutMs = 200;
    bool dataSizeChange;
    Status lastRc;
    Status checkConnectStatus;
    const int32_t minRetryOnceRpcMs = 1; // The 1st level of retryIntervalsMs
    do {
        dataSizeChange = false;
        BatchGetObjectRemoteReqPb reqPb;
        BatchGetObjectRemoteRspPb rspPb;
        std::vector<RpcMessage> payloads;
        if (!infos.empty()) {
            reqPb.mutable_requests()->Reserve(static_cast<int>(infos.size()));
        }
        auto constructAndSend = [&]() {
            PerfPoint point(PerfKey::WORKER_BATCH_GET_CONSTRUCT_AND_SEND_PRE);
            // If address is empty, we fallback to get non-batched object from L2 Cache.
            CHECK_FAIL_RETURN_STATUS(!address.empty(), K_RUNTIME_ERROR,
                                     FormatString("Fail to get objects from remote worker, no object copy exists."));
            CHECK_FAIL_RETURN_STATUS(address != localAddress_.ToString(), K_RUNTIME_ERROR,
                                     "Remote getting from self address is invalid");
            // If connection status is faulty, we fallback to get individual object from other AZ.
            // Otherwise we get from the local AZ.
            HostPort hostAddr;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hostAddr.ParseString(address),
                                             FormatString("Parse object address %s failed", address));
            checkConnectStatus = etcdCM_->CheckConnection(hostAddr);
            CHECK_FAIL_RETURN_STATUS(checkConnectStatus.IsOk(), K_RUNTIME_ERROR,
                                     FormatString("Fail to get objects from remote worker, no object copy exists."));
            INJECT_POINT("worker.before_GetObjectFromRemoteWorkerAndDump");
            point.RecordAndReset(PerfKey::WORKER_BATCH_GET_CONSTRUCT_GET_REQUEST);
            RETURN_IF_NOT_OK(
                ConstructBatchGetRequest(address, infos, request, successIds, needRetryIds, failedIds, reqPb));
            INJECT_POINT("worker.remote_get_failed");
            point.RecordAndReset(PerfKey::WORKER_BATCH_GET_CREATE_REMOTE_API);
            std::shared_ptr<WorkerRemoteWorkerOCApi> workerStub;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(address, akSkManager_, workerStub),
                                             "Create remote worker api failed.");
            std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> clientApi;
            // If getting data from other AZ, then we leave 3/4 remain time to query from L2 cache in case getting
            // data failed.
            int64_t timeoutMs =
                reqTimeoutDuration.CalcRealRemainingTime() / (etcdCM_->CheckIfOtherAzNodeConnected(hostAddr) ? 4 : 1);
            timeoutMs = !isMigrateData ? timeoutMs : std::min(timeoutMs, migrateDataTimeoutMs);
            point.RecordAndReset(PerfKey::WORKER_BATCH_GET_SEND_AND_RECV);
            RETURN_IF_NOT_OK(RetryOnErrorRepent(
                timeoutMs,
                [&workerStub, &reqPb, &rspPb, &clientApi, &address, &payloads, this](int32_t) {
                    PerfPoint point(PerfKey::WORKER_BATCH_REMOTE_GET_RPC);
                    RETURN_IF_NOT_OK(workerStub->BatchGetObjectRemote(&clientApi));
                    RETURN_IF_NOT_OK(workerStub->BatchGetObjectRemoteWrite(clientApi, reqPb));

                    auto rc = clientApi->Read(rspPb);
                    RETURN_IF_NOT_OK(TryReconnectRemoteWorker(address, rc));
                    // Fallback to downlevel client as multiple objects can be contained in the payload.
                    // Only spill case would actually send payload via RPC, so performance-wise it would be acceptable.
                    RETURN_IF_NOT_OK(clientApi->ReceivePayload(payloads));
                    return Status::OK();
                },
                []() { return Status::OK(); },
                { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
                  StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_URMA_CONNECT_FAILED, StatusCode::K_URMA_WAIT_TIMEOUT },
                minRetryOnceRpcMs));
            return Status::OK();
        };
        PerfPoint point(PerfKey::WORKER_BATCH_GET_CONSTRUCT_AND_SEND);
        Status rc = constructAndSend();
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
