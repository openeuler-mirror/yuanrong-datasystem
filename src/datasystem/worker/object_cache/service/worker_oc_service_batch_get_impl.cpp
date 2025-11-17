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
#include <utility>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_int64(batch_get_threshold_mb, 100, "The payload threshold to batch get objects");
DS_DEFINE_validator(batch_get_threshold_mb, &Validator::ValidateBatchGetThreshold);
DS_DECLARE_bool(enable_urma);

using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

const size_t WORKER_BATCH_THREAD_NUM = 8;

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
        shmUnit->id = GetStringUuid();
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
    LOG(WARNING) << "Get object from remote failed, start to remove location from master";
    (void)RemoveLocation(objectKey, version);
    if (entry->Get() != nullptr && entry->Get()->GetShmUnit() != nullptr) {
        entry->Get()->GetShmUnit()->SetHardFreeMemory();
    }
    if (isInsert) {
        (void)objectTable_->Erase(objectKey, *entry);
    } else if (entry->Get() != nullptr) {
        entry->Get()->FreeResources();
        entry->Get()->SetLifeState(ObjectLifeState::OBJECT_INVALID);
        entry->Get()->stateInfo.SetCacheInvalid(true);
    }
}

Status WorkerOcServiceGetImpl::GetObjectsFromAnywhereBatched(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                             const std::shared_ptr<GetRequest> &request,
                                                             std::vector<RpcMessage> &payloads,
                                                             std::map<ReadKey, LockedEntity> &lockedEntries,
                                                             std::unordered_set<std::string> &failedIds,
                                                             std::set<ReadKey> &needRetryIds)
{
    Status lastRc = Status::OK();
    std::vector<std::string> successIds;
    successIds.reserve(queryMetas.size());
    // First group queried meta by address, take out the ones that are problematic.
    // And also take out the ones that got their payload from QueryMeta already.
    std::unordered_map<std::string, std::list<std::pair<std::list<ObjectMetaPb *>, uint64_t>>> groupedQueryMetas;
    std::vector<QueryMetaInfoPb> payloadIndexMetas;
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
            GroupQueryMeta(queryMeta, groupedQueryMetas);
            SetObjectEntryAccordingToMeta(meta, GetMetadataSize(), *safeObj);
        }
    }

    // For the ones that already got their payload from queried meta, fallback to existing logic.
    lastRc =
        GetObjectsFromAnywhereSerially(payloadIndexMetas, request, payloads, lockedEntries, failedIds, needRetryIds);

    // And then deal with the requests that can be batched.
    std::vector<std::future<Status>> futures;
    std::list<ObjectMetaPb *> failedMetas;
    std::vector<std::list<ObjectMetaPb *>> tempFailedMetas(groupedQueryMetas.size());
    std::vector<std::vector<std::string>> tempSuccessIds(groupedQueryMetas.size());
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(groupedQueryMetas.size());
    std::vector<std::unordered_set<std::string>> tempFailedIds(groupedQueryMetas.size());
    int index = 0;
    auto workerBatchThreadPool_ = std::make_shared<ThreadPool>(1, WORKER_BATCH_THREAD_NUM, "OcWorkerBatch");
    auto traceId = Trace::Instance().GetTraceID();

    for (auto queryMeta = groupedQueryMetas.begin(); queryMeta != groupedQueryMetas.end(); ++queryMeta, ++index) {
        auto &address = queryMeta->first;
        auto &metaList = queryMeta->second;
        futures.emplace_back(workerBatchThreadPool_->Submit([this, &lastRc, address, &metaList, &request,
                                                             &lockedEntries, &tempSuccessIds, &tempNeedRetryIds,
                                                             &tempFailedIds, &tempFailedMetas, index, traceId] {
            for (auto &metaPair : metaList) {
                auto &metas = metaPair.first;
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                lastRc = BatchGetObjectFromRemoteOnLock(address, metas, request, lockedEntries, tempSuccessIds[index],
                                                        tempNeedRetryIds[index], tempFailedIds[index],
                                                        tempFailedMetas[index]);
            }
            return lastRc;
        }));
    }
    for (auto &fut : futures) {
        if (!fut.get().IsOk()) {
            LOG(ERROR) << "BatchGetObjectFromRemoteOnLock failed";
        }
    }

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
    auto metaIter = failedMetas.begin();
    while (metaIter != failedMetas.end()) {
        auto &objectKey = (*metaIter)->object_key();
        auto &pair = lockedEntries.at(ReadKey(objectKey));
        auto &entry = pair.safeObj;
        bool isInsert = pair.insert;
        HandleGetFailureHelper(objectKey, (*metaIter)->version(), entry, isInsert);
        metaIter++;
    }
    if (successIds.size() != (queryMetas.size() - payloadIndexMetas.size())) {
        LOG(ERROR) << "Failed to get object data from remote. " << successIds.size() << " objects pulled success: ["
                   << VectorToString(successIds) << "], meta data num: " << queryMetas.size()
                   << " lastRc: " << lastRc.ToString();
    }
    return lastRc;
}

void WorkerOcServiceGetImpl::GroupQueryMeta(
    master::QueryMetaInfoPb &queryMeta,
    std::unordered_map<std::string, std::list<std::pair<std::list<ObjectMetaPb *>, uint64_t>>> &groupedQueryMetas)
{
    const static uint64_t maxPayloadSize = FLAGS_batch_get_threshold_mb * 1024 * 1024;
    const auto &meta = queryMeta.meta();
    auto &splitList = groupedQueryMetas[queryMeta.address()];
    if (!(FLAGS_enable_urma) && (FLAGS_batch_get_threshold_mb != 0)) {
        auto payloadSize = meta.data_size() < UINT64_MAX - splitList.back().second
                               ? splitList.back().second + meta.data_size()
                               : UINT64_MAX;
        if (splitList.empty() || payloadSize > maxPayloadSize) {
            splitList.emplace_back(std::make_pair(std::list<ObjectMetaPb *>{}, 0));
        }
    } else {
        if (splitList.empty()) {
            splitList.emplace_back(std::make_pair(std::list<ObjectMetaPb *>{}, 0));
        }
    }
    splitList.back().first.emplace_back(queryMeta.mutable_meta());
    splitList.back().second += meta.data_size();
}

void WorkerOcServiceGetImpl::BatchGetObjectHandleIndividualStatus(Status &status, const ReadKey &readKey,
                                                                  std::vector<std::string> &successIds,
                                                                  std::vector<ReadKey> &needRetryIds,
                                                                  std::unordered_set<std::string> &failedIds)
{
    if (status.IsOk()) {
        successIds.emplace_back(readKey.objectKey);
    } else if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
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
        failedIds.emplace(readKey.objectKey);
    }
}

Status WorkerOcServiceGetImpl::HandleBatchSubResponse(const GetObjectRemoteRspPb &subResp, ObjectMetaPb *meta,
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
    } else {
        if (subRc.IsOk() && !subResp.data_in_payload()) {
            // At this point, we haven't materialized the payload which is still sitting in the tcp/ip buffers.
            // We either receive payload directly into shared memory or fall back to the old behavior to save
            // the payload in ZMQ private memory
            subRc = BatchGetRetrieveRemotePayload(subResp.data_size(), objectKV, payloads, payloadIndex);
        }
    }
    return subRc;
}

void WorkerOcServiceGetImpl::HandleBatchSubResponsePart2(Status &subRc, const std::string &address, ObjectMetaPb *meta,
                                                         ReadObjectKV &objectKV, const Status &checkConnectStatus,
                                                         bool &tryGetFromElsewhere)
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
        evictionManager_->Add(objectKey);
        entry->stateInfo.SetNeedToDelete(true);
        ConsistencyType type = ConsistencyType(meta->config().consistency_type());
        subRc = ProcessObjectEntryAndSyncMetadata(IsUpdateLocation(type), objectKV);
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
    const std::string &address, Status &checkConnectStatus, std::list<ObjectMetaPb *> &metas,
    const std::shared_ptr<GetRequest> &request, std::map<ReadKey, LockedEntity> &lockedEntries, const Status &status,
    BatchGetObjectRemoteRspPb &rspPb, std::vector<RpcMessage> &payloads, std::vector<std::string> &successIds,
    std::vector<ReadKey> &needRetryIds, std::unordered_set<std::string> &failedIds,
    std::list<ObjectMetaPb *> &failedMetas, bool &dataSizeChange)
{
    Status lastRc = status;
    uint64_t payloadIndex = 0;
    auto metaIter = metas.begin();
    for (int i = 0; metaIter != metas.end(); i++) {
        auto &objectKey = (*metaIter)->object_key();
        auto iter = lockedEntries.find(ReadKey(objectKey));
        if (iter == lockedEntries.cend()) {
            continue;
        }
        auto &entry = iter->second.safeObj;
        const auto &readKey = iter->first;
        ReadObjectKV objectKV(readKey, *entry);
        Status subRc = status;
        bool tryGetFromElsewhere = true;
        bool dataSizeChanged = false;
        if (subRc.IsOk()) {
            PerfPoint point(PerfKey::WORKER_HANDLE_BATCH_SUB_RESP);
            auto &subResp = rspPb.responses(i);
            subRc = HandleBatchSubResponse(subResp, *metaIter, objectKV, payloads, payloadIndex, tryGetFromElsewhere,
                                           dataSizeChanged);
            point.Record();
        }
        if (tryGetFromElsewhere) {
            PerfPoint point1(PerfKey::WORKER_HANDLE_BATCH_SUB_RESP_PT_2);
            HandleBatchSubResponsePart2(subRc, address, *metaIter, objectKV, checkConnectStatus, tryGetFromElsewhere);
            point1.Record();
        }
        if (subRc.IsError() && tryGetFromElsewhere) {
            HostPort hostAddr;
            hostAddr.ParseString(address);
            // Note that rc can change upon TryGetObjectFromOtherAZ.
            TryGetObjectFromOtherAZ(**metaIter, hostAddr, objectKV, subRc);
        }
        if (subRc.IsError() && tryGetFromElsewhere) {
            Timer timer;
            bool ifWorkerConnected = checkConnectStatus.IsOk();
            TryGetFromL2CacheWhenNotFoundInWorker(**metaIter, address, ifWorkerConnected, objectKV, subRc);
            LOG(INFO) << "Query from L2 cache use " << timer.ElapsedMilliSecond()
                      << " millisecond, address: " << address << ", ifWorkerConnected: " << ifWorkerConnected;
            CheckAndReturnPullNotFoundForRetry(**metaIter, address, *entry, checkConnectStatus, subRc);
        }
        if (subRc.IsOk() && entry->Get() == nullptr) {
            subRc = Status(K_NOT_FOUND, FormatString("Get from remote worker failed, object(%s) not exist in "
                                                     "worker, maybe the object has been deleted.",
                                                     objectKey));
        }
        if (subRc.IsOk()) {
            subRc = UpdateRequestForSuccess(objectKV, request);
        }
        if (!dataSizeChanged) {
            if (subRc.IsError()) {
                failedMetas.emplace_back(*metaIter);
            }
            metaIter = metas.erase(metaIter);
        } else {
            dataSizeChange = true;
            metaIter++;
        }
        BatchGetObjectHandleIndividualStatus(subRc, readKey, successIds, needRetryIds, failedIds);
        lastRc = subRc;
    }
    return lastRc;
}

Status WorkerOcServiceGetImpl::BatchGetObjectFromRemoteWorker(
    const std::string &address, std::list<ObjectMetaPb *> &metas, const std::shared_ptr<GetRequest> &request,
    std::map<ReadKey, LockedEntity> &lockedEntries, std::vector<std::string> &successIds,
    std::vector<ReadKey> &needRetryIds, std::unordered_set<std::string> &failedIds,
    std::list<ObjectMetaPb *> &failedMetas)
{
    bool dataSizeChange;
    Status lastRc;
    Status checkConnectStatus;
    do {
        dataSizeChange = false;
        BatchGetObjectRemoteReqPb reqPb;
        BatchGetObjectRemoteRspPb rspPb;
        std::vector<RpcMessage> payloads;
        auto constructAndSend = [&]() {
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
            RETURN_IF_NOT_OK(
                ConstructBatchGetRequest(address, metas, lockedEntries, successIds, needRetryIds, failedIds, reqPb));
            INJECT_POINT("worker.remote_get_failed");
            std::shared_ptr<WorkerRemoteWorkerOCApi> workerStub;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(address, akSkManager_, workerStub),
                                             "Create remote worker api failed.");
            std::unique_ptr<ClientUnaryWriterReader<BatchGetObjectRemoteReqPb, BatchGetObjectRemoteRspPb>> clientApi;
            // If getting data from other AZ, then we leave 3/4 remain time to query from L2 cache in case getting
            // data failed.
            int64_t timeoutMs =
                reqTimeoutDuration.CalcRealRemainingTime() / (etcdCM_->CheckIfOtherAzNodeConnected(hostAddr) ? 4 : 1);
            RETURN_IF_NOT_OK(RetryOnErrorRepent(
                timeoutMs,
                [&workerStub, &reqPb, &rspPb, &clientApi, &payloads](int32_t) {
                    RETURN_IF_NOT_OK(workerStub->BatchGetObjectRemote(&clientApi));
                    RETURN_IF_NOT_OK(workerStub->BatchGetObjectRemoteWrite(clientApi, reqPb));
                    RETURN_IF_NOT_OK(clientApi->Read(rspPb));
                    // Fallback to downlevel client as multiple objects can be contained in the payload.
                    // Only spill case would actually send payload via RPC, so performance-wise it would be acceptable.
                    RETURN_IF_NOT_OK(clientApi->ReceivePayload(payloads));
                    return Status::OK();
                },
                []() { return Status::OK(); },
                { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
                  StatusCode::K_RPC_UNAVAILABLE }));
            return Status::OK();
        };
        PerfPoint point(PerfKey::WORKER_CONSTRUCT_AND_SEND);
        Status rc = constructAndSend();
        point.Record();
        lastRc = ProcessBatchResponse(address, checkConnectStatus, metas, request, lockedEntries, rc, rspPb, payloads,
                                      successIds, needRetryIds, failedIds, failedMetas, dataSizeChange);
    } while (dataSizeChange);
    return lastRc;
}

Status WorkerOcServiceGetImpl::BatchGetObjectFromRemoteOnLock(
    const std::string &address, std::list<ObjectMetaPb *> &metas, const std::shared_ptr<GetRequest> &request,
    std::map<ReadKey, LockedEntity> &lockedEntries, std::vector<std::string> &successIds,
    std::vector<ReadKey> &needRetryIds, std::unordered_set<std::string> &failedIds,
    std::list<ObjectMetaPb *> &failedMetas)
{
    PerfPoint point(PerfKey::WORKER_PULL_REMOTE_DATA);
    // Unlock entries at exit.
    Raii raii([this, &metas, &lockedEntries]() {
        for (auto &meta : metas) {
            const auto &objectKey = meta->object_key();
            RemoveInRemoteGetObject(objectKey);
            lockedEntries.at(ReadKey(objectKey)).safeObj->WUnlock();
        }
    });
    // Construct and send request for batch remote get.
    Timer endToEndTimer;
    Status rc = BatchGetObjectFromRemoteWorker(address, metas, request, lockedEntries, successIds, needRetryIds,
                                               failedIds, failedMetas);
    LOG(INFO) << FormatString("object get from remote finish, use %f millisecond.", endToEndTimer.ElapsedMilliSecond());
    return rc;
}

Status WorkerOcServiceGetImpl::AggregateAllocateHelper(const std::list<ObjectMetaPb *> &metas,
                                                       std::map<ReadKey, LockedEntity> &lockedEntries,
                                                       std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                                       std::vector<uint32_t> &shmIndexMapping)
{
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> traversalHelper =
        [&metas, &lockedEntries](std::function<void(uint64_t, uint64_t, uint32_t)> collector, bool &needAggregate) {
            needAggregate = metas.size() > 1;
            uint32_t objectIndex = 0;
            for (auto metaIter = metas.begin(); metaIter != metas.end() && needAggregate; metaIter++, objectIndex++) {
                auto &meta = *metaIter;
                auto dataSz = meta->data_size();

                const auto &objectKey = meta->object_key();
                auto &lockedEntity = lockedEntries.at(ReadKey(objectKey));
                auto &entry = *(lockedEntity.safeObj);
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
    auto firstObjectKey = metas.front()->object_key();
    RETURN_IF_NOT_OK(AggregateAllocate(firstObjectKey, traversalHelper, evictionManager_, shmOwners, shmIndexMapping));
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
