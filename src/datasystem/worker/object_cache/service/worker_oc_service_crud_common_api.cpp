/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the worker service common CRUD function.
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

#include <cstddef>

#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

DS_DECLARE_bool(ipc_through_shared_memory);
DS_DECLARE_uint64(oc_shm_transfer_threshold_kb);
namespace datasystem {
namespace object_cache {

static constexpr int DEBUG_LOG_LEVEL = 2;

AsyncPersistenceDelManager::AsyncPersistenceDelManager(std::shared_ptr<ThreadPool> oldVerDelAsyncPool,
                                                       std::shared_ptr<PersistenceApi> persistenceApi)
    : persistenceApi_(persistenceApi)
{
    oldVerDelAsyncPool->Execute([this]() { ProcessDelPersistenceOldVerSion(); });
}

void AsyncPersistenceDelManager::Add(const std::string &objectKey, uint64_t oldVersionMax)
{
    std::lock_guard<std::mutex> l(mutex_);
    auto res = persistenceDelMap_.try_emplace(objectKey, oldVersionMax);
    if (!res.second) {
        if (oldVersionMax > res.first->second) {
            LOG(INFO) << "update need del key: " << objectKey << ", old version: " << res.first->second
                      << ", new version: " << oldVersionMax;
            res.first->second = oldVersionMax;
        }
    } else {
        LOG(INFO) << "add need del key: " << objectKey << ", version: " << oldVersionMax;
    }
}

AsyncPersistenceDelManager::~AsyncPersistenceDelManager()
{
    exit_ = true;
}

void AsyncPersistenceDelManager::ProcessDelPersistenceOldVerSion()
{
    while (!exit_) {
        const int waitInterval = 100;
        std::unordered_map<std::string, std::uint64_t> needDelKeysWithVersion;
        {
            std::lock_guard<std::mutex> l(mutex_);
            needDelKeysWithVersion = std::move(persistenceDelMap_);
        }
        if (needDelKeysWithVersion.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(waitInterval));
            continue;
        }
        for (const auto &obj : needDelKeysWithVersion) {
            if (exit_) {
                LOG(INFO) << "worker exit, no need to process del old version task";
                break;
            }
            // 1.No matter the upper save success or not, we always trigger old version deletion, the param
            // deleteAllVersion is false ensure that keeping a newest version exists in persistence.
            // 2.In clear object old version scenarios, we don't care about success very much, actually, in most case it
            //   will success, but we also can tolerate failure, so we ignore the call result
            LOG_IF_ERROR(persistenceApi_->Del(obj.first, obj.second, false),
                         FormatString("worker delete object's old version failed, objectKey:%s", obj.first));
        }
    }
}

WorkerOcServiceCrudCommonApi::WorkerOcServiceCrudCommonApi(WorkerOcServiceCrudParam &initParam)
    : workerMasterApiManager_(initParam.workerMasterApiManager),
      workerRequestManager_(initParam.workerRequestManager),
      persistenceApi_(initParam.persistenceApi),
      memoryRefTable_(initParam.memoryRefTable),
      objectTable_(initParam.objectTable),
      evictionManager_(initParam.evictionManager),
      workerDevOcManager_(initParam.workerDevOcManager),
      asyncSendManager_(initParam.asyncSendManager),
      asyncRollbackManager_(initParam.asyncRollbackManager),
      metadataSize_(initParam.metadataSize),
      etcdCM_(initParam.etcdCM),
      asyncPersistenceDelManager_(initParam.asyncPersistenceDelManager)
{
    supportL2Storage_ = GetCurrentStorageType();
}

Status WorkerOcServiceCrudCommonApi::SaveBinaryObjectToPersistence(ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();

    auto buf = std::make_shared<std::stringstream>();
    auto shmUnit = entry->GetShmUnit();
    RETURN_RUNTIME_ERROR_IF_NULL(shmUnit);
    buf->rdbuf()->pubsetbuf(static_cast<char *>(shmUnit->GetPointer()) + entry->GetMetadataSize(),
                            entry->GetDataSize());

    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    PerfPoint point(PerfKey::WORKER_SAVE_L2_CACHE);
    Status res = persistenceApi_->Save(
        objectKey, entry->GetCreateTime(), remainingTime, buf, 0, entry->modeInfo.GetWriteMode());
    point.Record();

    uint64_t oldVersionMax = entry->GetCreateTime() - 1;
    auto traceID = Trace::Instance().GetTraceID();
    RETURN_RUNTIME_ERROR_IF_NULL(asyncPersistenceDelManager_);
    asyncPersistenceDelManager_->Add(objectKey, oldVersionMax);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(res, FormatString("Call save to L2Cache failed. objectKey:%s", objectKey));
    return Status::OK();
}

Status WorkerOcServiceCrudCommonApi::UpdateRequestForSuccess(ReadObjectKV &objectKV,
                                                             const std::shared_ptr<GetRequest> &request)
{
    // trigger local pipeline h2d if data is fetched from other point
    // ignore failure now, this will be treated in fillinresponse
    if (OsXprtPipln::IsPiplnH2DRequest(request->GetH2DChunkManager()))
        OsXprtPipln::MaybeTriggerLocalPipelineRH2D(request->GetH2DChunkManager(), objectKV.GetObjKey(),
            objectKV.GetReadOffset() + objectKV.GetObjEntry()->GetMetadataSize(),
            objectKV.GetReadSize(), objectKV.GetObjEntry()->GetShmUnit());
    const auto dataFormat = objectKV.GetObjEntry()->stateInfo.GetDataFormat();
    if (dataFormat == DataFormat::BINARY) {
        if (request != nullptr) {
            return request->MarkSuccess(objectKV.GetObjKey(), objectKV.GetObjEntry());
        }
        return workerRequestManager_.NotifyPendingGetRequest(objectKV);
    }
    if (dataFormat == DataFormat::HETERO) {
        return workerDevOcManager_->UpdateRequestForSuccess(objectKV);
    }
    RETURN_STATUS(K_INVALID, "The dataformat is neither BINARY nor HETERO");
}

Status WorkerOcServiceCrudCommonApi::DeleteObjectFromDisk(ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(WorkerOcSpill::Instance()->Delete(objectKey),
                                     FormatString("[ObjectKey %s] Delete from disk failed", objectKey));
    objectKV.GetObjEntry()->stateInfo.SetSpillState(false);
    return Status::OK();
}

Status WorkerOcServiceCrudCommonApi::CheckIfL2CacheNeededAndWritable(const L2StorageType &supportType,
                                                                     WriteMode writeMode)
{
    if (supportType == L2StorageType::NONE) {
        bool isSetWriteL2Cache =
            (writeMode == WriteMode::WRITE_THROUGH_L2_CACHE || writeMode == WriteMode::WRITE_BACK_L2_CACHE
             || writeMode == WriteMode::WRITE_BACK_L2_CACHE_EVICT);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!isSetWriteL2Cache, K_INVALID,
                                             "The key is set to WRITE_THROUGH_L2_CACHE, WRITE_BACK_L2_CACHE or "
                                             "WRITE_BACK_L2_CACHE_EVICT, but L2_cache_type is set to none.");
    }
    return Status::OK();
}

bool WorkerOcServiceCrudCommonApi::ClientShmEnabled(const ClientKey &clientId)
{
    auto clientInfo = worker::ClientManager::Instance().GetClientInfo(clientId);
    return clientInfo != nullptr && clientInfo->ShmEnabled();
}

bool WorkerOcServiceCrudCommonApi::CanTransferByShm(uint64_t dataSize)
{
    return FLAGS_ipc_through_shared_memory && dataSize >= FLAGS_oc_shm_transfer_threshold_kb * KB;
}

bool WorkerOcServiceCrudCommonApi::ShmEnable()
{
    return FLAGS_ipc_through_shared_memory;
}

size_t WorkerOcServiceCrudCommonApi::GetMetadataSize() const
{
    return metadataSize_;
}

Status WorkerOcServiceCrudCommonApi::AttachShmUnitToObject(const ClientKey &clientId, const std::string &objectKey,
                                                           const ShmKey &shmUnitId, uint64_t dataSize,
                                                           SafeObjType &entry)
{
    INJECT_POINT("AttachShmUnitToObject.error");
    std::shared_ptr<ShmUnit> shmUnit;
    if (!shmUnitId.Empty()) {
        auto status = memoryRefTable_->GetShmUnit(shmUnitId, shmUnit);
        if (status.IsError()) {
            if (ClientShmEnabled(clientId) && ShmEnable()) {
                return status;
            }
            auto shm = entry->GetShmUnit();
            if (shm != nullptr && shm->GetId() == shmUnitId) {
                shmUnit = shm;
            } else {
                return status;
            }
        }
    } else {
        uint64_t metaDataSz = GetMetadataSize();
        RETURN_IF_NOT_OK(AllocateNewShmUnit(objectKey, dataSize, metaDataSz, false, evictionManager_, shmUnit,
                                            entry->modeInfo.GetCacheType()));
    }
    entry->SetShmUnit(shmUnit);
    entry->SetDataSize(dataSize);
    return Status::OK();
}

Status WorkerOcServiceCrudCommonApi::CheckShmUnitByTenantId(const std::string &tenantId, const ClientKey &clientId,
                                                            std::vector<ShmKey> &shmUnitIds,
                                                            std::shared_ptr<SharedMemoryRefTable> memoryRefTable)
{
    RETURN_OK_IF_TRUE(!ClientShmEnabled(clientId));
    for (const auto &shmUnitId : shmUnitIds) {
        std::shared_ptr<ShmUnit> shmUnit;
        if (!shmUnitId.Empty()) {
            RETURN_IF_NOT_OK(memoryRefTable->GetShmUnit(shmUnitId, shmUnit));
            if (tenantId != shmUnit->GetTenantId()) {
                LOG(ERROR) << FormatString("req tenantId: %s is not equal shmUnit tenantId: %s", tenantId,
                                           shmUnit->GetTenantId());
                RETURN_STATUS(K_NOT_AUTHORIZED, "worker shmunit auth check failed");
            }
        }
    }
    return Status::OK();
}

Status WorkerOcServiceCrudCommonApi::ClearObject(ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    INJECT_POINT("worker.clear_object_failure");
    PerfPoint point(PerfKey::WORKER_CLEAR_OBJECT);
    // The object must be locked by the caller to make this call
    CHECK_FAIL_RETURN_STATUS(entry.IsWLockedByCurrentThread(), K_RUNTIME_ERROR,
                             "Clearing a locked object that was not locked first!");
    uint64_t dataSize = 0;
    if (entry.Get() != nullptr) {
        dataSize = entry->GetDataSize();
        VLOG(1) << FormatString("ClearObject %s, size:%zu.", objectKey, dataSize);
        if (entry->IsSpilled()) {
            VLOG(DEBUG_LOG_LEVEL) << FormatString("Object %s spilled to disk, prepare to delete it.", objectKey);
            RETURN_IF_NOT_OK_APPEND_MSG(DeleteObjectFromDisk(objectKV),
                                        FormatString("Failed delete object %s from disk.", objectKey));
        }
        if (entry->IsWriteBackMode() && IsSupportL2Storage(supportL2Storage_)) {
            asyncSendManager_->Remove(objectKey);
        }
    }
    INJECT_POINT("worker.ClearObject.BeforeErase");
    RETURN_IF_NOT_OK_APPEND_MSG(objectTable_->Erase(objectKey, entry),
                                FormatString("Failed to erase object %s from object table", objectKey));
    evictionManager_->Erase(objectKey);
    return Status::OK();
}

void WorkerOcServiceCrudCommonApi::BatchUnlock(const std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    for (auto &entry : lockedEntries) {
        entry.second->WUnlock();
    }
}

Status WorkerOcServiceCrudCommonApi::BatchLockWithInsert(
    const std::vector<std::string> &objectKeys, std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries,
    std::vector<std::string> &successIds, std::vector<std::string> &failedIds)
{
    Status lastRc;
    lockedEntries.clear();
    std::set<std::string> toLockIds{ objectKeys.begin(), objectKeys.end() };
    for (const auto &objectKey : toLockIds) {
        std::shared_ptr<SafeObjType> entry;
        bool isInsert = false;
        Status s = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert);
        if (s.IsOk()) {
            if (isInsert) {
                SetEmptyObjectEntry(objectKey, *entry);
            }
            (void)lockedEntries.emplace(objectKey, std::move(entry));
            successIds.emplace_back(objectKey);
        } else {
            lastRc = s;
            failedIds.emplace_back(objectKey);
        }
    }
    return lastRc;
}

Status WorkerOcServiceCrudCommonApi::GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr)
{
    std::string dbName;
    RETURN_IF_NOT_OK(etcdCM_->GetPrimaryReplicaLocationByAddr(srcAddr, destAddr, dbName));
    g_MetaRocksDbName = dbName;
    return Status::OK();
}

Status WorkerOcServiceCrudCommonApi::RemoveMeta(const std::list<std::string> &objectKeysRemoveList,
                                                const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
                                                const master::RemoveMetaReqPb::Cause removeCause,
                                                const uint64_t version, bool needRedirct,
                                                const std::string &localAddress,
                                                const std::unordered_map<std::string, uint64_t> &batchKeyVersions,
                                                master::RemoveMetaRspPb &response)
{
    master::RemoveMetaReqPb request;
    request.set_address(localAddress);
    request.set_cause(removeCause);
    request.set_version(version);
    request.set_redirect(needRedirct);
    *request.mutable_ids() = { objectKeysRemoveList.begin(), objectKeysRemoveList.end() };
    if (!batchKeyVersions.empty()) {
        for (const auto &objKeyVersion : batchKeyVersions) {
            auto *objKeyVersionPb = request.add_id_with_version();
            objKeyVersionPb->set_id(objKeyVersion.first);
            objKeyVersionPb->set_version(objKeyVersion.second);
        }
    }
    std::function<Status(master::RemoveMetaReqPb &, master::RemoveMetaRspPb &)> func =
        [workerMasterApi](master::RemoveMetaReqPb &req, master::RemoveMetaRspPb &rsp) {
            return workerMasterApi->RemoveMeta(req, rsp);
        };
    return WorkerOcServiceCrudCommonApi::RedirectRetryWhenMetasMoving(request, response, func);
}

Status WorkerOcServiceCrudCommonApi::RemoveMetadataFromRedirectMaster(
    master::RemoveMetaRspPb &rsp, const master::RemoveMetaReqPb::Cause removeCause, const std::string &localAddress,
    const std::unordered_map<std::string, uint64_t> &batchKeyVersions, std::vector<std::string> &failedIds,
    std::vector<std::string> &needMigrateIds, std::vector<std::string> &needWaitIds)
{
    for (const auto &redirectInfo : rsp.info()) {
        master::RemoveMetaReqPb redirectReq;
        master::RemoveMetaRspPb redirectRsp;
        std::list<std::string> redirectIds = { redirectInfo.change_meta_ids().begin(),
                                               redirectInfo.change_meta_ids().end() };
        HostPort redirectMasterAddr;
        RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr));
        auto status = etcdCM_->CheckConnection(redirectMasterAddr);
        if (status.IsError()) {
            LOG(WARNING) << "remove meta failed: " << status.ToString();
            failedIds.insert(failedIds.end(), redirectIds.begin(), redirectIds.end());
            continue;
        }
        std::shared_ptr<worker::WorkerMasterOCApi> redirectWorkerMasterApi =
            workerMasterApiManager_->GetWorkerMasterApi(redirectMasterAddr);
        if (redirectWorkerMasterApi == nullptr) {
            failedIds.insert(failedIds.end(), redirectIds.begin(), redirectIds.end());
            LOG(ERROR) << "failed to get redirectWorkerMasterApi, masterAddr: " << redirectInfo.redirect_meta_address();
            continue;
        }
        Status result = RemoveMeta(redirectIds, redirectWorkerMasterApi, removeCause, UINT64_MAX, false, localAddress,
                                   batchKeyVersions, redirectRsp);
        // save the result to rsp and payload
        if (result.IsError()) {
            LOG(WARNING) << "remove meta failed: " << result.ToString();
            failedIds.insert(failedIds.end(), redirectIds.begin(), redirectIds.end());
        } else {
            failedIds.insert(failedIds.end(), redirectRsp.failed_ids().begin(), redirectRsp.failed_ids().end());
            needMigrateIds.insert(needMigrateIds.end(), redirectRsp.need_data_ids().begin(),
                                  redirectRsp.need_data_ids().end());
            needWaitIds.insert(needWaitIds.end(), redirectRsp.need_wait_ids().begin(),
                               redirectRsp.need_wait_ids().end());
        }
    }
    return Status::OK();
}

void WorkerOcServiceCrudCommonApi::BatchRemoveMeta(const std::vector<std::string> &objectKeys,
    const std::shared_ptr<worker::WorkerMasterOCApi> &workerMasterApi,
    const master::RemoveMetaReqPb::Cause removeCause, const std::string &localAddress,
    const std::unordered_map<std::string, uint64_t> &batchKeyVersions,
    std::vector<std::string> &failedIds, std::vector<std::string> &needMigrateIds,
    std::vector<std::string> &needWaitIds)
{
    std::list<std::string> objectKeysRemoveList;
    const uint32_t objBatch = 300;
    uint32_t count = 0;
    uint64_t version = UINT64_MAX;
    for (auto &objectKey : objectKeys) {
        objectKeysRemoveList.emplace_back(objectKey);
        ++count;
        if (count >= objBatch) {
            // dest node failed or local node failed, stop remove.
            if (etcdCM_->CheckVoluntaryScaleDown()) {
                break;
            }
            auto status = etcdCM_->CheckConnection(objectKey);
            if (status.IsError()) {
                LOG(WARNING) << "remove meta failed: " << status.ToString();
                failedIds.insert(failedIds.end(), objectKeysRemoveList.begin(), objectKeysRemoveList.end());
                continue;
            }
            if (batchKeyVersions.empty()) {
                reqTimeoutDuration.Init(RPC_TIMEOUT);
            }
            master::RemoveMetaRspPb response;
            auto result = RemoveMeta(objectKeysRemoveList, workerMasterApi, removeCause, version, true,
                localAddress, batchKeyVersions, response);
            if (result.IsError()) {
                LOG(WARNING) << "remove meta failed: " << result.ToString();
                failedIds.insert(failedIds.end(), objectKeysRemoveList.begin(), objectKeysRemoveList.end());
            } else {
                failedIds.insert(failedIds.end(), response.failed_ids().begin(), response.failed_ids().end());
                needMigrateIds.insert(needMigrateIds.end(), response.need_data_ids().begin(),
                                      response.need_data_ids().end());
                needWaitIds.insert(needWaitIds.end(), response.need_wait_ids().begin(), response.need_wait_ids().end());
            }
            RemoveMetadataFromRedirectMaster(response, removeCause, localAddress, batchKeyVersions,
                                            failedIds, needMigrateIds, needWaitIds);
            objectKeysRemoveList.clear();
            count = 0;
        }
    }
    if (count > 0) {
        if (batchKeyVersions.empty()) {
            reqTimeoutDuration.Init(RPC_TIMEOUT);
        }
        master::RemoveMetaRspPb response;
        Status result = RemoveMeta(objectKeysRemoveList, workerMasterApi, removeCause, version, true,
                                  localAddress, batchKeyVersions, response);
        if (result.IsError()) {
            LOG(WARNING) << "remove meta failed: " << result.ToString();
            failedIds.insert(failedIds.end(), objectKeysRemoveList.begin(), objectKeysRemoveList.end());
        } else {
            failedIds.insert(failedIds.end(), response.failed_ids().begin(), response.failed_ids().end());
            needMigrateIds.insert(needMigrateIds.end(), response.need_data_ids().begin(),
                                  response.need_data_ids().end());
            needWaitIds.insert(needWaitIds.end(), response.need_wait_ids().begin(), response.need_wait_ids().end());
        }
        RemoveMetadataFromRedirectMaster(response, removeCause, localAddress, batchKeyVersions,
                                        failedIds, needMigrateIds, needWaitIds);
    }
}

void WorkerOcServiceCrudCommonApi::GroupAndRemoveMeta(const std::vector<std::string> &objKeys,
                                                      const master::RemoveMetaReqPb::Cause &removeCase,
                                                      const std::string &localAddress,
                                                      const std::unordered_map<std::string, uint64_t> &objKeyVersions,
                                                      std::vector<std::string> &failedIds,
                                                      std::vector<std::string> &needMigrateIds,
                                                      std::vector<std::string> &needWaitIds)
{
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objKeys);
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> currentObjectKeysRemove = item.second;
        std::shared_ptr<worker::WorkerMasterOCApi> workerMasterApi =
            workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            failedIds.insert(failedIds.end(), currentObjectKeysRemove.begin(), currentObjectKeysRemove.end());
            LOG(WARNING) << "master address is empty, objectKeys don't belong to any master,"
                         << "remove meta failed, failed ids size is:" << currentObjectKeysRemove.size();
            continue;
        }
        std::unordered_map<std::string, uint64_t> batchKeyVersions;
        for (const auto &objKey : currentObjectKeysRemove) {
            auto it = objKeyVersions.find(objKey);
            if (it != objKeyVersions.end()) {
                batchKeyVersions[objKey] = it->second;
            }
        }
        LOG(INFO) << "remove meta req send to master: " << masterAddr.ToString() << ", removeCase: " << removeCase;
        BatchRemoveMeta(currentObjectKeysRemove, workerMasterApi, removeCase, localAddress, batchKeyVersions, failedIds,
                        needMigrateIds, needWaitIds);
    }
}
}  // namespace object_cache
}  // namespace datasystem
