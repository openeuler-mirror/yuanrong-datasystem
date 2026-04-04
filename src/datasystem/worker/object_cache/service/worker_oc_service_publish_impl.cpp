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
 * Description: Defines the worker service processing publish process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_publish_impl.h"

#include <utility>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/l2cache/l2_storage.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rdma/rdma_util.h"

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {
static constexpr int DEBUG_LOG_LEVEL = 2;

WorkerOcServicePublishImpl::WorkerOcServicePublishImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                                       std::shared_ptr<ThreadPool> memCpyThreadPool,
                                                       std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
      etcdCM_(etcdCM),
      memCpyThreadPool_(std::move(memCpyThreadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(localAddress)
{
}

Status WorkerOcServicePublishImpl::VertifyObjectReleaseValidity(const PublishReqPb &req, const SafeObjType &safeObj)
{
    if (safeObj->IsSealed()) {
        if (!req.is_retry()) {
            RETURN_STATUS(StatusCode::K_OC_ALREADY_SEALED, "already be sealed.");
        }
    } else if (safeObj->IsPublished()) {
        if (WriteMode(req.write_mode()) != safeObj->modeInfo.GetWriteMode()
            || ConsistencyType(req.consistency_type()) != safeObj->modeInfo.GetConsistencyType()
            || static_cast<CacheType>(req.cache_type()) != safeObj->modeInfo.GetCacheType()) {
            RETURN_STATUS(StatusCode::K_INVALID, "The write mode, consistency type or cache type cannot be modified.");
        }
    }
    return Status::OK();
}

Status WorkerOcServicePublishImpl::PrepareForPublish(const PublishReqPb &req, const ClientKey &clientId,
                                                     const ShmKey &shmUnitId, ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &safeObj = objectKV.GetObjEntry();
    // Update or create entry state, set object cache invalid here
    if (safeObj.Get() != nullptr) {  // Case 1: Get.
        CHECK_FAIL_RETURN_STATUS(safeObj->IsBinary(), K_INVALID,
                                 "The key you set already exists, which is used by hset.");
        RETURN_IF_NOT_OK(VertifyObjectReleaseValidity(req, safeObj));
        UpdateObjectEntry(ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                          static_cast<CacheType>(req.cache_type()), GetMetadataSize(), safeObj);
    } else {  // Case 2: First Time Publish.
        SetNewObjectEntry(objectKey, ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                          static_cast<CacheType>(req.cache_type()), req.data_size(), GetMetadataSize(), safeObj);
    }

    RETURN_IF_NOT_OK(CheckIfL2CacheNeededAndWritable(supportL2Storage_, WriteMode(req.write_mode())));

    return AttachShmUnitToObject(clientId, objectKey, shmUnitId, req.data_size(), safeObj);
}

Status WorkerOcServicePublishImpl::CreateMetadataToMaster(const ObjectKV &objectKV, const PublishParams &params,
                                                          uint64_t &version)
{
    INJECT_POINT("worker.before_CreateMetadataToMaster");
    const auto &objectKey = objectKV.GetObjKey();
    const SafeObjType &safeObj = objectKV.GetObjEntry();

    master::CreateMetaReqPb metaReq;
    datasystem::ObjectMetaPb *metadata = metaReq.mutable_meta();
    if (params.lifeState != ObjectLifeState::OBJECT_INVALID) {
        *metaReq.mutable_nested_keys() = { params.nestedObjectKeys.begin(), params.nestedObjectKeys.end() };
    }
    metadata->set_object_key(objectKey);
    metadata->set_data_size(safeObj->GetDataSize());
    metadata->set_life_state(static_cast<uint32_t>(params.lifeState));
    metadata->set_ttl_second(params.ttlSecond);
    metadata->set_existence(static_cast<::datasystem::ExistenceOptPb>(params.existence));
    ConfigPb *configPb = metadata->mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>(safeObj->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>(safeObj->stateInfo.GetDataFormat()));
    configPb->set_consistency_type(static_cast<uint32_t>(safeObj->modeInfo.GetConsistencyType()));
    configPb->set_cache_type(static_cast<uint32_t>(params.cacheType));
    metaReq.set_address(localAddress_.ToString());
    metaReq.set_redirect(true);
    master::CreateMetaRspPb metaResp;
    PerfPoint point(PerfKey::WORKER_CREATE_META);

    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(objectKey, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, CreateMetadataToMaster failed");
    std::function<Status(CreateMetaReqPb &, CreateMetaRspPb &)> func = [&workerMasterApi](CreateMetaReqPb &metaReq,
                                                                                          CreateMetaRspPb &metaResp) {
        LOG(INFO) << FormatString("Create meta to master[%s]", workerMasterApi->GetHostPort());
        return workerMasterApi->CreateMeta(metaReq, metaResp);
    };
    RETURN_IF_NOT_OK(RedirectRetryWhenMetaMoving(metaReq, metaResp, workerMasterApi, func));
    point.Record();
    version = metaResp.version();
    return Status::OK();
}

Status WorkerOcServicePublishImpl::UpdateMetadataToMaster(const ObjectKV &objectKV, const PublishParams &params,
                                                          uint64_t &version)
{
    const auto &objectKey = objectKV.GetObjKey();
    const SafeObjType &safeObj = objectKV.GetObjEntry();

    UpdateMetaReqPb metaReq;
    metaReq.set_object_key(objectKey);
    metaReq.set_address(localAddress_.ToString());
    metaReq.set_life_state(static_cast<uint32_t>(params.lifeState));
    metaReq.set_data_size(safeObj->GetDataSize());
    metaReq.set_redirect(true);
    *metaReq.mutable_nested_keys() = { params.nestedObjectKeys.begin(), params.nestedObjectKeys.end() };
    metaReq.set_ttl_second(params.ttlSecond);
    auto binaryFormatParams = metaReq.mutable_binary_format_params();
    binaryFormatParams->set_write_mode((uint32_t)safeObj->modeInfo.GetWriteMode());
    binaryFormatParams->set_data_format((uint32_t)safeObj->stateInfo.GetDataFormat());
    binaryFormatParams->set_consistency_type((uint32_t)safeObj->modeInfo.GetConsistencyType());
    binaryFormatParams->set_cache_type(static_cast<uint32_t>(params.cacheType));

    UpdateMetaRspPb metaRsp;
    VLOG(1) << FormatString("Send Update metadata to master for object: %s, address: %s", objectKey,
                            localAddress_.ToString());
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(objectKey, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, UpdateMetadataToMaster failed");
    std::function<Status(UpdateMetaReqPb &, UpdateMetaRspPb &)> func = [&workerMasterApi](UpdateMetaReqPb &metaReq,
                                                                                          UpdateMetaRspPb &metaRsp) {
        LOG(INFO) << FormatString("Update meta to master[%s]", workerMasterApi->GetHostPort());
        return workerMasterApi->UpdateMeta(metaReq, metaRsp);
    };
    RETURN_IF_NOT_OK(RedirectRetryWhenMetaMoving(metaReq, metaRsp, workerMasterApi, func));
    version = metaRsp.version();
    return Status::OK();
}

Status WorkerOcServicePublishImpl::RequestingToMaster(ObjectKV &objectKV, const PublishParams &params)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &safeObj = objectKV.GetObjEntry();

    if (safeObj->IsSealed() && params.lifeState == ObjectLifeState::OBJECT_SEALED && params.isRetry) {
        safeObj->stateInfo.SetCacheInvalid(false);
    }
    CHECK_FAIL_RETURN_STATUS(!safeObj->IsSealed(), StatusCode::K_OC_ALREADY_SEALED, "already be sealed.");
    INJECT_POINT("worker.publish.before_request_to_master");

    Status rc;
    uint64_t publishVersion = 0;
    if (safeObj->IsInvalid()) {
        rc = CreateMetadataToMaster(objectKV, params, publishVersion);
    } else if (safeObj->IsPublished()) {
        rc = UpdateMetadataToMaster(objectKV, params, publishVersion);
        if (rc.GetCode() == K_NOT_FOUND) {
            // when worker restart and rocksdb failed, no_l2_cache obj cant recover meta, retry create meta.
            rc = CreateMetadataToMaster(objectKV, params, publishVersion);
        }
    }
    if (rc.IsOk()) {
        safeObj->SetCreateTime(publishVersion);
    } else {
        LOG_IF_ERROR(safeObj->FreeResources(), "SafeObj free failed");
        LOG(ERROR) << FormatString("[ObjectKey %s] RequestingToMaster failed, status: %s", objectKey, rc.ToString());
        if (IsRpcTimeout(rc)) {
            return Status(rc.GetCode(), FormatString("Create meta to master failed. detail: %s", rc.ToString()));
        }
        const std::unordered_set<StatusCode> passthroughError{
            StatusCode::K_WORKER_DEADLOCK,   StatusCode::K_KVSTORE_ERROR, StatusCode::K_OC_KEY_ALREADY_EXIST,
            StatusCode::K_OC_ALREADY_SEALED, StatusCode::K_INVALID,       StatusCode::K_TRY_AGAIN
        };
        if (passthroughError.find(rc.GetCode()) == passthroughError.end()) {
            rc = Status(K_RUNTIME_ERROR,
                        FormatString("CreateMeta failed, objectKey: %s, status: %s", objectKey,
                                     rc.GetCode() == StatusCode::K_RUNTIME_ERROR ? rc.GetMsg() : rc.ToString()));
        }
    }
    return rc;
}

Status WorkerOcServicePublishImpl::RollbackPublishFailure(ObjectKV &objectKV, ObjectLifeState oldLifeState,
                                                          ObjectLifeState newLifeState)
{
    const auto &objectKey = objectKV.GetObjKey();

    objectKV.GetObjEntry()->stateInfo.SetCacheInvalid(true);
    if (newLifeState == ObjectLifeState::OBJECT_SEALED) {
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
            workerMasterApiManager_->GetWorkerMasterApi(objectKey, etcdCM_);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "Hash master get failed, RollbackPublish failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApi->RollbackSeal(objectKey, static_cast<uint32_t>(oldLifeState)),
                                         FormatString("RollbackSeal failed."));
    }
    return Status::OK();
}

Status WorkerOcServicePublishImpl::PublishObject(ObjectKV &objectKV, const PublishParams &params,
                                                 std::vector<RpcMessage> &payloads)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &safeObj = objectKV.GetObjEntry();
    auto oldLifeState = safeObj->GetLifeState();
    LOG(INFO) << FormatString("Current life state: %d, next state: %d, ttl second: %u.", (int)oldLifeState,
                              (int)params.lifeState, params.ttlSecond);
    // Step 1: Verify and request to master, object may be expired due to network latency.
    RETURN_IF_NOT_OK(RequestingToMaster(objectKV, params));
    LOG(INFO) << "Request to master success";

    // Step 2: In case of Non-Shm object, save it to memory first. Write to l2cache if in write-through mode.
    if (!payloads.empty()) {
        RETURN_IF_NOT_OK(SaveBinaryObjectToMemory(objectKV, payloads, evictionManager_, memCpyThreadPool_));
    }

    INJECT_POINT("worker.free_object_resource", [&safeObj]() {
        LOG_IF_ERROR(safeObj->FreeResources(), "SafeObj free failed");
        return Status::OK();
    });
    if (safeObj->IsWriteThroughMode() && IsSupportL2Storage(supportL2Storage_)) {
        LOG(INFO) << "Save binary object to l2cache begin";
        Status res = SaveBinaryObjectToPersistence(objectKV);
        if (res.IsError()) {
            LOG(ERROR) << "SaveBinaryObjectToPersistence failed. status:" << res.ToString();
            RETURN_IF_NOT_OK(RollbackPublishFailure(objectKV, oldLifeState, params.lifeState));
            return res;
        }
    }

    // Step 3: Notify GetRequest for subscription purpose.
    safeObj->stateInfo.SetNeedToDelete(false);
    RETURN_IF_NOT_OK(workerRequestManager_.NotifyPendingGetRequest(objectKV));
    safeObj->SetLifeState(params.lifeState);
    safeObj->stateInfo.SetPrimaryCopy(true);
    safeObj->stateInfo.SetCacheInvalid(false);
    safeObj->stateInfo.SetIncompleted(false);
    if (safeObj->IsSpilled()) {
        LOG(INFO) << FormatString("[ObjectKey %s] Spilled to disk, prepare to delete it.", objectKey);
        RETURN_IF_NOT_OK_EXCEPT(DeleteObjectFromDisk(objectKV), StatusCode::K_NOT_FOUND);
        VLOG(DEBUG_LOG_LEVEL) << FormatString("Finishing deleting the object %s from disk.", objectKey);
    }
    evictionManager_->Add(objectKey);
    return Status::OK();
}

Status WorkerOcServicePublishImpl::PublishObjectWithLock(const std::string &objectKey, const PublishReqPb &req,
                                                         const ClientKey &clientId, const ShmKey &shmUnitId,
                                                         const std::vector<std::string> &nestedObjectKeys,
                                                         std::vector<RpcMessage> &payloads, std::future<Status> &future)
{
    INJECT_POINT("worker.PublishObjectWithLock.begin");
    std::shared_ptr<SafeObjType> entry;
    bool isInsert;
    Timer timer;
    Status status = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert, req.existence() == ExistenceOptPb::NX);
    if (req.existence() == ExistenceOptPb::NX && status.GetCode() == K_OC_KEY_ALREADY_EXIST) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(status);
    CHECK_FAIL_RETURN_STATUS(!asyncRollbackManager_->IsObjectsInRollBack({ objectKey }), K_OC_KEY_ALREADY_EXIST,
                             "The object is being rolled back.");
    int64_t elapsed = timer.ElapsedMilliSecond();
    Raii unlock([&entry]() { entry->WUnlock(); });
    ObjectKV objectKV(objectKey, *entry);
    RETURN_IF_NOT_OK(PrepareForPublish(req, clientId, shmUnitId, objectKV));
    LOG(INFO) << FormatString("Client %s is putting the object %s, ReserveGetAndLock elapsed %zu ms.", req.client_id(),
                              objectKey, elapsed);

    // Step 3: Request to master and save data (non-shm copy).
    auto newLifeState = req.is_seal() ? ObjectLifeState::OBJECT_SEALED : ObjectLifeState::OBJECT_PUBLISHED;
    const PublishParams params = { .lifeState = newLifeState,
                                   .nestedObjectKeys = nestedObjectKeys,
                                   .isRetry = req.is_retry(),
                                   .ttlSecond = req.ttl_second(),
                                   .existence = req.existence(),
                                   .cacheType = static_cast<CacheType>(req.cache_type()) };
    auto rc = PublishObject(objectKV, params, payloads);
    INJECT_POINT("publish.sleep");
    if (rc.IsError()) {
        if ((*entry)->GetShmUnit() == nullptr) {
            LOG_IF_ERROR(TryDeleteObjFromEvictionAndSpillFile(objectKV, isInsert), "Try delete obj from evict fail");
        }
        return rc;
    }

    if ((*entry)->IsWriteBackMode() && IsSupportL2Storage(supportL2Storage_)) {
        LOG(INFO) << "Asyn save binary object to l2cache";
        return asyncSendManager_->Add(objectKey, entry, future);
    }
    return Status::OK();
}

Status WorkerOcServicePublishImpl::TryDeleteObjFromEvictionAndSpillFile(ObjectKV &objectKV, bool isInsert)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &safeObj = objectKV.GetObjEntry();
    (void)evictionManager_->Erase(objectKey);
    if (safeObj->IsSpilled()) {
        LOG(INFO) << FormatString("[ObjectKey %s] Spilled to disk, prepare to delete it.", objectKey);
        RETURN_IF_NOT_OK_EXCEPT(DeleteObjectFromDisk(objectKV), StatusCode::K_NOT_FOUND);
    }
    if (isInsert) {
        LOG_IF_ERROR(objectTable_->Erase(objectKey, safeObj),
                     FormatString("Failed to erase object %s from object table", objectKey));
    }
    return Status::OK();
}

Status WorkerOcServicePublishImpl::PublishImpl(const PublishReqPb &req, PublishRspPb &resp,
                                               std::vector<RpcMessage> &payloads)
{
    // Step1: Add namespace for objectKeys.
    LOG(INFO) << FormatString("[ObjectKey %s] is being publishing [Sz: %zu].", req.object_key(), req.data_size());
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    std::vector<ShmKey> shmUnits = std::vector<ShmKey>{ ShmKey::Intern(req.shm_id()) };
    RETURN_IF_NOT_OK(WorkerOcServiceCrudCommonApi::CheckShmUnitByTenantId(tenantId, ClientKey::Intern(req.client_id()),
                                                                          shmUnits, memoryRefTable_));
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_key());
    PerfPoint point(PerfKey::WORKER_SEAL_OBJECT);
    auto nestedObjectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.nested_keys());

    // Step 2: Do create-or-get logic on object table, and get both locks.
    std::future<Status> future;
    auto shmUnitId = ShmKey::Intern(req.shm_id());
    auto clientId = ClientKey::Intern(req.client_id());
    Status rc =
        RetryWhenDeadlock([this, &namespaceUri, &req, &clientId, &shmUnitId, &nestedObjectKeys, &payloads, &future] {
            return PublishObjectWithLock(namespaceUri, req, clientId, shmUnitId, nestedObjectKeys, payloads, future);
        });

    // If worker diable shared-memory transfer but the request still carries shmUnitId, client-to-worker data transfer
    // is through UB; we need clean memoryRefTable to avoid memory leak.
    if (!shmUnitId.Empty() && !(ShmEnable() && ClientShmEnabled(clientId))) {
        memoryRefTable_->RemoveShmUnit(clientId, shmUnitId);
    }

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("[ObjectKey %s] Publish failed.", namespaceUri));
    Status futureRc = Status::OK();
    INJECT_POINT("worker.async_send_wait_future", [&future, &futureRc]() {
        futureRc = future.get();
        LOG(INFO) << "Inject point async_send_wait, futureRc: " << futureRc;
        return Status::OK();
    });
    RETURN_IF_NOT_OK(futureRc);

    // record the published object size.
    (void)resp;
    point.Record();
    if (req.is_seal()) {
        INJECT_POINT("worker.seal_failure");
    } else {
        INJECT_POINT("worker.publish_failure");
    }
    LOG(INFO) << "Put success";
    return Status::OK();
}

Status WorkerOcServicePublishImpl::Publish(const PublishReqPb &req, PublishRspPb &resp,
                                           std::vector<RpcMessage> &payloads)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_PUBLISH);
    Status rc = PublishImpl(req, resp, payloads);
    std::vector<std::string> nestedObjectKeys = { req.nested_keys().begin(), req.nested_keys().end() };
    RequestParam reqParam;
    reqParam.objectKey = req.object_key().substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.nestedKey = objectKeysToString(nestedObjectKeys);
    reqParam.keep = std::to_string(req.keep());
    reqParam.writeMode = std::to_string(req.write_mode());
    reqParam.consistencyType = std::to_string(req.consistency_type());
    reqParam.isSeal = std::to_string(req.is_seal());
    reqParam.isRetry = std::to_string(req.is_retry());
    reqParam.ttlSecond = std::to_string(req.ttl_second());
    reqParam.existence = std::to_string(req.existence());
    reqParam.cacheType = std::to_string(req.cache_type());
    posixPoint.Record(rc.GetCode(), std::to_string(req.data_size()), reqParam, rc.GetMsg());
    workerOperationTimeCost.Append("Total Publish", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of worker Publish %s", workerOperationTimeCost.GetInfo());
    return rc;
}

}  // namespace object_cache
}  // namespace datasystem
