/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Defines the worker service processing publish process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_publish_impl.h"

#include <algorithm>
#include <utility>


#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/l2cache/l2_storage.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/request_context.h"
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
static constexpr double US_PER_MS = 1000.0;
static constexpr int64_t META_ROUTE_ATTEMPT_TIMEOUT_MS = 2 * 1000;
static constexpr int64_t META_ROUTE_RETRY_INTERVAL_MS = 100;

namespace {
bool IsMetadataRouteRetryable(const Status &rc)
{
    static const std::unordered_set<StatusCode> retryableCodes{
        K_NOT_READY, K_RPC_CANCELLED, K_RPC_DEADLINE_EXCEEDED, K_RPC_UNAVAILABLE, K_SCALING, K_TRY_AGAIN
    };
    return retryableCodes.find(rc.GetCode()) != retryableCodes.end();
}

template <typename Resolver, typename Requester, typename Sleeper>
Status RetryMetadataRequestWithRouteRefresh(std::shared_ptr<WorkerMasterOCApi> &workerMasterApi,
                                            bool retryRequestFailure, Resolver &&resolver, Requester &&requester,
                                            Sleeper &&sleeper)
{
    Status rc;
    bool shouldRetry = true;
    while (shouldRetry) {
        workerMasterApi.reset();
        bool requestAttempted = false;
        rc = resolver(workerMasterApi);
        if (rc.IsOk()) {
            CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "Resolve metadata owner failed");
            auto &requestTimeout = GetRequestContext()->reqTimeoutDuration;
            auto savedTimeout = requestTimeout;
            Raii restoreTimeout([&requestTimeout, savedTimeout]() { requestTimeout = savedTimeout; });
            requestTimeout.Init(std::min<int64_t>(requestTimeout.CalcRealRemainingTime(),
                                                  META_ROUTE_ATTEMPT_TIMEOUT_MS));
            requestAttempted = true;
            rc = requester(workerMasterApi);
        }
        shouldRetry = IsMetadataRouteRetryable(rc) && (!requestAttempted || retryRequestFailure);
        if (shouldRetry) {
            int64_t remainingMs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
            shouldRetry = remainingMs > META_ROUTE_RETRY_INTERVAL_MS;
            if (shouldRetry) {
                sleeper(std::min<int64_t>(remainingMs, META_ROUTE_RETRY_INTERVAL_MS));
            }
        }
    }
    return rc;
}
}  // namespace

WorkerOcServicePublishImpl::WorkerOcServicePublishImpl(WorkerOcServiceCrudParam &initParam,
                                                       std::shared_ptr<ThreadPool> memCpyThreadPool,
                                                       std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
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

void WorkerOcServicePublishImpl::ConstructCreateMetaRequest(const ObjectKV &objectKV, const PublishParams &params,
                                                            CreateMetaReqPb &metaReq) const
{
    const SafeObjType &safeObj = objectKV.GetObjEntry();
    ObjectMetaPb *metadata = metaReq.mutable_meta();
    if (params.lifeState != ObjectLifeState::OBJECT_INVALID) {
        *metaReq.mutable_nested_keys() = { params.nestedObjectKeys.begin(), params.nestedObjectKeys.end() };
    }
    metadata->set_object_key(objectKV.GetObjKey());
    metadata->set_data_size(safeObj->GetDataSize());
    metadata->set_life_state(static_cast<uint32_t>(params.lifeState));
    metadata->set_ttl_second(params.ttlSecond);
    metadata->set_existence(static_cast<ExistenceOptPb>(params.existence));
    ConfigPb *configPb = metadata->mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>(safeObj->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>(safeObj->stateInfo.GetDataFormat()));
    configPb->set_consistency_type(static_cast<uint32_t>(safeObj->modeInfo.GetConsistencyType()));
    configPb->set_cache_type(static_cast<uint32_t>(params.cacheType));
    metaReq.set_address(localAddress_.ToString());
    metaReq.set_redirect(true);
}

Status WorkerOcServicePublishImpl::CreateMetadataToMaster(const ObjectKV &objectKV, const PublishParams &params,
                                                          uint64_t &version)
{
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    INJECT_POINT("worker.before_CreateMetadataToMaster");
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_CREATE_META_RPC_START);
    }
    const auto &objectKey = objectKV.GetObjKey();

    master::CreateMetaReqPb metaReq;
    ConstructCreateMetaRequest(objectKV, params, metaReq);
    master::CreateMetaRspPb metaResp;
    PerfPoint point(PerfKey::WORKER_CREATE_META);

    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    Timer rpcTimer;
    const bool retryRequestFailure = params.lifeState != ObjectLifeState::OBJECT_SEALED;
    Status rc = RetryMetadataRequestWithRouteRefresh(
        workerMasterApi, retryRequestFailure,
        [this, &objectKey](std::shared_ptr<WorkerMasterOCApi> &api) {
            return workerMasterApiManager_->GetWorkerMasterApi(objectKey, api);
        },
        [this, &metaReq, &metaResp](std::shared_ptr<WorkerMasterOCApi> &api) {
            metaResp.Clear();
            std::function<Status(CreateMetaReqPb &, CreateMetaRspPb &)> func =
                [&api](CreateMetaReqPb &req, CreateMetaRspPb &rsp) {
                    VLOG(1) << AppendSrcDstForLog(FormatString("Create meta to master[%s]", api->GetHostPort()),
                                                  req.address(), api->GetHostPort());
                    return api->CreateMeta(req, rsp);
                };
            return RedirectRetryWhenMetaMoving(metaReq, metaResp, api, func);
        },
        [this](int64_t sleepTimeMs) { SleepForMetaMovingRetry(sleepTimeMs); });
    const auto rpcUs = static_cast<uint64_t>(rpcTimer.ElapsedMicroSecond());
    FinalizeMasterRpcLatency(LatencyTickKey::WORKER_CREATE_META_RPC_END, config, traceEnabled,
                             metaResp, objectKey, rpcUs, rc, workerMasterApi, "CreateMeta");
    RETURN_IF_NOT_OK(rc);
    point.Record();
    version = metaResp.version();
    return Status::OK();
}

Status WorkerOcServicePublishImpl::UpdateMetadataToMaster(const ObjectKV &objectKV, const PublishParams &params,
                                                          uint64_t &version)
{
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_UPDATE_META_RPC_START);
    }
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
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    Timer rpcTimer;
    const bool retryRequestFailure = params.lifeState != ObjectLifeState::OBJECT_SEALED;
    Status rc = RetryMetadataRequestWithRouteRefresh(
        workerMasterApi, retryRequestFailure,
        [this, &objectKey](std::shared_ptr<WorkerMasterOCApi> &api) {
            return workerMasterApiManager_->GetWorkerMasterApi(objectKey, api);
        },
        [this, &metaReq, &metaRsp](std::shared_ptr<WorkerMasterOCApi> &api) {
            metaRsp.Clear();
            std::function<Status(UpdateMetaReqPb &, UpdateMetaRspPb &)> func =
                [&api](UpdateMetaReqPb &req, UpdateMetaRspPb &rsp) {
                    VLOG(1) << AppendSrcDstForLog(FormatString("Update meta to master[%s]", api->GetHostPort()),
                                                  req.address(), api->GetHostPort());
                    return api->UpdateMeta(req, rsp);
                };
            return RedirectRetryWhenMetaMoving(metaReq, metaRsp, api, func);
        },
        [this](int64_t sleepTimeMs) { SleepForMetaMovingRetry(sleepTimeMs); });
    const auto rpcUs = static_cast<uint64_t>(rpcTimer.ElapsedMicroSecond());
    FinalizeMasterRpcLatency(LatencyTickKey::WORKER_UPDATE_META_RPC_END, config, traceEnabled,
                             metaRsp, objectKey, rpcUs, rc, workerMasterApi, "UpdateMeta");
    RETURN_IF_NOT_OK(rc);
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

    // Deadline gate: if the client's request budget is already exhausted, do not
    // fire the master CreateMeta/UpdateMeta RPC — its result would be an orphan
    // (the client already gave up on the Publish). Skipping avoids burning a
    // master round-trip and avoids half-created metadata that
    // RollbackPublishFailure must then unwind. reqTimeoutDuration is initialized
    // from the brpc controller deadline in the plain-unary CallMethod prologue
    // (brpc_service_generator), so it reflects the client-configured request
    // budget propagated through the worker. Use the real remaining (not the
    // network-latency-deducted one): this is a hard "budget exhausted" check.
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    if (remainingUs <= 0) {
        LOG(INFO) << FormatString("[ObjectKey %s] deadline gate: skip master RPC, client budget exhausted "
                                  "(remaining %ld us).", objectKey, remainingUs);
        return Status(StatusCode::K_RPC_DEADLINE_EXCEEDED,
                      FormatString("Request timeout before master RPC, remaining %ld us.", remainingUs));
    }

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
        safeObj->SetTtlSecond(params.ttlSecond);
    } else {
        LOG_IF_ERROR(safeObj->FreeResources(), "SafeObj free failed");
        LOG(ERROR) << FormatString("[ObjectKey %s] RequestingToMaster failed, status: %s", objectKey, rc.ToString());
        if (IsRpcTimeout(rc)) {
            return Status(rc.GetCode(), FormatString("Create meta to master failed. detail: %s", rc.ToString()));
        }
        const std::unordered_set<StatusCode> passthroughError{
            StatusCode::K_WORKER_TIMEOUT,    StatusCode::K_KVSTORE_ERROR, StatusCode::K_OC_KEY_ALREADY_EXIST,
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
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
        RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApi(objectKey, workerMasterApi));
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
    VLOG(1) << FormatString("Current life state: %d, next state: %d, ttl second: %u.", (int)oldLifeState,
                            (int)params.lifeState, params.ttlSecond);
    // Step 1: Verify and request to master, object may be expired due to network latency.
    RETURN_IF_NOT_OK(RequestingToMaster(objectKV, params));

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
    int64_t elapsed = timer.ElapsedMilliSecond();
    Raii unlock([&entry]() { entry->WUnlock(); });
    ObjectKV objectKV(objectKey, *entry);
    RETURN_IF_NOT_OK(PrepareForPublish(req, clientId, shmUnitId, objectKV));
    constexpr double reserveGetLogThresholdMs = 0.1;  // 100us
    if (elapsed > reserveGetLogThresholdMs) {
        VLOG(1) << FormatString("Client %s is putting the object %s, ReserveGetAndLock elapsed %lld ms.",
                                req.client_id(), objectKey, static_cast<long long>(elapsed));
    }

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
    VLOG(1) << FormatString("[ObjectKey %s] is being publishing [Sz: %zu].", req.object_key(), req.data_size());
    std::string tenantId;
    Status authRc = req.is_routed() ? worker::AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId)
                                    : worker::Authenticate(akSkManager_, req, tenantId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(authRc, "Authenticate failed.");
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
    VLOG(1) << "Put success";
    INJECT_POINT("worker.after_publish");
    return Status::OK();
}

Status WorkerOcServicePublishImpl::Publish(const PublishReqPb &req, PublishRspPb &resp,
                                           std::vector<RpcMessage> &payloads)
{
    ScopedRequestContext ctx;
    Timer timer;
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_PUBLISH_START);
    }
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_PUBLISH);
    access.ObjectKeyProvider([&req]() -> std::string { return req.object_key(); }).DataSize(req.data_size());
    if (req.nested_keys_size() > 0) {
        access.NestedKeyProvider([&req] { return objectKeysToString(req.nested_keys()); });
    }
    access.Keep(req.keep())
        .WriteMode(req.write_mode())
        .ConsistencyType(req.consistency_type())
        .IsSeal(req.is_seal())
        .IsRetry(req.is_retry())
        .TtlSecond(req.ttl_second())
        .Existence(req.existence())
        .CacheType(req.cache_type());
    Status rc = PublishImpl(req, resp, payloads);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_PUBLISH_END);
    }
    const auto totalPublishUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    if (ShouldPrintLatencySummary(totalPublishUs, config)) {
        PhaseDurationResult result = ComputePhaseDurations(
            Trace::Instance().GetLatencyTicks(), Trace::Instance().GetLatencyTickCount(),
            Trace::Instance().GetLatencyTickDroppedCount());
        bool hasDownstream = Trace::Instance().GetDownstreamPhases().count > 0;
        MergeDownstreamPhases(result);
        bool gateHit = CheckPhaseGate(result, config);
        if (gateHit) {
            Trace::Instance().SetLatencySummary(FormatLatencySummary(result));
        }
        if (gateHit || hasDownstream) {
            EncodePhaseProto(resp, result);
        }
    }
    access.Result(rc).Record();
    const double totalPublishMs = static_cast<double>(totalPublishUs) / US_PER_MS;
    GetWorkerTimeCost().Append("Total Publish", totalPublishMs);
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && totalPublishUs >= config.processSlowerThanUs, 1,
        FormatString("Publish done, cost: %.3fms, %s", totalPublishMs, GetWorkerTimeCost().GetInfo()));
    return rc;
}

}  // namespace object_cache
}  // namespace datasystem
