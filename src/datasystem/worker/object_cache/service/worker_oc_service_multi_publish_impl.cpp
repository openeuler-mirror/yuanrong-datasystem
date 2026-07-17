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
 * Description: Defines the worker service processing multi-publish process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.h"

#include <algorithm>
#include <cstddef>
#include <utility>


#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/object_cache/service/service_execution_policy.h"

DS_DECLARE_bool(enable_distributed_master);

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {
static constexpr int RETRY_INTERNAL_MS_META_MOVING = 200;
static constexpr int THREAD_WAIT_TIME_MS = 10;
WorkerOcServiceMultiPublishImpl::WorkerOcServiceMultiPublishImpl(
    WorkerOcServiceCrudParam &initParam, std::shared_ptr<ThreadPool> memCpyThreadPool,
    std::shared_ptr<ThreadPool> threadPool,
    std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
      memCpyThreadPool_(std::move(memCpyThreadPool)),
      threadPool_(std::move(threadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(localAddress)
{
}

Status WorkerOcServiceMultiPublishImpl::MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                                     std::vector<RpcMessage> &payloads, const ClientKey &clientId)
{
    ScopedRequestContext ctx;
    Timer timer;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_MULTIPUBLISH);
    access
        .ObjectKeyProvider([&req]() -> std::string {
            return req.object_info().empty() ? std::string() : req.object_info(0).object_key();
        })
        .DataSizeProvider(
            [&req]() -> uint64_t { return req.object_info().empty() ? 0 : req.object_info(0).data_size(); });
    if (req.object_info().empty()) {
        Status rc(K_INVALID, __LINE__, __FILE__, "The list of object info is empty.");
        access.Result(rc).Record();
        return rc;
    }
    Status status = MultiPublishImpl(req, resp, payloads, clientId);
    access.Result(status).Record();
    auto totalMs = timer.ElapsedMilliSecond();
    GetWorkerTimeCost().Append("Total MultiPublish", totalMs);
    auto vlogLevel = (totalMs > 1 || status.IsError()) ? 0 : 1;
    VLOG(vlogLevel) << FormatString("MultiPublish done, cost: %.1fms, %s", totalMs, GetWorkerTimeCost().GetInfo());
    return status;
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishImpl(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                                         std::vector<RpcMessage> &payloads, const ClientKey &clientId)
{
    INJECT_POINT("worker.before_CreateMultiMetaToMaster");
    PerfPoint pointAll(PerfKey::WORKER_MULTI_PUBLISH_TOTAL);
    PerfPoint point(PerfKey::WORKER_MULTI_PUBLISH_INPUT_CHECK);
    // Add namespace for objectKey.
    std::string tenantId;
    Status authRc = req.is_routed() ? worker::AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId)
                                    : worker::Authenticate(akSkManager_, req, tenantId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(authRc, "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_info_size()),
                                         StatusCode::K_INVALID,
                                         "The objectKeys size exceed " + std::to_string(OBJECT_KEYS_MAX_SIZE_LIMIT));
    std::vector<ShmKey> shmUnits;
    shmUnits.reserve(req.object_info_size());
    for (const auto &info : req.object_info()) {
        if (!info.shm_id().empty()) {
            shmUnits.emplace_back(ShmKey::Intern(info.shm_id()));
        }
    }
    RETURN_IF_NOT_OK(
        WorkerOcServiceCrudCommonApi::CheckShmUnitByTenantId(tenantId, clientId, shmUnits, memoryRefTable_));

    CHECK_FAIL_RETURN_STATUS(!(FLAGS_enable_distributed_master && req.existence() == ExistenceOptPb::NX),
                             K_RUNTIME_ERROR,
                             "The MSet with NX existence option is not supported in distributed master mode");

    RETURN_IF_NOT_OK(CheckIfL2CacheNeededAndWritable(supportL2Storage_, WriteMode(req.write_mode())));

    std::vector<std::string> namespaceUri;
    size_t objectSize = static_cast<size_t>(req.object_info_size());
    namespaceUri.reserve(objectSize);
    Status checkStatus = Status::OK();
    for (size_t i = 0; i < objectSize; ++i) {
        namespaceUri.emplace_back(
            TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_info(i).object_key()));
    }

    VLOG(1) << FormatString("Process multi pub, cacheType: %d", req.cache_type());
    RETURN_IF_NOT_OK(VerifyDuplicateKeys(namespaceUri));
    VLOG(1) << "Multi publish with key : " << VectorToString(namespaceUri);

    // Insert entry to object table and get lock.
    std::vector<bool> ifInserts(namespaceUri.size(), false);
    std::vector<std::shared_ptr<SafeObjType>> entries(namespaceUri.size(), nullptr);
    point.RecordAndReset(PerfKey::WORKER_MULTI_PUBLISH_NTX);
    return MultiPublishNtx(namespaceUri, entries, ifInserts, req, resp, payloads);
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishNtx(std::vector<std::string> &namespaceUri,
                                                        std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                        std::vector<bool> &ifInserts, const MultiPublishReqPb &req,
                                                        MultiPublishRspPb &resp, std::vector<RpcMessage> &payloads)
{
    PerfPoint point(PerfKey::WORKER_MULTI_PUBLISH_NTX_BATCH_LOCK);
    ExistenceOptPb existenceOpt = static_cast<::datasystem::ExistenceOptPb>(req.existence());
    std::unordered_set<std::string> localExistKeys;
    RETURN_IF_NOT_OK(BatchLockForSetNtx(namespaceUri, existenceOpt, ifInserts, entries, localExistKeys));
    point.Record();
    Raii unlockRaii([&entries]() {
        for (const auto &entry : entries) {
            if (entry != nullptr) {
                entry->WUnlock();
            }
        }
    });
    std::unordered_set<std::string> failedKeys;
    VerifyObjectsNtx(req, namespaceUri, entries, failedKeys, localExistKeys);
    Status lastRc;
    point.RecordAndReset(PerfKey::WORKER_MULTI_PUBLISH_NTX_IMPL);
    Status rc = MultiPublishObjectNtx(req, namespaceUri, entries, payloads, lastRc, failedKeys);
    point.RecordAndReset(PerfKey::WORKER_MULTI_PUBLISH_NTX_POST_PROCESS);
    if (rc.IsError()) {
        BatchRollBackEntries(namespaceUri, ifInserts, entries);
        return rc;
    }
    resp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
    // roll back failed ids;
    BatchRollBackEntries(namespaceUri, ifInserts, entries, failedKeys, resp);
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::VerifyDuplicateKeys(const std::vector<std::string> &objectKeys)
{
    std::unordered_set<std::string> keys(objectKeys.begin(), objectKeys.end());
    if (keys.size() != objectKeys.size()) {
        RETURN_STATUS_LOG_ERROR(
            K_DUPLICATED, FormatString("Multi publish with duplicate object key: %s", VectorToString(objectKeys)));
    }
    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::VerifyObjectsNtx(const MultiPublishReqPb &req,
                                                       const std::vector<std::string> &objectKeys,
                                                       std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                       std::unordered_set<std::string> &failedKeys,
                                                       const std::unordered_set<std::string> &localExistKeys)
{
    for (size_t i = 0; i < entries.size(); i++) {
        if (entries[i] == nullptr) {
            if (localExistKeys.find(objectKeys[i]) != localExistKeys.end()) {
                continue;
            }
            failedKeys.emplace(objectKeys[i]);
            continue;
        }
        if (entries[i]->Get() != nullptr) {
            Status rc = VerifyObjectReleaseValidity(req, *entries[i]);
            if (rc.IsError()) {
                LOG(ERROR) << "ObjectKey: " << objectKeys[i] << " verify validity failed, rc: " << rc.ToString();
                entries[i]->WUnlock();
                entries[i].reset();
                failedKeys.emplace(objectKeys[i]);
            }
        }
    }
}

Status WorkerOcServiceMultiPublishImpl::VerifyObjectReleaseValidity(const MultiPublishReqPb &req,
                                                                    const SafeObjType &safeObj)
{
    if (safeObj->IsSealed()) {
        RETURN_STATUS(StatusCode::K_OC_ALREADY_SEALED, "already be sealed.");
    } else if (safeObj->IsPublished()) {
        if (static_cast<CacheType>(req.cache_type()) != safeObj->modeInfo.GetCacheType()) {
            RETURN_STATUS(StatusCode::K_INVALID, "The cache type cannot be modified.");
        }
    }
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishObjectNtx(const MultiPublishReqPb &req,
                                                              std::vector<std::string> &objectKeys,
                                                              std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                              std::vector<RpcMessage> &payloads, Status &lastRc,
                                                              std::unordered_set<std::string> &failedKeys)
{
    PerfPoint point(PerfKey::WORKER_MULTI_PUBLISH_NTX_SAVE_ENTRY);
    // Save shmId to the entry, it also need to copy data to share memory if it's the small data.
    const auto &clientId = ClientKey::Intern(req.client_id());
    for (size_t i = 0; i < entries.size(); i++) {
        PerfPoint pointIn(PerfKey::WORKER_MULTI_PUBLISH_NTX_SET_ENTRY);
        if (failedKeys.find(objectKeys[i]) != failedKeys.end() || entries[i] == nullptr) {
            continue;
        }
        if (entries[i]->Get() != nullptr) {
            if (!(*entries[i])->IsBinary()) {
                LOG(ERROR) << "The objectKey: " << objectKeys[i] << "you set already exists, which is used by hset.";
                lastRc = Status(K_INVALID, "The object already exists, which is used by hset");
                failedKeys.emplace(objectKeys[i]);
                continue;
            }
            UpdateObjectEntry(ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                              static_cast<CacheType>(req.cache_type()), GetMetadataSize(), *entries[i]);
        } else {
            SetNewObjectEntry(objectKeys[i], ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                              static_cast<CacheType>(req.cache_type()), req.object_info(i).data_size(),
                              GetMetadataSize(), *entries[i]);
        }

        pointIn.RecordAndReset(PerfKey::WORKER_MULTI_PUBLISH_NTX_ATTACH_TO_ENTRY);
        lastRc = AttachShmUnitToObject(clientId, objectKeys[i], ShmKey::Intern(req.object_info(i).shm_id()),
                                       req.object_info(i).data_size(), *entries[i]);
        pointIn.Record();
        if (lastRc.IsError()) {
            LOG(ERROR) << "objKey: " << objectKeys[i] << " AttachShmUnitToObject failed, status: " << lastRc.ToString();
            failedKeys.emplace(objectKeys[i]);
            continue;
        }

        if (!req.object_info(i).shm_id().empty()) {
            continue;
        }

        // For MultiPublishObjectNtx, the max value size less than 500KB, use payload to transter the val,
        // the object and RpcMessage is one-to-one.
        if (i >= payloads.size()) {
            LOG(ERROR) << "Small object: " << objectKeys[i] << " can't find data, payload size: " << payloads.size();
            lastRc = Status(K_RUNTIME_ERROR, "Small object doesn't match the payload size.");
            failedKeys.emplace(objectKeys[i]);
            continue;
        }
        pointIn.Reset(PerfKey::WORKER_MULTI_PUBLISH_NTX_SAVE_PATLOAD);
        std::vector<RpcMessage> payload(1);
        payload[0] = std::move(payloads[i]);
        ObjectKV objectKV(objectKeys[i], *entries[i]);
        lastRc = SaveBinaryObjectToMemory(objectKV, payload, evictionManager_, memCpyThreadPool_);
        if (lastRc.IsError()) {
            LOG(ERROR) << "objKey: " << objectKeys[i]
                       << " SaveBinaryObjectToMemory failed, status:" << lastRc.ToString();
            failedKeys.emplace(objectKeys[i]);
            continue;
        }
    }
    point.RecordAndReset(PerfKey::WORKER_MULTI_PUBLISH_NTX_SEND_TO_MASTER);
    RETURN_IF_NOT_OK(SendToMasterAndUpdateObject(req, objectKeys, failedKeys, entries, lastRc));
    return Status::OK();
}

WorkerOcServiceMultiPublishImpl::CreateMultiMetaResult WorkerOcServiceMultiPublishImpl::BuildCreateMultiMetaResult(
    const std::shared_ptr<worker::WorkerMasterOCApi> &api, master::CreateMultiMetaReqPb &req,
    const HostPort &masterAddr)
{
    CreateMultiMetaRspPb rsp;
    PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META);
    auto rc = RetryCreateMultiMeta(api, req, rsp);
    if (IsRpcTimeout(rc)) {
        rsp.mutable_last_rc()->set_error_code(rc.GetCode());
        rsp.mutable_last_rc()->set_error_msg(rc.GetMsg());
        for (auto &meta : *(req.mutable_metas())) {
            rsp.add_failed_object_keys(meta.object_key());
        }
    }
    return CreateMultiMetaResult{ rc, rsp, masterAddr };
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaParallel(const std::vector<HostPort> &masterAddrs,
                                                                std::vector<master::CreateMultiMetaReqPb> &reqs,
                                                                std::vector<CreateMultiMetaResult> &respRes)
{
    if (masterAddrs.empty()) {
        return Status::OK();
    }
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    if (remainingUs <= 0) {
        return Status(K_RPC_DEADLINE_EXCEEDED,
                      FormatString("RPC deadline exceeded before dispatch, remaining %ld us.", remainingUs));
    }
    auto dispatchTime = std::chrono::steady_clock::now();
    auto traceId = Trace::Instance().GetTraceID();
    std::vector<std::future<CreateMultiMetaResult>> futures;
    CreateMultiMetaResult lastRc;
    const bool useThreadPoolFanout = ShouldUseServiceThreadPoolFanout(FLAGS_use_brpc);
    for (size_t i = 0; i < masterAddrs.size(); i++) {
        auto &masterAddr = masterAddrs[i];
        auto &req = reqs[i];
        auto func = [this, &masterAddr, &req, remainingUs, dispatchTime, &traceId] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId, true);
            std::shared_ptr<WorkerMasterOCApi> api;
            auto rc = workerMasterApiManager_->GetWorkerMasterApi(masterAddr, api);
            if (rc.IsError()) {
                return CreateMultiMetaResult{ rc, {}, masterAddr };
            }
            auto initRc = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
            if (initRc.IsError()) {
                return CreateMultiMetaResult{ initRc, {}, masterAddr };
            }
            return BuildCreateMultiMetaResult(api, req, masterAddr);
        };
        auto runInline = [&func]() {
            // Use the caller thread for brpc mode and for the last non-brpc task.
            ApiDeadline::Instance().Push();
            Raii deadlineRaii([]() { ApiDeadline::Instance().Pop(); });
            return func();
        };
        if (!useThreadPoolFanout) {
            respRes.emplace_back(runInline());
        } else if (i == masterAddrs.size() - 1) {
            lastRc = runInline();
        } else {
            futures.emplace_back(threadPool_->Submit(std::move(func)));
        }
    }

    if (!useThreadPoolFanout) {
        return Status::OK();
    }
    for (auto &future : futures) {
        respRes.emplace_back(future.get());
    }
    respRes.emplace_back(lastRc);
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::RetryCreateMultiMeta(std::shared_ptr<WorkerMasterOCApi> api,
                                                             CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp)
{
    while (true) {
        CHECK_FAIL_RETURN_STATUS(GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime() > 0,
                                 K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
        RETURN_IF_NOT_OK(api->CreateMultiMeta(req, rsp, true));
        if (rsp.info().empty()) {
            return Status::OK();
        }
        if (rsp.meta_is_moving()) {
            int64_t remainingTimeMs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
            CHECK_FAIL_RETURN_STATUS(remainingTimeMs > 0, K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
            rsp.Clear();
            SleepForMetaMovingRetry(std::min<int64_t>(RETRY_INTERNAL_MS_META_MOVING, remainingTimeMs));
            continue;
        }
        return Status(K_SCALING, "The cluster is scaling, please try again.");
    }
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaToDistributedMasterNtx(
    const std::vector<std::string> &objectKeys, const std::vector<std::shared_ptr<SafeObjType>> &entries,
    const MultiPublishReqPb &pubReq, CreateMultiMetaRspPb &totalResp, std::vector<uint64_t> &versions)
{
    PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META_ROUTER);
    CHECK_FAIL_RETURN_STATUS(metadataRouteResolver_ != nullptr, K_NOT_READY, "Metadata route resolver is unavailable");
    auto grouped = metadataRouteResolver_->GroupIndexedOwners(objectKeys);
    const auto &objGroup = grouped.groups;
    // Fixme: Currently, even if there is only one object for which the master node has not been identified, we will
    // refuse to process all objects, which is very inefficient.
    CHECK_FAIL_RETURN_STATUS(grouped.failures.empty(), K_RPC_UNAVAILABLE,
                             "Getting master api failed. ObjectKey: " + grouped.failures.begin()->first);
    point.RecordAndReset(PerfKey::WORKER_CREATE_MULTI_META_CONSTRUCT_REQ);

    std::vector<HostPort> addrs;
    addrs.reserve(objGroup.size());
    std::vector<CreateMultiMetaReqPb> createReqs(objGroup.size());
    int idx = 0;
    // Group by metadata owner HostPort. The current topology has one worker per HostPort.
    for (const auto &[masterAddr, objInfos] : objGroup) {
        auto &req = createReqs[idx];
        addrs.emplace_back(masterAddr);
        ConstructCreateReq(objInfos, entries, pubReq, req);
        idx++;
    }

    point.RecordAndReset(PerfKey::WORKER_CREATE_MULTI_META_PARALLEL);
    std::vector<CreateMultiMetaResult> respRes;
    respRes.reserve(createReqs.size());
    RETURN_IF_NOT_OK(CreateMultiMetaParallel(addrs, createReqs, respRes));
    point.RecordAndReset(PerfKey::WORKER_CREATE_MULTI_META_FILL_RSP);
    CHECK_FAIL_RETURN_STATUS(
        respRes.size() == createReqs.size(), K_RUNTIME_ERROR,
        FormatString("The object size(%d) and the versions(%d) is not equal", createReqs.size(), respRes.size()));

    for (size_t index = 0; index < respRes.size(); index++) {
        auto &resp = respRes[index].rsp;
        for (const auto &failedId : resp.failed_object_keys()) {
            totalResp.add_failed_object_keys(failedId);
        }
        LOG_IF_ERROR(respRes[index].rc, "Get error with createMeta");
        for (const auto &obj : objGroup.at(respRes[index].masterAddr)) {
            versions[obj.second] = resp.version();
        }
        if (resp.has_last_rc() && static_cast<StatusCode>(resp.last_rc().error_code()) != StatusCode::K_OK) {
            totalResp.mutable_last_rc()->set_error_code(resp.last_rc().error_code());
            totalResp.mutable_last_rc()->set_error_msg(resp.last_rc().error_msg());
        }
    }
    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::ConstructCreateReqCommon(SafeObjType &entry, const MultiPublishReqPb &pubReq,
                                                               CreateMultiMetaReqPb &req)
{
    req.set_address(localAddress_.ToString());
    req.set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_PUBLISHED));
    req.set_ttl_second(pubReq.ttl_second());
    req.set_existence(static_cast<::datasystem::ExistenceOptPb>(pubReq.existence()));
    ConfigPb *configPb = req.mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>(entry->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>(entry->stateInfo.GetDataFormat()));
    configPb->set_consistency_type(static_cast<uint32_t>(entry->modeInfo.GetConsistencyType()));
    configPb->set_cache_type(static_cast<uint32_t>(entry->modeInfo.GetCacheType()));
    configPb->set_is_replica(pubReq.is_replica());
}

void WorkerOcServiceMultiPublishImpl::ConstructCreateReq(const std::vector<std::pair<std::string, size_t>> &objectInfos,
                                                         const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                         const MultiPublishReqPb &pubReq, CreateMultiMetaReqPb &req)
{
    for (const auto &[objectKey, i] : objectInfos) {
        ObjectBaseInfoPb meta;
        meta.set_object_key(objectKey);
        meta.set_data_size((*entries[i])->GetDataSize());
        if (pubReq.object_info(i).blob_sizes_size() > 0) {
            meta.mutable_device_info()->mutable_blob_sizes()->Add(pubReq.object_info(i).blob_sizes().begin(),
                                                                  pubReq.object_info(i).blob_sizes().end());
        }
        req.mutable_metas()->Add(std::move(meta));
    }
    ConstructCreateReqCommon(*entries[0], pubReq, req);
}

void WorkerOcServiceMultiPublishImpl::RollbackPersistenceIfFailed(const Status &status,
                                                                  const std::string &objectKey, uint64_t version)
{
    if (status.IsOk()) {
        return;
    }
    LOG(ERROR) << FormatString("Multiple set fails to save object %s to l2cache.", objectKey);
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    auto routeRc = workerMasterApiManager_->GetWorkerMasterApi(objectKey, workerMasterApi);
    if (routeRc.IsError()) {
        LOG(ERROR) << "Getting metadata owner failed during rollback: " << routeRc.ToString();
        return;
    }
    master::RollbackMultiMetaReqPb req;
    master::RollbackMultiMetaRspPb resp;
    req.set_address(localAddress_.ToString());
    req.set_persistence_only(true);
    req.add_object_keys(objectKey);
    req.add_versions(version);
    workerMasterApi->RollbackMultiMeta(req, resp);
}

void WorkerOcServiceMultiPublishImpl::UpdateObjectAfterCreatingMeta(
    const std::vector<std::string> &objectKeys, const std::vector<std::shared_ptr<SafeObjType>> &objectEntries,
    const std::vector<uint64_t> &versions, uint32_t ttlSecond)
{
    const auto &keys = objectKeys;
    const auto &entries = objectEntries;
    for (size_t i = 0; i < keys.size(); i++) {
        ObjectKV objectKV(keys[i], *entries[i]);
        objectKV.GetObjEntry()->SetCreateTime(versions[i]);
        objectKV.GetObjEntry()->SetTtlSecond(ttlSecond);
        // Save object to L2 cache
        if ((*entries[i])->IsWriteThroughMode()) {
            if (IsSupportL2Storage(supportL2Storage_)) {
                Status rc = SaveBinaryObjectToPersistence(objectKV);
                RollbackPersistenceIfFailed(rc, keys[i], versions[i]);
            }
        }
        if ((*entries[i])->IsWriteBackMode() && IsSupportL2Storage(supportL2Storage_)) {
            std::future<Status> future;
            LOG_IF_ERROR(asyncSendManager_->Add(keys[i], entries[i], future),
                         FormatString("Multiple set fails to add object %s to async L2cache manager.", keys[i]));
        }
        // Update entry information
        (*entries[i])->stateInfo.SetNeedToDelete(false);
        (*entries[i])->SetLifeState(ObjectLifeState::OBJECT_PUBLISHED);
        (*entries[i])->stateInfo.SetPrimaryCopy(true);
        (*entries[i])->stateInfo.SetCacheInvalid(false);
        (*entries[i])->stateInfo.SetIncompleted(false);
        // Evict and spill process
        if ((*entries[i])->IsSpilled()) {
            LOG_IF_ERROR(DeleteObjectFromDisk(objectKV),
                         FormatString("Multiple set fails to delete spilled object %s from disk.", keys[i]));
        }
    }
    PerfPoint point(PerfKey::WORKER_MULTI_PUBLISH_ASYNC_NOTIFY);
    auto traceId = Trace::Instance().GetTraceID();
    threadPool_->Execute([this, keys, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Status rc;
        for (const auto &key : keys) {
            evictionManager_->Add(key);
            std::shared_ptr<SafeObjType> entry;
            Raii raii([key, &rc]() { LOG_IF_ERROR(rc, FormatString("Fails to update object %s get request.", key)); });
            rc = objectTable_->Get(key, entry);
            if (rc.IsError()) {
                continue;
            }
            rc = entry->RLock();
            if (rc.IsError()) {
                continue;
            }
            ObjectKV objectKV(key, *entry);
            rc = workerRequestManager_.NotifyPendingGetRequest(objectKV);
            entry->RUnlock();
        }
    });
}

Status WorkerOcServiceMultiPublishImpl::SendToMasterAndUpdateObject(
    const MultiPublishReqPb &req, std::vector<std::string> &objectKeys, std::unordered_set<std::string> &failedKeys,
    std::vector<std::shared_ptr<SafeObjType>> &objectEntries, Status &lastRc)
{
    CreateMultiMetaRspPb rsp;
    std::vector<std::string> keys;
    std::vector<std::shared_ptr<SafeObjType>> entries;
    keys.reserve(objectKeys.size());
    entries.reserve(objectEntries.size());
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        if (failedKeys.find(objectKeys[i]) != failedKeys.end() || objectEntries[i] == nullptr) {
            continue;
        }
        keys.emplace_back(objectKeys[i]);
        entries.emplace_back(objectEntries[i]);
    }

    PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META_NTX);
    std::vector<uint64_t> versions(keys.size());
    if (!keys.empty()) {
        RETURN_IF_NOT_OK(CreateMultiMetaToDistributedMasterNtx(keys, entries, req, rsp, versions));
    }
    point.Record();

    failedKeys.insert(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());

    if (rsp.has_last_rc()) {
        Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
        lastRc = recvRc.IsOk() ? lastRc : recvRc;
    }
    point.Reset(PerfKey::WORKER_UPDATE_OBJECT_AFTER_CREATE_META);
    UpdateObjectAfterCreatingMeta(keys, entries, versions, req.ttl_second());

    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::BatchRollBackEntriesImpl(const std::string &objectKey, bool ifInsert,
                                                               std::shared_ptr<SafeObjType> &entry)
{
    if (entry == nullptr) {
        return;
    }
    if (entry->Get() != nullptr && entry->Get()->GetShmUnit() != nullptr) {
        entry->Get()->GetShmUnit()->SetHardFreeMemory();
    }
    if (ifInsert) {
        (void)objectTable_->Erase(objectKey, *(entry));
        return;
    }
    if (entry->Get() != nullptr) {
        LOG_IF_ERROR(entry->Get()->FreeResources(), "roll back failed object entry failed when FreeResource");
        entry->Get()->SetLifeState(ObjectLifeState::OBJECT_INVALID);
        entry->Get()->stateInfo.SetCacheInvalid(true);
    }
}

void WorkerOcServiceMultiPublishImpl::BatchRollBackEntries(const std::vector<std::string> &objectKeys,
                                                           const std::vector<bool> &ifInserts,
                                                           std::vector<std::shared_ptr<SafeObjType>> &entries)
{
    size_t size = objectKeys.size();
    for (size_t i = 0; i < size; ++i) {
        BatchRollBackEntriesImpl(objectKeys[i], ifInserts[i], entries[i]);
    }
}

void WorkerOcServiceMultiPublishImpl::BatchRollBackEntries(const std::vector<std::string> &objectKeys,
                                                           const std::vector<bool> &ifInserts,
                                                           std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                           const std::unordered_set<std::string> &failedKeys,
                                                           MultiPublishRspPb &resp)
{
    if (failedKeys.empty()) {
        return;
    }
    for (size_t i = 0; i < objectKeys.size(); i++) {
        if (failedKeys.find(objectKeys[i]) == failedKeys.end()) {
            continue;
        }
        resp.add_failed_object_keys(objectKeys[i]);
        BatchRollBackEntriesImpl(objectKeys[i], ifInserts[i], entries[i]);
    }
}

Status WorkerOcServiceMultiPublishImpl::BatchLockForSetNtx(const std::vector<std::string> &objectKeys,
                                                           const ExistenceOptPb &existence,
                                                           std::vector<bool> &isInserts,
                                                           std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                           std::unordered_set<std::string> &localExistKeys)
{
    std::map<std::string, size_t> objectToIdx;
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        objectToIdx[objectKeys[i]] = i;
    }
    std::atomic<size_t> failCnt{ 0 };
    std::function<Status()> GetEntryLock = [&]() {
        for (auto itr = objectToIdx.begin(); itr != objectToIdx.end(); ++itr) {
            std::shared_ptr<SafeObjType> entry;
            bool isInsert;
            auto rc = objectTable_->ReserveGetAndLock(itr->first, entry, isInsert, existence == ExistenceOptPb::NX);
            if (rc.IsError()) {
                if (existence == ExistenceOptPb::NX && rc.GetCode() == K_OC_KEY_ALREADY_EXIST) {
                    localExistKeys.emplace(itr->first);
                    continue;
                }
                LOG(ERROR) << FormatString("Lock for object %s failed, rc=%s", itr->first.c_str(), rc.ToString());
                failCnt.fetch_add(1);
                continue;
            }
            isInserts[itr->second] = isInsert;
            entries[itr->second] = entry;
        }
        return Status::OK();
    };

    GetEntryLock();
    return failCnt == objectKeys.size() ? Status(K_TRY_AGAIN, "Lock objects failed.") : Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
