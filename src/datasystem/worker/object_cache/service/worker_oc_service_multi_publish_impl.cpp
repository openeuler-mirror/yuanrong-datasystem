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
 * Description: Defines the worker service processing multi-publish process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_multi_publish_impl.h"

#include <utility>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"

DS_DECLARE_bool(enable_distributed_master);

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {
static constexpr int RETRY_INTERNAL_MS_META_MOVING = 200;
static constexpr int THREAD_WAIT_TIME_MS = 10;
// If exceed MSET_PENDING_TTL_US, the metadata may be preempted by other workers.
static constexpr int64_t MSET_TRANSACTION_TIMEOUT_MS = 50'000;

WorkerOcServiceMultiPublishImpl::WorkerOcServiceMultiPublishImpl(
    WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM, std::shared_ptr<ThreadPool> memCpyThreadPool,
    std::shared_ptr<ThreadPool> threadPool, std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
      etcdCM_(etcdCM),
      memCpyThreadPool_(std::move(memCpyThreadPool)),
      threadPool_(std::move(threadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(localAddress)
{
}

Status WorkerOcServiceMultiPublishImpl::MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                                     std::vector<RpcMessage> &payloads)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_MULTIPUBLISH);
    CHECK_FAIL_RETURN_STATUS(!req.object_info().empty(), K_INVALID, "The list of object info is empty.");
    Status status = MultiPublishImpl(req, resp, payloads);
    RequestParam reqParam;
    reqParam.objectKey = FormatString(
        "%s,count:%s", req.object_info(0).object_key().substr(0, LOG_OBJECT_KEY_SIZE_LIMIT), req.object_info_size());
    posixPoint.Record(status.GetCode(), std::to_string(req.object_info(0).data_size()), reqParam, status.GetMsg());
    workerOperationTimeCost.Append("Total MultiPublish", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of worker MultiPublish %s", workerOperationTimeCost.GetInfo());
    return status;
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishImpl(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                                         std::vector<RpcMessage> &payloads)
{
    // Add namespace for objectKey.
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_info_size()),
                                         StatusCode::K_INVALID, "invalid object info size");
    std::vector<std::string> shmUnits;
    for (const auto &info : req.object_info()) {
        if (!info.shm_id().empty()) {
            shmUnits.emplace_back(info.shm_id());
        }
    }
    RETURN_IF_NOT_OK(
        WorkerOcServiceCrudCommonApi::CheckShmUnitByTenantId(tenantId, req.client_id(), shmUnits, memoryRefTable_));

    // MSet doesn't support distributed master with : case1: NTX and Existence::NX ; case2: TX and Existence::NONE.
    if (FLAGS_enable_distributed_master) {
        std::string result = req.istx() ? "tx_" : "ntx_";
        result = "The MSet doesn't support " + result + std::to_string(req.existence());
        CHECK_FAIL_RETURN_STATUS((req.istx() && req.existence() == ExistenceOptPb::NX)
                                     || (!req.istx() && req.existence() == ExistenceOptPb::NONE),
                                 K_RUNTIME_ERROR, result);
    }

    RETURN_IF_NOT_OK(CheckIfL2CacheNeededAndWritable(supportL2Storage_, WriteMode(req.write_mode())));

    std::vector<std::string> namespaceUri;
    size_t objectSize = static_cast<size_t>(req.object_info_size());
    for (size_t i = 0; i < objectSize; ++i) {
        namespaceUri.emplace_back(
            TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_info(i).object_key()));
    }

    LOG(INFO) << FormatString("Process multi pub from client: %s, cacheType: %d", req.client_id(), req.cache_type());
    VLOG(1) << "Multi publish with key : " << VectorToString(namespaceUri);

    // Insert entry to object table and get lock.
    std::vector<bool> ifInserts(namespaceUri.size(), false);
    std::vector<std::shared_ptr<SafeObjType>> entries(namespaceUri.size(), nullptr);
    if (req.istx()) {
        return MultiPublishTx(namespaceUri, entries, ifInserts, req, payloads);
    }
    return MultiPublishNtx(namespaceUri, entries, ifInserts, req, resp, payloads);
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishTx(std::vector<std::string> &namespaceUri,
                                                       std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                       std::vector<bool> &ifInserts, const MultiPublishReqPb &req,
                                                       std::vector<RpcMessage> &payloads)
{
    std::vector<size_t> successLockIndex;
    Status status = BatchLockForSet(namespaceUri, ifInserts, entries, successLockIndex);
    Raii unlockRaii([&entries, &successLockIndex]() { BatchUnlockForSet(entries, successLockIndex); });
    if (status.IsError()) {
        LOG(ERROR) << "Fail to lock all the objects: " << status.ToString();
        BatchRollBackEntries(namespaceUri, ifInserts, entries);
        return status;
    }

    status = VerifyObjectsAndRollBackIfNeedTx(req, namespaceUri, ifInserts, entries);
    if (status.IsError()) {
        LOG(ERROR) << "Fail to verify objects: " << status.ToString();
        return status;
    }

    // Publish object to master
    status = MultiPublishObject(req, namespaceUri, entries, payloads);
    if (status.IsError()) {
        LOG(ERROR) << "Fail to create all the objects on master: " << status.ToString();
        BatchRollBackEntries(namespaceUri, ifInserts, entries);
        return status;
    }
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::VerifyObjectsAndRollBackIfNeedTx(
    const MultiPublishReqPb &req, const std::vector<std::string> &objectKeys, const std::vector<bool> &ifInserts,
    std::vector<std::shared_ptr<SafeObjType>> &entries)
{
    bool needRollback = false;
    Status rc;
    for (size_t i = 0; i < entries.size(); ++i) {
        if (entries[i]->Get() == nullptr) {
            continue;
        }
        rc = VerifyObjectReleaseValidity(req, *entries[i]);
        if (rc.IsError()) {
            needRollback = true;
            LOG(INFO) << "ObjectKey: " << objectKeys[i] << " verify validity failed, rc: " << rc.ToString();
            break;
        }
    }
    if (!needRollback) {
        return rc;
    }

    for (size_t i = 0; i < objectKeys.size(); ++i) {
        if (entries[i] == nullptr) {
            continue;
        }
        if (ifInserts[i]) {
            (void)objectTable_->Erase(objectKeys[i], *(entries[i]));
        }
    }
    return rc;
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishNtx(std::vector<std::string> &namespaceUri,
                                                        std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                        std::vector<bool> &ifInserts, const MultiPublishReqPb &req,
                                                        MultiPublishRspPb &resp, std::vector<RpcMessage> &payloads)
{
    std::vector<size_t> successIndex;
    std::vector<size_t> failedIndex;
    Status lastRc;
    ExistenceOptPb existenceOpt = static_cast<::datasystem::ExistenceOptPb>(req.existence());
    Status status =
        BatchLockForSetNtx(namespaceUri, existenceOpt, ifInserts, entries, successIndex, failedIndex, lastRc);
    std::vector<size_t> successLockIndex = { successIndex.begin(), successIndex.end() };
    Raii unlockRaii([&entries, &successLockIndex]() { BatchUnlockForSet(entries, successLockIndex); });
    if (status.GetCode() == K_WORKER_DEADLOCK) {
        return status;
    }
    VerifyObjectsNtx(req, namespaceUri, successIndex, failedIndex, entries);
    status = MultiPublishObjectNtx(req, successIndex, failedIndex, namespaceUri, entries, payloads, lastRc);
    if (status.IsError()) {
        BatchRollBackEntries(namespaceUri, ifInserts, entries);
        return status;
    }
    resp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
    // roll back failed ids;
    BatchRollBackEntries(namespaceUri, ifInserts, entries, failedIndex, resp);
    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::VerifyObjectsNtx(const MultiPublishReqPb &req,
                                                       const std::vector<std::string> &objectKeys,
                                                       std::vector<size_t> &successIndex,
                                                       std::vector<size_t> &failedIndex,
                                                       std::vector<std::shared_ptr<SafeObjType>> &entries)
{
    for (auto index = successIndex.begin(); index != successIndex.end();) {
        auto i = *index;
        if (entries[i]->Get() != nullptr) {
            Status rc = VerifyObjectReleaseValidity(req, *entries[i]);
            if (rc.IsError()) {
                LOG(INFO) << "ObjectKey: " << objectKeys[i] << " verify validity failed, rc: " << rc.ToString();
                entries[i]->WUnlock();
                entries[i].reset();
                failedIndex.emplace_back(i);
                index = successIndex.erase(index);
                continue;
            }
        }
        ++index;
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
                                                              std::vector<size_t> &successIndex,
                                                              std::vector<size_t> &failedIndex,
                                                              std::vector<std::string> &objectKeys,
                                                              std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                              std::vector<RpcMessage> &payloads, Status &lastRc)
{
    // Save shmId to the entry, it also need to copy data to share memory if it's the small data.
    const bool shmEnabled = ClientShmEnabled(req.client_id());
    for (auto index = successIndex.begin(); index != successIndex.end();) {
        auto i = *index;
        if (entries[i]->Get() != nullptr) {
            if (!(*entries[i])->IsBinary()) {
                LOG(ERROR) << "The objectKey: " << objectKeys[i] << "you set already exists, which is used by hset.";
                lastRc = Status(K_INVALID, "The object already exists, which is used by hset");
                successIndex.erase(index);
                failedIndex.emplace_back(i);
                continue;
            }
            UpdateObjectEntry(ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                              static_cast<CacheType>(req.cache_type()), GetMetadataSize(), *entries[i]);
        } else {
            SetNewObjectEntry(objectKeys[i], ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                              static_cast<CacheType>(req.cache_type()), req.object_info(i).data_size(),
                              GetMetadataSize(), *entries[i]);
        }

        lastRc = AttachShmUnitToObject(shmEnabled, objectKeys[i], req.object_info(i).shm_id(),
                                       req.object_info(i).data_size(), *entries[i]);
        if (lastRc.IsError()) {
            LOG(ERROR) << "objKey: " << objectKeys[i] << " AttachShmUnitToObject failed, status: " << lastRc.ToString();
            successIndex.erase(index);
            failedIndex.emplace_back(i);
            continue;
        }

        if (!req.object_info(i).shm_id().empty()) {
            index++;
            continue;
        }

        // For MultiPublishObjectNtx, the max value size less than 500KB, use payload to transter the val,
        // the object and RpcMessage is one-to-one.
        if (i >= payloads.size()) {
            LOG(ERROR) << "Small object: " << objectKeys[i] << " can't find data, payload size: " << payloads.size();
            lastRc = Status(K_RUNTIME_ERROR, "Small object doesn't match the payload size.");
            failedIndex.emplace_back(i);
            successIndex.erase(index);
            continue;
        }
        std::vector<RpcMessage> payload(1);
        payload[0] = std::move(payloads[i]);
        ObjectKV objectKV(objectKeys[i], *entries[i]);
        lastRc = SaveBinaryObjectToMemory(objectKV, payload, evictionManager_, memCpyThreadPool_);
        if (lastRc.IsError()) {
            LOG(ERROR) << "objKey: " << objectKeys[i]
                       << " SaveBinaryObjectToMemory failed, status:" << lastRc.ToString();
            successIndex.erase(index);
            failedIndex.emplace_back(i);
            continue;
        }
        index++;
    }
    RETURN_IF_NOT_OK(SendToMasterAndUpdateObject(req, successIndex, failedIndex, objectKeys, entries, lastRc));
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::MultiPublishObject(const MultiPublishReqPb &req,
                                                           std::vector<std::string> &objectKeys,
                                                           std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                           std::vector<RpcMessage> &payloads)
{
    // Save shmId to the entry, it also need to copy data to share memory if it's the small data.
    const bool shmEnabled = ClientShmEnabled(req.client_id());
    size_t idx = 0;
    for (size_t i = 0; i < objectKeys.size(); ++i) {
        if (entries[i]->Get() != nullptr) {
            CHECK_FAIL_RETURN_STATUS((*entries[i])->IsBinary(), K_INVALID,
                                     "The key you set already exists, which is used by hset.");
            UpdateObjectEntry(ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                              static_cast<CacheType>(req.cache_type()), GetMetadataSize(), *entries[i]);
        } else {
            SetNewObjectEntry(objectKeys[i], ConsistencyType(req.consistency_type()), WriteMode(req.write_mode()),
                              static_cast<CacheType>(req.cache_type()), req.object_info(i).data_size(),
                              GetMetadataSize(), *entries[i]);
        }
        RETURN_IF_NOT_OK(AttachShmUnitToObject(shmEnabled, objectKeys[i], req.object_info(i).shm_id(),
                                               req.object_info(i).data_size(), *entries[i]));
        // Small object use payload to transfer the value, the object and RpcMessage is one-to-one.
        if (req.object_info(i).shm_id().empty()) {
            if (idx >= payloads.size()) {
                LOG(ERROR) << "Small object: " << objectKeys[i]
                           << " can't find data, payload size: " << payloads.size();
                return Status(K_RUNTIME_ERROR, "Small object doesn't match the payload size.");
            }
            std::vector<RpcMessage> payload(1);
            payload[0] = std::move(payloads[idx]);
            ++idx;
            ObjectKV objectKV(objectKeys[i], *entries[i]);
            RETURN_IF_NOT_OK(SaveBinaryObjectToMemory(objectKV, payload, evictionManager_, memCpyThreadPool_));
        }
    }

    std::vector<uint64_t> versions(objectKeys.size());
    std::vector<size_t> successIndex;
    for (size_t i = 0; i < objectKeys.size(); i++) {
        successIndex.emplace_back(i);
    }
    if (FLAGS_enable_distributed_master) {
        RETURN_IF_NOT_OK(CreateMultiMetaToDistributedMaster(objectKeys, entries, req, versions));
    } else {
        RETURN_IF_NOT_OK(CreateMultiMetaToCentralMaster(objectKeys, entries, req, versions));
    }

    UpdateObjectAfterCreatingMeta(objectKeys, entries, versions, successIndex);

    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::FillMultiMetaReqPhaseOne(
    const std::vector<std::pair<std::string, size_t>> &objectKeys,
    const std::vector<std::shared_ptr<SafeObjType>> &entries, const MultiPublishReqPb &pubReq,
    CreateMultiMetaReqPb &req)
{
    for (const auto &obj : objectKeys) {
        auto entry = entries[obj.second];
        ObjectBaseInfoPb metadata;
        metadata.set_object_key(obj.first);
        metadata.set_data_size((*entry)->GetDataSize());
        req.mutable_metas()->Add(std::move(metadata));
    }
    std::sort(
        req.mutable_metas()->begin(), req.mutable_metas()->end(),
        [](const ObjectBaseInfoPb &fir, const ObjectBaseInfoPb &sec) { return fir.object_key() < sec.object_key(); });
    req.set_address(localAddress_.ToString());
    req.set_istx(true);
    req.set_is_pre_commit(true);
    req.set_redirect(true);
    req.set_life_state(static_cast<uint32_t>(ObjectLifeState::OBJECT_PUBLISHED));
    req.set_ttl_second(pubReq.ttl_second());
    req.set_existence(static_cast<::datasystem::ExistenceOptPb>(pubReq.existence()));
    auto &firstEntry = *entries[0];
    ConfigPb *configPb = req.mutable_config();
    configPb->set_write_mode(static_cast<uint32_t>(firstEntry->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>(firstEntry->stateInfo.GetDataFormat()));
    configPb->set_consistency_type(static_cast<uint32_t>(firstEntry->modeInfo.GetConsistencyType()));
    configPb->set_cache_type(pubReq.cache_type());
}

Status WorkerOcServiceMultiPublishImpl::RetryRollbackMultiMetaWhenMoving(std::shared_ptr<WorkerMasterOCApi> api,
                                                                         RollbackMultiMetaReqPb &req,
                                                                         RollbackMultiMetaRspPb &rsp)
{
    LOG(INFO) << "RollbackMultiMetaReq to " << api->GetHostPort() << " objects: " << VectorToString(req.object_keys());
    while (true) {
        RETURN_IF_NOT_OK(api->RollbackMultiMeta(req, rsp));
        if (rsp.info().empty()) {
            return Status::OK();
        } else if (rsp.meta_is_moving()) {
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERNAL_MS_META_MOVING));
            continue;
        }
        std::set<std::string> objs;
        objs.insert(req.object_keys().begin(), req.object_keys().end());
        RollbackMultiMetaReqPb newReq;
        newReq.set_address(req.address());
        RollbackMultiMetaRspPb newRsp;
        for (const auto &info : rsp.info()) {
            std::shared_ptr<WorkerMasterOCApi> newApi;
            RETURN_IF_NOT_OK(
                workerMasterApiManager_->GetWorkerMasterApiByAddr(info.redirect_meta_address(), etcdCM_, newApi));
            LOG(INFO) << FormatString("Objects[%s] has been migrated to the new master[%s]",
                                      VectorToString(info.change_meta_ids()), newApi->GetHostPort());
            CHECK_FAIL_RETURN_STATUS(newApi != nullptr, K_RUNTIME_ERROR,
                                     "hash master get failed, RetryRollbackMultiMetaWhenMoving failed");
            for (const auto &id : info.change_meta_ids()) {
                newReq.add_object_keys(id);
                objs.erase(id);
            }
            if (newApi->RollbackMultiMeta(newReq, newRsp).IsError()) {
                rsp.mutable_failed_object_keys()->MergeFrom(newReq.object_keys());
            } else {
                rsp.mutable_failed_object_keys()->MergeFrom(newRsp.failed_object_keys());
            }
            newReq.clear_object_keys();
            newRsp.Clear();
        }
        RETURN_OK_IF_TRUE(objs.empty());
        for (const auto &obj : objs) {
            newReq.add_object_keys(obj);
        }
        if (api->RollbackMultiMeta(newReq, newRsp).IsError()) {
            rsp.mutable_failed_object_keys()->MergeFrom(newReq.object_keys());
        } else {
            rsp.mutable_failed_object_keys()->MergeFrom(newRsp.failed_object_keys());
        }
        return Status::OK();
    }
    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::RollbackMultiMetaReq(std::vector<std::shared_ptr<WorkerMasterOCApi>> &apis,
                                                           const ObjGroupMap &objGroup)
{
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    auto traceId = Trace::Instance().GetTraceID();
    std::vector<std::future<std::vector<std::string>>> futures;
    Timer timer;
    for (auto &api : apis) {
        const auto &objs = objGroup.at(api->GetHostPort());
        futures.emplace_back(threadPool_->Submit([this, &api, &objs, timeout, traceId, timer] {
            int64_t elapsed = timer.ElapsedMilliSecond();
            LOG_IF(WARNING, elapsed > THREAD_WAIT_TIME_MS) << FormatString(
                "Create meta threads statistics: %s, elapsed ms: %lld", threadPool_->GetStatistics(), elapsed);
            RollbackMultiMetaReqPb req;
            req.set_address(localAddress_.ToString());
            req.set_redirect(true);
            for (const auto &obj : objs) {
                req.add_object_keys(obj.first);
            }
            std::vector<std::string> res;
            if (elapsed >= timeout) {
                res = { req.object_keys().begin(), req.object_keys().end() };
                return res;
            }
            reqTimeoutDuration.Init(timeout - elapsed);
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            RollbackMultiMetaRspPb rsp;
            Status rc = RetryRollbackMultiMetaWhenMoving(api, req, rsp);
            if (rc.IsError()) {
                LOG(WARNING) << "RollbackMultiMeta failed, rc: " << rc.ToString();
                res = { req.object_keys().begin(), req.object_keys().end() };
                return res;
            }
            res = { rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
            return res;
        }));
    }
    std::vector<std::string> failedObjs;
    for (auto &future : futures) {
        const auto &objs = future.get();
        std::move(objs.begin(), objs.end(), std::back_inserter(failedObjs));
    }
    if (!failedObjs.empty()) {
        asyncRollbackManager_->AddBatch(failedObjs);
        LOG(WARNING) << "Rollback failed, objs will be rolled back asynchronously: " << VectorToString(failedObjs);
    }
}

Status WorkerOcServiceMultiPublishImpl::RetryCreateMultiMetaWhenMoving(std::shared_ptr<WorkerMasterOCApi> api,
                                                                       CreateMultiMetaReqPb &req,
                                                                       CreateMultiMetaRspPb &rsp)
{
    while (true) {
        RETURN_IF_NOT_OK(api->CreateMultiMeta(req, rsp, false));
        if (rsp.info().empty()) {
            return Status::OK();
        } else if (rsp.meta_is_moving()) {
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERNAL_MS_META_MOVING));
            continue;
        }
        return Status(K_SCALING, "The cluster is scaling, please try again.");
    }
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::RetryCreateMultiMetaPhaseTwoWhenMoving(std::shared_ptr<WorkerMasterOCApi> api,
                                                                               CreateMultiMetaPhaseTwoReqPb &req,
                                                                               CreateMultiMetaRspPb &rsp)
{
    while (true) {
        RETURN_IF_NOT_OK(api->CreateMultiMetaPhaseTwo(req, rsp));
        if (rsp.info().empty()) {
            return Status::OK();
        } else if (rsp.meta_is_moving()) {
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_INTERNAL_MS_META_MOVING));
            continue;
        }
        return Status(K_SCALING, "The cluster is scaling, please try again.");
    }
    return Status::OK();
}

void WorkerOcServiceMultiPublishImpl::CreateMultiMetaParallel(
    const std::vector<std::shared_ptr<worker::WorkerMasterOCApi>> &apis,
    std::vector<master::CreateMultiMetaReqPb> &reqs, std::vector<CreateMeta2PCRes> &respRes)
{
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    auto traceId = Trace::Instance().GetTraceID();
    std::vector<std::future<CreateMeta2PCRes>> futures;
    Timer timer;
    for (size_t i = 0; i < apis.size(); i++) {
        auto &api = apis[i];
        auto &req = reqs[i];
        futures.emplace_back(threadPool_->Submit([this, &api, &req, timeout, traceId, timer] {
            int64_t elapsed = timer.ElapsedMilliSecond();
            LOG_IF(WARNING, elapsed > THREAD_WAIT_TIME_MS) << FormatString(
                "Create meta threads statistics: %s, elapsed ms: %lld", threadPool_->GetStatistics(), elapsed);
            if (elapsed >= timeout) {
                return CreateMeta2PCRes{ Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc timeout" }, {}, api };
            }
            reqTimeoutDuration.Init(std::min(timeout - elapsed, MSET_TRANSACTION_TIMEOUT_MS));
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            CreateMultiMetaRspPb rsp;
            PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META);
            Status rc = RetryCreateMultiMetaWhenMoving(api, req, rsp);
            return CreateMeta2PCRes{ rc, rsp, api };
        }));
    }

    for (auto &future : futures) {
        respRes.emplace_back(future.get());
    }
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaParallel(
    const ObjGroupMap &objGroup, const std::vector<std::shared_ptr<WorkerMasterOCApi>> &apis,
    std::vector<CreateMultiMetaReqPb> &reqs)
{
    INJECT_POINT("worker.CreateMultiMetaParallel.begin");
    std::vector<CreateMeta2PCRes> respRes;
    respRes.reserve(reqs.size());
    CreateMultiMetaParallel(apis, reqs, respRes);
    Status lastRc;
    std::vector<std::shared_ptr<WorkerMasterOCApi>> preCommitted;
    for (auto &meta : respRes) {
        if (meta.rc.IsError()) {
            lastRc = lastRc.GetCode() == K_OC_KEY_ALREADY_EXIST ? lastRc : meta.rc;
            continue;
        }
        preCommitted.emplace_back(meta.api);
    }
    if (lastRc.IsError()) {
        RollbackMultiMetaReq(preCommitted, objGroup);
    }
    return lastRc;
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaSerial(
    const ObjGroupMap &objGroup, const std::vector<std::shared_ptr<WorkerMasterOCApi>> &apis,
    std::vector<CreateMultiMetaReqPb> &reqs)
{
    int64_t timeoutMs = std::min(reqTimeoutDuration.CalcRealRemainingTime(), MSET_TRANSACTION_TIMEOUT_MS);
    return RetryOnErrorRepent(
        timeoutMs,
        [&](int32_t) {
            std::vector<std::shared_ptr<WorkerMasterOCApi>> preCommitted;
            for (size_t i = 0; i < apis.size(); i++) {
                PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META);
                CreateMultiMetaRspPb rsp;
                Status rc = RetryCreateMultiMetaWhenMoving(apis[i], reqs[i], rsp);
                point.Record();
                if (rc.IsError()) {
                    RollbackMultiMetaReq(preCommitted, objGroup);
                    return rc;
                }
                preCommitted.emplace_back(apis[i]);
            }
            return Status::OK();
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_WORKER_DEADLOCK });
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaPhaseOne(
    const ObjGroupMap &objGroup, const std::vector<std::shared_ptr<SafeObjType>> &entries,
    const MultiPublishReqPb &pubReq)
{
    std::vector<std::shared_ptr<WorkerMasterOCApi>> apis(objGroup.size());
    std::vector<CreateMultiMetaReqPb> reqs(objGroup.size());
    int idx = 0;
    for (const auto &[masterAddr, objectKeys] : objGroup) {
        RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApiByAddr(masterAddr, etcdCM_, apis[idx]));
        FillMultiMetaReqPhaseOne(objectKeys, entries, pubReq, reqs[idx]);
        idx++;
    }

    Status rc = CreateMultiMetaParallel(objGroup, apis, reqs);
    int conflictCnt = 0;
    const int conflictTolerance = 2;
    auto isConflictFunc = [&conflictCnt](const Status &rc) {
        if (rc.GetCode() == K_TRY_AGAIN || rc.GetCode() == K_WORKER_DEADLOCK) {
            conflictCnt++;
            return true;
        }
        conflictCnt = 0;
        return false;
    };
    while ((IsRpcTimeout(rc) || isConflictFunc(rc)) && reqTimeoutDuration.CalcRealRemainingTime() > 0
           && conflictCnt <= conflictTolerance) {
        LOG(WARNING) << "CreateMultiMetaParallel failed with " << rc.ToString() << ", will retry.";
        const int64_t sleepUpperMs = 200;
        int64_t maxWaitInterval = std::min(sleepUpperMs, std::max(0L, reqTimeoutDuration.CalcRealRemainingTime()));
        uint64_t waitTime = RandomData().GetRandomUint64(0, maxWaitInterval * MILLITOMICR);
        std::this_thread::sleep_for(std::chrono::microseconds(waitTime));
        rc = CreateMultiMetaParallel(objGroup, apis, reqs);
    }
    if (isConflictFunc(rc)) {
        LOG(WARNING) << "Metadata creation conflicts, change from parallel to serial.";
        rc = CreateMultiMetaSerial(objGroup, apis, reqs);
    }
    return rc;
}

Status WorkerOcServiceMultiPublishImpl::Process2PCResults(std::vector<std::future<CreateMeta2PCRes>> &futures,
                                                          const ObjGroupMap &objGroup, std::vector<uint64_t> &versions)
{
    std::vector<std::shared_ptr<WorkerMasterOCApi>> needRollBack;
    Status lastRc;
    for (auto &future : futures) {
        auto res = future.get();
        needRollBack.emplace_back(res.api);
        if (res.rc.IsError()) {
            lastRc = lastRc.GetCode() == K_OC_KEY_ALREADY_EXIST ? lastRc : res.rc;
            continue;
        }
        const auto &objs = objGroup.at(res.api->GetHostPort());
        for (size_t i = 0; i < objs.size(); i++) {
            if (objs[i].second > versions.size()) {
                continue;
            }
            versions[objs[i].second] = res.rsp.version();
        }
    }
    if (lastRc.IsError()) {
        RollbackMultiMetaReq(needRollBack, objGroup);
    }
    return lastRc;
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaPhaseTwo(const ObjGroupMap &objGroup,
                                                                const MultiPublishReqPb &pubReq,
                                                                std::vector<uint64_t> &versions)
{
    std::vector<std::shared_ptr<WorkerMasterOCApi>> apis(objGroup.size());
    std::vector<std::vector<std::string>> objs(objGroup.size());
    int idx = 0;
    for (const auto &[masterAddr, ids] : objGroup) {
        RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApiByAddr(masterAddr, etcdCM_, apis[idx]));
        for (const auto &id : ids) {
            objs[idx].emplace_back(id.first);
        }
        idx++;
    }
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    auto traceId = Trace::Instance().GetTraceID();
    std::vector<std::future<CreateMeta2PCRes>> futures;
    Timer timer;
    for (size_t i = 0; i < apis.size(); i++) {
        futures.emplace_back(threadPool_->Submit([this, &pubReq, i, &apis, &objs, timeout, traceId, timer] {
            int64_t elapsed = timer.ElapsedMilliSecond();
            LOG_IF(WARNING, elapsed > THREAD_WAIT_TIME_MS) << FormatString(
                "Create meta threads statistics: %s, elapsed ms: %lld", threadPool_->GetStatistics(), elapsed);
            if (elapsed >= timeout) {
                return CreateMeta2PCRes{ Status{ K_RPC_DEADLINE_EXCEEDED, "Rpc timeout" }, {}, apis[i] };
            }
            reqTimeoutDuration.Init(std::min(timeout - elapsed, MSET_TRANSACTION_TIMEOUT_MS));
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            CreateMultiMetaPhaseTwoReqPb req;
            for (const auto &obj : objs[i]) {
                req.add_object_keys(obj);
            }
            req.set_address(localAddress_.ToString());
            req.set_write_mode(pubReq.write_mode());
            req.set_redirect(true);
            CreateMultiMetaRspPb rsp;
            PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META_PHASE_TWO);
            Status rc = RetryCreateMultiMetaPhaseTwoWhenMoving(apis[i], req, rsp);
            return CreateMeta2PCRes{ rc, rsp, apis[i] };
        }));
    }
    return Process2PCResults(futures, objGroup, versions);
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaToDistributedMaster(
    const std::vector<std::string> &objectKeys, const std::vector<std::shared_ptr<SafeObjType>> &entries,
    const MultiPublishReqPb &pubReq, std::vector<uint64_t> &versions)
{
    CHECK_FAIL_RETURN_STATUS(!asyncRollbackManager_->IsObjectsInRollBack(objectKeys), K_OC_KEY_ALREADY_EXIST,
                             "The object is being rolled back.");
    ObjGroupMap objGroup;
    std::stringstream str;
    for (size_t i = 0; i < objectKeys.size(); i++) {
        std::shared_ptr<WorkerMasterOCApi> api;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(objectKeys[i], etcdCM_, api),
                                         "Getting master api failed. ObjectKey = " + objectKeys[i]);
        auto addr = api->GetHostPort();
        str << " " << (objGroup[addr].empty() ? addr : "");
        objGroup[addr].emplace_back(objectKeys[i], i);
    }
    LOG(INFO) << FormatString("Create meta to masters [%s]", str.str());
    RETURN_IF_NOT_OK(CreateMultiMetaPhaseOne(objGroup, entries, pubReq));
    INJECT_POINT("worker.CreateMultiMetaPhaseTwo.delay");
    LOG(INFO) << FormatString("Create meta to  masters phase two");
    // If phase one causes an asynchronous rollback, we need return.
    CHECK_FAIL_RETURN_STATUS(!asyncRollbackManager_->IsObjectsInRollBack(objectKeys), K_OC_KEY_ALREADY_EXIST,
                             "The object is being rolled back.");
    RETURN_IF_NOT_OK(CreateMultiMetaPhaseTwo(objGroup, pubReq, versions));
    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaToDistributedMasterNtx(
    const std::vector<std::string> &objectKeys, std::vector<size_t> &successIndex,
    const std::vector<std::shared_ptr<SafeObjType>> &entries, const MultiPublishReqPb &pubReq,
    CreateMultiMetaRspPb &totalResp, std::vector<uint64_t> &versions)
{
    ObjGroupMap objGroup;
    std::unordered_map<std::string, std::shared_ptr<WorkerMasterOCApi>> workerAddrToApi;
    for (const auto index : successIndex) {
        std::shared_ptr<WorkerMasterOCApi> api;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(objectKeys[index], etcdCM_, api),
                                         "Getting master api failed. ObjectKey = " + objectKeys[index]);
        auto addr = api->GetHostPort();
        objGroup[addr].emplace_back(objectKeys[index], index);
        workerAddrToApi[addr] = api;
    }

    std::vector<std::shared_ptr<WorkerMasterOCApi>> apis(objGroup.size());
    std::vector<CreateMultiMetaReqPb> createReqs(objGroup.size());

    int idx = 0;
    for (const auto &[masterAddr, objInfos] : objGroup) {
        auto &req = createReqs[idx];
        apis[idx] = workerAddrToApi[masterAddr];
        ConstructCreateReq(objInfos, entries, pubReq, req);
        idx++;
    }

    std::vector<CreateMeta2PCRes> respRes;
    respRes.reserve(createReqs.size());
    CreateMultiMetaParallel(apis, createReqs, respRes);

    CHECK_FAIL_RETURN_STATUS(respRes.size() == createReqs.size(), K_RUNTIME_ERROR,
                             "The object size and the versions is not equal");

    for (size_t index = 0; index < respRes.size(); index++) {
        auto &resp = respRes[index].rsp;
        for (const auto &failedId : resp.failed_object_keys()) {
            totalResp.add_failed_object_keys(failedId);
        }
        LOG_IF_ERROR(respRes[index].rc, "Get error with createMeta");
        for (const auto &obj : objGroup.at(respRes[index].api->GetHostPort())) {
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
    if (pubReq.istx()) {
        // Optimize the scenario when worker1 set key1 and key2, while worker2 set key2 and key1, if both request
        // arrives the master concurrently, master generate meta for key1 of the worker1 and key2 of the worker2
        // initially, then master try to process key2 of the worker1 and key1 of the worker2, it will find the keys have
        // already been occupied that will caused both request failed.
        std::sort(req.mutable_metas()->begin(), req.mutable_metas()->end(),
                  [](const ObjectBaseInfoPb &fir, const ObjectBaseInfoPb &sec) {
                      return fir.object_key() < sec.object_key();
                  });
    }
    req.set_address(localAddress_.ToString());
    req.set_istx(pubReq.istx());
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

void WorkerOcServiceMultiPublishImpl::ConstructCreateReq(const std::vector<std::string> &objectInfos,
                                                         const std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                         const MultiPublishReqPb &pubReq, CreateMultiMetaReqPb &req)
{
    for (size_t i = 0; i < objectInfos.size(); i++) {
        ObjectBaseInfoPb meta;
        meta.set_object_key(objectInfos[i]);
        meta.set_data_size((*entries[i])->GetDataSize());
        if (pubReq.object_info(i).blob_sizes_size() > 0) {
            meta.mutable_device_info()->mutable_blob_sizes()->Add(pubReq.object_info(i).blob_sizes().begin(),
                                                                  pubReq.object_info(i).blob_sizes().end());
        }
        req.mutable_metas()->Add(std::move(meta));
    }
    ConstructCreateReqCommon(*entries[0], pubReq, req);
}

Status WorkerOcServiceMultiPublishImpl::CreateMultiMetaToCentralMaster(
    const std::vector<std::string> &objectKeys, const std::vector<std::shared_ptr<SafeObjType>> &entries,
    const MultiPublishReqPb &pubReq, std::vector<uint64_t> &versions)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(objectKeys[0], etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, CreateMetadataToMaster failed");
    LOG(INFO) << FormatString("Create meta to master[%s]", workerMasterApi->GetHostPort());

    CreateMultiMetaReqPb req;
    std::vector<std::pair<std::string, size_t>> objectInfos;
    ConstructCreateReq(objectKeys, entries, pubReq, req);
    PerfPoint point(PerfKey::WORKER_CREATE_MULTI_META);

    CreateMultiMetaRspPb resp;
    Status status =
        RetryWhenDeadlock([&workerMasterApi, &req, &resp] { return workerMasterApi->CreateMultiMeta(req, resp); });
    point.Record();
    for (auto &version : versions) {
        version = resp.version();
    }
    return status;
}

void WorkerOcServiceMultiPublishImpl::UpdateObjectAfterCreatingMeta(std::vector<std::string> &objectKeys,
                                                                    std::vector<std::shared_ptr<SafeObjType>> entries,
                                                                    const std::vector<uint64_t> &versions,
                                                                    std::vector<size_t> &successIndex)
{
    auto rollBackPersistenceIfFail = [this, &versions, &objectKeys](const Status &rc, const ObjectKV &kv, int idx) {
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Multiple set fails to save object %s to l2cache.", objectKeys[idx]);
            std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
                workerMasterApiManager_->GetWorkerMasterApi(kv.GetObjKey(), etcdCM_);
            master::RollbackMultiMetaReqPb req;
            master::RollbackMultiMetaRspPb resp;
            req.set_persistence_only(true);
            req.add_object_keys(kv.GetObjKey());
            req.add_versions(versions[idx]);
            workerMasterApi->RollbackMultiMeta(req, resp);
        }
    };

    std::vector<std::string> objectKeysSucc;
    objectKeysSucc.reserve(successIndex.size());
    for (auto index = successIndex.begin(); index != successIndex.end(); index++) {
        auto i = *index;
        ObjectKV objectKV(objectKeys[i], *entries[i]);
        objectKV.GetObjEntry()->SetCreateTime(versions[i]);
        // Save object to L2 cache
        if ((*entries[i])->IsWriteThroughMode()) {
            if (IsSupportL2Storage(supportL2Storage_)) {
                Status rc = SaveBinaryObjectToPersistence(objectKV);
                rollBackPersistenceIfFail(rc, objectKV, i);
            }
        }
        if ((*entries[i])->IsWriteBackMode() && IsSupportL2Storage(supportL2Storage_)) {
            std::future<Status> future;
            LOG_IF_ERROR(asyncSendManager_->Add(objectKeys[i], entries[i], future),
                         FormatString("Multiple set fails to add object %s to async L2cache manager.", objectKeys[i]));
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
                         FormatString("Multiple set fails to delete spilled object %s from disk.", objectKeys[i]));
        }
        objectKeysSucc.emplace_back(objectKeys[i]);
    }
    threadPool_->Execute([this, objectKeys = std::move(objectKeysSucc)]() {
        Status rc;
        for (const auto &key : objectKeys) {
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
    const MultiPublishReqPb &req, std::vector<size_t> &successIndex, std::vector<size_t> &failedIndex,
    std::vector<std::string> &objectKeys, std::vector<std::shared_ptr<SafeObjType>> &entries, Status &lastRc)
{
    CreateMultiMetaRspPb rsp;

    std::unordered_map<std::string, size_t> objectIndexMap;
    for (const auto index : successIndex) {
        objectIndexMap[objectKeys[index]] = index;
    }

    std::vector<uint64_t> versions(objectKeys.size());
    RETURN_IF_NOT_OK(CreateMultiMetaToDistributedMasterNtx(objectKeys, successIndex, entries, req, rsp, versions));

    std::vector<std::string> failedObjectKey(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    std::set<size_t> sortedFailedIndex;
    auto lambdaFunc = [&objectIndexMap, &failedIndex](const std::string &key) -> size_t {
        auto it = objectIndexMap.find(key);
        if (it == objectIndexMap.end()) {
            LOG(ERROR) << "CreateMeta error, get error key which not in request : " << key;
            return 0;
        }
        failedIndex.emplace_back(it->second);
        return it->second;
    };
    std::transform(failedObjectKey.begin(), failedObjectKey.end(),
                   std::inserter(sortedFailedIndex, sortedFailedIndex.begin()), lambdaFunc);

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sortedFailedIndex.size() == failedObjectKey.size(), K_RUNTIME_ERROR,
                                         "Got unexpected key");
    // successIndex is sorted
    auto itSuccess = successIndex.begin();
    auto itFailed = sortedFailedIndex.begin();
    while (itSuccess != successIndex.end() && itFailed != sortedFailedIndex.end()) {
        if (*itSuccess < *itFailed) {
            ++itSuccess;
        } else if (*itSuccess > *itFailed) {
            ++itFailed;
        } else {
            itSuccess = successIndex.erase(itSuccess);
            ++itFailed;
        }
    }

    if (rsp.has_last_rc()) {
        Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
        lastRc = recvRc.IsOk() ? lastRc : recvRc;
    }
    UpdateObjectAfterCreatingMeta(objectKeys, entries, versions, successIndex);

    return Status::OK();
}

Status WorkerOcServiceMultiPublishImpl::BatchLockForSet(const std::vector<std::string> &objectKeys,
                                                        std::vector<bool> &isInserts,
                                                        std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                        std::vector<size_t> &successIndex)
{
    size_t keyNum = objectKeys.size();
    // If client1 lock key1, key2 and key3, while worker2 lock ke3, key2 and key1, it will cause dead lock,
    // so sort them according to the object before locking the obejcts. And the lock should be got one by one,
    // it can't be skip the middle object and get the next object, or it will also cause dead lock.
    std::map<std::string, size_t> objectToIdx;
    for (size_t i = 0; i < keyNum; ++i) {
        objectToIdx[objectKeys[i]] = i;
    }
    std::vector<bool> isFinish(keyNum, false);
    std::function<Status()> GetEntryLock = [&](void) {
        for (auto itr = objectToIdx.begin(); itr != objectToIdx.end(); ++itr) {
            if (isFinish[itr->second]) {
                continue;
            }
            std::shared_ptr<SafeObjType> entry;
            bool isInsert;
            RETURN_IF_NOT_OK(objectTable_->ReserveGetAndLock(itr->first, entry, isInsert, true));
            isInserts[itr->second] = isInsert;
            isFinish[itr->second] = true;
            entries[itr->second] = entry;
            successIndex.emplace_back(itr->second);
        }
        return Status::OK();
    };
    return RetryWhenDeadlock(GetEntryLock);
}

void WorkerOcServiceMultiPublishImpl::BatchUnlockForSet(std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                        const std::vector<size_t> &successIndex)
{
    for (auto index : successIndex) {
        if (entries[index] != nullptr) {
            entries[index]->WUnlock();
        }
    }
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
                                                           std::vector<size_t> &failedIndex, MultiPublishRspPb &resp)
{
    for (auto index = failedIndex.begin(); index != failedIndex.end(); index++) {
        auto i = *index;
        resp.add_failed_object_keys(objectKeys[i]);
        BatchRollBackEntriesImpl(objectKeys[i], ifInserts[i], entries[i]);
    }
}

Status WorkerOcServiceMultiPublishImpl::BatchLockForSetNtx(const std::vector<std::string> &objectKeys,
                                                           const ExistenceOptPb &existence,
                                                           std::vector<bool> &isInserts,
                                                           std::vector<std::shared_ptr<SafeObjType>> &entries,
                                                           std::vector<size_t> &successIndex,
                                                           std::vector<size_t> &failedIndex, Status &lastRc)
{
    size_t keyNum = objectKeys.size();
    std::map<std::string, size_t> objectToIdx;
    for (size_t i = 0; i < keyNum; ++i) {
        if (objectToIdx.find(objectKeys[i]) != objectToIdx.end()) {
            continue;
        }
        objectToIdx[objectKeys[i]] = i;
    }
    std::vector<bool> isFinish(keyNum, false);
    std::function<Status()> GetEntryLock = [&](void) {
        for (auto itr = objectToIdx.begin(); itr != objectToIdx.end(); ++itr) {
            if (isFinish[itr->second]) {
                continue;
            }
            std::shared_ptr<SafeObjType> entry;
            bool isInsert;
            Status ret = objectTable_->ReserveGetAndLock(objectKeys[itr->second], entry, isInsert,
                                                         existence == ExistenceOptPb::NX);
            if (ret.IsOk()) {
                isInserts[itr->second] = isInsert;
                isFinish[itr->second] = true;
                entries[itr->second] = entry;
                successIndex.emplace_back(itr->second);
                continue;
            }
            if (ret.GetCode() != K_WORKER_DEADLOCK) {
                LOG(ERROR) << "lock for object: " << objectKeys[itr->second] << " failed, status: " << ret.ToString();
                failedIndex.emplace_back(itr->second);
                isFinish[itr->second] = true;
                lastRc = ret;
                continue;
            }
            if (ret.GetCode() == K_WORKER_DEADLOCK) {
                return ret;
            }
        }
        return Status::OK();
    };
    return RetryWhenDeadlock(GetEntryLock);
}

}  // namespace object_cache
}  // namespace datasystem
