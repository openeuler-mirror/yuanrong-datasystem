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
 * Description: Defines the worker service processing global reference process.
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_global_reference_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

WorkerOcServiceGlobalReferenceImpl::WorkerOcServiceGlobalReferenceImpl(
    WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
    std::shared_ptr<ObjectGlobalRefTable> globalRefTable, std::shared_ptr<AkSkManager> akSkManager,
    HostPort &localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
      etcdCM_(etcdCM),
      globalRefTable_(std::move(globalRefTable)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(localAddress)
{
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    if (!req.remote_client_id().empty()) {
        return GIncreaseRefWithRemoteClientId(req, resp);
    }
    workerOperationTimeCost.Clear();
    Timer timer;
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_GINCREASEREF);
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "[Ref] GIncreaseRef client id: " << req.address() << ", object list: " << VectorToString(objectKeys);
    INJECT_POINT("worker.GIncrease_ref_failure");
    std::vector<std::string> failIncIds;
    std::vector<std::string> firstIncIds;
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    BatchGRefLock(objectKeys, true, lockedEntries);
    Raii unlockAll([&lockedEntries]() { BatchGRefUnlock(lockedEntries); });

    Status lastErr = globalRefTable_->GIncreaseRef(req.address(), objectKeys, failIncIds, firstIncIds);
    LOG_IF_ERROR(lastErr, "[Ref] GIncreaseRef in worker failed");
    if (!firstIncIds.empty()) {
        Status status = UpdateMasterForFirstIds(req, firstIncIds, failIncIds);
        if (status.IsError()) {
            lastErr = status;
        } else if (!failIncIds.empty()) {
            // Need modify later
            lastErr = Status(StatusCode::K_RUNTIME_ERROR, "failIncIds is not empty.");
        }
    }
    if (!failIncIds.empty()) {
        for (const auto &objectKey : failIncIds) {
            std::string failedId;
            TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKey, failedId);
            resp.add_failed_object_keys(failedId);
        }
    }
    resp.mutable_last_rc()->set_error_code(lastErr.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastErr.GetMsg());
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(objectKeys);
    posixPoint.Record(lastErr.GetCode(), std::to_string(0), reqParam, lastErr.GetMsg());
    workerOperationTimeCost.Append("Total GIncreaseRef", timer.ElapsedMilliSecond());

    bool partlySuccess = !failIncIds.empty() && (objectKeys.size() > failIncIds.size());
    LOG(INFO) << FormatString("[Ref] GIncreaseRef finish with status: %s. The operations of worker GIncreaseRef %s",
                              lastErr.ToString(), workerOperationTimeCost.GetInfo());
    return partlySuccess ? Status::OK() : lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    workerOperationTimeCost.Clear();
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "[Ref] GDecreaseRef client id: " << req.address() << ", object list: " << VectorToString(objectKeys);
    INJECT_POINT("worker.GDecreaseRef.before");
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_GDECREASEREF);
    std::vector<std::string> failedObjectKeys;
    Status rc = RetryWhenDeadlock([this, &objectKeys, &req, &failedObjectKeys] {
        if (!failedObjectKeys.empty()) {
            objectKeys = std::move(failedObjectKeys);
        }
        if (!req.remote_client_id().empty()) {
            return GDecreaseRefWithLockWithRemoteClientId(objectKeys, req.remote_client_id(), failedObjectKeys);
        }
        return GDecreaseRefWithLock(objectKeys, req.address(), failedObjectKeys);
    });

    if (!failedObjectKeys.empty()) {
        LOG(WARNING) << "[Ref] GDecreaseRef failed list: " << VectorToString(failedObjectKeys);
        for (auto &objectKey : failedObjectKeys) {
            std::string failedId;
            TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKey, failedId);
            resp.add_failed_object_keys(failedId);
        }
    }
    resp.mutable_last_rc()->set_error_code(rc.GetCode());
    resp.mutable_last_rc()->set_error_msg(rc.GetMsg());
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(objectKeys);
    reqParam.remoteClientId = req.remote_client_id();
    posixPoint.Record(rc.GetCode(), std::to_string(0), reqParam, rc.GetMsg());
    LOG(INFO) << FormatString("[Ref] GDecreaseRef finish with status: %s. The operations %s", rc.ToString(),
                              workerOperationTimeCost.GetInfo());
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    LOG(INFO) << "[Ref] ReleaseGRefs object list: " << req.remote_client_id();
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_RELEASEGREFS);
    HostPort masterAddr;
    if (etcdCM_ == nullptr) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    RETURN_IF_NOT_OK(etcdCM_->GetMasterAddr(req.remote_client_id(), masterAddr));
    if (masterAddr != localAddress_) {
        RETURN_IF_NOT_OK(etcdCM_->CheckConnection(masterAddr));
    }
    auto api = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    CHECK_FAIL_RETURN_STATUS(api != nullptr, StatusCode::K_INVALID,
                             "Getting master api failed. masterAddrs: " + masterAddr.ToString());
    master::ReleaseGRefsReqPb releaseReq;
    releaseReq.set_address(localAddress_.ToString());
    releaseReq.set_remote_client_id(req.remote_client_id());
    master::ReleaseGRefsRspPb releaseRsp;
    Status rc = api->ReleaseGRefs(releaseReq, releaseRsp);
    resp.mutable_last_rc()->set_error_code(rc.GetCode());
    resp.mutable_last_rc()->set_error_msg(rc.GetMsg());
    RequestParam reqParam;
    reqParam.remoteClientId = req.remote_client_id();
    posixPoint.Record(rc.GetCode(), std::to_string(0), reqParam, rc.GetMsg());
    LOG(INFO) << FormatString("[Ref] ReleaseGRefs finish with status: %s", rc.ToString());
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req,
                                                             QueryGlobalRefNumRspCollectionPb &rsp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto namespaceUris = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "worker qurey global ref number begin, namespaceUris: " << VectorToString(namespaceUris);
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_QUERY_GLOBAL_REF_NUM);
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(namespaceUris);
    // Group ObjectKeys by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(namespaceUris);
    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> &currentObjectKeys = item.second;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "hash master get failed, QueryGlobal failed");
        QueryGlobalRefNumReqPb currentReq;
        currentReq.set_redirect(true);
        *currentReq.mutable_object_keys() = { currentObjectKeys.begin(), currentObjectKeys.end() };
        std::function<Status(QueryGlobalRefNumReqPb &, QueryGlobalRefNumRspCollectionPb &)> func =
            [&workerMasterApi](QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp) {
                return workerMasterApi->QueryGlobalRefNum(req, rsp);
            };
        std::function<void(QueryGlobalRefNumRspCollectionPb &, QueryGlobalRefNumRspCollectionPb &)> mergeFunc =
            [](QueryGlobalRefNumRspCollectionPb &srcRsp, QueryGlobalRefNumRspCollectionPb &destRsp) {
                destRsp.mutable_objs_glb_refs()->MergeFrom(srcRsp.objs_glb_refs());
            };
        Status rc = WaitForRedirectWhenRefMoving(currentReq, rsp, workerMasterApi, func, mergeFunc);
        if (rc.IsError()) {
            posixPoint.Record(rc.GetCode(), std::to_string(0), reqParam, rc.GetMsg());
            LOG(ERROR) << "workerMasterApi qurey global ref number failed.";
            return rc;
        }
    }
    posixPoint.Record(StatusCode::K_OK, std::to_string(0), reqParam);
    workerOperationTimeCost.Append("Total QueryGlobalRefNum", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString(
        "worker query global ref number end, namespaceUris: %s. The operations of worker QueryGlobalRefNum %s",
        VectorToString(namespaceUris), workerOperationTimeCost.GetInfo());
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseRefWithRemoteClientId(const GIncreaseReqPb &req,
                                                                          GIncreaseRspPb &resp)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    workerOperationTimeCost.Clear();
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_GINCREASEREF);
    INJECT_POINT("worker.GIncrease_ref_failure");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "[Ref] GIncreaseRefWithRemoteClientId object list: " << VectorToString(objectKeys)
              << ", remoteClientId:" << req.remote_client_id();
    std::vector<std::string> failIncIds;
    Status lastErr;
    Status status = GIncreaseMasterRef(req, objectKeys, failIncIds);
    if (status.IsError()) {
        lastErr = status;
    } else if (!failIncIds.empty()) {
        lastErr = Status(StatusCode::K_RUNTIME_ERROR, "failIncIds is not empty.");
    }
    if (!failIncIds.empty()) {
        for (const auto &objectKey : failIncIds) {
            std::string failedId;
            TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKey, failedId);
            resp.add_failed_object_keys(failedId);
        }
    }
    resp.mutable_last_rc()->set_error_code(lastErr.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastErr.GetMsg());

    bool partlySuccess = !failIncIds.empty() && (objectKeys.size() > failIncIds.size());
    RequestParam reqParam;
    reqParam.objectKey = objectKeysToString(objectKeys);
    reqParam.remoteClientId = req.remote_client_id();
    posixPoint.Record(lastErr.GetCode(), std::to_string(0), reqParam, lastErr.GetMsg());
    LOG(INFO) << FormatString("[Ref] GIncreaseRefWithRemoteClientId finish with status: %s, The operations %s",
                              lastErr.ToString(), workerOperationTimeCost.GetInfo());
    return partlySuccess ? Status::OK() : lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseRefWithLockWithRemoteClientId(
    const std::vector<std::string> &objectKeys, const std::string &remoteClientId, std::vector<std::string> &failDecIds)
{
    VLOG(1) << "GDecRefWithClientId objectKeys size: " << objectKeys.size() << ",remoteClientId:" << remoteClientId;
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    std::vector<std::string> sendToMasterIds;
    std::vector<std::string> failedLockIds;
    Timer timer;
    Status lastErr = BatchLockWithInsert(objectKeys, lockedEntries, sendToMasterIds, failedLockIds);
    auto elapsedMs = static_cast<uint64_t>(timer.ElapsedMilliSecondAndReset());
    workerOperationTimeCost.Append("BatchLockWithInsert", elapsedMs);

    if (sendToMasterIds.empty()) {
        LOG(ERROR) << "[Ref] All objects lock failed: " << lastErr.ToString();
        failDecIds = std::move(failedLockIds);
        return lastErr;
    }

    Raii unlockAll([&lockedEntries, this]() {
        BatchUnlock(lockedEntries);
    });

    std::unordered_set<std::string> unAliveIds;
    Status status = GDecreaseMasterRef(remoteClientId, sendToMasterIds, unAliveIds, failDecIds);
    if (status.IsError()) {
        lastErr = status;
    } else if (!failDecIds.empty()) {
        // Need modify later
        lastErr = Status(StatusCode::K_RUNTIME_ERROR, "failDecIds is not empty.");
    }

    for (const auto &data : lockedEntries) {
        auto &entry = *(data.second);
        const auto &objectKey = data.first;
        ObjectKV objectKV(objectKey, entry);
        // To add object lock we constructed a metadata with size 0, so need to release it after decrease.
        if ((unAliveIds.find(objectKey) != unAliveIds.end() || entry->GetDataSize() == 0) && entry.Get() != nullptr) {
            LOG(INFO) << "ClearObject";
            Status rc = ClearObject(objectKV);
            if (rc.IsOk()) {
                continue;
            }
            LOG(ERROR) << FormatString("[ObjectKey %s] ClearObject failed, error: %s.", objectKey, lastErr.ToString());
            failDecIds.emplace_back(objectKey);
            lastErr = rc;
        }
    }
    failDecIds.insert(failDecIds.end(), std::make_move_iterator(failedLockIds.begin()),
                      std::make_move_iterator(failedLockIds.end()));
    VLOG(1) << "GDecreaseRefWithLockWithRemoteClientId end";
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRef(const GIncreaseReqPb &req,
                                                              const std::vector<std::string> &firstIncIds,
                                                              std::vector<std::string> &failIncIds)
{
    const std::string &remoteClientId = req.remote_client_id();
    // Group ObjectKeys by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(firstIncIds);
    Status lastErr;
    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> &currentIncIds = item.second;
        Status res = etcdCM_->CheckConnection(masterAddr);
        if (res.IsError()) {
            LOG(ERROR) << FormatString("The master %s of [%s] cannot be connected: %s", masterAddr.ToString(),
                                       VectorToString(currentIncIds), res.ToString());
            lastErr = Status(K_RUNTIME_ERROR, "Cannot connect to master.");
            (void)failIncIds.insert(failIncIds.end(), currentIncIds.begin(), currentIncIds.end());
        } else {
            std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
                workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
            if (workerMasterApi == nullptr) {
                LOG(WARNING) << "get master api failed. masterAddr=" << masterAddr.ToString();
                continue;
            }
            master::GIncreaseReqPb newReq;
            *newReq.mutable_object_keys() = { currentIncIds.begin(), currentIncIds.end() };
            newReq.set_address(localAddress_.ToString());
            newReq.set_remote_client_id(remoteClientId);
            newReq.set_redirect(true);
            master::GIncreaseRspPb rsp;
            std::function<Status(master::GIncreaseReqPb &, master::GIncreaseRspPb &)> func =
                [&workerMasterApi](master::GIncreaseReqPb &req, master::GIncreaseRspPb &rsp) {
                    return workerMasterApi->GIncreaseMasterRef(req, rsp);
                };
            std::function<void(master::GIncreaseRspPb &, master::GIncreaseRspPb &)> mergeFunc =
                [](master::GIncreaseRspPb &srcRsp, master::GIncreaseRspPb &desRsp) {
                    desRsp.mutable_failed_object_keys()->MergeFrom(srcRsp.failed_object_keys());
                };
            Status masterResult = WaitForRedirectWhenRefMoving(newReq, rsp, workerMasterApi, func, mergeFunc);
            if (masterResult.IsError()) {
                LOG(ERROR) << "[Ref] GDecreaseRef " << masterResult.ToString();
                (void)failIncIds.insert(failIncIds.end(), currentIncIds.begin(), currentIncIds.end());
                lastErr = masterResult;
                continue;
            }
            Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
            (void)failIncIds.insert(failIncIds.end(), rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
            if (recvRc.IsError()) {
                LOG(ERROR) << "GIncreaseMasterRef response " << LogHelper::IgnoreSensitive(rsp);
                lastErr = recvRc;
            }
        }
    }
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseMasterRef(const std::string &remoteClientId,
                                                              const std::vector<std::string> &finishDecIds,
                                                              std::unordered_set<std::string> &unAliveIds,
                                                              std::vector<std::string> &failDecIds)
{
    INJECT_POINT("worker.gdecrease");
    // Group ObjectKeys by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(finishDecIds);
    Status status = Status::OK();

    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> &currentIncIds = item.second;
        Status res = etcdCM_->CheckConnection(masterAddr);
        master::GDecreaseReqPb req;
        *req.mutable_object_keys() = { currentIncIds.begin(), currentIncIds.end() };
        req.set_address(localAddress_.ToString());
        req.set_remote_client_id(remoteClientId);
        req.set_redirect(true);
        master::GDecreaseRspPb rsp;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (res.IsOk() && workerMasterApi != nullptr) {
            std::function<Status(master::GDecreaseReqPb &, master::GDecreaseRspPb &)> func =
                [&workerMasterApi](master::GDecreaseReqPb &req, master::GDecreaseRspPb &rsp) {
                    return workerMasterApi->GDecreaseMasterRef(req, rsp);
                };
            std::function<void(master::GDecreaseRspPb &, master::GDecreaseRspPb &)> mergeFunc =
                [](master::GDecreaseRspPb &srcRsp, master::GDecreaseRspPb &desRsp) {
                    desRsp.mutable_no_ref_ids()->MergeFrom(srcRsp.no_ref_ids());
                    desRsp.mutable_failed_object_keys()->MergeFrom(srcRsp.failed_object_keys());
                };
            res = WaitForRedirectWhenRefMoving(req, rsp, workerMasterApi, func, mergeFunc);
        }
        if (res.IsError()) {
            LOG(ERROR) << "GDecreaseMasterRef failed on " << masterAddr.ToString() << " with " << res.ToString();
            (void)failDecIds.insert(failDecIds.end(), currentIncIds.begin(), currentIncIds.end());
            status = res;
            continue;
        }
        (void)unAliveIds.insert(rsp.no_ref_ids().begin(), rsp.no_ref_ids().end());
        (void)failDecIds.insert(failDecIds.end(), rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
        Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
        if (recvRc.IsError()) {
            LOG(ERROR) << "GDecreaseMasterRef response " << LogHelper::IgnoreSensitive(rsp);
            status = recvRc;
            continue;
        }
        currentIncIds.clear();
    }
    return status;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRefWithLock(
    std::function<bool(const std::string &)> matchFunc, std::string masterAddr)
{
    std::unordered_map<std::string, std::unordered_set<std::string>> refTable;
    std::vector<std::string> increaseMasterObjs;
    globalRefTable_->GetAllRef(refTable);
    for (const auto &objRef : refTable) {
        if (matchFunc(objRef.first)) {
            increaseMasterObjs.emplace_back(objRef.first);
        }
    }
    RETURN_OK_IF_TRUE(increaseMasterObjs.empty());

    LOG(INFO) << "Gincrease master ref with lock, master addr: " << masterAddr
              << " , object size: " << increaseMasterObjs.size();
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    BatchGRefLock(increaseMasterObjs, true, lockedEntries);
    Raii unlockAll([&lockedEntries]() { BatchGRefUnlock(lockedEntries); });
    HostPort masterAddress;

    RETURN_IF_NOT_OK(masterAddress.ParseString(masterAddr));
    master::GIncreaseReqPb req;
    *req.mutable_object_keys() = { increaseMasterObjs.begin(), increaseMasterObjs.end() };
    req.set_address(localAddress_.ToString());
    req.set_redirect(false);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    master::GIncreaseRspPb rsp;
    HostPort destAddr;
    RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(masterAddr, destAddr));
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(destAddr);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "workerMasterApi get failed");
    const int maxRetryCount = 3;
    int retryCount = 0;
    Status res;
    do {
        RETURN_IF_NOT_OK(etcdCM_->CheckConnection(masterAddress));
        res = workerMasterApi->GIncreaseMasterRef(req, rsp);
        if (res.IsOk() && !rsp.failed_object_keys().empty()) {
            retryCount++;
            LOG(INFO) << "retry for gincrease master ref failed object, objectSize: " << rsp.failed_object_keys_size();
            req.clear_object_keys();
            *req.mutable_object_keys() = { rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
            res = Status(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
        } else {
            break;
        }
    } while (retryCount < maxRetryCount);
    LOG_IF_ERROR(res, FormatString("GIncrease Ref fail masterAddr:%s, status:%s", masterAddr, res.ToString()));
    return res;
}

Status WorkerOcServiceGlobalReferenceImpl::UpdateMasterForFirstIds(const GIncreaseReqPb &req,
                                                                   const std::vector<std::string> &firstIncIds,
                                                                   std::vector<std::string> &failIncIds)
{
    // Group ObjectKeys by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(firstIncIds);
    // Send requests for each master
    Status lastErr;
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> &currentFirstIncIds = item.second;
        std::vector<std::string> rpcFailedIds;
        Status res = etcdCM_->CheckConnection(masterAddr);
        if (res.IsError()) {
            LOG(ERROR) << FormatString("The master %s of [%s] cannot be connected: %s", masterAddr.ToString(),
                                       VectorToString(currentFirstIncIds), res.ToString());
            lastErr = Status(K_RUNTIME_ERROR, "Cannot connect to master, check the network.");
            std::vector<std::string> temFailedIds;
            std::vector<std::string> temFirstIds;
            (void)globalRefTable_->GDecreaseRef(req.address(), currentFirstIncIds, temFailedIds, temFirstIds);
            (void)failIncIds.insert(failIncIds.end(), currentFirstIncIds.begin(), currentFirstIncIds.end());
        } else {
            Status masterResult = GIncreaseMasterRef(currentFirstIncIds, masterAddr, rpcFailedIds);
            if (masterResult.IsError()) {
                lastErr = masterResult;
                LOG(WARNING) << "[Ref] GDecreaseRef " << masterResult.ToString();
            }
            // rpc fail reset refcount
            if (!rpcFailedIds.empty()) {
                currentFirstIncIds.clear();
                std::vector<std::string> tmpFailIds;
                (void)globalRefTable_->GDecreaseRef(req.address(), rpcFailedIds, tmpFailIds, currentFirstIncIds);
                (void)failIncIds.insert(failIncIds.end(), tmpFailIds.begin(), tmpFailIds.end());
            }
        }
    }
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRef(const std::vector<std::string> &firstIncIds,
                                                              const HostPort &masterAddr,
                                                              std::vector<std::string> &failIncIds)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(workerMasterApi != nullptr, K_RUNTIME_ERROR, "GetWorkerMasterApi failed");
    master::GIncreaseReqPb newReq;
    *newReq.mutable_object_keys() = { firstIncIds.begin(), firstIncIds.end() };
    newReq.set_address(localAddress_.ToString());
    newReq.set_redirect(true);
    master::GIncreaseRspPb rsp;
    std::function<Status(master::GIncreaseReqPb &, master::GIncreaseRspPb &)> func =
        [&workerMasterApi](master::GIncreaseReqPb &req, master::GIncreaseRspPb &rsp) {
            return workerMasterApi->GIncreaseMasterRef(req, rsp);
        };
    std::function<void(master::GIncreaseRspPb &, master::GIncreaseRspPb &)> mergeFunc =
        [](master::GIncreaseRspPb &srcRsp, master::GIncreaseRspPb &desRsp) {
            desRsp.mutable_failed_object_keys()->MergeFrom(srcRsp.failed_object_keys());
        };
    Status masterResult = WaitForRedirectWhenRefMoving(newReq, rsp, workerMasterApi, func, mergeFunc);
    if (masterResult.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRef failed on " << masterAddr.ToString() << " with " << masterResult.ToString();
        failIncIds = { newReq.object_keys().begin(), newReq.object_keys().end() };
        return masterResult;
    }
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRef response " << LogHelper::IgnoreSensitive(rsp);
        failIncIds = { rsp.failed_object_keys().begin(), rsp.failed_object_keys().end() };
        return recvRc;
    }
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseRefWithLock(const std::vector<std::string> &objectKeys,
                                                                const std::string &clientId,
                                                                std::vector<std::string> &failDecIds)
{
    VLOG(1) << "GDecreaseRefWithLock begin, objectKeys size: " << objectKeys.size();
    std::vector<std::string> finishDecIds;
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    std::vector<std::string> sendToMasterIds;
    std::vector<std::string> failedLockIds;
    Timer timer;
    Status lastErr = BatchLockWithInsert(objectKeys, lockedEntries, sendToMasterIds, failedLockIds);
    auto elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecondAndReset()));
    workerOperationTimeCost.Append("BatchLockWithInsert", elapsedMs);

    if (sendToMasterIds.empty()) {
        LOG(ERROR) << "[Ref] All objects lock failed: " << lastErr.ToString();
        failDecIds = std::move(failedLockIds);
        return lastErr;
    }

    BatchGRefLock(lockedEntries);
    elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecondAndReset()));
    workerOperationTimeCost.Append("BatchGRefLock", elapsedMs);
    Raii unlockAll([&lockedEntries, this]() {
        BatchGRefUnlock(lockedEntries);
        BatchUnlock(lockedEntries);
    });

    lastErr = globalRefTable_->GDecreaseRef(clientId, sendToMasterIds, failDecIds, finishDecIds);
    LOG_IF_ERROR(lastErr, "[Ref] GDecreaseRef in worker failed");

    std::unordered_set<std::string> unAliveIds;
    if (!finishDecIds.empty()) {
        Status status = UpdateMasterForFinishedIds(clientId, finishDecIds, unAliveIds, failDecIds);
        if (status.IsError()) {
            lastErr = status;
        } else if (!failDecIds.empty()) {
            // Need modify later
            lastErr = Status(StatusCode::K_RUNTIME_ERROR, "failDecIds is not empty.");
        }
    }

    for (const auto &data : lockedEntries) {
        auto &entry = *(data.second);
        const auto &objectKey = data.first;
        ObjectKV objectKV(objectKey, entry);
        INJECT_POINT("entry.setNull", [&entry] () {
            entry.SetRealObject(nullptr);
            return Status::OK();
        });
        // To add object lock we constructed a metadata with size 0, so need to release it after decrease.
        if (entry.Get() != nullptr && (unAliveIds.find(objectKey) != unAliveIds.end() || entry->GetDataSize() == 0)) {
            Status rc = ClearObject(objectKV);
            if (rc.IsOk()) {
                continue;
            }
            LOG(ERROR) << FormatString("[ObjectKey %s] ClearObject failed, error: %s.", objectKey, lastErr.ToString());
            failDecIds.emplace_back(objectKey);
            lastErr = rc;
        }
    }

    failDecIds.insert(failDecIds.end(), std::make_move_iterator(failedLockIds.begin()),
                      std::make_move_iterator(failedLockIds.end()));
    VLOG(1) << "GDecreaseRefWithLock end";
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::UpdateMasterForFinishedIds(const std::string &clientId,
                                                                      const std::vector<std::string> &finishDecIds,
                                                                      std::unordered_set<std::string> &unAliveIds,
                                                                      std::vector<std::string> &failDecIds)
{
    // Group ObjectKeys by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(finishDecIds);
    Status status = Status::OK();

    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> &currentFinishDecIds = item.second;
        std::vector<std::string> rpcFailedIds;
        std::unordered_set<std::string> rpcUnAliveIds;
        Status res = etcdCM_->CheckConnection(masterAddr);
        if (res.IsError()) {
            rpcFailedIds = currentFinishDecIds;
        }
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (res.IsOk() && workerMasterApi != nullptr) {
            res = GDecreaseMasterRef("", currentFinishDecIds, rpcUnAliveIds, rpcFailedIds);
        }
        if (!rpcUnAliveIds.empty()) {
            unAliveIds.insert(rpcUnAliveIds.begin(), rpcUnAliveIds.end());
        }
        // rpc fail reset refcount
        if (!rpcFailedIds.empty()) {
            currentFinishDecIds.clear();
            std::vector<std::string> tmpFailIds;
            std::vector<std::string> tempFinishDecIds;
            (void)globalRefTable_->GIncreaseRef(clientId, rpcFailedIds, tmpFailIds, tempFinishDecIds);
            (void)failDecIds.insert(failDecIds.end(), rpcFailedIds.begin(), rpcFailedIds.end());
        }
        if (res.IsError()) {
            status = res;
        }
    }
    return status;
}

void WorkerOcServiceGlobalReferenceImpl::BatchGRefLock(
    const std::vector<std::string> &objectKeys, bool insertable,
    std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    std::set<std::string> needLock{ objectKeys.begin(), objectKeys.end() };
    for (const auto &objectKey : needLock) {
        std::shared_ptr<SafeObjType> entry;
        (void)objectTable_->Get(objectKey, entry);
        if (entry == nullptr && insertable) {
            entry = std::make_shared<SafeObjType>();
            SetEmptyObjectEntry(objectKey, *entry);
            objectTable_->InsertOrGet(objectKey, entry);
        }
        if (entry == nullptr) {
            continue;
        }
        entry->GRefLock();
        lockedEntries.emplace(objectKey, std::move(entry));
    }
}

void WorkerOcServiceGlobalReferenceImpl::BatchGRefLock(
    std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    for (auto &entry : lockedEntries) {
        const auto &safeObj = entry.second;
        if (safeObj != nullptr) {
            safeObj->GRefLock();
        }
    }
}

void WorkerOcServiceGlobalReferenceImpl::BatchGRefUnlock(
    const std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    for (auto &entry : lockedEntries) {
        entry.second->GRefUnlock();
    }
}

Status WorkerOcServiceGlobalReferenceImpl::IncNestedRef(const std::vector<std::string> &nestedObjectKeys)
{
    // Send notifications to owner masters
    // Get map of objectKeys grouped by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(nestedObjectKeys);

    // send requests for each master
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        const std::vector<std::string> &currentIds = item.second;
        master::GIncNestedRefReqPb req;
        *req.mutable_object_keys() = { currentIds.begin(), currentIds.end() };
        req.set_address(localAddress_.ToString());
        req.set_redirect(true);
        master::GIncNestedRefRspPb rsp;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "Hash master get failed, IncNestedRef failed");
        std::function<Status(master::GIncNestedRefReqPb &, master::GIncNestedRefRspPb &)> func =
            [&workerMasterApi](master::GIncNestedRefReqPb &req, master::GIncNestedRefRspPb &rsp) {
                return workerMasterApi->GIncNestedRef(req, rsp);
            };
        RETURN_IF_NOT_OK(WaitForRedirectWhenRefMoving(req, rsp, workerMasterApi, func));
    }
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::DecNestedRef(const std::vector<std::string> &nestedObjectKeys,
                                                        std::vector<std::string> &unAliveIds)
{
    // Get map of objectKeys grouped by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(nestedObjectKeys);

    // send requests for each master
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        LOG(INFO) << "master address nested ref is sending notification " << masterAddr.ToString();
        const std::vector<std::string> &currentIds = item.second;
        master::GDecNestedRefReqPb req;
        *req.mutable_object_keys() = { currentIds.begin(), currentIds.end() };
        req.set_address(localAddress_.ToString());
        req.set_redirect(true);
        master::GDecNestedRefRspPb rsp;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "Hash master get failed, DecNestedRef failed");

        int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
        remainingTime = remainingTime > 0 ? remainingTime : RPC_TIMEOUT;  // maybe call by local master
        reqTimeoutDuration.InitWithPositiveTime(remainingTime);
        req.set_timeout(remainingTime);
        std::function<Status(master::GDecNestedRefReqPb &, master::GDecNestedRefRspPb &)> func =
            [&workerMasterApi](master::GDecNestedRefReqPb &req, master::GDecNestedRefRspPb &rsp) {
                return workerMasterApi->GDecNestedRef(req, rsp);
            };
        std::function<void(master::GDecNestedRefRspPb &, master::GDecNestedRefRspPb &)> mergeFunc =
            [](master::GDecNestedRefRspPb &srcRsp, master::GDecNestedRefRspPb &desRsp) {
                desRsp.mutable_no_ref_ids()->MergeFrom(srcRsp.no_ref_ids());
            };
        RETURN_IF_NOT_OK(WaitForRedirectWhenRefMoving(req, rsp, workerMasterApi, func, mergeFunc));
        (void)unAliveIds.insert(unAliveIds.end(), rsp.no_ref_ids().begin(), rsp.no_ref_ids().end());
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
