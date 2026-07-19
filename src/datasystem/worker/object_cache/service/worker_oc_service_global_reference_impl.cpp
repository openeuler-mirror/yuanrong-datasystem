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
 * Description: Defines the worker service processing global reference process.
 */

#include "datasystem/worker/object_cache/service/worker_oc_service_global_reference_impl.h"

#include <iterator>
#include <unordered_set>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {
namespace {
constexpr int K_REF_MOVING_MAX_RETRY_TIMES = 10;

void AppendObjectKeys(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)
{
    (void)failedObjectKeys.insert(failedObjectKeys.end(), objectKeys.begin(), objectKeys.end());
}

void AppendObjectKeySet(const std::unordered_set<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys)
{
    (void)failedObjectKeys.insert(failedObjectKeys.end(), objectKeys.begin(), objectKeys.end());
}

void AppendObjectKeysExcept(const std::vector<std::string> &objectKeys, const std::vector<std::string> &excludedKeys,
                            std::vector<std::string> &targetKeys)
{
    std::unordered_set<std::string> excluded(excludedKeys.begin(), excludedKeys.end());
    for (const auto &objectKey : objectKeys) {
        if (excluded.find(objectKey) == excluded.end()) {
            targetKeys.emplace_back(objectKey);
        }
    }
}

template <typename Iterator>
void AppendObjectKeyRange(Iterator begin, Iterator end, std::vector<std::string> &failedObjectKeys)
{
    (void)failedObjectKeys.insert(failedObjectKeys.end(), begin, end);
}

template <typename GroupIterator>
void AppendRemainingRouteGroupKeys(GroupIterator iter, const GroupIterator &end,
                                   std::vector<std::string> &failedObjectKeys)
{
    for (; iter != end; ++iter) {
        AppendObjectKeys(iter->second, failedObjectKeys);
    }
}

Status LogGDecreaseMasterRefError(const HostPort &masterAddr, const Status &status)
{
    LOG(ERROR) << FormatString("GDecreaseMasterRef to master %s failed, err: %s", masterAddr.ToString(),
                               status.ToString());
    return status;
}

Status MakeRollbackFailureStatus(const Status &rollbackRc, const std::string &operation)
{
    if (rollbackRc.IsError()) {
        return rollbackRc;
    }
    return Status(K_RUNTIME_ERROR, operation + " rollback failed for some object keys.");
}
}  // namespace

bool WorkerOcServiceGlobalReferenceImpl::IsRefMovingRetry(const Status &status)
{
    return status.GetCode() == K_TRY_AGAIN && status.GetMsg() == REF_MOVING_RETRY_STATUS_MESSAGE;
}

Status WorkerOcServiceGlobalReferenceImpl::SleepForRefMovingRetry() const
{
    INJECT_POINT("WorkerOcServiceGlobalReferenceImpl.SleepForRefMovingRetry.beforeSleep");
    int64_t remainingTimeMs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTimeMs > 0, K_RPC_DEADLINE_EXCEEDED,
                             "GRef redirect wait timeout while ref is moving.");
    SleepForMetaMovingRetry(std::min(REF_MOVING_RETRY_SLEEP_TIME_MS, remainingTimeMs));
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::PrepareNextRefMovingRetry(const std::vector<std::string> &retryObjectKeys,
                                                                     std::vector<std::string> &pendingObjectKeys,
                                                                     int &retryCount, const std::string &lostKeysMsg)
{
    CHECK_FAIL_RETURN_STATUS(!retryObjectKeys.empty(), K_RUNTIME_ERROR, lostKeysMsg);
    CHECK_FAIL_RETURN_STATUS(retryCount < K_REF_MOVING_MAX_RETRY_TIMES, K_RPC_DEADLINE_EXCEEDED,
                             "GRef metadata moving retry exceeded max retry times.");
    ++retryCount;
    pendingObjectKeys = retryObjectKeys;
    return SleepForRefMovingRetry();
}

Status WorkerOcServiceGlobalReferenceImpl::RollbackIncreaseRefForRetry(const ClientKey &clientId,
                                                                       const std::vector<std::string> &objectKeys,
                                                                       std::vector<std::string> &retryIncIds,
                                                                       std::vector<std::string> &failIncIds)
{
    std::vector<std::string> tmpFailIds;
    std::vector<std::string> tmpFirstIds;
    Status rollbackRc = globalRefTable_->GDecreaseRef(clientId, objectKeys, tmpFailIds, tmpFirstIds);
    if (rollbackRc.IsOk() && tmpFailIds.empty()) {
        AppendObjectKeys(objectKeys, retryIncIds);
        return Status::OK();
    }
    LOG(ERROR) << "Rollback GIncreaseRef for ref moving retry failed, status: " << rollbackRc.ToString()
               << ", failed keys: " << VectorToString(tmpFailIds);
    if (rollbackRc.IsError()) {
        AppendObjectKeys(objectKeys, failIncIds);
        return rollbackRc;
    }
    AppendObjectKeys(tmpFailIds, failIncIds);
    AppendObjectKeysExcept(objectKeys, tmpFailIds, retryIncIds);
    return MakeRollbackFailureStatus(rollbackRc, "GIncreaseRef");
}

Status WorkerOcServiceGlobalReferenceImpl::RollbackDecreaseRefForRetry(const ClientKey &clientId,
                                                                       const std::vector<std::string> &objectKeys,
                                                                       std::vector<std::string> &retryDecIds,
                                                                       std::vector<std::string> &failDecIds)
{
    std::vector<std::string> tmpFailIds;
    std::vector<std::string> tmpFinishDecIds;
    Status rollbackRc = globalRefTable_->GIncreaseRef(clientId, objectKeys, tmpFailIds, tmpFinishDecIds);
    if (rollbackRc.IsOk() && tmpFailIds.empty()) {
        AppendObjectKeys(objectKeys, retryDecIds);
        return Status::OK();
    }
    LOG(ERROR) << "Rollback GDecreaseRef for ref moving retry failed, status: " << rollbackRc.ToString()
               << ", failed keys: " << VectorToString(tmpFailIds);
    if (rollbackRc.IsError()) {
        AppendObjectKeys(objectKeys, failDecIds);
        return rollbackRc;
    }
    AppendObjectKeys(tmpFailIds, failDecIds);
    AppendObjectKeysExcept(objectKeys, tmpFailIds, retryDecIds);
    return MakeRollbackFailureStatus(rollbackRc, "GDecreaseRef");
}

WorkerOcServiceGlobalReferenceImpl::WorkerOcServiceGlobalReferenceImpl(
    WorkerOcServiceCrudParam &initParam,
    std::shared_ptr<ObjectGlobalRefTable<ClientKey>> globalRefTable, std::shared_ptr<AkSkManager> akSkManager,
    HostPort &localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
      globalRefTable_(std::move(globalRefTable)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(localAddress)
{
}

void WorkerOcServiceGlobalReferenceImpl::FillFailedObjectKeys(
    GIncreaseRspPb &resp, const std::vector<std::string> &failIds)
{
    for (const auto &objectKey : failIds) {
        std::string failedId;
        TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKey, failedId);
        resp.add_failed_object_keys(failedId);
    }
}

Status WorkerOcServiceGlobalReferenceImpl::TryGIncreaseRef(const GIncreaseReqPb &req,
                                                           const std::vector<std::string> &pendingObjectKeys,
                                                           std::vector<std::string> &failIncIds,
                                                           std::vector<std::string> &retryIncIds)
{
    std::vector<std::string> firstIncIds;
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    BatchGRefLock(pendingObjectKeys, true, lockedEntries);
    Raii unlockAll([&lockedEntries]() { BatchGRefUnlock(lockedEntries); });

    Status lastErr = globalRefTable_->GIncreaseRef(ClientKey::Intern(req.client_id()), pendingObjectKeys, failIncIds,
                                                   firstIncIds);
    LOG_IF_ERROR(lastErr, "[Ref] GIncreaseRef in worker failed");
    if (firstIncIds.empty()) {
        return lastErr;
    }
    Status status = UpdateMasterForFirstIds(req, firstIncIds, failIncIds, &retryIncIds);
    if (status.IsError()) {
        return status;
    }
    if (!failIncIds.empty()) {
        return Status(StatusCode::K_RUNTIME_ERROR, "failIncIds is not empty.");
    }
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::RetryGIncreaseRef(const GIncreaseReqPb &req,
                                                             const std::vector<std::string> &objectKeys,
                                                             std::vector<std::string> &failIncIds)
{
    std::vector<std::string> accumulatedFailIncIds;
    std::vector<std::string> pendingObjectKeys = objectKeys;
    Status lastErr;
    bool retryRefMoving = true;
    int retryCount = 0;
    while (retryRefMoving) {
        failIncIds.clear();
        std::vector<std::string> retryIncIds;
        lastErr = TryGIncreaseRef(req, pendingObjectKeys, failIncIds, retryIncIds);
        retryRefMoving = IsRefMovingRetry(lastErr);
        if (retryRefMoving) {
            accumulatedFailIncIds.insert(accumulatedFailIncIds.end(), failIncIds.begin(), failIncIds.end());
            RETURN_IF_NOT_OK(PrepareNextRefMovingRetry(retryIncIds, pendingObjectKeys, retryCount,
                                                       "GRef metadata moving retry lost its retry object keys."));
        }
    }
    failIncIds.insert(failIncIds.end(), accumulatedFailIncIds.begin(), accumulatedFailIncIds.end());
    if (lastErr.IsOk() && !failIncIds.empty()) {
        return Status(StatusCode::K_RUNTIME_ERROR, "failIncIds is not empty.");
    }
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    if (!req.remote_client_id().empty()) {
        return GIncreaseRefWithRemoteClientId(req, resp);
    }
    ScopedRequestContext ctx;
    Timer timer;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_GINCREASEREF);
    access.ObjectKeysRef(req.object_keys());
    std::string tenantId;
    Status authRc = worker::Authenticate(akSkManager_, req, tenantId);
    if (authRc.IsError()) {
        LOG(ERROR) << "Authenticate failed. Detail: " << authRc.ToString();
        access.Result(authRc).Record();
        return authRc;
    }
    if (!Validator::IsBatchSizeUnderLimit(req.object_keys_size())) {
        LOG(ERROR) << "invalid object size";
        Status rc(StatusCode::K_INVALID, __LINE__, __FILE__, "invalid object size");
        access.Result(rc).Record();
        return rc;
    }
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    access.ObjectKeysRef(objectKeys);
    LOG(INFO) << "[Ref] GIncreaseRef client id: " << req.address() << ", object list: " << VectorToString(objectKeys);
    INJECT_POINT("worker.GIncrease_ref_failure");
    std::vector<std::string> failIncIds;
    Status lastErr = RetryGIncreaseRef(req, objectKeys, failIncIds);
    FillFailedObjectKeys(resp, failIncIds);
    resp.mutable_last_rc()->set_error_code(lastErr.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastErr.GetMsg());
    access.Result(lastErr).Record();
    GetWorkerTimeCost().Append("Total GIncreaseRef", timer.ElapsedMilliSecond());

    bool partlySuccess = !failIncIds.empty() && (objectKeys.size() > failIncIds.size());
    LOG(INFO) << FormatString("[Ref] GIncreaseRef finish with status: %s. The operations of worker GIncreaseRef %s",
                              lastErr.ToString(), GetWorkerTimeCost().GetInfo());
    return partlySuccess ? Status::OK() : lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    ScopedRequestContext ctx;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "[Ref] GDecreaseRef client id: " << req.address() << ", object list: " << VectorToString(objectKeys);
    INJECT_POINT("worker.GDecreaseRef.before");
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_GDECREASEREF);
    access.ObjectKeysRef(objectKeys).RemoteClientId(req.remote_client_id());
    std::vector<std::string> failedObjectKeys;
    Status rc = RetryWhenDeadlock([this, &objectKeys, &req, &failedObjectKeys] {
        if (!failedObjectKeys.empty()) {
            objectKeys = std::move(failedObjectKeys);
        }
        if (!req.remote_client_id().empty()) {
            return GDecreaseRefWithLockWithRemoteClientId(objectKeys, req.remote_client_id(), failedObjectKeys);
        }
        return GDecreaseRefWithLock(objectKeys, ClientKey::Intern(req.client_id()), failedObjectKeys);
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
    access.Result(rc).Record();
    LOG(INFO) << FormatString("[Ref] GDecreaseRef finish with status: %s. The operations %s", rc.ToString(),
                              GetWorkerTimeCost().GetInfo());
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    LOG(INFO) << "[Ref] ReleaseGRefs object list: " << req.remote_client_id();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_RELEASEGREFS);
    access.RemoteClientId(req.remote_client_id());
    HostPort masterAddr;
    if (metadataRouteResolver_ == nullptr || endpointPolicy_ == nullptr) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    RETURN_IF_NOT_OK(metadataRouteResolver_->ResolveOwner(req.remote_client_id(), masterAddr));
    if (masterAddr != localAddress_) {
        RETURN_IF_NOT_OK(endpointPolicy_->CheckEndpoint(masterAddr, allowDirectoryLag_));
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
    access.Result(rc).Record();
    LOG(INFO) << FormatString("[Ref] ReleaseGRefs finish with status: %s", rc.ToString());
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req,
                                                             QueryGlobalRefNumRspCollectionPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto namespaceUris = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "worker qurey global ref number begin, namespaceUris: " << VectorToString(namespaceUris);
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_QUERY_GLOBAL_REF_NUM);
    access.ObjectKeysRef(namespaceUris);
    // Group ObjectKeys by master
    auto grouped = metadataRouteResolver_->GroupOwners(namespaceUris);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first;
        std::vector<std::string> &currentObjectKeys = item.second;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            Status rc(K_RUNTIME_ERROR, __LINE__, __FILE__, "hash master get failed, QueryGlobal failed");
            access.Result(rc).Record();
            return rc;
        }
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
            access.Result(rc).Record();
            LOG(ERROR) << "workerMasterApi qurey global ref number failed. master addr: " << masterAddr.ToString();
            return rc;
        }
    }
    access.Result(StatusCode::K_OK).Record();
    GetWorkerTimeCost().Append("Total QueryGlobalRefNum", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString(
        "worker query global ref number end, namespaceUris: %s. The operations of worker QueryGlobalRefNum %s",
        VectorToString(namespaceUris), GetWorkerTimeCost().GetInfo());
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseRefWithRemoteClientId(const GIncreaseReqPb &req,
                                                                          GIncreaseRspPb &resp)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    ScopedRequestContext ctx;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_GINCREASEREF);
    INJECT_POINT("worker.GIncrease_ref_failure");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    access.ObjectKeysRef(objectKeys).RemoteClientId(req.remote_client_id());
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
    access.Result(lastErr).Record();
    LOG(INFO) << FormatString("[Ref] GIncreaseRefWithRemoteClientId finish with status: %s, The operations %s",
                              lastErr.ToString(), GetWorkerTimeCost().GetInfo());
    return partlySuccess ? Status::OK() : lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::ClearObjectsAfterGDecrease(
    const std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries,
    const std::unordered_set<std::string> &unAliveIds, const std::unordered_set<std::string> &retryDecIdSet,
    std::vector<std::string> &failDecIds, const Status &lastErr, bool injectSetNull)
{
    Status result = lastErr;
    for (const auto &data : lockedEntries) {
        auto &entry = *(data.second);
        const auto &objectKey = data.first;
        ObjectKV objectKV(objectKey, entry);
        if (injectSetNull) {
            INJECT_POINT("entry.setNull", [&entry]() {
                entry.SetRealObject(nullptr);
                return Status::OK();
            });
        }
        if (retryDecIdSet.find(objectKey) != retryDecIdSet.end() || entry.Get() == nullptr
            || (unAliveIds.find(objectKey) == unAliveIds.end() && entry->GetDataSize() != 0)) {
            continue;
        }
        Status rc = ClearObject(objectKV);
        if (rc.IsOk()) {
            continue;
        }
        LOG(ERROR) << FormatString("[ObjectKey %s] ClearObject failed, error: %s.", objectKey, result.ToString());
        failDecIds.emplace_back(objectKey);
        result = rc;
    }
    return result;
}

Status WorkerOcServiceGlobalReferenceImpl::TryGDecreaseRefWithRemoteClientId(
    const std::vector<std::string> &pendingObjectKeys, const std::string &remoteClientId,
    std::unordered_set<std::string> &accumulatedFailDecIds, std::vector<std::string> &failDecIds,
    std::vector<std::string> &retryDecIds)
{
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    std::vector<std::string> sendToMasterIds;
    std::vector<std::string> failedLockIds;
    Timer timer;
    Status lastErr = BatchLockWithInsert(pendingObjectKeys, lockedEntries, sendToMasterIds, failedLockIds);
    GetWorkerTimeCost().Append("BatchLockWithInsert", static_cast<uint64_t>(timer.ElapsedMilliSecondAndReset()));
    if (sendToMasterIds.empty()) {
        LOG(ERROR) << "[Ref] All objects lock failed: " << lastErr.ToString();
        failDecIds = std::move(failedLockIds);
        AppendObjectKeySet(accumulatedFailDecIds, failDecIds);
        return lastErr;
    }
    Raii unlockAll([&lockedEntries, this]() { BatchUnlock(lockedEntries); });
    std::unordered_set<std::string> unAliveIds;
    Status status = GDecreaseMasterRef(remoteClientId, sendToMasterIds, unAliveIds, failDecIds, false);
    if (status.IsError()) {
        lastErr = status;
        if (IsRefMovingRetry(status)) {
            retryDecIds = failDecIds;
            failDecIds.clear();
        }
    } else if (!failDecIds.empty()) {
        lastErr = Status(StatusCode::K_RUNTIME_ERROR, "failDecIds is not empty.");
    }
    std::unordered_set<std::string> retryDecIdSet(retryDecIds.begin(), retryDecIds.end());
    lastErr = ClearObjectsAfterGDecrease(lockedEntries, unAliveIds, retryDecIdSet, failDecIds, lastErr, false);
    if (IsRefMovingRetry(lastErr)) {
        accumulatedFailDecIds.insert(failedLockIds.begin(), failedLockIds.end());
        return lastErr;
    }
    failDecIds.insert(failDecIds.end(), std::make_move_iterator(failedLockIds.begin()),
                      std::make_move_iterator(failedLockIds.end()));
    AppendObjectKeySet(accumulatedFailDecIds, failDecIds);
    return lastErr.IsOk() && !failDecIds.empty() ? Status(StatusCode::K_RUNTIME_ERROR, "failDecIds is not empty.")
                                                : lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseRefWithLockWithRemoteClientId(
    const std::vector<std::string> &objectKeys, const std::string &remoteClientId, std::vector<std::string> &failDecIds)
{
    VLOG(1) << "GDecRefWithClientId objectKeys size: " << objectKeys.size() << ",remoteClientId:" << remoteClientId;
    std::vector<std::string> pendingObjectKeys = objectKeys;
    std::unordered_set<std::string> accumulatedFailDecIds;
    Status lastErr;
    bool retryRefMoving = true;
    int retryCount = 0;
    while (retryRefMoving) {
        failDecIds.clear();
        std::vector<std::string> retryDecIds;
        lastErr = TryGDecreaseRefWithRemoteClientId(pendingObjectKeys, remoteClientId, accumulatedFailDecIds,
                                                    failDecIds, retryDecIds);
        retryRefMoving = IsRefMovingRetry(lastErr);
        if (retryRefMoving) {
            RETURN_IF_NOT_OK(PrepareNextRefMovingRetry(retryDecIds, pendingObjectKeys, retryCount,
                                                       "GRef metadata moving retry lost its retry object keys."));
        }
    }
    VLOG(1) << "GDecreaseRefWithLockWithRemoteClientId end";
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRef(const GIncreaseReqPb &req,
                                                              const std::vector<std::string> &firstIncIds,
                                                              std::vector<std::string> &failIncIds,
                                                              bool waitWhenRefMoving)
{
    const std::string &remoteClientId = req.remote_client_id();
    auto grouped = metadataRouteResolver_->GroupOwners(firstIncIds);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    Status lastErr;
    for (auto item = objKeysGrpByMaster.begin(); item != objKeysGrpByMaster.end(); ++item) {
        const HostPort &masterAddr = item->first;
        std::vector<std::string> &currentIncIds = item->second;
        Status res = endpointPolicy_->CheckEndpoint(masterAddr, allowDirectoryLag_);
        if (res.IsError()) {
            LOG(ERROR) << FormatString("The master %s of [%s] cannot be connected: %s", masterAddr.ToString(),
                                       VectorToString(currentIncIds), res.ToString());
            lastErr = Status(K_RUNTIME_ERROR, "Cannot connect to master.");
            AppendObjectKeys(currentIncIds, failIncIds);
            if (!waitWhenRefMoving) {
                AppendRemainingRouteGroupKeys(std::next(item), objKeysGrpByMaster.end(), failIncIds);
                return lastErr;
            }
            continue;
        }
        Status masterResult =
            SendGIncreaseMasterRefToMaster(masterAddr, remoteClientId, currentIncIds, failIncIds, waitWhenRefMoving);
        if (masterResult.IsError()) {
            lastErr = masterResult;
            if (!waitWhenRefMoving || IsRefMovingRetry(masterResult)) {
                AppendRemainingRouteGroupKeys(std::next(item), objKeysGrpByMaster.end(), failIncIds);
                return lastErr;
            }
        }
    }
    return lastErr;
}

void WorkerOcServiceGlobalReferenceImpl::BuildGIncreaseMasterReq(const std::vector<std::string> &objectKeys,
                                                                 const std::string &remoteClientId,
                                                                 master::GIncreaseReqPb &req) const
{
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    req.set_address(localAddress_.ToString());
    req.set_remote_client_id(remoteClientId);
    req.set_redirect(true);
}

Status WorkerOcServiceGlobalReferenceImpl::SendGIncreaseMasterRefToMaster(const HostPort &masterAddr,
                                                                          const std::string &remoteClientId,
                                                                          const std::vector<std::string> &currentIncIds,
                                                                          std::vector<std::string> &failIncIds,
                                                                          bool waitWhenRefMoving)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    if (workerMasterApi == nullptr) {
        LOG(WARNING) << "get master api failed. masterAddr=" << masterAddr.ToString();
        AppendObjectKeys(currentIncIds, failIncIds);
        return Status(K_RUNTIME_ERROR, "GetWorkerMasterApi failed");
    }

    master::GIncreaseReqPb newReq;
    BuildGIncreaseMasterReq(currentIncIds, remoteClientId, newReq);
    master::GIncreaseRspPb rsp;
    std::function<Status(master::GIncreaseReqPb &, master::GIncreaseRspPb &)> func =
        [&workerMasterApi](master::GIncreaseReqPb &req, master::GIncreaseRspPb &rsp) {
            return workerMasterApi->GIncreaseMasterRef(req, rsp);
        };
    std::function<void(master::GIncreaseRspPb &, master::GIncreaseRspPb &)> mergeFunc =
        [](master::GIncreaseRspPb &srcRsp, master::GIncreaseRspPb &desRsp) {
            desRsp.mutable_failed_object_keys()->MergeFrom(srcRsp.failed_object_keys());
        };
    Status masterResult = WaitForRedirectWhenRefMoving(newReq, rsp, workerMasterApi, func, mergeFunc,
                                                       waitWhenRefMoving);
    if (masterResult.IsError()) {
        LOG(ERROR) << FormatString("[Ref] GIncreaseRef to master %s failed, err: %s", masterAddr.ToString(),
                                   masterResult.ToString());
        AppendObjectKeyRange(newReq.object_keys().begin(), newReq.object_keys().end(), failIncIds);
        return masterResult;
    }

    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    size_t failSizeBeforeResponse = failIncIds.size();
    AppendObjectKeyRange(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end(), failIncIds);
    if (recvRc.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRef response " << LogHelper::IgnoreSensitive(rsp);
        if (IsRefMovingRetry(recvRc) && failIncIds.size() == failSizeBeforeResponse) {
            AppendObjectKeyRange(newReq.object_keys().begin(), newReq.object_keys().end(), failIncIds);
        }
    }
    return recvRc;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseMasterRef(const std::string &remoteClientId,
                                                              const std::vector<std::string> &finishDecIds,
                                                              std::unordered_set<std::string> &unAliveIds,
                                                              std::vector<std::string> &failDecIds,
                                                              bool waitWhenRefMoving)
{
    INJECT_POINT("worker.gdecrease");
    auto grouped = metadataRouteResolver_->GroupOwners(finishDecIds);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    Status status = Status::OK();

    for (auto item = objKeysGrpByMaster.begin(); item != objKeysGrpByMaster.end(); ++item) {
        Status res = SendGDecreaseMasterRefToMaster(item->first, remoteClientId, item->second, unAliveIds, failDecIds,
                                                    waitWhenRefMoving);
        if (res.IsError()) {
            status = res;
            if (!waitWhenRefMoving || IsRefMovingRetry(res)) {
                AppendRemainingRouteGroupKeys(std::next(item), objKeysGrpByMaster.end(), failDecIds);
                return status;
            }
            continue;
        }
        item->second.clear();
    }
    return status;
}

Status WorkerOcServiceGlobalReferenceImpl::SendGDecreaseMasterRefToMaster(
    const HostPort &masterAddr, const std::string &remoteClientId, const std::vector<std::string> &currentIncIds,
    std::unordered_set<std::string> &unAliveIds, std::vector<std::string> &failDecIds, bool waitWhenRefMoving)
{
    master::GDecreaseReqPb req;
    *req.mutable_object_keys() = { currentIncIds.begin(), currentIncIds.end() };
    req.set_address(localAddress_.ToString());
    req.set_remote_client_id(remoteClientId);
    req.set_redirect(true);
    master::GDecreaseRspPb rsp;
    Status res = endpointPolicy_->CheckEndpoint(masterAddr, allowDirectoryLag_);
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    if (res.IsOk() && workerMasterApi == nullptr) {
        res = Status(K_RPC_UNAVAILABLE, "GetWorkerMasterApi failed for global reference decrease.");
    }
    if (res.IsOk()) {
        std::function<Status(master::GDecreaseReqPb &, master::GDecreaseRspPb &)> func =
            [&workerMasterApi](master::GDecreaseReqPb &req, master::GDecreaseRspPb &rsp) {
                return workerMasterApi->GDecreaseMasterRef(req, rsp);
            };
        std::function<void(master::GDecreaseRspPb &, master::GDecreaseRspPb &)> mergeFunc =
            [](master::GDecreaseRspPb &srcRsp, master::GDecreaseRspPb &desRsp) {
                desRsp.mutable_no_ref_ids()->MergeFrom(srcRsp.no_ref_ids());
                desRsp.mutable_failed_object_keys()->MergeFrom(srcRsp.failed_object_keys());
            };
        res = WaitForRedirectWhenRefMoving(req, rsp, workerMasterApi, func, mergeFunc, waitWhenRefMoving);
    }
    if (res.IsError()) {
        LOG(ERROR) << "GDecreaseMasterRef failed on " << masterAddr.ToString() << " with " << res.ToString();
        AppendObjectKeyRange(req.object_keys().begin(), req.object_keys().end(), failDecIds);
        return res;
    }
    (void)unAliveIds.insert(rsp.no_ref_ids().begin(), rsp.no_ref_ids().end());
    size_t failSizeBeforeResponse = failDecIds.size();
    AppendObjectKeyRange(rsp.failed_object_keys().begin(), rsp.failed_object_keys().end(), failDecIds);
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    if (recvRc.IsError()) {
        LOG(ERROR) << "GDecreaseMasterRef response " << LogHelper::IgnoreSensitive(rsp);
        if (IsRefMovingRetry(recvRc) && failDecIds.size() == failSizeBeforeResponse) {
            AppendObjectKeyRange(req.object_keys().begin(), req.object_keys().end(), failDecIds);
        }
    }
    return recvRc;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRefWithLock(
    std::function<bool(const std::string &)> matchFunc,
    std::vector<std::string> &increaseFailedIds)
{
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    std::vector<std::string> increaseMasterObjs;
    globalRefTable_->GetAllRef(refTable);
    for (const auto &objRef : refTable) {
        if (matchFunc(objRef.first)) {
            increaseMasterObjs.emplace_back(objRef.first);
        }
    }
    RETURN_OK_IF_TRUE(increaseMasterObjs.empty());

    LOG(INFO) << "Gincrease master ref with lock, object size: " << increaseMasterObjs.size();
    GIncreaseReqPb req;
    Status rc;
    bool retryRefMoving = true;
    int retryCount = 0;
    while (retryRefMoving) {
        increaseFailedIds.clear();
        {
            std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
            BatchGRefLock(increaseMasterObjs, true, lockedEntries);
            Raii unlockAll([&lockedEntries]() { BatchGRefUnlock(lockedEntries); });
            RETURN_OK_IF_TRUE(increaseMasterObjs.empty());
            rc = GIncreaseMasterRef(req, increaseMasterObjs, increaseFailedIds, false);
        }
        retryRefMoving = IsRefMovingRetry(rc);
        if (retryRefMoving) {
            RETURN_IF_NOT_OK(PrepareNextRefMovingRetry(increaseFailedIds, increaseMasterObjs, retryCount,
                                                       "GRef metadata moving retry lost its retry object keys."));
        }
    }
    if (rc.IsError() && increaseFailedIds.empty()) {
        increaseFailedIds.insert(increaseFailedIds.end(), increaseMasterObjs.begin(), increaseMasterObjs.end());
    }
    CHECK_FAIL_RETURN_STATUS(
        rc.IsOk() && increaseFailedIds.empty(), rc.IsError() ? rc.GetCode() : K_RUNTIME_ERROR,
        rc.IsError() ? rc.GetMsg()
                     : "GIncreaseMasterRef failed object size: " + std::to_string(increaseFailedIds.size()));
    return rc;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRefWithLock(
    const HostPort &masterAddr, const std::vector<std::string> &objectKeys, std::vector<std::string> &increaseFailedIds)
{
    RETURN_OK_IF_TRUE(objectKeys.empty());
    std::unordered_set<std::string> objectKeySet(objectKeys.begin(), objectKeys.end());
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    std::vector<std::string> increaseMasterObjs;
    globalRefTable_->GetAllRef(refTable);
    for (const auto &objRef : refTable) {
        if (objectKeySet.count(objRef.first) != 0) {
            increaseMasterObjs.emplace_back(objRef.first);
        }
    }
    RETURN_OK_IF_TRUE(increaseMasterObjs.empty());

    LOG(INFO) << "Gincrease master ref with lock to master " << masterAddr.ToString()
              << ", object size: " << increaseMasterObjs.size();
    Status rc;
    bool retryRefMoving = true;
    int retryCount = 0;
    while (retryRefMoving) {
        increaseFailedIds.clear();
        {
            std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
            BatchGRefLock(increaseMasterObjs, true, lockedEntries);
            Raii unlockAll([&lockedEntries]() { BatchGRefUnlock(lockedEntries); });
            RETURN_OK_IF_TRUE(increaseMasterObjs.empty());
            rc = GIncreaseMasterRef(increaseMasterObjs, masterAddr, increaseFailedIds, false);
        }
        retryRefMoving = IsRefMovingRetry(rc);
        if (retryRefMoving) {
            RETURN_IF_NOT_OK(PrepareNextRefMovingRetry(increaseFailedIds, increaseMasterObjs, retryCount,
                                                       "GRef metadata moving retry lost its retry object keys."));
        }
    }
    if (rc.IsError() && increaseFailedIds.empty()) {
        increaseFailedIds.insert(increaseFailedIds.end(), increaseMasterObjs.begin(), increaseMasterObjs.end());
    }
    CHECK_FAIL_RETURN_STATUS(
        rc.IsOk() && increaseFailedIds.empty(), rc.IsError() ? rc.GetCode() : K_RUNTIME_ERROR,
        rc.IsError() ? rc.GetMsg()
                     : "GIncreaseMasterRef failed object size: " + std::to_string(increaseFailedIds.size()));
    return rc;
}

Status WorkerOcServiceGlobalReferenceImpl::UpdateMasterForFirstIds(const GIncreaseReqPb &req,
                                                                   const std::vector<std::string> &firstIncIds,
                                                                   std::vector<std::string> &failIncIds,
                                                                   std::vector<std::string> *retryIncIds)
{
    // Group ObjectKeys by master
    auto grouped = metadataRouteResolver_->GroupOwners(firstIncIds);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    // Send requests for each master
    Status lastErr;
    auto clientId = ClientKey::Intern(req.client_id());
    for (auto item = objKeysGrpByMaster.begin(); item != objKeysGrpByMaster.end(); ++item) {
        const HostPort &masterAddr = item->first;
        std::vector<std::string> &currentFirstIncIds = item->second;
        std::vector<std::string> rpcFailedIds;
        Status res = endpointPolicy_->CheckEndpoint(masterAddr, allowDirectoryLag_);
        if (res.IsError()) {
            LOG(ERROR) << FormatString("The master %s of [%s] cannot be connected: %s", masterAddr.ToString(),
                                       VectorToString(currentFirstIncIds), res.ToString());
            lastErr = Status(K_RUNTIME_ERROR, "Cannot connect to master, check the network.");
            std::vector<std::string> temFailedIds;
            std::vector<std::string> temFirstIds;
            (void)globalRefTable_->GDecreaseRef(clientId, currentFirstIncIds, temFailedIds, temFirstIds);
            (void)failIncIds.insert(failIncIds.end(), currentFirstIncIds.begin(), currentFirstIncIds.end());
            continue;
        }
        Status masterResult = GIncreaseMasterRef(currentFirstIncIds, masterAddr, rpcFailedIds, false);
        if (masterResult.IsError()) {
            lastErr = masterResult;
            LOG(WARNING) << FormatString("[Ref] GIncreaseRef to master %s failed, err: %s", masterAddr.ToString(),
                                         masterResult.ToString());
        }
        if (rpcFailedIds.empty()) {
            continue;
        }
        currentFirstIncIds.clear();
        if (IsRefMovingRetry(masterResult) && retryIncIds != nullptr) {
            RETURN_IF_NOT_OK(RollbackIncreaseRefForRetry(clientId, rpcFailedIds, *retryIncIds, failIncIds));
            for (auto iter = std::next(item); iter != objKeysGrpByMaster.end(); ++iter) {
                RETURN_IF_NOT_OK(RollbackIncreaseRefForRetry(clientId, iter->second, *retryIncIds, failIncIds));
            }
            return masterResult;
        }
        std::vector<std::string> tmpFailIds;
        (void)globalRefTable_->GDecreaseRef(clientId, rpcFailedIds, tmpFailIds, currentFirstIncIds);
        (void)failIncIds.insert(failIncIds.end(), rpcFailedIds.begin(), rpcFailedIds.end());
    }
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GIncreaseMasterRef(const std::vector<std::string> &firstIncIds,
                                                              const HostPort &masterAddr,
                                                              std::vector<std::string> &failIncIds,
                                                              bool waitWhenRefMoving)
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
    Status masterResult = WaitForRedirectWhenRefMoving(newReq, rsp, workerMasterApi, func, mergeFunc,
                                                       waitWhenRefMoving);
    if (masterResult.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRef failed on " << masterAddr.ToString() << " with " << masterResult.ToString();
        failIncIds = { newReq.object_keys().begin(), newReq.object_keys().end() };
        return masterResult;
    }
    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());
    (void)failIncIds.insert(failIncIds.end(), rsp.failed_object_keys().begin(), rsp.failed_object_keys().end());
    if (recvRc.IsError()) {
        LOG(ERROR) << "GIncreaseMasterRef response " << LogHelper::IgnoreSensitive(rsp);
        if (IsRefMovingRetry(recvRc) && failIncIds.empty()) {
            failIncIds = { newReq.object_keys().begin(), newReq.object_keys().end() };
        }
        return recvRc;
    }
    if (!failIncIds.empty()) {
        LOG(ERROR) << "GIncreaseMasterRef response contains failed object keys: " << LogHelper::IgnoreSensitive(rsp);
        RETURN_STATUS(K_RUNTIME_ERROR, "GIncreaseMasterRef failed object size: " + std::to_string(failIncIds.size()));
    }
    return Status::OK();
}

Status WorkerOcServiceGlobalReferenceImpl::TryGDecreaseRefWithLock(
    const std::vector<std::string> &pendingObjectKeys, const ClientKey &clientId,
    std::unordered_set<std::string> &accumulatedFailDecIds, std::vector<std::string> &failDecIds,
    std::vector<std::string> &retryDecIds)
{
    std::vector<std::string> finishDecIds;
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    std::vector<std::string> sendToMasterIds;
    std::vector<std::string> failedLockIds;
    Timer timer;
    Status lastErr = BatchLockWithInsert(pendingObjectKeys, lockedEntries, sendToMasterIds, failedLockIds);
    auto elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecondAndReset()));
    GetWorkerTimeCost().Append("BatchLockWithInsert", elapsedMs);
    if (sendToMasterIds.empty()) {
        LOG(ERROR) << "[Ref] All objects lock failed: " << lastErr.ToString();
        failDecIds = std::move(failedLockIds);
        AppendObjectKeySet(accumulatedFailDecIds, failDecIds);
        return lastErr;
    }
    BatchGRefLock(lockedEntries);
    elapsedMs = static_cast<uint64_t>(std::round(timer.ElapsedMilliSecondAndReset()));
    GetWorkerTimeCost().Append("BatchGRefLock", elapsedMs);
    Raii unlockAll([&lockedEntries, this]() {
        BatchGRefUnlock(lockedEntries);
        BatchUnlock(lockedEntries);
    });
    lastErr = globalRefTable_->GDecreaseRef(clientId, sendToMasterIds, failDecIds, finishDecIds);
    LOG_IF_ERROR(lastErr, "[Ref] GDecreaseRef in worker failed");
    std::unordered_set<std::string> unAliveIds;
    if (!finishDecIds.empty()) {
        Status status = UpdateMasterForFinishedIds(clientId, finishDecIds, unAliveIds, failDecIds, &retryDecIds);
        if (status.IsError()) {
            lastErr = status;
        } else if (!failDecIds.empty()) {
            lastErr = Status(StatusCode::K_RUNTIME_ERROR, "failDecIds is not empty.");
        }
    }
    std::unordered_set<std::string> retryDecIdSet(retryDecIds.begin(), retryDecIds.end());
    lastErr = ClearObjectsAfterGDecrease(lockedEntries, unAliveIds, retryDecIdSet, failDecIds, lastErr, true);
    if (IsRefMovingRetry(lastErr)) {
        accumulatedFailDecIds.insert(failDecIds.begin(), failDecIds.end());
        accumulatedFailDecIds.insert(failedLockIds.begin(), failedLockIds.end());
        return lastErr;
    }
    failDecIds.insert(failDecIds.end(), std::make_move_iterator(failedLockIds.begin()),
                      std::make_move_iterator(failedLockIds.end()));
    AppendObjectKeySet(accumulatedFailDecIds, failDecIds);
    return lastErr.IsOk() && !failDecIds.empty() ? Status(StatusCode::K_RUNTIME_ERROR, "failDecIds is not empty.")
                                                : lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::GDecreaseRefWithLock(const std::vector<std::string> &objectKeys,
                                                                const ClientKey &clientId,
                                                                std::vector<std::string> &failDecIds)
{
    VLOG(1) << "GDecreaseRefWithLock begin, objectKeys size: " << objectKeys.size();
    std::vector<std::string> pendingObjectKeys = objectKeys;
    std::unordered_set<std::string> accumulatedFailDecIds;
    Status lastErr;
    bool retryRefMoving = true;
    int retryCount = 0;
    while (retryRefMoving) {
        failDecIds.clear();
        std::vector<std::string> retryDecIds;
        lastErr = TryGDecreaseRefWithLock(pendingObjectKeys, clientId, accumulatedFailDecIds, failDecIds, retryDecIds);
        retryRefMoving = IsRefMovingRetry(lastErr);
        if (retryRefMoving) {
            RETURN_IF_NOT_OK(PrepareNextRefMovingRetry(retryDecIds, pendingObjectKeys, retryCount,
                                                       "GRef metadata moving retry lost its retry object keys."));
        }
    }
    VLOG(1) << "GDecreaseRefWithLock end";
    return lastErr;
}

Status WorkerOcServiceGlobalReferenceImpl::UpdateMasterForFinishedIds(const ClientKey &clientId,
                                                                      const std::vector<std::string> &finishDecIds,
                                                                      std::unordered_set<std::string> &unAliveIds,
                                                                      std::vector<std::string> &failDecIds,
                                                                      std::vector<std::string> *retryDecIds)
{
    auto grouped = metadataRouteResolver_->GroupOwners(finishDecIds);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;
    Status status = Status::OK();

    for (auto item = objKeysGrpByMaster.begin(); item != objKeysGrpByMaster.end(); ++item) {
        const HostPort &masterAddr = item->first;
        std::vector<std::string> &currentFinishDecIds = item->second;
        std::vector<std::string> rpcFailedIds;
        std::unordered_set<std::string> rpcUnAliveIds;
        Status res = endpointPolicy_->CheckEndpoint(masterAddr, allowDirectoryLag_);
        if (res.IsError()) {
            rpcFailedIds = currentFinishDecIds;
        }
        if (res.IsOk()) {
            res = GDecreaseMasterRef("", currentFinishDecIds, rpcUnAliveIds, rpcFailedIds, false);
        }
        if (!rpcUnAliveIds.empty()) {
            unAliveIds.insert(rpcUnAliveIds.begin(), rpcUnAliveIds.end());
        }
        if (rpcFailedIds.empty()) {
            if (res.IsError()) {
                status = LogGDecreaseMasterRefError(masterAddr, res);
            }
            continue;
        }
        currentFinishDecIds.clear();
        if (IsRefMovingRetry(res) && retryDecIds != nullptr) {
            RETURN_IF_NOT_OK(RollbackDecreaseRefForRetry(clientId, rpcFailedIds, *retryDecIds, failDecIds));
            for (auto iter = std::next(item); iter != objKeysGrpByMaster.end(); ++iter) {
                RETURN_IF_NOT_OK(RollbackDecreaseRefForRetry(clientId, iter->second, *retryDecIds, failDecIds));
            }
            return res;
        }
        std::vector<std::string> tmpFailIds;
        std::vector<std::string> tempFinishDecIds;
        (void)globalRefTable_->GIncreaseRef(clientId, rpcFailedIds, tmpFailIds, tempFinishDecIds);
        (void)failDecIds.insert(failDecIds.end(), rpcFailedIds.begin(), rpcFailedIds.end());
        if (res.IsError()) {
            status = LogGDecreaseMasterRefError(masterAddr, res);
        }
    }
    return status;
}

void WorkerOcServiceGlobalReferenceImpl::BatchGRefLock(
    const std::vector<std::string> &objectKeys, bool insertable,
    std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    std::vector<std::shared_ptr<SafeObjType>> acquiredEntries;
    RaiiPlus rollback([&acquiredEntries]() {
        for (auto it = acquiredEntries.rbegin(); it != acquiredEntries.rend(); ++it) {
            (*it)->GRefUnlock();
        }
    });
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
        bool tracked = false;
        try {
            acquiredEntries.emplace_back(entry);
            tracked = true;
            bool inserted = lockedEntries.emplace(objectKey, entry).second;
            if (!inserted) {
                acquiredEntries.pop_back();
                entry->GRefUnlock();
            }
        } catch (...) {
            if (!tracked) {
                entry->GRefUnlock();
            }
            throw;
        }
    }
    rollback.ClearAllTask();
}

void WorkerOcServiceGlobalReferenceImpl::BatchGRefLock(
    std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    std::vector<std::shared_ptr<SafeObjType>> acquiredEntries;
    RaiiPlus rollback([&acquiredEntries]() {
        for (auto it = acquiredEntries.rbegin(); it != acquiredEntries.rend(); ++it) {
            (*it)->GRefUnlock();
        }
    });
    for (auto &entry : lockedEntries) {
        const auto &safeObj = entry.second;
        if (safeObj != nullptr) {
            safeObj->GRefLock();
            bool tracked = false;
            try {
                acquiredEntries.emplace_back(safeObj);
                tracked = true;
            } catch (...) {
                if (!tracked) {
                    safeObj->GRefUnlock();
                }
                throw;
            }
        }
    }
    rollback.ClearAllTask();
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
    auto grouped = metadataRouteResolver_->GroupOwners(nestedObjectKeys);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;

    // send requests for each master
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first;
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
    auto grouped = metadataRouteResolver_->GroupOwners(nestedObjectKeys);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;

    // send requests for each master
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first;
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

        int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
        remainingTime = remainingTime > 0 ? remainingTime : RPC_TIMEOUT;  // maybe call by local master
        GetRequestContext()->reqTimeoutDuration.InitWithPositiveTime(remainingTime);
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
