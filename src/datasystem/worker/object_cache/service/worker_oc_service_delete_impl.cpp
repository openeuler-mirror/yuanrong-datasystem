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
 * Description: Defines the worker service processing delete process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_delete_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

WorkerOcServiceDeleteImpl::WorkerOcServiceDeleteImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                                     std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress,
                                                     std::shared_ptr<WorkerOcServiceGetImpl> getProc)
    : WorkerOcServiceCrudCommonApi(initParam),
      etcdCM_(etcdCM),
      akSkManager_(std::move(akSkManager)),
      localAddress_(localAddress),
      getProc_(std::move(getProc))
{
}

Status WorkerOcServiceDeleteImpl::DeleteAllCopy(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_DELETE_ALL_COPY);
    uint64_t deletedSize = 0;
    Status rc = DeleteAllCopyImpl(req, resp, deletedSize);
    RequestParam reqParam;
    reqParam.objectKey = ObjectKeysToAbbrStr(req.object_keys());
    posixPoint.Record(rc.GetCode(), std::to_string(deletedSize), reqParam, rc.GetMsg());
    workerOperationTimeCost.Append("Total DeleteAllCopy", timer.ElapsedMilliSecond());
    LOG(INFO) << "DeleteAllCopy finish with request size: " << req.object_keys_size()
              << ", failed size: " << resp.fail_object_keys_size() << ", the operation cost "
              << workerOperationTimeCost.GetInfo();
    return rc;
}

Status WorkerOcServiceDeleteImpl::DeleteCopyNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp)
{
    bool isAsync = req.is_async();
    VLOG(1) << "DeleteCopyNotification begin, request: " << LogHelper::IgnoreSensitive(req);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        req.object_keys_size() == req.versions_size(), K_RUNTIME_ERROR,
        FormatString("Unexpected error happen, it should not happen, object key size: %d, version size: %d",
                     req.object_keys_size(), req.versions_size()));
    Status lastErr;
    for (int i = 0; i < req.object_keys_size(); ++i) {
        const auto &objectKey = req.object_keys(i);
        uint64_t version = req.versions(i);
        Status rc = DeleteObjectFromNotification(objectKey, version, isAsync);
        if (rc.IsOk()) {
            continue;
        }
        if (rc.GetCode() == K_NOT_FOUND) {
            // If the object does not exist, the deletion is successful.
            LOG(INFO) << FormatString("[ObjectKey %s] DeleteObject not exist.", objectKey);
            continue;
        }
        LOG(ERROR) << FormatString("[ObjectKey %s] Delete failed, %s", objectKey, rc.ToString());
        rsp.add_failed_object_keys(objectKey);
        // if dead lock happened, the last error should return K_WORKER_DEADLOCK.
        lastErr = lastErr.GetCode() == K_WORKER_DEADLOCK ? lastErr : rc;
    }
    rsp.mutable_last_rc()->set_error_code(lastErr.GetCode());
    rsp.mutable_last_rc()->set_error_msg(lastErr.GetMsg());
    VLOG(1) << "DeleteCopyNotification end.";
    return Status::OK();
}

Status WorkerOcServiceDeleteImpl::DeletePersistenceObject(const DeletePersistenceObjectReqPb &req,
                                                          DeletePersistenceObjectRspPb &rsp)
{
    (void)rsp;
    VLOG(1) << "DeletePersistenceObject begin, request: " << LogHelper::IgnoreSensitive(req);
    auto objectVersion = req.object_version();
    auto rc = persistenceApi_->Del(req.object_key(), req.max_version_to_delete(), req.delete_all_version(),
                                   req.async_elapse(), &objectVersion, req.list_incomplete_versions());
    VLOG(1) << "DeletePersistenceObject end, rc: " << rc.ToString();
    return rc;
}

Status WorkerOcServiceDeleteImpl::DeleteObjectFromNotification(const std::string &objectKey, uint64_t version,
                                                               bool async)
{
    INJECT_POINT("worker.DeleteObjectWithTryLock.before");
    bool insert = false;
    std::shared_ptr<SafeObjType> entry;
    // If entry insert failed, it would not be locked.
    RETURN_IF_NOT_OK(objectTable_->ReserveGetAndLock(objectKey, entry, insert, false, false));
    if (insert) {
        Raii innerUnlock([&entry]() { entry->WUnlock(); });
        (void)objectTable_->Erase(objectKey, *entry);
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "Object not found");
    } else if (async) {
        ObjectKV objectKV(objectKey, *entry);
        if (entry->IsWLockedByCurrentThread()) {
            if ((*entry)->GetCreateTime() > version) {
                LOG(INFO) << FormatString("[ObjectKey %s] Version %d is large than the request version %d, skip.",
                                          objectKey, (*entry)->GetCreateTime(), version);
                return Status::OK();
            }
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                ClearObject(objectKV), FormatString("[ObjectKey %s] ClearObjectAndDelL2cache failed.", objectKey));
        } else {
            RETURN_IF_NOT_OK(entry->WLock());
            Raii unlock([&entry]() { entry->WUnlock(); });
            if ((*entry)->GetCreateTime() > version) {
                LOG(INFO) << FormatString("[ObjectKey %s] Version %d is large than the request version %d, skip.",
                                          objectKey, (*entry)->GetCreateTime(), version);
                return Status::OK();
            }
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                ClearObject(objectKV), FormatString("[ObjectKey %s] ClearObjectAndDelL2cache failed.", objectKey));
        }
    } else {
        RETURN_IF_NOT_OK(TryLockWithRetry(objectKey, entry));
        ObjectKV objectKV(objectKey, *entry);
        Raii innerUnlock([&entry]() { entry->WUnlock(); });
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ClearObject(objectKV),
                                         FormatString("[ObjectKey %s] ClearObjectAndDelL2cache failed.", objectKey));
    }
    return Status::OK();
}

Status WorkerOcServiceDeleteImpl::DeleteAllCopyImpl(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp,
                                                    uint64_t &deletedSize)
{
    Timer timerCost;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto needDeleteObjs = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    LOG(INFO) << "DeleteAllCopy request: client_id: " << req.client_id()
              << ", objectKeys: " << VectorToString(needDeleteObjs)
              << " before delete elapsed ms: " << timerCost.ElapsedMilliSecond();

    std::vector<std::string> failedObjectKeys;
    Status rc = RetryWhenDeadlock([this, &needDeleteObjs, &failedObjectKeys, &deletedSize] {
        if (reqTimeoutDuration.CalcRealRemainingTime() <= 0) {
            return Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
        }
        if (!failedObjectKeys.empty()) {
            needDeleteObjs = std::move(failedObjectKeys);
        }
        return DeleteAllCopyWithLock(needDeleteObjs, failedObjectKeys, deletedSize);
    });
    VLOG(1) << FormatString("The total size of the current deletion is %llu", deletedSize);
    if (!failedObjectKeys.empty()) {
        LOG(WARNING) << "Delete failed list: " << VectorToString(failedObjectKeys);
        for (auto &objectKey : failedObjectKeys) {
            std::string failedId;
            TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKey, failedId);
            resp.add_fail_object_keys(failedId);
        }
    }
    resp.mutable_last_rc()->set_error_code(rc.GetCode());
    resp.mutable_last_rc()->set_error_msg(rc.GetMsg());
    if (rc.GetCode() == K_RPC_DEADLINE_EXCEEDED) {
        return rc;
    }
    return Status::OK();
}

Status WorkerOcServiceDeleteImpl::DeleteAllCopyWithLock(const std::vector<std::string> &objectKeys,
                                                        std::vector<std::string> &failedObjectKeys,
                                                        uint64_t &deletedSize)
{
    std::map<std::string, std::shared_ptr<SafeObjType>> lockedEntries;
    std::vector<std::string> sendToMasterIds;
    Status lastErr = BatchLockWithInsert(objectKeys, lockedEntries, sendToMasterIds, failedObjectKeys);
    Raii unlockAll([&lockedEntries, this]() { BatchUnlock(lockedEntries); });

    if (sendToMasterIds.empty()) {
        return lastErr;
    }
    std::unordered_set<std::string> masterFailedIds;
    lastErr = DeleteAllCopyMetaFromMaster(sendToMasterIds, masterFailedIds);

    for (const auto &kv : lockedEntries) {
        const auto &objectKey = kv.first;
        if (masterFailedIds.count(objectKey) > 0) {
            failedObjectKeys.emplace_back(objectKey);
            LOG(ERROR) << FormatString("[ObjectKey %s] Delete metadata in master failed.", objectKey);
            continue;
        }
        std::shared_ptr<SafeObjType> entry = kv.second;
        auto dataSize = entry->Get() == nullptr ? 0 : (*entry)->GetDataSize();
        ObjectKV objectKV(objectKey, *entry);
        Status rc = ClearObject(objectKV);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[ObjectKey %s] ClearObjectAndDelL2cache failed, error: %s.", objectKey,
                                       rc.ToString());
            failedObjectKeys.emplace_back(objectKey);
            lastErr = rc;
        } else {
            deletedSize += dataSize;
        }
    }
    return lastErr;
}

void WorkerOcServiceDeleteImpl::ExtractCrossAzOfflineWorkerIdKeyWithEmptyAddress(
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMasterId,
    std::unordered_map<std::string, Status> &errInfos,
    std::unordered_map<std::string, std::vector<std::string>> &crossAzOfflineWorkerIdKeys)
{
    MetaAddrInfo emptyInfo;
    auto objectKeysUnknownMaster = objKeysGrpByMasterId.find(emptyInfo);
    if (objectKeysUnknownMaster == objKeysGrpByMasterId.end()) {
        return;
    }
    auto &objKeys = objectKeysUnknownMaster->second;

    // isWorkerIdKey && belongToOtherAz && masterIsOffline, then:
    //   record in crossAzOfflineWorkerIdKeys and remove from objKeysGrpByMasterId and errInfos
    (void)EraseIf(objKeys, [this, &crossAzOfflineWorkerIdKeys, &errInfos](const std::string &objKey) {
        std::string workerId;
        if (TrySplitWorkerIdFromObjecId(objKey, workerId).IsError()) {
            return false;
        }
        std::string azName = etcdCM_->GetOtherAzNameByWorkerIdInefficient(workerId);
        if (azName.empty()) {
            return false;
        }
        if (etcdCM_->CheckConnection(objKey).GetCode() != K_NOT_FOUND) {
            return false;
        }
        crossAzOfflineWorkerIdKeys.emplace(objKey,
                                           std::vector<std::string>{ azName });  // key: objectKey, value: azName
        errInfos.erase(objKey);
        return true;  // delete from objKeysGrpByMasterId
    });
}

void WorkerOcServiceDeleteImpl::DeleteCrossAzKeyWhenMasterFailed(
    const std::unordered_map<std::string, std::vector<std::string>> &keys)
{
    if (keys.empty()) {
        return;
    }
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(localAddress_);
    auto localApi = std::dynamic_pointer_cast<worker::WorkerLocalMasterOCApi>(workerMasterApi);
    if (localApi == nullptr) {
        LOG(ERROR) << "WorkerLocalMasterOCApi not found. local address is " << localAddress_.ToString();
        return;
    }
    // use the local master service as an agent to delete cross-az meta and data.
    localApi->AsyncNotifyCrossAzDelete(keys);
}

Status WorkerOcServiceDeleteImpl::DeleteAllCopyMetaFromMaster(const std::vector<std::string> &needDeleteObjectKey,
                                                              std::unordered_set<std::string> &failedObjectKeys)
{
    Status lastRc;
    // Group ObjectKeys by masterId
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMasterId;
    std::optional<std::unordered_map<std::string, Status>> errInfos;
    errInfos.emplace();
    etcdCM_->GroupObjKeysByMasterHostPortWithStatus(needDeleteObjectKey, objKeysGrpByMasterId, errInfos);

    std::unordered_map<std::string, std::vector<std::string>> crossAzOfflineWorkerIdKeys;  // map<objectKey, azName>
    ExtractCrossAzOfflineWorkerIdKeyWithEmptyAddress(objKeysGrpByMasterId, *errInfos, crossAzOfflineWorkerIdKeys);
    DeleteCrossAzKeyWhenMasterFailed(crossAzOfflineWorkerIdKeys);

    for (const auto &kv : *errInfos) {
        // If objectKey don't belong to any master, just ignore it.
        if (kv.second.GetCode() != K_NOT_FOUND) {
            failedObjectKeys.emplace(kv.first);
            lastRc = kv.second;
        }
    }

    // Send requests for each master
    for (auto &item : objKeysGrpByMasterId) {
        HostPort masterAddr = item.first.GetAddressAndSaveDbName();
        // Skip the empty master addr, the related object exists in errInfos.
        if (masterAddr.Empty()) {
            continue;
        }
        std::vector<std::string> &currentNeedDeleteObjectKey = item.second;
        LOG(INFO) << "Delete all copy meta from master: " << masterAddr
                  << ", objects: " << VectorToString(currentNeedDeleteObjectKey);
        master::DeleteAllCopyMetaReqPb deleteReq;
        *deleteReq.mutable_object_keys() = { currentNeedDeleteObjectKey.begin(), currentNeedDeleteObjectKey.end() };
        deleteReq.set_address(localAddress_.ToString());
        deleteReq.set_redirect(true);
        deleteReq.set_need_forward_objs_without_meta(true);
        master::DeleteAllCopyMetaRspPb deleteRsp;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            LOG(WARNING) << "get master api failed. masterAddr=" << masterAddr.ToString();
            continue;
        }
        std::function<Status(DeleteAllCopyMetaReqPb &, DeleteAllCopyMetaRspPb &)> func =
            [&workerMasterApi](DeleteAllCopyMetaReqPb &deleteReq, DeleteAllCopyMetaRspPb &deleteRsp) {
                return workerMasterApi->DeleteAllCopyMeta(deleteReq, deleteRsp);
            };
        Status status = RedirectRetryWhenMetasMoving(deleteReq, deleteRsp, func);
        Status recvRc(static_cast<StatusCode>(deleteRsp.last_rc().error_code()), deleteRsp.last_rc().error_msg());
        RETURN_IF_NOT_OK(InsertFailedId(status, recvRc, failedObjectKeys, needDeleteObjectKey, deleteRsp));

        if (!deleteRsp.info().empty()) {
            for (const auto &redirectInfo : deleteRsp.info()) {
                master::DeleteAllCopyMetaReqPb redirectDeleteReq;
                master::DeleteAllCopyMetaRspPb redirectDeleteRsp;
                *redirectDeleteReq.mutable_object_keys() = { redirectInfo.change_meta_ids().begin(),
                                                             redirectInfo.change_meta_ids().end() };
                redirectDeleteReq.set_address(localAddress_.ToString());
                HostPort redirectMasterAddr;
                RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr));
                std::shared_ptr<WorkerMasterOCApi> redirectWorkerMasterApi =
                    workerMasterApiManager_->GetWorkerMasterApi(redirectMasterAddr);
                CHECK_FAIL_RETURN_STATUS(redirectWorkerMasterApi != nullptr, K_RUNTIME_ERROR,
                                         "hash master get failed, DeleteAllCopyMetaFromMaster failed");
                status = redirectWorkerMasterApi->DeleteAllCopyMeta(redirectDeleteReq, redirectDeleteRsp);
                recvRc =
                    Status(static_cast<StatusCode>(deleteRsp.last_rc().error_code()), deleteRsp.last_rc().error_msg());
                RETURN_IF_NOT_OK(
                    InsertFailedId(status, recvRc, failedObjectKeys, needDeleteObjectKey, redirectDeleteRsp));
            }
        }
    }

    return lastRc;
}

Status WorkerOcServiceDeleteImpl::InsertFailedId(Status &rpcStatus, Status &recvRc,
                                                 std::unordered_set<std::string> &failedObjectKeys,
                                                 const std::vector<std::string> &needDeleteObjectKey,
                                                 master::DeleteAllCopyMetaRspPb &deleteRsp)
{
    if (rpcStatus.IsError()) {
        LOG(ERROR) << "DeleteAllCopyMeta failed with " << rpcStatus.ToString();
        failedObjectKeys.insert(needDeleteObjectKey.begin(), needDeleteObjectKey.end());
        return rpcStatus;
    }
    if (recvRc.IsError()) {
        LOG(ERROR) << "DeleteAllCopyMeta response " << LogHelper::IgnoreSensitive(deleteRsp);
        failedObjectKeys.insert(deleteRsp.failed_object_keys().begin(), deleteRsp.failed_object_keys().end());
    }
    return recvRc;
}

}  // namespace object_cache
}  // namespace datasystem
