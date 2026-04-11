/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Defines the worker to master service processing main class implementation.
 */
#include "datasystem/worker/object_cache/master_worker_oc_service_impl.h"

#include <limits>
#include <sstream>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {
MasterWorkerOCServiceImpl::MasterWorkerOCServiceImpl(
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc, std::shared_ptr<AkSkManager> akSkManager)
    : ocClientWorkerSvc_(clientSvc), akSkManager_(akSkManager)
{
}

MasterWorkerOCServiceImpl::~MasterWorkerOCServiceImpl()
{
    LOG(INFO) << "MasterWorkerOCServiceImpl exit";
}

Status MasterWorkerOCServiceImpl::Init()
{
    CHECK_FAIL_RETURN_STATUS(ocClientWorkerSvc_ != nullptr, StatusCode::K_NOT_READY,
                             "ClientWorkerService must be initialized before MasterWorkerService construction");
    return MasterWorkerOCService::Init();
}

Status MasterWorkerOCServiceImpl::WaitWorkerOCServiceImplInit()
{
    return ocClientWorkerSvc_->WaitInit();
}

Status MasterWorkerOCServiceImpl::UpdateNotification(const UpdateObjectReqPb &reqs, UpdateObjectRspPb &rsp)
{
    INJECT_POINT("worker.UpdateNotification.begin");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(reqs), "AK/SK failed.");
    LOG(INFO) << "Received UpdateNotification request";
    std::vector<std::string> updateFailIds;
    for (const UpdateObjectInfoPb &req : reqs.object_infos()) {
        Status rc = UpdateSingleNotification(req, reqs.sync());
        if (rc.GetCode() == StatusCode::K_WORKER_TIMEOUT) {
            LOG(ERROR) << FormatString("[ObjectKey %s] Cache invalidation meet a deadlock, rollback.",
                                       req.object_key());
            return rc;
        } else if (rc.IsError()) {
            updateFailIds.emplace_back(req.object_key());
        }
    }
    *rsp.mutable_failed_ids() = { updateFailIds.begin(), updateFailIds.end() };
    INJECT_POINT("MasterWorkerOCServiceImpl.UpdateNotification.retry");
    LOG(INFO) << "UpdateNotification done";
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::UpdateSingleNotification(const UpdateObjectInfoPb &req, bool sync)
{
    auto getEntryAndLock = [this](const std::string &objectKey, bool sync, std::shared_ptr<SafeObjType> &entry) {
        RETURN_IF_NOT_OK(ocClientWorkerSvc_->objectTable_->Get(objectKey, entry));
        if (sync) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(TryLockWithRetry(objectKey, entry), "Lock failed");
        } else {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(entry->WLock(), "Lock failed");
        }
        return Status::OK();
    };

    const std::string &objectKey = req.object_key();
    const uint64_t version = req.version();
    LOG(INFO) << FormatString("[ObjectKey %s] Receive UpdateNotification from %s", objectKey, req.address());
    std::shared_ptr<SafeObjType> safeEntry;
    Status status = getEntryAndLock(objectKey, sync, safeEntry);
    if (status.IsError()) {
        return status.GetCode() == StatusCode::K_NOT_FOUND ? Status::OK() : status;
    }

    Raii unlockRaii([safeEntry]() { safeEntry->WUnlock(); });
    if ((*safeEntry)->IsBinary()) {
        if (version > (*safeEntry)->GetCreateTime()) {
            // Update version to latest time to keep invalidation version.
            (*safeEntry)->SetCreateTime(version);
            ObjectKV objectKV(objectKey, *safeEntry);
            RETURN_IF_NOT_OK(BinaryObjectCacheInvalidation(req, objectKV));
        } else if (version < (*safeEntry)->GetCreateTime()) {
            LOG(INFO) << "Cache Invalidation should not be performed because the version is out of date.";
            RETURN_STATUS(K_INVALID, "Version expired.");
        }
    } else if ((*safeEntry)->IsHashmap()) {
        for (const auto &field : req.secondary_keys()) {
            Status rc = (*safeEntry)->Erase(field);
            if (rc.IsError() && rc.GetCode() != K_NOT_FOUND) {
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "UpdateSingleNotification erase data failed");
            }
        }
        LOG(INFO) << FormatString("[ObjectKey %s] Cache invalidation delete success", objectKey);
    } else {
        RETURN_STATUS_LOG_ERROR(K_UNKNOWN_ERROR, "object data format not supported");
    }
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::MetaChange(const MetaChangeReqPb &req, MetaChangeRspPb &rsp)
{
    // Deprecated
    (void)req;
    (void)rsp;
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::PublishMeta(const PublishMetaReqPb &req, PublishMetaRspPb &resp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    (void)resp;
    LOG(INFO) << FormatString("[ObjectKey %s] Publish meta for object", req.meta().object_key());
    auto traceID = Trace::Instance().GetTraceID();
    ocClientWorkerSvc_->threadPool_->Execute([this, req, traceID] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        reqTimeoutDuration.InitWithPositiveTime(req.timeout());
        Raii outerResetDuration([]() { timeoutDuration.Reset(); });
        Status rc = Status::OK();
        master::QueryMetaInfoPb queryMeta;
        std::vector<RpcMessage> payloads;
        queryMeta.mutable_meta()->CopyFrom(req.meta());
        queryMeta.set_address(req.address());
        queryMeta.set_is_from_other_az(req.is_from_other_az());
        ReadKey readKey(queryMeta.meta().object_key());
        RetryGetObjectFromRemote(readKey, queryMeta, payloads);
    });
    return Status::OK();
}

void MasterWorkerOCServiceImpl::RetryGetObjectFromRemote(const ReadKey &readKey,
                                                         const master::QueryMetaInfoPb &queryMeta,
                                                         std::vector<RpcMessage> &payloads)
{
    const auto &objectKey = queryMeta.meta().object_key();
    constexpr int MAX_RETRY = 5;
    Status rc;
    for (int i = 0; i < MAX_RETRY; i++) {
        rc = ocClientWorkerSvc_->GetObjectFromAnywhere(readKey, queryMeta, payloads);
        if (rc.IsOk()) {
            LOG(INFO) << FormatString("[ObjectKey %s] Get object from remote success", objectKey);
            break;
        }
    }
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("GetObjectFromRemote failed: %s", rc.ToString());
    }
}

Status MasterWorkerOCServiceImpl::ClearData(const ClearDataReqPb &req, ClearDataRspPb &rsp)
{
    RETURN_IF_NOT_OK(ocClientWorkerSvc_->ClearObject(req));
    (void)rsp;
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::DeleteNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Processing DeleteNotification request with async:" << req.is_async()
              << ", objectKeys: " << VectorToString(req.object_keys());
    if (req.is_async()) {
        auto traceID = Trace::Instance().GetTraceID();
        ocClientWorkerSvc_->threadPool_->Execute([this, req, traceID] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            DeleteObjectRspPb rsp;
            auto rc = ocClientWorkerSvc_->DeleteCopyNotification(req, rsp);
            if (rc.IsError()) {
                LOG(ERROR) << FormatString("DeleteCopyNotification failed: %s", rc.ToString());
            }
        });
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->DeleteCopyNotification(req, rsp),
                                         "woker DeleteCopyNotification failed");
    }
    INJECT_POINT("MasterWorkerOCServiceImpl.DeleteNotification.retry");
    VLOG(1) << "DeleteNotification done";
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::DeletePersistenceObject(const DeletePersistenceObjectReqPb &req,
                                                          DeletePersistenceObjectRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Delete persistence object: " << req.object_key();
    auto rc = ocClientWorkerSvc_->DeletePersistenceObject(req, rsp);
    rsp.mutable_last_rc()->set_error_code(rc.GetCode());
    rsp.mutable_last_rc()->set_error_msg(rc.GetMsg());
    VLOG(1) << "Delete persistence object done";
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::QueryGlobalRefNumOnWorker(const QueryGlobalRefNumReqPb &req,
                                                            QueryGlobalRefNumRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "[GRef] Query All Objs Global References Rpc Received";
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    ocClientWorkerSvc_->globalRefTable_->GetAllRef(refTable);
    if (req.object_keys_size() == 0) {
        // Query All objects
        LOG(INFO) << "[GRef] Query All Objs Global References:";
        for (const auto &entry : refTable) {
            GRefDistributionPb entryPb;
            std::string objectKey;
            TenantAuthManager::Instance()->NamespaceUriToObjectKey(entry.first, objectKey);
            entryPb.set_object_key(objectKey);
            *entryPb.mutable_referred_addr() = { entry.second.begin(), entry.second.end() };

            LOG(INFO) << "[GRef] Obj: " << entry.first << "is referenced by: " << entryPb.referred_addr().data();

            rsp.add_objs_glb_ref()->CopyFrom(entryPb);
        }
    } else {
        // Query specific objects
        for (const auto &id : req.object_keys()) {
            // Find each object ref and serialize to pb
            LOG(INFO) << "[GRef] Query Given Objs Global References:";
            auto iter = refTable.find(id);
            if (iter == refTable.end()) {
                LOG(ERROR) << "[GRef] Obj:" << id << "is not referenced by any runtime.";
                continue;
            }
            GRefDistributionPb entryPb;
            std::string objectKey;
            TenantAuthManager::Instance()->NamespaceUriToObjectKey(iter->first, objectKey);
            entryPb.set_object_key(objectKey);
            *entryPb.mutable_referred_addr() = { iter->second.begin(), iter->second.end() };
            LOG(INFO) << "[GRef] Obj: " << iter->first << "is referenced by: " << entryPb.referred_addr().data();
            rsp.add_objs_glb_ref()->CopyFrom(entryPb);
        }
    }
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::BinaryObjectCacheInvalidation(const UpdateObjectInfoPb &req, ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &safeEntry = objectKV.GetObjEntry();
    if (safeEntry->IsSpilled()) {
        LOG(INFO) << FormatString("[ObjectKey %s] Spilled and prepare to clean it.", objectKey);
        RETURN_IF_NOT_OK_EXCEPT(WorkerOcServiceCrudCommonApi::DeleteObjectFromDisk(objectKV), K_NOT_FOUND);
    }
    // Mark as invalid and memorize address.
    safeEntry->stateInfo.SetCacheInvalid(true);
    safeEntry->stateInfo.SetPrimaryCopy(false);
    LOG_IF_ERROR(safeEntry->FreeResources(), "SafeObj free failed");
    safeEntry->SetAddress(req.address());
    safeEntry->SetLifeState(ObjectLifeState::OBJECT_INVALID);  // To make IsInvalid() call true.
    LOG(INFO) << FormatString("[ObjectKey %s] Cache invalidation success", objectKey);
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::PushMetaToWorker(const PushMetaToWorkerReqPb &req, PushMetaToWorkerRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    (void)rsp;
    LOG(INFO) << "Worker receive PushMetaToWorker request from master: " << req.source_address();
    for (auto &cacheInvalid : req.cache_invalids()) {
        LOG(INFO) << FormatString("Receive PushMetaToWorker cache invalid for object:%s", cacheInvalid.object_key());
        Status rc = UpdateSingleNotification(cacheInvalid);
        // If we can not found the object, we just ignore it.
        if (rc.GetCode() == StatusCode::K_NOT_FOUND) {
            rc = Status::OK();
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "worker update notification failed");
    }

    for (const auto &id : req.primary_copy_invalid_ids()) {
        LOG(INFO) << FormatString("Receive PushMetaToWorker cache invalid for object:%s", id);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ProcessChangePrimaryCopy(id, false), "worker change primary copy failed");
    }

    DeleteObjectReqPb delReq;
    DeleteObjectRspPb delRsp;
    delReq.mutable_object_keys()->CopyFrom(req.delete_object_keys());
    for (int i = 0; i < delReq.object_keys_size(); ++i) {
        delReq.add_versions(std::numeric_limits<uint64_t>::max());
    }
    delReq.set_is_async(true);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->GenerateSignature(delReq), "GenerateSignature failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(DeleteNotification(delReq, delRsp), "worker delete notification failed");
    // We have received the metadata from master, lets reconciliation its data with
    // reconnected client and notify master to remove the not exist client metadata.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->Reconciliation(req), "worker reconciliation failed");
    LOG(INFO) << "PushMetaToWorker done";
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::RequestMetaFromWorker(const RequestMetaFromWorkerReqPb &req,
                                                        RequestMetaFromWorkerRspPb &rsp)
{
    LOG(INFO) << FormatString("worker(%s) received RequestMetaFromWorker request: %s", localAddress_.ToString(),
                              req.address());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->FillRequestMetaByMaster(req, rsp),
                                     "worker FillRequestMetaByMaster failed");
    LOG(INFO) << "RequestMetaFromWorker done";
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::ProcessChangePrimaryCopy(const std::string &objectKey, bool isPrimaryCopy)
{
    INJECT_POINT("process.change.primary.copy");
    LOG(INFO) << FormatString("ProcessChangePrimaryCopy for object:%s", objectKey);
    std::shared_ptr<SafeObjType> safeEntry;
    RETURN_IF_NOT_OK(ocClientWorkerSvc_->objectTable_->Get(objectKey, safeEntry));
    RETURN_IF_NOT_OK(safeEntry->WLock());
    Raii unlockRaii([safeEntry]() { safeEntry->WUnlock(); });
    if ((*safeEntry)->IsBinary()) {
        (*safeEntry)->stateInfo.SetPrimaryCopy(isPrimaryCopy);
    }
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::ChangePrimaryCopy(const ChangePrimaryCopyReqPb &req, ChangePrimaryCopyRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "worker(" << localAddress_.ToString() << ") receive ChangePrimaryCopy request";
    // If migrate data task (Scale-In) has been started, we should not change primary copy in this node.
    if (ocClientWorkerSvc_->MigrateDataStarted()) {
        std::stringstream ss;
        ss << "worker(" << localAddress_.ToString() << ") is exiting, cannot accept change primary copy request";
        LOG(INFO) << ss.str();
        RETURN_STATUS(StatusCode::K_NOT_READY, ss.str());
    }
    for (const auto &id : req.object_keys()) {
        Status rc = ProcessChangePrimaryCopy(id, true);
        if (rc.IsError()) {
            LOG(INFO) << "worker ChangePrimaryCopy failed: " << rc.GetMsg();
            continue;
        }
        rsp.add_success_ids(id);
    }
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::NotifyMasterIncNestedRefs(const NotifyMasterIncNestedReqPb &req,
                                                            NotifyMasterIncNestedResPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    (void)rsp;  // empty response
    std::vector<std::string> nested_objs(req.nested_object_keys().begin(), req.nested_object_keys().end());
    LOG(INFO) << "worker received NotifyMasterIncNestedRefs, nested objectkeys: " << VectorToString(nested_objs);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->IncNestedRef(nested_objs), "worker IncNestedRef failed");
    LOG(INFO) << "worker NotifyMasterIncNestedRefs done";
    return Status::OK();
}

Status MasterWorkerOCServiceImpl::NotifyMasterDecNestedRefs(const NotifyMasterDecNestedReqPb &req,
                                                            NotifyMasterDecNestedResPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    (void)rsp;  // empty response
    std::vector<std::string> nested_objs(req.nested_object_keys().begin(), req.nested_object_keys().end());
    LOG(INFO) << "worker received NotifyMasterDecNestedRefs, nested objectkeys: " << VectorToString(nested_objs);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ocClientWorkerSvc_->DecNestedRef(nested_objs), "worker DecNestedRef failed");
    LOG(INFO) << "worker NotifyMasterDecNestedRefs done";
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
