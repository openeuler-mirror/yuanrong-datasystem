/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Code to manage device object.
 */
#include "datasystem/worker/object_cache/device/worker_device_oc_manager.h"

#include <memory>

#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/object_cache/device/device_obj_cache.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

using namespace datasystem::worker;

namespace datasystem {
namespace object_cache {

Status WorkerDeviceOcManager::PublishDeviceObject(const std::string &devObjectKey, const PublishDeviceObjectReqPb &req,
                                                  const std::vector<RpcMessage> &payloads)
{
    (void)payloads;
    std::shared_ptr<SafeObjType> entry;
    bool isInsert;
    RETURN_IF_NOT_OK(workerOcImpl_->objectTable_->ReserveGetAndLock(devObjectKey, entry, isInsert));
    Raii unlock([&entry]() {
        if (entry->IsWLockedByCurrentThread()) {
            entry->WUnlock();
        }
    });

    ObjectKV objectKV(devObjectKey, *entry);

    if (entry->Get() == nullptr || (*entry)->IsEmpty() || !(*entry)->IsPublished()) {
        auto devObj = std::make_unique<DeviceObjCache>();
        devObj->SetMetadataSize(workerOcImpl_->GetMetadataSize());
        devObj->SetDeviceIdx(req.device_id());
        devObj->SetOffset(req.offset());
        devObj->stateInfo.SetDataFormat(DataFormat::HETERO);
        entry->SetRealObject(std::move(devObj));
        workerOcImpl_->publishProc_->AttachShmUnitToObject(WorkerOcServiceCreateImpl::ClientShmEnabled(req.client_id()),
                                                           req.dev_object_key(), ShmKey::Intern(req.shm_id()),
                                                           req.data_size(), *entry);
    } else {
        CHECK_FAIL_RETURN_STATUS(!(*entry)->IsHetero(), K_INVALID,
                                 "The key you publish already exists, which is not published as a device object");
        return { K_INVALID, "The key you publish already exists, device object not support repeat publish." };
    }

    Status rc = CreateDeviceMetaToMaster(objectKV);
    if (rc.IsError()) {
        LOG_IF_ERROR(objectKV.GetObjEntry()->FreeResources(), "SafeDevObj free failed.");
        LOG(ERROR) << FormatString("[DevObjectKey %s] RequestingToMaster failed, status: %s", devObjectKey,
                                   rc.ToString());
        return rc;
    }

    LOG(INFO) << "DeviceObject publish to master success";
    SafeObjType::GetDerived<DeviceObjCache>(*entry)->SetPublished();
    if (!payloads.empty()) {
        RETURN_IF_NOT_OK(SaveBinaryObjectToMemory(objectKV, payloads, workerOcImpl_->evictionManager_,
                                                  workerOcImpl_->memCpyThreadPool_));
    }
    deviceReqManager_.UpdateRequestForSuccess(objectKV);
    return Status::OK();
}

Status WorkerDeviceOcManager::CreateDeviceMetaToMaster(const ObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &safeObj = objectKV.GetObjEntry();

    master::CreateMetaReqPb metaReq;
    datasystem::ObjectMetaPb *metadata = metaReq.mutable_meta();

    metadata->set_object_key(objectKey);
    metadata->set_data_size(safeObj->GetDataSize());
    ConfigPb *configPb = metadata->mutable_config();
    configPb->set_data_format(static_cast<uint32_t>(safeObj->stateInfo.GetDataFormat()));
    metaReq.set_address(workerOcImpl_->localAddress_.ToString());

    auto *devObj = SafeObjType::GetDerived<DeviceObjCache>(safeObj);
    datasystem::DeviceMetaInfoPb *devInfo = metadata->mutable_device_info();
    devInfo->set_device_id(devObj->GetDeviceIdx());
    devInfo->set_offset(devObj->GetOffset());

    master::CreateMetaRspPb metaResp;
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerOcImpl_->workerMasterApiManager_->GetWorkerMasterApi(objectKey, workerOcImpl_->etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, CreateDeviceMetaToMaster failed");
    return workerMasterApi->CreateMeta(metaReq, metaResp);
}

void WorkerDeviceOcManager::SetEmptyDeviceObject(SafeObjType &entry)
{
    auto devObj = std::make_unique<DeviceObjCache>();
    entry.SetRealObject(std::move(devObj));
}

Status WorkerDeviceOcManager::ProcessGetDeviceObjectRequest(
    const std::vector<std::string> &objectKeys,
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDeviceObjectRspPb, GetDeviceObjectReqPb>> serverApi,
    int64_t subTimeout, const std::string &clientId)
{
    std::vector<std::string> objectsNeedGetRemote;
    auto request = std::make_shared<GetDeviceObjectRequest>(objectKeys, std::move(serverApi), clientId, nullptr);

    // Try get from local.
    TryGetDeviceObjectFromLocal(request, objectsNeedGetRemote);

    // Try get from remote worker.
    RETURN_IF_NOT_OK(TryGetDeviceObjectFromRemote(subTimeout, request, objectsNeedGetRemote));

    RETURN_OK_IF_TRUE(request->isReturn_);
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (request->numSatisfiedObjects_ == request->numWaitingObjects_ || subTimeout == 0 || remainingTimeMs <= 0) {
        LOG(INFO) << "The satisfied objects num: " << request->numSatisfiedObjects_
                  << ", the waiting objects num: " << request->numWaitingObjects_
                  << ", the sub timeout: " << subTimeout;
        Status rc = deviceReqManager_.ReturnFromGetDeviceObjectRequest(request);
        return rc;
    }

    TimerQueue::TimerImpl timer;
    auto traceID = Trace::Instance().GetTraceID();
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        std::min<int64_t>(subTimeout, remainingTimeMs),
        [this, subTimeout, request, traceID]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            LOG(ERROR) << "The device get request times out, the sub timeout: " << subTimeout
                       << " clientId: " << request->clientId_ << ", satisfied num: " << request->numSatisfiedObjects_
                       << ", waiting num: " << request->numWaitingObjects_;
            deviceReqManager_.ReturnFromGetDeviceObjectRequest(request);
            // Avoid timeCostPoint destruct after traceGuard.
            request->accessRecorderPoint_.reset();
        },
        timer));
    request->timer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
    return Status::OK();
}

void WorkerDeviceOcManager::TryGetDeviceObjectFromLocal(std::shared_ptr<GetDeviceObjectRequest> &request,
                                                        std::vector<std::string> &objectsNeedGetRemote)
{
    for (const auto &objectKey : request->deduplicatedObjectKeys_) {
        Status status = GetDeviceObjectInLocalMem(objectKey, request, objectsNeedGetRemote);
        if (status.IsError()) {
            LOG(ERROR) << "PreProcessGetObject failed:" << status.GetMsg();
            request->SetStatus(status.GetCode() == K_OUT_OF_MEMORY ? status : Status(K_RUNTIME_ERROR, status.GetMsg()));
            if (request->objects_.emplace(objectKey, nullptr)) {
                (void)request->numSatisfiedObjects_.fetch_add(1);
            }
        }
        // Add request even if failed.
        (void)deviceReqManager_.AddRequest(objectKey, request);
    }
}

Status WorkerDeviceOcManager::GetDeviceObjectInLocalMem(const std::string &objectKey,
                                                        std::shared_ptr<GetDeviceObjectRequest> &request,
                                                        std::vector<std::string> &objectsNeedGetRemote)
{
    std::shared_ptr<SafeObjType> entry;
    // Fetch the object and lock it.
    // If the object is not found, add it to the GetRemote list and return.
    Status rc = workerOcImpl_->objectTable_->Get(objectKey, entry);
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    if (rc.GetCode() == K_NOT_FOUND) {
        objectsNeedGetRemote.push_back(objectKey);
        return Status::OK();
    }
    ObjectKV objectKV(objectKey, *entry);
    // If entry WLock is not found, it means the object is deleting locally.
    // Try to get object from remote.
    rc = entry->WLock();
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    if (rc.GetCode() == K_NOT_FOUND) {
        objectsNeedGetRemote.push_back(objectKey);
        return Status::OK();
    }
    Raii unlock([&entry]() { entry->WUnlock(); });
    if ((*entry).Get() == nullptr) {
        objectsNeedGetRemote.push_back(objectKey);
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS((*entry)->IsEmpty() || (*entry)->IsHetero(), K_INVALID, "Not a Hetero Object");
    if ((*entry)->IsPublished()) {
        LOG(INFO) << FormatString("[ObjectKey %s] exists in memory", objectKey);
        if (request->objects_.emplace(objectKey, DeviceObjEntryParams::ConstructDeviceObjEntryParams(*entry))) {
            (void)request->numSatisfiedObjects_.fetch_add(1);
        }
    } else {
        // case 3: object didn't exist in local node, is not published or sealed yet.
        objectsNeedGetRemote.push_back(objectKey);
    }
    return Status::OK();
}

Status WorkerDeviceOcManager::TryGetDeviceObjectFromRemote(const int64_t subTimeout,
                                                           std::shared_ptr<GetDeviceObjectRequest> &request,
                                                           std::vector<std::string> &objectsNeedGetRemote)
{
    std::set<ReadKey> needGetIds;
    for (const auto &id : objectsNeedGetRemote) {
        (void)needGetIds.emplace(ReadKey(id));
    }
    if (!objectsNeedGetRemote.empty()) {
        PerfPoint pointRemote(PerfKey::WORKER_PROCESS_GET_OBJECT_REMOTE);
        std::unordered_set<std::string> failedIds;
        std::set<ReadKey> needRetryIds;
        Status status;
        do {
            needRetryIds.clear();
            status =
                workerOcImpl_->getProc_->ProcessObjectsNotExistInLocal(needGetIds, subTimeout, failedIds, needRetryIds);
            if (status.IsOk()) {
                break;
            }
            if (status.GetCode() == K_OUT_OF_MEMORY || reqTimeoutDuration.CalcRealRemainingTime() <= 0) {
                std::for_each(needRetryIds.begin(), needRetryIds.end(),
                              [&failedIds](const ReadKey &key) { failedIds.emplace(key.objectKey); });
                break;
            }
            needGetIds.swap(needRetryIds);
        } while (!needGetIds.empty());
        pointRemote.Record();
        if (status.GetCode() == K_OUT_OF_MEMORY) {
            return deviceReqManager_.ReturnFromGetDeviceObjectRequest(request, status);
        }
        if (status.IsError()) {
            // If the error is RPC error, return them directly, other error would be covered up as RUNTIME_ERROR.
            Status lastRc = IsRpcTimeoutOrTryAgain(status) ? status : Status(K_RUNTIME_ERROR, status.GetMsg());
            for (const auto &id : failedIds) {
                LOG_IF_ERROR(deviceReqManager_.UpdateRequestForFailed(id, lastRc), "UpdateRequestForFailed failed");
            }
        }
    }
    return Status::OK();
}

Status WorkerDeviceOcManager::UpdateRequestForSuccess(const ObjectKV &objectKV)
{
    return deviceReqManager_.UpdateRequestForSuccess(objectKV);
}

WorkerDeviceOcManager::WorkerDeviceOcManager(WorkerOCServiceImpl *impl)
{
    workerOcImpl_ = impl;
}

Status WorkerDeviceOcManager::ProcessGetP2PMetaRequest(
    GetP2PMetaReqPb &req,
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> &serverApi)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerOcImpl_->workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerOcImpl_->etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, GetP2P meta failed");
    return workerMasterApi->GetP2PMeta(req, serverApi, workerOcImpl_->asyncRpcManager_);
}

Status WorkerDeviceOcManager::ProcessSubscribeReceiveEventRequest(
    SubscribeReceiveEventReqPb &req,
    std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> &serverApi)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerOcImpl_->workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerOcImpl_->etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, send RootInfo failed");
    return workerMasterApi->SubscribeReceiveEvent(req, serverApi, workerOcImpl_->asyncRpcManager_);
}

Status WorkerDeviceOcManager::ProcessRecvRootInfoRequest(
    RecvRootInfoReqPb &req, std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> &serverApi)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerOcImpl_->workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerOcImpl_->etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, recv RootInfo failed");
    return workerMasterApi->RecvRootInfo(req, serverApi, workerOcImpl_->asyncRpcManager_);
}

Status WorkerDeviceOcManager::ProcessGetDataInfoRequest(
    GetDataInfoReqPb &req,
    std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
    const int64_t subTimeout)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerOcImpl_->workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerOcImpl_->etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                             "hash master get failed, get dataInfo failed");
    return workerMasterApi->GetDataInfo(req, serverApi, subTimeout, workerOcImpl_->asyncRpcManager_);
}
}  // namespace object_cache
}  // namespace datasystem
