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
 * Description: Defines the worker class to communicate with the worker service.
 */
#include "datasystem/worker/object_cache/worker_request_manager.h"
#include <chrono>
#include <memory>
#include <shared_mutex>
#include <thread>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/object_cache/buffer.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

namespace datasystem {
namespace object_cache {
std::function<Status(const std::string &, uint64_t)> WorkerRequestManager::deleteFunc_ = nullptr;

Status WorkerRequestManager::AddRequest(const std::string &objectKey, std::shared_ptr<GetRequest> &request)
{
    return requestTable_.AddRequest(objectKey, request);
}

Status WorkerRequestManager::UpdateRequestForSuccess(ReadObjectKV &objectKV,
                                                     std::shared_ptr<SharedMemoryRefTable> &memoryRefApi,
                                                     bool isDelayToReturn, const std::shared_ptr<GetRequest> &request)
{
    SafeObjType &safeObj = objectKV.GetObjEntry();
    CHECK_FAIL_RETURN_STATUS(safeObj.Get() != nullptr && memoryRefApi != nullptr, K_INVALID,
                             "The pointer of entry and memoryRefApi for UpdateRequest is null.");
    auto entry = GetObjEntryParams::Create(safeObj, objectKV.GetReadOffset(), objectKV.GetReadSize());
    return UpdateRequestImpl(objectKV.GetObjKey(), entry, memoryRefApi, Status::OK(), request, isDelayToReturn);
}

Status WorkerRequestManager::UpdateRequestForPublish(ObjectKV &objectKV,
                                                     std::shared_ptr<SharedMemoryRefTable> &memoryRefApi)
{
    SafeObjType &safeObj = objectKV.GetObjEntry();
    CHECK_FAIL_RETURN_STATUS(safeObj.Get() != nullptr && memoryRefApi != nullptr, K_INVALID,
                             "The pointer of entry and memoryRefApi for UpdateRequest is null.");
    auto entry = GetObjEntryParams::Create(safeObj, 0, 0);
    return UpdateRequestImpl(objectKV.GetObjKey(), entry, memoryRefApi);
}

Status WorkerRequestManager::UpdateRequestForFailed(const std::string &objectKey, Status lastRc,
                                                    std::shared_ptr<SharedMemoryRefTable> &memoryRefApi)
{
    CHECK_FAIL_RETURN_STATUS(memoryRefApi != nullptr, K_INVALID,
                             "The pointer memoryRefApi for UpdateFailedRequest is null.");

    CHECK_FAIL_RETURN_STATUS(lastRc.IsError(), K_INVALID, "The lastRc not failed.");
    return UpdateRequestImpl(objectKey, nullptr, memoryRefApi, lastRc);
}

Status WorkerRequestManager::UpdateSpecificRequestForFailed(const std::shared_ptr<GetRequest> &request,
                                                            const std::string &objectKey, Status lastRc,
                                                            std::shared_ptr<SharedMemoryRefTable> &memoryRefApi)
{
    CHECK_FAIL_RETURN_STATUS(memoryRefApi != nullptr, K_INVALID,
                             "The pointer memoryRefApi for UpdateFailedRequest is null.");

    CHECK_FAIL_RETURN_STATUS(lastRc.IsError(), K_INVALID, "The lastRc not failed.");
    return UpdateRequestImpl(objectKey, nullptr, memoryRefApi, lastRc, request);
}

Status WorkerRequestManager::UpdateRequestImpl(const std::string &objectKey, std::shared_ptr<GetObjEntryParams> entry,
                                               std::shared_ptr<SharedMemoryRefTable> &memoryRefApi, Status lastRc,
                                               const std::shared_ptr<GetRequest> &request, bool isDelayToReturn)
{
    auto checkFun =
        [&entry](const std::string &objKey, const std::shared_ptr<GetRequest> req) {
            OffsetInfo info(entry->readOffset, entry->readSize);
            return req->IsOffsetAndSizeMatchWithoutLock(objKey, entry->dataSize, info);
        };

    if (!isDelayToReturn) {
        return requestTable_.UpdateRequest(
            objectKey, entry, lastRc,
            [memoryRefApi, this](std::shared_ptr<GetRequest> req) mutable {
                VLOG(1) << "All object data ready for clientId: " + req->clientId_;
                LOG_IF_ERROR(ReturnFromGetRequest(req, memoryRefApi), "ReturnFromGetRequest failed");
            },
            request, false, checkFun);
    } else {
        // only check to set finish for finished request, not return to client.
        return requestTable_.UpdateRequest(
            objectKey, entry, lastRc,
            [memoryRefApi](std::shared_ptr<GetRequest> req) mutable {
                VLOG(1) << "All object data ready for clientId: " + req->clientId_;
                req->isFinished_ = true;
            },
            request, false, checkFun);
    }
}

void WorkerRequestManager::CheckAndReturnToClient(const std::string objectKey,
                                                  std::shared_ptr<SharedMemoryRefTable> &memoryRefApi)
{
    auto requests = requestTable_.GetRequestsByObject(objectKey);
    for (auto &request : requests) {
        if (request->isFinished_) {
            LOG_IF_ERROR(ReturnFromGetRequest(request, memoryRefApi), "return to client failed");
        }
    }
}

Status WorkerRequestManager::AddEntryToGetResponse(
    const std::shared_ptr<GetRequest> &request,
    const std::pair<std::string *, std::shared_ptr<GetObjEntryParams>> &retIdEntry, GetRspPb &resp,
    std::vector<RpcMessage> &outPayloads, std::shared_ptr<SharedMemoryRefTable> &memoryRefApi,
    std::map<std::string, uint64_t> &needDeleteObjects)
{
    const std::string &namespaceUri = *retIdEntry.first;
    std::string objectKey;
    TenantAuthManager::Instance()->NamespaceUriToObjectKey(namespaceUri, objectKey);
    auto safeEntry = retIdEntry.second;
    auto clientInfo = worker::ClientManager::Instance().GetClientInfo(request->clientId_);
    bool shmEnabled = clientInfo != nullptr && clientInfo->ShmEnabled();
    // Only add shm ref when we will return this shmUnit to the client.
    if (shmEnabled) {
        GetRspPb::ObjectInfoPb *object = resp.add_objects();
        SetObjectInfoPb(objectKey, *safeEntry, *object);
        // If object is shm, we increase the refCnt for client.
        // The client will be using this object and be responsible for releasing this object.
        if (worker::ClientManager::Instance().CheckClientId(request->clientId_).IsOk()) {
            memoryRefApi->AddShmUnit(request->clientId_, safeEntry->shmUnit);
        }
    } else {
        RETURN_IF_NOT_OK(CopyShmUnitToPayloads(retIdEntry, resp, outPayloads));
    }
    // If object is shm, must delete entry after memoryRefApi->AddShmUnit so that the ShmUnit won't be released
    // immediately when entry is releasing.
    bool needDeleted = safeEntry->objectState.IsNeedToDelete();
    INJECT_POINT("worker.AddEntryToGetResponse", [&needDeleted] {
        needDeleted = true;
        return Status::OK();
    });
    if (needDeleted) {
        needDeleteObjects.emplace(namespaceUri, safeEntry->version);
    }
    return Status::OK();
}

Status WorkerRequestManager::CopyShmUnitToPayloads(
    const std::pair<std::string *, std::shared_ptr<GetObjEntryParams>> &retIdEntry, GetRspPb &resp,
    std::vector<RpcMessage> &outPayloads)
{
    const std::string &namespaceUri = *retIdEntry.first;
    std::string objectKey;
    TenantAuthManager::Instance()->NamespaceUriToObjectKey(namespaceUri, objectKey);
    auto safeEntry = retIdEntry.second;
    const uint64_t metaSize = safeEntry->metaSize;
    const uint64_t dataSize = safeEntry->dataSize;
    const uint64_t readOffset = safeEntry->readOffset;
    const uint64_t readSize = safeEntry->readSize;

    ShmGuard shmGuard(safeEntry->shmUnit, dataSize, metaSize);
    if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            shmGuard.TryRLatch(),
            FormatString("Try read latch failed while getting object %s from shmUnit.", objectKey));
    }
    auto curIndex = outPayloads.size();
    LOG(INFO) << FormatString("CopyShmUnitToPayloads, objectKey: %s, read offset: %ld, read size: %ld", objectKey,
                              readOffset, readSize);
    RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayloads, readOffset, readSize));
    auto lastIndex = outPayloads.size();
    GetRspPb::PayloadInfoPb *payloadInfo = resp.add_payload_info();
    SetPayloadInfoPb(objectKey, *safeEntry, *payloadInfo);
    for (auto index = curIndex; index < lastIndex; index++) {
        payloadInfo->add_part_index(index);
    }
    return Status::OK();
}

void WorkerRequestManager::ConstructGetRsp(std::shared_ptr<GetRequest> &req, uint64_t &totalSize, Status &lastRc,
                                           std::shared_ptr<SharedMemoryRefTable> &memoryRefApi, GetRspPb &resp,
                                           std::vector<RpcMessage> &payloads,
                                           std::map<std::string, uint64_t> &needDeleteObjects)
{
    std::string objectKey;
    for (auto &namespaceUri : req->rawObjectKeys_) {
        TenantAuthManager::Instance()->NamespaceUriToObjectKey(namespaceUri, objectKey);
        GetRequest::TbbGetObjsTable::const_accessor accessor;
        bool isFindObj = false;
        if (req->objects_.find(accessor, namespaceUri) && accessor->second != nullptr) {
            isFindObj = true;
            totalSize += accessor->second->dataSize;
            lastRc = AddEntryToGetResponse(req, std::make_pair(&namespaceUri, accessor->second), resp, payloads,
                                           memoryRefApi, needDeleteObjects);
        }
        if (!isFindObj || lastRc.IsError()) {
            if (req->lastRc_.GetCode() != K_OUT_OF_MEMORY) {
                req->SetStatus(lastRc);
            }
            LOG(ERROR) << FormatString("Can't find object %s or AddEntryToGetResponse failed, clientId %s, rc %s",
                                       namespaceUri, req->clientId_, lastRc.ToString());
            GetRspPb::ObjectInfoPb *object = resp.add_objects();
            SetDefaultObjectInfoPb(objectKey, *object);
        }
    }
    resp.mutable_last_rc()->set_error_code(req->lastRc_.GetCode());
    resp.mutable_last_rc()->set_error_msg(req->lastRc_.GetMsg());
    VLOG(1) << FormatString("The total size of the currently get is %llu", totalSize);
}

Status WorkerRequestManager::ReturnFromGetRequest(std::shared_ptr<GetRequest> req,
                                                  std::shared_ptr<SharedMemoryRefTable> &memoryRefApi, Status lastRc)
{
    PerfPoint point(PerfKey::WORKER_RETURN_FROM_GET_REQUEST);
    RETURN_RUNTIME_ERROR_IF_NULL(req);
    bool expected = false;
    RETURN_OK_IF_TRUE(!req->isReturn_.compare_exchange_strong(expected, true));
    VLOG(1) << "Begin to ReturnFromGetRequest, client id: " << req->clientId_;

    uint64_t totalSize = 0;
    Raii raii([&totalSize, &req] {
        GetReqPb reqPb;
        RequestParam reqParam;
        reqParam.subTimeout = "0";
        if (req->serverApi_->Read(reqPb).IsOk()) {
            reqParam.subTimeout = std::to_string(reqPb.sub_timeout());
        }
        reqParam.objectKey = objectKeysToAbbrStr(req->rawObjectKeys_);
        StatusCode code = req->lastRc_.GetCode() == K_NOT_FOUND ? K_OK : req->lastRc_.GetCode();
        req->accessRecorderPoint_->Record(code, std::to_string(totalSize), reqParam, req->lastRc_.GetMsg());
    });
    std::map<std::string, uint64_t> needDeleteObjects;
    Raii deleteRaii([&needDeleteObjects] { DeleteObjects(needDeleteObjects); });
    std::lock_guard<std::mutex> lck(req->mutex_);
    req->SetStatus(lastRc);
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        LOG(ERROR) << "ReturnFromGetRequest timeout when get object: " << VectorToString(req->rawObjectKeys_);
        RemoveGetRequest(req);
        auto rc = req->lastRc_.IsOk() ? Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout") : req->lastRc_;
        req->SetStatus(rc);
        return req->serverApi_->SendStatus(rc);
    }
    GetRspPb resp;
    std::vector<RpcMessage> payloads;
    ConstructGetRsp(req, totalSize, lastRc, memoryRefApi, resp, payloads, needDeleteObjects);
    // Remove the get request from each of the relevant object_get_requests hash
    // tables if it is present there. It should only be present there if the get request timed out.
    RemoveGetRequest(req);

    // Close the request time out event.
    if (req->timer_ != nullptr) {
        if (!TimerQueue::GetInstance()->Cancel(*(req->timer_))) {
            LOG(ERROR) << "Failed to Cancel the timer: " << req->timer_->GetId();
        }
        req->timer_.reset();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(req->serverApi_->Write(resp), "Write reply to client stream failed.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(req->serverApi_->SendPayload(payloads), "SendPayload to client stream failed");
    return Status::OK();
}

void WorkerRequestManager::SetDefaultObjectInfoPb(const std::string &objectKey, GetRspPb::ObjectInfoPb &info)
{
    info.set_object_key(objectKey);
    info.set_store_fd(-1);
    info.set_offset(-1);
    info.set_data_size(-1);
    info.set_metadata_size(-1);
    info.set_mmap_size(-1);
    info.set_version(-1);
    info.set_is_seal(false);
    info.set_write_mode(static_cast<uint32_t>(WriteMode::NONE_L2_CACHE));
    info.set_consistency_type(static_cast<uint32_t>(ConsistencyType::PRAM));
}

void WorkerRequestManager::SetObjectInfoPb(const std::string &objectKey, GetObjEntryParams &safeEntry,
                                           GetRspPb::ObjectInfoPb &info)
{
    auto &shmUnit = safeEntry.shmUnit;
    info.set_object_key(objectKey);
    info.set_store_fd(shmUnit->GetFd());
    info.set_offset(static_cast<int64_t>(shmUnit->GetOffset()));
    info.set_data_size(static_cast<int64_t>(safeEntry.dataSize));
    info.set_metadata_size(static_cast<int64_t>(safeEntry.metaSize));
    info.set_mmap_size(static_cast<int64_t>(shmUnit->GetMmapSize()));
    info.set_version(static_cast<int64_t>(safeEntry.createTime));
    info.set_is_seal(safeEntry.isSealed);
    info.set_write_mode(static_cast<uint32_t>(safeEntry.objectMode.GetWriteMode()));
    info.set_consistency_type(static_cast<uint32_t>(safeEntry.objectMode.GetConsistencyType()));
    info.set_shm_id(shmUnit->id);
}

void WorkerRequestManager::SetPayloadInfoPb(const std::string &objectKey, GetObjEntryParams &safeEntry,
                                            GetRspPb::PayloadInfoPb &info)
{
    info.set_object_key(objectKey);
    info.set_data_size(static_cast<int64_t>(safeEntry.readSize));
    info.set_version(static_cast<int64_t>(safeEntry.createTime));
    info.set_is_seal(safeEntry.isSealed);
    info.set_write_mode(static_cast<uint32_t>(safeEntry.objectMode.GetWriteMode()));
    info.set_consistency_type(static_cast<uint32_t>(safeEntry.objectMode.GetConsistencyType()));
}

void WorkerRequestManager::RemoveGetRequest(std::shared_ptr<GetRequest> &request)
{
    VLOG(1) << "Begin to RemoveGetRequest, client id: " << request->clientId_;
    requestTable_.RemoveRequest(request);
}

void WorkerRequestManager::SetDeleteObjectsFunc(std::function<Status(const std::string &, uint64_t)> deleteFunc)
{
    deleteFunc_ = std::move(deleteFunc);
}

bool WorkerRequestManager::IsInGettingObject(const std::string &objectKey)
{
    return requestTable_.ObjectInRequest(objectKey);
}

void WorkerRequestManager::DeleteObjects(std::map<std::string, uint64_t> &objects)
{
    if (deleteFunc_ == nullptr) {
        LOG(ERROR) << "WorkerRequestManager deleteFunc not set.";
        return;
    }
    for (const auto &kv : objects) {
        // If two client get the same objectKey in the same time, may call delete object twice, and the second
        // call will
        // return fail because the object don't exist on objectTable_. So we don't handle the error when
        // delete.
        LOG_IF_ERROR(deleteFunc_(kv.first, kv.second), FormatString("delete object %s failed", kv.first));
    }
}
}  // namespace object_cache
}  // namespace datasystem
