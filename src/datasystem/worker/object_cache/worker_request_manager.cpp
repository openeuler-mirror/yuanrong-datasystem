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
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

namespace datasystem {
namespace object_cache {
std::function<Status(const std::string &, uint64_t)> WorkerRequestManager::deleteFunc_ = nullptr;

Status GetRequest::Init(const std::string &tenantId, const GetReqPb &req,
                        std::shared_ptr<SharedMemoryRefTable> shmRefTable,
                        std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> api)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");

    rawObjectKeys_ = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());

    // Get offset and size.
    uint64_t objectsCount = rawObjectKeys_.size();
    uint64_t readOffsetCount = static_cast<uint64_t>(req.read_offset_list_size());
    uint64_t readSizeCount = static_cast<uint64_t>(req.read_size_list_size());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        objectsCount == readOffsetCount || readOffsetCount == 0, K_INVALID,
        FormatString("Invalid readOffsetCount %zu, should be 0 or eqeal to objectCount %zu", readOffsetCount,
                     objectsCount));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        objectsCount == readSizeCount || readSizeCount == 0, K_INVALID,
        FormatString("Invalid readSizeCount %zu, should be 0 or eqeal to objectCount %zu", readSizeCount,
                     objectsCount));

    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        readOffsetCount == readSizeCount, K_INVALID,
        FormatString("readOffsetCount %zu should be the same with readSizeCount %zu", readOffsetCount, readSizeCount));

    clientId_ = req.client_id();
    subTimeout_ = req.sub_timeout();
    shmRefTable_ = std::move(shmRefTable);
    serverApi_ = std::move(api);
    noQueryL2Cache_ = req.no_query_l2cache();
    enableReturnObjectIndex_ = req.return_object_index();
    for (size_t i = 0; i < objectsCount; i++) {
        const auto &objectKey = rawObjectKeys_[i];
        OffsetInfo offsetInfo;
        if (readOffsetCount > 0 && readSizeCount > 0) {
            offsetInfo.readOffset = req.read_offset_list(static_cast<int>(i));
            offsetInfo.readSize = req.read_size_list(static_cast<int>(i));
        }
        GetObjInfo info{ .offsetInfo = offsetInfo, .params = nullptr, .rc = Status::OK() };
        auto [iter, insert] = objects_.emplace(objectKey, std::move(info));
        if (!insert) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(iter->second.offsetInfo == offsetInfo, K_INVALID,
                                                 FormatString("Duplicate offset read for objectKey %s", objectKey));
        }
        VLOG(1) << "objectKey " << objectKey << " add to GetRequest success";
    }

    return Status::OK();
}

Status GetRequest::UpdateAfterLocalGet(Status rc, size_t remoteObjectCount)
{
    CHECK_FAIL_RETURN_STATUS(!Registered(), K_RUNTIME_ERROR,
                             FormatString("UpdateAfterLocalGet called after GetRequest Register"));
    auto uniqueObjectCount = objects_.size();
    CHECK_FAIL_RETURN_STATUS(readyCount_ == 0, K_RUNTIME_ERROR,
                             FormatString("Invalid readyCount_ %zu, should be 0 when call UpdateAfterLocalGet"));
    CHECK_FAIL_RETURN_STATUS(uniqueObjectCount >= remoteObjectCount, K_RUNTIME_ERROR,
                             FormatString("The remote object key count %zu exceed the request object count %zu",
                                          remoteObjectCount, uniqueObjectCount));
    // exist in local or failed when get from local
    readyCount_ = uniqueObjectCount - remoteObjectCount;
    if (rc.IsError()) {
        lastRc_ = std::move(rc);
    }

    // Direct return to client if get all objects.
    return remoteObjectCount == 0 ? ReturnToClient() : Status::OK();
}

Status GetRequest::MarkSuccess(const ObjectKey &objectKey, SafeObjType &safeObj)
{
    VLOG(1) << "MarkSuccess for object key " << objectKey;
    auto params = GetObjEntryParams::Create(objectKey, safeObj);
    return MarkSuccessImpl(objectKey, std::move(params));
}

Status GetRequest::MarkFailed(const ObjectKey &objectKey, const Status &rc)
{
    VLOG(1) << "MarkFailed for object key " << objectKey;
    CHECK_FAIL_RETURN_STATUS(rc.IsError(), K_RUNTIME_ERROR, "Invalid Status when MarkFailed");
    auto iter = objects_.find(objectKey);
    CHECK_FAIL_RETURN_STATUS(iter != objects_.cend(), K_RUNTIME_ERROR,
                             FormatString("Not found object key %s in GetRequest", objectKey));
    readyCount_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> locker(mutex_);
        lastRc_ = rc;
        iter->second.rc = rc;
    }
    return Status::OK();
}

Status GetRequest::MarkSuccessForNotify(const ObjectKey &objectKey, std::unique_ptr<GetObjEntryParams> params)
{
    VLOG(1) << "MarkSuccessForNotify for object key " << objectKey;
    CHECK_FAIL_RETURN_STATUS(Registered(), K_RUNTIME_ERROR,
                             FormatString("MarkSuccessForNotify called before GetRequest Register"));
    RETURN_IF_NOT_OK(MarkSuccessImpl(objectKey, std::move(params)));
    return GetNotReadyCount() == 0 ? ReturnToClient() : Status::OK();
}

Status GetRequest::MarkSuccessImpl(const ObjectKey &objectKey, std::unique_ptr<GetObjEntryParams> params)
{
    auto iter = objects_.find(objectKey);
    CHECK_FAIL_RETURN_STATUS(iter != objects_.cend(), K_RUNTIME_ERROR,
                             FormatString("Not found object key %s in GetRequest", objectKey));
    {
        std::lock_guard<std::mutex> locker(mutex_);
        RETURN_OK_IF_TRUE(iter->second.params != nullptr);
        iter->second.params = std::move(params);
    }
    readyCount_.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

void GetRequest::SetStatus(const Status &rc)
{
    if (rc.IsError()) {
        lastRc_ = rc;
    }
}

size_t GetRequest::GetReadyCount() const
{
    return readyCount_;
}

size_t GetRequest::GetNotReadyCount() const
{
    return objects_.size() - readyCount_;
}

bool GetRequest::AlreadyReturn() const
{
    return isReturn_;
}

const std::string &GetRequest::GetClientId() const
{
    return clientId_;
}

bool GetRequest::NoQueryL2Cache() const
{
    return noQueryL2Cache_;
}

const std::vector<ObjectKey> &GetRequest::GetRawObjectKeys() const
{
    return rawObjectKeys_;
}

std::unordered_map<ObjectKey, GetObjInfo> &GetRequest::GetObjects()
{
    return objects_;
}

std::vector<ObjectKey> GetRequest::GetUniqueObjectkeys() const
{
    std::vector<ObjectKey> objectKeys;
    objectKeys.reserve(objects_.size());
    for (const auto &kv : objects_) {
        objectKeys.emplace_back(kv.first);
    }
    return objectKeys;
}

std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> GetRequest::GetServerApi() const
{
    return serverApi_;
}

void GetRequest::Register(WorkerRequestManager *workerRequestManager)
{
    workerRequestManager_ = workerRequestManager;
    auto request = shared_from_this();
    for (auto &[objectKey, objectInfo] : objects_) {
        // The object key not found in local and remote
        VLOG(1) << "Register GetRequest for objectKey " << objectKey << ", params "
                << (objectInfo.params == nullptr ? "is null" : "not null") << ", status: " << objectInfo.rc.ToString();
        if (objectInfo.params == nullptr && objectInfo.rc.IsOk()) {
            workerRequestManager_->AddRequest(objectKey, request);
        }
    }
}

void GetRequest::UnRegister()
{
    if (Registered()) {
        workerRequestManager_->RemoveGetRequest(shared_from_this());
    }
}

void GetRequest::SetTimer(std::unique_ptr<TimerQueue::TimerImpl> timer)
{
    std::lock_guard<std::mutex> locker(mutex_);
    timer_ = std::move(timer);
}

bool GetRequest::Registered() const
{
    return workerRequestManager_ != nullptr;
}

Status GetRequest::ReturnToClient(const Status &rc)
{
    INJECT_POINT("worker.Get.beforeReturn");
    PerfPoint point(PerfKey::WORKER_RETURN_FROM_GET_REQUEST);
    bool expected = false;
    RETURN_OK_IF_TRUE(!isReturn_.compare_exchange_strong(expected, true));
    VLOG(1) << "Begin to ReturnToClient, client id: " << clientId_;
    Status lastRc;
    {
        std::lock_guard<std::mutex> locker(mutex_);
        lastRc = lastRc_;
    }
    if (rc.IsError()) {
        lastRc = rc;
    }
    uint64_t totalSize = 0;
    Raii raii([this, &totalSize, &lastRc] {
        GetReqPb reqPb;
        RequestParam reqParam;
        reqParam.subTimeout = std::to_string(subTimeout_);
        reqParam.objectKey = ObjectKeysToAbbrStr(rawObjectKeys_);
        StatusCode code = lastRc.GetCode() == K_NOT_FOUND ? K_OK : lastRc.GetCode();
        recorder_.Record(code, std::to_string(totalSize), reqParam, lastRc.GetMsg());
    });
    std::map<std::string, uint64_t> needDeleteObjects;
    Raii deleteRaii([&needDeleteObjects] { WorkerRequestManager::DeleteObjects(needDeleteObjects); });
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        LOG(ERROR) << "ReturnFromGetRequest timeout when get object: " << VectorToString(rawObjectKeys_);
        UnRegister();
        auto rc = lastRc.IsOk() ? Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout") : lastRc;
        return serverApi_->SendStatus(rc);
    }
    GetRspPb resp;
    std::vector<RpcMessage> payloads;
    auto constructRc = ConstructResponse(totalSize, resp, payloads, needDeleteObjects);
    if (constructRc.IsError() && lastRc.GetCode() != K_OUT_OF_MEMORY) {
        lastRc = constructRc;
    }
    // Remove the get request from each of the relevant object_get_requests hash
    // tables if it is present there. It should only be present there if the get request timed out.
    UnRegister();

    {
        // Close the request time out event.
        std::lock_guard<std::mutex> locker(mutex_);
        if (timer_ != nullptr) {
            if (!TimerQueue::GetInstance()->Cancel(*timer_)) {
                LOG(ERROR) << "Failed to Cancel the timer: " << timer_->GetId();
            }
            timer_.reset();
        }
    }
    resp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi_->Write(resp), "Write reply to client stream failed.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi_->SendPayload(payloads), "SendPayload to client stream failed");
    return Status::OK();
}

Status GetRequest::ConstructResponse(uint64_t &totalSize, GetRspPb &resp, std::vector<RpcMessage> &payloads,
                                     std::map<std::string, uint64_t> &needDeleteObjects)
{
    auto clientInfo = worker::ClientManager::Instance().GetClientInfo(clientId_);
    bool shmEnabled = clientInfo != nullptr && clientInfo->ShmEnabled();
    Status lastRc;
    for (size_t objectIndex = 0; objectIndex < rawObjectKeys_.size(); objectIndex++) {
        auto &objectKeyUri = rawObjectKeys_[objectIndex];
        Status rc;
        auto iter = objects_.find(objectKeyUri);
        if (iter == objects_.cend() || iter->second.params == nullptr) {
            LOG(ERROR) << FormatString("Can't find object %s, clientId %s", objectKeyUri, clientId_);
            SetDefaultObjectInfoPb(objectKeyUri, objectIndex, *resp.add_objects());
            continue;
        }
        const auto &params = iter->second.params;
        totalSize += params->dataSize;
        rc = AddObjectToResponse(iter->first, iter->second, objectIndex, shmEnabled, resp, payloads);
        if (shmEnabled) {
            // If object is shm, we increase the refCnt for client.
            // The client will be using this object and be responsible for releasing this object.
            shmRefTable_->AddShmUnit(clientId_, params->shmUnit);
        }

        bool needDeleted = params->objectState.IsNeedToDelete();
        INJECT_POINT("worker.AddEntryToGetResponse", [&needDeleted] {
            needDeleted = true;
            return Status::OK();
        });
        if (needDeleted) {
            needDeleteObjects.emplace(objectKeyUri, params->version);
        }
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Can't find object %s or AddObjectToResponse failed, clientId %s, rc %s",
                                       objectKeyUri, clientId_, rc.ToString());
            lastRc = rc;
            SetDefaultObjectInfoPb(objectKeyUri, objectIndex, *resp.add_objects());
        }
    }
    VLOG(1) << FormatString("The total size of the currently get is %llu", totalSize);
    return lastRc;
}

Status GetRequest::AddObjectToResponse(const ObjectKey &objectKeyUri, GetObjInfo &objectInfo, size_t objectIndex,
                                       bool shmEnabled, GetRspPb &resp, std::vector<RpcMessage> &outPayloads)
{
    const auto &params = objectInfo.params;
    if (shmEnabled) {
        GetRspPb::ObjectInfoPb *object = resp.add_objects();
        SetShmObjectInfoPb(objectKeyUri, objectIndex, *params, *object);
        return Status::OK();
    }

    const uint64_t metaSize = params->metaSize;
    const uint64_t dataSize = params->dataSize;
    objectInfo.offsetInfo.AdjustReadSize(dataSize);
    const uint64_t readOffset = objectInfo.offsetInfo.readOffset;
    const uint64_t readSize = objectInfo.offsetInfo.readSize;

    ShmGuard shmGuard(params->shmUnit, dataSize, metaSize);
    if (WorkerOcServiceCrudCommonApi::ShmEnable()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            shmGuard.TryRLatch(),
            FormatString("Try read latch failed while getting object %s from shmUnit.", objectKeyUri));
    }
    auto curIndex = outPayloads.size();
    LOG(INFO) << FormatString("CopyShmUnitToPayloads, objectKey: %s, read offset: %ld, read size: %ld", objectKeyUri,
                              readOffset, readSize);
    RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayloads, readOffset, readSize));
    auto lastIndex = outPayloads.size();
    GetRspPb::PayloadInfoPb *payloadInfo = resp.add_payload_info();
    SetNoShmObjectInfoPb(objectKeyUri, objectIndex, objectInfo, *payloadInfo);
    for (auto index = curIndex; index < lastIndex; index++) {
        payloadInfo->add_part_index(index);
    }
    return Status::OK();
}

void GetRequest::SetShmObjectInfoPb(const ObjectKey &objectKeyUri, size_t objectIndex, GetObjEntryParams &safeEntry,
                                    GetRspPb::ObjectInfoPb &info)
{
    auto &shmUnit = safeEntry.shmUnit;
    if (enableReturnObjectIndex_) {
        info.set_object_index(objectIndex);
    } else {
        ObjectKey objectKey;
        TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKeyUri, objectKey);
        info.set_object_key(objectKey);
    }
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

void GetRequest::SetNoShmObjectInfoPb(const ObjectKey &objectKeyUri, size_t objectIndex, const GetObjInfo &objectInfo,
                                      GetRspPb::PayloadInfoPb &info)
{
    if (enableReturnObjectIndex_) {
        info.set_object_index(objectIndex);
    } else {
        ObjectKey objectKey;
        TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKeyUri, objectKey);
        info.set_object_key(objectKey);
    }
    const auto &safeEntry = *objectInfo.params;
    info.set_data_size(static_cast<int64_t>(objectInfo.offsetInfo.readSize));
    info.set_version(static_cast<int64_t>(safeEntry.createTime));
    info.set_is_seal(safeEntry.isSealed);
    info.set_write_mode(static_cast<uint32_t>(safeEntry.objectMode.GetWriteMode()));
    info.set_consistency_type(static_cast<uint32_t>(safeEntry.objectMode.GetConsistencyType()));
}

void GetRequest::SetDefaultObjectInfoPb(const ObjectKey &objectKeyUri, size_t objectIndex, GetRspPb::ObjectInfoPb &info)
{
    if (enableReturnObjectIndex_) {
        info.set_object_index(objectIndex);
    } else {
        ObjectKey objectKey;
        TenantAuthManager::Instance()->NamespaceUriToObjectKey(objectKeyUri, objectKey);
        info.set_object_key(objectKey);
    }
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

Status WorkerRequestManager::AddRequest(const std::string &objectKey, std::shared_ptr<GetRequest> &request)
{
    return requestTable_.AddRequest(objectKey, request);
}

Status WorkerRequestManager::NotifyPendingGetRequest(ObjectKV &objectKV)
{
    SafeObjType &safeObj = objectKV.GetObjEntry();
    CHECK_FAIL_RETURN_STATUS(safeObj.Get() != nullptr, K_INVALID,
                             "The pointer of entry and memoryRefApi for UpdateRequest is null.");
    auto params = GetObjEntryParams::Create(objectKV.GetObjKey(), safeObj);
    return requestTable_.NotifyPendingGetRequest(objectKV.GetObjKey(), std::move(params));
}

void WorkerRequestManager::RemoveGetRequest(const std::shared_ptr<GetRequest> &request)
{
    VLOG(1) << "Begin to RemoveGetRequest, client id: " << request->GetClientId();
    requestTable_.RemoveRequest(request);
}

void WorkerRequestManager::SetDeleteObjectsFunc(std::function<Status(const std::string &, uint64_t)> deleteFunc)
{
    deleteFunc_ = std::move(deleteFunc);
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
