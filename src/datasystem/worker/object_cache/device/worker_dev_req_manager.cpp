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
 * Description: Defines the class to manager device get request.
 */
#include "datasystem/worker/object_cache/device/worker_dev_req_manager.h"

#include <memory>
#include <vector>

#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

DS_DECLARE_uint64(oc_shm_transfer_threshold_kb);
namespace datasystem {
namespace object_cache {

Status WorkerDevReqManager::AddRequest(const std::string objectKey, std::shared_ptr<GetDeviceObjectRequest> &request)
{
    return requestTable_.AddRequest(objectKey, request);
}

void WorkerDevReqManager::RemoveRequest(std::shared_ptr<GetDeviceObjectRequest> &request)
{
    VLOG(1) << "Begin to remove GetDeviceObjectRequest, client id: " << request->clientId_;
    return requestTable_.RemoveRequest(request);
}

Status WorkerDevReqManager::UpdateRequestForSuccess(const ObjectKV &objectKV)
{
    auto param = DeviceObjEntryParams::ConstructDeviceObjEntryParams(objectKV.GetObjEntry());
    return UpdateRequestImpl(objectKV.GetObjKey(), param, Status::OK());
}

Status WorkerDevReqManager::UpdateRequestForFailed(const std::string objectKey, const Status &lastRc)
{
    return UpdateRequestImpl(objectKey, nullptr, lastRc);
}

Status WorkerDevReqManager::UpdateRequestImpl(const std::string &objectKey,
                                              std::shared_ptr<DeviceObjEntryParams> entryParam, Status lastRc)
{
    requestTable_.UpdateRequest(objectKey, entryParam, lastRc, [this](std::shared_ptr<GetDeviceObjectRequest> req) {
        LOG_IF_ERROR(ReturnFromGetDeviceObjectRequest(req), "ReturnFromGetDeviceObjectRequest failed");
    });
    return Status::OK();
}

Status WorkerDevReqManager::ReturnFromGetDeviceObjectRequest(std::shared_ptr<GetDeviceObjectRequest> req, Status lastRc)
{
    RETURN_RUNTIME_ERROR_IF_NULL(req);
    bool expected = false;
    RETURN_OK_IF_TRUE(!req->isReturn_.compare_exchange_strong(expected, true));

    VLOG(1) << "Begin to ReturnFromGetDeviceObjectRequest, client id: " << req->clientId_;

    std::lock_guard<std::mutex> lck(req->mutex_);
    req->SetStatus(lastRc);
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (remainingTimeMs <= 0) {
        RemoveRequest(req);
        auto rc = req->lastRc_.IsOk() ? Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout") : req->lastRc_;
        return req->serverApi_->SendStatus(rc);
    }

    GetDeviceObjectRspPb resp;
    std::vector<RpcMessage> payloads;
    for (auto &namespaceUri : req->rawObjectKeys_) {
        std::string objectKey;
        TenantAuthManager::Instance()->NamespaceUriToObjectKey(namespaceUri, objectKey);
        GetDeviceObjectRequest::TbbGetObjsTable::const_accessor accessor;
        bool isFindObj = false;
        if (req->objects_.find(accessor, namespaceUri) && accessor->second != nullptr) {
            isFindObj = true;
            lastRc = AddEntryToGetDeviceObjectResponse(req, objectKey, accessor->second, resp, payloads);
        }
        if (!isFindObj || lastRc.IsError()) {
            if (req->lastRc_.GetCode() != K_OUT_OF_MEMORY) {
                req->SetStatus(lastRc);
            }
            LOG(ERROR) << FormatString("Can't find object %s or AddEntryToGetResponse failed, clientId %s, rc %s",
                                       namespaceUri, req->clientId_, lastRc.ToString());
        }
    }
    resp.mutable_last_rc()->set_error_code(req->lastRc_.GetCode());
    resp.mutable_last_rc()->set_error_msg(req->lastRc_.GetMsg());
    RemoveRequest(req);

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

Status WorkerDevReqManager::AddEntryToGetDeviceObjectResponse(const std::shared_ptr<GetDeviceObjectRequest> &request,
                                                              const std::string objectKey,
                                                              std::shared_ptr<DeviceObjEntryParams> param,
                                                              GetDeviceObjectRspPb &resp,
                                                              std::vector<RpcMessage> &outPayloads)
{
    auto clientInfo = worker::ClientManager::Instance().GetClientInfo(request->clientId_);
    bool shmEnabled = clientInfo != nullptr && clientInfo->ShmEnabled();
    const uint64_t dataSize = param->dataSize;
    auto entryShmUnit = param->shmUnit;
    auto metaSize = param->metaSize;
    // Only add shm ref when we will return this shmUnit to the client.
    if (shmEnabled && dataSize >= FLAGS_oc_shm_transfer_threshold_kb * KB) {
        auto *shmInfo = resp.mutable_shm_info();
        shmInfo->set_shm_id(entryShmUnit->GetId());
        shmInfo->set_store_fd(entryShmUnit->GetFd());
        shmInfo->set_offset(entryShmUnit->GetOffset());
        shmInfo->set_mmap_size(entryShmUnit->GetMmapSize());
        shmInfo->set_metadata_size(metaSize);
    } else {
        ShmGuard shmGuard(entryShmUnit, dataSize, metaSize);
        if (WorkerOcServiceCrudCommonApi::CanTransferByShm(dataSize)) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                shmGuard.TryRLatch(),
                FormatString("Try read latch failed while getting object %s from shmUnit.", objectKey));
        }
        RETURN_IF_NOT_OK(shmGuard.TransferTo(outPayloads));
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem