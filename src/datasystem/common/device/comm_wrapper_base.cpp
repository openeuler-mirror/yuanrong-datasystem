/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

#include "datasystem/common/device/comm_wrapper_base.h"

#include "datasystem/common/device/acl_pipeline_p2p_task.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {

CommWrapperBase::CommWrapperBase(const std::string &commId, int localDeviceId, int remoteDeviceId,
                                 std::shared_ptr<HcclCommMagr> &threadControl, DeviceResourceManager *resourceMgr)
    : resourceMgr_(resourceMgr),
      commId_(commId),
      localDeviceIdx_(localDeviceId),
      remoteDeviceIdx_(remoteDeviceId),
      commState_(CommState::UNCREATE),
      commThreadControl_(threadControl)
{
    deviceImpl_ = DeviceManagerFactory::GetDeviceManager();
    std::tie(bindThreadId_, pool_) = commThreadControl_->AssignThreadToComm(commId_);
    if (bindThreadId_ == -1 || pool_ == nullptr) {
        LOG(ERROR) << "Comm object init error with commId : " << commId;
        return;
    }
    resource_ = std::make_shared<acl::TwoPhaseAclPipeLineResource>();
    auto func = [this] {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(deviceImpl_->SetDevice(localDeviceIdx_), "Acl set device failed.");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(resource_->Init(localDeviceIdx_), "Init resource failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(resource_->NotifyStart(), "NotifyStart failed");
        return Status::OK();
    };
    pool_->Execute([func]() { (void)func(); });

    commDetailState_ = Status::OK();
}

CommWrapperBase::~CommWrapperBase()
{
}

aclrtStream CommWrapperBase::GetStream()
{
    return resource_->PrimaryStream();
}

bool CommWrapperBase::IsCommReady() const
{
    return commReady_.load();
}

void CommWrapperBase::SetCommReady(bool ready)
{
    std::lock_guard<std::mutex> lock(callbackMutex_);
    bool wasReady = commReady_.exchange(ready);
    if (ready && !wasReady) {
        ExecuteReadyCallbacks();
    }
}

void CommWrapperBase::ExecuteReadyCallbacks()
{
    std::vector<std::function<void()>> callbacksToExecute;
    callbacksToExecute = std::move(readyCallbacks_);
    readyCallbacks_.clear();

    for (auto &callback : callbacksToExecute) {
        callback();
    }
}

void CommWrapperBase::AddReadyCallback(std::function<void()> callback)
{
    std::lock_guard<std::mutex> lock(callbackMutex_);
    if (IsCommReady()) {
        callback();
    } else {
        readyCallbacks_.push_back(callback);
    }
}

void CommWrapperBase::SetStatus(const Status &commStatus)
{
    if (commStatus.IsOk()) {
        commState_ = CommState::VALID;
    } else {
        commState_ = CommState::INVALID;
    }
}

Status CommWrapperBase::GetDetailStatus() const
{
    std::lock_guard<std::mutex> lock(commDetailStateMutex_);
    return commDetailState_;
}

CommState CommWrapperBase::GetCommStatus() const
{
    return commState_;
}

void CommWrapperBase::SetDetailStatus(const Status &result)
{
    std::lock_guard<std::mutex> lock(commDetailStateMutex_);
    if (commDetailState_.IsOk()) {
        commDetailState_ = result;
    }
}

int CommWrapperBase::GetLocalDeviceId() const
{
    return localDeviceIdx_;
}

int CommWrapperBase::GetRemoteDeviceId() const
{
    return remoteDeviceIdx_;
}

std::string CommWrapperBase::GetCommId() const
{
    return commId_;
}

std::chrono::steady_clock::time_point CommWrapperBase::GetInitTimeStamp() const
{
    return commConnectTimestamp_;
}

Status CommWrapperBase::CheckHealth(uint32_t createTimeoutMs)
{
    // Detect the communication domain (timeout or fault).
    auto now = std::chrono::steady_clock::now();
    auto commDuration = std::chrono::duration_cast<std::chrono::milliseconds>(now - GetInitTimeStamp());
    // If the communication domain is not created
    auto returnRc = GetCommStatus();
    auto injectTest = [] {
        INJECT_POINT("client.CheckHealth.SetHcclError", [] { return true; });
        return false;
    };
    if (injectTest()) {
        returnRc = CommState::INVALID;
    }
    if (returnRc == CommState::CREATING) {
        if (commDuration.count() >= createTimeoutMs) {
            std::string errorMsg = FormatString("HcclComm with %s create timeout in %d ms", commId_, createTimeoutMs);
            return Status(K_HCCL_ERROR, errorMsg);
        }
        // created, and a fault is found when it is called
    } else if (returnRc == CommState::INVALID) {
        std::string errorMsg =
            FormatString("HcclComm with %s have error, HcclResult error code is %d", commId_, GetDetailStatus());
        return Status(K_HCCL_ERROR, errorMsg);
    }
    return Status::OK();
}

Status CommWrapperBase::InitPipeline(CommDirection direction)
{
    if (direction == CommDirection::SEND) {
        sender_ = std::make_unique<acl::PipeLineP2PSend>(static_cast<AclResourceManager *>(resourceMgr_));
        return sender_->Init(resource_);
    } else {
        receiver_ = std::make_unique<acl::PipeLineP2PRecv>(static_cast<AclResourceManager *>(resourceMgr_));
        return receiver_->Init(resource_);
    }
}

Status CommWrapperBase::CheckTranPointer(const void *pointer, const std::string &pointerName)
{
    if (pointer == nullptr) {
        auto rc = GetDetailStatus();
        std::string errMsg = FormatString(
            "The pointer [%s] is null, "
            "which usually indicates that the hccl communication domain creation failed. "
            "Specifically: [%s]",
            pointerName, rc.GetMsg());
        return Status(rc.GetCode(), errMsg);
    }
    return Status::OK();
}

Status CommWrapperBase::SubmitPipelineTask(acl::P2PSendTask task)
{
    RETURN_IF_NOT_OK(CheckTranPointer(sender_.get(), "sender_"));
    return sender_->Submit(std::move(task));
}

Status CommWrapperBase::SubmitPipelineTask(acl::P2PRecvTask task)
{
    RETURN_IF_NOT_OK(CheckTranPointer(receiver_.get(), "receiver_"));
    return receiver_->Submit(std::move(task));
}

}  // namespace datasystem
