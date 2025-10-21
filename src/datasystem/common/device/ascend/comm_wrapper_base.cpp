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

#include "datasystem/common/device/ascend/comm_wrapper_base.h"

#include "datasystem/common/device/ascend/acl_pipeline_p2p_task.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {

CommWrapperBase::CommWrapperBase(const std::string &commId, int localDeviceId, int remoteDeviceId,
                                 std::shared_ptr<HcclCommMagr> &threadControl, AclResourceManager *aclResourceMgr)
    : aclResourceMgr_(aclResourceMgr),
      commId_(commId),
      localDeviceIdx_(localDeviceId),
      remoteDeviceIdx_(remoteDeviceId),
      hcclCommState_(HcclCommState::UNCREATE),
      hcclThreadControl_(threadControl)
{
    aclImpl_ = acl::AclDeviceManager::Instance();
    std::tie(bindThreadId_, pool_) = hcclThreadControl_->AssignThreadToComm(commId_);
    if (bindThreadId_ == -1 || pool_ == nullptr) {
        LOG(ERROR) << "Comm object init error with commId : " << commId;
        return;
    }
    resource_ = std::make_shared<acl::TwoPhaseAclPipeLineResource>();
    auto func = [this] {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclImpl_->SetDeviceIdx(localDeviceIdx_), "Acl set device failed.");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(resource_->Init(localDeviceIdx_), "Init resource failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(resource_->NotifyStart(), "NotifyStart failed");
        return Status::OK();
    };
    pool_->Execute([func]() { (void)func(); });

    hcclDetailState_ = HCCL_SUCCESS;
}

CommWrapperBase::~CommWrapperBase()
{
}

aclrtStream CommWrapperBase::GetStream()
{
    return resource_->PrimaryStream();
}

void CommWrapperBase::SetStatus(const Status &commStatus)
{
    if (commStatus.IsOk()) {
        hcclCommState_ = HcclCommState::VALID;
    } else {
        hcclCommState_ = HcclCommState::INVALID;
    }
}

HcclResult CommWrapperBase::GetDetailStatus() const
{
    return hcclDetailState_;
}

HcclCommState CommWrapperBase::GetCommStatus() const
{
    return hcclCommState_;
}

void CommWrapperBase::SetHcclDetailState(HcclResult result)
{
    hcclDetailState_ = result;
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
        returnRc = HcclCommState::INVALID;
    }
    if (returnRc == HcclCommState::CREATING) {
        if (commDuration.count() >= createTimeoutMs) {
            std::string errorMsg = FormatString("HcclComm with %s create timeout in %d ms", commId_, createTimeoutMs);
            return Status(K_HCCL_ERROR, errorMsg);
        }
        // created, and a fault is found when it is called
    } else if (returnRc == HcclCommState::INVALID) {
        std::string errorMsg =
            FormatString("HcclComm with %s have error, HcclResult error code is %d", commId_, GetDetailStatus());
        return Status(K_HCCL_ERROR, errorMsg);
    }
    return Status::OK();
}

Status CommWrapperBase::InitPipeline(HcclCommDirection direction)
{
    if (direction == HcclCommDirection::SEND) {
        sender_ = std::make_unique<acl::PipeLineP2PSend>(aclResourceMgr_);
        return sender_->Init(resource_);
    } else {
        receiver_ = std::make_unique<acl::PipeLineP2PRecv>(aclResourceMgr_);
        return receiver_->Init(resource_);
    }
}

Status CommWrapperBase::SubmitPipelineTask(acl::P2PSendTask task)
{
    RETURN_RUNTIME_ERROR_IF_NULL(sender_);
    return sender_->Submit(std::move(task));
}

Status CommWrapperBase::SubmitPipelineTask(acl::P2PRecvTask task)
{
    RETURN_RUNTIME_ERROR_IF_NULL(receiver_);
    return receiver_->Submit(std::move(task));
}

}  // namespace datasystem
