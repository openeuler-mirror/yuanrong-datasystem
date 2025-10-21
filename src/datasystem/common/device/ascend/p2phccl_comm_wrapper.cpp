/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
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

#include "datasystem/common/device/ascend/p2phccl_comm_wrapper.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
P2PHcclCommWrapper::P2PHcclCommWrapper(const std::string &commId, int localDeviceId, int remoteDeviceId,
                                       std::shared_ptr<HcclCommMagr> &threadControl, AclResourceManager *aclResourceMgr)
    : CommWrapperBase(commId, localDeviceId, remoteDeviceId, threadControl, aclResourceMgr)
{
}

P2PHcclCommWrapper::~P2PHcclCommWrapper()
{
    ShutDown();
}

void P2PHcclCommWrapper::ShutDown()
{
    if ((hcclCommState_ != HcclCommState::DESTROY)) {
        hcclCommState_ = HcclCommState::DESTROY;
        if (pool_) {
            try {
                pool_->Execute([this, resource = resource_]() {
                    LOG_IF_ERROR(
                        aclImpl_->RtSynchronizeStreamWithTimeout(resource->PrimaryStream(),
                                                                 SYNC_STREAM_WAIT_TIMEOUT_MS),
                        "Timed out waiting for all tasks in Stream to complete, check that HcclRecv is not called");
                    resource->Release();
                    acl::AclDeviceManager::Instance()->DSP2PCommDestroy(GetRef());
                    LOG(INFO) << "Destroy HcclComm ok";
                });
            } catch (const std::exception &e) {
                LOG(ERROR) << e.what();
            }
        }
        (void)hcclThreadControl_->RemoveThreadPoolCommRecord(bindThreadId_, commId_);
    }
}

Status P2PHcclCommWrapper::InitP2PComm(const HcclRootInfo *rootInfo, P2pKind kind, bool isSameNode)
{
    LOG(INFO) << "InitP2PComm";
    commConnectTimestamp_ = std::chrono::steady_clock::now();
    hcclCommState_ = HcclCommState::CREATING;
    Status rc;
    if (isSameNode) {
        LOG(INFO) <<"InitP2PComm HCCS dir: "<< kind;
        rc = aclImpl_->DSP2PCommInitRootInfo(rootInfo, kind, P2pLink::P2P_LINK_HCCS, &GetRef());
    } else {
        LOG(INFO) <<"InitP2PComm ROCE dir: "<< kind;
        rc = aclImpl_->DSP2PCommInitRootInfo(rootInfo, kind, P2pLink::P2P_LINK_ROCE, &GetRef());
    }

    LOG_IF_ERROR(rc, "HcclCommInitRootInfo failed.");
    SetStatus(rc);
    return rc;
}

Status P2PHcclCommWrapper::P2PSend(const std::vector<DataInfo> &dataInfos,
                                   const std::shared_ptr<AclRtEventWrapper> &event, aclrtStream stream)
{
    LOG(INFO) << "p2phccl start to send " << (dataInfos.size() > 0 ? DataInfoToString(dataInfos[0]) : "")
              << ", info num: " << dataInfos.size();
    (void)event;
    auto &comm = GetRef();
    if (comm == nullptr) {
        return { K_RUNTIME_ERROR, "HcclComm is nullptr" };
    }
    for (size_t i = 0; i < dataInfos.size(); i++) {
        auto injectTest = [] {
            INJECT_POINT("client.P2PSend.skip_DSHcclSend", [] { return true; });
            return false;
        };
        if (injectTest()) {
            continue;
        }
        RETURN_IF_NOT_OK(aclImpl_->DSP2PSend(dataInfos[i].devPtr, dataInfos[i].count,
                                             static_cast<HcclDataType>(dataInfos[i].dataType), comm, stream));
    }
    VLOG(1) << "Send hccl ok";
    return Status::OK();
}

Status P2PHcclCommWrapper::P2PRecv(const std::vector<DataInfo> &dataInfos,
                                   const std::shared_ptr<AclRtEventWrapper> &event, aclrtStream stream)
{
    LOG(INFO) << "p2phccl receiving " << (dataInfos.size() > 0 ? DataInfoToString(dataInfos[0]) : "")
              << ", info num: " << dataInfos.size();
    auto &comm = GetRef();
    if (comm == nullptr) {
        return { K_RUNTIME_ERROR, "HcclComm is nullptr" };
    }

    auto timeoutInjectTest = [] {
        INJECT_POINT("client.P2PRecv.timeoutMs", []() {
            LOG(ERROR) << "timeoutInjectTest true";
            return true;
        });
        return false;
    };
    if (timeoutInjectTest()) {
        int eightS = 8;
        std::this_thread::sleep_for(std::chrono::seconds(eightS));
    }
    for (size_t i = 0; i < dataInfos.size(); i++) {
        RETURN_IF_NOT_OK(aclImpl_->DSP2PRecv(dataInfos[i].devPtr, dataInfos[i].count,
                                             static_cast<HcclDataType>(dataInfos[i].dataType), comm, stream));
    }

    RETURN_IF_NOT_OK(event->RecordEvent(stream));
    VLOG(1) << "Recv hccl ok";
    return Status::OK();
}

HcclResult P2PHcclCommWrapper::HcclGetCommAsyncError()
{
    auto &comm = GetRef();
    // Don't check if comm is creating.
    if (hcclCommState_ == HcclCommState::CREATING || hcclCommState_ == HcclCommState::UNCREATE) {
        return HCCL_SUCCESS;
    }
    HcclResult asyncError;
    aclImpl_->DSP2PGetCommAsyncError(comm, &asyncError);
    return asyncError;
}

Status P2PHcclCommWrapper::InitCommunicator(HcclRootInfo &rootInfo, const HcclCommDirection direction, bool isSameNode)
{
    InitPipeline(direction);
    if (direction == HcclCommDirection::SEND) {
        return InitP2PComm(&rootInfo, P2pKind::P2P_SENDER, isSameNode);
    }
    return InitP2PComm(&rootInfo, P2pKind::P2P_RECEIVER, isSameNode);
}

Status P2PHcclCommWrapper::WarmUpComm(HcclCommDirection eventType)
{
    void *devPtr = nullptr;
    RETURN_IF_NOT_OK(aclImpl_->MallocDeviceMemory(sizeof(char), devPtr));
    Raii raii([this, &devPtr]() { aclImpl_->FreeDeviceMemory(devPtr); });
    std::shared_ptr<AclRtEventWrapper> event;
    if (eventType == HcclCommDirection::SEND) {
        RETURN_IF_NOT_OK(aclImpl_->DSP2PSend(devPtr, WARM_UP_DATA_COUNT, HCCL_DATA_TYPE_INT8, Get(), GetStream()));
    } else if (eventType == HcclCommDirection::RECV) {
        RETURN_IF_NOT_OK(aclImpl_->DSP2PRecv(devPtr, WARM_UP_DATA_COUNT, HCCL_DATA_TYPE_INT8, Get(), GetStream()));
    }
    RETURN_IF_NOT_OK(aclImpl_->RtSynchronizeStream(GetStream()));
    LOG(INFO) << "communicator p2phccl warmup ok";
    return Status::OK();
}

Status P2PHcclCommWrapper::CreateRootInfo(HcclRootInfo &rootInfo)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclImpl_->DSP2PGetRootInfo(&rootInfo), "GetRootInfo failed.");
    return Status::OK();
}

}  // namespace datasystem
