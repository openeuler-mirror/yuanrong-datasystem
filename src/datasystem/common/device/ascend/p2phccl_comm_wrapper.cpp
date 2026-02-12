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
                                       std::shared_ptr<HcclCommMagr> &threadControl, DeviceResourceManager *resourceMgr)
    : CommWrapperBase(commId, localDeviceId, remoteDeviceId, threadControl, resourceMgr)
{
}

P2PHcclCommWrapper::~P2PHcclCommWrapper()
{
    ShutDown();
}

void P2PHcclCommWrapper::ShutDown()
{
    if ((commState_ != CommState::DESTROY)) {
        commState_ = CommState::DESTROY;
        if (pool_) {
            try {
                pool_->Execute([this, resource = resource_]() {
                    LOG_IF_ERROR(
                        deviceImpl_->SynchronizeStreamWithTimeout(resource->PrimaryStream(),
                                                                  SYNC_STREAM_WAIT_TIMEOUT_MS),
                        "Timed out waiting for all tasks in Stream to complete, check that HcclRecv is not called");
                    resource->Release();
                    deviceImpl_->P2PCommDestroy(GetRef());
                    LOG(INFO) << "Destroy HcclComm ok";
                });
            } catch (const std::exception &e) {
                LOG(ERROR) << e.what();
            }
        }
        (void)commThreadControl_->RemoveThreadPoolCommRecord(bindThreadId_, commId_);
    }
}

Status P2PHcclCommWrapper::InitP2PComm(const CommRootInfo *rootInfo, P2pKindBase kind, bool isSameNode)
{
    LOG(INFO) << "InitP2PComm";
    commConnectTimestamp_ = std::chrono::steady_clock::now();
    commState_ = CommState::CREATING;
    Status rc;
    if (isSameNode) {
        LOG(INFO) << "InitP2PComm HCCS dir: " << static_cast<int>(kind);
        rc = deviceImpl_->P2PCommInitRootInfo(rootInfo,
            kind, P2pLinkBase::HCCS, reinterpret_cast<void**>(&GetRef()));
    } else {
        LOG(INFO) << "InitP2PComm ROCE dir: " << static_cast<int>(kind);
        rc = deviceImpl_->P2PCommInitRootInfo(rootInfo,
            kind, P2pLinkBase::ROCE, reinterpret_cast<void**>(&GetRef()));
    }

    LOG_IF_ERROR(rc, "HcclCommInitRootInfo failed.");
    SetStatus(rc);
    return rc;
}

Status P2PHcclCommWrapper::P2PSend(const std::vector<Blob> &blobs,
                                   const std::shared_ptr<DeviceRtEventWrapper> &event,
                                   aclrtStream stream)
{
    LOG(INFO) << "p2phccl start to send " << (blobs.size() > 0 ?  std::to_string(blobs[0].size) : "")
              << ", info num: " << blobs.size();
    (void)event;
    auto &comm = GetRef();
    if (comm == nullptr) {
        return { K_RUNTIME_ERROR, "HcclComm is nullptr" };
    }
    for (size_t i = 0; i < blobs.size(); i++) {
        auto injectTest = [] {
            INJECT_POINT("client.P2PSend.skip_DSHcclSend", [] { return true; });
            return false;
        };
        if (injectTest()) {
            continue;
        }
        RETURN_IF_NOT_OK(deviceImpl_->P2PSend(blobs[i].pointer, blobs[i].size, CommDataType::INT8,
            comm, stream));
    }
    VLOG(1) << "Send hccl ok";
    return Status::OK();
}

Status P2PHcclCommWrapper::P2PRecv(const std::vector<Blob> &blobs, const std::shared_ptr<DeviceRtEventWrapper> &event,
                                   aclrtStream stream)
{
    LOG(INFO) << "p2phccl receiving " << (blobs.size() > 0 ?  std::to_string(blobs[0].size) : "")
              << ", info num: " << blobs.size();
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
    for (size_t i = 0; i < blobs.size(); i++) {
        RETURN_IF_NOT_OK(
            deviceImpl_->P2PRecv(blobs[i].pointer, blobs[i].size, CommDataType::INT8, comm, stream));
    }

    RETURN_IF_NOT_OK(event->RecordEvent(stream));
    VLOG(1) << "Recv hccl ok";
    return Status::OK();
}

Status P2PHcclCommWrapper::GetCommAsyncError()
{
    // Don't check if comm is creating.
    if (commState_ == CommState::CREATING || commState_ == CommState::UNCREATE) {
        return Status::OK();
    }
    auto &comm = GetRef();
    return deviceImpl_->P2PGetCommAsyncError(comm);
}

Status P2PHcclCommWrapper::InitCommunicator(CommRootInfo &rootInfo, const CommDirection direction, bool isSameNode)
{
    InitPipeline(direction);
    if (direction == CommDirection::SEND) {
        return InitP2PComm(&rootInfo, P2pKindBase::SENDER, isSameNode);
    }
    return InitP2PComm(&rootInfo, P2pKindBase::RECEIVER, isSameNode);
}

Status P2PHcclCommWrapper::WarmUpComm(CommDirection eventType)
{
    void *devPtr = nullptr;
    RETURN_IF_NOT_OK(deviceImpl_->MallocDeviceMemory(sizeof(char), devPtr));
    Raii raii([this, &devPtr]() { deviceImpl_->FreeDeviceMemory(devPtr); });
    std::shared_ptr<DeviceRtEventWrapper> event;
    if (eventType == CommDirection::SEND) {
        RETURN_IF_NOT_OK(deviceImpl_->P2PSend(devPtr, WARM_UP_DATA_COUNT, CommDataType::INT8, Get(), GetStream()));
    } else if (eventType == CommDirection::RECV) {
        RETURN_IF_NOT_OK(deviceImpl_->P2PRecv(devPtr, WARM_UP_DATA_COUNT, CommDataType::INT8, Get(), GetStream()));
    }
    RETURN_IF_NOT_OK(deviceImpl_->SynchronizeStream(GetStream()));
    LOG(INFO) << "communicator p2phccl warmup ok";
    return Status::OK();
}

Status P2PHcclCommWrapper::CreateRootInfo(CommRootInfo &rootInfo)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(deviceImpl_->P2PGetRootInfo(&rootInfo), "GetRootInfo failed.");
    return Status::OK();
}

}  // namespace datasystem
