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

#include "datasystem/common/device/ascend/hccl_comm_wrapper.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
HcclCommWrapper::HcclCommWrapper(const std::string &commId, int localDeviceId, int remoteDeviceId,
                                 std::shared_ptr<HcclCommMagr> &threadControl, AclResourceManager *aclResourceMgr)
    : CommWrapperBase(commId, localDeviceId, remoteDeviceId, threadControl, aclResourceMgr)
{
}

void HcclCommWrapper::ShutDown()
{
    if ((hcclCommState_ != HcclCommState::DESTROY)) {
        hcclCommState_ = HcclCommState::DESTROY;
        std::lock_guard<std::mutex> lock(mutex_);
        if (hasShutDown_) {
            return;
        }
        if (pool_) {
            auto traceId = Trace::Instance().GetTraceID();
            pool_->Submit([this, resource = resource_, traceId]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                LOG_IF_ERROR(
                    deviceImpl_->SynchronizeStreamWithTimeout(resource->PrimaryStream(), SYNC_STREAM_WAIT_TIMEOUT_MS),
                    "Timed out waiting for all tasks in Stream to complete, check that HcclRecv is not called");
                resource->Release();
                deviceImpl_->CommDestroy(GetRef());
                LOG(INFO) << "Destroy HcclComm ok, commId: " << commId_;
            });
        }
        (void)hcclThreadControl_->RemoveThreadPoolCommRecord(bindThreadId_, commId_);
        hasShutDown_ = true;
        pool_.reset();
        hcclThreadControl_.reset();
    }
}

HcclCommWrapper::~HcclCommWrapper()
{
    ShutDown();
}

Status HcclCommWrapper::InitHcclComm(int numRanks, CommRootInfo &rootInfo, int rank)
{
    LOG(INFO) << "InitHcclComm";
    commConnectTimestamp_ = std::chrono::steady_clock::now();
    hcclCommState_ = HcclCommState::CREATING;
    auto rc = deviceImpl_->CommInitRootInfo(numRanks, &rootInfo,
                                            rank, reinterpret_cast<void**>(&GetRef()));
    LOG_IF_ERROR(rc, "HcclCommInitRootInfo failed.");
    SetStatus(rc);
    return rc;
}

Status HcclCommWrapper::P2PSend(const std::vector<Blob> &blobs, const std::shared_ptr<AclRtEventWrapper> &event,
                                aclrtStream stream)
{
    LOG(INFO) << "hccl start to send " << (!blobs.empty() ? std::to_string(blobs[0].size) : "")
              << ", info num: " << blobs.size();
    (void)event;
    auto &comm = GetRef();
    RETURN_IF_NOT_OK(CheckHcclCommPtr(comm));
    for (size_t i = 0; i < blobs.size(); i++) {
        RETURN_IF_NOT_OK(deviceImpl_->CommSend(blobs[i].pointer, blobs[i].size, CommDataType::INT8,
                                               P2P_RECV_RANK, comm, stream));
    }
    VLOG(1) << "Send hccl ok";
    return Status::OK();
}

Status HcclCommWrapper::HcclGetCommAsyncError()
{
    // Don't check if comm is creating.
    if (hcclCommState_ == HcclCommState::CREATING || hcclCommState_ == HcclCommState::UNCREATE) {
          return Status::OK();
    }
    auto &comm = GetRef();
    return deviceImpl_->CommGetAsyncError(comm);
}

Status HcclCommWrapper::P2PRecv(const std::vector<Blob> &blobs, const std::shared_ptr<AclRtEventWrapper> &event,
                                aclrtStream stream)
{
    LOG(INFO) << "hccl receiving " << (!blobs.empty() ?  std::to_string(blobs[0].size) : "")
              << ", info num: " << blobs.size();
    (void)event;
    auto &comm = GetRef();
    RETURN_IF_NOT_OK(CheckHcclCommPtr(comm));
    for (size_t i = 0; i < blobs.size(); i++) {
        RETURN_IF_NOT_OK(deviceImpl_->CommRecv(blobs[i].pointer, blobs[i].size, CommDataType::INT8,
            P2P_SEND_RANK, comm, stream));
    }

    VLOG(1) << "Recv hccl ok";
    return Status::OK();
}

Status HcclCommWrapper::InitCommunicator(CommRootInfo &rootInfo, const HcclCommDirection direction, bool isSameNode)
{
    (void)isSameNode;
    InitPipeline(direction);
    if (direction == HcclCommDirection::SEND) {
        return InitHcclComm(P2P_RANK_NUM, rootInfo, P2P_SEND_RANK);
    }
    return InitHcclComm(P2P_RANK_NUM, rootInfo, P2P_RECV_RANK);
}

Status HcclCommWrapper::WarmUpComm(HcclCommDirection eventType)
{
    void *devPtr = nullptr;
    RETURN_IF_NOT_OK(deviceImpl_->MallocDeviceMemory(sizeof(char), devPtr));
    Raii raii([this, &devPtr]() { deviceImpl_->FreeDeviceMemory(devPtr); });
    std::shared_ptr<AclRtEventWrapper> event;
    if (eventType == HcclCommDirection::SEND) {
        RETURN_IF_NOT_OK(
            deviceImpl_->CommSend(devPtr, WARM_UP_DATA_COUNT, CommDataType::INT8, P2P_RECV_RANK, Get(), GetStream()));
    } else if (eventType == HcclCommDirection::RECV) {
        RETURN_IF_NOT_OK(
            deviceImpl_->CommRecv(devPtr, WARM_UP_DATA_COUNT, CommDataType::INT8, P2P_SEND_RANK, Get(), GetStream()));
    }
    RETURN_IF_NOT_OK(deviceImpl_->SynchronizeStream(GetStream()));
    LOG(INFO) << "communicator hccl warmup ok";
    return Status::OK();
}

Status HcclCommWrapper::CreateRootInfo(CommRootInfo &rootInfo)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(deviceImpl_->CommGetRootInfo(&rootInfo), "GetRootInfo failed.");
    return Status::OK();
}

Status HcclCommWrapper::CheckHcclCommPtr(const void *ptr)
{
    if (ptr == nullptr) {
        auto errorStatus = GetDetailStatus();
        return {K_RUNTIME_ERROR,
            FormatString("HcclComm is nullptr, create HCCL communication domain failed. Detail:%s", errorStatus)};
    }
    return Status::OK();
}
}  // namespace datasystem
