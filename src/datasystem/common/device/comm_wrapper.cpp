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

#include "datasystem/common/device/comm_wrapper.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
CommWrapper::CommWrapper(const std::string &commId, int localDeviceId, int remoteDeviceId,
                         std::shared_ptr<HcclCommMagr> &threadControl, DeviceResourceManager *resourceMgr)
    : CommWrapperBase(commId, localDeviceId, remoteDeviceId, threadControl, resourceMgr)
{
}

void CommWrapper::ShutDown()
{
    if ((commState_ != CommState::DESTROY)) {
        commState_ = CommState::DESTROY;
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
                    "Timed out waiting for all tasks in Stream to complete, check that Recv is not called");
                resource->Release();
                deviceImpl_->CommDestroy(GetRef());
                LOG(INFO) << "Destroy Comm ok, commId: " << commId_;
            });
        }
        (void)commThreadControl_->RemoveThreadPoolCommRecord(bindThreadId_, commId_);
        hasShutDown_ = true;
        pool_.reset();
        commThreadControl_.reset();
    }
}

CommWrapper::~CommWrapper()
{
    ShutDown();
}

Status CommWrapper::InitComm(int numRanks, CommRootInfo &rootInfo, int rank)
{
    LOG(INFO) << "InitComm";
    commConnectTimestamp_ = std::chrono::steady_clock::now();
    commState_ = CommState::CREATING;
    auto rc = deviceImpl_->CommInitRootInfo(numRanks, &rootInfo,
                                            rank, reinterpret_cast<void**>(&GetRef()));
    LOG_IF_ERROR(rc, "CommInitRootInfo failed.");
    SetStatus(rc);
    return rc;
}

Status CommWrapper::P2PSend(const std::vector<Blob> &blobs,
                            const std::shared_ptr<DeviceRtEventWrapper> &event,
                            aclrtStream stream)
{
    LOG(INFO) << "comm start to send " << (!blobs.empty() ? std::to_string(blobs[0].size) : "")
              << ", info num: " << blobs.size();
    (void)event;
    auto &comm = GetRef();
    RETURN_IF_NOT_OK(CheckCommPtr(comm));
    for (size_t i = 0; i < blobs.size(); i++) {
        RETURN_IF_NOT_OK(deviceImpl_->CommSend(blobs[i].pointer, blobs[i].size, CommDataType::INT8,
                                               P2P_RECV_RANK, comm, stream));
    }
    VLOG(1) << "Send comm ok";
    return Status::OK();
}

Status CommWrapper::GetCommAsyncError()
{
    // Don't check if comm is creating.
    if (commState_ == CommState::CREATING || commState_ == CommState::UNCREATE) {
        return Status::OK();
    }
    auto &comm = GetRef();
    return deviceImpl_->CommGetAsyncError(comm);
}

Status CommWrapper::P2PRecv(const std::vector<Blob> &blobs,
                            const std::shared_ptr<DeviceRtEventWrapper> &event,
                            aclrtStream stream)
{
    LOG(INFO) << "comm receiving " << (!blobs.empty() ?  std::to_string(blobs[0].size) : "")
              << ", info num: " << blobs.size();
    (void)event;
    auto &comm = GetRef();
    RETURN_IF_NOT_OK(CheckCommPtr(comm));
    for (size_t i = 0; i < blobs.size(); i++) {
        RETURN_IF_NOT_OK(deviceImpl_->CommRecv(blobs[i].pointer, blobs[i].size, CommDataType::INT8,
            P2P_SEND_RANK, comm, stream));
    }

    VLOG(1) << "Recv comm ok";
    return Status::OK();
}

Status CommWrapper::InitCommunicator(CommRootInfo &rootInfo, const CommDirection direction, bool isSameNode)
{
    (void)isSameNode;
    InitPipeline(direction);
    if (direction == CommDirection::SEND) {
        return InitComm(P2P_RANK_NUM, rootInfo, P2P_SEND_RANK);
    }
    return InitComm(P2P_RANK_NUM, rootInfo, P2P_RECV_RANK);
}

Status CommWrapper::WarmUpComm(CommDirection eventType)
{
    void *devPtr = nullptr;
    RETURN_IF_NOT_OK(deviceImpl_->MallocDeviceMemory(sizeof(char), devPtr));
    Raii raii([this, &devPtr]() { deviceImpl_->FreeDeviceMemory(devPtr); });
    std::shared_ptr<DeviceRtEventWrapper> event;
    if (eventType == CommDirection::SEND) {
        RETURN_IF_NOT_OK(
            deviceImpl_->CommSend(devPtr, WARM_UP_DATA_COUNT, CommDataType::INT8, P2P_RECV_RANK, Get(), GetStream()));
    } else if (eventType == CommDirection::RECV) {
        RETURN_IF_NOT_OK(
            deviceImpl_->CommRecv(devPtr, WARM_UP_DATA_COUNT, CommDataType::INT8, P2P_SEND_RANK, Get(), GetStream()));
    }
    RETURN_IF_NOT_OK(deviceImpl_->SynchronizeStream(GetStream()));
    LOG(INFO) << "communicator warmup ok";
    return Status::OK();
}

Status CommWrapper::CreateRootInfo(CommRootInfo &rootInfo)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(deviceImpl_->CommGetRootInfo(&rootInfo), "GetRootInfo failed.");
    return Status::OK();
}

Status CommWrapper::CheckCommPtr(const void *ptr)
{
    if (ptr == nullptr) {
        auto errorStatus = GetDetailStatus();
        return {K_RUNTIME_ERROR,
            FormatString("Comm is nullptr, create communication domain failed. Detail:%s", errorStatus)};
    }
    return Status::OK();
}
}  // namespace datasystem
