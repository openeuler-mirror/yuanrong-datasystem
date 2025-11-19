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
#include "datasystem/client/object_cache/device/hccl_comm_factory.h"

#include <memory>
#include <mutex>

#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/hccl_comm_wrapper.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Print the root info, rootInfo include some control character so it can't print directly.
 * @param[in] rootInfo The HcclRootInfo reference.
 */

template <typename T>
inline static void PrintRootInfo(const T &rootInfo)
{
    std::stringstream forPrint;
    for (char i : rootInfo.internal) {
        if (i >= '!' && i <= '`') {
            forPrint << i;
        }
    }
    VLOG(1) << "[RootInfo]:" << forPrint.str();
}

HcclCommFactory::HcclCommFactory(std::shared_ptr<object_cache::ClientWorkerApi> workerApi,
                                 AclResourceManager *aclResourceMgr)
    : ClientDeviceCurd(std::move(workerApi)), aclResourceMgr_(aclResourceMgr)
{
    hcclThreadControl_ = std::make_shared<HcclCommMagr>();
}

void HcclCommFactory::ShutDown()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    for (auto &commPair : commTable_) {
        commPair.second->ShutDown();
    }
    commTable_.clear();
}

HcclCommFactory::~HcclCommFactory()
{
    ShutDown();
}

Status HcclCommFactory::SetStateIfError(std::shared_ptr<CommWrapperBase> &comm, Status status)
{
    if (status.IsError()) {
        comm->SetHcclDetailState(status);
        return status;
    }
    return Status::OK();
}

std::string HcclCommFactory::GetHcclCommKey(P2PEventType eventType, int32_t localDeviceId,
                                            const std::string &remoteClientId, int32_t remoteDeviceId)
{
    auto splitStr = "==";
    std::stringstream ss;
    ss << static_cast<int>(eventType) << splitStr << localDeviceId << splitStr << remoteDeviceId << splitStr
       << remoteClientId;
    return ss.str();
}

Status HcclCommFactory::GetOrCreateHcclComm(P2PEventType eventType, int32_t localDeviceId,
                                            const std::string &remoteClientId, int32_t remoteDeviceId, bool isSameNode,
                                            bool enableP2Ptransfer, std::shared_ptr<CommWrapperBase> &comm)
{
    PerfPoint perfPoint(PerfKey::GET_OR_CREATE_HCCL_COMMONE);
    auto commKey = GetHcclCommKey(eventType, localDeviceId, remoteClientId, remoteDeviceId);
    TbbHcclCommTable::accessor acc;
    VLOG(1) << FormatString("Trying to acquire read lock, commKey: %s", commKey);
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (commTable_.find(acc, commKey)) {
        comm = acc->second;
        return CreateHcclCommCheckError(comm);
    }

    // If insert failed, mean the hccl comm exist, get it and return.
    if (!commTable_.insert(acc, commKey)) {
        comm = acc->second;
        return CreateHcclCommCheckError(comm);
    }

    if (enableP2Ptransfer) {
        LOG(INFO) << "used p2phccl comm";
        comm = std::make_shared<P2PHcclCommWrapper>(commKey, localDeviceId, remoteDeviceId, hcclThreadControl_,
                                                    aclResourceMgr_);
        acc->second = comm;
        if (eventType == P2PEventType::SEND) {
            CreateHcclCommInSend(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
            return CreateHcclCommCheckError(comm);
        }
        CreateHcclCommInRecv(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
        return CreateHcclCommCheckError(comm);
    }
    LOG(INFO) << "used hccl comm";
    comm =
        std::make_shared<HcclCommWrapper>(commKey, localDeviceId, remoteDeviceId, hcclThreadControl_, aclResourceMgr_);
    acc->second = comm;
    if (eventType == P2PEventType::SEND) {
        CreateHcclCommInSend(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
        return CreateHcclCommCheckError(comm);
    }
    CreateHcclCommInRecv(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
    return CreateHcclCommCheckError(comm);
}

void HcclCommFactory::CreateHcclCommInSend(int32_t localDeviceId, const std::string &remoteClientId,
                                           int32_t remoteDeviceId, bool isSameNode,
                                           std::shared_ptr<CommWrapperBase> &comm)
{
    auto traceId = Trace::Instance().GetTraceID();
    auto processFunc = [this, comm, localDeviceId, remoteDeviceId, remoteClientId, isSameNode, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        INJECT_POINT("CreateHcclCommInSend.sleep");
        PerfPoint point(PerfKey::CLIENT_CREATE_HCCL_IN_SEND);
        auto localClientId = clientWorkerApi_->GetClientId();
        auto peerId = GetHcclPeerId(localClientId, localDeviceId, remoteClientId, remoteDeviceId);
        VLOG(1) << FormatString("[Sender] Sender try to acquire write lock, peerId: %s", peerId);
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);

        LOG(INFO) << FormatString("[Sender] Start to recv RootInfo from worker, peerId: %s", peerId);
        RecvRootInfoReqPb rootInfoReq;
        rootInfoReq.set_dst_client_id(remoteClientId);
        rootInfoReq.set_dst_device_id(remoteDeviceId);
        rootInfoReq.set_src_client_id(localClientId);
        rootInfoReq.set_src_device_id(localDeviceId);
        RecvRootInfoRspPb rootInfoResp;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerApi_->RecvRootInfo(rootInfoReq, rootInfoResp),
                                         "Failed with receive");
        if (rootInfoResp.is_dead_lock()) {
            std::string msg = "[Sender] Deadlock detected, releasing lock and retrying.";
            LOG(WARNING) << msg;
            RETURN_IF_NOT_OK(SetStateIfError(comm, Status(K_CLIENT_DEADLOCK, msg)));
        }

        HcclRootInfo rootInfo;
        if (rootInfoResp.root_info().length() != sizeof(rootInfo.internal)) {
            std::string msg = FormatString(
                "The rsp rootInfo size is not as expected: %d, which usually indicates that the receiver "
                "did not send the rootInfo properly. Check if there are any errors on the receiver side, peerId: %s",
                rootInfoResp.root_info().length(),
                peerId);
            RETURN_IF_NOT_OK(SetStateIfError(comm, Status(K_RUNTIME_ERROR, msg)));
        }
        auto ret = memcpy_s(static_cast<void *>(rootInfo.internal),
            sizeof(rootInfo.internal),
            static_cast<const void *>(rootInfoResp.root_info().c_str()),
            rootInfoResp.root_info().length());
        if (ret != EOK) {
            RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Copy root info failed, the memcpy_s return: %d", ret));
        }
        LOG(INFO) << "[Sender] Start init hccl comm";
        PrintRootInfo(rootInfo);
        auto rc = comm->InitCommunicator(rootInfo, HcclCommDirection::SEND, isSameNode);
        RETURN_IF_NOT_OK(SetStateIfError(comm, rc));
        PerfPoint perfPoint(PerfKey::CLIENT_HCCL_WARMUP_IN_SEND);
        return comm->WarmUpComm(HcclCommDirection::SEND);
    };

    auto commPtr = comm;
    comm->Execute([this, commPtr, processFunc, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        constexpr int32_t timeoutMs = 60 * 1000;
        this->AsyncRetryWithTimeout(
            commPtr, processFunc, [this](auto comm) { return this->CreateHcclCommCheckError(comm); }, timeoutMs,
            { StatusCode::K_CLIENT_DEADLOCK },
            [this](auto comm, Status result, Status checkRc) {
                this->StatusComparisonWithSetStatus(comm, result, checkRc);
            });
    });
}

void HcclCommFactory::StatusComparisonWithSetStatus(std::shared_ptr<CommWrapperBase> &comm, Status processStatus,
                                                    Status checkErrorStatus)
{
    if (processStatus.IsOk() && checkErrorStatus.IsOk()) {
        comm->SetStatus(Status::OK());
    } else if (processStatus.IsError()) {
        comm->SetStatus(processStatus);
    } else if (checkErrorStatus.IsError()) {
        comm->SetStatus(checkErrorStatus);
    }
}

Status HcclCommFactory::CreateHcclCommCheckError(std::shared_ptr<CommWrapperBase> &comm)
{
    auto status = comm->HcclGetCommAsyncError();
    comm->SetHcclDetailState(status);
    return status;
}

void HcclCommFactory::AsyncRetryWithTimeout(
    std::shared_ptr<CommWrapperBase> comm, std::function<Status()> processFunc,
    std::function<Status(std::shared_ptr<CommWrapperBase>)> errorCheckFunc, int32_t timeoutMs,
    const std::vector<StatusCode> retryableErrors,
    std::function<void(std::shared_ptr<CommWrapperBase>, Status, Status)> finalHandler)
{
    auto startTime = std::chrono::steady_clock::now();
    auto retryFunction = std::make_shared<std::function<void()>>();
    *retryFunction = [comm, processFunc, errorCheckFunc, startTime, timeoutMs, retryableErrors, finalHandler,
                      retryFunction]() mutable {
        // Execute processing function
        Status result = processFunc();

        // Check if error is retriable
        bool shouldRetry = false;
        for (auto errorCode : retryableErrors) {
            if (result.GetCode() == errorCode) {
                shouldRetry = true;
                break;
            }
        }
        if (shouldRetry) {
            // Verify timeout
            auto currentTime = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - startTime).count();
            if (elapsed < timeoutMs) {
                // Requeue for retry
                comm->Execute(*retryFunction);
                return;
            }
        }

        // Final processing step
        auto checkRc = errorCheckFunc(comm);
        finalHandler(comm, result, checkRc);
        comm->SetCommReady(true);
    };

    // Start execution
    (*retryFunction)();
}

void HcclCommFactory::CreateHcclCommInRecv(int32_t localDeviceId, const std::string &remoteClientId,
                                           int32_t remoteDeviceId, bool isSameNode,
                                           std::shared_ptr<CommWrapperBase> &comm)
{
    auto traceId = Trace::Instance().GetTraceID();
    auto process = [this, comm, localDeviceId, remoteDeviceId, remoteClientId, isSameNode, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        INJECT_POINT("CreateHcclCommInRecv.sleep");
        PerfPoint point(PerfKey::CLIENT_CREATE_HCCL_IN_RECV);
        auto localClientId = clientWorkerApi_->GetClientId();
        auto peerId = GetHcclPeerId(remoteClientId, remoteDeviceId, localClientId, localDeviceId);
        VLOG(1) << FormatString("[Receiver] Try to acquire write lock, peerId: %s", peerId);
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);

        HcclRootInfo rootInfo;
        RETURN_IF_NOT_OK(SetStateIfError(comm, comm->CreateRootInfo(rootInfo)));

        // rootInfo contain \0, must construct string like this.
        // use c_str() return to rootInfo.
        SendRootInfoReqPb req;
        req.set_root_info(std::string(std::begin(rootInfo.internal), std::end(rootInfo.internal)));
        req.set_dst_client_id(localClientId);
        req.set_dst_device_id(localDeviceId);
        req.set_src_client_id(remoteClientId);
        req.set_src_device_id(remoteDeviceId);

        LOG(INFO) << "[Receiver] Send root info to worker, peerId: " << peerId;
        SendRootInfoRspPb rsp;
        auto rc = clientWorkerApi_->SendRootInfo(req, rsp);
        if (rc.GetCode() == StatusCode::K_CLIENT_DEADLOCK) {
            LOG(WARNING) << "[Receiver] Deadlock occurred, release the lock and retry";
            RETURN_IF_NOT_OK(SetStateIfError(comm, rc));
        }

        LOG(INFO) << "[Receiver] Start init hccl comm";
        PrintRootInfo(rootInfo);
        rc = comm->InitCommunicator(rootInfo, HcclCommDirection::RECV, isSameNode);
        RETURN_IF_NOT_OK(SetStateIfError(comm, rc));
        PerfPoint perfPoint(PerfKey::CLIENT_HCCL_WARMUP_IN_RECV);
        return comm->WarmUpComm(HcclCommDirection::RECV);
    };
    auto commPtr = comm;
    comm->Execute([this, commPtr, process, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        constexpr int32_t timeoutMs = 60 * 1000;
        this->AsyncRetryWithTimeout(
            commPtr, process, [this](auto comm) { return this->CreateHcclCommCheckError(comm); }, timeoutMs,
            { StatusCode::K_CLIENT_DEADLOCK },
            [this](auto comm, Status result, Status checkRc) {
                this->StatusComparisonWithSetStatus(comm, result, checkRc);
            });
    });
}

std::vector<std::shared_ptr<CommWrapperBase>> HcclCommFactory::GetAllHcclComm()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    std::vector<std::shared_ptr<CommWrapperBase>> hcclCommVec;
    hcclCommVec.reserve(commTable_.size());
    for (auto &iter : commTable_) {
        if (iter.second != nullptr) {
            hcclCommVec.emplace_back(iter.second);
        }
    }
    return hcclCommVec;
}

Status HcclCommFactory::DelComm(std::string commId)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    TbbHcclCommTable::accessor acc;
    if (commTable_.find(acc, commId)) {
        (void)commTable_.erase(acc);
    }
    return Status::OK();
}

size_t HcclCommFactory::GetHcclCommSize()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return commTable_.size();
}

void HcclCommFactory::DestroyHcclComm(std::string commId)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    TbbHcclCommTable::accessor acc;
    if (commTable_.find(acc, commId)) {
        auto comm = acc->second;
        comm->ShutDown();
        (void)commTable_.erase(acc);
    }
}
}  // namespace datasystem
