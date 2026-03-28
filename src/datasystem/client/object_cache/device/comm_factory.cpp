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
#include "datasystem/client/object_cache/device/comm_factory.h"

#include <memory>
#include <mutex>

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/p2phccl_comm_wrapper.h"
#include "datasystem/common/device/ascend/p2phccl_types.h"
#include "datasystem/common/device/comm_wrapper.h"
#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Print the root info, rootInfo include some control character so it can't print directly.
 * @param[in] rootInfo The CommRootInfo reference.
 */

template <typename T>
inline static void PrintRootInfo(const T &rootInfo)
{
    std::stringstream forPrint;
    for (char i : rootInfo.data) {
        if (i >= '!' && i <= '`') {
            forPrint << i;
        }
    }
    VLOG(1) << "[RootInfo]:" << forPrint.str();
}

CommFactory::CommFactory(std::shared_ptr<object_cache::IClientWorkerApi> workerApi,
                         DeviceResourceManager *resourceMgr)
    : ClientDeviceCurd(std::move(workerApi)), resourceMgr_(resourceMgr)
{
    commThreadControl_ = std::make_shared<HcclCommMagr>();
}

void CommFactory::ShutDown()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    for (auto &commPair : commTable_) {
        commPair.second->ShutDown();
    }
    commTable_.clear();
}

CommFactory::~CommFactory()
{
    ShutDown();
}

Status CommFactory::SetStateIfError(std::shared_ptr<CommWrapperBase> &comm, Status status)
{
    if (status.IsError()) {
        comm->SetDetailStatus(status);
        return status;
    }
    return Status::OK();
}

std::string CommFactory::GetCommKey(P2PEventType eventType, int32_t localDeviceId,
                                    const std::string &remoteClientId, int32_t remoteDeviceId)
{
    auto splitStr = "==";
    std::stringstream ss;
    ss << static_cast<int>(eventType) << splitStr << localDeviceId << splitStr << remoteDeviceId << splitStr
       << remoteClientId;
    return ss.str();
}

Status CommFactory::GetOrCreateComm(P2PEventType eventType, int32_t localDeviceId,
                                    const std::string &remoteClientId, int32_t remoteDeviceId, bool isSameNode,
                                    bool enableP2Ptransfer, std::shared_ptr<CommWrapperBase> &comm)
{
    PerfPoint perfPoint(PerfKey::GET_OR_CREATE_HCCL_COMMONE);
    auto commKey = GetCommKey(eventType, localDeviceId, remoteClientId, remoteDeviceId);
    TbbCommTable::accessor acc;
    VLOG(1) << FormatString("Trying to acquire read lock, commKey: %s", commKey);
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (commTable_.find(acc, commKey)) {
        comm = acc->second;
        return CreateCommCheckError(comm);
    }
    LOG(INFO) << FormatString(
        "Comm cache miss, initialize new %s comm, commId: %s", enableP2Ptransfer ? "p2phccl" : "hccl", commKey);

    // If insert failed, mean the hccl comm exist, get it and return.
    if (!commTable_.insert(acc, commKey)) {
        comm = acc->second;
        return CreateCommCheckError(comm);
    }

    if (enableP2Ptransfer) {
        comm = std::make_shared<P2PHcclCommWrapper>(commKey, localDeviceId, remoteDeviceId, commThreadControl_,
                                                    resourceMgr_);
        acc->second = comm;
        if (eventType == P2PEventType::SEND) {
            CreateCommInSend(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
            return CreateCommCheckError(comm);
        }
        CreateCommInRecv(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
        return CreateCommCheckError(comm);
    }
    comm =
        std::make_shared<CommWrapper>(commKey, localDeviceId, remoteDeviceId, commThreadControl_, resourceMgr_);
    acc->second = comm;
    if (eventType == P2PEventType::SEND) {
        CreateCommInSend(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
        return CreateCommCheckError(comm);
    }
    CreateCommInRecv(localDeviceId, remoteClientId, remoteDeviceId, isSameNode, comm);
    return CreateCommCheckError(comm);
}

void CommFactory::CreateCommInSend(int32_t localDeviceId, const std::string &remoteClientId,
                                   int32_t remoteDeviceId, bool isSameNode,
                                   std::shared_ptr<CommWrapperBase> &comm)
{
    auto traceId = Trace::Instance().GetTraceID();
    auto processFunc = [this, comm, localDeviceId, remoteDeviceId, remoteClientId, isSameNode, traceId]() mutable {
        return this->ProcessCommCreationInSend(comm, localDeviceId, remoteDeviceId, remoteClientId, isSameNode,
                                               traceId);
    };

    auto commPtr = comm;
    comm->Execute([this, commPtr, processFunc, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        constexpr int32_t timeoutMs = 60 * 1000;
        this->AsyncRetryWithTimeout(
            commPtr, processFunc, [this](auto comm) { return this->CreateCommCheckError(comm); }, timeoutMs,
            { StatusCode::K_CLIENT_DEADLOCK },
            [this](auto comm, Status result, Status checkRc) {
                this->StatusComparisonWithSetStatus(comm, result, checkRc);
            });
    });
}

void CommFactory::StatusComparisonWithSetStatus(std::shared_ptr<CommWrapperBase> &comm, Status processStatus,
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

Status CommFactory::CreateCommCheckError(std::shared_ptr<CommWrapperBase> &comm)
{
    auto status = comm->GetCommAsyncError();
    comm->SetDetailStatus(status);
    return status;
}

void CommFactory::AsyncRetryWithTimeout(
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

void CommFactory::CreateCommInRecv(int32_t localDeviceId, const std::string &remoteClientId,
                                   int32_t remoteDeviceId, bool isSameNode,
                                   std::shared_ptr<CommWrapperBase> &comm)
{
    auto traceId = Trace::Instance().GetTraceID();
    auto process = [this, comm, localDeviceId, remoteDeviceId, remoteClientId, isSameNode, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        TraceGuard subTraceGuard = Trace::Instance().SetSubTraceID(GetSubCommIdForIdentifier(comm));
        INJECT_POINT("CreateCommInRecv.sleep");
        PerfPoint point(PerfKey::CLIENT_CREATE_HCCL_IN_RECV);
        auto localClientId = clientWorkerApi_->clientId_;
        auto peerId = GetPeerId(remoteClientId, remoteDeviceId, localClientId, localDeviceId);
        VLOG(1) << FormatString("Try to acquire write lock, peerId: %s", peerId);
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);

        CommRootInfo rootInfo;
        RETURN_IF_NOT_OK(SetStateIfError(comm, comm->CreateRootInfo(rootInfo)));

        // rootInfo contain \0, must construct string like this.
        // use c_str() return to rootInfo.
        SendRootInfoReqPb req;
        req.set_root_info(std::string(std::begin(rootInfo.data), std::end(rootInfo.data)));
        req.set_dst_client_id(localClientId);
        req.set_dst_device_id(localDeviceId);
        req.set_src_client_id(remoteClientId);
        req.set_src_device_id(remoteDeviceId);

        LOG(INFO) << "Send root info to worker, peerId: " << peerId;
        SendRootInfoRspPb rsp;
        auto rc = clientWorkerApi_->SendRootInfo(req, rsp);
        if (rc.GetCode() == StatusCode::K_CLIENT_DEADLOCK) {
            LOG(WARNING) << "Deadlock occurred, release the lock and retry";
            RETURN_IF_NOT_OK(SetStateIfError(comm, rc));
        }

        LOG(INFO) << "Start init hccl comm";
        PrintRootInfo(rootInfo);
        rc = comm->InitCommunicator(rootInfo, CommDirection::RECV, isSameNode);
        RETURN_IF_NOT_OK(SetStateIfError(comm, rc));
        PerfPoint perfPoint(PerfKey::CLIENT_HCCL_WARMUP_IN_RECV);
        return comm->WarmUpComm(CommDirection::RECV);
    };
    auto commPtr = comm;
    comm->Execute([this, commPtr, process, traceId]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        constexpr int32_t timeoutMs = 60 * 1000;
        this->AsyncRetryWithTimeout(
            commPtr, process, [this](auto comm) { return this->CreateCommCheckError(comm); }, timeoutMs,
            { StatusCode::K_CLIENT_DEADLOCK },
            [this](auto comm, Status result, Status checkRc) {
                this->StatusComparisonWithSetStatus(comm, result, checkRc);
            });
    });
}

std::vector<std::shared_ptr<CommWrapperBase>> CommFactory::GetAllComm()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    std::vector<std::shared_ptr<CommWrapperBase>> commVec;
    commVec.reserve(commTable_.size());
    for (auto &iter : commTable_) {
        if (iter.second != nullptr) {
            commVec.emplace_back(iter.second);
        }
    }
    return commVec;
}

Status CommFactory::DelComm(std::string commId)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    TbbCommTable::accessor acc;
    if (commTable_.find(acc, commId)) {
        (void)commTable_.erase(acc);
    }
    return Status::OK();
}

size_t CommFactory::GetCommSize()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return commTable_.size();
}

void CommFactory::DestroyComm(std::string commId)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    TbbCommTable::accessor acc;
    if (commTable_.find(acc, commId)) {
        auto comm = acc->second;
        comm->ShutDown();
        (void)commTable_.erase(acc);
    }
}

std::string CommFactory::GetSubCommIdForIdentifier(std::shared_ptr<CommWrapperBase> &comm)
{
    const size_t SUB_STR_LENGTH = 7;
    std::string commId = comm->GetCommId();
    if (commId.size() >= SUB_STR_LENGTH) {
        return FormatString("[%s]", commId.substr(0, SUB_STR_LENGTH));
    }
    return FormatString("[%s]", commId);
}

Status CommFactory::ProcessCommCreationInSend(std::shared_ptr<CommWrapperBase> comm, int32_t localDeviceId,
                                              int32_t remoteDeviceId, const std::string &remoteClientId,
                                              bool isSameNode, const std::string &traceId)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
    TraceGuard subTraceGuard = Trace::Instance().SetSubTraceID(GetSubCommIdForIdentifier(comm));
    INJECT_POINT("CreateCommInSend.sleep");
    PerfPoint point(PerfKey::CLIENT_CREATE_HCCL_IN_SEND);
    auto localClientId = clientWorkerApi_->clientId_;
    auto peerId = GetPeerId(localClientId, localDeviceId, remoteClientId, remoteDeviceId);
    VLOG(1) << FormatString("Sender try to acquire write lock, peerId: %s", peerId);
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);

    LOG(INFO) << FormatString("Start to recv RootInfo from worker, peerId: %s", peerId);
    RecvRootInfoReqPb rootInfoReq;
    rootInfoReq.set_dst_client_id(remoteClientId);
    rootInfoReq.set_dst_device_id(remoteDeviceId);
    rootInfoReq.set_src_client_id(localClientId);
    rootInfoReq.set_src_device_id(localDeviceId);
    RecvRootInfoRspPb rootInfoResp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(clientWorkerApi_->RecvRootInfo(rootInfoReq, rootInfoResp),
                                     "Failed with receive");
    if (rootInfoResp.is_dead_lock()) {
        std::string msg = "Deadlock detected, releasing lock and retrying.";
        LOG(WARNING) << msg;
        RETURN_IF_NOT_OK(SetStateIfError(comm, Status(K_CLIENT_DEADLOCK, msg)));
    }

    CommRootInfo rootInfo;
    if (rootInfoResp.root_info().length() != sizeof(rootInfo.data)) {
        std::string msg = FormatString(
            "The rsp rootInfo size is not as expected: %d, which usually indicates that the receiver "
            "did not send the rootInfo properly. Check if there are any errors on the receiver side, peerId: %s",
            rootInfoResp.root_info().length(),
            peerId);
        RETURN_IF_NOT_OK(SetStateIfError(comm, Status(K_RUNTIME_ERROR, msg)));
    }
    auto ret = memcpy_s(static_cast<void *>(rootInfo.data),
        sizeof(rootInfo.data),
        static_cast<const void *>(rootInfoResp.root_info().c_str()),
        rootInfoResp.root_info().length());
    if (ret != EOK) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Copy root info failed, the memcpy_s return: %d", ret));
    }
    LOG(INFO) << "Start init hccl comm";
    PrintRootInfo(rootInfo);
    auto rc = comm->InitCommunicator(rootInfo, CommDirection::SEND, isSameNode);
    RETURN_IF_NOT_OK(SetStateIfError(comm, rc));
    PerfPoint perfPoint(PerfKey::CLIENT_HCCL_WARMUP_IN_SEND);
    return comm->WarmUpComm(CommDirection::SEND);
}
}  // namespace datasystem
