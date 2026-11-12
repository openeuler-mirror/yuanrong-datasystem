/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/client/worker_api/listen_worker.h"

#include <sys/socket.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {
constexpr int32_t REMOVABLE_NOTIFY_RETRY_TIMEOUT_MS = 1000;
constexpr int32_t REMOVABLE_NOTIFY_RPC_TIMEOUT_MS = 200;
}  // namespace

ListenWorker::ListenWorker(std::shared_ptr<IClientWorkerCommonApi> clientCommonWorker, HeartbeatType type,
                           bool closeAdmissionOnScaleDown)
    : clientCommonWorker_(std::move(clientCommonWorker)),
      heartbeatType_(type),
      closeAdmissionOnScaleDown_(closeAdmissionOnScaleDown)
{
    waitPost_ = std::make_unique<WaitPost>();
    firstHeartbeatWaitPost_ = std::make_unique<WaitPost>();
    udsEventLoop_ = std::make_shared<SockEventLoop>();
    clientId_ = clientCommonWorker_->clientId_;
}

ListenWorker::~ListenWorker()
{
    if (heartbeatType_ == HeartbeatType::NO_HEARTBEAT) {
        return;
    }
    stop_ = true;
    if (socketFd_ != INVALID_SOCKET_FD) {
        LOG_IF_ERROR(udsEventLoop_->DelFdEvent(socketFd_), "SockEventLoop delete fd failed ");
        int error = shutdown(socketFd_, SHUT_RDWR);
        VLOG(HEARTBEAT_LEVEL) << FormatString("shutdown socket fd:%d, error:%d ", (int)socketFd_, error);
    }
    JoinListenWorker();
    VLOG(1) << "ListenWorker Destructor End.";
}

Status ListenWorker::StartListenWorker(int socketFd)
{
    VLOG(1) << "Start listen worker, heartbeat type: " << static_cast<int>(heartbeatType_)
            << ", socketFd: " << socketFd;
    if (heartbeatType_ == HeartbeatType::NO_HEARTBEAT) {
        return Status::OK();
    }
    if (heartbeatType_ == HeartbeatType::SOCKET_HEARTBEAT) {
        CHECK_FAIL_RETURN_STATUS(socketFd != INVALID_SOCKET_FD, K_RUNTIME_ERROR,
                                 "Start to listen worker failed, socket invalid.");
        RETURN_IF_NOT_OK(udsEventLoop_->Init());
        socketFd_ = socketFd;
        RETURN_IF_NOT_OK(udsEventLoop_->AddFdEvent(
            socketFd_, EPOLLIN | EPOLLHUP,
            [this]() {
                LOG(INFO) << "The client detects that the worker is disconnected, socket fd: " << socketFd_;
                workerAvailable_ = false;
                auto rc = RunAllCallback(WorkerRecoveryReason::CONNECTION_BROKEN);
                LOG_IF_ERROR(rc, "Failed to recover client after worker connection broke");
            },
            nullptr));
        return Status::OK();
    }

    workerListenedThread_ = Thread(&ListenWorker::CheckHeartbeat, this);
    workerListenedThread_.set_name("ListenWorker");
    firstHeartbeatWaitPost_->WaitFor(clientCommonWorker_->connectTimeoutMs_);
    INJECT_POINT("listen_worker.StartListenWorker");
    CHECK_FAIL_RETURN_STATUS(firstHeartbeatReceived_.load(), K_CLIENT_WORKER_DISCONNECT,
                             "Cannot receive heartbeat from worker.");
    return Status::OK();
}

void ListenWorker::StopListenWorker(bool stopActively)
{
    if (workerAvailable_) {
        stopActively_ = stopActively;
    }
    stop_ = true;
    workerAvailable_ = false;
    if (stopActively) {
        JoinListenWorker();
    }
}

void ListenWorker::JoinListenWorker()
{
    bool expected = false;
    if (heartbeatType_ == HeartbeatType::RPC_HEARTBEAT
        && workerListenedThreadJoined_.compare_exchange_strong(expected, true)) {
        waitPost_->Set();
        firstHeartbeatWaitPost_->Set();
        if (workerListenedThread_.joinable()) {
            workerListenedThread_.join();
        }
    }
}

Status ListenWorker::NotifyClientRemovable()
{
    clientCommonWorker_->removable_.store(true, std::memory_order_relaxed);
    if (heartbeatType_ == HeartbeatType::NO_HEARTBEAT) {
        return Status::OK();
    }
    bool workerReboot = false;
    bool clientRemoved = false;
    bool isWorkerVoluntaryScaleDown = false;
    auto releasedWorkerFds = fdReleaseHelper_.GetReleasedWorkerFds();
    std::vector<int64_t> expiredWorkerFds;
    auto sendHeartbeat = [this, &workerReboot, &clientRemoved, &isWorkerVoluntaryScaleDown, &releasedWorkerFds,
                          &expiredWorkerFds](int32_t rpcTimeoutMs) {
        return clientCommonWorker_->SendHeartbeat(workerReboot, clientRemoved, rpcTimeoutMs, isWorkerVoluntaryScaleDown,
                                                  releasedWorkerFds, expiredWorkerFds);
    };
    RETURN_IF_NOT_OK(RetryOnError(
        REMOVABLE_NOTIFY_RETRY_TIMEOUT_MS, sendHeartbeat, []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE },
        REMOVABLE_NOTIFY_RPC_TIMEOUT_MS));
    fdReleaseHelper_.Update(std::move(expiredWorkerFds));
    return Status::OK();
}

int64_t GetRemainTime(Timer &timer, int64_t nodeTimeoutMs)
{
    auto elapsedTimeMs = timer.ElapsedMilliSecond();
    return static_cast<double>(nodeTimeoutMs) > elapsedTimeMs ? nodeTimeoutMs - elapsedTimeMs : 0;
}

uint32_t GetErrorWaitInterval(Timer &timer, int64_t clientDeadTimeoutMs, uint32_t defaultInterval)
{
    uint32_t interval = defaultInterval;
    INJECT_POINT("ListenWorker.CheckHeartbeat.interval", [&interval](int time) {
        interval = static_cast<uint32_t>(time);
        return interval;
    });
    int64_t remainTime = GetRemainTime(timer, clientDeadTimeoutMs);
    return interval > remainTime && remainTime > 0 ? remainTime : interval;
}

void ListenWorker::NotifyFirstHeartbeat(bool success)
{
    if (!firstHeartbeatReceived_.load()) {
        firstHeartbeatReceived_.store(success);
        firstHeartbeatWaitPost_->Set();
    }
}

void ListenWorker::SetReleaseFdCallBack(std::function<void(const std::vector<int64_t> &)> callback)
{
    fdReleaseHelper_.SetReleaseFdCallBack(std::move(callback));
}

void ListenWorker::SetWorkerTimeoutHandle(std::function<void()> callback)
{
    std::lock_guard<SharedMutex> l(workerTimeoutHandleMutex_);
    workerTimeoutHandle_ = std::move(callback);
}

Status ListenWorker::CheckHeartbeat()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(clientId_);
    LOG(INFO) << "client start to check heartbeat: " << clientId_;
    int32_t lostHeartbeatTimes = 0;
    std::vector<uint32_t> heartbeatIntervalMs{ 0, 500, 1000, 5000, 5000 };
    int intervalMs = clientCommonWorker_->heartBeatIntervalMs_;
    INJECT_POINT("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", [&intervalMs](int time) {
        intervalMs = time;
        return Status::OK();
    });
    auto clientDeadTimeoutMs = clientCommonWorker_->clientDeadTimeoutMs_;
    auto remainTime = clientDeadTimeoutMs;
    Timer timer;
    while (!stop_) {
        bool workerReboot;
        bool clientRemoved;
        bool isWorkerVoluntaryScaleDown;
        std::vector<int64_t> expiredWorkerFds;
        Status status =
            clientCommonWorker_->SendHeartbeat(workerReboot, clientRemoved, remainTime, isWorkerVoluntaryScaleDown,
                                               fdReleaseHelper_.GetReleasedWorkerFds(), expiredWorkerFds);
        if (status.IsError()) {
            HandleHeartbeatError(timer, clientDeadTimeoutMs, heartbeatIntervalMs, lostHeartbeatTimes, remainTime,
                                 status);
            continue;
        }
        HandleScaleDown(isWorkerVoluntaryScaleDown);
        INJECT_POINT("listen_worker.reboot", [&workerReboot]() {
            workerReboot = true;
            return Status::OK();
        });
        if (ProcessHeartbeatResult(workerReboot, clientRemoved, std::move(expiredWorkerFds), lostHeartbeatTimes,
                                   intervalMs, timer, clientDeadTimeoutMs, remainTime)) {
            continue;
        }
    }
    return Status::OK();
}

void ListenWorker::HandleHeartbeatError(Timer &timer, uint64_t clientDeadTimeoutMs,
                                        const std::vector<uint32_t> &heartbeatIntervalMs, int32_t &lostHeartbeatTimes,
                                        uint64_t &remainTime, const Status &status)
{
    CheckAndSetClientTimeout(timer.ElapsedMilliSecond(), clientDeadTimeoutMs, status);
    auto interval = GetErrorWaitInterval(timer, clientDeadTimeoutMs, heartbeatIntervalMs[lostHeartbeatTimes]);
    waitPost_->WaitForAndClear(interval);
    remainTime = GetRemainTime(timer, clientDeadTimeoutMs);
    CheckAndSetClientTimeout(timer.ElapsedMilliSecond(), clientDeadTimeoutMs, status);
    lostHeartbeatTimes = (lostHeartbeatTimes + 1) % (heartbeatIntervalMs.size() - 1);
}

void ListenWorker::HandleScaleDown(bool isWorkerVoluntaryScaleDown)
{
    if (!closeAdmissionOnScaleDown_) {
        isWorkerVoluntaryScaleDown_ = isWorkerVoluntaryScaleDown;
        return;
    }
    bool wasScalingDown = isWorkerVoluntaryScaleDown_.exchange(isWorkerVoluntaryScaleDown);
    if (isWorkerVoluntaryScaleDown) {
        workerAvailable_ = false;
        if (!wasScalingDown) {
            LOG(INFO) << "Connected worker will scale down, client id: " << clientId_;
        }
        if (!clientCommonWorker_->HaveInvokeCount()
            && !clientCommonWorker_->removable_.exchange(true, std::memory_order_relaxed)) {
            LOG(INFO) << "Client has drained in-flight requests and is removable, client id: " << clientId_;
        }
    } else if (wasScalingDown) {
        clientCommonWorker_->removable_.store(false, std::memory_order_relaxed);
        LOG(INFO) << "Connected worker cancelled scale down, client id: " << clientId_;
    }
}

bool ListenWorker::ProcessHeartbeatResult(bool workerReboot, bool clientRemoved, std::vector<int64_t> expiredWorkerFds,
                                          int32_t &lostHeartbeatTimes, int intervalMs, Timer &timer,
                                          uint64_t clientDeadTimeoutMs, uint64_t &remainTime)
{
    lostHeartbeatTimes = 0;
    WorkerRecoveryReason recoveryReason = WorkerRecoveryReason::RETRY_PENDING;
    if (workerReboot || clientRemoved) {
        workerAvailable_ = false;
        recoveryPending_ = true;
        recoveryReason = workerReboot ? WorkerRecoveryReason::WORKER_REBOOT : WorkerRecoveryReason::CLIENT_REMOVED;
    }
    fdReleaseHelper_.Update(std::move(expiredWorkerFds));
    if (recoveryPending_) {
        TryRecoverClientResources(recoveryReason);
        waitPost_->WaitForAndClear(intervalMs);
        remainTime = clientDeadTimeoutMs;
        timer.Reset();
        return true;
    }
    NotifyFirstHeartbeat(true);
    workerAvailable_ = !closeAdmissionOnScaleDown_ || !isWorkerVoluntaryScaleDown_;
    waitPost_->WaitForAndClear(intervalMs);
    remainTime = clientDeadTimeoutMs;
    timer.Reset();
    return false;
}

void ListenWorker::TryRecoverClientResources(WorkerRecoveryReason reason)
{
    if (reason == WorkerRecoveryReason::RETRY_PENDING) {
        VLOG(1) << "Heartbeat success, retry client resource recovery.";
    } else {
        LOG(INFO) << "Heartbeat success, start to recover client resources.";
    }
    auto recoveryStatus = RunAllCallback(reason);
    NotifyFirstHeartbeat(true);
    if (recoveryStatus.IsError()) {
        constexpr int logInterval = 10;
        LOG_EVERY_T(ERROR, logInterval) << "Client resource recovery failed, keep request admission closed: "
                                        << recoveryStatus.ToString();
        return;
    }
    recoveryPending_ = false;
    workerAvailable_ = !closeAdmissionOnScaleDown_ || !isWorkerVoluntaryScaleDown_;
    LOG(INFO) << "Client resources recovered, reopen request admission.";
}

void ListenWorker::CheckAndSetClientTimeout(int64_t failureTime, int64_t nodeTimeoutMs, const Status &status)
{
    if (failureTime < nodeTimeoutMs || !workerAvailable_) {
        return;
    }
    LOG(WARNING) << FormatString(
        "Lost heartbeat, set worker available to false with clientID:%s, worker address:%s, Detail:%s.",
        clientCommonWorker_->clientId_, clientCommonWorker_->hostPort_.ToString(), status.ToString());
    workerAvailable_ = false;
    recoveryPending_ = true;
    NotifyFirstHeartbeat(false);
    std::shared_lock<SharedMutex> l(workerTimeoutHandleMutex_);
    if (workerTimeoutHandle_) {
        workerTimeoutHandle_();
    }
}

Status ListenWorker::RunAllCallback(WorkerRecoveryReason reason)
{
    if (stop_) {
        return Status::OK();
    }
    CleanInvalidCallback();
    auto traceId = Trace::Instance().GetTraceID();
    auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
    std::shared_lock<SharedMutex> l(callbackMutex_);
    Status result = Status::OK();
    for (const auto &entry : callBackTable_) {
        if (stop_) {
            return Status::OK();
        }
        if (entry.second) {
            auto rc = entry.second(reason);
            if (result.IsOk() && rc.IsError()) {
                result = std::move(rc);
            }
        }
    }
    return result;
}

void ListenWorker::AddCallBackFunc(void *pointer, std::function<void()> callback)
{
    AddRecoveryCallback(pointer, [callback = std::move(callback)](WorkerRecoveryReason) {
        callback();
        return Status::OK();
    });
}

void ListenWorker::AddRecoveryCallback(void *pointer, std::function<Status(WorkerRecoveryReason)> callback)
{
    if (stop_ || pointer == nullptr) {
        return;
    }
    std::lock_guard<SharedMutex> l(callbackMutex_);
    if (callBackTable_.find(pointer) != callBackTable_.end()) {
        LOG(WARNING) << "Try to add fail handle function twice.";
    }
    callBackTable_.emplace(pointer, std::move(callback));
}

void ListenWorker::RemoveCallBackFunc(void *pointer)
{
    if (stop_ || pointer == nullptr) {
        return;
    }
    std::lock_guard<SharedMutex> l(deletedCallbackMutex_);
    deletedCallbacks_.emplace(pointer);
}

Status ListenWorker::UpdateSocketFd(int socketFd)
{
    socketFd_ = socketFd;
    LOG(INFO) << "Start to listen new socket fd " << socketFd;
    Status status = udsEventLoop_->AddFdEvent(
        socketFd, EPOLLIN | EPOLLHUP,
        [this]() {
            LOG(INFO) << "The client detects that the worker is disconnected, socket fd: " << socketFd_;
            workerAvailable_ = false;
            auto rc = RunAllCallback(WorkerRecoveryReason::CONNECTION_BROKEN);
            LOG_IF_ERROR(rc, "Failed to recover client after worker connection broke");
        },
        nullptr);
    LOG_IF_ERROR(status, FormatString("Listen to new socket fd %d failed", socketFd));
    return status;
}

void ListenWorker::SetWorkerAvailable(bool workerStatus)
{
    workerAvailable_ = workerStatus;
}

Status ListenWorker::CheckWorkerAvailable()
{
    if (!workerAvailable_ && !stopActively_) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RPC_UNAVAILABLE,
                                FormatString("Client %s disconnected from worker.", clientId_));
    }
    return Status::OK();
}

void ListenWorker::CleanInvalidCallback()
{
    std::lock_guard<SharedMutex> lock(deletedCallbackMutex_);
    std::lock_guard<SharedMutex> l(callbackMutex_);
    for (auto pointer : deletedCallbacks_) {
        callBackTable_.erase(pointer);
    }
    deletedCallbacks_.clear();
}

bool ListenWorker::IsWorkerVoluntaryScaleDown()
{
    return isWorkerVoluntaryScaleDown_;
}
}  // namespace client
}  // namespace datasystem
