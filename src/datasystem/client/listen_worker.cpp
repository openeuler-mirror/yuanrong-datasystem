/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Client listen worker class.
 */
#include "datasystem/client/listen_worker.h"

#include <atomic>
#include <memory>
#include <sys/socket.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace client {
ListenWorker::ListenWorker(std::shared_ptr<IClientWorkerCommonApi> clientCommonWorker, HeartbeatType type,
                           uint32_t index, ThreadPool *pool)
    : clientCommonWorker_(std::move(clientCommonWorker)),
      heartbeatType_(type),
      asyncSwitchWorkerPool_(pool),
      index_(index)
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
    bool expected = false;
    if (heartbeatType_ == HeartbeatType::RPC_HEARTBEAT
        && workerListenedThreadJoined_.compare_exchange_strong(expected, true)) {
        waitPost_->Set();
        firstHeartbeatWaitPost_->Set();
        if (workerListenedThread_.joinable()) {
            workerListenedThread_.join();
        }
    }
    VLOG(1) << "ListenWorker Destructor End.";
}

Status ListenWorker::StartListenWorker(int socketFd)
{
    auto toString = [](HeartbeatType type) -> std::string {
        std::string name;
        switch (type) {
            case HeartbeatType::NO_HEARTBEAT:
                name = "NO_HEARTBEAT";
                break;
            case HeartbeatType::UDS_HEARTBEAT:
                name = "UDS_HEARTBEAT";
                break;
            case HeartbeatType::RPC_HEARTBEAT:
                name = "RPC_HEARTBEAT";
                break;
        }
        return name;
    };
    VLOG(1) << "Start listen worker, heartbeat type: " << toString(heartbeatType_) << ", socketFd: " << socketFd;
    if (heartbeatType_ == HeartbeatType::NO_HEARTBEAT) {
        return Status::OK();
    }

    if (heartbeatType_ == HeartbeatType::UDS_HEARTBEAT) {
        CHECK_FAIL_RETURN_STATUS(socketFd != INVALID_SOCKET_FD, K_RUNTIME_ERROR,
                                 "Start to listen worker failed, socket invalid.");
        RETURN_IF_NOT_OK(udsEventLoop_->Init());
        socketFd_ = socketFd;
        RETURN_IF_NOT_OK(udsEventLoop_->AddFdEvent(
            socketFd_, EPOLLIN | EPOLLHUP,
            [this]() {
                LOG(INFO) << "The client detects that the worker is disconnected, socket fd: " << socketFd_;
                workerAvailable_ = false;
                RunAllCallback();
            },
            nullptr));
    } else {
        workerListenedThread_ = Thread(&ListenWorker::CheckHeartbeat, this);
        workerListenedThread_.set_name("ListenWorker");
        firstHeartbeatWaitPost_->WaitFor(clientCommonWorker_->connectTimeoutMs_);
        INJECT_POINT("listen_worker.StartListenWorker");
        if (!firstHeartbeatReceived_.load()) {
            return Status(K_CLIENT_WORKER_DISCONNECT, "Cannot receive heartbeat from worker.");
        }
    }
    return Status::OK();
}

void ListenWorker::StopListenWorker(bool stopActively)
{
    INJECT_POINT("listen_worker.TryShutdownStandbyConnection", []() { return; });
    if (workerAvailable_ == true) {
        stopActively_ = stopActively;
    }
    stop_ = true;
    workerAvailable_ = false;
    bool expected = false;
    if (stopActively && heartbeatType_ == HeartbeatType::RPC_HEARTBEAT
        && workerListenedThreadJoined_.compare_exchange_strong(expected, true)) {
        waitPost_->Set();
        if (workerListenedThread_.joinable()) {
            workerListenedThread_.join();
        }
    }
}

int64_t GetRemainTime(Timer &timer, int64_t nodeTimeoutMs)
{
    auto elapsedTimeMs = timer.ElapsedMilliSecond();
    return static_cast<double>(nodeTimeoutMs) > elapsedTimeMs ? static_cast<double>(nodeTimeoutMs) - elapsedTimeMs : 0;
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
    if (firstHeartbeatReceived_.load()) {
        return;
    }
    firstHeartbeatReceived_.store(success);
    firstHeartbeatWaitPost_->Set();
}

void ListenWorker::SetReleaseFdCallBack(std::function<void(const std::vector<int64_t> &)> callback)
{
    fdReleaseHelper_.SetReleaseFdCallBack(std::move(callback));
}

void ListenWorker::SetRediscoverHandle(std::function<bool()> callback)
{
    rediscoverHandle_ = std::move(callback);
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
    bool prevSwitchedState = isSwitched_;
    while (!stop_) {
        // Reset heartbeat timer after any switch transition (to standby or back to local
        // via rediscovery). Without this, the stale timer accumulated during the original
        // disconnection would instantly exceed nodeTimeoutMs and trigger another switch.
        if (isSwitched_ != prevSwitchedState) {
            timer.Reset();
            lostHeartbeatTimes = 0;
            remainTime = clientDeadTimeoutMs;
            prevSwitchedState = isSwitched_;
        }
        bool workerReboot, clientRemoved, isWorkerVoluntaryScaleDown;
        std::vector<int64_t> expiredWorkerFds;
        Status status =
            clientCommonWorker_->SendHeartbeat(workerReboot, clientRemoved, remainTime, isWorkerVoluntaryScaleDown,
                                               fdReleaseHelper_.GetReleasedWorkerFds(), expiredWorkerFds);
        if (status.IsError()) {
            CheckAndSetClientTimeout(timer.ElapsedMilliSecond(), clientDeadTimeoutMs, status);
            TryRediscoverLocalWorker();
            auto interval = GetErrorWaitInterval(timer, clientDeadTimeoutMs, heartbeatIntervalMs[lostHeartbeatTimes]);
            waitPost_->WaitFor(interval);
            if (isSwitched_ != prevSwitchedState) {
                continue;
            }
            remainTime = GetRemainTime(timer, clientDeadTimeoutMs);
            CheckAndSetClientTimeout(timer.ElapsedMilliSecond(), clientDeadTimeoutMs, status);
            lostHeartbeatTimes++;
            if (lostHeartbeatTimes >= (int32_t)(heartbeatIntervalMs.size() - 1)) {
                lostHeartbeatTimes = 0;
            }
            continue;
        }
        isWorkerVoluntaryScaleDown_ = isWorkerVoluntaryScaleDown;
        if (IsVoluntarySwitchable()) {
            constexpr int logInterval = 10;
            LOG_EVERY_T(INFO, logInterval)
                << "[Switch] Connected worker will scale down, switch worker, client id: " << clientId_;
            SwitchToRemoteWorker();
        } else if (clientCommonWorker_->removable_.exchange(false, std::memory_order_relaxed)) {
            LOG(INFO) << "[Switch] Client " << clientId_ << " recover to normal state now";
            continue;
        }

        // Idle means remote connection request is 0 and switched.
        if (IsIdle()) {
            // If we are standby connection and idle, try shutdown ourselves.
            TryShutdownStandbyConnection();
            // If we are local connection and idle, we can tell local worker that we can be removed safely.
            if (IsVoluntarySwitchable() && !clientCommonWorker_->removable_.exchange(true, std::memory_order_relaxed)) {
                LOG(INFO) << "[Switch] Client " << clientId_ << " is removable now";
                continue;
            }
        }
        // For RPC heartbeats, the callback function is executed only when the heartbeat is successful again
        // to avoid incorrect triggering due to network congestion.
        lostHeartbeatTimes = 0;
        INJECT_POINT("listen_worker.reboot", [&workerReboot]() {
            workerReboot = true;
            return Status::OK();
        });
        if (IsReconnectable() && (workerReboot || clientRemoved)) {
            LOG(INFO) << "Heartbeat success, start to run all callback.";
            RunAllCallback();
            NotifyFirstHeartbeat(true);
            continue;
        }
        fdReleaseHelper_.Update(std::move(expiredWorkerFds));
        NotifyFirstHeartbeat(true);
        workerAvailable_ = true;
        TrySwitchBackToLocalWorker();
        waitPost_->WaitFor(intervalMs);
        remainTime = clientDeadTimeoutMs;
        timer.Reset();
    }
    return Status::OK();
}

void ListenWorker::CheckAndSetClientTimeout(int64_t failureTime, int64_t nodeTimeoutMs, const Status &status)
{
    if (failureTime >= nodeTimeoutMs) {
        if (workerAvailable_) {
            // If the heartbeat is not lost for the first time, the client stops sending requests.
            LOG(WARNING) << FormatString(
                "Lost heartbeat, set worker available to false with clientID:%s, "
                "worker address:%s, Detail:%s.",
                clientCommonWorker_->clientId_, clientCommonWorker_->hostPort_.ToString(), status.ToString());
            workerAvailable_ = false;
            NotifyFirstHeartbeat(false);
        }
        SwitchToRemoteWorker();
    }
}

void ListenWorker::RunAllCallback()
{
    if (stop_) {
        return;
    }
    CleanInvalidCallback();

    LOG(INFO) << "All callback size: " << callBackTable_.size() << ", local worker: " << isLocalWorker_;
    auto traceId = Trace::Instance().GetTraceID();
    auto func = [this, traceId]() {
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        std::shared_lock<std::shared_timed_mutex> l(callbackMutex_);
        for (const auto &func : callBackTable_) {
            if (stop_) {
                return;
            }
            if (func.second) {
                func.second();
            }
        }
    };
    if (asyncSwitchWorkerPool_ == nullptr || isLocalWorker_) {
        func();
    } else {
        LOG(INFO) << "async pool statistics: " << asyncSwitchWorkerPool_->GetStatistics();
        auto future = asyncSwitchWorkerPool_->Submit(func);
        future.get();
    }
}

void ListenWorker::AddCallBackFunc(void *pointer, std::function<void()> callback)
{
    if (stop_ || pointer == nullptr) {
        return;
    }
    std::lock_guard<std::shared_timed_mutex> l(callbackMutex_);
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
    std::lock_guard<std::shared_timed_mutex> l(deletedCallbackMutex_);
    deletedCallbacks_.emplace(pointer);
}

void ListenWorker::SetSwitchWorkerHandle(std::function<bool(uint32_t)> callback)
{
    std::lock_guard<std::shared_timed_mutex> l(switchWorkerHandleMutex_);
    switchWorkerHandle_ = std::move(callback);
}

bool ListenWorker::TryAcquireAsyncSwitchPool(std::shared_ptr<Raii> &raii)
{
    if (!asyncSwitchWorkerPool_) {
        LOG_FIRST_N(INFO, 1) << "[Switch] Async switch worker pool is null, ignore switch worker";
        return false;
    }
    bool expected = false;
    if (!isInAsyncSwitchWorkerPool_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
        VLOG(1) << "[Switch] Async switch worker pool has task executing";
        return false;
    }
    raii = std::make_shared<Raii>([this]() {
        isInAsyncSwitchWorkerPool_.store(false, std::memory_order_relaxed);
    });
    return true;
}

void ListenWorker::SwitchToRemoteWorker()
{
    {
        std::shared_lock<std::shared_timed_mutex> l(switchWorkerHandleMutex_);
        if (!switchWorkerHandle_) {
            return;
        }
    }
    std::shared_ptr<Raii> raii;
    if (isSwitched_ || !TryAcquireAsyncSwitchPool(raii)) {
        return;
    }
    auto traceId = Trace::Instance().GetTraceID();
    asyncSwitchWorkerPool_->Execute([this, traceId, raii]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        std::shared_lock<std::shared_timed_mutex> l(switchWorkerHandleMutex_);
        LOG(INFO) << "[Switch] Worker " << clientCommonWorker_->workerId_
                  << " will be switched, client id: " << clientId_;
        isSwitched_ = switchWorkerHandle_(index_);
    });
}

void ListenWorker::TrySwitchBackToLocalWorker()
{
    {
        std::shared_lock<std::shared_timed_mutex> l(switchWorkerHandleMutex_);
        if (!switchWorkerHandle_) {
            return;
        }
    }
    std::shared_ptr<Raii> raii;
    if (!isSwitched_ || !isLocalWorker_ || isWorkerVoluntaryScaleDown_ || !TryAcquireAsyncSwitchPool(raii)) {
        return;
    }
    auto traceId = Trace::Instance().GetTraceID();
    asyncSwitchWorkerPool_->Execute([this, traceId, raii]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        std::shared_lock<std::shared_timed_mutex> l(switchWorkerHandleMutex_);
        LOG(INFO) << "[Switch] Local worker " << clientCommonWorker_->workerId_ << " is recovering";
        isSwitched_ = !switchWorkerHandle_(index_);
    });
}

void ListenWorker::TryRediscoverLocalWorker()
{
    std::shared_ptr<Raii> raii;
    if (!isSwitched_ || !isLocalWorker_ || !rediscoverHandle_ || !TryAcquireAsyncSwitchPool(raii)) {
        return;
    }
    VLOG(1) << "[Switch] Attempting to rediscover local worker";
    auto traceId = Trace::Instance().GetTraceID();
    asyncSwitchWorkerPool_->Execute([this, traceId, raii]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        if (rediscoverHandle_()) {
            LOG(INFO) << "[Switch] Local worker rediscovered successfully";
            isSwitched_ = false;
            waitPost_->Set();
        }
    });
}

void ListenWorker::SetIsLocalWorker(bool isLocalWorker)
{
    isLocalWorker_ = isLocalWorker;
}

Status ListenWorker::UpdateSocketFd(const int socketFd)
{
    socketFd_ = socketFd;
    LOG(INFO) << "Start to listen new socket fd " << socketFd;
    Status status = udsEventLoop_->AddFdEvent(
        socketFd, EPOLLIN | EPOLLHUP,
        [this]() {
            LOG(INFO) << "The client detects that the worker is disconnected, socket fd: " << socketFd_;
            workerAvailable_ = false;
            RunAllCallback();
        },
        nullptr);
    if (status.IsError()) {
        LOG(ERROR) << "Listen to new socket fd: " << socketFd << " failed, detail: " << status.ToString();
    } else {
        LOG(INFO) << "Listen to new socket fd " << socketFd << " success";
    }
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
    std::lock_guard<std::shared_timed_mutex> lock(deletedCallbackMutex_);
    std::lock_guard<std::shared_timed_mutex> l(callbackMutex_);
    for (auto pointer : deletedCallbacks_) {
        callBackTable_.erase(pointer);
    }
    deletedCallbacks_.clear();
}

bool ListenWorker::IsWorkerVoluntaryScaleDown()
{
    return isWorkerVoluntaryScaleDown_;
}

void ListenWorker::ShutdownStandbyConnection()
{
    LOG(INFO) << "[Switch] Try to shutdown idle standby client: " << clientId_;
    StopListenWorker(true);
    LOG_IF_ERROR(clientCommonWorker_->Disconnect(false), "[Switch] Disconnect idle client failed");
}

void ListenWorker::TryShutdownStandbyConnection()
{
    std::shared_ptr<Raii> raii;
    if (isLocalWorker_ || stop_ || !TryAcquireAsyncSwitchPool(raii)) {
        return;
    }
    INJECT_POINT("TryShutdownStandbyConnection", [] { return; });
    auto weakThis = weak_from_this();
    auto traceId = Trace::Instance().GetTraceID();
    asyncSwitchWorkerPool_->Execute([weakThis, traceId, raii]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        auto listen = weakThis.lock();
        if (listen == nullptr) {
            return;
        }
        listen->ShutdownStandbyConnection();
    });
}
}  // namespace client
}  // namespace datasystem
