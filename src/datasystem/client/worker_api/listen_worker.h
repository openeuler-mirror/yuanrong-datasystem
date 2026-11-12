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

#ifndef DATASYSTEM_CLIENT_LISTEN_WORKER_H
#define DATASYSTEM_CLIENT_LISTEN_WORKER_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/client/worker_api/client_worker_common_api.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace client {
constexpr int INVALID_SOCKET_FD = -1;

enum class WorkerRecoveryReason : uint8_t {
    WORKER_REBOOT = 0,
    CLIENT_REMOVED,
    CONNECTION_BROKEN,
    RETRY_PENDING,
};

class FdReleaseHelper {
public:
    void SetReleaseFdCallBack(std::function<void(const std::vector<int64_t> &fds)> callBack)
    {
        releaseCallBack_ = std::move(callBack);
    }

    std::vector<int64_t> GetReleasedWorkerFds() const
    {
        if (releaseCallBack_) {
            releaseCallBack_(expiredWorkerFds_);
            return expiredWorkerFds_;
        }
        return {};
    }

    void Update(std::vector<int64_t> &&expiredFds)
    {
        expiredWorkerFds_ = std::move(expiredFds);
    }

private:
    std::vector<int64_t> expiredWorkerFds_;
    std::function<void(const std::vector<int64_t> &fds)> releaseCallBack_;
};

class ListenWorker {
public:
    ListenWorker(std::shared_ptr<IClientWorkerCommonApi> clientCommonWorker, HeartbeatType type,
                 bool closeAdmissionOnScaleDown = false);
    virtual ~ListenWorker();
    ListenWorker(const ListenWorker &, HeartbeatType type) = delete;
    ListenWorker &operator=(const ListenWorker &) = delete;

    Status StartListenWorker(int socket = INVALID_SOCKET_FD);
    void StopListenWorker(bool stopActively = false);
    void JoinListenWorker();
    Status CheckWorkerAvailable();
    void SetWorkerAvailable(bool workerStatus);
    void AddCallBackFunc(void *pointer, std::function<void()> callback);
    void AddRecoveryCallback(void *pointer, std::function<Status(WorkerRecoveryReason)> callback);
    void RemoveCallBackFunc(void *pointer);
    Status UpdateSocketFd(int socketFd);
    void CheckAndSetClientTimeout(int64_t failureTime, int64_t nodeTimeoutMs, const Status &status);
    bool IsWorkerVoluntaryScaleDown();
    void SetReleaseFdCallBack(std::function<void(const std::vector<int64_t> &)> callback);
    void SetWorkerTimeoutHandle(std::function<void()> callback);
    Status NotifyClientRemovable();

private:
    Status CheckHeartbeat();
    void HandleHeartbeatError(Timer &timer, uint64_t clientDeadTimeoutMs,
                              const std::vector<uint32_t> &heartbeatIntervalMs, int32_t &lostHeartbeatTimes,
                              uint64_t &remainTime, const Status &status);
    void HandleScaleDown(bool isWorkerVoluntaryScaleDown);
    bool ProcessHeartbeatResult(bool workerReboot, bool clientRemoved, std::vector<int64_t> expiredWorkerFds,
                                int32_t &lostHeartbeatTimes, int intervalMs, Timer &timer, uint64_t clientDeadTimeoutMs,
                                uint64_t &remainTime);
    void NotifyFirstHeartbeat(bool success);
    Status RunAllCallback(WorkerRecoveryReason reason);
    void TryRecoverClientResources(WorkerRecoveryReason reason);
    void CleanInvalidCallback();

    std::shared_ptr<IClientWorkerCommonApi> clientCommonWorker_;
    std::atomic<bool> workerAvailable_{ true };
    std::atomic<bool> stopActively_{ false };
    Thread workerListenedThread_;
    std::atomic<bool> workerListenedThreadJoined_{ false };
    std::atomic<int> socketFd_{ INVALID_SOCKET_FD };
    std::atomic<bool> stop_{ false };
    std::string clientId_;
    std::unordered_map<void *, std::function<Status(WorkerRecoveryReason)>> callBackTable_;
    SharedMutex callbackMutex_;
    std::unordered_set<void *> deletedCallbacks_;
    SharedMutex deletedCallbackMutex_;
    std::unique_ptr<WaitPost> waitPost_;
    bool recoveryPending_{ false };
    std::atomic<bool> firstHeartbeatReceived_{ false };
    std::unique_ptr<WaitPost> firstHeartbeatWaitPost_;
    HeartbeatType heartbeatType_;
    std::shared_ptr<SockEventLoop> udsEventLoop_;
    std::atomic<bool> isWorkerVoluntaryScaleDown_{ false };
    const bool closeAdmissionOnScaleDown_;
    FdReleaseHelper fdReleaseHelper_;
    std::function<void()> workerTimeoutHandle_;
    SharedMutex workerTimeoutHandleMutex_;
};
}  // namespace client
}  // namespace datasystem
#endif
