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
#ifndef DATASYSTEM_CLIENT_LISTEN_WORKER_H
#define DATASYSTEM_CLIENT_LISTEN_WORKER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/common/eventloop/event_loop.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/fd_pass.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
namespace client {
constexpr int INVALID_SOCKET_FD = -1;
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
    std::vector<int64_t> expiredWorkerFds_;  // use only in heartbeat thread, no lock here.
    std::function<void(const std::vector<int64_t> &fds)> releaseCallBack_;
};

class ListenWorker : public std::enable_shared_from_this<ListenWorker> {
public:
    ListenWorker(std::shared_ptr<IClientWorkerCommonApi> clientCommonWorker, HeartbeatType type, uint32_t index = 0,
                 ThreadPool *pool = nullptr);
    virtual ~ListenWorker();
    ListenWorker(const ListenWorker &, HeartbeatType type) = delete;
    ListenWorker &operator=(const ListenWorker &) = delete;

    /**
     * @brief Start listen thread.
     * @param[in] socket The socket's fd which is listened.
     * @return Status of the call.
     */
    Status StartListenWorker(int socket = INVALID_SOCKET_FD);

    /**
     * @brief Stop listening worker.
     * @param[in] stopActively If it is an active shutdown scenario,
     * we need to actively send a Disconnect request to the worker.
     */
    void StopListenWorker(bool stopActively = false);

    /**
     * @brief Get the Worker status.
     * @return Return true if worker is OK, else return false.
     */
    Status CheckWorkerAvailable();

    /**
     * @brief Set the workerStatus_ after reconnection.
     * @param[in] workerStatus The worker status that worker alive or not.
     */
    void SetWorkerAvailable(bool workerStatus);

    /**
     * @brief Set the callback function.
     * @param[in] pointer Object pointer.
     * @param[in] callback Callback function invoked when the worker is faulty.
     */
    void AddCallBackFunc(void *pointer, std::function<void()> callback);

    /**
     * @brief Remove the callback function.
     * @param[in] pointer Object pointer.
     */
    void RemoveCallBackFunc(void *pointer);

    /**
     * @brief Set the socketFd after reconnected.
     * @param[in] socketFd the tcp connection fd.
     * @return Return true if worker is OK, else return false.
     */
    Status UpdateSocketFd(const int socketFd);

    /**
     * @brief Set the switch worker handle.
     * @param[in] callback The handle of switch worker.
     */
    void SetSwitchWorkerHandle(std::function<bool(uint32_t)> callback);

    /**
     * @brief Set the isLocalWorker.
     * @param[in] isLocalWorker The worker is local or not.
     */
    void SetIsLocalWorker(bool isLocalWorker);

    /**
     * @brief Check and set client timeout.
     * @param[in] failureTime Client heart beat failure time.
     * @param[in] nodeTimeoutMs Client timeoutMs.
     * @param[in] status Status of heartbeat.
     */
    void CheckAndSetClientTimeout(int64_t failureTime, int64_t nodeTimeoutMs, const Status &status);

    /**
     * @brief Check if worker is voluntary scale down.
     * @return T/F
     */
    bool IsWorkerVoluntaryScaleDown();

    /**
     * @brief Set the fd reclamation handle.
     * @param[in] callback The handle of fd reclamation.
     */
    void SetReleaseFdCallBack(std::function<void(const std::vector<int64_t> &)> callback);

    /**
     * @brief Set the rediscover local worker handle for IP change scenarios.
     * @param[in] callback Returns true if local worker was successfully rediscovered and reconnected.
     */
    void SetRediscoverHandle(std::function<bool()> callback);

    /**
     * @brief Set standby worker is switched, it would happen when local worker is recover.
     */
    void SetSwitched()
    {
        isSwitched_ = true;
    }

    /**
     * @brief Shutdown standby connection
     */
    void ShutdownStandbyConnection();

protected:
    std::atomic<bool> isInAsyncSwitchWorkerPool_{ false };

private:
    /**
     * @brief Send heartbeat messages periodically.
     */
    Status CheckHeartbeat();

    /**
     * @brief Notify the heartbeat result from worker.
     * @param[in] success True if receive the heartbeat from worker.
     */
    void NotifyFirstHeartbeat(bool success);

    /**
     * @brief Run all client lost handler.
     */
    void RunAllCallback();

    void CleanInvalidCallback();

    /**
     * @brief Check pool availability and atomically acquire isInAsyncSwitchWorkerPool_.
     * @param[out] raii RAII guard that resets the flag on destruction. Capture in pool lambda.
     * @return True if acquired, false if pool unavailable or already acquired.
     */
    bool TryAcquireAsyncSwitchPool(std::shared_ptr<Raii> &raii);

    /**
     * @brief Call switchWorkerHandle_ to switch worker.
     */
    void SwitchToRemoteWorker();

    /**
     * @brief Try switch back to local worker.
     */
    void TrySwitchBackToLocalWorker();

    /**
     * @brief Try rediscover local worker via ServiceDiscovery when heartbeat fails and already switched.
     */
    void TryRediscoverLocalWorker();

    /**
     * @brief Check if client is switchable or not.
     * @return True if client is switchable.
     */
    bool IsVoluntarySwitchable()
    {
        return clientCommonWorker_->enableCrossNodeConnection_ && isWorkerVoluntaryScaleDown_;
    }

    /**
     * @brief Check if client is idle.
     * @return True if client is idle.
     */
    bool IsIdle()
    {
        INJECT_POINT("IsIdle", [this] {
            isLocalWorker_ = false;
            return true;
        });
        return !clientCommonWorker_->HaveInvokeCount() && isSwitched_;
    }

    /**
     * @brief Check if client is reconnectable.
     * @return True if client is reconnectable.
     */
    bool IsReconnectable()
    {
        INJECT_POINT("IsReconnectable", [this] {
            isLocalWorker_ = false;
            return false;
        });
        return isLocalWorker_ || !IsIdle();
    }

    /**
     * @brief Try shutdown myself if idle.
     */
    void TryShutdownStandbyConnection();

    std::shared_ptr<IClientWorkerCommonApi> clientCommonWorker_;

    std::atomic<bool> workerAvailable_{ true };
    // This parameter is used to determine whether the worker needs to be actively notified
    // after the client is disconnected.
    std::atomic<bool> stopActively_{ false };

    Thread workerListenedThread_;

    std::atomic<bool> workerListenedThreadJoined_{ false };

    std::atomic<int> socketFd_{ INVALID_SOCKET_FD };

    std::atomic<bool> stop_{ false };

    std::string clientId_;

    // Worker fail handle callback function.
    std::unordered_map<void *, std::function<void()>> callBackTable_;
    std::shared_timed_mutex callbackMutex_;  // Protect 'callBackTable_'.
    std::unordered_set<void *> deletedCallbacks_;
    std::shared_timed_mutex deletedCallbackMutex_;  // Protect 'deletedCallbacks'
    std::function<bool(uint32_t)> switchWorkerHandle_;
    std::shared_timed_mutex switchWorkerHandleMutex_;  // Protect 'switchWorkerHandle_'.
    std::atomic<bool> isSwitched_{ false };
    std::atomic<bool> isLocalWorker_{ true };

    std::unique_ptr<WaitPost> waitPost_{ nullptr };  // wait for some second to check rpc type heartbeat timeout

    std::atomic<bool> firstHeartbeatReceived_{ false };  // wait for first heartbeat
    std::unique_ptr<WaitPost> firstHeartbeatWaitPost_{ nullptr };

    HeartbeatType heartbeatType_;
    // udsEventLoop_ needs to be placed at the bottom to ensure that it is destructed first.
    std::shared_ptr<SockEventLoop> udsEventLoop_{ nullptr };  // This event loop is used for uds type heartbeat

    std::atomic<bool> isWorkerVoluntaryScaleDown_{ false };
    FdReleaseHelper fdReleaseHelper_;
    std::function<bool()> rediscoverHandle_;
    ThreadPool *asyncSwitchWorkerPool_;
    const uint32_t index_;
};
}  // namespace client
}  // namespace datasystem
#endif