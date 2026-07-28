// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_OPERATION_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_OPERATION_H

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"
#include "datasystem/utils/status.h"

namespace datasystem::coordinator::detail {

using RaftOperationCallback = std::function<void(Status)>;
using RaftOperationDrainEntryObserver = std::function<void()>;

inline void InvokeRaftOperationCallback(RaftOperationCallback &&callback, Status &&result) noexcept
{
    try {
        callback(std::move(result));
    } catch (const std::exception &e) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
    } catch (...) {
        LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker;
    }
}

class RaftOperationDrainToken;
class CoordinatorRaftNodeTestAccessor;

class RaftOperationDrainState final {
public:
    RaftOperationDrainState() = default;
    ~RaftOperationDrainState() = default;

    RaftOperationDrainState(const RaftOperationDrainState &) = delete;
    RaftOperationDrainState &operator=(const RaftOperationDrainState &) = delete;

    void StopAcceptingNewTokens()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        acceptingNewTokens_ = false;
    }

    void WaitForDrain()
    {
        NotifyDrainEntryObserver();
        std::unique_lock<std::mutex> lock(mutex_);
        drained_.wait(lock, [this] { return inFlight_ == 0; });
    }

private:
    friend class RaftOperationDrainToken;
    friend class CoordinatorRaftNodeTestAccessor;

    void SetDrainEntryObserverForTest(RaftOperationDrainEntryObserver observer)
    {
        auto ownedObserver =
            observer ? std::make_shared<RaftOperationDrainEntryObserver>(std::move(observer)) : nullptr;
        std::lock_guard<std::mutex> lock(observerMutex_);
        drainEntryObserver_ = std::move(ownedObserver);
    }

    void NotifyDrainEntryObserver() noexcept
    {
        std::shared_ptr<RaftOperationDrainEntryObserver> observer;
        {
            std::lock_guard<std::mutex> lock(observerMutex_);
            observer = drainEntryObserver_;
        }
        if (observer != nullptr) {
            try {
                (*observer)();
            } catch (const std::exception &e) {
                LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker << ": " << e.what();
            } catch (...) {
                LOG(ERROR) << kCoordinatorRaftCallbackFailureMarker;
            }
        }
    }

    bool TryAcquire()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!acceptingNewTokens_) {
            return false;
        }
        ++inFlight_;
        return true;
    }

    void Release() noexcept
    {
        bool drained = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --inFlight_;
            drained = inFlight_ == 0;
        }
        if (drained) {
            drained_.notify_all();
        }
    }

    std::mutex mutex_;
    std::condition_variable drained_;
    bool acceptingNewTokens_{ true };
    size_t inFlight_{ 0 };
    std::mutex observerMutex_;
    std::shared_ptr<RaftOperationDrainEntryObserver> drainEntryObserver_;
};

class RaftOperationDrainToken final {
public:
    explicit RaftOperationDrainToken(std::shared_ptr<RaftOperationDrainState> state) : state_(std::move(state))
    {
        if (state_ != nullptr && !state_->TryAcquire()) {
            state_.reset();
        }
    }

    ~RaftOperationDrainToken()
    {
        if (state_ != nullptr) {
            state_->Release();
        }
    }

    RaftOperationDrainToken(const RaftOperationDrainToken &) = delete;
    RaftOperationDrainToken &operator=(const RaftOperationDrainToken &) = delete;

    bool IsAcquired() const
    {
        return state_ != nullptr;
    }

private:
    std::shared_ptr<RaftOperationDrainState> state_;
};

struct PendingRaftOperationCallback {
    RaftOperationCallback callback;
    Status result;
};

class RaftOperationSubmissionGate final {
public:
    RaftOperationSubmissionGate() = default;
    explicit RaftOperationSubmissionGate(RaftOperationCallback callback) : callback_(std::move(callback))
    {
    }
    ~RaftOperationSubmissionGate() = default;

    void DispatchOrDefer(RaftOperationCallback callback, Status result)
    {
        DispatchOrDeferInternal(std::move(callback), std::move(result));
    }

    void DispatchOrDefer(Status result)
    {
        DispatchOrDeferInternal({}, std::move(result));
    }

    void MarkSubmissionComplete()
    {
        std::optional<PendingRaftOperationCallback> pendingCallback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            submissionComplete_ = true;
            if (pendingResult_.has_value()) {
                pendingCallback.emplace(
                    PendingRaftOperationCallback{ std::move(callback_), std::move(*pendingResult_) });
                pendingResult_.reset();
            }
        }
        if (pendingCallback.has_value()) {
            InvokeRaftOperationCallback(std::move(pendingCallback->callback), std::move(pendingCallback->result));
        }
    }

private:
    void DispatchOrDeferInternal(RaftOperationCallback callback, Status result)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (resultReceived_) {
                return;
            }
            resultReceived_ = true;
            if (callback) {
                callback_ = std::move(callback);
            }
            if (!submissionComplete_) {
                pendingResult_.emplace(std::move(result));
                return;
            }
            callback = std::move(callback_);
        }
        InvokeRaftOperationCallback(std::move(callback), std::move(result));
    }

    std::mutex mutex_;
    bool submissionComplete_{ false };
    bool resultReceived_{ false };
    RaftOperationCallback callback_;
    std::optional<Status> pendingResult_;
};

}  // namespace datasystem::coordinator::detail

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_OPERATION_H
