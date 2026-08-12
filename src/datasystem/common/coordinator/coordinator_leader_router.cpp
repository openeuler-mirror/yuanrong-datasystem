/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/common/coordinator/coordinator_leader_router.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <exception>
#include <thread>
#include <unordered_set>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
CoordinatorLeaderRouter::LeaderSubscription::LeaderSubscription(CoordinatorLeaderRouter &router, uint64_t id)
    : router_(&router), id_(id)
{
}

CoordinatorLeaderRouter::LeaderSubscription::~LeaderSubscription()
{
    if (router_ != nullptr) {
        router_->RemoveSubscription(id_);
    }
}

namespace {
constexpr int32_t ROUTER_TOTAL_TIMEOUT_MS = 3'000;
constexpr std::array<int32_t, 4> ROUTER_RETRY_INTERVALS_MS{ 1, 5, 50, 200 };
bool AddAddress(const std::string &value, std::unordered_set<std::string> &seen, std::vector<HostPort> &addresses)
{
    HostPort address;
    if (address.ParseString(value).IsError() || address.Empty() || !seen.emplace(address.ToString()).second) {
        return false;
    }
    addresses.emplace_back(std::move(address));
    return true;
}
}  // namespace

CoordinatorLeaderRouter::CoordinatorLeaderRouter(std::shared_ptr<ICoordinatorDiscovery> discovery,
                                                 std::vector<HostPort> initialCandidates, ClockFn clock, WaitFn wait,
                                                 std::chrono::milliseconds totalTimeout)
    : discovery_(std::move(discovery)),
      candidates_(std::move(initialCandidates)),
      clock_(std::move(clock)),
      wait_(std::move(wait)),
      totalTimeout_(totalTimeout)
{
    if (clock_ == nullptr) {
        clock_ = [] { return std::chrono::steady_clock::now(); };
    }
    if (wait_ == nullptr) {
        wait_ = [](std::chrono::milliseconds duration) { std::this_thread::sleep_for(duration); };
    }
    if (totalTimeout_ <= std::chrono::milliseconds::zero()) {
        totalTimeout_ = std::chrono::milliseconds(ROUTER_TOTAL_TIMEOUT_MS);
    }
}

Status CoordinatorLeaderRouter::Execute(const AttemptFn &attempt, bool recoveryControl)
{
    CHECK_FAIL_RETURN_STATUS(attempt != nullptr, K_INVALID, "Coordinator route attempt is null");
    const auto deadline = clock_() + totalTimeout_;
    std::vector<HostPort> candidates;
    CoordinatorLeaderIdentity cached;
    LoadCandidateSnapshot(cached, candidates);
    std::unordered_set<std::string> attempted;
    Status lastStatus(K_NOT_READY, "no serving Coordinator leader");
    bool hasCoordinatorResponse = false;
    bool refreshImmediately = true;
    size_t retryCount = 0;
    while (clock_() < deadline) {
        const auto result = TryCandidates(attempt, recoveryControl, deadline, cached, candidates, attempted,
                                          hasCoordinatorResponse, lastStatus);
        if (result == CandidateRoundResult::SUCCEEDED) {
            return Status::OK();
        }
        if (result == CandidateRoundResult::DEADLINE_EXCEEDED || result == CandidateRoundResult::TERMINAL
            || clock_() >= deadline) {
            return lastStatus;
        }
        if (!refreshImmediately && !WaitBeforeRefresh(retryCount++, deadline)) {
            return lastStatus;
        }
        bool expected = false;
        if (!refreshInFlight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            refreshImmediately = false;
            LoadCandidateSnapshot(cached, candidates);
            continue;
        }
        struct RefreshGuard {
            std::atomic<bool> &flag;
            ~RefreshGuard()
            {
                flag.store(false, std::memory_order_release);
            }
        } refreshGuard{ refreshInFlight_ };
        candidates.clear();
        const auto refreshStatus = RefreshCandidates(deadline, candidates);
        if (refreshStatus.IsOk()) {
            cached = CoordinatorLeaderIdentity();
        } else if (!hasCoordinatorResponse && lastStatus.GetCode() == K_NOT_READY) {
            lastStatus = refreshStatus;
        }
        refreshImmediately = false;
    }
    return hasCoordinatorResponse ? lastStatus
                                  : Status(K_RPC_DEADLINE_EXCEEDED, "Coordinator routing deadline exceeded");
}

void CoordinatorLeaderRouter::LoadCandidateSnapshot(CoordinatorLeaderIdentity &cached,
                                                    std::vector<HostPort> &candidates) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    cached = leader_;
    candidates = candidates_;
}

bool CoordinatorLeaderRouter::WaitBeforeRefresh(size_t retryCount, std::chrono::steady_clock::time_point deadline)
{
    const auto delay = std::chrono::milliseconds(
        ROUTER_RETRY_INTERVALS_MS[std::min(retryCount, ROUTER_RETRY_INTERVALS_MS.size() - 1)]);
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - clock_());
    if (remaining <= std::chrono::milliseconds::zero()) {
        return false;
    }
    wait_(std::min(delay, remaining));
    return clock_() < deadline;
}

CoordinatorLeaderRouter::CandidateRoundResult CoordinatorLeaderRouter::TryCandidates(
    const AttemptFn &attempt, bool recoveryControl, std::chrono::steady_clock::time_point deadline,
    const CoordinatorLeaderIdentity &cached, const std::vector<HostPort> &candidates,
    std::unordered_set<std::string> &attempted, bool &hasCoordinatorResponse, Status &lastStatus)
{
    std::vector<HostPort> batch;
    if (cached.hasLeader) {
        batch.emplace_back(cached.address);
    }
    batch.insert(batch.end(), candidates.begin(), candidates.end());
    for (size_t index = 0; index < batch.size(); ++index) {
        const auto &address = batch[index];
        if (!attempted.emplace(address.ToString()).second) {
            continue;
        }
        const auto now = clock_();
        if (now >= deadline) {
            if (!hasCoordinatorResponse) {
                lastStatus = Status(K_RPC_DEADLINE_EXCEEDED, "Coordinator routing deadline exceeded");
            }
            return CandidateRoundResult::DEADLINE_EXCEEDED;
        }
        const auto remaining = static_cast<int32_t>(
            std::max<int64_t>(1, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count()));
        coordinator::ResponseHeader header;
        bool hasHeader = false;
        const auto status = attempt(address, remaining, header, hasHeader);
        const bool coordinatorResponded = hasHeader && IsUsableHeader(header);
        if (coordinatorResponded) {
            const bool currentTerm = ObserveHeader(address, header);
            const bool acceptsRequest =
                header.is_leader()
                || (recoveryControl && header.serving_state() == coordinator::ResponseHeader::LEADER_RECOVERING);
            if (currentTerm && status.IsOk() && acceptsRequest) {
                return CandidateRoundResult::SUCCEEDED;
            }
            InsertRedirectCandidate(header, index + 1, attempted, batch);
            if (currentTerm && status.IsOk() && header.serving_state() == coordinator::ResponseHeader::LEADER_RECOVERING
                && !recoveryControl) {
                lastStatus = Status(K_NOT_READY, "Coordinator leader is recovering");
                return CandidateRoundResult::TERMINAL;
            }
        }
        if (coordinatorResponded || !hasCoordinatorResponse) {
            lastStatus = status.IsOk() ? Status(K_NOT_READY, "Coordinator is not serving business RPCs") : status;
        }
        hasCoordinatorResponse = hasCoordinatorResponse || coordinatorResponded;
    }
    return CandidateRoundResult::EXHAUSTED;
}

void CoordinatorLeaderRouter::InsertRedirectCandidate(const coordinator::ResponseHeader &header, size_t nextIndex,
                                                      const std::unordered_set<std::string> &attempted,
                                                      std::vector<HostPort> &candidates)
{
    if (header.leader_address().empty()) {
        return;
    }
    HostPort redirect;
    if (redirect.ParseString(header.leader_address()).IsError() || redirect.Empty()
        || attempted.count(redirect.ToString()) != 0) {
        return;
    }
    candidates.insert(candidates.begin() + static_cast<std::ptrdiff_t>(nextIndex), std::move(redirect));
}

CoordinatorLeaderIdentity CoordinatorLeaderRouter::GetLeaderCache() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return leader_;
}

std::unique_ptr<ICoordinatorLeaderRouteProvider::Subscription> CoordinatorLeaderRouter::SubscribeLeaderChanges(
    std::function<void(const CoordinatorLeaderIdentity &)> callback)
{
    if (callback == nullptr) {
        return nullptr;
    }
    auto state = std::make_shared<SubscriptionState>();
    state->callback = std::move(callback);
    std::lock_guard<std::mutex> lock(mutex_);
    const uint64_t id = nextSubscriptionId_++;
    subscriptions_.emplace(id, std::move(state));
    return std::make_unique<LeaderSubscription>(*this, id);
}

Status CoordinatorLeaderRouter::RefreshCandidates(std::chrono::steady_clock::time_point deadline,
                                                  std::vector<HostPort> &candidates)
{
    CHECK_FAIL_RETURN_STATUS(discovery_ != nullptr, K_INVALID, "Coordinator discovery is null");
    std::vector<std::string> values;
    Status status;
    try {
        if (auto *deadlineAware = dynamic_cast<IDeadlineAwareCoordinatorDiscovery *>(discovery_.get());
            deadlineAware != nullptr) {
            status = deadlineAware->GetCoordinators(deadline, values);
        } else {
            CHECK_FAIL_RETURN_STATUS(clock_() < deadline, K_RPC_DEADLINE_EXCEEDED,
                                     "Coordinator discovery deadline exceeded");
            status = discovery_->GetCoordinators(values);
        }
    } catch (const std::exception &) {
        return Status(K_RUNTIME_ERROR, "Coordinator discovery refresh threw an exception");
    } catch (...) {
        return Status(K_RUNTIME_ERROR, "Coordinator discovery refresh threw an unknown exception");
    }
    RETURN_IF_NOT_OK(status);
    std::unordered_set<std::string> seen;
    for (const auto &value : values) {
        static_cast<void>(AddAddress(value, seen, candidates));
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        candidates_ = candidates;
    }
    return Status::OK();
}

bool CoordinatorLeaderRouter::IsUsableHeader(const coordinator::ResponseHeader &header)
{
    return header.coordinator_id().size() == UUID_SIZE;
}

bool CoordinatorLeaderRouter::ObserveHeader(const HostPort &address, const coordinator::ResponseHeader &header)
{
    std::vector<std::shared_ptr<SubscriptionState>> callbacks;
    CoordinatorLeaderIdentity identity;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (header.leader_term() < maxObservedTerm_) {
            return false;
        }
        maxObservedTerm_ = header.leader_term();
        if (header.serving_state() != coordinator::ResponseHeader::LEADER_RECOVERING
            && header.serving_state() != coordinator::ResponseHeader::LEADER_SERVING) {
            return true;
        }
        const bool identityChanged = !leader_.hasLeader || leader_.address.ToString() != address.ToString()
                                     || leader_.leaderTerm != header.leader_term()
                                     || leader_.coordinatorId != header.coordinator_id();
        leader_.address = address;
        leader_.coordinatorId = header.coordinator_id();
        leader_.leaderTerm = header.leader_term();
        leader_.hasLeader = true;
        if (identityChanged) {
            ++leader_.routeEpoch;
            identity = leader_;
            callbacks.reserve(subscriptions_.size());
            for (const auto &[id, state] : subscriptions_) {
                (void)id;
                callbacks.emplace_back(state);
            }
            changed = true;
        }
    }
    if (changed) {
        NotifyLeaderChange(identity, callbacks);
    }
    return true;
}

void CoordinatorLeaderRouter::NotifyLeaderChange(
    const CoordinatorLeaderIdentity &identity, const std::vector<std::shared_ptr<SubscriptionState>> &subscriptions)
{
    // Router never holds its mutex while notifying. A shared state keeps an already selected callback alive.
    for (const auto &state : subscriptions) {
        std::function<void(const CoordinatorLeaderIdentity &)> callback;
        {
            std::lock_guard<std::mutex> callbackLock(state->mutex);
            if (state->active && state->callback != nullptr) {
                ++state->inFlight;
                ++state->callbackThreads[std::this_thread::get_id()];
                callback = state->callback;
            }
        }
        if (callback != nullptr) {
            callback(identity);
            std::lock_guard<std::mutex> callbackLock(state->mutex);
            const auto thread = std::this_thread::get_id();
            if (--state->callbackThreads[thread] == 0) {
                state->callbackThreads.erase(thread);
            }
            if (--state->inFlight == 0) {
                state->drained.notify_all();
            }
        }
    }
}

void CoordinatorLeaderRouter::RemoveSubscription(uint64_t id)
{
    std::shared_ptr<SubscriptionState> state;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto iter = subscriptions_.find(id);
        if (iter == subscriptions_.end()) {
            return;
        }
        state = std::move(iter->second);
        subscriptions_.erase(iter);
    }
    std::unique_lock<std::mutex> callbackLock(state->mutex);
    state->active = false;
    // A callback may release its own subscription. Its shared state survives until this invocation returns.
    if (state->callbackThreads.count(std::this_thread::get_id()) == 0) {
        state->drained.wait(callbackLock, [&state] { return state->inFlight == 0; });
    }
    state->callback = nullptr;
}
}  // namespace datasystem
