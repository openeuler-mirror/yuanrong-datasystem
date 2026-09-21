/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/common/coordinator/coordinator_leader_router.h"

#include <algorithm>
#include <deque>
#include <random>
#include <sstream>
#include <unordered_set>
#include <utility>

#include "datasystem/common/coordinator/coordinator_log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/rpc/brpc_status_util.h"

namespace datasystem {
namespace {
constexpr auto MIN_INITIAL_LEADER_TIMEOUT = std::chrono::milliseconds(1'000);

std::chrono::milliseconds RouteRetryDelay(std::chrono::milliseconds interval)
{
    constexpr auto maxDelay = std::chrono::milliseconds(200);
    constexpr int jitterDivisor = 2;
    const auto base = std::min(interval, maxDelay);
    thread_local std::mt19937 generator{ std::random_device{}() };
    return base + std::chrono::milliseconds(
        std::uniform_int_distribution<int64_t>(0, base.count() / jitterDivisor)(generator));
}

Status DeadlineExceeded()
{
    return Status(K_RPC_DEADLINE_EXCEEDED, "Coordinator routing deadline exceeded");
}

void AddCandidate(const std::string &candidate, std::unordered_set<std::string> &queued,
                  std::deque<std::string> &candidates)
{
    if (!candidate.empty() && queued.emplace(candidate).second) {
        candidates.emplace_back(candidate);
    }
}
}  // namespace

CoordinatorLeaderRouter::CoordinatorLeaderRouter(Dependencies dependencies) : dependencies_(std::move(dependencies))
{
}

Status CoordinatorLeaderRouter::Execute(const RpcCall &rpc, TimePoint deadline, std::chrono::milliseconds maxRpcTimeout,
                                        std::chrono::milliseconds retryInterval, bool recoveryControl)
{
    RETURN_IF_NOT_OK(Validate(rpc, deadline, maxRpcTimeout, retryInterval));

    const auto started = dependencies_.now();
    CallState call;
    const auto initialLeader = GetCachedLeaderAddress();
    constexpr int leaderBudgetDivisor = 2;
    const auto totalBudget = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - started);
    const auto initialLeaderTimeout = std::min(
        { maxRpcTimeout, totalBudget, std::max(totalBudget / leaderBudgetDivisor, MIN_INITIAL_LEADER_TIMEOUT) });
    const auto trackedRpc = [&](const HostPort &address, std::chrono::milliseconds timeout) {
        if (call.attempts == 0 && initialLeader.has_value() && address.ToString() == *initialLeader) {
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - dependencies_.now());
            if (remaining <= std::chrono::milliseconds::zero()) {
                return RpcResult{ DeadlineExceeded(), std::nullopt };
            }
            timeout = std::min(initialLeaderTimeout, remaining);
        }
        ++call.attempts;
        auto result = rpc(address, timeout);
        const bool timedOut = !result.header.has_value()
                              && result.status.GetCode() == K_RPC_DEADLINE_EXCEEDED
                              && !IsBrpcServerApplicationError(result.status);
        if (timedOut && call.firstTimeoutAddress.empty()) {
            call.firstTimeoutAddress = address.ToString();
            call.firstTimeoutBudget = timeout;
        }
        if (result.header.has_value() && !call.firstTimeoutAddress.empty()) {
            call.lastHeader = result.header;
        }
        return result;
    };
    auto status = ExecuteImpl(trackedRpc, deadline, maxRpcTimeout, retryInterval, recoveryControl, call);
    if (status.IsError() && !call.firstTimeoutAddress.empty()) {
        std::ostringstream summary;
        summary << "Coordinator route: first_timeout_address=" << call.firstTimeoutAddress
                << " first_timeout_ms=" << call.firstTimeoutBudget.count() << " attempts=" << call.attempts
                << " retry_rounds=" << call.retryRounds
                << " elapsed_ms="
                << std::chrono::duration_cast<std::chrono::milliseconds>(dependencies_.now() - started).count()
                << " deadline_reached=" << (dependencies_.now() >= deadline);
        if (call.lastHeader.has_value()) {
            summary << " last_response_state=" << static_cast<int>(call.lastHeader->state)
                    << " leader_hint=" << call.lastHeader->leaderAddress
                    << " coordinator_id=" << CoordinatorIdLogPrefix(call.lastHeader->coordinatorId)
                    << " term=" << call.lastHeader->leaderTerm;
        }
        status.AppendMsg(summary.str());
    }
    return status;
}

Status CoordinatorLeaderRouter::ExecuteImpl(const RpcCall &rpc, TimePoint deadline,
                                            std::chrono::milliseconds maxRpcTimeout,
                                            std::chrono::milliseconds retryInterval, bool recoveryControl,
                                            CallState &call)
{
    Status lastStatus = DeadlineExceeded();
    bool hasCoordinatorResponse = false;
    while (dependencies_.now() < deadline) {
        std::deque<std::string> candidates;
        std::unordered_set<std::string> seenCandidates;
        if (auto cached = GetCachedLeaderAddress(); cached.has_value()) {
            AddCandidate(*cached, seenCandidates, candidates);
        }
        for (const auto &candidate : dependencies_.getCandidateSnapshot()) {
            AddCandidate(candidate, seenCandidates, candidates);
        }
        if (candidates.empty()) {
            return Status(K_NOT_READY, "Coordinator Discovery has no candidates");
        }

        auto result = TryCandidates(std::move(candidates), rpc, deadline, maxRpcTimeout, retryInterval, recoveryControl,
                                    lastStatus, hasCoordinatorResponse);
        if (result.action == RoundAction::COMPLETE) {
            return result.status;
        }
        if (result.action == RoundAction::RETRY_CURRENT_ROUTE) {
            continue;
        }
        ++call.retryRounds;
        dependencies_.refreshCandidates();
        if (!WaitForRetry(deadline, RouteRetryDelay(retryInterval))) {
            return result.status;
        }
    }
    return hasCoordinatorResponse ? lastStatus : DeadlineExceeded();
}

std::optional<CoordinatorLeaderRouter::LeaderIdentity> CoordinatorLeaderRouter::GetLeaderIdentity() const
{
    std::lock_guard<std::mutex> lock(state_.mutex);
    return state_.cachedLeader;
}

std::optional<std::string> CoordinatorLeaderRouter::GetCachedLeaderAddress() const
{
    std::lock_guard<std::mutex> lock(state_.mutex);
    if (!state_.cachedLeader.has_value()) {
        return std::nullopt;
    }
    return state_.cachedLeader->address.ToString();
}

Status CoordinatorLeaderRouter::Validate(const RpcCall &rpc, TimePoint deadline,
                                         std::chrono::milliseconds maxRpcTimeout,
                                         std::chrono::milliseconds retryInterval) const
{
    if (rpc == nullptr) {
        return Status(K_INVALID, "Coordinator RPC is null");
    }
    if (dependencies_.getCandidateSnapshot == nullptr) {
        return Status(K_INVALID, "Coordinator candidate snapshot dependency is null");
    }
    if (dependencies_.refreshCandidates == nullptr) {
        return Status(K_INVALID, "Coordinator candidate refresh dependency is null");
    }
    if (dependencies_.publishLeaderIdentity == nullptr) {
        return Status(K_INVALID, "Coordinator Leader identity publisher dependency is null");
    }
    if (dependencies_.now == nullptr) {
        return Status(K_INVALID, "Coordinator clock dependency is null");
    }
    if (dependencies_.wait == nullptr) {
        return Status(K_INVALID, "Coordinator wait dependency is null");
    }
    if (maxRpcTimeout <= std::chrono::milliseconds::zero()) {
        return Status(K_INVALID, "Coordinator maximum RPC timeout must be positive");
    }
    if (retryInterval <= std::chrono::milliseconds::zero()) {
        return Status(K_INVALID, "Coordinator retry interval must be positive");
    }
    if (dependencies_.now() >= deadline) {
        return DeadlineExceeded();
    }
    return Status::OK();
}

CoordinatorLeaderRouter::CandidateRoundResult CoordinatorLeaderRouter::TryCandidates(
    std::deque<std::string> candidates, const RpcCall &rpc, TimePoint deadline, std::chrono::milliseconds maxRpcTimeout,
    std::chrono::milliseconds retryInterval, bool recoveryControl, Status &lastStatus, bool &hasCoordinatorResponse)
{
    bool hasResponse = false;
    bool headerlessNotReady = false;
    while (!candidates.empty()) {
        auto address = std::move(candidates.front());
        candidates.pop_front();
        const auto remainingCandidateCount = 1 + candidates.size();
        auto attempt = TryCandidate(address, rpc, deadline, maxRpcTimeout, retryInterval, recoveryControl,
                                    remainingCandidateCount);
        const bool readinessOnly = attempt.rpcAttempted && !attempt.rpc.header.has_value()
                                   && attempt.rpc.status.GetCode() == K_NOT_READY;
        const bool acceptedResponse =
            readinessOnly || attempt.recoveryStatus.has_value()
            || (attempt.rpc.header.has_value() && attempt.observation == ResponseObservation::ACCEPTED);
        if ((attempt.rpcAttempted || !attempt.deadlineReached) && (acceptedResponse || !hasCoordinatorResponse)) {
            const auto &status = attempt.recoveryStatus.has_value() && !attempt.rpc.header.has_value()
                                     ? *attempt.recoveryStatus
                                     : attempt.rpc.status;
            lastStatus = status.IsOk() ? Status(K_NOT_READY, "Coordinator is not serving business RPCs") : status;
        }
        hasCoordinatorResponse = hasCoordinatorResponse || acceptedResponse;
        hasResponse = hasResponse || attempt.hasResponse;
        headerlessNotReady = headerlessNotReady || readinessOnly;
        if (attempt.observation == ResponseObservation::ROUTE_CHANGED) {
            return { RoundAction::RETRY_CURRENT_ROUTE, lastStatus };
        }
        if (attempt.deadlineReached) {
            return { RoundAction::COMPLETE, hasCoordinatorResponse ? lastStatus : DeadlineExceeded() };
        }
        if (!attempt.rpc.header.has_value() && attempt.rpc.status.IsError()
            && attempt.rpc.status.GetCode() != K_NOT_READY
            && IsBrpcServerApplicationError(attempt.rpc.status)) {
            return { RoundAction::COMPLETE, attempt.rpc.status };
        }
        if (!attempt.rpc.header.has_value()) {
            continue;
        }

        if (auto completed = HandleResponse(attempt.rpc, recoveryControl, candidates, deadline, retryInterval);
            completed.has_value()) {
            return std::move(*completed);
        }
    }

    if (headerlessNotReady && !hasResponse) {
        return { RoundAction::COMPLETE, lastStatus };
    }
    return { RoundAction::RETRY, lastStatus };
}

CoordinatorLeaderRouter::CandidateAttemptResult CoordinatorLeaderRouter::TryCandidate(
    const std::string &address, const RpcCall &rpc, TimePoint deadline, std::chrono::milliseconds maxRpcTimeout,
    std::chrono::milliseconds retryInterval, bool recoveryControl, size_t remainingCandidateCount)
{
    CandidateAttemptResult attempt;
    attempt.rpc.status = DeadlineExceeded();
    HostPort parsedAddress;
    if (parsedAddress.ParseString(address).IsError() || parsedAddress.Empty()) {
        attempt.rpc.status = Status(K_INVALID, "Invalid Coordinator address");
        return attempt;
    }
    while (dependencies_.now() < deadline) {
        const auto attemptTimeout = GetAttemptTimeout(deadline, maxRpcTimeout, remainingCandidateCount);
        if (!attemptTimeout.has_value()) {
            break;
        }
        const auto attemptRouteEpoch = CaptureRouteEpoch();
        attempt.rpcAttempted = true;
        attempt.rpc = rpc(parsedAddress, *attemptTimeout);
        if (!attempt.rpc.header.has_value()) {
            return attempt;
        }
        attempt.hasResponse = true;
        attempt.observation = ObserveResponse(parsedAddress, *attempt.rpc.header, attemptRouteEpoch);
        if (attempt.observation != ResponseObservation::ACCEPTED) {
            attempt.recoveryStatus.reset();
            attempt.rpc.header.reset();
            attempt.rpc.status = Status(K_TRY_AGAIN, "Stale Coordinator response from " + address);
            return attempt;
        }
        if (attempt.rpc.header->state != RpcResponseHeader::State::RECOVERING || recoveryControl) {
            return attempt;
        }
        if (attempt.rpc.status.IsOk()) {
            attempt.rpc.status = Status(K_NOT_READY, "Coordinator leader at " + address + " is recovering");
        }
        attempt.recoveryStatus = attempt.rpc.status;
        if (!WaitForRetry(deadline, retryInterval)) {
            attempt.deadlineReached = true;
            return attempt;
        }
    }
    attempt.deadlineReached = true;
    return attempt;
}

std::optional<CoordinatorLeaderRouter::CandidateRoundResult> CoordinatorLeaderRouter::HandleResponse(
    const RpcResult &result, bool recoveryControl, std::deque<std::string> &candidates,
    TimePoint deadline, std::chrono::milliseconds retryInterval) const
{
    if (result.header->state == RpcResponseHeader::State::UNSPECIFIED
        || result.header->state == RpcResponseHeader::State::SERVING
        || (result.header->state == RpcResponseHeader::State::RECOVERING && recoveryControl)) {
        return CandidateRoundResult{ RoundAction::COMPLETE, result.status };
    }
    if (result.header->state != RpcResponseHeader::State::NOT_LEADER || result.header->leaderAddress.empty()) {
        return std::nullopt;
    }
    if (!WaitForRetry(deadline, retryInterval)) {
        return CandidateRoundResult{ RoundAction::COMPLETE,
            result.status.IsOk() ? Status(K_NOT_READY, "Coordinator leader redirect deadline exceeded")
                                 : result.status };
    }
    candidates.erase(std::remove(candidates.begin(), candidates.end(), result.header->leaderAddress), candidates.end());
    candidates.emplace_front(result.header->leaderAddress);
    return std::nullopt;
}

bool CoordinatorLeaderRouter::WaitForRetry(TimePoint deadline, std::chrono::milliseconds retryInterval) const
{
    const auto now = dependencies_.now();
    if (now >= deadline) {
        return false;
    }
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
    if (remaining <= std::chrono::milliseconds::zero()) {
        return false;
    }
    dependencies_.wait(std::min(retryInterval, remaining));
    return dependencies_.now() < deadline;
}

std::optional<std::chrono::milliseconds> CoordinatorLeaderRouter::GetAttemptTimeout(
    TimePoint deadline, std::chrono::milliseconds maxRpcTimeout, size_t remainingCandidateCount) const
{
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - dependencies_.now());
    if (remaining <= std::chrono::milliseconds::zero()) {
        return std::nullopt;
    }
    if (remainingCandidateCount == 0) {
        return std::nullopt;
    }
    const auto fairShare = std::chrono::milliseconds(std::max<int64_t>(1, remaining.count() / remainingCandidateCount));
    return std::min(maxRpcTimeout, fairShare);
}

uint64_t CoordinatorLeaderRouter::CaptureRouteEpoch() const
{
    std::lock_guard<std::mutex> lock(state_.mutex);
    return state_.cachedLeader.has_value() ? state_.cachedLeader->routeEpoch : 0;
}

CoordinatorLeaderRouter::ResponseObservation CoordinatorLeaderRouter::ObserveResponse(const HostPort &address,
                                                                                      const RpcResponseHeader &header,
                                                                                      uint64_t attemptRouteEpoch)
{
    std::optional<LeaderIdentity> identityToPublish;
    {
        std::lock_guard<std::mutex> lock(state_.mutex);
        const bool sameCoordinator =
            state_.cachedLeader.has_value() && state_.cachedLeader->coordinatorId == header.coordinatorId;
        const bool leaderResponse =
            header.state == RpcResponseHeader::State::RECOVERING || header.state == RpcResponseHeader::State::SERVING;
        const bool sameIdentity = sameCoordinator && state_.cachedLeader->address == address
                                  && state_.cachedLeader->leaderTerm == header.leaderTerm;
        const auto currentEpoch = state_.cachedLeader.has_value() ? state_.cachedLeader->routeEpoch : 0;
        if (attemptRouteEpoch != currentEpoch && !(leaderResponse && sameIdentity)) {
            return ResponseObservation::ROUTE_CHANGED;
        }
        if (sameCoordinator && header.leaderTerm < state_.maxObservedTermForCachedCoordinator) {
            return ResponseObservation::STALE_TERM;
        }
        if (sameCoordinator) {
            state_.maxObservedTermForCachedCoordinator =
                std::max(state_.maxObservedTermForCachedCoordinator, header.leaderTerm);
        }
        if (!leaderResponse) {
            return ResponseObservation::ACCEPTED;
        }
        if (!sameIdentity) {
            if (!sameCoordinator) {
                state_.maxObservedTermForCachedCoordinator = header.leaderTerm;
            }
            state_.cachedLeader =
                LeaderIdentity{ address, header.coordinatorId, header.leaderTerm, state_.nextRouteEpoch++ };
            identityToPublish = state_.cachedLeader;
        }
    }
    if (identityToPublish.has_value()) {
        LOG(INFO) << "Observed new Coordinator leader at " << identityToPublish->address.ToString()
                  << ", coordinatorId: " << CoordinatorIdLogPrefix(identityToPublish->coordinatorId)
                  << ", leaderTerm: " << identityToPublish->leaderTerm
                  << ", routeEpoch: " << identityToPublish->routeEpoch;
        dependencies_.publishLeaderIdentity(*identityToPublish);
    }
    return ResponseObservation::ACCEPTED;
}
}  // namespace datasystem
