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
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LEADER_ROUTER_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LEADER_ROUTER_H

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class CoordinatorLeaderRouter final {
public:
    struct RpcResponseHeader {
        enum class State {
            UNSPECIFIED,
            NOT_LEADER,
            RECOVERING,
            SERVING,
        };

        State state{ State::UNSPECIFIED };
        std::string leaderAddress;
        std::string coordinatorId;
        uint64_t leaderTerm{ 0 };
    };

    struct RpcResult {
        Status status;
        std::optional<RpcResponseHeader> header;
    };

    struct LeaderIdentity {
        HostPort address;
        std::string coordinatorId;
        uint64_t leaderTerm{ 0 };
        uint64_t routeEpoch{ 0 };
    };

    using TimePoint = std::chrono::steady_clock::time_point;
    using RpcCall = std::function<RpcResult(const HostPort &, std::chrono::milliseconds)>;

    struct Dependencies {
        std::function<std::vector<std::string>()> getCandidateSnapshot;
        std::function<void()> refreshCandidates;
        std::function<void(const LeaderIdentity &)> publishLeaderIdentity;
        std::function<TimePoint()> now;
        std::function<void(std::chrono::milliseconds)> wait;
    };

    explicit CoordinatorLeaderRouter(Dependencies dependencies);
    ~CoordinatorLeaderRouter() = default;

    CoordinatorLeaderRouter(const CoordinatorLeaderRouter &) = delete;
    CoordinatorLeaderRouter &operator=(const CoordinatorLeaderRouter &) = delete;
    CoordinatorLeaderRouter(CoordinatorLeaderRouter &&) = delete;
    CoordinatorLeaderRouter &operator=(CoordinatorLeaderRouter &&) = delete;

    Status Execute(const RpcCall &rpc, TimePoint deadline, std::chrono::milliseconds maxRpcTimeout,
                   std::chrono::milliseconds retryInterval, bool recoveryControl = false);
    std::optional<LeaderIdentity> GetLeaderIdentity() const;

private:
    enum class RoundAction {
        COMPLETE,
        RETRY,
        RETRY_CURRENT_ROUTE,
    };

    enum class ResponseObservation {
        ACCEPTED,
        STALE_TERM,
        ROUTE_CHANGED,
    };

    struct CandidateRoundResult {
        RoundAction action;
        Status status;
        std::optional<std::string> leaderHint;
    };

    struct CandidateAttemptResult {
        RpcResult rpc;
        std::optional<Status> recoveryStatus;
        ResponseObservation observation{ ResponseObservation::ACCEPTED };
        bool hasResponse{ false };
        bool rpcAttempted{ false };
        bool deadlineReached{ false };
    };

    struct State {
        mutable std::mutex mutex;
        std::optional<LeaderIdentity> cachedLeader;
        uint64_t maxObservedTermForCachedCoordinator{ 0 };
        uint64_t nextRouteEpoch{ 1 };
    };

    Status Validate(const RpcCall &rpc, TimePoint deadline, std::chrono::milliseconds maxRpcTimeout,
                    std::chrono::milliseconds retryInterval) const;
    CandidateRoundResult TryCandidates(std::deque<std::string> candidates, const RpcCall &rpc, TimePoint deadline,
                                       std::chrono::milliseconds maxRpcTimeout,
                                       std::chrono::milliseconds retryInterval, bool recoveryControl,
                                       std::unordered_set<std::string> &attempted, Status &lastStatus,
                                       bool &hasCoordinatorResponse);
    CandidateAttemptResult TryCandidate(const std::string &address, const RpcCall &rpc, TimePoint deadline,
                                        std::chrono::milliseconds maxRpcTimeout,
                                        std::chrono::milliseconds retryInterval, bool recoveryControl,
                                        size_t remainingCandidateCount);
    CandidateRoundResult FinishRound(bool hasResponse, Status status,
                                     std::optional<std::string> nextRoundLeaderHint);
    std::optional<CandidateRoundResult> HandleResponse(
        const RpcResult &result, bool recoveryControl, const std::unordered_set<std::string> &attempted,
        std::deque<std::string> &candidates, std::optional<std::string> &nextRoundLeaderHint) const;
    bool WaitForRetry(TimePoint deadline, std::chrono::milliseconds retryInterval) const;
    std::optional<std::chrono::milliseconds> GetAttemptTimeout(
        TimePoint deadline, std::chrono::milliseconds maxRpcTimeout, size_t remainingCandidateCount) const;
    uint64_t CaptureRouteEpoch() const;
    ResponseObservation ObserveResponse(const HostPort &address, const RpcResponseHeader &header,
                                        uint64_t attemptRouteEpoch);
    std::optional<std::string> GetCachedLeaderAddress() const;

    const Dependencies dependencies_;
    State state_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LEADER_ROUTER_H
