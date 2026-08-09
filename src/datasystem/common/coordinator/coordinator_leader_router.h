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
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LEADER_ROUTER_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LEADER_ROUTER_H

#include <chrono>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/coordinator.pb.h"
#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

namespace datasystem {

struct CoordinatorLeaderIdentity {
    HostPort address;
    std::string coordinatorId;
    uint64_t leaderTerm{ 0 };
    uint64_t routeEpoch{ 0 };
    bool hasLeader{ false };
};

class IDeadlineAwareCoordinatorDiscovery : public ICoordinatorDiscovery {
public:
    ~IDeadlineAwareCoordinatorDiscovery() override = default;

    virtual Status GetCoordinators(std::chrono::steady_clock::time_point deadline,
                                   std::vector<std::string> &addresses) = 0;
};

class ICoordinatorLeaderRouteProvider {
public:
    virtual ~ICoordinatorLeaderRouteProvider() = default;
    virtual CoordinatorLeaderIdentity GetLeaderCache() const = 0;
    class Subscription {
    public:
        virtual ~Subscription() = default;
    };
    virtual std::unique_ptr<Subscription> SubscribeLeaderChanges(
        std::function<void(const CoordinatorLeaderIdentity &)>)
    {
        return nullptr;
    }
};

/**
 * @brief Route one logical Coordinator RPC across candidates within one fixed deadline.
 */
class CoordinatorLeaderRouter final : public ICoordinatorLeaderRouteProvider {
public:
    using AttemptFn = std::function<Status(const HostPort &, int32_t, coordinator::ResponseHeader &, bool &)>;
    using ClockFn = std::function<std::chrono::steady_clock::time_point()>;
    using WaitFn = std::function<void(std::chrono::milliseconds)>;

    explicit CoordinatorLeaderRouter(std::shared_ptr<ICoordinatorDiscovery> discovery,
                                    std::vector<HostPort> initialCandidates = {}, ClockFn clock = {}, WaitFn wait = {},
                                    std::chrono::milliseconds totalTimeout = {});
    ~CoordinatorLeaderRouter() override = default;

    Status Execute(const AttemptFn &attempt, bool recoveryControl = false);
    CoordinatorLeaderIdentity GetLeaderCache() const override;
    std::unique_ptr<Subscription> SubscribeLeaderChanges(
        std::function<void(const CoordinatorLeaderIdentity &)> callback) override;

private:
    class LeaderSubscription final : public Subscription {
    public:
        LeaderSubscription(CoordinatorLeaderRouter &router, uint64_t id);
        ~LeaderSubscription() override;

    private:
        CoordinatorLeaderRouter *router_;
        uint64_t id_;
    };

    enum class CandidateRoundResult {
        SUCCEEDED,
        EXHAUSTED,
        DEADLINE_EXCEEDED,
        TERMINAL,
    };

    struct SubscriptionState {
        std::mutex mutex;
        std::condition_variable drained;
        std::function<void(const CoordinatorLeaderIdentity &)> callback;
        bool active{ true };
        size_t inFlight{ 0 };
        std::unordered_map<std::thread::id, size_t> callbackThreads;
    };

    Status RefreshCandidates(std::chrono::steady_clock::time_point deadline, std::vector<HostPort> &candidates);
    bool WaitBeforeRefresh(size_t retryCount, std::chrono::steady_clock::time_point deadline);
    void LoadCandidateSnapshot(CoordinatorLeaderIdentity &cached, std::vector<HostPort> &candidates) const;
    CandidateRoundResult TryCandidates(const AttemptFn &attempt, bool recoveryControl,
                                       std::chrono::steady_clock::time_point deadline,
                                       const CoordinatorLeaderIdentity &cached, const std::vector<HostPort> &candidates,
                                       std::unordered_set<std::string> &attempted, bool &hasCoordinatorResponse,
                                       Status &lastStatus);
    bool ObserveHeader(const HostPort &address, const coordinator::ResponseHeader &header);
    static void NotifyLeaderChange(const CoordinatorLeaderIdentity &identity,
                                   const std::vector<std::shared_ptr<SubscriptionState>> &subscriptions);
    void RemoveSubscription(uint64_t id);
    static bool IsUsableHeader(const coordinator::ResponseHeader &header);
    static void InsertRedirectCandidate(const coordinator::ResponseHeader &header, size_t nextIndex,
                                        const std::unordered_set<std::string> &attempted,
                                        std::vector<HostPort> &candidates);

    std::shared_ptr<ICoordinatorDiscovery> discovery_;
    mutable std::mutex mutex_;
    std::vector<HostPort> candidates_;
    CoordinatorLeaderIdentity leader_;
    uint64_t maxObservedTerm_{ 0 };
    ClockFn clock_;
    WaitFn wait_;
    std::chrono::milliseconds totalTimeout_;
    std::atomic<bool> refreshInFlight_{ false };
    uint64_t nextSubscriptionId_{ 1 };
    std::unordered_map<uint64_t, std::shared_ptr<SubscriptionState>> subscriptions_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_LEADER_ROUTER_H
