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

/**
 * Description: Unit tests for Coordinator bootstrap observation convergence.
 */

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#define private public
#include "datasystem/coordinator/raft/coordinator_election_manager.h"
#undef private
#include "datasystem/utils/coordinator_discovery.h"
#include "ut/common.h"

namespace datasystem::coordinator {
namespace {
constexpr char kPeer1[] = "127.0.0.1:18480";
constexpr char kPeer2[] = "127.0.0.2:18480";
constexpr char kPeer3[] = "127.0.0.3:18480";
constexpr char kPeer4[] = "127.0.0.4:18480";
constexpr char kPeer5[] = "127.0.0.5:18480";
constexpr char kPeer6[] = "127.0.0.6:18480";
constexpr char kDataDir[] = "coordinator-election-manager-test-data";
constexpr int kHeartbeatIntervalMs = 100;
constexpr int kElectionTimeoutMs = 1'000;
constexpr std::chrono::milliseconds kHealthCheckInterval{ 10 };
constexpr std::chrono::milliseconds kMemberFailureGrace{ 20 };
constexpr std::chrono::hours kDiscoveryRetryInterval{ 1 };
constexpr std::chrono::milliseconds kBootstrapWarningInterval{ 30 };
constexpr std::chrono::seconds kWaitTimeout{ 2 };
constexpr std::chrono::milliseconds kStableViewElapsed{ 1'000 };
constexpr std::chrono::milliseconds kStaleRpcDelay{ 100 };

class EmptyCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        serviceList.clear();
        return Status::OK();
    }
};

struct DependencyState {
    mutable std::mutex mutex;
    std::condition_variable cv;
    RaftMetadataState metadataState{ RaftMetadataState::ABSENT };
    Status metadataStatus;
    std::vector<std::string> discoveredPeers;
    Status discoveryStatus;
    std::map<std::string, Status> exchangeStatus;
    std::map<std::string, RaftBootstrapObservationPb> exchangeResponses;
    std::vector<std::string> exchangePeers;
    std::chrono::steady_clock::time_point now;
    size_t discoveryCalls{ 0 };
    size_t exchangeCalls{ 0 };
    size_t createNodeCalls{ 0 };
    size_t startNodeCalls{ 0 };
    size_t createMembershipCalls{ 0 };
    size_t startMembershipCalls{ 0 };
    size_t shutdownMembershipCalls{ 0 };
    bool nodeAlive{ false };
    bool membershipAlive{ false };
    CoordinatorRaftOptions raftOptions;

    bool WaitFor(const std::function<bool()> &predicate)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_until(lock, std::chrono::steady_clock::now() + kWaitTimeout, predicate);
    }

    bool WaitForManager(const std::function<bool()> &predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + kWaitTimeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait_until(lock, std::min(deadline, std::chrono::steady_clock::now() + kHealthCheckInterval));
        }
        return predicate();
    }
};

CoordinatorElectionOptions MakeOptions(
    const std::string &localPeer, size_t expectedMemberCount,
    RaftBootstrapMode bootstrapMode = RaftBootstrapMode::DISCOVERY_OBSERVATION)
{
    CoordinatorElectionOptions options;
    options.raftFlags = CoordinatorRaftFlags{ localPeer,
                                              kDataDir,
                                              kHeartbeatIntervalMs,
                                              kElectionTimeoutMs,
                                              static_cast<uint32_t>(
                                                  std::chrono::duration_cast<std::chrono::milliseconds>(
                                                      kDiscoveryRetryInterval)
                                                      .count()),
                                              static_cast<uint32_t>(kMemberFailureGrace.count()),
                                              static_cast<uint32_t>(kHealthCheckInterval.count()),
                                              static_cast<uint32_t>(kBootstrapWarningInterval.count()) };
    options.membershipOptions = CoordinatorMembershipOptions{ expectedMemberCount,
                                                               kHealthCheckInterval,
                                                               kMemberFailureGrace,
                                                               kDiscoveryRetryInterval };
    options.bootstrapMode = bootstrapMode;
    return options;
}

CoordinatorElectionManager::Dependencies MakeDependencies(const std::shared_ptr<DependencyState> &state)
{
    CoordinatorElectionManager::Dependencies dependencies;
    dependencies.probeLocalMetadata = [state](const std::string &, RaftMetadataState &metadataState) {
        std::lock_guard<std::mutex> lock(state->mutex);
        metadataState = state->metadataState;
        return state->metadataStatus;
    };
    dependencies.discoverCandidates =
        [state](const std::shared_ptr<ICoordinatorDiscovery> &, std::vector<std::string> &peers) {
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                peers = state->discoveredPeers;
                ++state->discoveryCalls;
            }
            state->cv.notify_all();
            return state->discoveryStatus;
        };
    dependencies.exchangeObservation =
        [state](const std::string &peer, int32_t, const RaftBootstrapObservationPb &request,
                RaftBootstrapObservationPb &response) {
            Status status(K_RPC_UNAVAILABLE, "No bootstrap observation response configured");
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                ++state->exchangeCalls;
                state->exchangePeers.emplace_back(peer);
                const auto iter = state->exchangeStatus.find(peer);
                if (iter != state->exchangeStatus.end()) {
                    status = iter->second;
                }
                const auto responseIter = state->exchangeResponses.find(peer);
                if (responseIter != state->exchangeResponses.end()) {
                    response = responseIter->second;
                    status = Status::OK();
                } else {
                    response = request;
                }
            }
            state->cv.notify_all();
            return status;
        };
    dependencies.now = [state] {
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->now;
    };
    dependencies.createNode =
        [state](const CoordinatorRaftOptions &options, const CoordinatorRaftEventCallbacks &) {
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                ++state->createNodeCalls;
                state->nodeAlive = true;
                state->raftOptions = options;
            }
            state->cv.notify_all();
            auto handle = std::make_unique<CoordinatorElectionManager::NodeHandle>();
            handle->onDestroyed = [state] {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->nodeAlive = false;
            };
            return handle;
        };
    dependencies.startNode = [state](CoordinatorElectionManager::NodeHandle &, RaftMetadataState) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->startNodeCalls;
        }
        state->cv.notify_all();
        return Status::OK();
    };
    dependencies.createMembership =
        [state](const CoordinatorMembershipOptions &, CoordinatorElectionManager::NodeHandle &,
                const std::shared_ptr<ICoordinatorDiscovery> &) {
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                ++state->createMembershipCalls;
                state->membershipAlive = true;
            }
            state->cv.notify_all();
            auto handle = std::make_unique<CoordinatorElectionManager::MembershipHandle>();
            handle->onDestroyed = [state] {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->membershipAlive = false;
            };
            return handle;
        };
    dependencies.startMembership = [state](CoordinatorElectionManager::MembershipHandle &) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->startMembershipCalls;
        }
        state->cv.notify_all();
        return Status::OK();
    };
    dependencies.shutdownMembership = [state](CoordinatorElectionManager::MembershipHandle &) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->shutdownMembershipCalls;
        }
        state->cv.notify_all();
        return Status::OK();
    };
    dependencies.isLeader = [](const CoordinatorElectionManager::NodeHandle &) { return false; };
    dependencies.getLeader = [](const CoordinatorElectionManager::NodeHandle &, std::string &) {
        return Status(K_NOT_READY, "No leader");
    };
    return dependencies;
}

std::unique_ptr<CoordinatorElectionManager> MakeManager(const std::shared_ptr<DependencyState> &state,
                                                        size_t expectedMemberCount = 3,
                                                        RaftBootstrapMode bootstrapMode =
                                                            RaftBootstrapMode::DISCOVERY_OBSERVATION)
{
    return std::make_unique<CoordinatorElectionManager>(
        MakeOptions(kPeer1, expectedMemberCount, bootstrapMode), CoordinatorRaftEventCallbacks{},
        std::make_shared<EmptyCoordinatorDiscovery>(), MakeDependencies(state));
}

RaftBootstrapObservationPb MakeObservation(const std::string &sender, size_t expectedMemberCount,
                                           const std::vector<std::string> &peers,
                                           RaftBootstrapObservationPhasePb phase = RAFT_BOOTSTRAP_OBSERVING,
                                           const std::vector<std::string> &committedPeers = {})
{
    RaftBootstrapObservationPb observation;
    observation.set_sender_peer(sender);
    observation.set_expected_member_count(expectedMemberCount);
    observation.set_phase(phase);
    for (const auto &peer : peers) {
        observation.add_peers(peer);
    }
    for (const auto &peer : committedPeers) {
        observation.add_committed_peers(peer);
    }
    return observation;
}

void SendObservation(CoordinatorElectionManager &manager, const RaftBootstrapObservationPb &observation)
{
    RaftBootstrapObservationPb response;
    DS_ASSERT_OK(manager.ExchangeBootstrapObservation(observation, response));
}

void SetNow(const std::shared_ptr<DependencyState> &state, std::chrono::steady_clock::time_point now)
{
    std::lock_guard<std::mutex> lock(state->mutex);
    state->now = now;
}

TEST(CoordinatorElectionManagerTest, IncompleteExpectedViewDoesNotCreateNode)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredPeers = { kPeer1, kPeer2, kPeer3 };
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());

    SendObservation(*manager, MakeObservation(kPeer2, 3, { kPeer1, kPeer2 }));
    ASSERT_TRUE(state->WaitFor([state] { return state->exchangeCalls >= 2; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->createNodeCalls, 0U);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, CompleteStableViewAndMatchingFrozenPlansStartFullConfiguration)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3 };
    state->discoveredPeers = peers;
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());

    SendObservation(*manager, MakeObservation(kPeer2, 3, peers));
    SendObservation(*manager, MakeObservation(kPeer3, 3, peers));
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.consistentView.has_value();
    }));

    SetNow(state, std::chrono::steady_clock::time_point{} + kStableViewElapsed);
    SendObservation(*manager, MakeObservation(kPeer2, 3, peers));
    SendObservation(*manager, MakeObservation(kPeer3, 3, peers));
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.frozenPlan.has_value();
    }));

    SendObservation(*manager, MakeObservation(kPeer2, 3, peers, RAFT_BOOTSTRAP_PROPOSED));
    SendObservation(*manager, MakeObservation(kPeer3, 3, peers, RAFT_BOOTSTRAP_PROPOSED));
    ASSERT_TRUE(state->WaitFor([state] { return state->createNodeCalls == 1; }));

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        const auto *plan = std::get_if<BootstrapPlan>(&state->raftOptions.startPlan);
        ASSERT_NE(plan, nullptr);
        EXPECT_EQ(plan->initialPeers, peers);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, FourOfFiveReproductionWaitsUntilFifthConfirmsSamePlan)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3, kPeer4, kPeer5 };
    state->discoveredPeers = peers;
    auto manager = MakeManager(state, 5);
    DS_ASSERT_OK(manager->Start());

    const std::vector<std::string> fourPeers{ kPeer1, kPeer2, kPeer3, kPeer4 };
    for (const auto &peer : { kPeer2, kPeer3, kPeer4 }) {
        SendObservation(*manager, MakeObservation(peer, 5, fourPeers));
    }
    ASSERT_TRUE(state->WaitFor([state] { return state->exchangeCalls >= 4; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->createNodeCalls, 0U);
    }

    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, peers));
    }
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.consistentView.has_value();
    }));
    SetNow(state, std::chrono::steady_clock::time_point{} + kStableViewElapsed);
    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, peers));
    }
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.frozenPlan.has_value();
    }));
    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, peers, RAFT_BOOTSTRAP_PROPOSED));
    }
    ASSERT_TRUE(state->WaitFor([state] { return state->createNodeCalls == 1; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        const auto *plan = std::get_if<BootstrapPlan>(&state->raftOptions.startPlan);
        ASSERT_NE(plan, nullptr);
        EXPECT_EQ(plan->initialPeers, peers);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, FivePeersBootstrapWhenDiscoveryContainsUnreachableStalePeer)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3, kPeer4, kPeer5 };
    state->discoveredPeers = { kPeer1, kPeer2, kPeer3, kPeer4, kPeer5, kPeer6 };
    state->exchangeStatus.emplace(kPeer6, Status(K_RPC_UNAVAILABLE, "stale discovery endpoint"));
    auto manager = MakeManager(state, 5);
    DS_ASSERT_OK(manager->Start());

    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, peers));
    }
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.consistentView.has_value();
    }));

    SetNow(state, std::chrono::steady_clock::time_point{} + kStableViewElapsed);
    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, peers));
    }
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.frozenPlan.has_value();
    }));

    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, peers, RAFT_BOOTSTRAP_PROPOSED));
    }
    ASSERT_TRUE(state->WaitFor([state] { return state->createNodeCalls == 1; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        const auto *plan = std::get_if<BootstrapPlan>(&state->raftOptions.startPlan);
        ASSERT_NE(plan, nullptr);
        EXPECT_EQ(plan->initialPeers, peers);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, SuccessfulExchangeResponsesContributeToConsistentView)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3 };
    state->discoveredPeers = peers;
    state->exchangeResponses.emplace(kPeer2, MakeObservation(kPeer2, 3, peers));
    state->exchangeResponses.emplace(kPeer3, MakeObservation(kPeer3, 3, peers));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.consistentView.has_value()
               && snapshot.knownPeers.size() == 2;
    }));
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, StaleDiscoveryFanoutIsBoundedAndKeepsActivePeersFresh)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state, 5);
    const std::vector<std::string> activePeers{ kPeer1, kPeer2, kPeer3, kPeer4, kPeer5 };
    for (const auto &peer : { kPeer2, kPeer3, kPeer4, kPeer5 }) {
        SendObservation(*manager, MakeObservation(peer, 5, activePeers));
    }
    std::vector<std::string> candidates{ kPeer1,          kPeer2,          kPeer3,          kPeer4,
                                         kPeer5,          kPeer6,          "127.0.0.7:18480",
                                         "127.0.0.8:18480", "127.0.0.9:18480", "127.0.0.10:18480",
                                         "127.0.0.11:18480", "127.0.0.12:18480", "127.0.0.13:18480",
                                         "127.0.0.14:18480", "127.0.0.15:18480" };
    std::sort(candidates.begin(), candidates.end());
    std::mutex callsMutex;
    std::vector<std::string> calls;
    manager->dependencies_.exchangeObservation =
        [&callsMutex, &calls](const std::string &peer, int32_t, const RaftBootstrapObservationPb &,
                             RaftBootstrapObservationPb &) {
            {
                std::lock_guard<std::mutex> lock(callsMutex);
                calls.emplace_back(peer);
            }
            std::this_thread::sleep_for(kStaleRpcDelay);
            return Status(K_RPC_UNAVAILABLE, "stale discovery endpoint");
        };

    const auto start = std::chrono::steady_clock::now();
    DS_ASSERT_OK(manager->ExchangeBootstrapRound(candidates));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    std::sort(calls.begin(), calls.end());
    EXPECT_EQ(calls.size(), 5U);
    EXPECT_TRUE(std::includes(calls.begin(), calls.end(), activePeers.begin() + 1, activePeers.end()));
    EXPECT_LT(elapsed, kStableViewElapsed);
}

TEST(CoordinatorElectionManagerTest, StaticFivePeerPlanStartsWithoutObservationExchange)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> staticPeers{ kPeer1, kPeer2, kPeer3, kPeer4, kPeer5 };
    state->discoveredPeers = staticPeers;
    auto manager = MakeManager(state, 5, RaftBootstrapMode::STATIC_INITIAL_PEERS);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([&] { return state->startMembershipCalls == 1; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->exchangeCalls, 0U);
        ASSERT_TRUE(std::holds_alternative<BootstrapPlan>(state->raftOptions.startPlan));
        EXPECT_EQ(std::get<BootstrapPlan>(state->raftOptions.startPlan).initialPeers, staticPeers);
    }
    RaftBootstrapObservationPb response;
    EXPECT_EQ(manager->ExchangeBootstrapObservation(MakeObservation(kPeer2, 5, staticPeers), response).GetCode(),
              K_INVALID);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, DifferentPeerViewsDoNotEnterStabilityWindow)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredPeers = { kPeer1, kPeer2, kPeer3 };
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());

    SendObservation(*manager, MakeObservation(kPeer2, 3, { kPeer1, kPeer2, kPeer3 }));
    SendObservation(*manager, MakeObservation(kPeer3, 3, { kPeer1, kPeer3 }));
    ASSERT_TRUE(state->WaitFor([state] { return state->exchangeCalls >= 2; }));

    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_FALSE(snapshot.consistentView.has_value());
    EXPECT_FALSE(snapshot.frozenPlan.has_value());
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, InconsistentViewResetsStabilityWindow)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);
    const std::vector<std::string> fullView{ kPeer1, kPeer2, kPeer3 };
    const auto start = std::chrono::steady_clock::time_point{};
    SetNow(state, start);
    SendObservation(*manager, MakeObservation(kPeer2, 3, fullView));
    SendObservation(*manager, MakeObservation(kPeer3, 3, fullView));

    RaftStartPlan startPlan;
    EXPECT_EQ(manager->TryBuildStartPlan(startPlan).GetCode(), K_NOT_READY);
    SetNow(state, start + std::chrono::milliseconds(500));
    SendObservation(*manager, MakeObservation(kPeer2, 3, { kPeer1, kPeer2 }));
    EXPECT_EQ(manager->TryBuildStartPlan(startPlan).GetCode(), K_NOT_READY);

    SetNow(state, start + kStableViewElapsed);
    SendObservation(*manager, MakeObservation(kPeer2, 3, fullView));
    SendObservation(*manager, MakeObservation(kPeer3, 3, fullView));
    EXPECT_EQ(manager->TryBuildStartPlan(startPlan).GetCode(), K_NOT_READY);
    EXPECT_FALSE(manager->bootstrapState_.frozenPlan.has_value());

    SetNow(state, start + kStableViewElapsed * 2);
    SendObservation(*manager, MakeObservation(kPeer2, 3, fullView));
    SendObservation(*manager, MakeObservation(kPeer3, 3, fullView));
    EXPECT_EQ(manager->TryBuildStartPlan(startPlan).GetCode(), K_NOT_READY);
    ASSERT_TRUE(manager->bootstrapState_.frozenPlan.has_value());
    EXPECT_EQ(manager->bootstrapState_.frozenPlan->initialPeers, fullView);
}

TEST(CoordinatorElectionManagerTest, QuorumConfirmedCommittedConfigurationOverridesFreshBootstrap)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3 };
    state->discoveredPeers = peers;
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());

    SendObservation(*manager, MakeObservation(kPeer2, 3, peers, RAFT_BOOTSTRAP_STARTED, peers));
    SendObservation(*manager, MakeObservation(kPeer3, 3, peers, RAFT_BOOTSTRAP_STARTED, peers));
    ASSERT_TRUE(state->WaitFor([state] { return state->createNodeCalls == 1; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        const auto *plan = std::get_if<BootstrapPlan>(&state->raftOptions.startPlan);
        ASSERT_NE(plan, nullptr);
        EXPECT_EQ(plan->initialPeers, peers);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, StartedRecoveryPropagatesTransitionCommittedConfigurationSeparately)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);
    const std::vector<std::string> transitionPeers{ kPeer1, kPeer2, kPeer3, kPeer4 };
    manager->bootstrapState_.phase = RaftBootstrapPhase::STARTED;
    manager->bootstrapState_.committedPeers = transitionPeers;

    RaftBootstrapObservationPb localObservation;
    DS_ASSERT_OK(manager->BuildLocalObservationLocked({}, localObservation));
    EXPECT_TRUE(localObservation.peers().empty());
    EXPECT_EQ(std::vector<std::string>(localObservation.committed_peers().begin(),
                                       localObservation.committed_peers().end()),
              transitionPeers);

    for (const auto &peer : { kPeer2, kPeer3, kPeer4 }) {
        SendObservation(
            *manager, MakeObservation(peer, 3, {}, RAFT_BOOTSTRAP_STARTED, transitionPeers));
    }
    RaftStartPlan startPlan;
    DS_ASSERT_OK(manager->TryBuildStartPlan(startPlan));
    const auto *plan = std::get_if<BootstrapPlan>(&startPlan);
    ASSERT_NE(plan, nullptr);
    EXPECT_EQ(plan->initialPeers, transitionPeers);
}

TEST(CoordinatorElectionManagerTest, ValidLocalMetadataRecoversWithoutDiscovery)
{
    auto state = std::make_shared<DependencyState>();
    state->metadataState = RaftMetadataState::VALID;
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->createNodeCalls == 1; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_TRUE(std::holds_alternative<RecoverPlan>(state->raftOptions.startPlan));
        EXPECT_EQ(state->discoveryCalls, 0U);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, CorruptLocalMetadataFailsClosed)
{
    auto state = std::make_shared<DependencyState>();
    state->metadataState = RaftMetadataState::CORRUPT;
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitForManager([&manager] {
        RaftBootstrapState snapshot;
        return manager->GetBootstrapState(snapshot).IsOk() && snapshot.phase == RaftBootstrapPhase::TERMINAL;
    }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->createNodeCalls, 0U);
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ObservationValidationRejectsMismatchedTargetAndUnnormalizedPeers)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);

    RaftBootstrapObservationPb response;
    EXPECT_EQ(manager->ExchangeBootstrapObservation(
                  MakeObservation(kPeer2, 5, { kPeer1, kPeer2, kPeer3 }), response)
                  .GetCode(),
              K_INVALID);
    EXPECT_EQ(manager->ExchangeBootstrapObservation(
                  MakeObservation(kPeer2, 3, { kPeer2, kPeer1, kPeer3 }), response)
                  .GetCode(),
              K_INVALID);
}

}  // namespace
}  // namespace datasystem::coordinator
