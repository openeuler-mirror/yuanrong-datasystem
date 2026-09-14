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

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <brpc/socket.h>
#include <brpc/socket_map.h>
#include <butil/endpoint.h>
#include <gtest/gtest.h>

#define private public
#include "datasystem/coordinator/raft/coordinator_election_manager.h"
#undef private
#include "cluster/test_port_allocator.h"
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
    size_t bootstrapExitCalls{ 0 };
    bool nodeAlive{ false };
    bool membershipAlive{ false };
    bool blockedExchangeStarted{ false };
    bool releaseBlockedExchange{ false };
    bool blockedExchangeFinished{ false };
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
    dependencies.onBootstrapWorkerExit = [state] {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->bootstrapExitCalls;
        }
        state->cv.notify_all();
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
                const std::shared_ptr<ICoordinatorDiscovery> &,
                CoordinatorMembershipManager::PeerMetadataProbe) {
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
    dependencies.getLeadershipSnapshot =
        [](const CoordinatorElectionManager::NodeHandle &, CoordinatorLeadershipSnapshot &) {
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

std::vector<brpc::SocketId> FindSocketMapIds(const butil::EndPoint &endpoint)
{
    std::vector<brpc::SocketId> socketIds;
    brpc::SocketMapList(&socketIds);
    socketIds.erase(std::remove_if(socketIds.begin(), socketIds.end(), [&endpoint](brpc::SocketId socketId) {
                        brpc::SocketUniquePtr socket;
                        return brpc::Socket::AddressFailedAsWell(socketId, &socket) < 0 || socket == nullptr
                               || socket->remote_side() != endpoint;
                    }),
                    socketIds.end());
    return socketIds;
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

TEST(CoordinatorElectionManagerTest, BootstrapExitCallbackRunsAfterSuccessfulStartup)
{
    auto state = std::make_shared<DependencyState>();
    state->metadataState = RaftMetadataState::VALID;
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->bootstrapExitCalls == 1; }));
    std::lock_guard<std::mutex> lock(state->mutex);
    EXPECT_EQ(state->startMembershipCalls, 1U);
}

TEST(CoordinatorElectionManagerTest, BootstrapExitCallbackRunsAfterTerminalFailure)
{
    auto state = std::make_shared<DependencyState>();
    state->metadataStatus = Status(K_RUNTIME_ERROR, "metadata probe failed");
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->bootstrapExitCalls == 1; }));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::TERMINAL);
}

TEST(CoordinatorElectionManagerTest, BootstrapExitCallbackRunsAfterCancellation)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredPeers = { kPeer1 };
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->discoveryCalls > 0; }));
    DS_ASSERT_OK(manager->Shutdown());
    std::lock_guard<std::mutex> lock(state->mutex);
    EXPECT_EQ(state->bootstrapExitCalls, 1U);
}

TEST(CoordinatorElectionManagerTest, BootstrapExitWaitsForAllParallelExchanges)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredPeers = { kPeer1, kPeer2, kPeer3 };
    auto manager = MakeManager(state);
    manager->dependencies_.exchangeObservation =
        [state](const std::string &peer, int32_t, const RaftBootstrapObservationPb &request,
                RaftBootstrapObservationPb &response) {
            if (peer == kPeer2) {
                response = request;
                response.set_sender_peer(kPeer3);
                return Status::OK();
            }
            std::unique_lock<std::mutex> lock(state->mutex);
            state->blockedExchangeStarted = true;
            state->cv.notify_all();
            state->cv.wait_until(lock, std::chrono::steady_clock::now() + kWaitTimeout,
                                 [state] { return state->releaseBlockedExchange; });
            state->blockedExchangeFinished = true;
            state->cv.notify_all();
            return Status(K_RPC_UNAVAILABLE, "blocked exchange released");
        };

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->blockedExchangeStarted; }));
    auto shutdown = std::async(std::launch::async, [&manager] { return manager->Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(kStaleRpcDelay), std::future_status::timeout);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->bootstrapExitCalls, 0U);
        state->releaseBlockedExchange = true;
    }
    state->cv.notify_all();
    ASSERT_EQ(shutdown.wait_for(kWaitTimeout), std::future_status::ready);
    DS_ASSERT_OK(shutdown.get());
    ASSERT_TRUE(state->WaitFor(
        [state] { return state->blockedExchangeFinished && state->bootstrapExitCalls == 1; }));
}

CoordinatorElectionManager::Dependencies MakeChannelCleanupDependencies(
    const std::shared_ptr<DependencyState> &state, const std::string &idlePeer,
    const CoordinatorElectionManager::Dependencies &productionDependencies)
{
    auto dependencies = MakeDependencies(state);
    const auto exchangeObservation = productionDependencies.exchangeObservation;
    const auto releaseChannels = productionDependencies.onBootstrapWorkerExit;
    dependencies.exchangeObservation =
        [state, exchangeObservation](const std::string &peer, int32_t timeoutMs,
                                     const RaftBootstrapObservationPb &request,
                                     RaftBootstrapObservationPb &response) {
            auto status = exchangeObservation(peer, timeoutMs, request, response);
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                ++state->exchangeCalls;
            }
            state->cv.notify_all();
            return status;
        };
    dependencies.discoverCandidates =
        [state, idlePeer](const std::shared_ptr<ICoordinatorDiscovery> &, std::vector<std::string> &peers) {
            std::unique_lock<std::mutex> lock(state->mutex);
            ++state->discoveryCalls;
            if (state->discoveryCalls == 1) {
                peers = { kPeer1, idlePeer };
            } else {
                state->cv.wait_until(lock, std::chrono::steady_clock::now() + kWaitTimeout,
                                     [state] { return state->releaseBlockedExchange; });
                state->now += kStableViewElapsed;
                peers = { kPeer1 };
            }
            state->cv.notify_all();
            return Status::OK();
        };
    dependencies.onBootstrapWorkerExit = [state, releaseChannels] {
        if (releaseChannels) {
            releaseChannels();
        }
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->bootstrapExitCalls;
        }
        state->cv.notify_all();
    };
    return dependencies;
}

class CoordinatorElectionManagerProductionDependenciesTest : public testing::Test {
protected:
    void TearDown() override
    {
        datasystem::st::TestPortAllocator::Instance().ReleaseAll();
    }
};

TEST_F(CoordinatorElectionManagerProductionDependenciesTest, BootstrapExitReleasesCachedChannel)
{
    auto &allocator = datasystem::st::TestPortAllocator::Instance();
    allocator.SetOwnerInfo("coordinator_election_manager_test", "BootstrapExitReleasesCachedChannel",
                           kDataDir);
    datasystem::st::TestPortLease portLease;
    DS_ASSERT_OK(allocator.Reserve("idle_bootstrap_peer", portLease));
    const std::string idlePeer = "127.0.0.1:" + std::to_string(portLease.Port());
    butil::EndPoint idleEndpoint;
    ASSERT_EQ(butil::str2endpoint(idlePeer.c_str(), &idleEndpoint), 0);
    EXPECT_TRUE(FindSocketMapIds(idleEndpoint).empty());

    auto state = std::make_shared<DependencyState>();
    auto productionDependencies = CoordinatorElectionManager::MakeProductionDependencies();
    auto manager = std::make_unique<CoordinatorElectionManager>(
        MakeOptions(kPeer1, 1), CoordinatorRaftEventCallbacks{}, std::make_shared<EmptyCoordinatorDiscovery>(),
        MakeChannelCleanupDependencies(state, idlePeer, productionDependencies));

    DS_ASSERT_OK(manager->Start());
    const bool exchangeCompleted = state->WaitFor([state] { return state->exchangeCalls == 1; });
    const auto cachedSocketIds = FindSocketMapIds(idleEndpoint);
    EXPECT_TRUE(exchangeCompleted);
    EXPECT_EQ(cachedSocketIds.size(), 1U);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->releaseBlockedExchange = true;
    }
    state->cv.notify_all();
    ASSERT_TRUE(state->WaitFor([state] { return state->bootstrapExitCalls == 1; }));
    EXPECT_TRUE(FindSocketMapIds(idleEndpoint).empty());
    DS_ASSERT_OK(manager->Shutdown());

    auto request = MakeObservation(kPeer1, 1, {}, RAFT_BOOTSTRAP_STARTED);
    RaftBootstrapObservationPb response;
    EXPECT_FALSE(productionDependencies.exchangeObservation(
        idlePeer, static_cast<int32_t>(kStaleRpcDelay.count()), request, response).IsOk());
    EXPECT_TRUE(FindSocketMapIds(idleEndpoint).empty());
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

TEST(CoordinatorElectionManagerTest, MissingDataWaitsForCommittedExclusionBeforeStarting)
{
    auto state = std::make_shared<DependencyState>();
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3 };
    state->discoveredPeers = peers;
    auto manager = MakeManager(state);
    manager->localMetadataState_ = RaftMetadataState::ABSENT;
    SendObservation(*manager, MakeObservation(kPeer2, 3, {}, RAFT_BOOTSTRAP_STARTED, peers));
    SendObservation(*manager, MakeObservation(kPeer3, 3, {}, RAFT_BOOTSTRAP_STARTED, peers));
    RaftStartPlan plan;
    EXPECT_EQ(manager->TryBuildStartPlan(plan).GetCode(), K_NOT_READY);
    EXPECT_EQ(state->createNodeCalls, 0);
    RaftBootstrapObservationPb response;
    DS_ASSERT_OK(manager->BuildLocalObservationLocked(state->now, response));
    EXPECT_EQ(response.metadata_state(), RaftBootstrapObservationPb::ABSENT);

    const std::vector<std::string> excluded{ kPeer2, kPeer3 };
    SendObservation(*manager, MakeObservation(kPeer2, 3, {}, RAFT_BOOTSTRAP_STARTED, excluded));
    EXPECT_EQ(manager->TryBuildStartPlan(plan).GetCode(), K_NOT_READY);
    SendObservation(*manager, MakeObservation(kPeer3, 3, {}, RAFT_BOOTSTRAP_STARTED, excluded));
    DS_ASSERT_OK(manager->TryBuildStartPlan(plan));
    EXPECT_TRUE(std::holds_alternative<WaitingToJoinPlan>(plan));
}

TEST(CoordinatorElectionManagerTest, LiveProbeTreatsLegacyMetadataAsUnknown)
{
    EXPECT_EQ(RaftBootstrapObservationPb::kMetadataStateFieldNumber, 6);
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);
    auto observation = MakeObservation(kPeer2, 3, { kPeer1, kPeer2, kPeer3 });
    observation.set_metadata_state(RaftBootstrapObservationPb::ABSENT);
    state->exchangeResponses[kPeer2] = observation;
    RaftMetadataState metadataState = RaftMetadataState::UNKNOWN;
    DS_ASSERT_OK(manager->ProbePeerMetadata(kPeer2, metadataState));
    EXPECT_EQ(metadataState, RaftMetadataState::ABSENT);

    observation.clear_metadata_state();
    state->exchangeResponses[kPeer2] = observation;
    metadataState = RaftMetadataState::VALID;
    DS_ASSERT_OK(manager->ProbePeerMetadata(kPeer2, metadataState));
    EXPECT_EQ(metadataState, RaftMetadataState::UNKNOWN);
}

TEST(CoordinatorElectionManagerTest, OnlyCurrentLeaderQueuesMissingDataNotifications)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);
    manager->state_ = CoordinatorElectionManager::LifecycleState::RUNNING;
    manager->node_ = std::make_unique<CoordinatorElectionManager::NodeHandle>();
    manager->membership_ = std::make_unique<CoordinatorElectionManager::MembershipHandle>();
    auto &membership = manager->membership_->membership;
    membership = std::make_unique<CoordinatorMembershipManager>(
        manager->options_.membershipOptions, CoordinatorMembershipManager::Dependencies{},
        std::make_shared<EmptyCoordinatorDiscovery>(), [] { return std::chrono::steady_clock::now(); });
    membership->state_ = CoordinatorMembershipManager::LifecycleState::RUNNING;

    manager->NotifyPeerMissingRaftData(kPeer2);
    EXPECT_TRUE(membership->pendingMissingDataPeers_.empty());

    bool isLeader = false;
    manager->dependencies_.getLeadershipSnapshot =
        [&isLeader](const CoordinatorElectionManager::NodeHandle &, CoordinatorLeadershipSnapshot &snapshot) {
            snapshot.isLeader = isLeader;
            return Status::OK();
        };
    manager->NotifyPeerMissingRaftData(kPeer2);
    EXPECT_TRUE(membership->pendingMissingDataPeers_.empty());
    isLeader = true;
    manager->NotifyPeerMissingRaftData(kPeer2);
    EXPECT_EQ(membership->pendingMissingDataPeers_, (std::set<std::string>{ kPeer2 }));
    isLeader = false;
    manager->NotifyPeerMissingRaftData(kPeer3);
    EXPECT_EQ(membership->pendingMissingDataPeers_, (std::set<std::string>{ kPeer2 }));
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ExistingClusterBarrierSurvivesObservationExpiry)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3 };
    SendObservation(*manager, MakeObservation(kPeer2, 3, {}, RAFT_BOOTSTRAP_STARTED, peers));
    SetNow(state, state->now + std::chrono::hours(1));
    SendObservation(*manager, MakeObservation(kPeer2, 3, peers));
    SendObservation(*manager, MakeObservation(kPeer3, 3, peers));
    RaftStartPlan plan;
    EXPECT_EQ(manager->TryBuildStartPlan(plan).GetCode(), K_NOT_READY);
    EXPECT_TRUE(manager->observedExistingCluster_);
    EXPECT_FALSE(manager->bootstrapState_.frozenPlan);
}

TEST(CoordinatorElectionManagerTest, MissingDataWarningRequiresReportedExistingLocalMember)
{
    const std::vector<std::string> existingPeers{ kPeer2, kPeer3, kPeer4 };
    for (const bool includesLocalPeer : { false, true }) {
        SCOPED_TRACE(includesLocalPeer);
        auto state = std::make_shared<DependencyState>();
        auto manager = MakeManager(state);
        const auto peers = includesLocalPeer ? std::vector<std::string>{ kPeer1, kPeer2, kPeer3 } : existingPeers;
        SendObservation(*manager, MakeObservation(kPeer2, 3, {}, RAFT_BOOTSTRAP_STARTED, peers));
        if (!includesLocalPeer) {
            SendObservation(*manager, MakeObservation(kPeer3, 3, {}, RAFT_BOOTSTRAP_STARTED, peers));
        }

        RaftStartPlan plan;
        testing::internal::CaptureStderr();
        const auto status = manager->TryBuildStartPlan(plan);
        const auto logs = testing::internal::GetCapturedStderr();

        EXPECT_EQ(logs.find("COORDINATOR_RAFT_EXISTING_MEMBER_WITHOUT_LOCAL_DATA") != std::string::npos,
                  includesLocalPeer);
        if (includesLocalPeer) {
            EXPECT_EQ(status.GetCode(), K_NOT_READY);
        } else {
            DS_ASSERT_OK(status);
            EXPECT_TRUE(std::holds_alternative<WaitingToJoinPlan>(plan));
        }
    }
}

TEST(CoordinatorElectionManagerTest, MissingDataWarningWithoutQuorumIsOncePerStartup)
{
    const std::vector<std::string> peers{ kPeer1, kPeer2, kPeer3, kPeer4, kPeer5 };
    for (const size_t activeCount : { 3, 4, 5 }) {
        SCOPED_TRACE(activeCount);
        auto state = std::make_shared<DependencyState>();
        auto manager = MakeManager(state, peers.size());
        SendObservation(*manager, MakeObservation(kPeer2, 5, {}, RAFT_BOOTSTRAP_STARTED, peers));
        SendObservation(*manager, MakeObservation(kPeer3, 5, {}, RAFT_BOOTSTRAP_STARTED, peers));
        for (size_t i = 3; i < activeCount; ++i) {
            SendObservation(*manager, MakeObservation(peers[i], 5, { peers[i] }));
        }
        RaftStartPlan plan;
        testing::internal::CaptureStderr();
        const auto first = manager->TryBuildStartPlan(plan);
        const auto retry = manager->TryBuildStartPlan(plan);
        const auto logs = testing::internal::GetCapturedStderr();
        EXPECT_EQ(first.GetCode(), K_NOT_READY);
        EXPECT_EQ(retry.GetCode(), K_NOT_READY);
        EXPECT_EQ(state->createNodeCalls, 0);
        const std::string marker = "COORDINATOR_RAFT_EXISTING_MEMBER_WITHOUT_LOCAL_DATA";
        const auto warning = logs.find(marker);
        ASSERT_NE(warning, std::string::npos);
        EXPECT_EQ(logs.find(marker, warning + marker.size()), std::string::npos);
        EXPECT_NE(logs.find("confirmations=2 required_quorum=3"), std::string::npos);
    }
}

TEST(CoordinatorElectionManagerTest, StaticFreshStartupAndPersistedRecoveryDoNotReportDataLoss)
{
    for (const auto metadataState : { RaftMetadataState::ABSENT, RaftMetadataState::VALID }) {
        SCOPED_TRACE(static_cast<int>(metadataState));
        auto state = std::make_shared<DependencyState>();
        state->metadataState = metadataState;
        state->discoveredPeers = { kPeer1, kPeer2, kPeer3 };
        auto manager = MakeManager(state, 3, RaftBootstrapMode::STATIC_INITIAL_PEERS);

        testing::internal::CaptureStderr();
        const auto startStatus = manager->Start();
        const bool nodeCreated = state->WaitFor([state] { return state->createNodeCalls == 1; });
        const auto shutdownStatus = manager->Shutdown();
        const auto logs = testing::internal::GetCapturedStderr();

        DS_ASSERT_OK(startStatus);
        DS_ASSERT_OK(shutdownStatus);
        ASSERT_TRUE(nodeCreated);
        EXPECT_EQ(logs.find("COORDINATOR_RAFT_EXISTING_MEMBER_WITHOUT_LOCAL_DATA"), std::string::npos);
        const auto absentLog = logs.find("COORDINATOR_RAFT_METADATA_ABSENT");
        if (metadataState == RaftMetadataState::ABSENT) {
            EXPECT_NE(absentLog, std::string::npos);
        } else {
            EXPECT_EQ(absentLog, std::string::npos);
        }
    }
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

    auto emptyState = std::make_shared<DependencyState>();
    auto emptyManager = MakeManager(emptyState);
    emptyManager->localMetadataState_ = RaftMetadataState::ABSENT;
    for (const auto &peer : { kPeer2, kPeer3, kPeer4 }) {
        SendObservation(
            *emptyManager, MakeObservation(peer, 3, {}, RAFT_BOOTSTRAP_STARTED, transitionPeers));
    }
    RaftStartPlan startPlan;
    EXPECT_EQ(emptyManager->TryBuildStartPlan(startPlan).GetCode(), K_NOT_READY);
    EXPECT_EQ(emptyState->createNodeCalls, 0);
}

TEST(CoordinatorElectionManagerTest, ValidLocalMetadataRecoversWithoutDiscovery)
{
    auto state = std::make_shared<DependencyState>();
    state->metadataState = RaftMetadataState::VALID;
    auto manager = MakeManager(state);
    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->startMembershipCalls == 1; }));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_TRUE(std::holds_alternative<RecoverPlan>(state->raftOptions.startPlan));
        EXPECT_EQ(state->discoveryCalls, 0U);
    }
    EXPECT_EQ(manager->localMetadataState_, RaftMetadataState::VALID);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, WaitingNodePublishesValidCurrentMetadataAfterStart)
{
    auto state = std::make_shared<DependencyState>();
    auto manager = MakeManager(state);
    manager->state_ = CoordinatorElectionManager::LifecycleState::RUNNING;
    manager->localMetadataState_ = RaftMetadataState::ABSENT;

    DS_ASSERT_OK(manager->StartOwnedComponents(WaitingToJoinPlan{}, RaftMetadataState::ABSENT));

    EXPECT_EQ(manager->localMetadataState_, RaftMetadataState::VALID);
    RaftBootstrapObservationPb observation;
    DS_ASSERT_OK(manager->BuildLocalObservationLocked(state->now, observation));
    EXPECT_EQ(observation.metadata_state(), RaftBootstrapObservationPb::VALID);
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
