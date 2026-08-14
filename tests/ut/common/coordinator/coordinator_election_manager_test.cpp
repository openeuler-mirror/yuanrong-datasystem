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
 * Description: Unit tests for the Coordinator bootstrap and election lifecycle owner.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/coordinator/raft/coordinator_membership_manager.h"
#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"
#define private public
#include "datasystem/coordinator/raft/coordinator_election_manager.h"
#undef private
#include "ut/common.h"

namespace datasystem::coordinator {
namespace {
constexpr char kPeer1[] = "127.0.0.1:18480";
constexpr char kPeer2[] = "127.0.0.2:18480";
constexpr char kPeer3[] = "127.0.0.3:18480";
constexpr char kPeer4[] = "127.0.0.4:18480";
constexpr char kDataDir[] = "coordinator-election-manager-test-data";
constexpr char kLeader[] = "127.0.0.2:18480";
constexpr int kHeartbeatIntervalMs = 100;
constexpr int kElectionTimeoutMs = 1'000;
constexpr size_t kExpectedMemberCount = 3;
constexpr std::chrono::milliseconds kHealthCheckInterval{ 10 };
constexpr std::chrono::milliseconds kMemberFailureGrace{ 20 };
constexpr std::chrono::hours kDiscoveryRetryInterval{ 1 };
constexpr std::chrono::milliseconds kBootstrapWarningInterval{ 30 };
constexpr std::chrono::seconds kConcurrencyDeadline{ 2 };
constexpr size_t kConcurrentSnapshotIterations = 100;
constexpr size_t kSha256HexLength = 64;

constexpr char kProbeLocal[] = "probe_local";
constexpr char kDiscovery[] = "discovery";
constexpr char kProbePeer[] = "probe_peer";
constexpr char kCreateNode[] = "create_node";
constexpr char kStartNode[] = "start_node";
constexpr char kCreateMembership[] = "create_membership";
constexpr char kStartMembership[] = "start_membership";
constexpr char kWorkerExit[] = "worker_exit";
constexpr char kShutdownMembership[] = "shutdown_membership";
constexpr char kDestroyMembership[] = "destroy_membership";
constexpr char kDestroyNode[] = "destroy_node";

static_assert(std::is_final_v<CoordinatorElectionManager>);
static_assert(!std::is_copy_constructible_v<CoordinatorElectionManager>);
static_assert(!std::is_copy_assignable_v<CoordinatorElectionManager>);
static_assert(!std::is_move_constructible_v<CoordinatorElectionManager>);
static_assert(!std::is_move_assignable_v<CoordinatorElectionManager>);

class EmptyCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        serviceList.clear();
        return Status::OK();
    }
};

struct PeerScript {
    RaftBootstrapState state;
    Status result;
    bool matchCandidateObservation{ true };
};

class DependencyState {
public:
    void Record(std::string call)
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            calls.emplace_back(std::move(call));
        }
        cv.notify_all();
    }

    bool WaitFor(const std::function<bool()> &predicate)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_until(lock, std::chrono::steady_clock::now() + kConcurrencyDeadline, predicate);
    }

    size_t CallCount(const std::string &call) const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return static_cast<size_t>(std::count(calls.begin(), calls.end(), call));
    }

    std::vector<std::string> Calls() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return calls;
    }

    mutable std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::string> calls;
    RaftMetadataState localMetadataState{ RaftMetadataState::ABSENT };
    Status localMetadataResult;
    bool blockLocalProbe{ false };
    bool releaseLocalProbe{ false };
    std::vector<std::string> discoveredCandidates;
    Status discoveryResult;
    bool discoveryThrows{ false };
    size_t discoveryCalls{ 0 };
    std::string latestDigest;
    size_t latestCandidateCount{ 0 };
    std::map<std::string, PeerScript> peers;
    size_t peerProbeCalls{ 0 };
    Status nodeStartResult;
    bool blockNodeStart{ false };
    bool releaseNodeStart{ false };
    bool nodeStartEntered{ false };
    bool invokeConfigurationOnNodeStart{ false };
    bool invokeErrorOnNodeStart{ false };
    bool invokeErrorOnMembershipStart{ false };
    Status managedErrorStatus;
    std::vector<std::string> configurationOnNodeStart;
    int64_t configurationIndex{ 1 };
    Status membershipStartResult;
    Status membershipShutdownResult;
    bool blockMembershipShutdown{ false };
    bool releaseMembershipShutdown{ false };
    bool membershipShutdownEntered{ false };
    bool nodeAlive{ false };
    bool membershipAlive{ false };
    bool leader{ true };
    CoordinatorRaftOptions raftOptions;
    CoordinatorMembershipOptions membershipOptions;
    CoordinatorRaftEventCallbacks managedCallbacks;
};

CoordinatorElectionOptions MakeOptions(const std::string &localPeer = kPeer1,
                                       size_t expectedMemberCount = kExpectedMemberCount)
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
    return options;
}

CoordinatorElectionManager::Dependencies MakeDependencies(const std::shared_ptr<DependencyState> &state)
{
    CoordinatorElectionManager::Dependencies dependencies;
    dependencies.probeLocalMetadata = [state](const std::string &, RaftMetadataState &metadataState) {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->calls.emplace_back(kProbeLocal);
        state->cv.notify_all();
        if (!state->cv.wait_until(lock, std::chrono::steady_clock::now() + kConcurrencyDeadline,
                                  [state] { return !state->blockLocalProbe || state->releaseLocalProbe; })) {
            return Status(K_RUNTIME_ERROR, "timed out waiting to release local metadata probe");
        }
        metadataState = state->localMetadataState;
        return state->localMetadataResult;
    };
    dependencies.discoverCandidates =
        [state](const std::shared_ptr<ICoordinatorDiscovery> &, std::vector<std::string> &candidates) {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->calls.emplace_back(kDiscovery);
            ++state->discoveryCalls;
            state->cv.notify_all();
            if (state->discoveryThrows) {
                throw std::runtime_error("scripted Discovery exception");
            }
            candidates = state->discoveredCandidates;
            return state->discoveryResult;
        };
    dependencies.digestCandidates = [state](const std::vector<std::string> &candidates, std::string &digest) {
        const auto marker = static_cast<char>('a' + std::min(candidates.size(), size_t{ 25 }));
        digest.assign(kSha256HexLength, marker);
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->latestDigest = digest;
            state->latestCandidateCount = candidates.size();
        }
        state->cv.notify_all();
        return Status::OK();
    };
    dependencies.probePeer = [state](const std::string &peer, int32_t, RaftBootstrapState &bootstrapState) {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->calls.emplace_back(std::string(kProbePeer) + ":" + peer);
        ++state->peerProbeCalls;
        state->cv.notify_all();
        const auto iter = state->peers.find(peer);
        if (iter == state->peers.end()) {
            return Status(K_NOT_READY, "unscripted peer");
        }
        bootstrapState = iter->second.state;
        if (iter->second.matchCandidateObservation) {
            bootstrapState.candidateCount = state->latestCandidateCount;
            bootstrapState.candidateDigest = state->latestDigest;
        }
        return iter->second.result;
    };
    dependencies.now = [] { return std::chrono::steady_clock::time_point{}; };
    dependencies.onBootstrapWorkerExit = [state] { state->Record(kWorkerExit); };
    dependencies.createNode =
        [state](const CoordinatorRaftOptions &options, const CoordinatorRaftEventCallbacks &callbacks) {
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->calls.emplace_back(kCreateNode);
                state->raftOptions = options;
                state->managedCallbacks = callbacks;
                state->nodeAlive = true;
            }
            state->cv.notify_all();
            auto handle = std::make_unique<CoordinatorElectionManager::NodeHandle>();
            handle->onDestroyed = [state] {
                {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    state->calls.emplace_back(kDestroyNode);
                    state->nodeAlive = false;
                }
                state->cv.notify_all();
            };
            return handle;
        };
    dependencies.startNode = [state](CoordinatorElectionManager::NodeHandle &, RaftMetadataState) {
        CoordinatorRaftEventCallbacks callbacks;
        std::vector<std::string> committedPeers;
        int64_t committedIndex = 0;
        bool invokeConfiguration = false;
        bool invokeError = false;
        Status managedErrorStatus;
        Status result;
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->calls.emplace_back(kStartNode);
            state->nodeStartEntered = true;
            state->cv.notify_all();
            if (!state->cv.wait_until(lock, std::chrono::steady_clock::now() + kConcurrencyDeadline,
                                      [state] { return !state->blockNodeStart || state->releaseNodeStart; })) {
                return Status(K_RUNTIME_ERROR, "timed out waiting to release Node startup");
            }
            callbacks = state->managedCallbacks;
            committedPeers = state->configurationOnNodeStart;
            committedIndex = state->configurationIndex;
            invokeConfiguration = state->invokeConfigurationOnNodeStart;
            invokeError = state->invokeErrorOnNodeStart;
            managedErrorStatus = state->managedErrorStatus;
            result = state->nodeStartResult;
        }
        if (result.IsOk() && invokeError && callbacks.onError) {
            callbacks.onError(std::move(managedErrorStatus));
        }
        if (result.IsOk() && invokeConfiguration && callbacks.onConfigurationCommitted) {
            callbacks.onConfigurationCommitted(std::move(committedPeers), committedIndex);
        }
        return result;
    };
    dependencies.createMembership =
        [state](const CoordinatorMembershipOptions &options, CoordinatorElectionManager::NodeHandle &,
                const std::shared_ptr<ICoordinatorDiscovery> &) {
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->calls.emplace_back(kCreateMembership);
                state->membershipOptions = options;
                state->membershipAlive = true;
            }
            state->cv.notify_all();
            auto handle = std::make_unique<CoordinatorElectionManager::MembershipHandle>();
            handle->onDestroyed = [state] {
                {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    state->calls.emplace_back(kDestroyMembership);
                    state->membershipAlive = false;
                }
                state->cv.notify_all();
            };
            return handle;
        };
    dependencies.startMembership = [state](CoordinatorElectionManager::MembershipHandle &) {
        CoordinatorRaftEventCallbacks callbacks;
        bool invokeError = false;
        Status managedErrorStatus;
        Status result;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->calls.emplace_back(kStartMembership);
            callbacks = state->managedCallbacks;
            invokeError = state->invokeErrorOnMembershipStart;
            managedErrorStatus = state->managedErrorStatus;
            result = state->membershipStartResult;
        }
        state->cv.notify_all();
        if (result.IsOk() && invokeError && callbacks.onError) {
            callbacks.onError(std::move(managedErrorStatus));
        }
        return result;
    };
    dependencies.shutdownMembership = [state](CoordinatorElectionManager::MembershipHandle &) {
        std::unique_lock<std::mutex> lock(state->mutex);
        state->calls.emplace_back(kShutdownMembership);
        state->membershipShutdownEntered = true;
        state->cv.notify_all();
        if (!state->cv.wait_until(
                lock, std::chrono::steady_clock::now() + kConcurrencyDeadline,
                [state] { return !state->blockMembershipShutdown || state->releaseMembershipShutdown; })) {
            return Status(K_RUNTIME_ERROR, "timed out waiting to release Membership shutdown");
        }
        return state->membershipShutdownResult;
    };
    dependencies.isLeader = [state](const CoordinatorElectionManager::NodeHandle &) {
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->leader;
    };
    dependencies.getLeader = [](const CoordinatorElectionManager::NodeHandle &, std::string &leaderAddress) {
        leaderAddress = kLeader;
        return Status::OK();
    };
    return dependencies;
}

PeerScript MakePeerState(const std::string &peer, RaftMetadataState metadataState,
                         std::vector<std::string> committedPeers = {})
{
    PeerScript script;
    script.state.probeReady = true;
    script.state.groupId = kCoordinatorRaftGroupId;
    script.state.localPeer = peer;
    script.state.expectedMemberCount = kExpectedMemberCount;
    script.state.metadataState = metadataState;
    script.state.committedPeers = std::move(committedPeers);
    return script;
}

std::unique_ptr<CoordinatorElectionManager> MakeManager(
    const std::shared_ptr<DependencyState> &state, CoordinatorElectionOptions options = MakeOptions(),
    CoordinatorRaftEventCallbacks callbacks = {})
{
    return std::make_unique<CoordinatorElectionManager>(
        std::move(options), std::move(callbacks), std::make_shared<EmptyCoordinatorDiscovery>(),
        MakeDependencies(state));
}

bool WaitForRetry(CoordinatorElectionManager &manager)
{
    std::unique_lock<std::mutex> lock(manager.bootstrapMutex_);
    return manager.bootstrapCv_.wait_until(lock, std::chrono::steady_clock::now() + kConcurrencyDeadline,
                                           [&manager] { return manager.bootstrapRetryWaiters_ > 0; });
}

void WakeRetry(CoordinatorElectionManager &manager)
{
    {
        std::lock_guard<std::mutex> lock(manager.bootstrapMutex_);
        ++manager.bootstrapWakeGeneration_;
    }
    manager.bootstrapCv_.notify_all();
}

bool WaitForTerminalStatus(CoordinatorElectionManager &manager)
{
    std::unique_lock<std::mutex> lock(manager.bootstrapMutex_);
    return manager.bootstrapCv_.wait_until(lock, std::chrono::steady_clock::now() + kConcurrencyDeadline, [&manager] {
        return manager.bootstrapState_.phase == RaftBootstrapPhase::TERMINAL;
    });
}

bool WaitForCall(const std::shared_ptr<DependencyState> &state, const std::string &call)
{
    return state->WaitFor([state, call] {
        return std::find(state->calls.begin(), state->calls.end(), call) != state->calls.end();
    });
}

bool StartAndWaitForWorkerExit(CoordinatorElectionManager &manager,
                               const std::shared_ptr<DependencyState> &state)
{
    return manager.Start().IsOk() && WaitForCall(state, kWorkerExit);
}

const BootstrapPlan &GetBootstrapPlan(const CoordinatorRaftOptions &options)
{
    return std::get<BootstrapPlan>(options.startPlan);
}
}  // namespace

TEST(CoordinatorElectionManagerTest, StartReturnsAfterOwningBootstrapWorker)
{
    auto state = std::make_shared<DependencyState>();
    state->blockLocalProbe = true;
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForCall(state, kProbeLocal));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->releaseLocalProbe = true;
    }
    state->cv.notify_all();
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, OneOfThreeCandidatesWaitsWithoutCreatingNode)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1 };
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::RETRYING);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_NOT_READY));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, TwoOfThreeCandidatesCreateTwoPeerBootstrapPlan)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer2, kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer2 }));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::STARTED);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_OK));
    EXPECT_TRUE(state->nodeAlive);
    EXPECT_TRUE(state->membershipAlive);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, TwoReachableStaticCandidatesBootstrapDespiteUnavailablePeer)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer3, kPeer2, kPeer1 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    auto unavailable = MakePeerState(kPeer3, RaftMetadataState::UNKNOWN);
    unavailable.result = Status(K_RPC_UNAVAILABLE, "scripted unavailable peer");
    state->peers.emplace(kPeer3, std::move(unavailable));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer2 }));
    EXPECT_EQ(state->peerProbeCalls, 2U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, OneReachableStaticCandidateWaitsForBootstrapQuorum)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer3, kPeer2, kPeer1 };
    auto unavailablePeer2 = MakePeerState(kPeer2, RaftMetadataState::UNKNOWN);
    unavailablePeer2.result = Status(K_RPC_UNAVAILABLE, "scripted unavailable peer2");
    auto unavailablePeer3 = MakePeerState(kPeer3, RaftMetadataState::UNKNOWN);
    unavailablePeer3.result = Status(K_RPC_UNAVAILABLE, "scripted unavailable peer3");
    state->peers.emplace(kPeer2, std::move(unavailablePeer2));
    state->peers.emplace(kPeer3, std::move(unavailablePeer3));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    EXPECT_EQ(state->peerProbeCalls, 2U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ReachableUnknownStaticCandidateBlocksFreshBootstrap)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer3, kPeer2, kPeer1 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->peers.emplace(kPeer3, MakePeerState(kPeer3, RaftMetadataState::UNKNOWN));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    EXPECT_EQ(state->peerProbeCalls, 2U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, MoreThanTargetCandidatesSelectFirstSortedPeers)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer4, kPeer3, kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->peers.emplace(kPeer3, MakePeerState(kPeer3, RaftMetadataState::ABSENT));
    state->peers.emplace(kPeer4, MakePeerState(kPeer4, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer2, kPeer3 }));
    EXPECT_EQ(state->CallCount(std::string(kProbePeer) + ":" + kPeer4), 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, UnselectedFirstBootstrapCandidateCreatesWaitingToJoinPlan)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer4, kPeer3, kPeer1, kPeer2 };
    state->peers.emplace(kPeer1, MakePeerState(kPeer1, RaftMetadataState::ABSENT));
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->peers.emplace(kPeer3, MakePeerState(kPeer3, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state, MakeOptions(kPeer4));

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_TRUE(std::holds_alternative<WaitingToJoinPlan>(state->raftOptions.startPlan));
    EXPECT_EQ(state->CallCount(kCreateNode), 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, SingleCommittedConfigurationObservationRetriesWithoutCreatingNode)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3 };
    state->peers.emplace(kPeer2,
                         MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 }));
    state->peers.emplace(kPeer3, MakePeerState(kPeer3, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::RETRYING);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_NOT_READY));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, MatchingFreshCommittedConfigurationCreatesBootstrapPlan)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer1, kPeer2 }));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers, (std::vector<std::string>{ kPeer1, kPeer2 }));
    EXPECT_EQ(state->CallCount(kCreateNode), 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, NonMemberCommittedConfigurationObservationDoesNotCountTowardQuorum)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer4 };
    state->peers.emplace(kPeer2,
                         MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 }));
    state->peers.emplace(kPeer4,
                         MakePeerState(kPeer4, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 }));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::RETRYING);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_NOT_READY));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    EXPECT_EQ(state->peerProbeCalls, 2U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, QuorumCommittedConfigurationWinsOverEarlierUnavailablePeer)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3, kPeer4 };
    auto unavailable = MakePeerState(kPeer2, RaftMetadataState::UNKNOWN);
    unavailable.result = Status(K_RPC_UNAVAILABLE, "scripted unavailable peer");
    state->peers.emplace(kPeer2, std::move(unavailable));
    state->peers.emplace(kPeer3,
                         MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer1, kPeer3, kPeer4 }));
    state->peers.emplace(kPeer4,
                         MakePeerState(kPeer4, RaftMetadataState::VALID, { kPeer1, kPeer3, kPeer4 }));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer3, kPeer4 }));
    EXPECT_EQ(state->peerProbeCalls, 3U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, QuorumCommittedConfigurationWinsOverEarlierAbsentDigestMismatch)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3, kPeer4 };
    auto mismatched = MakePeerState(kPeer2, RaftMetadataState::ABSENT);
    mismatched.matchCandidateObservation = false;
    mismatched.state.candidateCount = 2;
    mismatched.state.candidateDigest.assign(kSha256HexLength, 'f');
    state->peers.emplace(kPeer2, std::move(mismatched));
    state->peers.emplace(kPeer3,
                         MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer1, kPeer3, kPeer4 }));
    state->peers.emplace(kPeer4,
                         MakePeerState(kPeer4, RaftMetadataState::VALID, { kPeer1, kPeer3, kPeer4 }));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer3, kPeer4 }));
    EXPECT_EQ(state->peerProbeCalls, 3U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, BelowConfigQuorumCommittedObservationRetriesWithoutNode)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer2 };
    state->peers.emplace(kPeer2,
                         MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer2, kPeer3, kPeer4 }));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    EXPECT_EQ(state->peerProbeCalls, 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, AbsentCommittedMemberRebuildsAfterCommittedConfigurationQuorum)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3 };
    state->peers.emplace(kPeer2,
                         MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 }));
    state->peers.emplace(kPeer3,
                         MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 }));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer2, kPeer3 }));
    EXPECT_EQ(state->CallCount(kCreateNode), 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ConflictingAuthoritativeConfigurationsRetryUntilTheyConverge)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3 };
    state->peers.emplace(kPeer2,
                         MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 }));
    state->peers.emplace(
        kPeer3, MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3, kPeer4 }));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    RaftBootstrapState retryingSnapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(retryingSnapshot));
    EXPECT_EQ(retryingSnapshot.phase, RaftBootstrapPhase::RETRYING);
    EXPECT_EQ(retryingSnapshot.statusCode, static_cast<int32_t>(K_NOT_READY));
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->peers[kPeer3] =
            MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3 });
    }
    WakeRetry(*manager);
    ASSERT_TRUE(WaitForCall(state, kWorkerExit));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer2, kPeer3 }));
    EXPECT_EQ(state->CallCount(kCreateNode), 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, UnavailableCandidateWithoutValidConfigurationPreventsBootstrap)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    auto unavailable = MakePeerState(kPeer2, RaftMetadataState::UNKNOWN);
    unavailable.result = Status(K_RPC_UNAVAILABLE, "scripted unavailable peer");
    state->peers.emplace(kPeer2, std::move(unavailable));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    EXPECT_EQ(state->peerProbeCalls, 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, RemoteUnknownOrCorruptMetadataRetriesThenCreatesBootstrapPlan)
{
    for (const auto initialMetadataState :
         std::vector<RaftMetadataState>{ RaftMetadataState::UNKNOWN, RaftMetadataState::CORRUPT }) {
        SCOPED_TRACE(static_cast<int>(initialMetadataState));
        auto state = std::make_shared<DependencyState>();
        state->discoveredCandidates = { kPeer1, kPeer2 };
        state->peers.emplace(kPeer2, MakePeerState(kPeer2, initialMetadataState));
        auto manager = MakeManager(state);

        DS_ASSERT_OK(manager->Start());
        ASSERT_TRUE(WaitForRetry(*manager));
        EXPECT_EQ(state->CallCount(kCreateNode), 0U);
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->peers[kPeer2] = MakePeerState(kPeer2, RaftMetadataState::ABSENT);
        }
        WakeRetry(*manager);
        ASSERT_TRUE(WaitForCall(state, kWorkerExit));
        EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
                  (std::vector<std::string>{ kPeer1, kPeer2 }));
        EXPECT_EQ(state->CallCount(kCreateNode), 1U);
        DS_ASSERT_OK(manager->Shutdown());
    }
}

TEST(CoordinatorElectionManagerTest, RemoteUnknownOrCorruptMetadataRetriesThenCreatesWaitingToJoinPlan)
{
    for (const auto initialMetadataState :
         std::vector<RaftMetadataState>{ RaftMetadataState::UNKNOWN, RaftMetadataState::CORRUPT }) {
        SCOPED_TRACE(static_cast<int>(initialMetadataState));
        auto state = std::make_shared<DependencyState>();
        state->discoveredCandidates = { kPeer1, kPeer2, kPeer3 };
        state->peers.emplace(kPeer2, MakePeerState(kPeer2, initialMetadataState));
        state->peers.emplace(kPeer3, MakePeerState(kPeer3, initialMetadataState));
        auto manager = MakeManager(state);

        DS_ASSERT_OK(manager->Start());
        ASSERT_TRUE(WaitForRetry(*manager));
        EXPECT_EQ(state->CallCount(kCreateNode), 0U);
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->peers[kPeer2] =
                MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer2, kPeer3, kPeer4 });
            state->peers[kPeer3] =
                MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer2, kPeer3, kPeer4 });
        }
        WakeRetry(*manager);
        ASSERT_TRUE(WaitForCall(state, kWorkerExit));
        EXPECT_TRUE(std::holds_alternative<WaitingToJoinPlan>(state->raftOptions.startPlan));
        EXPECT_EQ(state->CallCount(kCreateNode), 1U);
        DS_ASSERT_OK(manager->Shutdown());
    }
}

TEST(CoordinatorElectionManagerTest, ValidLocalMetadataRecoversWithoutDiscoveryOrPeerProbe)
{
    auto state = std::make_shared<DependencyState>();
    state->localMetadataState = RaftMetadataState::VALID;
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_TRUE(std::holds_alternative<RecoverPlan>(state->raftOptions.startPlan));
    EXPECT_EQ(state->discoveryCalls, 0U);
    EXPECT_EQ(state->peerProbeCalls, 0U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, CorruptOrUnknownLocalMetadataFailsClosed)
{
    for (const auto metadataState :
         std::vector<RaftMetadataState>{ RaftMetadataState::CORRUPT, RaftMetadataState::UNKNOWN }) {
        SCOPED_TRACE(static_cast<int>(metadataState));
        auto state = std::make_shared<DependencyState>();
        state->localMetadataState = metadataState;
        auto manager = MakeManager(state);

        DS_ASSERT_OK(manager->Start());
        ASSERT_TRUE(WaitForTerminalStatus(*manager));
        RaftBootstrapState snapshot;
        DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
        EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::TERMINAL);
        EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_DATA_INCONSISTENCY));
        EXPECT_EQ(state->discoveryCalls, 0U);
        EXPECT_EQ(state->peerProbeCalls, 0U);
        EXPECT_EQ(state->CallCount(kCreateNode), 0U);
        DS_ASSERT_OK(manager->Shutdown());
    }
}

TEST(CoordinatorElectionManagerTest, ValidRemoteConfigurationQuorumCreatesWaitingToJoinPlan)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3 };
    state->peers.emplace(kPeer2,
                         MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer2, kPeer3, kPeer4 }));
    state->peers.emplace(kPeer3,
                         MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer2, kPeer3, kPeer4 }));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_TRUE(std::holds_alternative<WaitingToJoinPlan>(state->raftOptions.startPlan));
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, AbsentCommittedMemberRebuildsFromTransitionalConfigurationQuorum)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2, kPeer3, kPeer4 };
    state->peers.emplace(
        kPeer2, MakePeerState(kPeer2, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3, kPeer4 }));
    state->peers.emplace(
        kPeer3, MakePeerState(kPeer3, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3, kPeer4 }));
    state->peers.emplace(
        kPeer4, MakePeerState(kPeer4, RaftMetadataState::VALID, { kPeer1, kPeer2, kPeer3, kPeer4 }));
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    EXPECT_EQ(GetBootstrapPlan(state->raftOptions).initialPeers,
              (std::vector<std::string>{ kPeer1, kPeer2, kPeer3, kPeer4 }));
    EXPECT_EQ(state->CallCount(kCreateNode), 1U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, DigestMismatchRetriesWithoutCreatingNode)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    auto peer = MakePeerState(kPeer2, RaftMetadataState::ABSENT);
    peer.matchCandidateObservation = false;
    peer.state.candidateCount = 2;
    peer.state.candidateDigest.assign(kSha256HexLength, 'f');
    state->peers.emplace(kPeer2, std::move(peer));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, DiscoveryFailureRetriesUntilShutdown)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveryResult = Status(K_NOT_READY, "scripted Discovery outage");
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->discoveryCalls, 1U);
    WakeRetry(*manager);
    ASSERT_TRUE(state->WaitFor([state] { return state->discoveryCalls >= 2; }));
    ASSERT_TRUE(WaitForRetry(*manager));
    EXPECT_EQ(state->CallCount(kCreateNode), 0U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ConfigurationCallbackUpdatesBootstrapSnapshotBeforeExternalCallback)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->invokeConfigurationOnNodeStart = true;
    state->configurationOnNodeStart = { kPeer2, kPeer1 };
    std::promise<RaftBootstrapState> observedPromise;
    auto observedFuture = observedPromise.get_future();
    CoordinatorElectionManager *managerAddress = nullptr;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onConfigurationCommitted = [&observedPromise, &managerAddress](std::vector<std::string>, int64_t) {
        RaftBootstrapState snapshot;
        const auto status = managerAddress->GetBootstrapState(snapshot);
        if (status.IsOk()) {
            observedPromise.set_value(std::move(snapshot));
        }
    };
    auto manager = MakeManager(state, MakeOptions(), std::move(callbacks));
    managerAddress = manager.get();

    DS_ASSERT_OK(manager->Start());
    ASSERT_EQ(observedFuture.wait_until(std::chrono::steady_clock::now() + kConcurrencyDeadline),
              std::future_status::ready);
    EXPECT_EQ(observedFuture.get().committedPeers,
              (std::vector<std::string>{ kPeer1, kPeer2 }));
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ManagedRaftCallbacksCarryBootstrapTrace)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    std::vector<std::string> observedTraceIds;
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onLeaderStart = [&observedTraceIds](int64_t) {
        observedTraceIds.emplace_back(Trace::Instance().GetTraceID());
    };
    callbacks.onLeaderStop = [&observedTraceIds](Status) {
        observedTraceIds.emplace_back(Trace::Instance().GetTraceID());
    };
    callbacks.onConfigurationCommitted = [&observedTraceIds](std::vector<std::string>, int64_t) {
        observedTraceIds.emplace_back(Trace::Instance().GetTraceID());
    };
    callbacks.onError = [&observedTraceIds](Status) {
        observedTraceIds.emplace_back(Trace::Instance().GetTraceID());
    };
    callbacks.onShutdown = [&observedTraceIds] {
        observedTraceIds.emplace_back(Trace::Instance().GetTraceID());
    };
    auto manager = MakeManager(state, MakeOptions(), std::move(callbacks));

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    CoordinatorRaftEventCallbacks managedCallbacks;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        managedCallbacks = state->managedCallbacks;
    }
    ASSERT_TRUE(managedCallbacks.onLeaderStart);
    ASSERT_TRUE(managedCallbacks.onLeaderStop);
    ASSERT_TRUE(managedCallbacks.onConfigurationCommitted);
    ASSERT_TRUE(managedCallbacks.onError);
    ASSERT_TRUE(managedCallbacks.onShutdown);
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());

    managedCallbacks.onLeaderStart(2);
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
    managedCallbacks.onLeaderStop(Status::OK());
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
    managedCallbacks.onConfigurationCommitted({ kPeer2, kPeer1 }, 3);
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
    managedCallbacks.onError(Status(K_RPC_UNAVAILABLE, "scripted retryable callback error"));
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());
    managedCallbacks.onShutdown();
    EXPECT_TRUE(Trace::Instance().GetTraceID().empty());

    ASSERT_EQ(observedTraceIds.size(), 5U);
    for (const auto &traceId : observedTraceIds) {
        EXPECT_EQ(traceId.find("CoordinatorBootstrap;"), 0U) << traceId;
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, ShutdownJoinsBootstrapBeforeDestroyingMembershipAndNode)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state);
    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));

    DS_ASSERT_OK(manager->Shutdown());
    const auto calls = state->Calls();
    const auto workerExit = std::find(calls.begin(), calls.end(), kWorkerExit);
    const auto shutdownMembership = std::find(calls.begin(), calls.end(), kShutdownMembership);
    const auto destroyMembership = std::find(calls.begin(), calls.end(), kDestroyMembership);
    const auto destroyNode = std::find(calls.begin(), calls.end(), kDestroyNode);
    ASSERT_NE(workerExit, calls.end());
    ASSERT_NE(shutdownMembership, calls.end());
    ASSERT_NE(destroyMembership, calls.end());
    ASSERT_NE(destroyNode, calls.end());
    EXPECT_LT(workerExit, shutdownMembership);
    EXPECT_LT(shutdownMembership, destroyMembership);
    EXPECT_LT(destroyMembership, destroyNode);
}

TEST(CoordinatorElectionManagerTest, ConcurrentBootstrapStateQueryReturnsCopiedSnapshots)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state);
    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));

    auto writer = std::async(std::launch::async, [state] {
        for (size_t index = 0; index < kConcurrentSnapshotIterations; ++index) {
            state->managedCallbacks.onConfigurationCommitted({ kPeer2, kPeer1 },
                                                              static_cast<int64_t>(index + 1));
        }
    });
    std::vector<std::future<void>> readers;
    for (size_t reader = 0; reader < kExpectedMemberCount; ++reader) {
        readers.emplace_back(std::async(std::launch::async, [&manager] {
            for (size_t index = 0; index < kConcurrentSnapshotIterations; ++index) {
                RaftBootstrapState snapshot;
                DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
                EXPECT_EQ(snapshot.groupId, kCoordinatorRaftGroupId);
                EXPECT_TRUE(snapshot.committedPeers.empty()
                            || snapshot.committedPeers == std::vector<std::string>({ kPeer1, kPeer2 }));
            }
        }));
    }
    ASSERT_EQ(writer.wait_until(std::chrono::steady_clock::now() + kConcurrencyDeadline),
              std::future_status::ready);
    writer.get();
    for (auto &reader : readers) {
        ASSERT_EQ(reader.wait_until(std::chrono::steady_clock::now() + kConcurrencyDeadline),
                  std::future_status::ready);
        reader.get();
    }
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, TerminalNodeStartFailureLeavesManagerNotReadyAndShutdownSafe)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->blockNodeStart = true;
    state->nodeStartResult = Status(K_RUNTIME_ERROR, "scripted asynchronous Node start failure");
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(state->WaitFor([state] { return state->nodeStartEntered; }));
    std::string leader = "stale";
    EXPECT_FALSE(manager->IsLeader());
    EXPECT_EQ(manager->GetLeader(leader).GetCode(), K_NOT_READY);
    EXPECT_TRUE(leader.empty());
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->releaseNodeStart = true;
    }
    state->cv.notify_all();
    ASSERT_TRUE(WaitForTerminalStatus(*manager));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::TERMINAL);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_RUNTIME_ERROR));
    EXPECT_FALSE(manager->IsLeader());
    DS_ASSERT_OK(manager->Shutdown());
    EXPECT_EQ(state->CallCount(kCreateMembership), 0U);
    EXPECT_EQ(state->CallCount(kDestroyNode), 1U);
}

TEST(CoordinatorElectionManagerTest, FatalNodeStartErrorCallbackKeepsTerminalStateSticky)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->invokeErrorOnNodeStart = true;
    state->managedErrorStatus = Status(K_DATA_INCONSISTENCY, "scripted managed Node fatal error");
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::TERMINAL);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_DATA_INCONSISTENCY));
    EXPECT_EQ(state->CallCount(kCreateMembership), 0U);
    EXPECT_EQ(state->CallCount(kDestroyNode), 1U);
    EXPECT_FALSE(state->nodeAlive);
    EXPECT_FALSE(state->membershipAlive);

    DS_ASSERT_OK(manager->Shutdown());
    RaftBootstrapState shutdownSnapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(shutdownSnapshot));
    EXPECT_EQ(shutdownSnapshot.phase, snapshot.phase);
    EXPECT_EQ(shutdownSnapshot.statusCode, snapshot.statusCode);
    EXPECT_EQ(state->CallCount(kDestroyNode), 1U);
}

TEST(CoordinatorElectionManagerTest, RuntimeNodeStartErrorCallbackKeepsTerminalStateSticky)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->invokeErrorOnNodeStart = true;
    state->managedErrorStatus = Status(K_RUNTIME_ERROR, "scripted managed Node runtime error");
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::TERMINAL);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_RUNTIME_ERROR));
    EXPECT_EQ(state->CallCount(kCreateMembership), 0U);
    EXPECT_EQ(state->CallCount(kDestroyNode), 1U);
    EXPECT_FALSE(state->nodeAlive);
    EXPECT_FALSE(state->membershipAlive);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, TransientNodeStartErrorCallbackDoesNotRecordTerminalState)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->invokeErrorOnNodeStart = true;
    state->managedErrorStatus = Status(K_RPC_UNAVAILABLE, "scripted managed Node transient error");
    auto callbackCount = std::make_shared<std::atomic<int>>(0);
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onError = [callbackCount](Status status) {
        EXPECT_EQ(status.GetCode(), K_RPC_UNAVAILABLE);
        callbackCount->fetch_add(1, std::memory_order_relaxed);
    };
    auto manager = MakeManager(state, MakeOptions(), std::move(callbacks));

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::STARTED);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_OK));
    EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 1);
    EXPECT_EQ(state->CallCount(kCreateMembership), 1U);
    EXPECT_EQ(state->CallCount(kDestroyNode), 0U);
    EXPECT_TRUE(state->nodeAlive);
    EXPECT_TRUE(state->membershipAlive);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, SynchronousMembershipStartErrorCallbackKeepsTerminalStateSticky)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->invokeErrorOnMembershipStart = true;
    state->managedErrorStatus = Status(K_DATA_INCONSISTENCY, "scripted managed Membership error");
    auto manager = MakeManager(state);

    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));
    RaftBootstrapState snapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(snapshot));
    EXPECT_EQ(snapshot.phase, RaftBootstrapPhase::TERMINAL);
    EXPECT_EQ(snapshot.statusCode, static_cast<int32_t>(K_DATA_INCONSISTENCY));
    EXPECT_EQ(state->CallCount(kStartMembership), 1U);
    EXPECT_EQ(state->CallCount(kShutdownMembership), 1U);
    EXPECT_EQ(state->CallCount(kDestroyMembership), 1U);
    EXPECT_EQ(state->CallCount(kDestroyNode), 1U);
    EXPECT_FALSE(state->nodeAlive);
    EXPECT_FALSE(state->membershipAlive);

    DS_ASSERT_OK(manager->Shutdown());
    RaftBootstrapState shutdownSnapshot;
    DS_ASSERT_OK(manager->GetBootstrapState(shutdownSnapshot));
    EXPECT_EQ(shutdownSnapshot.phase, snapshot.phase);
    EXPECT_EQ(shutdownSnapshot.statusCode, snapshot.statusCode);
    EXPECT_EQ(state->CallCount(kDestroyMembership), 1U);
    EXPECT_EQ(state->CallCount(kDestroyNode), 1U);
}

TEST(CoordinatorElectionManagerTest, StopMembershipBeforeNodeCreationPreventsLaterMembershipOwnership)
{
    auto state = std::make_shared<DependencyState>();
    state->blockLocalProbe = true;
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForCall(state, kProbeLocal));
    DS_ASSERT_OK(manager->StopMembership());
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->releaseLocalProbe = true;
    }
    state->cv.notify_all();
    ASSERT_TRUE(WaitForCall(state, kWorkerExit));
    EXPECT_EQ(state->CallCount(kCreateNode), 1U);
    EXPECT_EQ(state->CallCount(kCreateMembership), 0U);
    DS_ASSERT_OK(manager->Shutdown());
}

TEST(CoordinatorElectionManagerTest, AsyncMembershipStartFailurePreservesFirstCleanupResult)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->membershipStartResult = Status(K_INVALID, "scripted membership start failure");
    state->membershipShutdownResult = Status(K_RUNTIME_ERROR, "scripted failed-start cleanup failure");
    auto manager = MakeManager(state);

    DS_ASSERT_OK(manager->Start());
    ASSERT_TRUE(WaitForTerminalStatus(*manager));
    const auto first = manager->Shutdown();
    const auto repeated = manager->Shutdown();
    EXPECT_EQ(first.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(repeated.GetCode(), first.GetCode());
    EXPECT_EQ(repeated.GetMsg(), first.GetMsg());
    const auto calls = state->Calls();
    const auto shutdownMembership = std::find(calls.begin(), calls.end(), kShutdownMembership);
    const auto destroyMembership = std::find(calls.begin(), calls.end(), kDestroyMembership);
    const auto destroyNode = std::find(calls.begin(), calls.end(), kDestroyNode);
    ASSERT_NE(shutdownMembership, calls.end());
    ASSERT_NE(destroyMembership, calls.end());
    ASSERT_NE(destroyNode, calls.end());
    EXPECT_LT(shutdownMembership, destroyMembership);
    EXPECT_LT(destroyMembership, destroyNode);
}

TEST(CoordinatorElectionManagerTest, ConcurrentShutdownCallersShareFirstCleanupResult)
{
    auto state = std::make_shared<DependencyState>();
    state->discoveredCandidates = { kPeer1, kPeer2 };
    state->peers.emplace(kPeer2, MakePeerState(kPeer2, RaftMetadataState::ABSENT));
    state->blockMembershipShutdown = true;
    state->membershipShutdownResult = Status(K_RUNTIME_ERROR, "scripted membership shutdown failure");
    auto manager = MakeManager(state);
    ASSERT_TRUE(StartAndWaitForWorkerExit(*manager, state));

    auto first = std::async(std::launch::async, [&manager] { return manager->Shutdown(); });
    ASSERT_TRUE(state->WaitFor([state] { return state->membershipShutdownEntered; }));
    std::promise<void> secondInvokedPromise;
    auto secondInvoked = secondInvokedPromise.get_future();
    auto second = std::async(std::launch::async, [&manager, &secondInvokedPromise] {
        secondInvokedPromise.set_value();
        return manager->Shutdown();
    });
    ASSERT_EQ(secondInvoked.wait_until(std::chrono::steady_clock::now() + kConcurrencyDeadline),
              std::future_status::ready);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->releaseMembershipShutdown = true;
    }
    state->cv.notify_all();

    ASSERT_EQ(first.wait_until(std::chrono::steady_clock::now() + kConcurrencyDeadline),
              std::future_status::ready);
    ASSERT_EQ(second.wait_until(std::chrono::steady_clock::now() + kConcurrencyDeadline),
              std::future_status::ready);
    const auto firstResult = first.get();
    const auto secondResult = second.get();
    EXPECT_EQ(firstResult.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(secondResult.GetCode(), firstResult.GetCode());
    EXPECT_EQ(secondResult.GetMsg(), firstResult.GetMsg());
    EXPECT_EQ(manager->Shutdown().GetMsg(), firstResult.GetMsg());
}

}  // namespace datasystem::coordinator
