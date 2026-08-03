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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include <butil/at_exit.h>
#include <gtest/gtest.h>

#include "cluster/test_port_allocator.h"
#include "common_test.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/coordinator/raft/coordinator_membership_manager.h"
#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#include "datasystem/coordinator/raft/coordinator_raft_service.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/utils/service_discovery.h"

namespace datasystem {
namespace st {
namespace {
using TimePoint = std::chrono::steady_clock::time_point;

constexpr char kLoopbackIp[] = "127.0.0.1";
constexpr int kHeartbeatIntervalMs = 50;
constexpr int kElectionTimeoutMs = 300;
constexpr int kMinimumObservedElectionTimeouts = 3;
constexpr std::chrono::milliseconds kBootstrapCaseBudget{ 6'000 };
constexpr std::chrono::milliseconds kRecoveryCaseBudget{ 6'000 };
constexpr std::chrono::milliseconds kWaitingToJoinObservationWindow{ 1'500 };
constexpr std::chrono::milliseconds kPollInterval{ 20 };
constexpr std::chrono::milliseconds kNegativeObservationDeadline{ 200 };
constexpr std::chrono::seconds kCoordinationDeadline{ 2 };
constexpr std::chrono::seconds kMembershipCaseBudget{ 7 };
constexpr std::chrono::seconds kWaitingNodeObservationWindow{ 1 };
constexpr std::chrono::milliseconds kManagerHealthCheckInterval{ 50 };
constexpr std::chrono::milliseconds kManagerFailureGrace{ 250 };
constexpr std::chrono::milliseconds kFailedLeaderReplacementGrace{ 2'000 };
constexpr std::chrono::milliseconds kManagerObservationTolerance = kManagerHealthCheckInterval + kPollInterval;
constexpr std::chrono::milliseconds kManagerRetryInterval{ 100 };
constexpr std::chrono::milliseconds kManagerNegativeObservationWindow{ 300 };
constexpr std::chrono::seconds kCtestTimeout{ 8 };
constexpr size_t kBootstrapNodeCount = 3;
constexpr size_t kMembershipNodeCount = 4;
constexpr size_t kWaitingNodeIndex = kMembershipNodeCount - 1;

static_assert(kWaitingToJoinObservationWindow
              >= kMinimumObservedElectionTimeouts * std::chrono::milliseconds{ kElectionTimeoutMs });
static_assert(kBootstrapCaseBudget < kCtestTimeout);
static_assert(kRecoveryCaseBudget < kCtestTimeout);
static_assert(kWaitingToJoinObservationWindow < kCtestTimeout);
static_assert(kNegativeObservationDeadline < kCoordinationDeadline);
static_assert(kCoordinationDeadline < kCtestTimeout);
static_assert(kMembershipCaseBudget < kCtestTimeout);
static_assert(kWaitingNodeObservationWindow < kMembershipCaseBudget);
static_assert(kManagerHealthCheckInterval < kManagerFailureGrace);
static_assert(kManagerHealthCheckInterval <= kManagerRetryInterval);
static_assert(kManagerNegativeObservationWindow >= 3 * kManagerHealthCheckInterval);
static_assert(kManagerObservationTolerance < kFailedLeaderReplacementGrace);
static_assert(kFailedLeaderReplacementGrace + kManagerObservationTolerance < kMembershipCaseBudget);

class DynamicCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!firstCallAt_.has_value()) {
            firstCallAt_ = std::chrono::steady_clock::now();
        }
        ++callCount_;
        serviceList = candidates_;
        return result_;
    }

    void SetCandidates(std::vector<std::string> candidates)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        candidates_ = std::move(candidates);
    }

    void SetResult(Status result)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = std::move(result);
    }

    size_t GetCallCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return callCount_;
    }

    std::optional<TimePoint> GetFirstCallAt() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return firstCallAt_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> candidates_;
    Status result_;
    size_t callCount_{ 0 };
    std::optional<TimePoint> firstCallAt_;
};

struct CommittedConfigurationRecord {
    std::vector<std::string> peers;
    int64_t index{ 0 };
};

class CommittedConfigurationHistory {
public:
    void Record(std::vector<std::string> peers, int64_t index)
    {
        std::sort(peers.begin(), peers.end());
        std::lock_guard<std::mutex> lock(mutex_);
        records_.emplace_back(CommittedConfigurationRecord{ std::move(peers), index });
    }

    void Clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        records_.clear();
    }

    bool HasOrderedTransition(const std::vector<std::string> &earlierPeers,
                              const std::vector<std::string> &laterPeers) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &earlier : records_) {
            if (earlier.peers != earlierPeers) {
                continue;
            }
            const auto later =
                std::find_if(records_.begin(), records_.end(), [&earlier, &laterPeers](const auto &record) {
                    return record.peers == laterPeers && record.index > earlier.index;
                });
            if (later != records_.end()) {
                return true;
            }
        }
        return false;
    }

    bool HasConfigurationOtherThan(const std::vector<std::string> &expectedPeers) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::any_of(records_.begin(), records_.end(),
                           [&expectedPeers](const auto &record) { return record.peers != expectedPeers; });
    }

private:
    mutable std::mutex mutex_;
    std::vector<CommittedConfigurationRecord> records_;
};

class DrainEntryObservation {
public:
    DrainEntryObservation() = default;
    ~DrainEntryObservation() = default;

    void Observe()
    {
        std::call_once(observedOnce_, [this] { observedPromise_.set_value(); });
    }

    std::future<void> GetObservedFuture()
    {
        return observedPromise_.get_future();
    }

private:
    std::once_flag observedOnce_;
    std::promise<void> observedPromise_;
};

class BlockingShutdownCallbackState {
public:
    BlockingShutdownCallbackState() : releaseFuture_(releasePromise_.get_future().share())
    {
    }

    void EnterAndWait()
    {
        std::call_once(enteredOnce_, [this] { enteredPromise_.set_value(); });
        releaseObserved_.store(releaseFuture_.wait_until(deadline_) == std::future_status::ready);
    }

    void EnterAndWaitUnlessInvokedOnSubmissionThread(std::thread::id submissionThread)
    {
        if (std::this_thread::get_id() == submissionThread) {
            invokedOnSubmissionThread_.store(true);
            std::call_once(enteredOnce_, [this] { enteredPromise_.set_value(); });
            return;
        }
        EnterAndWait();
    }

    std::future<void> GetEnteredFuture()
    {
        return enteredPromise_.get_future();
    }

    void SetDeadline(std::chrono::steady_clock::time_point deadline)
    {
        deadline_ = deadline;
    }

    void Release()
    {
        std::call_once(releaseOnce_, [this] { releasePromise_.set_value(); });
    }

    bool ReleaseWasObserved() const
    {
        return releaseObserved_.load();
    }

    bool WasInvokedOnSubmissionThread() const
    {
        return invokedOnSubmissionThread_.load();
    }

private:
    std::once_flag enteredOnce_;
    std::promise<void> enteredPromise_;
    std::once_flag releaseOnce_;
    std::promise<void> releasePromise_;
    std::shared_future<void> releaseFuture_;
    std::chrono::steady_clock::time_point deadline_;
    std::atomic<bool> releaseObserved_{ false };
    std::atomic<bool> invokedOnSubmissionThread_{ false };
};

class ScopedShutdownCallbackRelease {
public:
    explicit ScopedShutdownCallbackRelease(std::shared_ptr<BlockingShutdownCallbackState> state)
        : state_(std::move(state))
    {
    }

    ~ScopedShutdownCallbackRelease()
    {
        state_->Release();
    }

private:
    std::shared_ptr<BlockingShutdownCallbackState> state_;
};

struct RaftOperationCompletionState {
    std::atomic<int> callbackCount{ 0 };
    std::once_flag completionOnce;
    std::promise<Status> completion;
};

coordinator::RaftOperationCallback MakeRaftOperationCallback(const std::shared_ptr<RaftOperationCompletionState> &state)
{
    return [state](Status status) {
        state->callbackCount.fetch_add(1, std::memory_order_relaxed);
        std::call_once(state->completionOnce, [state, status = std::move(status)]() mutable {
            state->completion.set_value(std::move(status));
        });
    };
}

template <typename Predicate>
bool WaitUntil(Predicate &&predicate, std::chrono::steady_clock::time_point deadline)
{
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(kPollInterval);
    }
    return predicate();
}

class CoordinatorRaftMembershipTest : public CommonTest {
public:
    CoordinatorRaftMembershipTest() : CommonTest(std::to_string(getpid()))
    {
    }

protected:
    void SetUp() override
    {
        CommonTest::SetUp();

        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName =
            testInfo == nullptr ? "unknown" : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        rootDir_ = testCasePath_ + "/coordinator-raft-membership";

        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(rootDir_, error)) << error.message();

        std::vector<std::string> roles;
        roles.reserve(kMembershipNodeCount);
        for (size_t i = 0; i < kMembershipNodeCount; ++i) {
            roles.emplace_back("coordinator_raft_membership_node_" + std::to_string(i));
        }

        auto &allocator = TestPortAllocator::Instance();
        allocator.SetOwnerInfo("coordinator_raft_node_test", testName, rootDir_);
        const auto reserveStatus = allocator.ReserveBatch(roles, portLeases_);
        ASSERT_TRUE(reserveStatus.IsOk()) << reserveStatus.ToString();
        ASSERT_EQ(portLeases_.size(), kMembershipNodeCount);

        for (size_t i = 0; i < kMembershipNodeCount; ++i) {
            addresses_[i] = std::string(kLoopbackIp) + ":" + std::to_string(portLeases_[i].Port());
            dataDirs_[i] = rootDir_ + "/node-" + std::to_string(i);
            error.clear();
            ASSERT_TRUE(std::filesystem::create_directories(dataDirs_[i], error)) << error.message();
        }
    }

    void TearDown() override
    {
        StopManagers();
        for (auto &node : nodes_) {
            node.reset();
        }
        for (auto &server : rpcServers_) {
            if (server != nullptr) {
                server->Shutdown();
                server.reset();
            }
        }

        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
        TestPortAllocator::Instance().ReleaseAll();
        CommonTest::TearDown();
    }

    void PrepareBootstrapCluster(bool recordCommittedConfigurationHistory = false)
    {
        configurationHistory_->Clear();
        std::vector<std::string> bootstrapPeers(addresses_.begin(), addresses_.begin() + kBootstrapNodeCount);
        for (size_t i = 0; i < kMembershipNodeCount; ++i) {
            auto status = RpcServer::Builder().SetUseBrpc(true).Init(rpcServers_[i]);
            ASSERT_TRUE(status.IsOk()) << status.ToString();

            coordinator::RaftStartPlan startPlan =
                i < kBootstrapNodeCount ? coordinator::RaftStartPlan{ coordinator::BootstrapPlan{ bootstrapPeers } }
                                        : coordinator::RaftStartPlan{ coordinator::WaitingToJoinPlan{} };
            coordinator::CoordinatorRaftOptions options{ addresses_[i], dataDirs_[i], kHeartbeatIntervalMs,
                                                         kElectionTimeoutMs, std::move(startPlan) };
            coordinator::CoordinatorRaftEventCallbacks callbacks;
            if (recordCommittedConfigurationHistory) {
                auto history = configurationHistory_;
                callbacks.onConfigurationCommitted = [history](std::vector<std::string> peers, int64_t index) {
                    history->Record(std::move(peers), index);
                };
            }
            nodes_[i] = std::make_unique<coordinator::CoordinatorRaftNode>(std::move(options), std::move(callbacks));
            status = coordinator::RegisterCoordinatorRaftServices(*rpcServers_[i], addresses_[i]);
            ASSERT_TRUE(status.IsOk()) << status.ToString();
        }

        for (size_t i = 0; i < kMembershipNodeCount; ++i) {
            const auto status = rpcServers_[i]->StartBrpcServer(kLoopbackIp, portLeases_[i].Port());
            ASSERT_TRUE(status.IsOk()) << status.ToString();
        }
        for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
            const auto status = nodes_[i]->Start(coordinator::RaftMetadataState::ABSENT);
            ASSERT_TRUE(status.IsOk()) << status.ToString();
        }
    }

    void StartWaitingNode()
    {
        ASSERT_NE(nodes_[kWaitingNodeIndex], nullptr);
        const auto status = nodes_[kWaitingNodeIndex]->Start(coordinator::RaftMetadataState::ABSENT);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }

    void StopNode(size_t nodeIndex)
    {
        ASSERT_LT(nodeIndex, kMembershipNodeCount);
        ASSERT_NE(nodes_[nodeIndex], nullptr);
        StopManager(nodeIndex);
        nodes_[nodeIndex].reset();
    }

    bool HasUniqueBootstrapLeader(size_t &leaderIndex) const
    {
        size_t observedLeaderIndex = kMembershipNodeCount;
        size_t leaderCount = 0;
        for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
            if (nodes_[i] != nullptr && nodes_[i]->IsLeader()) {
                ++leaderCount;
                observedLeaderIndex = i;
            }
        }
        if (leaderCount != 1 || observedLeaderIndex >= kBootstrapNodeCount) {
            return false;
        }

        for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
            if (nodes_[i] == nullptr) {
                continue;
            }
            std::string observedLeader;
            const auto leaderStatus = nodes_[i]->GetLeader(observedLeader);
            if (leaderStatus.IsError() || observedLeader != addresses_[observedLeaderIndex]) {
                return false;
            }
        }
        leaderIndex = observedLeaderIndex;
        return true;
    }

    bool WaitForLeaderAndFollower(size_t &leaderIndex, size_t &followerIndex,
                                  std::chrono::steady_clock::time_point deadline) const
    {
        if (!WaitUntil([this, &leaderIndex] { return HasUniqueBootstrapLeader(leaderIndex); }, deadline)) {
            return false;
        }
        for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
            if (i != leaderIndex && nodes_[i] != nullptr) {
                followerIndex = i;
                return true;
            }
        }
        return false;
    }

    std::vector<std::string> BootstrapPeers() const
    {
        std::vector<std::string> peers(addresses_.begin(), addresses_.begin() + kBootstrapNodeCount);
        std::sort(peers.begin(), peers.end());
        return peers;
    }

    bool WaitForCommittedFollowerErrors(size_t leaderIndex, size_t followerIndex,
                                        const std::vector<std::string> &expectedPeers,
                                        std::chrono::steady_clock::time_point deadline,
                                        coordinator::CoordinatorRaftMembershipStatus &observedStatus) const
    {
        return WaitUntil(
            [this, leaderIndex, followerIndex, &expectedPeers, &observedStatus] {
                coordinator::CoordinatorRaftMembershipStatus currentStatus;
                const auto status = nodes_[leaderIndex]->GetMembershipStatus(currentStatus);
                if (status.IsError() || !currentStatus.isLeader || currentStatus.committedPeers != expectedPeers) {
                    return false;
                }
                const auto follower = std::find_if(
                    currentStatus.stableFollowers.begin(), currentStatus.stableFollowers.end(),
                    [this, followerIndex](const auto &entry) { return entry.peer == addresses_[followerIndex]; });
                if (follower == currentStatus.stableFollowers.end()
                    || follower->consecutiveErrorTimes <= coordinator::kCoordinatorFollowerFailureErrorThreshold) {
                    return false;
                }
                observedStatus = std::move(currentStatus);
                return true;
            },
            deadline);
    }

    void StartManager(size_t nodeIndex, size_t expectedMemberCount,
                      const std::shared_ptr<ICoordinatorDiscovery> &discovery,
                      std::chrono::milliseconds failureGrace = kManagerFailureGrace)
    {
        ASSERT_LT(nodeIndex, kMembershipNodeCount);
        ASSERT_NE(nodes_[nodeIndex], nullptr);
        ASSERT_EQ(managers_[nodeIndex], nullptr);
        coordinator::CoordinatorMembershipOptions options{ expectedMemberCount, kManagerHealthCheckInterval,
                                                           failureGrace, kManagerRetryInterval };
        managers_[nodeIndex] =
            std::make_unique<coordinator::CoordinatorMembershipManager>(options, *nodes_[nodeIndex], discovery);
        const auto status = managers_[nodeIndex]->Start();
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }

    void StopManager(size_t nodeIndex)
    {
        ASSERT_LT(nodeIndex, kMembershipNodeCount);
        if (managers_[nodeIndex] == nullptr) {
            return;
        }
        const auto status = managers_[nodeIndex]->Shutdown();
        EXPECT_TRUE(status.IsOk()) << status.ToString();
        managers_[nodeIndex].reset();
    }

    void StopManagers()
    {
        for (size_t i = 0; i < kMembershipNodeCount; ++i) {
            StopManager(i);
        }
    }

    bool WaitForCommittedConfiguration(coordinator::CoordinatorRaftNode &node,
                                       const std::vector<std::string> &expectedPeers, int64_t minimumIndex,
                                       std::chrono::steady_clock::time_point deadline, int64_t &observedIndex) const
    {
        std::vector<std::string> observedPeers;
        int64_t currentIndex = 0;
        const bool committed = WaitUntil(
            [&node, &expectedPeers, &observedPeers, &currentIndex, minimumIndex] {
                const auto status = node.GetCommittedConfiguration(observedPeers, currentIndex);
                return status.IsOk() && observedPeers == expectedPeers && currentIndex > minimumIndex;
            },
            deadline);
        if (committed) {
            observedIndex = currentIndex;
        }
        return committed;
    }

    butil::AtExitManager atExitManager_;
    std::vector<TestPortLease> portLeases_;
    std::string rootDir_;
    std::array<std::string, kMembershipNodeCount> addresses_;
    std::array<std::string, kMembershipNodeCount> dataDirs_;
    std::array<std::unique_ptr<RpcServer>, kMembershipNodeCount> rpcServers_;
    std::array<std::unique_ptr<coordinator::CoordinatorRaftNode>, kMembershipNodeCount> nodes_;
    std::shared_ptr<CommittedConfigurationHistory> configurationHistory_{
        std::make_shared<CommittedConfigurationHistory>()
    };
    std::array<std::unique_ptr<coordinator::CoordinatorMembershipManager>, kMembershipNodeCount> managers_;
};

class CoordinatorRaftNodeTest : public CommonTest {
public:
    CoordinatorRaftNodeTest() : CommonTest(std::to_string(getpid()))
    {
    }

protected:
    void SetUp() override
    {
        CommonTest::SetUp();

        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName =
            testInfo == nullptr ? "unknown" : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        rootDir_ = testCasePath_ + "/coordinator-raft-node";
        dataDir_ = rootDir_ + "/data";

        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(dataDir_, error)) << error.message();

        auto &allocator = TestPortAllocator::Instance();
        allocator.SetOwnerInfo("coordinator_raft_node_test", testName, rootDir_);
        const auto reserveStatus = allocator.Reserve("coordinator_raft_node", portLease_);
        ASSERT_TRUE(reserveStatus.IsOk()) << reserveStatus.ToString();
    }

    void TearDown() override
    {
        StopOneNode();

        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
        TestPortAllocator::Instance().ReleaseAll();
        CommonTest::TearDown();
    }

    void StopOneNode()
    {
        node_.reset();
        if (rpcServer_ != nullptr) {
            rpcServer_->Shutdown();
            rpcServer_.reset();
        }
    }

    void PrepareOneNode(coordinator::CoordinatorRaftEventCallbacks callbacks = {},
                        std::optional<coordinator::RaftStartPlan> startPlan = std::nullopt)
    {
        auto status = RpcServer::Builder().SetUseBrpc(true).Init(rpcServer_);
        ASSERT_TRUE(status.IsOk()) << status.ToString();

        localAddress_ = std::string(kLoopbackIp) + ":" + std::to_string(portLease_.Port());
        auto effectiveStartPlan = startPlan.has_value()
                                      ? std::move(*startPlan)
                                      : coordinator::RaftStartPlan{ coordinator::BootstrapPlan{ { localAddress_ } } };
        coordinator::CoordinatorRaftOptions options{ localAddress_, dataDir_, kHeartbeatIntervalMs,
                                                     kElectionTimeoutMs, std::move(effectiveStartPlan) };
        node_ = std::make_unique<coordinator::CoordinatorRaftNode>(std::move(options), std::move(callbacks));

        status = coordinator::RegisterCoordinatorRaftServices(*rpcServer_, localAddress_);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        status = rpcServer_->StartBrpcServer(kLoopbackIp, portLease_.Port());
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }

    void StartOneNode(coordinator::RaftMetadataState metadataState,
                      coordinator::CoordinatorRaftEventCallbacks callbacks = {},
                      std::optional<coordinator::RaftStartPlan> startPlan = std::nullopt)
    {
        ASSERT_NO_FATAL_FAILURE(PrepareOneNode(std::move(callbacks), std::move(startPlan)));
        const auto status = node_->Start(metadataState);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }

    butil::AtExitManager atExitManager_;
    TestPortLease portLease_;
    std::string rootDir_;
    std::string dataDir_;
    std::string localAddress_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::unique_ptr<coordinator::CoordinatorRaftNode> node_;
};

TEST_F(CoordinatorRaftMembershipTest, AddAndRemovePeerPublishCommittedMembership)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster());

    size_t leaderIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitUntil([this, &leaderIndex] { return HasUniqueBootstrapLeader(leaderIndex); }, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    ASSERT_LT(leaderIndex, kBootstrapNodeCount);
    auto &leader = *nodes_[leaderIndex];

    auto initialPeers = std::vector<std::string>(addresses_.begin(), addresses_.begin() + kBootstrapNodeCount);
    std::sort(initialPeers.begin(), initialPeers.end());
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(leader, initialPeers, 0, caseDeadline, initialConfigurationIndex))
        << "leader did not publish the initial committed configuration";
    EXPECT_GT(initialConfigurationIndex, 0);

    const auto waitingStartStatus = nodes_[kWaitingNodeIndex]->Start(coordinator::RaftMetadataState::ABSENT);
    ASSERT_TRUE(waitingStartStatus.IsOk()) << waitingStartStatus.ToString();
    const auto waitingObservationDeadline =
        std::min(caseDeadline, std::chrono::steady_clock::now() + kWaitingNodeObservationWindow);
    EXPECT_FALSE(WaitUntil([this] { return nodes_[kWaitingNodeIndex]->IsLeader(); }, waitingObservationDeadline));

    const size_t oldFollowerIndex = leaderIndex == 0 ? 1 : 0;
    ASSERT_LT(oldFollowerIndex, kBootstrapNodeCount);
    ASSERT_FALSE(nodes_[oldFollowerIndex]->IsLeader());

    EXPECT_EQ(leader.AddPeer(addresses_[kWaitingNodeIndex], coordinator::RaftOperationCallback{}).GetCode(), K_INVALID);

    auto invalidPeerCallbackCount = std::make_shared<std::atomic<int>>(0);
    EXPECT_EQ(leader
                  .AddPeer("invalid-peer",
                           [invalidPeerCallbackCount](const Status &) {
                               invalidPeerCallbackCount->fetch_add(1, std::memory_order_relaxed);
                           })
                  .GetCode(),
              K_INVALID);
    EXPECT_EQ(invalidPeerCallbackCount->load(std::memory_order_relaxed), 0);

    auto nonLeaderCallbackCount = std::make_shared<std::atomic<int>>(0);
    EXPECT_EQ(nodes_[oldFollowerIndex]
                  ->AddPeer(addresses_[kWaitingNodeIndex],
                            [nonLeaderCallbackCount](const Status &) {
                                nonLeaderCallbackCount->fetch_add(1, std::memory_order_relaxed);
                            })
                  .GetCode(),
              K_RUNTIME_ERROR);
    EXPECT_EQ(nonLeaderCallbackCount->load(std::memory_order_relaxed), 0);

    auto fourPeers = initialPeers;
    fourPeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(fourPeers.begin(), fourPeers.end());

    auto addCompletion = std::make_shared<RaftOperationCompletionState>();
    auto addFuture = addCompletion->completion.get_future();
    const auto addSubmissionStatus = leader.AddPeer(
        addresses_[kWaitingNodeIndex],
        [&leader, addCompletion, expectedPeers = fourPeers, initialConfigurationIndex](Status status) mutable {
            addCompletion->callbackCount.fetch_add(1, std::memory_order_relaxed);
            std::vector<std::string> committedPeers;
            int64_t configurationIndex = 0;
            const auto observeStatus = leader.GetCommittedConfiguration(committedPeers, configurationIndex);
            if (status.IsOk() && observeStatus.IsError()) {
                status = observeStatus;
            } else if (status.IsOk()
                       && (committedPeers != expectedPeers || configurationIndex <= initialConfigurationIndex)) {
                status = Status(K_RUNTIME_ERROR, "AddPeer callback ran before committed configuration publication");
            } else if (status.IsOk() && !leader.HasInFlightMembershipOperation()) {
                status = Status(K_RUNTIME_ERROR, "AddPeer token was released before completion callback returned");
            }
            std::call_once(addCompletion->completionOnce,
                           [addCompletion, status = std::move(status)]() mutable {
                               addCompletion->completion.set_value(std::move(status));
                           });
        });
    ASSERT_TRUE(addSubmissionStatus.IsOk()) << addSubmissionStatus.ToString();
    ASSERT_EQ(addFuture.wait_until(caseDeadline), std::future_status::ready)
        << "AddPeer callback did not complete before the membership deadline";
    const auto addCallbackStatus = addFuture.get();
    ASSERT_TRUE(addCallbackStatus.IsOk()) << addCallbackStatus.ToString();
    EXPECT_EQ(addCompletion->callbackCount.load(std::memory_order_relaxed), 1);
    ASSERT_TRUE(WaitUntil([&leader] { return !leader.HasInFlightMembershipOperation(); }, caseDeadline))
        << "AddPeer token was not released after completion callback returned";
    int64_t addConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(leader, fourPeers, initialConfigurationIndex, caseDeadline,
                                              addConfigurationIndex))
        << "leader did not publish the committed four-peer configuration";
    ASSERT_LT(initialConfigurationIndex, addConfigurationIndex);
    EXPECT_EQ(addCompletion->callbackCount.load(std::memory_order_relaxed), 1);

    auto removeCompletion = std::make_shared<RaftOperationCompletionState>();
    auto removeFuture = removeCompletion->completion.get_future();
    const auto removeSubmissionStatus =
        leader.RemovePeer(addresses_[oldFollowerIndex], MakeRaftOperationCallback(removeCompletion));
    ASSERT_TRUE(removeSubmissionStatus.IsOk()) << removeSubmissionStatus.ToString();
    ASSERT_EQ(removeFuture.wait_until(caseDeadline), std::future_status::ready)
        << "RemovePeer callback did not complete before the membership deadline";
    const auto removeCallbackStatus = removeFuture.get();
    ASSERT_TRUE(removeCallbackStatus.IsOk()) << removeCallbackStatus.ToString();
    EXPECT_EQ(removeCompletion->callbackCount.load(std::memory_order_relaxed), 1);

    std::vector<std::string> expectedFinalPeers;
    expectedFinalPeers.reserve(kBootstrapNodeCount);
    for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
        if (i != oldFollowerIndex) {
            expectedFinalPeers.emplace_back(addresses_[i]);
        }
    }
    expectedFinalPeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(expectedFinalPeers.begin(), expectedFinalPeers.end());

    int64_t removeConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(leader, expectedFinalPeers, addConfigurationIndex, caseDeadline,
                                              removeConfigurationIndex))
        << "leader did not publish the committed three-peer configuration after removal";
    ASSERT_LT(addConfigurationIndex, removeConfigurationIndex);
    EXPECT_EQ(removeCompletion->callbackCount.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(addCompletion->callbackCount.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(invalidPeerCallbackCount->load(std::memory_order_relaxed), 0);
    EXPECT_EQ(nonLeaderCallbackCount->load(std::memory_order_relaxed), 0);
}

TEST_F(CoordinatorRaftMembershipTest, DestructionWaitsForBlockedMembershipCallback)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster());

    size_t leaderIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitUntil([this, &leaderIndex] { return HasUniqueBootstrapLeader(leaderIndex); }, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    auto drainEntryObservation = std::make_shared<DrainEntryObservation>();
    auto drainEntered = drainEntryObservation->GetObservedFuture();
    coordinator::detail::CoordinatorRaftNodeTestAccessor::SetOperationDrainEntryObserver(
        *nodes_[leaderIndex], [drainEntryObservation] { drainEntryObservation->Observe(); });
    ASSERT_NO_FATAL_FAILURE(StartWaitingNode());

    auto callbackState = std::make_shared<BlockingShutdownCallbackState>();
    auto callbackEntered = callbackState->GetEnteredFuture();
    callbackState->SetDeadline(caseDeadline);
    std::future<void> destruction;
    // Release the callback before destruction's future performs an implicit join during assertion unwinding.
    ScopedShutdownCallbackRelease releaseOnExit(callbackState);
    const auto submissionThread = std::this_thread::get_id();
    const auto submissionStatus =
        nodes_[leaderIndex]->AddPeer(addresses_[kWaitingNodeIndex], [callbackState, submissionThread](Status) {
            callbackState->EnterAndWaitUnlessInvokedOnSubmissionThread(submissionThread);
        });
    ASSERT_TRUE(submissionStatus.IsOk()) << submissionStatus.ToString();
    ASSERT_EQ(callbackEntered.wait_until(caseDeadline), std::future_status::ready)
        << "AddPeer completion callback did not enter before the membership deadline";
    if (callbackState->WasInvokedOnSubmissionThread()) {
        callbackState->Release();
        GTEST_SKIP() << "braft completion used the inline fallback on the AddPeer submission thread";
    }

    destruction = std::async(std::launch::async, [node = std::move(nodes_[leaderIndex])]() mutable { node.reset(); });
    ASSERT_EQ(drainEntered.wait_until(caseDeadline), std::future_status::ready)
        << "destroyed leader did not enter the operation callback drain";
    EXPECT_EQ(destruction.wait_for(std::chrono::milliseconds::zero()), std::future_status::timeout)
        << "Node destruction returned from operation drain before the blocked membership callback was released";

    callbackState->Release();
    const auto destructionWaitStatus = destruction.wait_until(caseDeadline);
    EXPECT_EQ(destructionWaitStatus, std::future_status::ready);
    if (destructionWaitStatus == std::future_status::ready) {
        EXPECT_NO_THROW(destruction.get());
    }
    EXPECT_TRUE(callbackState->ReleaseWasObserved());
}

TEST_F(CoordinatorRaftMembershipTest, HealthyFullMembershipSkipsDiscovery)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster());

    size_t leaderIndex = kMembershipNodeCount;
    size_t followerIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitForLeaderAndFollower(leaderIndex, followerIndex, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    EXPECT_NE(leaderIndex, followerIndex);
    const auto expectedPeers = BootstrapPeers();
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(
        WaitForCommittedConfiguration(*nodes_[leaderIndex], expectedPeers, 0, caseDeadline, initialConfigurationIndex));

    auto discovery = std::make_shared<DynamicCoordinatorDiscovery>();
    ASSERT_NO_FATAL_FAILURE(StartManager(leaderIndex, kBootstrapNodeCount, discovery));
    const auto observationDeadline =
        std::min(caseDeadline, std::chrono::steady_clock::now() + kManagerNegativeObservationWindow);
    EXPECT_FALSE(WaitUntil([&discovery] { return discovery->GetCallCount() != 0; }, observationDeadline));

    std::vector<std::string> observedPeers;
    int64_t observedConfigurationIndex = 0;
    const auto status = nodes_[leaderIndex]->GetCommittedConfiguration(observedPeers, observedConfigurationIndex);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(observedPeers, expectedPeers);
    EXPECT_EQ(observedConfigurationIndex, initialConfigurationIndex);
    EXPECT_EQ(discovery->GetCallCount(), 0U);
    StopManager(leaderIndex);
}

TEST_F(CoordinatorRaftMembershipTest, ManagersOnAllNodesReplaceFailedLeaderWithDiscoveredCandidate)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster(true));

    size_t oldLeaderIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitUntil([this, &oldLeaderIndex] { return HasUniqueBootstrapLeader(oldLeaderIndex); }, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    const auto initialPeers = BootstrapPeers();
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(*nodes_[oldLeaderIndex], initialPeers, 0, caseDeadline,
                                              initialConfigurationIndex))
        << "initial leader did not publish the committed bootstrap configuration";
    ASSERT_NO_FATAL_FAILURE(StartWaitingNode());

    auto discovery = std::make_shared<DynamicCoordinatorDiscovery>();
    discovery->SetCandidates({ addresses_[kWaitingNodeIndex] });
    for (size_t i = 0; i < kMembershipNodeCount; ++i) {
        ASSERT_NO_FATAL_FAILURE(StartManager(i, kBootstrapNodeCount, discovery, kFailedLeaderReplacementGrace));
    }

    const auto healthyObservationDeadline =
        std::min(caseDeadline, std::chrono::steady_clock::now() + kManagerNegativeObservationWindow);
    EXPECT_FALSE(WaitUntil([&discovery] { return discovery->GetCallCount() != 0; }, healthyObservationDeadline));
    ASSERT_EQ(discovery->GetCallCount(), 0U);

    StopManager(oldLeaderIndex);
    ASSERT_NO_FATAL_FAILURE(StopNode(oldLeaderIndex));

    size_t newLeaderIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitUntil([this, &newLeaderIndex] { return HasUniqueBootstrapLeader(newLeaderIndex); }, caseDeadline))
        << "remaining bootstrap nodes did not elect one replacement leader";
    ASSERT_NE(newLeaderIndex, oldLeaderIndex);

    coordinator::CoordinatorRaftMembershipStatus observedStatus;
    ASSERT_TRUE(
        WaitForCommittedFollowerErrors(newLeaderIndex, oldLeaderIndex, initialPeers, caseDeadline, observedStatus))
        << "new leader did not report the destroyed old leader above the follower error threshold";

    const auto failedFollowerObservedAt = std::chrono::steady_clock::now();
    const auto noDiscoveryBefore =
        failedFollowerObservedAt + kFailedLeaderReplacementGrace - kManagerObservationTolerance;
    ASSERT_TRUE(noDiscoveryBefore < caseDeadline)
        << "failure grace measured from the new-term errors>5 observation must fit the membership case budget";
    ASSERT_EQ(discovery->GetCallCount(), 0U) << "Discovery was called before the new-term failed follower was observed";
    ASSERT_FALSE(discovery->GetFirstCallAt().has_value())
        << "Discovery recorded a call before the new-term failed follower was observed";
    EXPECT_FALSE(WaitUntil(
        [&discovery, noDiscoveryBefore] {
            const auto firstCallAt = discovery->GetFirstCallAt();
            return firstCallAt.has_value() && *firstCallAt < noDiscoveryBefore;
        },
        noDiscoveryBefore))
        << "Discovery was called before grace elapsed from the new-term errors>5 observation, allowing only the "
           "bounded Manager observation tolerance";

    auto intermediatePeers = initialPeers;
    intermediatePeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(intermediatePeers.begin(), intermediatePeers.end());
    auto finalPeers = initialPeers;
    finalPeers.erase(std::remove(finalPeers.begin(), finalPeers.end(), addresses_[oldLeaderIndex]), finalPeers.end());
    finalPeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(finalPeers.begin(), finalPeers.end());

    int64_t finalConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(*nodes_[newLeaderIndex], finalPeers, initialConfigurationIndex,
                                              caseDeadline, finalConfigurationIndex))
        << "new leader manager did not commit the discovered replacement";
    ASSERT_TRUE(
        WaitUntil([this, &intermediatePeers,
                   &finalPeers] { return configurationHistory_->HasOrderedTransition(intermediatePeers, finalPeers); },
                  caseDeadline))
        << "committed configuration history did not prove N+1 before final N";

    const auto firstCallAt = discovery->GetFirstCallAt();
    ASSERT_TRUE(firstCallAt.has_value()) << "replacement completed without a recorded Discovery call";
    ASSERT_TRUE(*firstCallAt >= noDiscoveryBefore)
        << "Discovery was called before grace elapsed from the new-term errors>5 observation";
}

TEST_F(CoordinatorRaftMembershipTest, FollowerFailureExceedsHealthThreshold)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster());

    size_t leaderIndex = kMembershipNodeCount;
    size_t followerIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitForLeaderAndFollower(leaderIndex, followerIndex, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    const auto expectedPeers = BootstrapPeers();
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(
        WaitForCommittedConfiguration(*nodes_[leaderIndex], expectedPeers, 0, caseDeadline, initialConfigurationIndex));

    ASSERT_NO_FATAL_FAILURE(StopNode(followerIndex));
    coordinator::CoordinatorRaftMembershipStatus observedStatus;
    ASSERT_TRUE(WaitForCommittedFollowerErrors(leaderIndex, followerIndex, expectedPeers, caseDeadline, observedStatus))
        << "leader did not report the stopped committed follower above the error threshold";
    const auto failedFollower = std::find_if(
        observedStatus.stableFollowers.begin(), observedStatus.stableFollowers.end(),
        [this, followerIndex](const auto &follower) { return follower.peer == addresses_[followerIndex]; });
    ASSERT_NE(failedFollower, observedStatus.stableFollowers.end());
    EXPECT_GT(failedFollower->consecutiveErrorTimes, coordinator::kCoordinatorFollowerFailureErrorThreshold);
    EXPECT_EQ(observedStatus.committedPeers, expectedPeers);
}

TEST_F(CoordinatorRaftMembershipTest, VacancyUsesDiscoveryToAddCandidate)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster());

    size_t leaderIndex = kMembershipNodeCount;
    size_t followerIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitForLeaderAndFollower(leaderIndex, followerIndex, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    EXPECT_NE(leaderIndex, followerIndex);
    const auto initialPeers = BootstrapPeers();
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(
        WaitForCommittedConfiguration(*nodes_[leaderIndex], initialPeers, 0, caseDeadline, initialConfigurationIndex));
    ASSERT_NO_FATAL_FAILURE(StartWaitingNode());

    auto discovery = std::make_shared<DynamicCoordinatorDiscovery>();
    discovery->SetCandidates({ addresses_[kWaitingNodeIndex] });
    ASSERT_NO_FATAL_FAILURE(StartManager(leaderIndex, kMembershipNodeCount, discovery));

    auto expectedPeers = initialPeers;
    expectedPeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(expectedPeers.begin(), expectedPeers.end());
    int64_t addedConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(*nodes_[leaderIndex], expectedPeers, initialConfigurationIndex,
                                              caseDeadline, addedConfigurationIndex))
        << "manager did not commit the discovered candidate into the vacant membership";
    EXPECT_GT(discovery->GetCallCount(), 0U);
    EXPECT_NE(std::find(expectedPeers.begin(), expectedPeers.end(), addresses_[kWaitingNodeIndex]),
              expectedPeers.end());
    for (const auto &originalPeer : initialPeers) {
        EXPECT_NE(std::find(expectedPeers.begin(), expectedPeers.end(), originalPeer), expectedPeers.end());
    }
    StopManager(leaderIndex);
}

TEST_F(CoordinatorRaftMembershipTest, ReplacementUsesDiscoveryAndCommitsAddBeforeRemove)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster(true));

    size_t leaderIndex = kMembershipNodeCount;
    size_t followerIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitForLeaderAndFollower(leaderIndex, followerIndex, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    const auto initialPeers = BootstrapPeers();
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(
        WaitForCommittedConfiguration(*nodes_[leaderIndex], initialPeers, 0, caseDeadline, initialConfigurationIndex));
    ASSERT_NO_FATAL_FAILURE(StartWaitingNode());
    ASSERT_NO_FATAL_FAILURE(StopNode(followerIndex));

    coordinator::CoordinatorRaftMembershipStatus observedStatus;
    ASSERT_TRUE(WaitForCommittedFollowerErrors(leaderIndex, followerIndex, initialPeers, caseDeadline, observedStatus))
        << "leader did not confirm errors for the stopped committed follower";

    auto discovery = std::make_shared<DynamicCoordinatorDiscovery>();
    discovery->SetCandidates({ addresses_[kWaitingNodeIndex] });
    ASSERT_NO_FATAL_FAILURE(StartManager(leaderIndex, kBootstrapNodeCount, discovery));

    auto intermediatePeers = initialPeers;
    intermediatePeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(intermediatePeers.begin(), intermediatePeers.end());
    std::vector<std::string> finalPeers;
    finalPeers.reserve(kBootstrapNodeCount);
    for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
        if (i != followerIndex) {
            finalPeers.emplace_back(addresses_[i]);
        }
    }
    finalPeers.emplace_back(addresses_[kWaitingNodeIndex]);
    std::sort(finalPeers.begin(), finalPeers.end());

    int64_t finalConfigurationIndex = 0;
    ASSERT_TRUE(WaitForCommittedConfiguration(*nodes_[leaderIndex], finalPeers, initialConfigurationIndex, caseDeadline,
                                              finalConfigurationIndex))
        << "manager did not commit the replacement membership";
    ASSERT_TRUE(
        WaitUntil([this, &intermediatePeers,
                   &finalPeers] { return configurationHistory_->HasOrderedTransition(intermediatePeers, finalPeers); },
                  caseDeadline))
        << "committed configuration history did not prove N+1 before final N";
    EXPECT_GT(discovery->GetCallCount(), 0U);
    EXPECT_EQ(finalPeers.size(), kBootstrapNodeCount);
    EXPECT_NE(std::find(finalPeers.begin(), finalPeers.end(), addresses_[kWaitingNodeIndex]), finalPeers.end());
    EXPECT_EQ(std::find(finalPeers.begin(), finalPeers.end(), addresses_[followerIndex]), finalPeers.end());
    StopManager(leaderIndex);
}

TEST_F(CoordinatorRaftMembershipTest, DiscoveryUnavailableKeepsOriginalMembership)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster(true));

    size_t leaderIndex = kMembershipNodeCount;
    size_t followerIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitForLeaderAndFollower(leaderIndex, followerIndex, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    const auto initialPeers = BootstrapPeers();
    int64_t initialConfigurationIndex = 0;
    ASSERT_TRUE(
        WaitForCommittedConfiguration(*nodes_[leaderIndex], initialPeers, 0, caseDeadline, initialConfigurationIndex));
    ASSERT_NO_FATAL_FAILURE(StopNode(followerIndex));

    coordinator::CoordinatorRaftMembershipStatus observedStatus;
    ASSERT_TRUE(WaitForCommittedFollowerErrors(leaderIndex, followerIndex, initialPeers, caseDeadline, observedStatus))
        << "leader did not confirm errors for the stopped committed follower";
    configurationHistory_->Clear();

    auto discovery = std::make_shared<DynamicCoordinatorDiscovery>();
    discovery->SetResult(Status(K_RUNTIME_ERROR, "test discovery unavailable"));
    ASSERT_NO_FATAL_FAILURE(StartManager(leaderIndex, kBootstrapNodeCount, discovery));
    ASSERT_TRUE(WaitUntil([&discovery] { return discovery->GetCallCount() > 0; }, caseDeadline))
        << "manager did not attempt Discovery after the failure grace";

    const auto observationDeadline =
        std::min(caseDeadline, std::chrono::steady_clock::now() + kManagerNegativeObservationWindow);
    EXPECT_FALSE(WaitUntil(
        [this, leaderIndex, &initialPeers, initialConfigurationIndex] {
            if (configurationHistory_->HasConfigurationOtherThan(initialPeers)) {
                return true;
            }
            std::vector<std::string> peers;
            int64_t index = 0;
            const auto status = nodes_[leaderIndex]->GetCommittedConfiguration(peers, index);
            return status.IsOk() && (peers != initialPeers || index != initialConfigurationIndex);
        },
        observationDeadline));

    std::vector<std::string> observedPeers;
    int64_t observedConfigurationIndex = 0;
    const auto status = nodes_[leaderIndex]->GetCommittedConfiguration(observedPeers, observedConfigurationIndex);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(observedPeers, initialPeers);
    EXPECT_EQ(observedConfigurationIndex, initialConfigurationIndex);
    EXPECT_FALSE(configurationHistory_->HasConfigurationOtherThan(initialPeers));
    StopManager(leaderIndex);
}

TEST_F(CoordinatorRaftMembershipTest, MembershipStatusReportsLeaderAndFollowerSemantics)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kMembershipCaseBudget;
    ASSERT_NO_FATAL_FAILURE(PrepareBootstrapCluster());

    size_t leaderIndex = kMembershipNodeCount;
    ASSERT_TRUE(WaitUntil([this, &leaderIndex] { return HasUniqueBootstrapLeader(leaderIndex); }, caseDeadline))
        << "three-node bootstrap cluster did not converge on one leader";
    ASSERT_LT(leaderIndex, kBootstrapNodeCount);

    auto expectedPeers = std::vector<std::string>(addresses_.begin(), addresses_.begin() + kBootstrapNodeCount);
    std::sort(expectedPeers.begin(), expectedPeers.end());
    coordinator::CoordinatorRaftMembershipStatus leaderStatus;
    ASSERT_TRUE(WaitUntil(
        [this, leaderIndex, &expectedPeers, &leaderStatus] {
            const auto status = nodes_[leaderIndex]->GetMembershipStatus(leaderStatus);
            return status.IsOk() && leaderStatus.isLeader && leaderStatus.committedPeers == expectedPeers
                   && leaderStatus.stableFollowers.size() == kBootstrapNodeCount - 1
                   && std::all_of(
                       leaderStatus.stableFollowers.begin(), leaderStatus.stableFollowers.end(),
                       [](const coordinator::CoordinatorFollowerStatus &follower) { return follower.valid; });
        },
        caseDeadline))
        << "leader did not publish two valid stable followers from the committed configuration";
    EXPECT_GT(leaderStatus.term, 0);
    EXPECT_GT(leaderStatus.configurationIndex, 0);

    std::vector<std::string> expectedFollowers;
    expectedFollowers.reserve(kBootstrapNodeCount - 1);
    for (size_t i = 0; i < kBootstrapNodeCount; ++i) {
        if (i != leaderIndex) {
            expectedFollowers.emplace_back(addresses_[i]);
        }
    }
    std::sort(expectedFollowers.begin(), expectedFollowers.end());
    ASSERT_EQ(leaderStatus.stableFollowers.size(), expectedFollowers.size());
    for (size_t i = 0; i < expectedFollowers.size(); ++i) {
        EXPECT_EQ(leaderStatus.stableFollowers[i].peer, expectedFollowers[i]);
        EXPECT_TRUE(leaderStatus.stableFollowers[i].valid);
        EXPECT_EQ(leaderStatus.stableFollowers[i].consecutiveErrorTimes, 0);
    }

    const size_t followerIndex = leaderIndex == 0 ? 1 : 0;
    ASSERT_LT(followerIndex, kBootstrapNodeCount);
    coordinator::CoordinatorRaftMembershipStatus followerStatus;
    ASSERT_TRUE(WaitUntil(
        [this, followerIndex, &expectedPeers, &followerStatus] {
            const auto status = nodes_[followerIndex]->GetMembershipStatus(followerStatus);
            return status.IsOk() && !followerStatus.isLeader && followerStatus.committedPeers == expectedPeers
                   && followerStatus.configurationIndex > 0 && followerStatus.stableFollowers.empty();
        },
        caseDeadline))
        << "follower did not expose committed membership with an empty Leader-only follower map";
    EXPECT_GT(followerStatus.term, 0);
    EXPECT_GT(followerStatus.configurationIndex, 0);
}

TEST_F(CoordinatorRaftNodeTest, MembershipStatusIsNotReadyBeforeStart)
{
    ASSERT_NO_FATAL_FAILURE(PrepareOneNode());
    coordinator::CoordinatorRaftMembershipStatus status;
    EXPECT_EQ(node_->GetMembershipStatus(status).GetCode(), K_NOT_READY);
}

TEST_F(CoordinatorRaftNodeTest, BootstrapOneNodePublishesLeaderAndCommittedConfiguration)
{
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::ABSENT));
    const auto caseDeadline = std::chrono::steady_clock::now() + kBootstrapCaseBudget;

    ASSERT_TRUE(WaitUntil([this] { return node_->IsLeader(); }, caseDeadline))
        << "one-node raft did not elect local leader " << localAddress_;

    std::string leaderAddress;
    const auto leaderStatus = node_->GetLeader(leaderAddress);
    ASSERT_TRUE(leaderStatus.IsOk()) << leaderStatus.ToString();
    EXPECT_EQ(leaderAddress, localAddress_);

    std::vector<std::string> peers;
    int64_t configurationIndex = 0;
    ASSERT_TRUE(
        WaitUntil(
            [this, &peers, &configurationIndex] {
                return node_->GetCommittedConfiguration(peers, configurationIndex).IsOk() && configurationIndex > 0;
            },
            caseDeadline))
        << "one-node raft did not publish a committed configuration before the case deadline";
    ASSERT_EQ(peers.size(), 1U);
    EXPECT_EQ(peers.front(), localAddress_);
    EXPECT_GT(configurationIndex, 0);
}

TEST_F(CoordinatorRaftNodeTest, ElectedLeaderForwardsLifecycleCallbacksWithItsTerm)
{
    std::atomic<uint64_t> startedTerm{ 0 };
    std::atomic<int> stopped{ 0 };
    coordinator::CoordinatorRaftEventCallbacks callbacks;
    callbacks.onLeaderStart = [&startedTerm](int64_t term) {
        startedTerm.store(static_cast<uint64_t>(term), std::memory_order_release);
    };
    callbacks.onLeaderStop = [&stopped](const Status &) { stopped.fetch_add(1, std::memory_order_relaxed); };
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::ABSENT, std::move(callbacks)));
    const auto caseDeadline = std::chrono::steady_clock::now() + kBootstrapCaseBudget;

    ASSERT_TRUE(WaitUntil([&startedTerm] { return startedTerm.load(std::memory_order_acquire) > 0; }, caseDeadline));
    EXPECT_TRUE(node_->IsLeader());
    EXPECT_GT(startedTerm.load(std::memory_order_acquire), 0UL);

    StopOneNode();
    EXPECT_GE(stopped.load(std::memory_order_relaxed), 1);
}

TEST_F(CoordinatorRaftNodeTest, SoleCommittedVoterRemovalFailsSynchronouslyAndPreservesRecovery)
{
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::ABSENT));
    auto caseDeadline = std::chrono::steady_clock::now() + kRecoveryCaseBudget;
    ASSERT_TRUE(WaitUntil([this] { return node_->IsLeader(); }, caseDeadline))
        << "one-node raft did not elect local leader " << localAddress_;

    std::vector<std::string> originalPeers;
    int64_t originalConfigurationIndex = 0;
    ASSERT_TRUE(WaitUntil(
        [this, &originalPeers, &originalConfigurationIndex] {
            return node_->GetCommittedConfiguration(originalPeers, originalConfigurationIndex).IsOk();
        },
        caseDeadline))
        << "one-node raft did not publish its committed configuration";
    ASSERT_EQ(originalPeers, std::vector<std::string>{ localAddress_ });

    auto callbackCount = std::make_shared<std::atomic<int>>(0);
    const auto removeStatus = node_->RemovePeer(
        localAddress_, [callbackCount](const Status &) { callbackCount->fetch_add(1, std::memory_order_relaxed); });
    EXPECT_EQ(removeStatus.GetCode(), K_INVALID) << removeStatus.ToString();
    EXPECT_NE(removeStatus.GetMsg().find("sole committed voter"), std::string::npos);
    EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 0);
    EXPECT_TRUE(node_->IsLeader());

    std::vector<std::string> unchangedPeers;
    int64_t unchangedConfigurationIndex = 0;
    ASSERT_TRUE(node_->GetCommittedConfiguration(unchangedPeers, unchangedConfigurationIndex).IsOk());
    EXPECT_EQ(unchangedPeers, originalPeers);
    EXPECT_EQ(unchangedConfigurationIndex, originalConfigurationIndex);

    StopOneNode();
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::VALID, {},
                                         coordinator::RaftStartPlan{ coordinator::RecoverPlan{} }));
    caseDeadline = std::chrono::steady_clock::now() + kRecoveryCaseBudget;
    ASSERT_TRUE(WaitUntil([this] { return node_->IsLeader(); }, caseDeadline))
        << "recovered node did not re-elect local leader " << localAddress_;

    std::vector<std::string> recoveredPeers;
    int64_t recoveredConfigurationIndex = 0;
    ASSERT_TRUE(WaitUntil(
        [this, &recoveredPeers, &recoveredConfigurationIndex] {
            return node_->GetCommittedConfiguration(recoveredPeers, recoveredConfigurationIndex).IsOk();
        },
        caseDeadline))
        << "recovered node did not publish its persisted committed configuration";
    EXPECT_EQ(recoveredPeers, originalPeers);
    EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 0);
}

TEST_F(CoordinatorRaftNodeTest, RecoverUsesPersistedConfigurationWithoutBootstrapPeers)
{
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::ABSENT));
    auto recoveryCaseDeadline = std::chrono::steady_clock::now() + kRecoveryCaseBudget;
    ASSERT_TRUE(WaitUntil([this] { return node_->IsLeader(); }, recoveryCaseDeadline))
        << "bootstrap node did not elect local leader " << localAddress_;

    std::vector<std::string> bootstrapPeers;
    int64_t bootstrapConfigurationIndex = 0;
    ASSERT_TRUE(WaitUntil(
        [this, &bootstrapPeers, &bootstrapConfigurationIndex] {
            return node_->GetCommittedConfiguration(bootstrapPeers, bootstrapConfigurationIndex).IsOk()
                   && bootstrapConfigurationIndex > 0;
        },
        recoveryCaseDeadline))
        << "bootstrap node did not publish a committed configuration";
    EXPECT_EQ(bootstrapPeers, std::vector<std::string>{ localAddress_ });
    EXPECT_GT(bootstrapConfigurationIndex, 0);

    StopOneNode();
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::VALID, {},
                                         coordinator::RaftStartPlan{ coordinator::RecoverPlan{} }));
    recoveryCaseDeadline = std::chrono::steady_clock::now() + kRecoveryCaseBudget;
    ASSERT_TRUE(WaitUntil([this] { return node_->IsLeader(); }, recoveryCaseDeadline))
        << "recovered node did not re-elect local leader " << localAddress_;

    std::vector<std::string> recoveredPeers;
    int64_t recoveredConfigurationIndex = 0;
    ASSERT_TRUE(WaitUntil(
        [this, &recoveredPeers, &recoveredConfigurationIndex] {
            return node_->GetCommittedConfiguration(recoveredPeers, recoveredConfigurationIndex).IsOk();
        },
        recoveryCaseDeadline))
        << "recovered node did not publish its persisted committed configuration";
    EXPECT_EQ(recoveredPeers, std::vector<std::string>{ localAddress_ });
    EXPECT_GT(recoveredConfigurationIndex, 0);
}

TEST_F(CoordinatorRaftNodeTest, WaitingToJoinDoesNotSelfElect)
{
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::ABSENT, {},
                                         coordinator::RaftStartPlan{ coordinator::WaitingToJoinPlan{} }));

    const auto noLeaderObservationDeadline = std::chrono::steady_clock::now() + kWaitingToJoinObservationWindow;
    EXPECT_FALSE(WaitUntil([this] { return node_->IsLeader(); }, noLeaderObservationDeadline));

    std::string leaderAddress;
    EXPECT_EQ(node_->GetLeader(leaderAddress).GetCode(), K_NOT_READY);
    std::vector<std::string> peers;
    int64_t configurationIndex = 0;
    EXPECT_EQ(node_->GetCommittedConfiguration(peers, configurationIndex).GetCode(), K_NOT_READY);
    StopOneNode();
}

TEST_F(CoordinatorRaftNodeTest, DestructionWaitsForSharedDrainCompletion)
{
    auto callbackState = std::make_shared<BlockingShutdownCallbackState>();
    auto callbackEntered = callbackState->GetEnteredFuture();
    coordinator::CoordinatorRaftEventCallbacks callbacks;
    callbacks.onShutdown = [callbackState] { callbackState->EnterAndWait(); };
    ASSERT_NO_FATAL_FAILURE(StartOneNode(coordinator::RaftMetadataState::ABSENT, std::move(callbacks)));

    const auto caseDeadline = std::chrono::steady_clock::now() + kCoordinationDeadline;
    callbackState->SetDeadline(caseDeadline);
    auto destruction = std::async(std::launch::async, [node = std::move(node_)]() mutable { node.reset(); });
    // Assertion unwinding must unblock the callback before std::future performs its implicit join.
    ScopedShutdownCallbackRelease releaseOnExit(callbackState);
    EXPECT_EQ(callbackEntered.wait_until(caseDeadline), std::future_status::ready);
    EXPECT_EQ(destruction.wait_for(kNegativeObservationDeadline), std::future_status::timeout);

    callbackState->Release();
    const auto destructionWaitStatus = destruction.wait_until(caseDeadline);
    EXPECT_EQ(destructionWaitStatus, std::future_status::ready);
    if (destructionWaitStatus == std::future_status::ready) {
        EXPECT_NO_THROW(destruction.get());
    }
    EXPECT_TRUE(callbackState->ReleaseWasObserved());
}

TEST_F(CoordinatorRaftNodeTest, FailedStartLeavesNodeStoppedAndDestructible)
{
    ASSERT_NO_FATAL_FAILURE(PrepareOneNode());
    const auto conflictingLogPath = dataDir_ + "/log";
    {
        std::ofstream conflictingLog(conflictingLogPath);
        ASSERT_TRUE(conflictingLog.is_open());
        conflictingLog << "conflicting regular file";
        ASSERT_TRUE(conflictingLog.good());
    }
    ASSERT_TRUE(std::filesystem::is_regular_file(conflictingLogPath));

    const auto startStatus = node_->Start(coordinator::RaftMetadataState::ABSENT);
    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR);
    EXPECT_FALSE(node_->IsLeader());
    std::string leaderAddress;
    EXPECT_EQ(node_->GetLeader(leaderAddress).GetCode(), K_NOT_READY);
    std::vector<std::string> peers;
    int64_t index = 0;
    EXPECT_EQ(node_->GetCommittedConfiguration(peers, index).GetCode(), K_NOT_READY);
    EXPECT_EQ(node_->Start(coordinator::RaftMetadataState::ABSENT).GetCode(), K_INVALID);

    node_.reset();
}
}  // namespace
}  // namespace st
}  // namespace datasystem
