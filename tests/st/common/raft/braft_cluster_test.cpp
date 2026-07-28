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

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include <braft/raft.h>
#include <brpc/server.h>
#include <butil/at_exit.h>
#include <butil/endpoint.h>
#include <butil/iobuf.h>
#include <gtest/gtest.h>

#include "cluster/test_port_allocator.h"
#include "common_test.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"

namespace datasystem {
namespace st {
namespace {
constexpr size_t kClusterNodeCount = 3;
constexpr int kElectionTimeoutMs = 300;
constexpr int kLeaderPollIntervalMs = 20;
constexpr std::chrono::seconds kClusterCaseBudget{ 6 };
constexpr std::chrono::seconds kReplayCaseBudget{ 6 };
constexpr std::chrono::seconds kDefaultTestBudget{ 8 };
constexpr std::chrono::milliseconds kRecoveredNodeObservationWindow{ 500 };
constexpr char kLoopbackIp[] = "127.0.0.1";
constexpr char kReplayRaftGroupId[] = "datasystem_braft_replay_test";
const std::string kRaftGroupId = "datasystem_braft_election_test";
const std::string kUnknownUserLog = "unknown-user-log";
const std::string kUnsupportedManagementLogMessage = "Coordinator raft management log apply is not supported yet";

static_assert(kRecoveredNodeObservationWindow < kReplayCaseBudget);
static_assert(kClusterCaseBudget < kDefaultTestBudget);
static_assert(kReplayCaseBudget < kDefaultTestBudget);

struct ApplyCompletion {
    std::promise<butil::Status> promise;
};

class SelfDeletingApplyClosure : public braft::Closure {
public:
    explicit SelfDeletingApplyClosure(std::shared_ptr<ApplyCompletion> completion) : completion_(std::move(completion))
    {
    }

    ~SelfDeletingApplyClosure() override = default;

    butil::IOBuf &Data()
    {
        return data_;
    }

    void Run() override
    {
        completion_->promise.set_value(status());
        delete this;
    }

private:
    std::shared_ptr<ApplyCompletion> completion_;
    butil::IOBuf data_;
};

struct ReplayErrorCompletion {
    std::once_flag once;
    std::promise<Status> promise;

    void Complete(Status status)
    {
        std::call_once(once, [this, status = std::move(status)]() mutable { promise.set_value(std::move(status)); });
    }
};

class BraftReplayTest : public CommonTest {
public:
    BraftReplayTest() : CommonTest(std::to_string(getpid()))
    {
    }

protected:
    void SetUp() override
    {
        CommonTest::SetUp();

        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName =
            testInfo == nullptr ? "unknown" : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        rootDir_ = testCasePath_ + "/braft-replay";
        dataDir_ = rootDir_ + "/node";

        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(dataDir_, error)) << error.message();

        auto &allocator = TestPortAllocator::Instance();
        allocator.SetOwnerInfo("braft_cluster_test", testName, rootDir_);
        const auto reserveStatus = allocator.Reserve("braft_replay_node", portLease_);
        ASSERT_TRUE(reserveStatus.IsOk()) << reserveStatus.ToString();

        ASSERT_EQ(butil::str2endpoint(kLoopbackIp, portLease_.Port(), &endpoint_), 0);
        peer_ = braft::PeerId(endpoint_);
    }

    void TearDown() override
    {
        StopNodeGeneration();
        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
        TestPortAllocator::Instance().ReleaseAll();
        CommonTest::TearDown();
    }

    void StartNodeGeneration(const braft::Configuration &initialConfiguration,
                             coordinator::CoordinatorRaftEventCallbacks callbacks)
    {
        ASSERT_EQ(node_, nullptr);
        ASSERT_EQ(stateMachine_, nullptr);
        ASSERT_EQ(server_, nullptr);

        server_ = std::make_unique<brpc::Server>();
        ASSERT_EQ(braft::add_service(server_.get(), endpoint_), 0);
        ASSERT_EQ(server_->Start(endpoint_, nullptr), 0);

        stateMachine_ = std::make_unique<coordinator::CoordinatorRaftStateMachine>(std::move(callbacks));
        braft::NodeOptions options;
        options.election_timeout_ms = kElectionTimeoutMs;
        options.initial_conf = initialConfiguration;
        options.fsm = stateMachine_.get();
        options.node_owns_fsm = false;
        options.log_uri = "local://" + dataDir_ + "/log";
        options.raft_meta_uri = "local://" + dataDir_ + "/raft_meta";
        options.snapshot_uri = "local://" + dataDir_ + "/snapshot";
        options.snapshot_interval_s = 0;
        options.disable_cli = true;

        auto node = std::make_unique<braft::Node>(kReplayRaftGroupId, peer_);
        ASSERT_EQ(node->init(options), 0);
        node_ = std::move(node);
    }

    void StopNodeGeneration()
    {
        if (node_ != nullptr) {
            node_->shutdown(nullptr);
            node_->join();
            node_.reset();
        }
        stateMachine_.reset();
        if (server_ != nullptr) {
            server_->Stop(0);
            server_->Join();
            server_.reset();
        }
    }

    bool WaitForLeader(std::chrono::steady_clock::time_point deadline) const
    {
        while (std::chrono::steady_clock::now() < deadline) {
            if (node_ != nullptr && node_->is_leader()) {
                return true;
            }
            const auto nextPoll = std::chrono::steady_clock::now() + std::chrono::milliseconds(kLeaderPollIntervalMs);
            std::this_thread::sleep_until(nextPoll < deadline ? nextPoll : deadline);
        }
        return node_ != nullptr && node_->is_leader();
    }

    bool WaitForNodeError(std::chrono::steady_clock::time_point deadline) const
    {
        while (std::chrono::steady_clock::now() < deadline) {
            braft::NodeStatus status;
            node_->get_status(&status);
            if (status.state == braft::STATE_ERROR) {
                return true;
            }
            const auto nextPoll = std::chrono::steady_clock::now() + std::chrono::milliseconds(kLeaderPollIntervalMs);
            std::this_thread::sleep_until(nextPoll < deadline ? nextPoll : deadline);
        }
        braft::NodeStatus status;
        node_->get_status(&status);
        return status.state == braft::STATE_ERROR;
    }

    bool RemainsNotLeaderUntil(std::chrono::steady_clock::time_point deadline) const
    {
        while (std::chrono::steady_clock::now() < deadline) {
            if (node_->is_leader()) {
                return false;
            }
            const auto nextPoll = std::chrono::steady_clock::now() + std::chrono::milliseconds(kLeaderPollIntervalMs);
            std::this_thread::sleep_until(nextPoll < deadline ? nextPoll : deadline);
        }
        return !node_->is_leader();
    }

    butil::AtExitManager atExitManager_;
    TestPortLease portLease_;
    std::string rootDir_;
    std::string dataDir_;
    butil::EndPoint endpoint_;
    braft::PeerId peer_;
    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<coordinator::CoordinatorRaftStateMachine> stateMachine_;
    std::unique_ptr<braft::Node> node_;
};

TEST_F(BraftReplayTest, UnsupportedUserLogReplayFailsClosedAfterRestart)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kReplayCaseBudget;
    braft::Configuration bootstrapConfiguration;
    bootstrapConfiguration.add_peer(peer_);
    ASSERT_NO_FATAL_FAILURE(StartNodeGeneration(bootstrapConfiguration, {}));
    ASSERT_TRUE(WaitForLeader(caseDeadline));

    auto applyCompletion = std::make_shared<ApplyCompletion>();
    auto applyFuture = applyCompletion->promise.get_future();
    auto *done = new SelfDeletingApplyClosure(std::move(applyCompletion));
    done->Data().append(kUnknownUserLog);

    braft::Task task;
    task.data = &done->Data();
    task.done = done;
    node_->apply(task);

    ASSERT_EQ(applyFuture.wait_until(caseDeadline), std::future_status::ready);
    const auto applyStatus = applyFuture.get();
    ASSERT_EQ(applyStatus.error_code(), braft::ESTATEMACHINE) << applyStatus.error_str();
    ASSERT_NE(std::string(applyStatus.error_str()).find(kUnsupportedManagementLogMessage), std::string::npos)
        << applyStatus.error_str();

    StopNodeGeneration();

    auto replayError = std::make_shared<ReplayErrorCompletion>();
    auto replayErrorFuture = replayError->promise.get_future();
    coordinator::CoordinatorRaftEventCallbacks callbacks;
    callbacks.onError = [replayError](Status status) { replayError->Complete(std::move(status)); };
    const braft::Configuration recoveryConfiguration;
    ASSERT_TRUE(recoveryConfiguration.empty());
    ASSERT_NO_FATAL_FAILURE(StartNodeGeneration(recoveryConfiguration, std::move(callbacks)));

    ASSERT_EQ(replayErrorFuture.wait_until(caseDeadline), std::future_status::ready);
    const auto replayStatus = replayErrorFuture.get();
    EXPECT_EQ(replayStatus.GetCode(), K_RUNTIME_ERROR);
    EXPECT_NE(replayStatus.GetMsg().find(kUnsupportedManagementLogMessage), std::string::npos)
        << replayStatus.ToString();
    ASSERT_TRUE(WaitForNodeError(caseDeadline));

    const auto observationDeadline = std::chrono::steady_clock::now() + kRecoveredNodeObservationWindow;
    ASSERT_LT(observationDeadline, caseDeadline) << "Replay left no time for the recovered-node observation window";
    EXPECT_TRUE(RemainsNotLeaderUntil(observationDeadline));
    braft::NodeStatus recoveredStatus;
    node_->get_status(&recoveredStatus);
    EXPECT_EQ(recoveredStatus.state, braft::STATE_ERROR);
    EXPECT_FALSE(node_->is_leader());
}

class BraftClusterTest : public CommonTest {
public:
    BraftClusterTest() : CommonTest(std::to_string(getpid()))
    {
    }

protected:
    void SetUp() override
    {
        CommonTest::SetUp();
        auto &allocator = TestPortAllocator::Instance();
        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        allocator.SetOwnerInfo("braft_cluster_test", testInfo == nullptr ? "unknown" : testInfo->name(), "");

        std::vector<std::string> roles;
        roles.reserve(kClusterNodeCount);
        for (size_t i = 0; i < kClusterNodeCount; ++i) {
            roles.emplace_back("braft_node_" + std::to_string(i));
        }
        auto status = allocator.ReserveBatch(roles, portLeases_);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        ASSERT_EQ(portLeases_.size(), kClusterNodeCount);

        rootDir_ = testCasePath_ + "/braft";
        allocator.SetOwnerInfo("braft_cluster_test", testInfo == nullptr ? "unknown" : testInfo->name(), rootDir_);
        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        error.clear();
        ASSERT_TRUE(std::filesystem::create_directories(rootDir_, error)) << error.message();

        for (size_t i = 0; i < kClusterNodeCount; ++i) {
            butil::EndPoint endpoint;
            ASSERT_EQ(butil::str2endpoint("127.0.0.1", portLeases_[i].Port(), &endpoint), 0);
            peers_[i] = braft::PeerId(endpoint);
            configuration_.add_peer(peers_[i]);

            servers_[i] = std::make_unique<brpc::Server>();
            ASSERT_EQ(braft::add_service(servers_[i].get(), endpoint), 0);
            ASSERT_EQ(servers_[i]->Start(endpoint, nullptr), 0);
        }

        for (size_t i = 0; i < kClusterNodeCount; ++i) {
            const auto nodeDir = rootDir_ + "/node-" + std::to_string(i);
            error.clear();
            ASSERT_TRUE(std::filesystem::create_directories(nodeDir, error)) << error.message();

            stateMachines_[i] = std::make_unique<coordinator::CoordinatorRaftStateMachine>(
                coordinator::CoordinatorRaftEventCallbacks{});
            braft::NodeOptions options;
            options.election_timeout_ms = kElectionTimeoutMs;
            options.initial_conf = configuration_;
            options.fsm = stateMachines_[i].get();
            options.node_owns_fsm = false;
            options.log_uri = "local://" + nodeDir + "/log";
            options.raft_meta_uri = "local://" + nodeDir + "/raft_meta";
            options.snapshot_uri = "local://" + nodeDir + "/snapshot";
            options.disable_cli = true;

            nodes_[i] = std::make_unique<braft::Node>(kRaftGroupId, peers_[i]);
            ASSERT_EQ(nodes_[i]->init(options), 0);
        }
    }

    void TearDown() override
    {
        for (auto &node : nodes_) {
            if (node != nullptr) {
                node->shutdown(nullptr);
            }
        }
        for (auto &node : nodes_) {
            if (node != nullptr) {
                node->join();
            }
        }
        for (auto &server : servers_) {
            if (server != nullptr) {
                server->Stop(0);
            }
        }
        for (auto &server : servers_) {
            if (server != nullptr) {
                server->Join();
            }
        }

        for (auto &node : nodes_) {
            node.reset();
        }
        for (auto &stateMachine : stateMachines_) {
            stateMachine.reset();
        }
        for (auto &server : servers_) {
            server.reset();
        }
        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
        TestPortAllocator::Instance().ReleaseAll();
        CommonTest::TearDown();
    }

    bool WaitForLeader(std::chrono::steady_clock::time_point deadline)
    {
        while (std::chrono::steady_clock::now() < deadline) {
            size_t leaderCount = 0;
            braft::PeerId electedLeader;
            for (const auto &node : nodes_) {
                if (node->is_leader()) {
                    ++leaderCount;
                    electedLeader = node->node_id().peer_id;
                }
            }

            bool converged = leaderCount == 1 && !electedLeader.is_empty();
            for (const auto &node : nodes_) {
                converged = converged && node->leader_id() == electedLeader;
            }
            if (converged) {
                return true;
            }
            const auto nextPoll = std::chrono::steady_clock::now() + std::chrono::milliseconds(kLeaderPollIntervalMs);
            std::this_thread::sleep_until(nextPoll < deadline ? nextPoll : deadline);
        }
        return false;
    }

    std::string ElectionState() const
    {
        std::ostringstream output;
        for (size_t i = 0; i < kClusterNodeCount; ++i) {
            output << "node[" << i << "] peer=" << peers_[i] << " is_leader=" << nodes_[i]->is_leader()
                   << " observed_leader=" << nodes_[i]->leader_id() << ';';
        }
        return output.str();
    }

protected:
    butil::AtExitManager atExitManager_;
    std::vector<TestPortLease> portLeases_;
    std::string rootDir_;
    braft::Configuration configuration_;
    std::array<braft::PeerId, kClusterNodeCount> peers_;
    std::array<std::unique_ptr<brpc::Server>, kClusterNodeCount> servers_;
    std::array<std::unique_ptr<coordinator::CoordinatorRaftStateMachine>, kClusterNodeCount> stateMachines_;
    std::array<std::unique_ptr<braft::Node>, kClusterNodeCount> nodes_;
};

TEST_F(BraftClusterTest, ThreeNodesElectOneLeader)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kClusterCaseBudget;
    ASSERT_TRUE(WaitForLeader(caseDeadline))
        << "cluster did not converge on one leader before the case deadline: " << ElectionState();

    size_t leaderCount = 0;
    braft::PeerId electedLeader;
    for (const auto &node : nodes_) {
        if (node->is_leader()) {
            ++leaderCount;
            electedLeader = node->node_id().peer_id;
        }
    }
    ASSERT_EQ(leaderCount, 1U) << ElectionState();
    ASSERT_TRUE(configuration_.contains(electedLeader));
    for (const auto &node : nodes_) {
        EXPECT_EQ(node->leader_id(), electedLeader) << ElectionState();
    }
}

TEST_F(BraftClusterTest, UnknownUserLogFailsClosed)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kClusterCaseBudget;
    ASSERT_TRUE(WaitForLeader(caseDeadline))
        << "cluster did not converge on one leader before the case deadline: " << ElectionState();

    braft::Node *leader = nullptr;
    for (const auto &node : nodes_) {
        if (node->is_leader()) {
            leader = node.get();
            break;
        }
    }
    ASSERT_NE(leader, nullptr) << ElectionState();

    auto completion = std::make_shared<ApplyCompletion>();
    auto completionFuture = completion->promise.get_future();
    auto *done = new SelfDeletingApplyClosure(std::move(completion));
    done->Data().append(kUnknownUserLog);

    braft::Task task;
    task.data = &done->Data();
    task.done = done;
    leader->apply(task);

    ASSERT_EQ(completionFuture.wait_until(caseDeadline), std::future_status::ready)
        << "unknown-log apply did not complete before the case deadline";
    const auto applyStatus = completionFuture.get();
    EXPECT_EQ(applyStatus.error_code(), braft::ESTATEMACHINE) << applyStatus.error_str();
    EXPECT_NE(std::string(applyStatus.error_str()).find(kUnsupportedManagementLogMessage), std::string::npos)
        << applyStatus.error_str();
}
}  // namespace
}  // namespace st
}  // namespace datasystem
