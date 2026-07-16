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
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <braft/raft.h>
#include <brpc/server.h>
#include <butil/at_exit.h>
#include <butil/endpoint.h>
#include <gtest/gtest.h>

#include "cluster/test_port_allocator.h"
#include "common_test.h"

namespace datasystem {
namespace st {
namespace {
constexpr size_t kClusterNodeCount = 3;
constexpr int kElectionTimeoutMs = 300;
constexpr int kLeaderElectionDeadlineMs = 5'000;
constexpr int kLeaderPollIntervalMs = 20;
const std::string kRaftGroupId = "datasystem_braft_election_test";

class EmptyStateMachine : public braft::StateMachine {
public:
    ~EmptyStateMachine() override = default;

    void on_apply(braft::Iterator &iterator) override
    {
        for (; iterator.valid(); iterator.next()) {
            if (iterator.done() != nullptr) {
                iterator.done()->Run();
            }
        }
    }
};

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

            stateMachines_[i] = std::make_unique<EmptyStateMachine>();
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

    bool WaitForLeader()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kLeaderElectionDeadlineMs);
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
            std::this_thread::sleep_for(std::chrono::milliseconds(kLeaderPollIntervalMs));
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
    std::array<std::unique_ptr<EmptyStateMachine>, kClusterNodeCount> stateMachines_;
    std::array<std::unique_ptr<braft::Node>, kClusterNodeCount> nodes_;
};

TEST_F(BraftClusterTest, ThreeNodesElectOneLeader)
{
    ASSERT_TRUE(WaitForLeader()) << ElectionState();

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
}  // namespace
}  // namespace st
}  // namespace datasystem
