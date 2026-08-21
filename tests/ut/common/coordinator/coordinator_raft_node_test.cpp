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
 * Description: Unit tests for coordinator raft node lifecycle and shared service registration.
 */

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <brpc/server.h>
#include <braft/raft.h>
#include <butil/at_exit.h>

#include "cluster/test_port_allocator.h"
#include "datasystem/common/rpc/rpc_server.h"
#include "datasystem/coordinator/raft/coordinator_raft_operation.h"
#include "datasystem/coordinator/raft/coordinator_raft_service.h"
#include "datasystem/coordinator/raft/coordinator_raft_state_machine.h"
#include "datasystem/coordinator/raft/coordinator_raft_types.h"
#include "datasystem/utils/status.h"
#define private public
#include "datasystem/coordinator/raft/coordinator_raft_node.h"
#undef private
#include "ut/common.h"

namespace datasystem::coordinator {
namespace {

constexpr char kLocalPeer[] = "127.0.0.1:18480";
constexpr char kDataDir[] = "coordinator-raft-node-test-data";
constexpr int kHeartbeatIntervalMs = 100;
constexpr int kElectionTimeoutMs = 1'000;

constexpr int64_t kValidConfigurationIndex = 1;
constexpr std::chrono::seconds kCoordinationDeadline{ 2 };
constexpr char kInternalLocalPeer[] = "127.0.0.1:18480:0";
constexpr char kUnreachablePeer[] = "127.0.0.1:18481";
constexpr char kNonzeroIndexPeer[] = "127.0.0.1:18480:1";
constexpr char kMalformedPeerPayload[] = "invalid-peer-private-payload";
constexpr char kSensitiveExceptionText[] = "injected onError exception detail";
constexpr char kOperationCallbackExceptionText[] = "injected raft operation callback detail";
constexpr char kCallbackFailureMarker[] = "Coordinator raft callback failure";

enum class NonStandardOnErrorException { SENTINEL };
enum class NonStandardRaftOperationException : uint8_t { SENTINEL };
enum class OnErrorThrowKind { NON_STANDARD, RUNTIME_ERROR };
enum class RaftOperationThrowKind : uint8_t { NON_STANDARD, RUNTIME_ERROR };

struct ConfigurationErrorState {
    std::atomic<int> configurationCallbackCount{ 0 };
    std::atomic<int> errorCount{ 0 };
    Status reportedStatus;
};

struct CommittedConfigurationRecord {
    std::vector<std::string> peers;
    int64_t index;
};

struct NonBootstrapCallbackState {
    std::atomic<int> configurationCallbackCount{ 0 };
    std::atomic<int> errorCount{ 0 };
    std::mutex mutex;
    std::vector<CommittedConfigurationRecord> records;
};

struct RaftOperationGateCallbackState {
    std::atomic<int> callbackCount{ 0 };
    std::once_flag completionOnce;
    std::promise<Status> completion;
};

struct InvalidConfigurationCase {
    const char *name;
    std::vector<std::string> peers;
    int64_t index;
    OnErrorThrowKind throwKind;
};

size_t CountOccurrences(const std::string &text, const std::string &needle)
{
    size_t count = 0;
    size_t position = 0;
    while ((position = text.find(needle, position)) != std::string::npos) {
        ++count;
        position += needle.size();
    }
    return count;
}

CoordinatorRaftOptions MakeOneNodeOptions()
{
    return CoordinatorRaftOptions{ kLocalPeer, kDataDir, kHeartbeatIntervalMs, kElectionTimeoutMs,
                                   RaftStartPlan{ BootstrapPlan{ { kLocalPeer } } } };
}

detail::RaftOperationCallback MakeGateCallback(const std::shared_ptr<RaftOperationGateCallbackState> &state)
{
    return [state](Status status) {
        state->callbackCount.fetch_add(1, std::memory_order_relaxed);
        std::call_once(state->completionOnce,
                       [state, status = std::move(status)]() mutable {
                           state->completion.set_value(std::move(status));
                       });
    };
}

class CoordinatorRaftNodeStartTest : public datasystem::ut::CommonTest {
protected:
    void SetUp() override
    {
        datasystem::ut::CommonTest::SetUp();
        rootDir_ = datasystem::ut::GetTestCaseDataDir() + "/coordinator-raft-node";
        std::error_code error;
        ASSERT_TRUE(std::filesystem::create_directories(rootDir_, error)) << error.message();

        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName =
            testInfo == nullptr ? "unknown" : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        auto &allocator = datasystem::st::TestPortAllocator::Instance();
        allocator.SetOwnerInfo("ds_ut", testName, rootDir_);
        const auto reserveStatus = allocator.Reserve("coordinator_raft_node", portLease_);
        ASSERT_TRUE(reserveStatus.IsOk()) << reserveStatus.ToString();
        localPeer_ = "127.0.0.1:" + std::to_string(portLease_.Port());

        auto status = RpcServer::Builder().SetUseBrpc(true).Init(rpcServer_);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        status = RegisterCoordinatorRaftServices(*rpcServer_, localPeer_);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        status = rpcServer_->StartBrpcServer("127.0.0.1", portLease_.Port());
        ASSERT_TRUE(status.IsOk()) << status.ToString();
    }

    void TearDown() override
    {
        node_.reset();
        if (rpcServer_ != nullptr) {
            rpcServer_->Shutdown();
            rpcServer_.reset();
        }
        datasystem::st::TestPortAllocator::Instance().ReleaseAll();
        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        EXPECT_FALSE(error) << error.message();
        datasystem::ut::CommonTest::TearDown();
    }

    CoordinatorRaftOptions MakeOptions(RaftStartPlan startPlan, const std::string &dataDir) const
    {
        return CoordinatorRaftOptions{ localPeer_, dataDir, kHeartbeatIntervalMs, kElectionTimeoutMs,
                                       std::move(startPlan) };
    }

    butil::AtExitManager atExitManager_;
    datasystem::st::TestPortLease portLease_;
    std::string rootDir_;
    std::string localPeer_;
    std::unique_ptr<RpcServer> rpcServer_;
    std::unique_ptr<CoordinatorRaftNode> node_;
};

CoordinatorRaftEventCallbacks MakeNonBootstrapCallbacks(const std::shared_ptr<NonBootstrapCallbackState> &state)
{
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onConfigurationCommitted = [state](std::vector<std::string> peers, int64_t index) {
        state->configurationCallbackCount.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(state->mutex);
        state->records.emplace_back(CommittedConfigurationRecord{ std::move(peers), index });
    };
    callbacks.onError = [state](Status) { state->errorCount.fetch_add(1, std::memory_order_relaxed); };
    return callbacks;
}

}  // namespace

TEST(RaftOperationSubmissionGateTest, DefersResultUntilSubmissionCompletes)
{
    auto gate = std::make_shared<detail::RaftOperationSubmissionGate>();
    auto callbackState = std::make_shared<RaftOperationGateCallbackState>();
    auto completion = callbackState->completion.get_future();

    gate->DispatchOrDefer(MakeGateCallback(callbackState), Status(K_RUNTIME_ERROR, "deferred result"));
    EXPECT_EQ(callbackState->callbackCount.load(std::memory_order_relaxed), 0);

    gate->MarkSubmissionComplete();
    const auto deadline = std::chrono::steady_clock::now() + kCoordinationDeadline;
    ASSERT_EQ(completion.wait_until(deadline), std::future_status::ready);
    const auto result = completion.get();
    EXPECT_EQ(result.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(result.GetMsg(), "deferred result");
    EXPECT_EQ(callbackState->callbackCount.load(std::memory_order_relaxed), 1);

    gate->DispatchOrDefer(MakeGateCallback(callbackState), Status::OK());
    EXPECT_EQ(callbackState->callbackCount.load(std::memory_order_relaxed), 1);
}

TEST(RaftOperationSubmissionGateTest, DispatchesImmediatelyAfterSubmissionCompletes)
{
    auto gate = std::make_shared<detail::RaftOperationSubmissionGate>();
    auto callbackState = std::make_shared<RaftOperationGateCallbackState>();
    auto completion = callbackState->completion.get_future();

    gate->MarkSubmissionComplete();
    gate->DispatchOrDefer(MakeGateCallback(callbackState), Status::OK());
    const auto deadline = std::chrono::steady_clock::now() + kCoordinationDeadline;
    ASSERT_EQ(completion.wait_until(deadline), std::future_status::ready);
    EXPECT_EQ(completion.get().GetCode(), K_OK);
    EXPECT_EQ(callbackState->callbackCount.load(std::memory_order_relaxed), 1);

    gate->DispatchOrDefer(MakeGateCallback(callbackState), Status(K_RUNTIME_ERROR, "duplicate result"));
    EXPECT_EQ(callbackState->callbackCount.load(std::memory_order_relaxed), 1);
}

TEST(RaftOperationSubmissionGateTest, InvokesReentrantCallbacksOutsideGateMutex)
{
    {
        auto gate = std::make_shared<detail::RaftOperationSubmissionGate>();
        auto callbackCount = std::make_shared<std::atomic<int>>(0);
        gate->DispatchOrDefer(
            [gate, callbackCount](const Status &) {
                callbackCount->fetch_add(1, std::memory_order_relaxed);
                gate->MarkSubmissionComplete();
            },
            Status::OK());

        auto completion = std::async(std::launch::async, [gate] { gate->MarkSubmissionComplete(); });
        const auto deadline = std::chrono::steady_clock::now() + kCoordinationDeadline;
        ASSERT_EQ(completion.wait_until(deadline), std::future_status::ready);
        EXPECT_NO_THROW(completion.get());
        EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 1);
    }

    {
        auto gate = std::make_shared<detail::RaftOperationSubmissionGate>();
        auto callbackCount = std::make_shared<std::atomic<int>>(0);
        gate->MarkSubmissionComplete();

        auto completion = std::async(std::launch::async, [gate, callbackCount] {
            gate->DispatchOrDefer(
                [gate, callbackCount](const Status &) {
                    callbackCount->fetch_add(1, std::memory_order_relaxed);
                    gate->DispatchOrDefer(
                        [callbackCount](const Status &) {
                            callbackCount->fetch_add(1, std::memory_order_relaxed);
                        },
                        Status::OK());
                },
                Status::OK());
        });
        const auto deadline = std::chrono::steady_clock::now() + kCoordinationDeadline;
        ASSERT_EQ(completion.wait_until(deadline), std::future_status::ready);
        EXPECT_NO_THROW(completion.get());
        EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 1);
    }
}

TEST(RaftOperationSubmissionGateTest, ContainsThrowingCallbacks)
{
    for (const auto throwKind : { RaftOperationThrowKind::RUNTIME_ERROR,
                                  RaftOperationThrowKind::NON_STANDARD }) {
        SCOPED_TRACE(throwKind == RaftOperationThrowKind::RUNTIME_ERROR ? "std::runtime_error"
                                                                        : "non-standard exception");
        auto gate = std::make_shared<detail::RaftOperationSubmissionGate>();
        auto callbackCount = std::make_shared<std::atomic<int>>(0);
        gate->MarkSubmissionComplete();
        bool exceptionEscaped = false;

        testing::internal::CaptureStderr();
        try {
            gate->DispatchOrDefer(
                [throwKind, callbackCount](const Status &) {
                    callbackCount->fetch_add(1, std::memory_order_relaxed);
                    if (throwKind == RaftOperationThrowKind::RUNTIME_ERROR) {
                        throw std::runtime_error(kOperationCallbackExceptionText);
                    }
                    throw NonStandardRaftOperationException::SENTINEL;
                },
                Status::OK());
        } catch (...) {
            exceptionEscaped = true;
        }
        const auto capturedStderr = testing::internal::GetCapturedStderr();

        EXPECT_FALSE(exceptionEscaped);
        EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 1);
        EXPECT_EQ(CountOccurrences(capturedStderr, kCallbackFailureMarker), 1U);
        if (throwKind == RaftOperationThrowKind::RUNTIME_ERROR) {
            EXPECT_NE(capturedStderr.find(kOperationCallbackExceptionText), std::string::npos);
        } else {
            EXPECT_EQ(capturedStderr.find(kOperationCallbackExceptionText), std::string::npos);
        }
    }
}

TEST(RaftOperationDrainStateTest, AdmitsOnlyOneInFlightOperation)
{
    auto state = std::make_shared<detail::RaftOperationDrainState>();
    EXPECT_FALSE(state->HasInFlight());

    auto first = std::make_unique<detail::RaftOperationDrainToken>(state);
    ASSERT_TRUE(first->IsAcquired());
    EXPECT_TRUE(state->HasInFlight());

    detail::RaftOperationDrainToken concurrent(state);
    EXPECT_FALSE(concurrent.IsAcquired());

    first.reset();
    EXPECT_FALSE(state->HasInFlight());

    detail::RaftOperationDrainToken next(state);
    EXPECT_TRUE(next.IsAcquired());
    EXPECT_TRUE(state->HasInFlight());
}

TEST(CoordinatorRaftNodeTest, StartFromConstructedValidatesOptionsWithoutRegistration)
{
    auto options = MakeOneNodeOptions();
    options.dataDir.clear();
    CoordinatorRaftNode node(std::move(options), {});

    EXPECT_EQ(node.Start(RaftMetadataState::ABSENT).GetCode(), K_INVALID);
    EXPECT_EQ(node.state_, CoordinatorRaftNode::LifecycleState::CONSTRUCTED);
}

TEST_F(CoordinatorRaftNodeStartTest, BootstrapDoesNotPublishInitialConfigurationBeforeRaftCommit)
{
    auto callbackCount = std::make_shared<std::atomic<int>>(0);
    CoordinatorRaftEventCallbacks callbacks;
    callbacks.onConfigurationCommitted = [callbackCount](std::vector<std::string>, int64_t) {
        callbackCount->fetch_add(1, std::memory_order_relaxed);
    };

    const auto dataDir = rootDir_ + "/bootstrap";
    node_ = std::make_unique<CoordinatorRaftNode>(
        MakeOptions(RaftStartPlan{ BootstrapPlan{ { localPeer_, kUnreachablePeer } } }, dataDir), std::move(callbacks));
    DS_ASSERT_OK(node_->Start(RaftMetadataState::ABSENT));

    std::vector<std::string> peers;
    int64_t index = -1;
    EXPECT_EQ(node_->GetCommittedConfiguration(peers, index).GetCode(), K_NOT_READY);
    EXPECT_EQ(callbackCount->load(std::memory_order_relaxed), 0);
}

TEST_F(CoordinatorRaftNodeStartTest, RecoverAndWaitingDoNotPublishEmptyInitialConfiguration)
{
    const auto persistedDataDir = rootDir_ + "/persisted";
    node_ = std::make_unique<CoordinatorRaftNode>(
        MakeOptions(RaftStartPlan{ BootstrapPlan{ { localPeer_ } } }, persistedDataDir),
        CoordinatorRaftEventCallbacks{});
    DS_ASSERT_OK(node_->Start(RaftMetadataState::ABSENT));
    node_.reset();

    auto recoverState = std::make_shared<NonBootstrapCallbackState>();
    node_ = std::make_unique<CoordinatorRaftNode>(
        MakeOptions(RaftStartPlan{ RecoverPlan{} }, persistedDataDir), MakeNonBootstrapCallbacks(recoverState));
    DS_ASSERT_OK(node_->Start(RaftMetadataState::VALID));
    node_.reset();
    EXPECT_EQ(recoverState->errorCount.load(std::memory_order_relaxed), 0);
    {
        std::lock_guard<std::mutex> lock(recoverState->mutex);
        EXPECT_TRUE(std::all_of(recoverState->records.begin(), recoverState->records.end(),
                                [](const auto &record) { return !record.peers.empty(); }));
    }

    auto waitingState = std::make_shared<NonBootstrapCallbackState>();
    node_ = std::make_unique<CoordinatorRaftNode>(
        MakeOptions(RaftStartPlan{ WaitingToJoinPlan{} }, rootDir_ + "/waiting"),
        MakeNonBootstrapCallbacks(waitingState));
    DS_ASSERT_OK(node_->Start(RaftMetadataState::ABSENT));
    std::vector<std::string> waitingPeers;
    int64_t waitingIndex = -1;
    EXPECT_EQ(node_->GetCommittedConfiguration(waitingPeers, waitingIndex).GetCode(), K_NOT_READY);
    node_.reset();
    EXPECT_EQ(waitingState->configurationCallbackCount.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(waitingState->errorCount.load(std::memory_order_relaxed), 0);
}

TEST(CoordinatorRaftServiceTest, RegistrationRequiresBrpcRpcServer)
{
    std::unique_ptr<RpcServer> server;
    DS_ASSERT_OK(RpcServer::Builder().Init(server));

    EXPECT_EQ(RegisterCoordinatorRaftServices(*server, kLocalPeer).GetCode(), K_INVALID);
}


TEST(CoordinatorRaftNodeTest, InvalidCommittedConfigurationLogsStandardOnErrorExceptionDetails)
{
    const std::array<InvalidConfigurationCase, 4> cases{
        InvalidConfigurationCase{ "negative index", { kInternalLocalPeer }, -1,
                                  OnErrorThrowKind::NON_STANDARD },
        InvalidConfigurationCase{ "malformed peer", { kMalformedPeerPayload }, kValidConfigurationIndex,
                                  OnErrorThrowKind::RUNTIME_ERROR },
        InvalidConfigurationCase{ "nonzero braft index", { kNonzeroIndexPeer }, kValidConfigurationIndex,
                                  OnErrorThrowKind::NON_STANDARD },
        InvalidConfigurationCase{ "duplicate normalized identity", { kInternalLocalPeer, kInternalLocalPeer },
                                  kValidConfigurationIndex, OnErrorThrowKind::RUNTIME_ERROR },
    };

    for (const auto &testCase : cases) {
        SCOPED_TRACE(testCase.name);
        auto state = std::make_shared<ConfigurationErrorState>();
        CoordinatorRaftEventCallbacks callbacks;
        callbacks.onConfigurationCommitted = [state](std::vector<std::string>, int64_t) {
            state->configurationCallbackCount.fetch_add(1);
        };
        callbacks.onError = [state, throwKind = testCase.throwKind](Status status) {
            state->reportedStatus = std::move(status);
            state->errorCount.fetch_add(1);
            if (throwKind == OnErrorThrowKind::NON_STANDARD) {
                throw NonStandardOnErrorException::SENTINEL;
            }
            throw std::runtime_error(kSensitiveExceptionText);
        };
        auto node = std::make_unique<CoordinatorRaftNode>(MakeOneNodeOptions(), std::move(callbacks));
        auto wrappedCallbacks = node->MakeStateMachineCallbacks();

        testing::internal::CaptureStderr();
        EXPECT_NO_THROW(wrappedCallbacks.onConfigurationCommitted(testCase.peers, testCase.index));
        const auto capturedStderr = testing::internal::GetCapturedStderr();

        EXPECT_EQ(state->configurationCallbackCount.load(), 0);
        EXPECT_EQ(state->errorCount.load(), 1);
        EXPECT_EQ(state->reportedStatus.GetCode(), K_DATA_INCONSISTENCY);
        EXPECT_EQ(state->reportedStatus.GetMsg().find(kSensitiveExceptionText), std::string::npos);
        EXPECT_EQ(state->reportedStatus.GetMsg().find(kMalformedPeerPayload), std::string::npos);
        EXPECT_EQ(CountOccurrences(capturedStderr, kCallbackFailureMarker), 1U);
        if (testCase.throwKind == OnErrorThrowKind::RUNTIME_ERROR) {
            EXPECT_NE(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
        } else {
            EXPECT_EQ(capturedStderr.find(kSensitiveExceptionText), std::string::npos);
        }
        EXPECT_EQ(capturedStderr.find(kMalformedPeerPayload), std::string::npos);

        node.reset();
    }
}

}  // namespace datasystem::coordinator
