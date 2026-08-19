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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <butil/at_exit.h>
#include <gtest/gtest.h>

#include "cluster/test_port_allocator.h"
#include "common_test.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/coordinator/coordinator_service_impl.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
#include "datasystem/utils/coordinator_discovery.h"

namespace datasystem::st {
namespace {
using Deadline = std::chrono::steady_clock::time_point;

constexpr char kLoopbackIp[] = "127.0.0.1";
constexpr int32_t kHeartbeatIntervalMs = 50;
constexpr int32_t kElectionTimeoutMs = 300;
constexpr uint32_t kHealthCheckIntervalMs = 50;
constexpr uint32_t kMemberFailureGraceMs = 500;
constexpr uint32_t kDiscoveryRetryIntervalMs = 100;
constexpr uint32_t kBootstrapWarningIntervalMs = 500;
constexpr int32_t kMinimumRpcTimeoutMs = 1;
constexpr int32_t kRpcTimeoutMs = 500;
constexpr std::chrono::milliseconds kPollInterval{ 20 };
constexpr std::chrono::seconds kCaseBudget{ 6 };
constexpr std::chrono::seconds kCtestTimeout{ 8 };
constexpr size_t kRaftLogIndexWidth = 20;
constexpr size_t kClosedRaftLogIndexCount = 2;
constexpr size_t kExpectedMemberCount = 1;
constexpr size_t kElectionExpectedMemberCount = 3;

constexpr char kRaftMetadataDirectory[] = "raft_meta";
constexpr char kRaftMetadataEntity[] = "raft_meta";
constexpr char kRaftLogDirectory[] = "log";
constexpr char kRaftLogMetadataEntity[] = "log_meta";
constexpr char kRaftLogSegmentPrefix[] = "log_";
constexpr char kRaftInProgressLogSegmentPrefix[] = "log_inprogress_";

static_assert(kElectionTimeoutMs % kHeartbeatIntervalMs == 0);
static_assert(kHealthCheckIntervalMs < kMemberFailureGraceMs);
static_assert(kHealthCheckIntervalMs <= kDiscoveryRetryIntervalMs);
static_assert(kCaseBudget < kCtestTimeout);

template <typename Predicate>
bool WaitUntil(Predicate &&predicate, Deadline deadline)
{
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        const auto nextPoll = std::min(deadline, std::chrono::steady_clock::now() + kPollInterval);
        std::this_thread::sleep_until(nextPoll);
    }
    return predicate();
}

bool SetReuseAddress(int fd)
{
    constexpr int enabled = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) == 0;
}

int BindLoopbackListener(int port)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    if (!SetReuseAddress(fd)) {
        close(fd);
        return -1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0 || listen(fd, 1) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

bool CanBindLoopbackPort(int port)
{
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }
    if (!SetReuseAddress(fd)) {
        close(fd);
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const bool canBind = bind(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == 0;
    close(fd);
    return canBind;
}

bool IsFixedWidthIndex(std::string_view value)
{
    return value.size() == kRaftLogIndexWidth && std::all_of(value.begin(), value.end(), [](char character) {
               return character >= '0' && character <= '9';
           });
}

bool IsBraftLogSegment(std::string_view entryName)
{
    const std::string_view inProgressPrefix = kRaftInProgressLogSegmentPrefix;
    if (entryName.substr(0, inProgressPrefix.size()) == inProgressPrefix) {
        return IsFixedWidthIndex(entryName.substr(inProgressPrefix.size()));
    }

    const std::string_view closedPrefix = kRaftLogSegmentPrefix;
    if (entryName.substr(0, closedPrefix.size()) != closedPrefix) {
        return false;
    }
    const auto indexes = entryName.substr(closedPrefix.size());
    const auto separator = indexes.find('_');
    return separator == kRaftLogIndexWidth && indexes.size() == kClosedRaftLogIndexCount * kRaftLogIndexWidth + 1
           && IsFixedWidthIndex(indexes.substr(0, separator)) && IsFixedWidthIndex(indexes.substr(separator + 1));
}

bool IsNonEmptyRegularFile(const std::filesystem::path &path)
{
    std::error_code error;
    if (!std::filesystem::is_regular_file(path, error) || error) {
        return false;
    }
    const auto fileSize = std::filesystem::file_size(path, error);
    return !error && fileSize > 0;
}

bool HasGeneratedBraftLogSegment(const std::filesystem::path &logDirectory)
{
    std::error_code error;
    for (std::filesystem::directory_iterator iter(logDirectory, error), end; !error && iter != end;
         iter.increment(error)) {
        if (IsBraftLogSegment(iter->path().filename().string()) && IsNonEmptyRegularFile(iter->path())) {
            return true;
        }
    }
    return false;
}

class CountingCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    explicit CountingCoordinatorDiscovery(std::string endpoint) : endpoint_(std::move(endpoint))
    {
    }

    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        calls_.fetch_add(1, std::memory_order_relaxed);
        serviceList = { endpoint_ };
        return Status::OK();
    }

    size_t CallCount() const
    {
        return calls_.load(std::memory_order_relaxed);
    }

private:
    std::string endpoint_;
    std::atomic<size_t> calls_{ 0 };
};

enum class BusinessRpc : uint8_t { GET_COORDINATOR_ID, PUT };
}  // namespace

class CoordinatorServiceElectionTestBase : public CommonTest {
public:
    CoordinatorServiceElectionTestBase() : CommonTest(std::to_string(getpid()))
    {
    }

protected:
    void SetUp() override
    {
        CommonTest::SetUp();

        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName =
            testInfo == nullptr ? "unknown" : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        rootDir_ = testCasePath_ + "/coordinator-service-election";
        dataRoot_ = rootDir_ + "/raft-data";

        std::error_code error;
        std::filesystem::remove_all(rootDir_, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(rootDir_, error)) << error.message();

        auto &allocator = TestPortAllocator::Instance();
        allocator.SetOwnerInfo("coordinator_service_election_test", testName, rootDir_);
        const auto reserveStatus = allocator.Reserve("coordinator_service_election", portLease_);
        ASSERT_TRUE(reserveStatus.IsOk()) << reserveStatus.ToString();
        endpoint_ = std::string(kLoopbackIp) + ":" + std::to_string(portLease_.Port());

    }

    void TearDown() override
    {
        if (service_ != nullptr) {
            EXPECT_TRUE(service_->Shutdown().IsOk());
            service_.reset();
        }
        if (conflictListener_ >= 0) {
            close(conflictListener_);
            conflictListener_ = -1;
        }


        if (!testCasePath_.empty()) {
            std::error_code error;
            std::filesystem::remove_all(testCasePath_, error);
            EXPECT_FALSE(error) << error.message();
        }
        TestPortAllocator::Instance().ReleaseAll();
        CommonTest::TearDown();
    }

    coordinator::CoordinatorRaftFlags MakeRaftFlags() const
    {
        return coordinator::CoordinatorRaftFlags{ endpoint_,
                                                  dataRoot_,
                                                  kHeartbeatIntervalMs,
                                                  kElectionTimeoutMs,
                                                  kDiscoveryRetryIntervalMs,
                                                  kMemberFailureGraceMs,
                                                  kHealthCheckIntervalMs,
                                                  kBootstrapWarningIntervalMs };
    }

    void CreateAndStartService(const std::shared_ptr<CountingCoordinatorDiscovery> &discovery, Deadline deadline,
                               size_t expectedMemberCount = kExpectedMemberCount)
    {
        ASSERT_EQ(service_, nullptr);
        const auto discoveryCallsBeforeRpcStart = discovery->CallCount();
        service_ = std::make_unique<coordinator::CoordinatorServiceImpl>(
            HostPort(kLoopbackIp, portLease_.Port()), discovery, expectedMemberCount, MakeRaftFlags());
        const auto initStatus = service_->Init();
        ASSERT_TRUE(initStatus.IsOk()) << initStatus.ToString();
        const auto startStatus = service_->Start();
        ASSERT_LT(std::chrono::steady_clock::now(), deadline);
        ASSERT_TRUE(startStatus.IsOk()) << startStatus.ToString();
        ASSERT_EQ(discovery->CallCount(), discoveryCallsBeforeRpcStart);
        const auto electionStatus = service_->StartElectionManager();
        ASSERT_LT(std::chrono::steady_clock::now(), deadline);
        ASSERT_TRUE(electionStatus.IsOk()) << electionStatus.ToString();
    }

    bool IsLocalRaftLeader() const
    {
        return service_ != nullptr && service_->IsLeader();
    }

    bool IsRaftServingGateOpen() const
    {
        return service_ != nullptr
               && service_->servingState_.load(std::memory_order_acquire)
                      == coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING;
    }

    void SetRaftServingGate(bool serving)
    {
        ASSERT_NE(service_, nullptr);
        service_->servingState_.store(serving ? coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING
                                              : coordinator::CoordinatorServiceImpl::ServingState::FOLLOWER_SERVING,
                                      std::memory_order_release);
    }

    Status GetRaftLeader(std::string &leader) const
    {
        if (service_ == nullptr) {
            return Status(K_NOT_READY, "Coordinator service is not available in the election ST");
        }
        return service_->GetLeader(leader);
    }

    void FillResponseHeader(coordinator::ResponseHeader *header) const
    {
        ASSERT_NE(service_, nullptr);
        service_->FillResponseHeader(header);
    }

    bool LifecycleOwnersReleased() const
    {
        return service_ != nullptr && service_->electionManager_ == nullptr && service_->rpcServer_ == nullptr
               && service_->brpcAdapter_ == nullptr;
    }

    std::unique_ptr<brpc::Channel> CreateBusinessChannel(Deadline deadline) const
    {
        const int64_t remainingMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        const auto timeoutMs =
            static_cast<int32_t>(std::clamp<int64_t>(remainingMs, kMinimumRpcTimeoutMs, kRpcTimeoutMs));

        BrpcChannelConfig config;
        config.endpoint = endpoint_;
        config.timeout_ms = timeoutMs;
        config.connect_timeout_ms = timeoutMs;
        config.max_retry = 0;
        config.enable_circuit_breaker = false;
        return BrpcChannelFactory::Create(config);
    }

    void ShutdownAndReleaseService(Deadline deadline)
    {
        ASSERT_NE(service_, nullptr);
        const auto shutdownStatus = service_->Shutdown();
        ASSERT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
        EXPECT_TRUE(LifecycleOwnersReleased());
        EXPECT_TRUE(service_->Shutdown().IsOk());
        service_.reset();
        ASSERT_TRUE(WaitUntil([this] { return CanBindLoopbackPort(portLease_.Port()); }, deadline))
            << "Coordinator endpoint remained bound after graceful shutdown: " << endpoint_;
        EXPECT_LT(std::chrono::steady_clock::now(), deadline);
    }

    void AssertRealBraftStorage() const
    {
        const std::filesystem::path dataRoot(dataRoot_);
        const auto metadataEntity = dataRoot / kRaftMetadataDirectory / kRaftMetadataEntity;
        const auto logDirectory = dataRoot / kRaftLogDirectory;
        const auto logMetadataEntity = logDirectory / kRaftLogMetadataEntity;
        ASSERT_TRUE(IsNonEmptyRegularFile(metadataEntity)) << metadataEntity;
        ASSERT_TRUE(IsNonEmptyRegularFile(logMetadataEntity)) << logMetadataEntity;
        ASSERT_TRUE(HasGeneratedBraftLogSegment(logDirectory)) << logDirectory;
    }

    butil::AtExitManager atExitManager_;
    TestPortLease portLease_;
    std::string rootDir_;
    std::string dataRoot_;
    std::string endpoint_;
    std::unique_ptr<coordinator::CoordinatorServiceImpl> service_;
    int conflictListener_{ -1 };

private:
    bool savedUseBrpc_{ false };
};

class CoordinatorServiceElectionTest : public CoordinatorServiceElectionTestBase,
                                       public testing::WithParamInterface<BusinessRpc> {};

TEST_F(CoordinatorServiceElectionTestBase, SingleExpectedMemberDisablesBootstrapRpc)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    auto discovery = std::make_shared<CountingCoordinatorDiscovery>(endpoint_);
    ASSERT_NO_FATAL_FAILURE(CreateAndStartService(discovery, caseDeadline));

    auto channel = CreateBusinessChannel(caseDeadline);
    ASSERT_NE(channel, nullptr);
    coordinator::CoordinatorService_BrpcGenericStub stub(channel.get());
    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;
    const auto bootstrapStatus = stub.GetRaftBootstrapState(request, response);
    EXPECT_EQ(bootstrapStatus.GetCode(), K_INVALID) << bootstrapStatus.ToString();
    EXPECT_EQ(discovery->CallCount(), 0U);

    request.set_group_id("wrong-coordinator-raft-group");
    const auto wrongGroupStatus = stub.GetRaftBootstrapState(request, response);
    EXPECT_EQ(wrongGroupStatus.GetCode(), K_INVALID) << wrongGroupStatus.ToString();
    ASSERT_NO_FATAL_FAILURE(ShutdownAndReleaseService(caseDeadline));
}

TEST_P(CoordinatorServiceElectionTest, SingleExpectedMemberServesBusinessRpcWithoutRaft)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    auto discovery = std::make_shared<CountingCoordinatorDiscovery>(endpoint_);
    ASSERT_NO_FATAL_FAILURE(CreateAndStartService(discovery, caseDeadline));
    const auto duplicateStartStatus = service_->Start();
    EXPECT_EQ(duplicateStartStatus.GetCode(), K_INVALID) << duplicateStartStatus.ToString();
    EXPECT_NE(duplicateStartStatus.GetMsg().find("already starting or running"), std::string::npos)
        << duplicateStartStatus.ToString();
    EXPECT_FALSE(IsLocalRaftLeader());
    EXPECT_TRUE(IsRaftServingGateOpen());

    std::string leader = "stale";
    const auto leaderStatus = GetRaftLeader(leader);
    EXPECT_EQ(leaderStatus.GetCode(), K_INVALID) << leaderStatus.ToString();
    EXPECT_TRUE(leader.empty());

    auto channel = CreateBusinessChannel(caseDeadline);
    ASSERT_NE(channel, nullptr);
    coordinator::CoordinatorService_BrpcGenericStub stub(channel.get());
    if (GetParam() == BusinessRpc::GET_COORDINATOR_ID) {
        coordinator::GetCoordinatorIdReqPb request;
        coordinator::GetCoordinatorIdRspPb response;
        const auto rpcStatus = stub.GetCoordinatorId(request, response);
        ASSERT_LT(std::chrono::steady_clock::now(), caseDeadline);
        ASSERT_TRUE(rpcStatus.IsOk()) << rpcStatus.ToString();
        EXPECT_FALSE(response.header().coordinator_id().empty());
    } else {
        coordinator::PutReqPb request;
        request.set_key("/coordinator-service-election/business-rpc");
        request.set_value("real-brpc-value");
        coordinator::PutRspPb response;
        const auto rpcStatus = stub.Put(request, response);
        ASSERT_LT(std::chrono::steady_clock::now(), caseDeadline);
        ASSERT_TRUE(rpcStatus.IsOk()) << rpcStatus.ToString();
        EXPECT_EQ(response.version(), 1);
        EXPECT_GT(response.revision(), 0);
    }
    EXPECT_EQ(discovery->CallCount(), 0U);
    ASSERT_NO_FATAL_FAILURE(ShutdownAndReleaseService(caseDeadline));
}

INSTANTIATE_TEST_SUITE_P(RealBusinessRpc, CoordinatorServiceElectionTest,
                         testing::Values(BusinessRpc::GET_COORDINATOR_ID, BusinessRpc::PUT));

TEST_F(CoordinatorServiceElectionTestBase, GracefulShutdownReleasesSingleCoordinatorEndpoint)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    auto discovery = std::make_shared<CountingCoordinatorDiscovery>(endpoint_);
    ASSERT_NO_FATAL_FAILURE(CreateAndStartService(discovery, caseDeadline));

    auto channel = CreateBusinessChannel(caseDeadline);
    ASSERT_NE(channel, nullptr);
    coordinator::CoordinatorService_BrpcGenericStub stub(channel.get());
    coordinator::GetCoordinatorIdReqPb request;
    coordinator::GetCoordinatorIdRspPb response;
    const auto rpcStatus = stub.GetCoordinatorId(request, response);
    ASSERT_LT(std::chrono::steady_clock::now(), caseDeadline);
    ASSERT_TRUE(rpcStatus.IsOk()) << rpcStatus.ToString();

    // There is intentionally no lifecycle call-order mock. Successful endpoint rebinding proves the externally visible
    // effect of shared-server teardown returning synchronously in single-Coordinator mode.
    ASSERT_NO_FATAL_FAILURE(ShutdownAndReleaseService(caseDeadline));
}

TEST_F(CoordinatorServiceElectionTestBase, RestartSingleCoordinatorDoesNotRediscoverOrCreateRaftState)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    auto discovery = std::make_shared<CountingCoordinatorDiscovery>(endpoint_);
    ASSERT_NO_FATAL_FAILURE(CreateAndStartService(discovery, caseDeadline));
    ASSERT_EQ(discovery->CallCount(), 0U);

    ASSERT_NO_FATAL_FAILURE(ShutdownAndReleaseService(caseDeadline));
    const auto callsAfterFirstStart = discovery->CallCount();

    ASSERT_NO_FATAL_FAILURE(CreateAndStartService(discovery, caseDeadline));
    EXPECT_FALSE(IsLocalRaftLeader());
    std::string recoveredLeader = "stale";
    const auto recoveredLeaderStatus = GetRaftLeader(recoveredLeader);
    EXPECT_EQ(recoveredLeaderStatus.GetCode(), K_INVALID) << recoveredLeaderStatus.ToString();
    EXPECT_TRUE(recoveredLeader.empty());
    EXPECT_EQ(discovery->CallCount(), callsAfterFirstStart);

    ASSERT_NO_FATAL_FAILURE(ShutdownAndReleaseService(caseDeadline));
}

TEST_F(CoordinatorServiceElectionTestBase, BindConflictReturnsOriginalErrorAndLeavesNoReusableResource)
{
    const auto caseDeadline = std::chrono::steady_clock::now() + kCaseBudget;
    conflictListener_ = BindLoopbackListener(portLease_.Port());
    ASSERT_GE(conflictListener_, 0);

    auto discovery = std::make_shared<CountingCoordinatorDiscovery>(endpoint_);
    service_ = std::make_unique<coordinator::CoordinatorServiceImpl>(HostPort(kLoopbackIp, portLease_.Port()),
                                                                     discovery, kExpectedMemberCount, MakeRaftFlags());
    const auto initStatus = service_->Init();
    ASSERT_TRUE(initStatus.IsOk()) << initStatus.ToString();

    const auto startStatus = service_->Start();
    ASSERT_LT(std::chrono::steady_clock::now(), caseDeadline);
    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR) << startStatus.ToString();
    EXPECT_NE(startStatus.GetMsg().find("Failed to start brpc server on " + endpoint_), std::string::npos)
        << startStatus.ToString();
    EXPECT_EQ(discovery->CallCount(), 0U);
    EXPECT_TRUE(LifecycleOwnersReleased());
    EXPECT_TRUE(service_->Shutdown().IsOk());
    EXPECT_TRUE(service_->Shutdown().IsOk());
    service_.reset();

    close(conflictListener_);
    conflictListener_ = -1;
    ASSERT_TRUE(WaitUntil([this] { return CanBindLoopbackPort(portLease_.Port()); }, caseDeadline));

    ASSERT_NO_FATAL_FAILURE(CreateAndStartService(discovery, caseDeadline));
    EXPECT_FALSE(IsLocalRaftLeader());
    EXPECT_EQ(discovery->CallCount(), 0U);
    ASSERT_NO_FATAL_FAILURE(ShutdownAndReleaseService(caseDeadline));
}
}  // namespace datasystem::st
