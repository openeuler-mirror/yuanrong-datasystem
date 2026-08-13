// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <google/protobuf/descriptor.h>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/flags/config_monitor_state.h"
#include "datasystem/common/flags/dynamic_flag_config.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/operation_logger.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/protos/coordinator.brpc.stub.pb.h"
#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"
#define private public
#include "datasystem/coordinator_server.h"
#include "datasystem/coordinator/coordinator_runtime.h"
#include "datasystem/coordinator/coordinator_service_impl.h"
#undef private
#include "cluster/test_port_allocator.h"
#include "ut/common.h"

DS_DECLARE_string(coordinator_address);
DS_DECLARE_string(coordinator_raft_data_dir);
DS_DECLARE_int32(coordinator_raft_heartbeat_interval_ms);
DS_DECLARE_int32(coordinator_raft_election_timeout_ms);
DS_DECLARE_uint32(coordinator_member_failure_grace_ms);
DS_DECLARE_uint32(coordinator_discovery_retry_interval_ms);
DS_DECLARE_double(request_sample_rate);
DS_DECLARE_double(access_sample_rate);
DS_DECLARE_double(diagnostic_sample_rate);
DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);
DS_DECLARE_bool(log_async);
DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace ut {
namespace {

TEST(CoordinatorServiceProtocolTest, KeepsLegacyZmqMethodIndexesStable)
{
    const auto *service = google::protobuf::DescriptorPool::generated_pool()->FindServiceByName(
        "datasystem.coordinator.CoordinatorService");
    ASSERT_NE(service, nullptr);
    const auto *reportCandidate = service->FindMethodByName("ReportTopologyRecoveryCandidate");
    const auto *rawSnapshot = service->FindMethodByName("GetClusterRawSnapshot");
    const auto *bootstrapState = service->FindMethodByName("GetRaftBootstrapState");
    ASSERT_NE(reportCandidate, nullptr);
    ASSERT_NE(rawSnapshot, nullptr);
    ASSERT_NE(bootstrapState, nullptr);
    EXPECT_EQ(reportCandidate->index(), 7);
    const auto *ensureMembership = service->FindMethodByName("EnsureLeaderMembership");
    ASSERT_NE(ensureMembership, nullptr);
    EXPECT_EQ(rawSnapshot->index(), 8);
    EXPECT_EQ(bootstrapState->index(), 9);
    EXPECT_EQ(ensureMembership->index(), 10);
}

class FakeCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        serviceList = { "127.0.0.1:31501" };
        return Status::OK();
    }
};

class CoordinatorRuntimeMock final : public CoordinatorRuntime {
public:
    explicit CoordinatorRuntimeMock(coordinator::CoordinatorRaftFlags flags) : flags_(std::move(flags))
    {
    }

    coordinator::CoordinatorRaftFlags Snapshot() const
    {
        return GetRaftFlags();
    }

    size_t SnapshotCallCount() const
    {
        return snapshotCallCount_.load();
    }

protected:
    coordinator::CoordinatorRaftFlags GetRaftFlags() const override
    {
        snapshotCallCount_.fetch_add(1);
        return flags_;
    }

private:
    coordinator::CoordinatorRaftFlags flags_;
    mutable std::atomic<size_t> snapshotCallCount_{ 0 };
};

class CoordinatorRuntimeAccessor final : public CoordinatorRuntime {
public:
    coordinator::CoordinatorRaftFlags Snapshot() const
    {
        return GetRaftFlags();
    }
};

void ExpectInvalidWithMessage(const Status &status, const std::string &message)
{
    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_NE(status.ToString().find(message), std::string::npos) << status.ToString();
}

void CreateNonEmptyFile(const std::filesystem::path &path, const std::string &contents)
{
    std::ofstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file << contents;
    file.close();
    ASSERT_FALSE(file.fail());
}

void CreateCoordinatorConfig(const std::string &path, bool useBrpc = true)
{
    CreateNonEmptyFile(path, std::string(R"({"use_brpc":{"value":")") + (useBrpc ? "true" : "false")
                                 + R"(","description":"Coordinator Runtime UT config."}})");
}

CoordinatorOptions MakeCoordinatorOptions(const std::shared_ptr<ICoordinatorDiscovery> &discovery,
                                          int expectedMemberCount = 1)
{
    CoordinatorOptions options;
    options.coordinatorDiscovery = discovery;
    options.expectedMemberCount = expectedMemberCount;
    return options;
}

class ScriptedCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        calls_.fetch_add(1);
        serviceList = candidates_;
        return Status::OK();
    }

    std::vector<std::string> candidates_;
    std::atomic<size_t> calls_{ 0 };
};

class BlockingReentrantCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    explicit BlockingReentrantCoordinatorDiscovery(std::string candidate) : candidate_(std::move(candidate))
    {
    }

    void BindService(coordinator::CoordinatorServiceImpl *service)
    {
        service_ = service;
    }

    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            entered_ = true;
        }
        cv_.notify_all();

        std::string leader;
        const auto leaderStatus = service_->GetLeader(leader);
        {
            std::unique_lock<std::mutex> lock(mutex_);
            reentrantLeaderStatus_ = leaderStatus;
            reentrantQueryComplete_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return released_; });
        }
        serviceList = { candidate_ };
        return Status::OK();
    }

    bool WaitForReentrantQuery(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] { return entered_ && reentrantQueryComplete_; });
    }

    Status ReentrantLeaderStatus() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return reentrantLeaderStatus_;
    }

    void Release()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

private:
    std::string candidate_;
    coordinator::CoordinatorServiceImpl *service_{ nullptr };
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool entered_{ false };
    bool reentrantQueryComplete_{ false };
    bool released_{ false };
    Status reentrantLeaderStatus_;
};

class ThrowOnCopyLifecycleCallback {
public:
    ThrowOnCopyLifecycleCallback(int &callCount, bool returnError) : callCount_(&callCount), returnError_(returnError)
    {
    }

    ThrowOnCopyLifecycleCallback(const ThrowOnCopyLifecycleCallback &)
    {
        throw std::runtime_error("lifecycle callback target copy is forbidden");
    }

    ThrowOnCopyLifecycleCallback(ThrowOnCopyLifecycleCallback &&) noexcept = default;
    ThrowOnCopyLifecycleCallback &operator=(const ThrowOnCopyLifecycleCallback &) = delete;
    ThrowOnCopyLifecycleCallback &operator=(ThrowOnCopyLifecycleCallback &&) noexcept = default;

    Status operator()() const
    {
        ++(*callCount_);
        return returnError_ ? Status(K_RUNTIME_ERROR, "scripted lifecycle callback error") : Status::OK();
    }

private:
    int *callCount_;
    bool returnError_;
};

class OccupiedTcpPort {
public:
    explicit OccupiedTcpPort(int port) : port_(port)
    {
        socketFd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socketFd_ < 0) {
            throw std::runtime_error("failed to create Coordinator Runtime UT socket");
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<uint16_t>(port_));
        if (bind(socketFd_, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0
            || listen(socketFd_, 1) != 0) {
            Release();
            throw std::runtime_error("failed to occupy Coordinator Runtime UT port");
        }
    }

    ~OccupiedTcpPort()
    {
        Release();
    }

    int Port() const
    {
        return port_;
    }

    void Release()
    {
        if (socketFd_ >= 0) {
            (void)close(socketFd_);
            socketFd_ = -1;
        }
    }

private:
    int socketFd_{ -1 };
    int port_{ 0 };
};

class ScopedCoordinatorServerRuntimeOverride {
public:
    explicit ScopedCoordinatorServerRuntimeOverride(std::unique_ptr<CoordinatorRuntime> runtime)
        : server_(CoordinatorServer::GetInstance()), originalRuntime_(std::move(server_->runtime_))
    {
        server_->runtime_ = std::move(runtime);
    }

    ~ScopedCoordinatorServerRuntimeOverride()
    {
        server_->runtime_ = std::move(originalRuntime_);
    }

private:
    CoordinatorServer *server_;
    std::unique_ptr<CoordinatorRuntime> originalRuntime_;
};

class ScopedDynamicConfigTestState {
public:
    ScopedDynamicConfigTestState()
        : originalRequestSampleRate_(FLAGS_request_sample_rate), originalAccessSampleRate_(FLAGS_access_sample_rate),
          originalDiagnosticSampleRate_(FLAGS_diagnostic_sample_rate),
          originalFileMonitorEnabled_(ConfigMonitorState::Instance().IsFileMonitorEnabled())
    {
        FLAGS_request_sample_rate = 1.0;
        FLAGS_access_sample_rate = 1.0;
        FLAGS_diagnostic_sample_rate = 1.0;
        LogSampler::Instance().ResetForTest();
        LogSampler::Instance().Init();
        ConfigMonitorState::Instance().SetFileMonitorEnabled(false);
    }

    ~ScopedDynamicConfigTestState()
    {
        FLAGS_request_sample_rate = originalRequestSampleRate_;
        FLAGS_access_sample_rate = originalAccessSampleRate_;
        FLAGS_diagnostic_sample_rate = originalDiagnosticSampleRate_;
        LogSampler::Instance().ResetForTest();
        LogSampler::Instance().Init();
        LogSampleUserConfig config;
        config.requestSampleRate = originalRequestSampleRate_;
        config.accessSampleRate = originalAccessSampleRate_;
        config.diagnosticSampleRate = originalDiagnosticSampleRate_;
        (void)LogSampler::Instance().UpdateConfigFromFlags(config);
        ConfigMonitorState::Instance().SetFileMonitorEnabled(originalFileMonitorEnabled_);
    }

private:
    double originalRequestSampleRate_;
    double originalAccessSampleRate_;
    double originalDiagnosticSampleRate_;
    bool originalFileMonitorEnabled_;
};

class ScopedTempDirectory {
public:
    ScopedTempDirectory()
    {
        char pathTemplate[] = "/tmp/ds-coord-ut-XXXXXX";
        const auto *createdPath = mkdtemp(pathTemplate);
        if (createdPath == nullptr) {
            throw std::runtime_error("failed to create Coordinator test directory");
        }
        path_ = createdPath;
    }

    ~ScopedTempDirectory()
    {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    std::string Child(const std::string &name) const
    {
        return (std::filesystem::path(path_) / name).string();
    }

private:
    std::string path_;
};

class CoordinatorElectionServiceTest : public testing::Test {
protected:
    void SetUp() override
    {
        savedUseBrpc_ = FLAGS_use_brpc;
        savedCoordinatorAddress_ = FLAGS_coordinator_address;
        savedRaftDataDir_ = FLAGS_coordinator_raft_data_dir;
        savedHeartbeatIntervalMs_ = FLAGS_coordinator_raft_heartbeat_interval_ms;
        savedElectionTimeoutMs_ = FLAGS_coordinator_raft_election_timeout_ms;
        savedFailureGraceMs_ = FLAGS_coordinator_member_failure_grace_ms;
        savedDiscoveryRetryMs_ = FLAGS_coordinator_discovery_retry_interval_ms;

        tempDirectory_ = std::make_unique<ScopedTempDirectory>();
        const auto *testInfo = testing::UnitTest::GetInstance()->current_test_info();
        const std::string testName =
            testInfo == nullptr ? "unknown" : std::string(testInfo->test_case_name()) + "." + testInfo->name();
        auto &allocator = st::TestPortAllocator::Instance();
        allocator.SetOwnerInfo("coordinator_server_options_test", testName,
                               tempDirectory_->Child("port-allocator-owner"));
        const std::vector<std::string> roles{ "coordinator-brpc", "coordinator-brpc-secondary",
                                              "coordinator-brpc-conflict" };
        for (const auto &role : roles) {
            st::TestPortLease lease;
            DS_ASSERT_OK(allocator.Reserve(role, lease));
            portLeases_.emplace_back(std::move(lease));
        }
        ASSERT_EQ(portLeases_.size(), roles.size());
        for (const auto &lease : portLeases_) {
            peers_.emplace_back(std::string(kLoopbackIp) + ":" + std::to_string(lease.Port()));
        }
        coordinatorAddress_ = peers_.front();

        FLAGS_use_brpc = true;
        FLAGS_coordinator_address = coordinatorAddress_;
        FLAGS_coordinator_raft_data_dir = tempDirectory_->Child("raft-data");
        FLAGS_coordinator_raft_heartbeat_interval_ms = kHeartbeatIntervalMs;
        FLAGS_coordinator_raft_election_timeout_ms = kElectionTimeoutMs;
        FLAGS_coordinator_member_failure_grace_ms = kFailureGraceMs;
        FLAGS_coordinator_discovery_retry_interval_ms = kDiscoveryRetryMs;
        raftFlags_ = coordinator::CoordinatorRaftFlags{ coordinatorAddress_, tempDirectory_->Child("raft-data"),
                                                        kHeartbeatIntervalMs, kElectionTimeoutMs,
                                                        kDiscoveryRetryMs,    kFailureGraceMs,
                                                        kHealthIntervalMs,    kBootstrapWarningIntervalMs };
    }

    void TearDown() override
    {
        StopAllServicesAndJoinThreads();

        FLAGS_use_brpc = savedUseBrpc_;
        FLAGS_coordinator_address = savedCoordinatorAddress_;
        FLAGS_coordinator_raft_data_dir = savedRaftDataDir_;
        FLAGS_coordinator_raft_heartbeat_interval_ms = savedHeartbeatIntervalMs_;
        FLAGS_coordinator_raft_election_timeout_ms = savedElectionTimeoutMs_;
        FLAGS_coordinator_member_failure_grace_ms = savedFailureGraceMs_;
        FLAGS_coordinator_discovery_retry_interval_ms = savedDiscoveryRetryMs_;

        auto &allocator = st::TestPortAllocator::Instance();
        for (const auto &lease : portLeases_) {
            allocator.Release(lease.Port());
        }
        portLeases_.clear();
        tempDirectory_.reset();
    }

    coordinator::CoordinatorServiceImpl *MakeService(const std::shared_ptr<ICoordinatorDiscovery> &discovery,
                                                     size_t expectedMemberCount)
    {
        return MakeService(discovery, expectedMemberCount, raftFlags_);
    }

    coordinator::CoordinatorServiceImpl *MakeService(const std::shared_ptr<ICoordinatorDiscovery> &discovery,
                                                     size_t expectedMemberCount,
                                                     coordinator::CoordinatorRaftFlags flags)
    {
        services_.emplace_back(std::make_unique<coordinator::CoordinatorServiceImpl>(
            HostPort(kLoopbackIp, portLeases_.front().Port()), discovery, expectedMemberCount, std::move(flags)));
        return services_.back().get();
    }

    void StopAllServicesAndJoinThreads()
    {
        for (auto &service : services_) {
            service->electionManagerPublishedHook_ = {};
            service->raftBootstrapSnapshotCopiedHook_ = {};
            service->raftBootstrapHandlerEnteredHook_ = {};
            service->rpcServerShutdownHook_ = {};
            service->electionManagerShutdownHook_ = {};
            (void)service->Shutdown();
        }
        services_.clear();
    }

    static constexpr const char *kLoopbackIp = "127.0.0.1";
    static constexpr size_t kCoordinatorCount = 3;
    static constexpr int32_t kHeartbeatIntervalMs = 500;
    static constexpr int32_t kElectionTimeoutMs = 4'000;
    static constexpr uint32_t kHealthIntervalMs = 1'000;
    static constexpr uint32_t kFailureGraceMs = 20'000;
    static constexpr uint32_t kDiscoveryRetryMs = 5'000;
    static constexpr uint32_t kBootstrapWarningIntervalMs = 30'000;

    std::unique_ptr<ScopedTempDirectory> tempDirectory_;
    std::vector<st::TestPortLease> portLeases_;
    std::vector<std::string> peers_;
    std::string coordinatorAddress_;
    coordinator::CoordinatorRaftFlags raftFlags_;
    std::vector<std::unique_ptr<coordinator::CoordinatorServiceImpl>> services_;
    bool savedUseBrpc_{ false };
    std::string savedCoordinatorAddress_;
    std::string savedRaftDataDir_;
    int32_t savedHeartbeatIntervalMs_{ 0 };
    int32_t savedElectionTimeoutMs_{ 0 };
    uint32_t savedFailureGraceMs_{ 0 };
    uint32_t savedDiscoveryRetryMs_{ 0 };
};

void ExpectAllBusinessRpcsReturn(coordinator::CoordinatorServiceImpl &service, StatusCode expectedCode,
                                 const std::string &expectedMessage)
{
    const auto expectStatus = [expectedCode, &expectedMessage](const char *rpcName, const Status &status) {
        SCOPED_TRACE(rpcName);
        EXPECT_EQ(status.GetCode(), expectedCode) << status.ToString();
        EXPECT_NE(status.ToString().find(expectedMessage), std::string::npos) << status.ToString();
    };

    coordinator::PutReqPb putReq;
    coordinator::PutRspPb putRsp;
    expectStatus("Put", service.Put(putReq, putRsp));
    coordinator::RangeReqPb rangeReq;
    coordinator::RangeRspPb rangeRsp;
    expectStatus("Range", service.Range(rangeReq, rangeRsp));
    coordinator::DeleteRangeReqPb deleteReq;
    coordinator::DeleteRangeRspPb deleteRsp;
    expectStatus("DeleteRange", service.DeleteRange(deleteReq, deleteRsp));
    coordinator::WatchRangeReqPb watchReq;
    coordinator::WatchRangeRspPb watchRsp;
    expectStatus("WatchRange", service.WatchRange(watchReq, watchRsp));
    coordinator::CancelWatchReqPb cancelReq;
    coordinator::CancelWatchRspPb cancelRsp;
    expectStatus("CancelWatch", service.CancelWatch(cancelReq, cancelRsp));
    coordinator::KeepAliveReqPb keepAliveReq;
    coordinator::KeepAliveRspPb keepAliveRsp;
    expectStatus("KeepAlive", service.KeepAlive(keepAliveReq, keepAliveRsp));
    coordinator::GetCoordinatorIdReqPb idReq;
    coordinator::GetCoordinatorIdRspPb idRsp;
    expectStatus("GetCoordinatorId", service.GetCoordinatorId(idReq, idRsp));
    coordinator::GetClusterRawSnapshotReqPb snapshotReq;
    coordinator::GetClusterRawSnapshotRspPb snapshotRsp;
    expectStatus("GetClusterRawSnapshot", service.GetClusterRawSnapshot(snapshotReq, snapshotRsp));
}

}  // namespace

TEST(CoordinatorServerOptionsTest, RejectsNullDiscoveryBeforeReadingConfig)
{
    CoordinatorRuntime runtime;
    CoordinatorOptions options;
    options.expectedMemberCount = 1;

    auto status = runtime.InitAndRun(options);

    ExpectInvalidWithMessage(status, "coordinatorDiscovery");
}

TEST(CoordinatorServerOptionsTest, RejectsNonPositiveExpectedMemberCountBeforeReadingConfig)
{
    CoordinatorOptions options;
    options.coordinatorDiscovery = std::make_shared<FakeCoordinatorDiscovery>();

    for (const int expectedMemberCount : { 0, -1 }) {
        CoordinatorRuntime runtime;
        options.expectedMemberCount = expectedMemberCount;
        auto status = runtime.InitAndRun(options);
        ExpectInvalidWithMessage(status, "expectedMemberCount");
    }
}

TEST(CoordinatorServerOptionsTest, RejectsUnpairedCallbacksBeforeReadingConfig)
{
    CoordinatorRuntime runtime;
    CoordinatorOptions options;
    options.coordinatorDiscovery = std::make_shared<FakeCoordinatorDiscovery>();
    options.expectedMemberCount = 1;
    options.onStart = [] { return Status::OK(); };

    auto status = runtime.InitAndRun(options);

    ExpectInvalidWithMessage(status, "onStart and onStop");
}

TEST(CoordinatorServerOptionsTest, FacadeRejectsEmptyConfigPath)
{
    CoordinatorOptions options;
    options.coordinatorDiscovery = std::make_shared<FakeCoordinatorDiscovery>();
    options.expectedMemberCount = 1;

    const auto status = CoordinatorServer::GetInstance()->InitAndRun(options);

    ExpectInvalidWithMessage(status, "configFilePath must not be empty");
}

TEST(CoordinatorServerOptionsTest, DefaultRaftFlagSnapshotCapturesEveryRuntimeFlag)
{
    const auto savedCoordinatorAddress = FLAGS_coordinator_address;
    const auto savedRaftDataDir = FLAGS_coordinator_raft_data_dir;
    const auto savedHeartbeatInterval = FLAGS_coordinator_raft_heartbeat_interval_ms;
    const auto savedElectionTimeout = FLAGS_coordinator_raft_election_timeout_ms;
    const auto savedFailureGrace = FLAGS_coordinator_member_failure_grace_ms;
    const auto savedDiscoveryRetry = FLAGS_coordinator_discovery_retry_interval_ms;
    FLAGS_coordinator_address = "127.0.0.1:31601";
    FLAGS_coordinator_raft_data_dir = "/tmp/coordinator-runtime-snapshot";
    FLAGS_coordinator_raft_heartbeat_interval_ms = 411;
    FLAGS_coordinator_raft_election_timeout_ms = 4'110;
    FLAGS_coordinator_member_failure_grace_ms = 20'101;
    FLAGS_coordinator_discovery_retry_interval_ms = 5'101;
    CoordinatorRuntimeAccessor runtime;

    const auto snapshot = runtime.Snapshot();

    EXPECT_EQ(snapshot.localAddress, FLAGS_coordinator_address);
    EXPECT_EQ(snapshot.dataDir, FLAGS_coordinator_raft_data_dir);
    EXPECT_EQ(snapshot.heartbeatIntervalMs, FLAGS_coordinator_raft_heartbeat_interval_ms);
    EXPECT_EQ(snapshot.electionTimeoutMs, FLAGS_coordinator_raft_election_timeout_ms);
    EXPECT_EQ(snapshot.memberFailureGraceMs, FLAGS_coordinator_member_failure_grace_ms);
    EXPECT_EQ(snapshot.discoveryRetryIntervalMs, FLAGS_coordinator_discovery_retry_interval_ms);
    EXPECT_EQ(snapshot.healthCheckIntervalMs, coordinator::kDefaultCoordinatorElectionHealthCheckIntervalMs);
    EXPECT_EQ(snapshot.bootstrapWarningIntervalMs,
              coordinator::kDefaultCoordinatorElectionBootstrapWarningIntervalMs);

    FLAGS_coordinator_address = savedCoordinatorAddress;
    FLAGS_coordinator_raft_data_dir = savedRaftDataDir;
    FLAGS_coordinator_raft_heartbeat_interval_ms = savedHeartbeatInterval;
    FLAGS_coordinator_raft_election_timeout_ms = savedElectionTimeout;
    FLAGS_coordinator_member_failure_grace_ms = savedFailureGrace;
    FLAGS_coordinator_discovery_retry_interval_ms = savedDiscoveryRetry;
}

TEST(CoordinatorServerOptionsTest, RuntimeMockReturnsInjectedRaftFlagSnapshot)
{
    coordinator::CoordinatorRaftFlags flags{
        "127.0.0.1:31611", "/tmp/injected-runtime-snapshot", 411, 4'110, 5'111, 20'111, 1'111, 30'111
    };
    CoordinatorRuntimeMock runtime(flags);

    const auto snapshot = runtime.Snapshot();

    EXPECT_EQ(snapshot.localAddress, flags.localAddress);
    EXPECT_EQ(snapshot.dataDir, flags.dataDir);
    EXPECT_EQ(snapshot.heartbeatIntervalMs, flags.heartbeatIntervalMs);
    EXPECT_EQ(snapshot.electionTimeoutMs, flags.electionTimeoutMs);
    EXPECT_EQ(snapshot.healthCheckIntervalMs, flags.healthCheckIntervalMs);
    EXPECT_EQ(snapshot.memberFailureGraceMs, flags.memberFailureGraceMs);
    EXPECT_EQ(snapshot.discoveryRetryIntervalMs, flags.discoveryRetryIntervalMs);
    EXPECT_EQ(snapshot.bootstrapWarningIntervalMs, flags.bootstrapWarningIntervalMs);
}

TEST(CoordinatorServerOptionsTest, ThrownStartExceptionStillInvokesStopExactlyOnce)
{
    CoordinatorRuntime runtime;
    int stopCount = 0;
    runtime.onStart_ = []() -> Status { throw std::runtime_error("start threw"); };
    runtime.onStop_ = [&stopCount] {
        ++stopCount;
        return Status::OK();
    };
    runtime.callbackState_ = CoordinatorRuntime::LifecycleCallbackState::READY;

    Status startStatus;
    EXPECT_NO_THROW(startStatus = runtime.InvokeOnStart());
    auto firstStopStatus = runtime.InvokeOnStop();
    auto secondStopStatus = runtime.InvokeOnStop();

    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(firstStopStatus.IsOk()) << firstStopStatus.ToString();
    EXPECT_TRUE(secondStopStatus.IsOk()) << secondStopStatus.ToString();
    EXPECT_EQ(stopCount, 1);
}

TEST(CoordinatorServerOptionsTest, StartFailureStillInvokesStopExactlyOnce)
{
    CoordinatorRuntime runtime;
    int startCount = 0;
    int stopCount = 0;
    runtime.onStart_ = [&startCount] {
        ++startCount;
        return Status(K_RUNTIME_ERROR, "start failed");
    };
    runtime.onStop_ = [&stopCount] {
        ++stopCount;
        return Status::OK();
    };
    runtime.callbackState_ = CoordinatorRuntime::LifecycleCallbackState::READY;

    auto startStatus = runtime.InvokeOnStart();
    auto firstStopStatus = runtime.InvokeOnStop();
    auto secondStopStatus = runtime.InvokeOnStop();

    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR);
    EXPECT_TRUE(firstStopStatus.IsOk()) << firstStopStatus.ToString();
    EXPECT_TRUE(secondStopStatus.IsOk()) << secondStopStatus.ToString();
    EXPECT_EQ(startCount, 1);
    EXPECT_EQ(stopCount, 1);
}

TEST(CoordinatorServerOptionsTest, CallbackTargetsThatThrowOnCopyAreMovedAndInvokedExactlyOnce)
{
    CoordinatorRuntime runtime;
    int startCount = 0;
    int stopCount = 0;
    std::function<Status()> onStart(ThrowOnCopyLifecycleCallback(startCount, true));
    std::function<Status()> onStop(ThrowOnCopyLifecycleCallback(stopCount, false));
    runtime.onStart_ = std::move(onStart);
    runtime.onStop_ = std::move(onStop);
    runtime.callbackState_ = CoordinatorRuntime::LifecycleCallbackState::READY;

    const auto startStatus = runtime.InvokeOnStart();
    const auto firstStopStatus = runtime.InvokeOnStop();
    const auto secondStopStatus = runtime.InvokeOnStop();

    EXPECT_EQ(startStatus.GetCode(), K_RUNTIME_ERROR) << startStatus.ToString();
    EXPECT_TRUE(firstStopStatus.IsOk()) << firstStopStatus.ToString();
    EXPECT_TRUE(secondStopStatus.IsOk()) << secondStopStatus.ToString();
    EXPECT_EQ(startCount, 1);
    EXPECT_EQ(stopCount, 1);
}

TEST(CoordinatorServerOptionsTest, StopOnlyWakesOwningRuntimeAndDoesNotSetProcessExitFlag)
{
    constexpr auto kWakeTimeout = std::chrono::seconds(1);
    constexpr auto kIsolationWindow = std::chrono::milliseconds(50);
    const auto savedExitFlag = g_exitFlag;
    g_exitFlag = 0;
    CoordinatorRuntime firstRuntime;
    CoordinatorRuntime secondRuntime;
    std::promise<void> firstExited;
    std::promise<void> secondExited;
    auto firstFuture = firstExited.get_future();
    auto secondFuture = secondExited.get_future();
    std::thread firstThread([&] {
        firstRuntime.RunEventLoop();
        firstExited.set_value();
    });
    std::thread secondThread([&] {
        secondRuntime.RunEventLoop();
        secondExited.set_value();
    });

    DS_ASSERT_OK(firstRuntime.Stop());
    const auto firstWait = firstFuture.wait_for(kWakeTimeout);
    const auto secondWait = secondFuture.wait_for(kIsolationWindow);
    const auto exitFlagAfterFirstStop = g_exitFlag;
    DS_ASSERT_OK(secondRuntime.Stop());
    const auto finalSecondWait = secondFuture.wait_for(kWakeTimeout);
    firstThread.join();
    secondThread.join();
    g_exitFlag = savedExitFlag;

    EXPECT_EQ(firstWait, std::future_status::ready);
    EXPECT_EQ(secondWait, std::future_status::timeout);
    EXPECT_EQ(finalSecondWait, std::future_status::ready);
    EXPECT_EQ(exitFlagAfterFirstStop, 0);
}

TEST_F(CoordinatorElectionServiceTest, RuntimeEmptyConfigPathSkipsParsingAndUsesRaftSnapshot)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    CoordinatorRuntimeMock runtime(raftFlags_);
    auto options = MakeCoordinatorOptions(discovery);
    int startCount = 0;
    int stopCount = 0;
    options.onStart = [&startCount] {
        ++startCount;
        return Status(K_RUNTIME_ERROR, "stop empty-path Runtime before election startup");
    };
    options.onStop = [&stopCount] {
        ++stopCount;
        return Status::OK();
    };

    const auto status = runtime.InitAndRun(options);

    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR) << status.ToString();
    EXPECT_EQ(runtime.SnapshotCallCount(), 1U);
    EXPECT_EQ(startCount, 1);
    EXPECT_EQ(stopCount, 1);
}

TEST_F(CoordinatorElectionServiceTest, RealRuntimePublishesConfigReadinessOnlyAfterStartupCompletes)
{
    constexpr auto kReadyTimeout = std::chrono::seconds(3);
    ScopedDynamicConfigTestState testState;
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    CoordinatorRuntimeMock runtime(raftFlags_);
    auto options = MakeCoordinatorOptions(discovery);
    Status updateDuringOnStart;
    options.onStart = [&] {
        updateDuringOnStart = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})");
        return Status::OK();
    };
    options.onStop = [] { return Status::OK(); };

    auto runFuture = std::async(std::launch::async, [&] { return runtime.InitAndRun(options); });
    Status readyUpdate(K_NOT_READY, "not attempted");
    const auto deadline = std::chrono::steady_clock::now() + kReadyTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
        readyUpdate = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})");
        if (readyUpdate.IsOk() || readyUpdate.GetCode() != K_NOT_READY) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    DS_ASSERT_OK(runtime.Stop());
    const auto runStatus = runFuture.get();
    EXPECT_EQ(updateDuringOnStart.GetCode(), K_NOT_READY) << updateDuringOnStart.ToString();
    EXPECT_TRUE(readyUpdate.IsOk()) << readyUpdate.ToString();
    EXPECT_DOUBLE_EQ(FLAGS_diagnostic_sample_rate, 0.75);
    EXPECT_TRUE(runStatus.IsOk()) << runStatus.ToString();
    const auto afterStop = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.5"})");
    EXPECT_EQ(afterStop.GetCode(), K_SHUTTING_DOWN) << afterStop.ToString();
}

TEST_F(CoordinatorElectionServiceTest, StartupFailureNeverOpensConfigAdmission)
{
    ScopedDynamicConfigTestState testState;
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    CoordinatorRuntimeMock runtime(raftFlags_);
    auto options = MakeCoordinatorOptions(discovery);
    Status updateDuringOnStart;
    options.onStart = [&] {
        updateDuringOnStart = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})");
        return Status(K_RUNTIME_ERROR, "scripted startup failure");
    };
    options.onStop = [] { return Status::OK(); };

    const auto runStatus = runtime.InitAndRun(options);

    EXPECT_EQ(updateDuringOnStart.GetCode(), K_NOT_READY) << updateDuringOnStart.ToString();
    EXPECT_EQ(runStatus.GetCode(), K_RUNTIME_ERROR) << runStatus.ToString();
    const auto afterFailure = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.5"})");
    EXPECT_EQ(afterFailure.GetCode(), K_SHUTTING_DOWN) << afterFailure.ToString();
    EXPECT_DOUBLE_EQ(FLAGS_diagnostic_sample_rate, 1.0);
}

TEST_F(CoordinatorElectionServiceTest, RuntimeNonEmptyConfigPathParsesProcessFlags)
{
    const auto configPath = tempDirectory_->Child("runtime-parse-config.json");
    CreateCoordinatorConfig(configPath, false);
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    CoordinatorRuntimeMock runtime(raftFlags_);
    auto options = MakeCoordinatorOptions(discovery, kCoordinatorCount);
    options.configFilePath = configPath;

    const auto status = runtime.InitAndRun(options);

    ExpectInvalidWithMessage(status, "use_brpc=true");
    EXPECT_EQ(runtime.SnapshotCallCount(), 1U);
    EXPECT_FALSE(FLAGS_use_brpc);
}

TEST_F(CoordinatorElectionServiceTest, MalformedConfigFailsBeforeRaftSnapshot)
{
    const auto configPath = tempDirectory_->Child("malformed-runtime-config.json");
    CreateNonEmptyFile(configPath, "{");
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    CoordinatorRuntimeMock runtime(raftFlags_);
    auto options = MakeCoordinatorOptions(discovery);
    options.configFilePath = configPath;

    const auto status = runtime.InitAndRun(options);

    ExpectInvalidWithMessage(status, "Parse config file");
    EXPECT_NE(status.ToString().find(configPath), std::string::npos) << status.ToString();
    EXPECT_EQ(runtime.SnapshotCallCount(), 0U);
}

TEST_F(CoordinatorElectionServiceTest, ConcurrentRuntimesUseIndependentRaftSnapshotsWithEmptyConfigPaths)
{
    constexpr auto kCallbackTimeout = std::chrono::seconds(2);
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = peers_;

    auto firstFlags = raftFlags_;
    auto secondFlags = raftFlags_;
    secondFlags.localAddress = peers_[1];
    secondFlags.dataDir = tempDirectory_->Child("second-raft-data");
    CoordinatorRuntimeMock firstRuntime(std::move(firstFlags));
    CoordinatorRuntimeMock secondRuntime(std::move(secondFlags));

    std::promise<void> secondCallbackPromise;
    auto secondCallbackFuture = secondCallbackPromise.get_future();
    std::future_status secondCallbackWait = std::future_status::deferred;
    Status secondStatus;
    std::thread secondThread;
    struct ThreadJoiner {
        std::thread &thread;

        void Join() noexcept
        {
            if (thread.joinable()) {
                thread.join();
            }
        }

        ~ThreadJoiner()
        {
            Join();
        }
    } threadJoiner{ secondThread };
    int firstStartCount = 0;
    int firstStopCount = 0;
    int secondStartCount = 0;
    int secondStopCount = 0;

    auto secondOptions = MakeCoordinatorOptions(discovery, kCoordinatorCount);
    secondOptions.onStart = [&] {
        ++secondStartCount;
        secondCallbackPromise.set_value();
        return Status(K_RUNTIME_ERROR, "stop second Runtime after callback");
    };
    secondOptions.onStop = [&secondStopCount] {
        ++secondStopCount;
        return Status::OK();
    };

    auto firstOptions = MakeCoordinatorOptions(discovery, kCoordinatorCount);
    firstOptions.onStart = [&] {
        ++firstStartCount;
        secondThread = std::thread([&] {
            try {
                secondStatus = secondRuntime.InitAndRun(secondOptions);
            } catch (const std::exception &error) {
                secondStatus = Status(K_RUNTIME_ERROR, std::string("second Runtime thread threw: ") + error.what());
            } catch (...) {
                secondStatus = Status(K_RUNTIME_ERROR, "second Runtime thread threw a non-standard exception");
            }
        });
        secondCallbackWait = secondCallbackFuture.wait_for(kCallbackTimeout);
        return Status(K_RUNTIME_ERROR, "stop first Runtime after concurrent callback check");
    };
    firstOptions.onStop = [&firstStopCount] {
        ++firstStopCount;
        return Status::OK();
    };

    const auto firstStatus = firstRuntime.InitAndRun(firstOptions);
    threadJoiner.Join();

    EXPECT_EQ(secondCallbackWait, std::future_status::ready);
    EXPECT_EQ(firstStatus.GetCode(), K_RUNTIME_ERROR) << firstStatus.ToString();
    EXPECT_EQ(secondStatus.GetCode(), K_RUNTIME_ERROR) << secondStatus.ToString();
    EXPECT_EQ(firstRuntime.SnapshotCallCount(), 1U);
    EXPECT_EQ(secondRuntime.SnapshotCallCount(), 1U);
    EXPECT_EQ(firstStartCount, 1);
    EXPECT_EQ(firstStopCount, 1);
    EXPECT_EQ(secondStartCount, 1);
    EXPECT_EQ(secondStopCount, 1);
}

TEST(CoordinatorServerOptionsTest, ServerStopDelegatesToOwnedRuntime)
{
    auto *server = CoordinatorServer::GetInstance();
    ASSERT_NE(server->runtime_, nullptr);
    auto &runtime = *server->runtime_;
    const auto originalConfigState = runtime.configState_;
    runtime.stopRequested_ = false;

    const auto stopStatus = server->Stop();

    EXPECT_TRUE(stopStatus.IsOk()) << stopStatus.ToString();
    EXPECT_TRUE(runtime.stopRequested_);
    runtime.stopRequested_ = false;
    runtime.configState_ = originalConfigState;
}

TEST(CoordinatorServerOptionsTest, UpdateConfigIsGatedByRuntimeLifecycle)
{
    ScopedDynamicConfigTestState testState;
    CoordinatorRuntime runtime;

    const auto beforeStart = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})");
    EXPECT_EQ(beforeStart.GetCode(), K_NOT_READY) << beforeStart.ToString();

    runtime.EnableConfigUpdates();
    const auto invalidStatus = runtime.UpdateConfig(R"({"v":2})");
    EXPECT_EQ(invalidStatus.GetCode(), K_INVALID) << invalidStatus.ToString();
    DS_ASSERT_OK(runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})"));
    EXPECT_DOUBLE_EQ(FLAGS_diagnostic_sample_rate, 0.75);

    DS_ASSERT_OK(runtime.Stop());
    const auto afterStop = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.5"})");
    EXPECT_EQ(afterStop.GetCode(), K_SHUTTING_DOWN) << afterStop.ToString();
    EXPECT_DOUBLE_EQ(FLAGS_diagnostic_sample_rate, 0.75);
}

TEST(CoordinatorServerOptionsTest, UpdateConfigRejectsFlagsWithoutCompleteRuntimeEffect)
{
    ScopedDynamicConfigTestState testState;
    const auto originalNodeDeadTimeout = FLAGS_node_dead_timeout_s;
    FLAGS_node_dead_timeout_s = 300;
    CoordinatorRuntime runtime;
    runtime.EnableConfigUpdates();

    const auto status = runtime.UpdateConfig(R"({"node_dead_timeout_s":"600"})");
    EXPECT_EQ(status.GetCode(), K_INVALID) << status.ToString();
    EXPECT_NE(status.GetMsg().find("not runtime-applicable"), std::string::npos) << status.ToString();
    EXPECT_EQ(FLAGS_node_dead_timeout_s, 300u);

    FLAGS_node_dead_timeout_s = originalNodeDeadTimeout;
}

TEST(CoordinatorServerOptionsTest, ServerUpdateConfigDelegatesToOwnedRuntime)
{
    ScopedDynamicConfigTestState testState;
    auto runtime = std::make_unique<CoordinatorRuntime>();
    runtime->EnableConfigUpdates();
    ScopedCoordinatorServerRuntimeOverride runtimeOverride(std::move(runtime));

    DS_ASSERT_OK(CoordinatorServer::GetInstance()->UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})"));
    EXPECT_DOUBLE_EQ(FLAGS_diagnostic_sample_rate, 0.75);

    DS_ASSERT_OK(CoordinatorServer::GetInstance()->Stop());
}

TEST(CoordinatorServerOptionsTest, StopWaitsForAcceptedUpdateAndClosesAdmission)
{
    constexpr auto kWaitTimeout = std::chrono::seconds(1);
    ScopedDynamicConfigTestState testState;
    CoordinatorRuntime runtime;
    runtime.EnableConfigUpdates();
    std::promise<void> updateEntered;
    auto updateEnteredFuture = updateEntered.get_future();
    std::promise<void> releaseUpdate;
    auto releaseUpdateFuture = releaseUpdate.get_future().share();
    std::once_flag updateEnteredOnce;
    runtime.runtimeFlags_->SetBatchCommitHandler([&](const auto &) {
        std::call_once(updateEnteredOnce, [&] {
            updateEntered.set_value();
            releaseUpdateFuture.wait();
        });
        return true;
    });

    Status updateStatus;
    std::thread updateThread([&] {
        updateStatus = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.75"})");
    });
    const auto updateEnteredStatus = updateEnteredFuture.wait_for(kWaitTimeout);
    if (updateEnteredStatus != std::future_status::ready) {
        releaseUpdate.set_value();
        updateThread.join();
        FAIL() << "UpdateConfig did not enter validation before the timeout";
        return;
    }
    std::promise<void> stopStarted;
    auto stopStartedFuture = stopStarted.get_future();
    auto stopFuture = std::async(std::launch::async, [&] {
        stopStarted.set_value();
        return runtime.Stop();
    });
    ASSERT_EQ(stopStartedFuture.wait_for(kWaitTimeout), std::future_status::ready);
    EXPECT_EQ(stopFuture.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    releaseUpdate.set_value();
    updateThread.join();
    EXPECT_TRUE(updateStatus.IsOk()) << updateStatus.ToString();
    EXPECT_TRUE(stopFuture.get().IsOk());
    const auto rejectedStatus = runtime.UpdateConfig(R"({"diagnostic_sample_rate":"0.5"})");
    EXPECT_EQ(rejectedStatus.GetCode(), K_SHUTTING_DOWN) << rejectedStatus.ToString();
}

TEST(CoordinatorServerOptionsTest, LifecycleRejectionsAreAuditedWithoutConfigPayload)
{
    ScopedTempDirectory tempDirectory;
    const auto originalLogDir = FLAGS_log_dir;
    const auto originalLogFilename = FLAGS_log_filename;
    const auto originalLogAsync = FLAGS_log_async;
    OperationLogger::Instance().Shutdown();
    FLAGS_log_dir = tempDirectory.Child("logs");
    std::filesystem::create_directories(FLAGS_log_dir);
    FLAGS_log_filename = "coordinator_lifecycle_audit";
    FLAGS_log_async = false;
    EXPECT_TRUE(OperationLogger::Instance().Init("coordinator"));
    CoordinatorRuntime runtime;
    constexpr const char *kPayloadMarker = "payload-must-not-be-audited";

    const auto beforeStart = runtime.UpdateConfig(
        std::string(R"({"diagnostic_sample_rate":")") + kPayloadMarker + R"("})");
    DS_ASSERT_OK(runtime.Stop());
    const auto afterStop = runtime.UpdateConfig(
        std::string(R"({"diagnostic_sample_rate":")") + kPayloadMarker + R"("})");
    OperationLogger::Instance().Shutdown();

    EXPECT_EQ(beforeStart.GetCode(), K_NOT_READY) << beforeStart.ToString();
    EXPECT_EQ(afterStop.GetCode(), K_SHUTTING_DOWN) << afterStop.ToString();
    std::ifstream operationLog(tempDirectory.Child("logs/coordinator_lifecycle_audit_operation.log"));
    const std::string content((std::istreambuf_iterator<char>(operationLog)), std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("CONFIG_FAILED: UpdateConfig Coordinator UpdateConfig: runtime is not ready"),
              std::string::npos);
    EXPECT_NE(content.find("CONFIG_FAILED: UpdateConfig Coordinator UpdateConfig: runtime is stopping"),
              std::string::npos);
    EXPECT_EQ(content.find(kPayloadMarker), std::string::npos);

    FLAGS_log_dir = originalLogDir;
    FLAGS_log_filename = originalLogFilename;
    FLAGS_log_async = originalLogAsync;
}

TEST_F(CoordinatorElectionServiceTest, ElectionInputsAndZmqFailBeforeNetwork)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    FLAGS_use_brpc = false;
    auto service = MakeService(discovery, kCoordinatorCount);

    const auto initStatus = service->Init();
    ExpectInvalidWithMessage(initStatus, "use_brpc=true");
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->coordinatorDiscovery_, discovery);
    EXPECT_EQ(service->expectedMemberCount_, kCoordinatorCount);
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_EQ(service->rpcServer_, nullptr);
    EXPECT_EQ(service->brpcAdapter_, nullptr);
    EXPECT_EQ(discovery->calls_.load(), 0U);
    EXPECT_EQ(service->topologyRecoveryManager_, nullptr);
    EXPECT_EQ(service->store_, nullptr);
    DS_ASSERT_OK(service->Shutdown());
    DS_ASSERT_OK(service->Shutdown());

    FLAGS_use_brpc = true;
    auto legacyService = MakeService(nullptr, 0);
    DS_ASSERT_OK(legacyService->Init());
    EXPECT_EQ(legacyService->coordinatorDiscovery_, nullptr);
    EXPECT_EQ(legacyService->expectedMemberCount_, 0U);
    EXPECT_FALSE(legacyService->IsElectionConfigured());
    DS_ASSERT_OK(legacyService->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, BuildElectionContextOnlyCopiesImmutableManagerInput)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_[2], peers_[0], peers_[1] };
    auto service = MakeService(discovery, kCoordinatorCount);
    service->raftFlags_.dataDir = tempDirectory_->Child("non-directory-manager-input");
    CreateNonEmptyFile(service->raftFlags_.dataDir, "Manager owns metadata probing");
    coordinator::CoordinatorElectionOptions options;

    DS_ASSERT_OK(service->BuildElectionStartupContext(options));

    EXPECT_EQ(options.raftFlags.localAddress, service->raftFlags_.localAddress);
    EXPECT_EQ(options.raftFlags.dataDir, service->raftFlags_.dataDir);
    EXPECT_EQ(options.raftFlags.electionTimeoutMs, service->raftFlags_.electionTimeoutMs);
    EXPECT_EQ(options.raftFlags.healthCheckIntervalMs, service->raftFlags_.healthCheckIntervalMs);
    EXPECT_EQ(options.raftFlags.memberFailureGraceMs, service->raftFlags_.memberFailureGraceMs);
    EXPECT_EQ(options.raftFlags.discoveryRetryIntervalMs, service->raftFlags_.discoveryRetryIntervalMs);
    EXPECT_EQ(options.raftFlags.bootstrapWarningIntervalMs, service->raftFlags_.bootstrapWarningIntervalMs);
    EXPECT_EQ(options.membershipOptions.expectedMemberCount, kCoordinatorCount);
    EXPECT_EQ(options.membershipOptions.healthCheckInterval, std::chrono::milliseconds(kHealthIntervalMs));
    EXPECT_EQ(options.membershipOptions.memberFailureGrace, std::chrono::milliseconds(kFailureGraceMs));
    EXPECT_EQ(options.membershipOptions.discoveryRetryInterval, std::chrono::milliseconds(kDiscoveryRetryMs));
    EXPECT_EQ(discovery->calls_.load(), 0U);
}

TEST_F(CoordinatorElectionServiceTest, BootstrapRpcValidatesGroupAndForwardsPublishedManagerSnapshot)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    auto service = MakeService(discovery, kCoordinatorCount);
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;

    const auto unpublishedStatus = service->GetRaftBootstrapState(request, response);
    EXPECT_EQ(unpublishedStatus.GetCode(), K_NOT_READY) << unpublishedStatus.ToString();

    coordinator::CoordinatorElectionOptions options;
    DS_ASSERT_OK(service->BuildElectionStartupContext(options));
    service->electionManager_ = std::make_unique<coordinator::CoordinatorElectionManager>(
        std::move(options), service->BuildRaftEventCallbacks(), discovery);

    DS_ASSERT_OK(service->GetRaftBootstrapState(request, response));
    EXPECT_FALSE(response.probe_ready());
    EXPECT_EQ(response.group_id(), coordinator::kCoordinatorRaftGroupId);
    EXPECT_EQ(response.local_peer(), peers_.front());
    EXPECT_EQ(response.expected_member_count(), kCoordinatorCount);
    EXPECT_EQ(response.metadata_state(), coordinator::RAFT_METADATA_UNKNOWN);
    EXPECT_EQ(response.candidate_count(), 0U);
    EXPECT_TRUE(response.candidate_digest().empty());
    EXPECT_EQ(response.committed_peers_size(), 0);
    EXPECT_EQ(response.phase(), coordinator::RAFT_BOOTSTRAP_OBSERVING);
    EXPECT_EQ(response.status_code(), static_cast<int32_t>(K_OK));
    EXPECT_EQ(discovery->calls_.load(), 0U);

    request.set_group_id("wrong-coordinator-raft-group");
    const auto wrongGroupStatus = service->GetRaftBootstrapState(request, response);
    EXPECT_EQ(wrongGroupStatus.GetCode(), K_INVALID) << wrongGroupStatus.ToString();
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, BootstrapHandlerReportsSanitizedTerminalPhaseForCorruptLocalMetadata)
{
    constexpr auto kTerminalDeadline = std::chrono::seconds(1);
    constexpr auto kPollInterval = std::chrono::milliseconds(10);
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    auto flags = raftFlags_;
    const auto corruptDataDir = tempDirectory_->Child("corrupt-raft-data");
    flags.dataDir = corruptDataDir;
    CreateNonEmptyFile(corruptDataDir, "not a raft data directory");
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    DS_ASSERT_OK(service->StartElectionManager());

    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;
    Status queryStatus;
    bool observedTerminal = false;
    const auto deadline = std::chrono::steady_clock::now() + kTerminalDeadline;
    while (std::chrono::steady_clock::now() < deadline) {
        queryStatus = service->GetRaftBootstrapState(request, response);
        if (queryStatus.IsOk() && response.phase() == coordinator::RAFT_BOOTSTRAP_TERMINAL) {
            observedTerminal = true;
            break;
        }
        std::this_thread::sleep_for(kPollInterval);
    }

    ASSERT_TRUE(observedTerminal) << queryStatus.ToString();
    EXPECT_EQ(response.status_code(), static_cast<int32_t>(K_DATA_INCONSISTENCY));
    EXPECT_EQ(response.GetDescriptor()->FindFieldByName("data_dir"), nullptr);
    EXPECT_EQ(response.GetDescriptor()->FindFieldByName("status_message"), nullptr);
    EXPECT_EQ(response.SerializeAsString().find(corruptDataDir), std::string::npos);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, PublishedManagerSnapshotIsReadableBeforeBootstrapWorkerStarts)
{
    constexpr auto kLifecycleDeadline = std::chrono::seconds(1);
    const auto deadline = std::chrono::steady_clock::now() + kLifecycleDeadline;
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-published-manager-ut";
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());

    std::promise<void> managerPublishedPromise;
    auto managerPublishedFuture = managerPublishedPromise.get_future();
    std::promise<void> releaseManagerStartPromise;
    auto releaseManagerStartFuture = releaseManagerStartPromise.get_future().share();
    service->electionManagerPublishedHook_ = [&] {
        managerPublishedPromise.set_value();
        (void)releaseManagerStartFuture.wait_until(deadline);
    };
    Status startStatus;
    std::thread startThread([&] { startStatus = service->StartElectionManager(); });

    const auto managerPublished = managerPublishedFuture.wait_until(deadline);
    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;
    const auto stateWhilePublished = service->servingState_.load(std::memory_order_acquire);
    const auto bootstrapStatus = service->GetRaftBootstrapState(request, response);
    const auto discoveryCallsBeforeWorkerStart = discovery->calls_.load();

    releaseManagerStartPromise.set_value();
    startThread.join();
    service->electionManagerPublishedHook_ = {};

    EXPECT_EQ(managerPublished, std::future_status::ready);
    EXPECT_EQ(stateWhilePublished, coordinator::CoordinatorServiceImpl::ServingState::STARTING);
    EXPECT_TRUE(bootstrapStatus.IsOk()) << bootstrapStatus.ToString();
    EXPECT_FALSE(response.probe_ready());
    EXPECT_EQ(response.group_id(), coordinator::kCoordinatorRaftGroupId);
    EXPECT_EQ(response.local_peer(), peers_.front());
    EXPECT_EQ(response.phase(), coordinator::RAFT_BOOTSTRAP_OBSERVING);
    EXPECT_EQ(response.status_code(), static_cast<int32_t>(K_OK));
    EXPECT_EQ(discoveryCallsBeforeWorkerStart, 0U);
    EXPECT_TRUE(startStatus.IsOk()) << startStatus.ToString();
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, BootstrapSnapshotRemainsSafeAfterConcurrentManagerShutdown)
{
    constexpr auto kLifecycleDeadline = std::chrono::seconds(1);
    const auto deadline = std::chrono::steady_clock::now() + kLifecycleDeadline;
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-bootstrap-rpc-shutdown-ut";
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    DS_ASSERT_OK(service->StartElectionManager());

    std::promise<void> snapshotCopiedPromise;
    auto snapshotCopiedFuture = snapshotCopiedPromise.get_future();
    std::promise<void> releaseSnapshotPromise;
    auto releaseSnapshotFuture = releaseSnapshotPromise.get_future().share();
    service->raftBootstrapSnapshotCopiedHook_ = [&] {
        snapshotCopiedPromise.set_value();
        (void)releaseSnapshotFuture.wait_until(deadline);
    };

    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;
    Status bootstrapStatus;
    std::thread bootstrapThread([&] { bootstrapStatus = service->GetRaftBootstrapState(request, response); });
    const auto snapshotCopied = snapshotCopiedFuture.wait_until(deadline);

    std::promise<Status> shutdownPromise;
    auto shutdownFuture = shutdownPromise.get_future();
    std::thread shutdownThread([&] { shutdownPromise.set_value(service->Shutdown()); });
    const auto shutdownCompletedBeforeResponseFormatting = shutdownFuture.wait_until(deadline);

    releaseSnapshotPromise.set_value();
    bootstrapThread.join();
    shutdownThread.join();
    const auto shutdownStatus = shutdownFuture.get();
    service->raftBootstrapSnapshotCopiedHook_ = {};

    EXPECT_EQ(snapshotCopied, std::future_status::ready);
    EXPECT_EQ(shutdownCompletedBeforeResponseFormatting, std::future_status::ready);
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
    EXPECT_TRUE(bootstrapStatus.IsOk()) << bootstrapStatus.ToString();
    EXPECT_EQ(response.group_id(), coordinator::kCoordinatorRaftGroupId);
    EXPECT_EQ(response.local_peer(), coordinatorAddress_);
    EXPECT_EQ(service->electionManager_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, AcceptedBootstrapRpcDoesNotDeadlockBrpcJoinDuringShutdown)
{
    constexpr auto kLifecycleTimeout = std::chrono::seconds(2);
    constexpr auto kRpcTimeoutMs = 4'000;
    constexpr auto kShutdownIsolationWindow = std::chrono::milliseconds(50);
    const auto deadline = std::chrono::steady_clock::now() + kLifecycleTimeout;
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-brpc-join-ut";
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    DS_ASSERT_OK(service->StartElectionManager());

    BrpcChannelConfig channelConfig;
    channelConfig.endpoint = coordinatorAddress_;
    channelConfig.timeout_ms = kRpcTimeoutMs;
    channelConfig.connect_timeout_ms = kRpcTimeoutMs;
    channelConfig.max_retry = 0;
    channelConfig.enable_circuit_breaker = false;
    auto channel = BrpcChannelFactory::Create(channelConfig);
    ASSERT_NE(channel, nullptr);

    std::promise<void> handlerEnteredPromise;
    auto handlerEnteredFuture = handlerEnteredPromise.get_future();
    std::promise<void> releaseHandlerPromise;
    auto releaseHandlerFuture = releaseHandlerPromise.get_future().share();
    service->raftBootstrapHandlerEnteredHook_ = [&] {
        handlerEnteredPromise.set_value();
        (void)releaseHandlerFuture.wait_until(deadline);
    };
    std::promise<void> serverShutdownEnteredPromise;
    auto serverShutdownEnteredFuture = serverShutdownEnteredPromise.get_future();
    service->rpcServerShutdownHook_ = [&] { serverShutdownEnteredPromise.set_value(); };

    Status rpcStatus;
    std::thread rpcThread([&] {
        coordinator::CoordinatorService_BrpcGenericStub stub(channel.get(), kRpcTimeoutMs);
        coordinator::GetRaftBootstrapStateReqPb request;
        request.set_group_id(coordinator::kCoordinatorRaftGroupId);
        coordinator::GetRaftBootstrapStateRspPb response;
        rpcStatus = stub.GetRaftBootstrapState(request, response);
    });
    const auto handlerEntered = handlerEnteredFuture.wait_until(deadline);

    std::promise<Status> shutdownPromise;
    auto shutdownFuture = shutdownPromise.get_future();
    std::thread shutdownThread([&] { shutdownPromise.set_value(service->Shutdown()); });
    const auto serverShutdownEntered = serverShutdownEnteredFuture.wait_until(deadline);
    const auto shutdownBeforeHandlerRelease = shutdownFuture.wait_for(kShutdownIsolationWindow);

    releaseHandlerPromise.set_value();
    rpcThread.join();
    shutdownThread.join();
    const auto shutdownStatus = shutdownFuture.get();
    service->raftBootstrapHandlerEnteredHook_ = {};
    service->rpcServerShutdownHook_ = {};

    EXPECT_EQ(handlerEntered, std::future_status::ready);
    EXPECT_EQ(serverShutdownEntered, std::future_status::ready);
    EXPECT_EQ(shutdownBeforeHandlerRelease, std::future_status::timeout);
    EXPECT_EQ(rpcStatus.GetCode(), K_SHUTTING_DOWN) << rpcStatus.ToString();
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->rpcServer_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, RaftLifecycleCallbacksDriveRecoveryGateAndRevokeServing)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    auto service = MakeService(discovery, kCoordinatorCount);
    DS_ASSERT_OK(service->Init());
    service->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::FOLLOWER_SERVING,
                                 std::memory_order_release);
    auto callbacks = service->BuildRaftEventCallbacks();

    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);
    callbacks.onLeaderStart(1);
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING);
    // An empty recovery round has no pending work, so it must not wait for node_dead_timeout_s.
    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);
    // The callback transition alone cannot grant business serving: the Raft manager must also report local leadership.
    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);
    callbacks.onLeaderStop(Status::OK());
    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);
    callbacks.onLeaderStart(2);
    callbacks.onError(Status(K_RUNTIME_ERROR, "injected Raft error"));
    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);
    callbacks.onLeaderStart(3);
    callbacks.onShutdown();
    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);

    auto legacyService = MakeService(nullptr, 0);
    legacyService->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING,
                                       std::memory_order_release);
    DS_ASSERT_OK(legacyService->CheckServing());
    DS_ASSERT_OK(service->Shutdown());
    DS_ASSERT_OK(legacyService->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, ServingGateRejectsStaleRaftLeadershipBeforeStopCallback)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    auto service = MakeService(discovery, kCoordinatorCount);
    DS_ASSERT_OK(service->Init());

    coordinator::CoordinatorElectionOptions options;
    DS_ASSERT_OK(service->BuildElectionStartupContext(options));
    service->electionManager_ = std::make_unique<coordinator::CoordinatorElectionManager>(
        std::move(options), service->BuildRaftEventCallbacks(), discovery);
    service->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING,
                                 std::memory_order_release);

    EXPECT_EQ(service->CheckServing().GetCode(), K_NOT_READY);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, SynchronousManagerStartFailureDetachesPublishedManager)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    auto service = MakeService(discovery, kCoordinatorCount);
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    service->raftFlags_.dataDir.clear();

    const auto startStatus = service->StartElectionManager();

    EXPECT_EQ(startStatus.GetCode(), K_INVALID) << startStatus.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STARTING);
    EXPECT_TRUE(service->electionStartAttempted_);
    EXPECT_FALSE(service->electionStartInProgress_);
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_NE(service->rpcServer_, nullptr);
    EXPECT_EQ(discovery->calls_.load(), 0U);

    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;
    EXPECT_EQ(service->GetRaftBootstrapState(request, response).GetCode(), K_NOT_READY);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, StartFailureShutsDownConstructedComponentsWithoutRunningRaft)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = peers_;
    auto service = MakeService(discovery, kCoordinatorCount);
    DS_ASSERT_OK(service->Init());
    service->builder_.SetPreStartCallback([] { return Status(K_RUNTIME_ERROR, "injected pre-start failure"); });

    const auto status = service->Start();

    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR) << status.ToString();
    EXPECT_NE(status.ToString().find("injected pre-start failure"), std::string::npos) << status.ToString();
    EXPECT_EQ(discovery->calls_.load(), 0U);
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_EQ(service->rpcServer_, nullptr);
    EXPECT_EQ(service->brpcAdapter_, nullptr);
    EXPECT_EQ(service->topologyRecoveryManager_, nullptr);
    EXPECT_EQ(service->store_, nullptr);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, SingleExpectedMemberKeepsElectionDisabled)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto service = MakeService(discovery, 1);

    EXPECT_FALSE(service->IsElectionConfigured());
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING);
    DS_ASSERT_OK(service->StartElectionManager());
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_FALSE(service->IsLeader());
    std::string leader = "stale";
    EXPECT_EQ(service->GetLeader(leader).GetCode(), K_INVALID);
    EXPECT_TRUE(leader.empty());
    EXPECT_EQ(discovery->calls_.load(), 0U);
    DS_ASSERT_OK(service->CheckServing());

    coordinator::GetRaftBootstrapStateReqPb request;
    request.set_group_id(coordinator::kCoordinatorRaftGroupId);
    coordinator::GetRaftBootstrapStateRspPb response;
    EXPECT_EQ(service->GetRaftBootstrapState(request, response).GetCode(), K_INVALID);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, ElectionStartupIsSplitAndBusinessRpcsRemainGated)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto service = MakeService(discovery, kCoordinatorCount);

    EXPECT_EQ(service->StartElectionManager().GetCode(), K_NOT_READY);
    DS_ASSERT_OK(service->Init());
    EXPECT_EQ(service->StartElectionManager().GetCode(), K_NOT_READY);
    DS_ASSERT_OK(service->Start());
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STARTING);
    EXPECT_EQ(discovery->calls_.load(), 0U);
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_FALSE(service->IsLeader());
    std::string leader = "stale";
    EXPECT_EQ(service->GetLeader(leader).GetCode(), K_NOT_READY);
    EXPECT_TRUE(leader.empty());
    ExpectAllBusinessRpcsReturn(*service, K_NOT_READY, "starting");

    DS_ASSERT_OK(service->StartElectionManager());
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::FOLLOWER_SERVING);
    EXPECT_NE(service->electionManager_, nullptr);
    EXPECT_EQ(service->StartElectionManager().GetCode(), K_INVALID);
    EXPECT_EQ(service->Start().GetCode(), K_INVALID);
    EXPECT_NE(service->rpcServer_, nullptr);
    ExpectAllBusinessRpcsReturn(*service, K_NOT_READY, "not the active Leader");
    DS_ASSERT_OK(service->Shutdown());
    EXPECT_EQ(service->StartElectionManager().GetCode(), K_SHUTTING_DOWN);
    EXPECT_EQ(service->GetLeader(leader).GetCode(), K_SHUTTING_DOWN);
}

TEST_F(CoordinatorElectionServiceTest, InvalidLocalAddressSnapshotFailsInitWithoutDiscovery)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    auto flags = raftFlags_;
    flags.localAddress = peers_[1];
    coordinator::CoordinatorServiceImpl service(HostPort(kLoopbackIp, portLeases_.front().Port()), discovery,
                                                kCoordinatorCount, std::move(flags));

    const auto status = service.Init();

    ExpectInvalidWithMessage(status, "localAddress");
    EXPECT_EQ(discovery->calls_.load(), 0U);
    EXPECT_EQ(service.servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service.rpcServer_, nullptr);
    EXPECT_EQ(service.electionManager_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, BackgroundBootstrapFailureKeepsRpcPublishedAndBusinessGateClosed)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-ut";
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    ASSERT_EQ(discovery->calls_.load(), 0U);

    const auto status = service->StartElectionManager();

    EXPECT_TRUE(status.IsOk()) << status.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::FOLLOWER_SERVING);
    EXPECT_TRUE(service->electionStartAttempted_);
    EXPECT_FALSE(service->electionStartInProgress_);
    EXPECT_NE(service->electionManager_, nullptr);
    EXPECT_NE(service->rpcServer_, nullptr);
    EXPECT_NE(service->brpcAdapter_, nullptr);
    EXPECT_NE(service->topologyRecoveryManager_, nullptr);
    EXPECT_NE(service->store_, nullptr);
    ExpectAllBusinessRpcsReturn(*service, K_NOT_READY, "not the active Leader");
    EXPECT_EQ(service->StartElectionManager().GetCode(), K_INVALID);

    DS_ASSERT_OK(service->Shutdown());
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_EQ(service->rpcServer_, nullptr);
    EXPECT_EQ(service->brpcAdapter_, nullptr);
    EXPECT_EQ(service->topologyRecoveryManager_, nullptr);
    EXPECT_EQ(service->store_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, PublicShutdownDrainsManagerWithoutLifecycleLockAndSharesResult)
{
    constexpr auto kShutdownConcurrencyTimeout = std::chrono::seconds(1);
    constexpr auto kConcurrentShutdownIsolationWindow = std::chrono::milliseconds(50);
    const auto shutdownConcurrencyDeadline = std::chrono::steady_clock::now() + kShutdownConcurrencyTimeout;
    const Status injectedManagerCleanupError(K_RUNTIME_ERROR, "injected Manager cleanup failure");
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-shutdown-owner-ut";
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    DS_ASSERT_OK(service->StartElectionManager());
    ASSERT_NE(service->electionManager_, nullptr);
    ASSERT_NE(service->rpcServer_, nullptr);
    auto callbacks = service->BuildRaftEventCallbacks();
    callbacks.onLeaderStart(1);
    ASSERT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::LEADER_SERVING);

    std::promise<void> managerDrainEnteredPromise;
    auto managerDrainEnteredFuture = managerDrainEnteredPromise.get_future();
    std::atomic<bool> managerDrainEnteredSignaled{ false };
    std::promise<void> releaseManagerDrainPromise;
    auto releaseManagerDrainFuture = releaseManagerDrainPromise.get_future().share();
    std::atomic<bool> managerDrainReleased{ false };
    std::atomic<size_t> managerCleanupCalls{ 0 };
    std::atomic<bool> servingGateClosedBeforeManagerDrain{ false };
    service->electionManagerShutdownHook_ = [&] {
        managerCleanupCalls.fetch_add(1);
        servingGateClosedBeforeManagerDrain.store(
            service->servingState_.load(std::memory_order_acquire)
            == coordinator::CoordinatorServiceImpl::ServingState::STOPPING);
        if (!managerDrainEnteredSignaled.exchange(true)) {
            managerDrainEnteredPromise.set_value();
        }
        if (releaseManagerDrainFuture.wait_until(shutdownConcurrencyDeadline) != std::future_status::ready) {
            return Status(K_RUNTIME_ERROR, "Timed out waiting to release Manager shutdown drain");
        }
        return injectedManagerCleanupError;
    };

    std::promise<Status> firstShutdownPromise;
    auto firstShutdownFuture = firstShutdownPromise.get_future();
    std::promise<void> secondShutdownInvokedPromise;
    auto secondShutdownInvokedFuture = secondShutdownInvokedPromise.get_future();
    std::promise<Status> secondShutdownPromise;
    auto secondShutdownFuture = secondShutdownPromise.get_future();
    std::thread firstShutdownThread;
    std::thread secondShutdownThread;
    struct ShutdownCleanup {
        std::promise<void> &releaseManagerDrainPromise;
        std::atomic<bool> &managerDrainReleased;
        std::thread &firstShutdownThread;
        std::thread &secondShutdownThread;
        coordinator::CoordinatorServiceImpl *service;
        bool completed{ false };

        void Run() noexcept
        {
            if (completed) {
                return;
            }
            completed = true;
            if (!managerDrainReleased.exchange(true)) {
                try {
                    releaseManagerDrainPromise.set_value();
                } catch (...) {
                }
            }
            if (firstShutdownThread.joinable()) {
                firstShutdownThread.join();
            }
            if (secondShutdownThread.joinable()) {
                secondShutdownThread.join();
            }
            service->electionManagerShutdownHook_ = {};
        }

        ~ShutdownCleanup()
        {
            Run();
        }
    } cleanup{ releaseManagerDrainPromise, managerDrainReleased, firstShutdownThread, secondShutdownThread, service };

    firstShutdownThread = std::thread([&] { firstShutdownPromise.set_value(service->Shutdown()); });
    const auto managerDrainEnteredWait = managerDrainEnteredFuture.wait_until(shutdownConcurrencyDeadline);

    std::string leader = "stale";
    Status leaderStatusDuringManagerDrain;
    bool managerOwnershipTransferred = false;
    bool serverPresentDuringManagerDrain = false;
    auto secondShutdownInvokedWait = std::future_status::deferred;
    auto secondShutdownBeforeManagerDrain = std::future_status::deferred;
    bool serverPresentWhileSecondShutdownWaits = false;
    if (managerDrainEnteredWait == std::future_status::ready) {
        leaderStatusDuringManagerDrain = service->GetLeader(leader);
        managerOwnershipTransferred = service->electionManager_ == nullptr;
        serverPresentDuringManagerDrain = service->rpcServer_ != nullptr;

        secondShutdownThread = std::thread([&] {
            secondShutdownInvokedPromise.set_value();
            secondShutdownPromise.set_value(service->Shutdown());
        });
        secondShutdownInvokedWait = secondShutdownInvokedFuture.wait_until(shutdownConcurrencyDeadline);
        if (secondShutdownInvokedWait == std::future_status::ready) {
            secondShutdownBeforeManagerDrain = secondShutdownFuture.wait_for(kConcurrentShutdownIsolationWindow);
            serverPresentWhileSecondShutdownWaits = service->rpcServer_ != nullptr;
        }
    }

    cleanup.Run();
    const auto firstShutdownStatus = firstShutdownFuture.get();

    EXPECT_EQ(managerDrainEnteredWait, std::future_status::ready)
        << "Timed out waiting for Manager shutdown drain hook to enter before the shared deadline";
    if (managerDrainEnteredWait == std::future_status::ready) {
        const auto secondShutdownStatus = secondShutdownFuture.get();
        EXPECT_EQ(secondShutdownInvokedWait, std::future_status::ready)
            << "Timed out waiting for the second Shutdown thread to start before the shared deadline";
        EXPECT_EQ(leaderStatusDuringManagerDrain.GetCode(), K_SHUTTING_DOWN)
            << leaderStatusDuringManagerDrain.ToString();
        EXPECT_TRUE(leader.empty());
        EXPECT_TRUE(managerOwnershipTransferred);
        EXPECT_TRUE(serverPresentDuringManagerDrain);
        if (secondShutdownInvokedWait == std::future_status::ready) {
            EXPECT_EQ(secondShutdownBeforeManagerDrain, std::future_status::timeout);
            EXPECT_TRUE(serverPresentWhileSecondShutdownWaits);
        }
        EXPECT_EQ(secondShutdownStatus.GetCode(), firstShutdownStatus.GetCode()) << secondShutdownStatus.ToString();
        EXPECT_EQ(secondShutdownStatus.GetMsg(), firstShutdownStatus.GetMsg());
    }
    EXPECT_EQ(managerCleanupCalls.load(), 1U);
    EXPECT_TRUE(servingGateClosedBeforeManagerDrain.load());
    EXPECT_EQ(firstShutdownStatus.GetCode(), injectedManagerCleanupError.GetCode()) << firstShutdownStatus.ToString();
    EXPECT_EQ(firstShutdownStatus.GetMsg(), injectedManagerCleanupError.GetMsg());
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->rpcServer_, nullptr);
    EXPECT_EQ(service->brpcAdapter_, nullptr);
    EXPECT_EQ(service->topologyRecoveryManager_, nullptr);
    EXPECT_EQ(service->store_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, DiscoveryReentrantLeaderQueryAndConcurrentShutdownDoNotDeadlock)
{
    constexpr auto kReentrantQueryTimeout = std::chrono::seconds(1);
    constexpr auto kShutdownIsolationWindow = std::chrono::milliseconds(50);
    auto discovery = std::make_shared<BlockingReentrantCoordinatorDiscovery>(peers_.front());
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-reentrant-ut";
    auto *service = MakeService(discovery, kCoordinatorCount, std::move(flags));
    discovery->BindService(service);
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    std::promise<Status> startPromise;
    auto startFuture = startPromise.get_future();
    std::thread startThread([&] { startPromise.set_value(service->StartElectionManager()); });

    const bool reentrantQueryCompleted = discovery->WaitForReentrantQuery(kReentrantQueryTimeout);
    std::promise<void> shutdownInvokedPromise;
    auto shutdownInvokedFuture = shutdownInvokedPromise.get_future();
    std::promise<Status> shutdownPromise;
    auto shutdownFuture = shutdownPromise.get_future();
    std::thread shutdownThread([&] {
        shutdownInvokedPromise.set_value();
        shutdownPromise.set_value(service->Shutdown());
    });
    shutdownInvokedFuture.wait();
    const auto shutdownBeforeRelease = shutdownFuture.wait_for(kShutdownIsolationWindow);
    const auto reentrantStatus = discovery->ReentrantLeaderStatus();
    const bool rpcListeningUntilElectionCompletion = service->rpcServer_ != nullptr;

    discovery->Release();
    startThread.join();
    shutdownThread.join();
    const auto startStatus = startFuture.get();
    const auto shutdownStatus = shutdownFuture.get();

    EXPECT_TRUE(reentrantQueryCompleted);
    EXPECT_EQ(reentrantStatus.GetCode(), K_NOT_READY) << reentrantStatus.ToString();
    EXPECT_EQ(shutdownBeforeRelease, std::future_status::timeout);
    EXPECT_TRUE(rpcListeningUntilElectionCompletion);
    EXPECT_TRUE(startStatus.IsOk()) << startStatus.ToString();
    EXPECT_TRUE(shutdownStatus.IsOk()) << shutdownStatus.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->electionManager_, nullptr);
    EXPECT_EQ(service->rpcServer_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, RuntimeInvokesOnStopBeforeElectionServiceShutdown)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    discovery->candidates_ = { peers_.front() };
    auto flags = raftFlags_;
    flags.dataDir = "/proc/datasystem-coordinator-raft-runtime-rollback-ut";
    auto service = std::make_unique<coordinator::CoordinatorServiceImpl>(
        HostPort(kLoopbackIp, portLeases_.front().Port()), discovery, 1, std::move(flags));
    DS_ASSERT_OK(service->Init());
    DS_ASSERT_OK(service->Start());
    DS_ASSERT_OK(service->StartElectionManager());
    ASSERT_NE(service->rpcServer_, nullptr);
    auto *serviceView = service.get();
    CoordinatorRuntime runtime;
    runtime.service_ = std::move(service);
    std::vector<std::string> events;
    bool serverPresentDuringOnStop = false;
    runtime.onStop_ = [&] {
        serverPresentDuringOnStop = serviceView->rpcServer_ != nullptr;
        events.emplace_back("onStop");
        return Status::OK();
    };
    runtime.callbackState_ = CoordinatorRuntime::LifecycleCallbackState::START_ATTEMPTED;

    DS_ASSERT_OK(runtime.InvokeOnStop());
    DS_ASSERT_OK(runtime.ShutdownService());
    events.emplace_back("shutdown");

    EXPECT_TRUE(serverPresentDuringOnStop);
    EXPECT_EQ(events, (std::vector<std::string>{ "onStop", "shutdown" }));
    EXPECT_EQ(runtime.service_, nullptr);
}

TEST_F(CoordinatorElectionServiceTest, ElectionInputsMustBeDisabledOrConfiguredTogether)
{
    auto discovery = std::make_shared<ScriptedCoordinatorDiscovery>();
    coordinator::CoordinatorServiceImpl missingDiscovery(HostPort(kLoopbackIp, portLeases_.front().Port()), nullptr,
                                                         kCoordinatorCount, raftFlags_);
    coordinator::CoordinatorServiceImpl missingCount(HostPort(kLoopbackIp, portLeases_.front().Port()), discovery, 0,
                                                     raftFlags_);

    ExpectInvalidWithMessage(missingDiscovery.Init(), "configured together");
    ExpectInvalidWithMessage(missingCount.Init(), "configured together");
}

TEST_F(CoordinatorElectionServiceTest, LifecycleRejectsInvalidTransitions)
{
    auto service = MakeService(nullptr, 0);

    const auto startBeforeInit = service->Start();
    EXPECT_EQ(startBeforeInit.GetCode(), K_NOT_READY) << startBeforeInit.ToString();
    EXPECT_NE(startBeforeInit.ToString().find("initialized before"), std::string::npos) << startBeforeInit.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::CREATED);

    DS_ASSERT_OK(service->Init());
    const auto duplicateInit = service->Init();
    EXPECT_EQ(duplicateInit.GetCode(), K_INVALID) << duplicateInit.ToString();
    EXPECT_NE(duplicateInit.ToString().find("only be initialized once"), std::string::npos) << duplicateInit.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::INITIALIZED);

    DS_ASSERT_OK(service->Shutdown());
    const auto startAfterShutdown = service->Start();
    EXPECT_EQ(startAfterShutdown.GetCode(), K_SHUTTING_DOWN) << startAfterShutdown.ToString();
    EXPECT_NE(startAfterShutdown.ToString().find("after shutdown"), std::string::npos) << startAfterShutdown.ToString();
    const auto initAfterShutdown = service->Init();
    EXPECT_EQ(initAfterShutdown.GetCode(), K_SHUTTING_DOWN) << initAfterShutdown.ToString();
    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
}

TEST_F(CoordinatorElectionServiceTest, ShutdownResumesCleanupFromStoppingState)
{
    auto service = MakeService(nullptr, 0);
    DS_ASSERT_OK(service->Init());
    service->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::STOPPING,
                                 std::memory_order_release);
    ExpectAllBusinessRpcsReturn(*service, K_SHUTTING_DOWN, "shutting down");

    DS_ASSERT_OK(service->Shutdown());

    EXPECT_EQ(service->servingState_.load(std::memory_order_acquire),
              coordinator::CoordinatorServiceImpl::ServingState::STOPPED);
    EXPECT_EQ(service->topologyRecoveryManager_, nullptr);
    EXPECT_EQ(service->store_, nullptr);
    DS_ASSERT_OK(service->Shutdown());
}

TEST_F(CoordinatorElectionServiceTest, EveryRpcUsesServingStateGateFirst)
{
    auto service = MakeService(nullptr, 0);
    ExpectAllBusinessRpcsReturn(*service, K_NOT_READY, "not initialized");

    DS_ASSERT_OK(service->Init());
    ExpectAllBusinessRpcsReturn(*service, K_NOT_READY, "has not started");

    service->servingState_.store(coordinator::CoordinatorServiceImpl::ServingState::STARTING,
                                 std::memory_order_release);
    ExpectAllBusinessRpcsReturn(*service, K_NOT_READY, "starting");

    DS_ASSERT_OK(service->Shutdown());
    DS_ASSERT_OK(service->Shutdown());
    ExpectAllBusinessRpcsReturn(*service, K_SHUTTING_DOWN, "stopped");
}

}  // namespace ut
}  // namespace datasystem
