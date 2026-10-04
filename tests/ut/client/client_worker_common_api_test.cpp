/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/client/worker_api/client_worker_common_api.h"

#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/client/worker_api/listen_worker.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/raii.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {

class TestClientWorkerRemoteCommonApi : public client::ClientWorkerRemoteCommonApi {
public:
    explicit TestClientWorkerRemoteCommonApi(HostPort hostPort)
        : client::IClientWorkerCommonApi(HostPort(hostPort), HeartbeatType::RPC_HEARTBEAT, false, nullptr),
          client::ClientWorkerRemoteCommonApi(std::move(hostPort))
    {
    }

    void ApplyMemoryAlignment(const RegisterClientRspPb &rsp)
    {
        UpdateMemoryAlignment(rsp);
    }

    void RunPostRegisterClient(const RegisterClientRspPb &rsp)
    {
        PostRegisterClient(1'000, rsp);
    }

    bool UbConfigWasApplied() const
    {
        return ubConfigWasApplied_;
    }

protected:
    void ApplyClientUbRegistrationConfig(const RegisterClientRspPb &rsp) override
    {
        ubConfigWasApplied_ = rsp.ub_runtime_enabled();
    }

private:
    bool ubConfigWasApplied_{ false };
};

class TransientHeartbeatFailureApi : public TestClientWorkerRemoteCommonApi {
public:
    explicit TransientHeartbeatFailureApi(HostPort hostPort)
        : client::IClientWorkerCommonApi(HostPort(hostPort), HeartbeatType::RPC_HEARTBEAT, false, nullptr),
          TestClientWorkerRemoteCommonApi(std::move(hostPort))
    {
    }

    Status SendHeartbeat(bool &, bool &, int64_t remainTime, bool &, const std::vector<int64_t> &,
                         std::vector<int64_t> &) override
    {
        attemptTimeouts_.emplace_back(remainTime);
        removableValues_.emplace_back(removable_.load(std::memory_order_relaxed));
        if (attemptTimeouts_.size() == 1) {
            return Status(K_RPC_UNAVAILABLE, "transient heartbeat failure");
        }
        return Status::OK();
    }

    std::vector<int64_t> attemptTimeouts_;
    std::vector<bool> removableValues_;
};

class RecoveredLocalWorkerApi : public TestClientWorkerRemoteCommonApi {
public:
    explicit RecoveredLocalWorkerApi(HostPort hostPort)
        : client::IClientWorkerCommonApi(HostPort(hostPort), HeartbeatType::RPC_HEARTBEAT, false, nullptr),
          TestClientWorkerRemoteCommonApi(std::move(hostPort))
    {
    }

    Status SendHeartbeat(bool &workerReboot, bool &clientRemoved, int64_t, bool &isWorkerVoluntaryScaleDown,
                         const std::vector<int64_t> &, std::vector<int64_t> &) override
    {
        workerReboot = false;
        clientRemoved = false;
        isWorkerVoluntaryScaleDown = false;
        heartbeatCalls_++;
        return Status::OK();
    }

    std::atomic<int64_t> heartbeatCalls_{ 0 };
};

TEST(ClientWorkerCommonApiTest, NotifyClientRemovableRetriesTransientHeartbeatFailure)
{
    constexpr int64_t expectedMaxAttemptTimeoutMs = 200;
    auto api = std::make_shared<TransientHeartbeatFailureApi>(HostPort("127.0.0.1", 1));
    api->clientDeadTimeoutMs_ = MIN_HEARTBEAT_TIMEOUT_MS;
    client::ListenWorker listenWorker(api, HeartbeatType::RPC_HEARTBEAT);
    int releaseFdCallbackCount = 0;
    listenWorker.SetReleaseFdCallBack(
        [&releaseFdCallbackCount](const std::vector<int64_t> &) { ++releaseFdCallbackCount; });

    ASSERT_TRUE(listenWorker.NotifyClientRemovable().IsOk());
    ASSERT_EQ(api->attemptTimeouts_.size(), 2u);
    EXPECT_LE(api->attemptTimeouts_[0], expectedMaxAttemptTimeoutMs);
    EXPECT_LE(api->attemptTimeouts_[1], expectedMaxAttemptTimeoutMs);
    EXPECT_EQ(api->removableValues_, (std::vector<bool>{ true, true }));
    EXPECT_EQ(releaseFdCallbackCount, 1);
}

TEST(ClientWorkerCommonApiTest, CheckHeartbeatLoopKeepsPacingAfterLocalWorkerRecovery)
{
    const std::string intervalInject = "ListenWorker.CheckHeartbeat.heartbeat_interval_ms";
    datasystem::inject::Set(intervalInject, "call(400)");
    Raii clearInject([intervalInject]() { (void)datasystem::inject::Clear(intervalInject); });

    auto api = std::make_shared<RecoveredLocalWorkerApi>(HostPort("127.0.0.1", 1));
    auto asyncSwitchWorkerPool = std::make_shared<ThreadPool>(0, 1);
    // Held by shared_ptr: the async recovery task captures shared_from_this().
    auto listenWorker = std::make_shared<client::ListenWorker>(api, HeartbeatType::RPC_HEARTBEAT, 1,
                                                               asyncSwitchWorkerPool.get());
    listenWorker->SetIsLocalWorker(false);
    std::atomic<bool> localWorkerRecovered{ false };
    listenWorker->SetRecoverLocalWorkerHandle([&localWorkerRecovered]() {
        localWorkerRecovered = true;
        return true;
    });
    // Keep the fallback endpoint's data plane busy so the drain keeps deferring.
    listenWorker->SetDataPlaneDrainHandle({ [](uint64_t) { return false; }, []() {} });

    api->connectTimeoutMs_ = 5000;  // StartListenWorker waits for the first heartbeat with it.
    Raii stopListener([&listenWorker]() { listenWorker->StopListenWorker(true); });
    DS_ASSERT_OK(listenWorker->StartListenWorker());

    auto recoverDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
    while (!localWorkerRecovered.load() && std::chrono::steady_clock::now() < recoverDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(localWorkerRecovered.load()) << "Recovery handle should have been invoked";

    // The recovery wake is one-shot: let the loop consume it before taking the baseline.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const int64_t baseline = api->heartbeatCalls_.load();
    constexpr int64_t maxHeartbeatsInWindow = 12;
    int64_t observed = 0;
    auto windowEnd = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
    while (std::chrono::steady_clock::now() < windowEnd) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        observed = api->heartbeatCalls_.load() - baseline;
        if (observed > maxHeartbeatsInWindow) {
            break;  // Busy spin detected: bail out early instead of flooding the log.
        }
    }
    // A 400ms heartbeat over a 2s window gives ~5 beats; only a sleepless loop exceeds this.
    EXPECT_LE(observed, maxHeartbeatsInWindow) << "Heartbeat loop must keep sleeping between beats";
    EXPECT_GE(observed, 1) << "Heartbeat loop must stay alive at heartbeat cadence";
}

TEST(ClientWorkerCommonApiTest, UsesWorkerMemoryAlignmentAndFallsBackForOldWorker)
{
    TestClientWorkerRemoteCommonApi api(HostPort("127.0.0.1", 1));
    RegisterClientRspPb rsp;

    api.ApplyMemoryAlignment(rsp);
    EXPECT_EQ(api.GetMemoryAlignment(), 64u);

    rsp.set_memory_alignment(4096);
    api.ApplyMemoryAlignment(rsp);
    EXPECT_EQ(api.GetMemoryAlignment(), 4096u);
}

TEST(ClientWorkerCommonApiTest, UsesFalseAsTheDefaultWorkerUrmaAffinityConfig)
{
    RegisterClientRspPb rsp;

    EXPECT_FALSE(rsp.ub_numa_affinity_enabled());

    rsp.set_ub_numa_affinity_enabled(true);
    EXPECT_TRUE(rsp.ub_numa_affinity_enabled());

    rsp.set_ub_numa_affinity_enabled(false);
    EXPECT_FALSE(rsp.ub_numa_affinity_enabled());
}

TEST(ClientWorkerCommonApiTest, CarriesWorkerUrmaRoundRobinType)
{
    RegisterClientRspPb rsp;

    rsp.set_ub_numa_rr_type(1);
    EXPECT_EQ(rsp.ub_numa_rr_type(), 1u);

    rsp.set_ub_numa_rr_type(2);
    EXPECT_EQ(rsp.ub_numa_rr_type(), 2u);

    rsp.set_ub_numa_rr_type(0);
    EXPECT_EQ(rsp.ub_numa_rr_type(), 0u);
}

TEST(ClientWorkerCommonApiTest, CarriesWorkerUrmaInflightDifferenceThreshold)
{
    RegisterClientRspPb rsp;

    EXPECT_EQ(rsp.ub_numa_inflight_wr_diff_threshold(), 0u);
    rsp.set_ub_numa_inflight_wr_diff_threshold(15);
    EXPECT_EQ(rsp.ub_numa_inflight_wr_diff_threshold(), 15u);
    rsp.set_ub_numa_inflight_wr_diff_threshold(0);
    EXPECT_EQ(rsp.ub_numa_inflight_wr_diff_threshold(), 0u);
}

TEST(ClientWorkerCommonApiTest, CarriesWorkerUrmaSourceChipPolicyAndDefaultsOldWorkerToRoundRobin)
{
    RegisterClientRspPb rsp;

    EXPECT_EQ(rsp.ub_numa_src_chip_policy(), 0u);
    rsp.set_ub_numa_src_chip_policy(1);
    EXPECT_EQ(rsp.ub_numa_src_chip_policy(), 1u);
    rsp.set_ub_numa_src_chip_policy(0);
    EXPECT_EQ(rsp.ub_numa_src_chip_policy(), 0u);
}

TEST(ClientWorkerCommonApiTest, SeparatesWorkerUbCapabilityFromEndpointTransportMode)
{
    RegisterClientRspPb rsp;
    EXPECT_FALSE(rsp.ub_runtime_enabled());

    rsp.set_ub_runtime_enabled(true);
    rsp.set_ub_numa_affinity_enabled(true);
    rsp.set_ub_numa_rr_type(2);
    rsp.set_ub_numa_inflight_wr_diff_threshold(15);
    rsp.set_ub_numa_src_chip_policy(1);

    // A same-host endpoint remains SHM/default while advertising the UB policy needed by cross-node connections.
    EXPECT_TRUE(rsp.ub_runtime_enabled());
    EXPECT_EQ(rsp.fast_transport_mode(), FastTransportMode::TCPIP);
    EXPECT_TRUE(rsp.ub_numa_affinity_enabled());
    EXPECT_EQ(rsp.ub_numa_rr_type(), 2u);
    EXPECT_EQ(rsp.ub_numa_inflight_wr_diff_threshold(), 15u);
    EXPECT_EQ(rsp.ub_numa_src_chip_policy(), 1u);
}

TEST(ClientWorkerCommonApiTest, AppliesUbConfigDuringPostRegisterBeforeDeferredHandshake)
{
    TestClientWorkerRemoteCommonApi api(HostPort("127.0.0.1", 1));
    RegisterClientRspPb rsp;
    rsp.set_ub_runtime_enabled(true);

    api.RunPostRegisterClient(rsp);

    EXPECT_TRUE(api.UbConfigWasApplied());
}

class InvalidWorkerMemoryAlignmentTest : public ::testing::TestWithParam<uint32_t> {
};

TEST_P(InvalidWorkerMemoryAlignmentTest, FallsBackToDefaultAlignment)
{
    TestClientWorkerRemoteCommonApi api(HostPort("127.0.0.1", 1));
    RegisterClientRspPb rsp;
    rsp.set_memory_alignment(GetParam());

    api.ApplyMemoryAlignment(rsp);

    EXPECT_EQ(api.GetMemoryAlignment(), 64u);
}

INSTANTIATE_TEST_SUITE_P(InvalidWorkerAlignments, InvalidWorkerMemoryAlignmentTest,
                         ::testing::Values(3u, 6u, 513u, 8192u));

class ClientFastTransportLocalAddrTest : public ::testing::Test {
protected:
    void TearDown() override
    {
        ASSERT_EQ(unsetenv("POD_IP"), 0);
    }
};

TEST_F(ClientFastTransportLocalAddrTest, ResolvesPodIpWhenPresent)
{
    ASSERT_EQ(setenv("POD_IP", "10.0.0.42", 1), 0);
    auto addr = GetClientFastTransportLocalAddr();
    EXPECT_EQ(addr.Host(), "10.0.0.42");
    EXPECT_EQ(addr.Port(), 0);
}

TEST_F(ClientFastTransportLocalAddrTest, EmptyWhenPodIpMissing)
{
    ASSERT_EQ(unsetenv("POD_IP"), 0);
    auto addr = GetClientFastTransportLocalAddr();
    EXPECT_TRUE(addr.Empty());
    EXPECT_EQ(addr.Port(), -1);
}

TEST_F(ClientFastTransportLocalAddrTest, EmptyWhenPodIpBlank)
{
    ASSERT_EQ(setenv("POD_IP", "", 1), 0);
    EXPECT_TRUE(GetClientFastTransportLocalAddr().Empty());
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
