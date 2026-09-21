/*
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

/**
 * Description: ST for SDK coordinator-backed service discovery.
 */
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "cluster/external_cluster.h"
#include "oc_client_common.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/utils/service_discovery.h"

DS_DECLARE_string(log_dir);
DS_DECLARE_string(log_filename);

namespace datasystem {
namespace st {
namespace {
constexpr char COORDINATOR_SD_HOST_ID_ENV0[] = "coordinator_sd_host_id_env0";
constexpr char COORDINATOR_SD_HOST_ID_ENV1[] = "coordinator_sd_host_id_env1";
constexpr char COORDINATOR_SD_HOST_ID_ENV_MISSING[] = "coordinator_sd_host_id_env_missing";
constexpr char COORDINATOR_SD_HOST_ID_VALUE0[] = "coordinator_sd_host_id0";
constexpr char COORDINATOR_SD_HOST_ID_VALUE1[] = "coordinator_sd_host_id1";
constexpr char COORDINATOR_SD_HOST_ID_VALUE_MISSING[] = "coordinator_sd_host_id_missing";
constexpr char COORDINATOR_SD_MISSING_CLUSTER[] = "coordinator_sd_missing_cluster";
constexpr int COORDINATOR_SD_SELECT_LOOP_COUNT = 5;
constexpr int COORDINATOR_SD_EMPTY_CLUSTER_CONNECT_TIMEOUT_MS = 4000;
constexpr int COORDINATOR_SD_EMPTY_CLUSTER_RECOVERY_WAIT_S = 10;
constexpr int COORDINATOR_SD_CONNECT_TIMEOUT_MS = 60000;
constexpr int COORDINATOR_SD_BACKOFF_WINDOW_S = 20;
constexpr int COORDINATOR_SD_BACKOFF_MAX_PROBES = 6;
constexpr int COORDINATOR_SD_BACKOFF_MIN_PROBES = 3;
constexpr int32_t COORDINATOR_SD_BACKOFF_INITIAL_MS = 1000;
constexpr int32_t COORDINATOR_SD_BACKOFF_CAP_MS = 8000;
constexpr int COORDINATOR_SD_RECOVERY_WAIT_S = 25;
constexpr auto COORDINATOR_RESTART_WAIT = std::chrono::seconds(10);
constexpr auto COORDINATOR_RETRY_INTERVAL = std::chrono::milliseconds(100);

std::vector<int32_t> ExtractDiscoveryBackoffValues(const std::string &logContent, size_t offset)
{
    std::vector<int32_t> values;
    constexpr char marker[] = "Discovery unreachable, back off ";
    auto pos = logContent.find(marker, offset);
    while (pos != std::string::npos) {
        auto digitsBegin = pos + strlen(marker);
        auto digitsEnd = logContent.find("ms", digitsBegin);
        if (digitsEnd == std::string::npos) {
            break;
        }
        bool allDigits = digitsEnd > digitsBegin;
        for (auto i = digitsBegin; i < digitsEnd; ++i) {
            allDigits = allDigits && logContent[i] >= '0' && logContent[i] <= '9';
        }
        if (allDigits) {
            values.emplace_back(atoi(logContent.substr(digitsBegin, digitsEnd - digitsBegin).c_str()));
        }
        pos = logContent.find(marker, digitsEnd);
    }
    return values;
}
}  // namespace

class CoordinatorServiceDiscoveryTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numWorkers = 2;
        opts.isObjectCache = true;
        opts.enableDistributedMaster = "true";
        opts.masterIdx = 0;
        opts.waitWorkerReady = true;
        opts.waitAfterStart = false;
        opts.disableRocksDB = true;
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -node_timeout_s=2 -node_dead_timeout_s=4 -add_node_wait_time_s=1"
            " -log_async=false -enable_reconciliation=false -enable_lossless_data_exit_mode=true";
        opts.coordinatorGflagParams = " -v=1 -node_dead_timeout_s=4";

        ASSERT_EQ(setenv(COORDINATOR_SD_HOST_ID_ENV0, COORDINATOR_SD_HOST_ID_VALUE0, 1), 0);
        ASSERT_EQ(setenv(COORDINATOR_SD_HOST_ID_ENV1, COORDINATOR_SD_HOST_ID_VALUE1, 1), 0);
        ASSERT_EQ(setenv(COORDINATOR_SD_HOST_ID_ENV_MISSING, COORDINATOR_SD_HOST_ID_VALUE_MISSING, 1), 0);
        opts.workerSpecifyGflagParams[0] = FormatString("-host_id_env_name=%s", COORDINATOR_SD_HOST_ID_ENV0);
        opts.workerSpecifyGflagParams[1] = FormatString("-host_id_env_name=%s", COORDINATOR_SD_HOST_ID_ENV1);
    }

protected:
    void GetCoordinatorServiceDiscovery(const std::string &hostIdEnvName, ServiceAffinityPolicy policy,
                                        std::shared_ptr<CoordinatorServiceDiscovery> &serviceDiscovery,
                                        const std::string &clusterNameOverride = "")
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster, nullptr);
        HostPort coordinatorAddr;
        DS_ASSERT_OK(externalCluster->GetCoordinatorAddr(0, coordinatorAddr));
        CoordinatorServiceDiscoveryOptions opts;
        opts.serviceAddress = coordinatorAddr.ToString();
        opts.clusterName = clusterNameOverride.empty() ? GetTestClusterName() : clusterNameOverride;
        opts.hostIdEnvName = hostIdEnvName;
        opts.affinityPolicy = policy;
        serviceDiscovery = std::make_shared<CoordinatorServiceDiscovery>(opts);
        DS_ASSERT_OK(serviceDiscovery->Init());
        const char *expectedHostId = hostIdEnvName.empty() ? nullptr : std::getenv(hostIdEnvName.c_str());
        ASSERT_EQ(serviceDiscovery->GetHostId(), expectedHostId == nullptr ? "" : expectedHostId);
    }

    void InitKVClientWithCoordinatorServiceDiscovery(std::shared_ptr<KVClient> &client, ServiceAffinityPolicy policy,
                                                     const std::string &hostIdEnvName = "")
    {
        std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
        GetCoordinatorServiceDiscovery(hostIdEnvName, policy, serviceDiscovery);

        ConnectOptions connectOptions;
        connectOptions.connectTimeoutMs = COORDINATOR_SD_CONNECT_TIMEOUT_MS;
        connectOptions.requestTimeoutMs = 0;
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        connectOptions.serviceDiscovery = serviceDiscovery;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void AssertSelectedWorker(const std::string &hostIdEnvName, ServiceAffinityPolicy policy, int expectedWorkerIndex)
    {
        std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
        GetCoordinatorServiceDiscovery(hostIdEnvName, policy, serviceDiscovery);
        ASSERT_NE(serviceDiscovery, nullptr);
        std::string workerIp;
        int workerPort = 0;
        auto rc = serviceDiscovery->SelectWorker(workerIp, workerPort);
        if (expectedWorkerIndex < 0) {
            ASSERT_TRUE(rc.IsError()) << rc.ToString();
            return;
        }

        DS_ASSERT_OK(rc);
        HostPort expectedWorker;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(expectedWorkerIndex, expectedWorker));
        ASSERT_EQ(workerIp, expectedWorker.Host()) << ", env:" << hostIdEnvName;
        ASSERT_EQ(workerPort, expectedWorker.Port());
    }

    void AssertSelectedWorkerInCluster(const std::string &hostIdEnvName, ServiceAffinityPolicy policy)
    {
        std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
        GetCoordinatorServiceDiscovery(hostIdEnvName, policy, serviceDiscovery);
        ASSERT_NE(serviceDiscovery, nullptr);
        std::string workerIp;
        int workerPort = 0;
        DS_ASSERT_OK(serviceDiscovery->SelectWorker(workerIp, workerPort));

        AssertAddressBelongsToWorker(HostPort(workerIp, workerPort));
    }

    void AssertAddressBelongsToWorker(const HostPort &selectedWorker)
    {
        bool found = false;
        for (size_t i = 0; i < cluster_->GetWorkerNum(); ++i) {
            HostPort workerAddr;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(i, workerAddr));
            if (workerAddr == selectedWorker) {
                found = true;
                break;
            }
        }
        ASSERT_TRUE(found) << selectedWorker.ToString();
    }

    void AssertWorkerAddr(const std::string &addr, int expectedWorkerIndex)
    {
        HostPort actual;
        DS_ASSERT_OK(actual.ParseString(addr));
        HostPort expected;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(expectedWorkerIndex, expected));
        ASSERT_EQ(actual, expected);
    }
};

TEST_F(CoordinatorServiceDiscoveryTest, RandomClientCanSetGet)
{
    std::shared_ptr<KVClient> client;
    InitKVClientWithCoordinatorServiceDiscovery(client, ServiceAffinityPolicy::RANDOM);
    const std::string key = "coordinator_sd_random_key";
    const std::string value = "coordinator_sd_random_value";
    DS_ASSERT_OK(client->Set(key, value));
    std::string valueGet;
    DS_ASSERT_OK(client->Get(key, valueGet));
    ASSERT_EQ(value, valueGet);
}

TEST_F(CoordinatorServiceDiscoveryTest, RestartedCoordinatorRecoversMembershipAndRouting)
{
    std::shared_ptr<KVClient> originalClient;
    InitKVClientWithCoordinatorServiceDiscovery(originalClient, ServiceAffinityPolicy::RANDOM);
    const std::string key = "coordinator_restart_route_key";
    DS_ASSERT_OK(originalClient->Set(key, "warmup"));

    std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
    GetCoordinatorServiceDiscovery("", ServiceAffinityPolicy::RANDOM, serviceDiscovery);
    std::atomic<bool> stopTraffic{ false };
    std::atomic<size_t> trafficIterations{ 0 };
    size_t trafficFailures = 0;
    std::string firstTrafficFailure;
    std::thread traffic([&] {
        while (!stopTraffic.load(std::memory_order_acquire)) {
            const auto iteration = trafficIterations.load(std::memory_order_relaxed);
            const auto expected = "value-" + std::to_string(iteration);
            auto rc = originalClient->Set(key, expected);
            if (rc.IsError()) {
                if (trafficFailures++ == 0) {
                    firstTrafficFailure = rc.ToString();
                }
            } else {
                std::string actual;
                rc = originalClient->Get(key, actual);
                if (rc.IsError() || actual != expected) {
                    if (trafficFailures++ == 0) {
                        firstTrafficFailure = rc.IsError() ? rc.ToString() : "read-after-write value mismatch";
                    }
                }
            }
            trafficIterations.fetch_add(1, std::memory_order_release);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    const auto trafficStartDeadline = std::chrono::steady_clock::now() + COORDINATOR_RESTART_WAIT;
    while (trafficIterations.load(std::memory_order_acquire) < 10
           && std::chrono::steady_clock::now() < trafficStartDeadline) {
        std::this_thread::sleep_for(COORDINATOR_RETRY_INTERVAL);
    }
    const auto beforeRestart = trafficIterations.load(std::memory_order_acquire);
    const auto restartRc = cluster_->StartNode(COORDINATOR, 0, "");

    const auto deadline = std::chrono::steady_clock::now() + COORDINATOR_RESTART_WAIT;
    bool membershipRecovered = false;
    while (std::chrono::steady_clock::now() < deadline) {
        std::vector<std::string> sameHost;
        std::vector<std::string> other;
        auto rc = serviceDiscovery->GetAllWorkers(sameHost, other);
        membershipRecovered = rc.IsOk() && sameHost.size() + other.size() == cluster_->GetWorkerNum();
        if (membershipRecovered) {
            break;
        }
        std::this_thread::sleep_for(COORDINATOR_RETRY_INTERVAL);
    }
    while (trafficIterations.load(std::memory_order_acquire) < beforeRestart + 20
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(COORDINATOR_RETRY_INTERVAL);
    }
    stopTraffic.store(true, std::memory_order_release);
    traffic.join();

    EXPECT_GE(beforeRestart, 10U);
    DS_ASSERT_OK(restartRc);
    ASSERT_TRUE(membershipRecovered);
    EXPECT_GE(trafficIterations.load(std::memory_order_acquire), beforeRestart + 20);
    EXPECT_EQ(trafficFailures, 0U) << firstTrafficFailure;

    std::shared_ptr<KVClient> recoveredClient;
    InitKVClientWithCoordinatorServiceDiscovery(recoveredClient, ServiceAffinityPolicy::RANDOM);
    std::string actual;
    DS_ASSERT_OK(recoveredClient->Get(key, actual));
    EXPECT_FALSE(actual.empty());
}

TEST_F(CoordinatorServiceDiscoveryTest, RandomSelectsReadyWorker)
{
    AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::RANDOM);
    AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV1, ServiceAffinityPolicy::RANDOM);
    AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV_MISSING, ServiceAffinityPolicy::RANDOM);
}

TEST_F(CoordinatorServiceDiscoveryTest, EmptyClusterMembershipReturnsNotReadyOnClientInit)
{
    std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::RANDOM, serviceDiscovery,
                                   COORDINATOR_SD_MISSING_CLUSTER);

    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::seconds(COORDINATOR_SD_EMPTY_CLUSTER_RECOVERY_WAIT_S);
    Status discoveryStatus(K_NOT_READY, "Standalone Coordinator recovery has not completed");
    while (std::chrono::steady_clock::now() < deadline) {
        discoveryStatus = serviceDiscovery->GetAllWorkers(sameHost, other);
        if (discoveryStatus.IsOk()) {
            break;
        }
        ASSERT_EQ(discoveryStatus.GetCode(), K_NOT_READY) << discoveryStatus.ToString();
        std::this_thread::sleep_for(COORDINATOR_RETRY_INTERVAL);
    }
    DS_ASSERT_OK(discoveryStatus);
    EXPECT_TRUE(sameHost.empty());
    EXPECT_TRUE(other.empty());

    ConnectOptions connectOptions;
    connectOptions.connectTimeoutMs = COORDINATOR_SD_EMPTY_CLUSTER_CONNECT_TIMEOUT_MS;
    connectOptions.requestTimeoutMs = 0;
    connectOptions.serviceDiscovery = serviceDiscovery;
    KVClient client(connectOptions);

    auto rc = client.Init();
    EXPECT_EQ(rc.GetCode(), K_NOT_READY) << rc.ToString();
    EXPECT_EQ(rc.GetMsg().find("No available worker is detected"), 0UL);

    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::REQUIRED_SAME_NODE,
                                   serviceDiscovery, COORDINATOR_SD_MISSING_CLUSTER);

    std::string workerIp;
    int workerPort = 0;
    bool isNoAvailableWorker = false;
    auto selectRc = serviceDiscovery->SelectWorker(workerIp, workerPort, nullptr, &isNoAvailableWorker);
    EXPECT_EQ(selectRc.GetCode(), K_TRY_AGAIN) << selectRc.ToString();
    EXPECT_TRUE(isNoAvailableWorker);
}

TEST_F(CoordinatorServiceDiscoveryTest, RequiredSameNodeSelectsMatchingHost)
{
    for (int i = 0; i < COORDINATOR_SD_SELECT_LOOP_COUNT; ++i) {
        AssertSelectedWorker(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::REQUIRED_SAME_NODE, 0);
        AssertSelectedWorker(COORDINATOR_SD_HOST_ID_ENV1, ServiceAffinityPolicy::REQUIRED_SAME_NODE, 1);
        AssertSelectedWorker(COORDINATOR_SD_HOST_ID_ENV_MISSING, ServiceAffinityPolicy::REQUIRED_SAME_NODE, -1);
    }
}

TEST_F(CoordinatorServiceDiscoveryTest, PreferredSameNodeSelectsMatchingHost)
{
    for (int i = 0; i < COORDINATOR_SD_SELECT_LOOP_COUNT; ++i) {
        AssertSelectedWorker(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::PREFERRED_SAME_NODE, 0);
        AssertSelectedWorker(COORDINATOR_SD_HOST_ID_ENV1, ServiceAffinityPolicy::PREFERRED_SAME_NODE, 1);
        AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV_MISSING, ServiceAffinityPolicy::PREFERRED_SAME_NODE);
    }
}

TEST_F(CoordinatorServiceDiscoveryTest, GetAllWorkersAppliesAffinityPolicy)
{
    std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::PREFERRED_SAME_NODE,
                                   serviceDiscovery);
    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    DS_ASSERT_OK(serviceDiscovery->GetAllWorkers(sameHost, other));
    ASSERT_EQ(sameHost.size(), 1UL);
    ASSERT_EQ(other.size(), 1UL);
    AssertWorkerAddr(sameHost.front(), 0);
    AssertWorkerAddr(other.front(), 1);

    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::RANDOM, serviceDiscovery);
    DS_ASSERT_OK(serviceDiscovery->GetAllWorkers(sameHost, other));
    ASSERT_TRUE(sameHost.empty());
    ASSERT_EQ(other.size(), cluster_->GetWorkerNum());
    for (const auto &addr : other) {
        HostPort workerAddr;
        DS_ASSERT_OK(workerAddr.ParseString(addr));
        AssertAddressBelongsToWorker(workerAddr);
    }

    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::REQUIRED_SAME_NODE,
                                   serviceDiscovery);
    DS_ASSERT_OK(serviceDiscovery->GetAllWorkers(sameHost, other));
    ASSERT_EQ(sameHost.size(), 1UL);
    ASSERT_TRUE(other.empty());
    AssertWorkerAddr(sameHost.front(), 0);
}

TEST_F(CoordinatorServiceDiscoveryTest, SelectSameNodeWorkerRequiresMatchingHost)
{
    std::shared_ptr<CoordinatorServiceDiscovery> serviceDiscovery;
    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::PREFERRED_SAME_NODE,
                                   serviceDiscovery);
    std::string workerIp;
    int workerPort = 0;
    DS_ASSERT_OK(serviceDiscovery->SelectSameNodeWorker(workerIp, workerPort));
    HostPort expectedWorker;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, expectedWorker));
    ASSERT_EQ(HostPort(workerIp, workerPort), expectedWorker);

    GetCoordinatorServiceDiscovery(COORDINATOR_SD_HOST_ID_ENV_MISSING, ServiceAffinityPolicy::PREFERRED_SAME_NODE,
                                   serviceDiscovery);
    ASSERT_TRUE(serviceDiscovery->SelectSameNodeWorker(workerIp, workerPort).IsError());
}

// A client whose host matches no worker keeps probing same-node discovery through the
// coordinator on every heartbeat. When the coordinator is lost, those probes must back off
// exponentially instead of hammering the coordinator address every second.
TEST_F(CoordinatorServiceDiscoveryTest, CoordinatorLossBacksOffHeartbeatDiscoveryProbes)
{
    std::shared_ptr<KVClient> client;
    InitKVClientWithCoordinatorServiceDiscovery(client, ServiceAffinityPolicy::PREFERRED_SAME_NODE,
                                                COORDINATOR_SD_HOST_ID_ENV_MISSING);
    const std::string key = "coordinator_sd_backoff_key";
    const std::string value = "coordinator_sd_backoff_value";
    DS_ASSERT_OK(client->Set(key, value));

    const auto clientInfoLog = JoinPath(FLAGS_log_dir, FLAGS_log_filename + ".INFO.log");
    std::string logContent;
    DS_ASSERT_OK(ReadWholeFile(clientInfoLog, logContent));
    const auto logOffsetBeforeLoss = logContent.size();

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->ShutdownNode(COORDINATOR, 0));

    // Probe rhythm after the loss: 1s, 2s, 4s, 8s, 8s gaps. In a 20s window at most
    // COORDINATOR_SD_BACKOFF_MAX_PROBES probes may run; without the backoff every heartbeat
    // (1s) fires one. The data plane through the fallback worker must stay healthy.
    std::this_thread::sleep_for(std::chrono::seconds(COORDINATOR_SD_BACKOFF_WINDOW_S));
    std::string valueGet;
    DS_ASSERT_OK(client->Get(key, valueGet));
    ASSERT_EQ(valueGet, value);

    DS_ASSERT_OK(ReadWholeFile(clientInfoLog, logContent));
    const auto backoffValues = ExtractDiscoveryBackoffValues(logContent, logOffsetBeforeLoss);
    ASSERT_GE(backoffValues.size(), static_cast<size_t>(COORDINATOR_SD_BACKOFF_MIN_PROBES));
    ASSERT_LE(backoffValues.size(), static_cast<size_t>(COORDINATOR_SD_BACKOFF_MAX_PROBES));
    for (size_t i = 0; i < backoffValues.size(); ++i) {
        ASSERT_GE(backoffValues[i], COORDINATOR_SD_BACKOFF_INITIAL_MS);
        ASSERT_LE(backoffValues[i], COORDINATOR_SD_BACKOFF_CAP_MS);
        if (i > 0) {
            ASSERT_GE(backoffValues[i], backoffValues[i - 1]);
        }
    }

    // The regression point is the ORIGINAL backoffed client, not a fresh discovery object:
    // after the coordinator restarts, its deferred probes must resume and reopen the gate,
    // bounded by one max backoff window on top of the restart wait.
    DS_ASSERT_OK(cluster_->StartNode(COORDINATOR, 0, ""));
    constexpr char recoveryMarker[] = "Discovery recovered, backoff reset";
    const auto recoveryDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(COORDINATOR_SD_RECOVERY_WAIT_S);
    bool gateReopened = false;
    while (std::chrono::steady_clock::now() < recoveryDeadline) {
        if (ReadWholeFile(clientInfoLog, logContent).IsOk()
            && logContent.find(recoveryMarker, logOffsetBeforeLoss) != std::string::npos) {
            gateReopened = true;
            break;
        }
        std::this_thread::sleep_for(COORDINATOR_RETRY_INTERVAL);
    }
    ASSERT_TRUE(gateReopened);
    DS_ASSERT_OK(client->Set(key, "coordinator_sd_backoff_value_after_restart"));
}
}  // namespace st
}  // namespace datasystem
