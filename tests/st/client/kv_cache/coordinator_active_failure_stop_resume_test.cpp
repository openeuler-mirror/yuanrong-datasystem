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

/** Description: Measures Coordinator active isolation after a Worker process is killed. */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "cluster/topology_token_helper.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/service_discovery.h"


namespace datasystem::st {
namespace {
constexpr uint32_t WORKER_COUNT = 3;
constexpr uint32_t CLIENT_RECOVERY_GUARD_WORKER_COUNT = 4;
constexpr uint32_t TARGET_WORKER_INDEX = 1;
constexpr uint32_t MULTI_KILL_WORKER_COUNT = 12;
constexpr uint32_t SECOND_TARGET_WORKER_INDEX = 2;
constexpr std::array<uint32_t, 4> MULTI_KILL_TARGET_INDICES = { TARGET_WORKER_INDEX, SECOND_TARGET_WORKER_INDEX, 3, 4 };
constexpr std::array<uint32_t, 10> MULTI_KILL_REPORTER_INDICES = { 0, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
constexpr int32_t REQUEST_TIMEOUT_MS = 20;
constexpr int32_t CONNECT_TIMEOUT_MS = 2'000;
constexpr uint32_t ONLINE_NODE_TIMEOUT_S = 3;
constexpr uint32_t ONLINE_NODE_DEAD_TIMEOUT_S = 30;
constexpr int64_t ISOLATION_TIMEOUT_MS = 10'000;
constexpr int64_t CLIENT_RECOVERY_TIMEOUT_MS = 12'000;
constexpr int64_t MULTI_KILL_OBSERVATION_TIMEOUT_MS = 20'000;
constexpr int64_t REJOIN_TIMEOUT_MS = CLIENT_RECOVERY_TIMEOUT_MS;
// Failure qualification is sampled by the 750 ms worker heartbeat. Allow one final sampling interval plus
// scheduling jitter while keeping this gate well below the 9-second isolation SLO.
constexpr int64_t FAILURE_SAMPLING_BUDGET_MS = 1'000;
constexpr int64_t SET_RECOVERY_EXPECT_MS = ONLINE_NODE_TIMEOUT_S * 1'000 + FAILURE_SAMPLING_BUDGET_MS;
constexpr int64_t GET_RECOVERY_EXPECT_MS = ONLINE_NODE_TIMEOUT_S * 1'000 + FAILURE_SAMPLING_BUDGET_MS;
constexpr int64_t POLL_INTERVAL_MS = 50;
constexpr int64_t TRAFFIC_INTERVAL_MS = 20;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr size_t TARGET_META_KEY_COUNT = 16;
constexpr size_t MULTI_KILL_KEYS_PER_OWNER = 64;
constexpr uint64_t REQUIRED_CONSECUTIVE_SUCCESSES = 3;
constexpr char ROUTE_KEY_PREFIX[] = "coordinator_active_failure_stop_";
constexpr char CLIENT_KILL_SET_KEY_PREFIX[] = "coordinator_active_failure_kill_set_";
constexpr char CLIENT_SD_HOST_ID_ENV0[] = "coordinator_active_failure_host_id_env0";
constexpr char CLIENT_SD_HOST_ID_ENV1[] = "coordinator_active_failure_host_id_env1";
constexpr char CLIENT_SD_HOST_ID_ENV2[] = "coordinator_active_failure_host_id_env2";
constexpr char CLIENT_SD_HOST_ID_ENV3[] = "coordinator_active_failure_host_id_env3";
constexpr char CLIENT_SD_HOST_ID_VALUE0[] = "coordinator_active_failure_host0";
constexpr char CLIENT_SD_HOST_ID_VALUE1[] = "coordinator_active_failure_host1";
constexpr char CLIENT_SD_HOST_ID_VALUE2[] = "coordinator_active_failure_host2";
constexpr char CLIENT_SD_HOST_ID_VALUE3[] = "coordinator_active_failure_host3";

int64_t ElapsedMs(std::chrono::steady_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
}

struct RecoveryMeasure {
    bool recovered = false;
    int64_t elapsedMs = 0;
    uint64_t attempts = 0;
    uint64_t failures = 0;
    int64_t lastFailureElapsedMs = -1;
    int64_t noMoreFailAfterMs = -1;
    std::string lastStatus;
    std::string lastFailureStatus;
};

void LogActiveFailureMeasure(const std::string &message)
{
    LOG(INFO) << message;
    (void)Logging::WriteLogToFile(__LINE__, __FILE__, "active_failure_measure.log", 'I', message);
}

#define LOG_ACTIVE_FAILURE_MEASURE(message) \
    do {                                    \
        std::ostringstream oss;             \
        oss << message;                     \
        LogActiveFailureMeasure(oss.str()); \
    } while (false)
}  // namespace

class CoordinatorActiveFailureStopResumeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 0;
        opts.numCoordinators = 1;
        opts.numWorkers = WORKER_COUNT;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1"
            " -node_timeout_s="
            + std::to_string(ONLINE_NODE_TIMEOUT_S)
            + " -node_dead_timeout_s=" + std::to_string(ONLINE_NODE_DEAD_TIMEOUT_S);
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        opts.coordinatorGflagParams = " -node_timeout_s=" + std::to_string(ONLINE_NODE_TIMEOUT_S)
                                      + " -node_dead_timeout_s=" + std::to_string(ONLINE_NODE_DEAD_TIMEOUT_S)
                                      + " -scale_in_collect_window_ms=0";
        ASSERT_EQ(setenv(CLIENT_SD_HOST_ID_ENV0, CLIENT_SD_HOST_ID_VALUE0, 1), 0);
        ASSERT_EQ(setenv(CLIENT_SD_HOST_ID_ENV1, CLIENT_SD_HOST_ID_VALUE1, 1), 0);
        ASSERT_EQ(setenv(CLIENT_SD_HOST_ID_ENV2, CLIENT_SD_HOST_ID_VALUE2, 1), 0);
        opts.workerSpecifyGflagParams[0] = FormatString("-host_id_env_name=%s", CLIENT_SD_HOST_ID_ENV0);
        opts.workerSpecifyGflagParams[1] = FormatString("-host_id_env_name=%s", CLIENT_SD_HOST_ID_ENV1);
        opts.workerSpecifyGflagParams[2] = FormatString("-host_id_env_name=%s", CLIENT_SD_HOST_ID_ENV2);
    }

    void SetUp() override
    {
#ifndef USE_URMA
        GTEST_SKIP() << "Strict fast-failover timing ST requires USE_URMA or USE_URMA_MOCK.";
#else
        ExternalClusterTest::SetUp();
        clusterStarted_ = true;
        InitHealthyWorkerClient(0);
        InitHealthyWorkerClient(2);
        ASSERT_TRUE(WaitForTargetState(MembershipPb::ACTIVE, std::chrono::milliseconds(ISOLATION_TIMEOUT_MS)));
        DS_ASSERT_OK(FindRouteKeyToWorker(TARGET_WORKER_INDEX, ROUTE_KEY_PREFIX, targetKey_));
        const auto warmup = MeasureClientRecovery(std::chrono::steady_clock::now(), [&](uint64_t) {
            return clients_.back()->Set(targetKey_, "active-failure-value");
        });
        ASSERT_TRUE(warmup.recovered) << warmup.lastStatus;
#endif
    }

    void TearDown() override
    {
#ifdef USE_URMA
        if (clusterStarted_) {
            clients_.clear();
            ExternalClusterTest::TearDown();
            clusterStarted_ = false;
        }
        (void)unsetenv(CLIENT_SD_HOST_ID_ENV0);
        (void)unsetenv(CLIENT_SD_HOST_ID_ENV1);
        (void)unsetenv(CLIENT_SD_HOST_ID_ENV2);
        (void)unsetenv(CLIENT_SD_HOST_ID_ENV3);
#endif
    }

protected:
    void InitHealthyWorkerClient(uint32_t workerIndex)
    {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(workerIndex, client, CONNECT_TIMEOUT_MS, false, REQUEST_TIMEOUT_MS);
        clients_.emplace_back(std::move(client));
    }

    void InitLocalCacheModeClient(uint32_t workerIndex, bool enableLocalCache, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions options;
        InitConnectOpt(workerIndex, options, CONNECT_TIMEOUT_MS);
        options.requestTimeoutMs = REQUEST_TIMEOUT_MS;
        options.enableLocalCache = enableLocalCache;
        client = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(client->Init());
    }

    void InitCoordinatorServiceDiscoveryClient(const std::string &hostIdEnvName, std::shared_ptr<KVClient> &client,
                                               bool enableLocalCache = true)
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster, nullptr);
        HostPort coordinatorAddr;
        DS_ASSERT_OK(externalCluster->GetCoordinatorAddr(0, coordinatorAddr));
        CoordinatorServiceDiscoveryOptions discoveryOptions;
        discoveryOptions.serviceAddress = coordinatorAddr.ToString();
        discoveryOptions.clusterName = GetTestClusterName();
        discoveryOptions.hostIdEnvName = hostIdEnvName;
        discoveryOptions.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<CoordinatorServiceDiscovery>(discoveryOptions);
        DS_ASSERT_OK(serviceDiscovery->Init());

        ConnectOptions connectOptions;
        InitConnectOpt(0, connectOptions, CONNECT_TIMEOUT_MS, true);
        connectOptions.requestTimeoutMs = REQUEST_TIMEOUT_MS;
        connectOptions.enableLocalCache = enableLocalCache;
        connectOptions.serviceDiscovery = serviceDiscovery;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    MembershipPb::StatePb WorkerState(uint32_t workerIndex) const
    {
        ClusterTopologyPb topology;
        DS_EXPECT_OK(cluster_->ReadClusterTopology(topology));
        HostPort target;
        DS_EXPECT_OK(cluster_->GetWorkerAddr(workerIndex, target));
        auto found = topology.members().find(target.ToString());
        if (found == topology.members().end()) {
            return MembershipPb::FAILED;
        }
        return found->second.state();
    }

    MembershipPb::StatePb TargetState() const
    {
        return WorkerState(TARGET_WORKER_INDEX);
    }

    bool WaitForTargetState(MembershipPb::StatePb expected, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (TargetState() == expected) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
        }
        return TargetState() == expected;
    }

    Status FindRouteKeyToWorker(uint32_t workerIndex, const std::string &prefix, std::string &key) const
    {
        ClusterTopologyPb ring;
        RETURN_IF_NOT_OK(cluster_->ReadClusterTopology(ring));
        HostPort targetWorker;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, targetWorker));
        CHECK_FAIL_RETURN_STATUS(ring.members().find(targetWorker.ToString()) != ring.members().end(), K_NOT_FOUND,
                                 "Target worker is absent from hash ring");

        std::map<uint32_t, std::string> tokenWorkers;
        for (const auto &worker : ring.members()) {
            if (worker.second.state() != MembershipPb::ACTIVE) {
                continue;
            }
            for (const auto token : RebuildTopologyMemberTokens(ring, worker.first, worker.second)) {
                tokenWorkers.emplace(token, worker.first);
            }
        }
        CHECK_FAIL_RETURN_STATUS(!tokenWorkers.empty(), K_NOT_FOUND, "Hash ring has no active worker tokens");
        for (size_t i = 0; i < KEY_SEARCH_LIMIT; ++i) {
            std::string candidate = prefix + std::to_string(i);
            auto owner = tokenWorkers.lower_bound(MurmurHash3_32(candidate));
            if (owner == tokenWorkers.end()) {
                owner = tokenWorkers.begin();
            }
            if (owner->second == targetWorker.ToString()) {
                key = std::move(candidate);
                return Status::OK();
            }
        }
        return Status(K_NOT_FOUND, "Unable to find a key for the target worker");
    }

    Status FindRouteKeysToWorker(uint32_t workerIndex, const std::string &prefix, std::vector<std::string> &keys,
                                 size_t keyCount = TARGET_META_KEY_COUNT) const
    {
        keys.clear();
        keys.reserve(keyCount);
        for (size_t i = 0; i < keyCount; ++i) {
            std::string key;
            RETURN_IF_NOT_OK(FindRouteKeyToWorker(workerIndex, prefix + std::to_string(i) + "_", key));
            keys.emplace_back(std::move(key));
        }
        return Status::OK();
    }

    RecoveryMeasure MeasureClientRecovery(const std::chrono::steady_clock::time_point start,
                                          const std::function<Status(uint64_t)> &operation)
    {
        RecoveryMeasure measure;
        uint64_t consecutiveSuccesses = 0;
        const auto deadline = start + std::chrono::milliseconds(CLIENT_RECOVERY_TIMEOUT_MS);
        while (std::chrono::steady_clock::now() < deadline) {
            auto rc = operation(measure.attempts);
            ++measure.attempts;
            measure.lastStatus = rc.ToString();
            if (rc.IsOk()) {
                ++consecutiveSuccesses;
                if (consecutiveSuccesses >= REQUIRED_CONSECUTIVE_SUCCESSES) {
                    measure.recovered = true;
                    measure.noMoreFailAfterMs = measure.lastFailureElapsedMs < 0 ? 0 : measure.lastFailureElapsedMs;
                    break;
                }
            } else {
                ++measure.failures;
                measure.lastFailureElapsedMs = ElapsedMs(start);
                measure.lastFailureStatus = measure.lastStatus;
                consecutiveSuccesses = 0;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
        }
        measure.elapsedMs = ElapsedMs(start);
        return measure;
    }

    RecoveryMeasure MeasureSetRecoveryAfterKill(const std::chrono::steady_clock::time_point start,
                                                const std::vector<std::shared_ptr<KVClient>> &setClients,
                                                const std::vector<std::string> &targetMetaKeys,
                                                std::string &recoveredKey)
    {
        return MeasureClientRecovery(start, [&](uint64_t attempt) {
            const std::string &key = targetMetaKeys[attempt % targetMetaKeys.size()];
            auto &setClient = setClients[attempt % setClients.size()];
            auto rc = setClient->Set(key, "set-after-kill-value-" + std::to_string(attempt));
            if (rc.IsOk()) {
                recoveredKey = key;
            }
            return rc;
        });
    }

    RecoveryMeasure MeasureGetRecoveryAfterKill(const std::chrono::steady_clock::time_point start,
                                                const std::shared_ptr<KVClient> &getClient,
                                                const std::string &recoveredKey)
    {
        return MeasureClientRecovery(start, [&](uint64_t) {
            std::string value;
            auto rc = getClient->Get(recoveredKey, value);
            RETURN_IF_NOT_OK(rc);
            CHECK_FAIL_RETURN_STATUS(value.find("set-after-kill-value-") == 0, K_RUNTIME_ERROR, "new value mismatch");
            return Status::OK();
        });
    }

    RecoveryMeasure MeasureSetRecoveryUntilIsolated(const std::chrono::steady_clock::time_point start,
                                                    const std::vector<std::shared_ptr<KVClient>> &setClients,
                                                    const std::vector<std::string> &targetMetaKeys,
                                                    std::string &recoveredKey)
    {
        return MeasureClientRecovery(start, [&](uint64_t attempt) {
            const std::string &key = targetMetaKeys[attempt % targetMetaKeys.size()];
            auto &setClient = setClients[attempt % setClients.size()];
            auto rc = setClient->Set(key, "set-after-stop-value-" + std::to_string(attempt));
            if (rc.IsOk()) {
                recoveredKey = key;
            }
            if (TargetState() == MembershipPb::FAILED) {
                return Status::OK();
            }
            CHECK_FAIL_RETURN_STATUS(rc.IsOk(), rc.GetCode(), rc.GetMsg());
            return Status(K_RUNTIME_ERROR, "target worker is not isolated yet");
        });
    }

    std::string targetKey_;
    std::vector<std::shared_ptr<KVClient>> clients_;
    bool clusterStarted_ = false;
};

class CoordinatorActiveFailureTwoWorkerTest : public CoordinatorActiveFailureStopResumeTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorActiveFailureStopResumeTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 2;
        opts.workerSpecifyGflagParams.erase(2);
    }

    void SetUp() override
    {
#ifndef USE_URMA
        GTEST_SKIP() << "Strict fast-failover timing ST requires USE_URMA or USE_URMA_MOCK.";
#else
        ExternalClusterTest::SetUp();
        clusterStarted_ = true;
        InitHealthyWorkerClient(0);
        InitHealthyWorkerClient(0);
        ASSERT_TRUE(WaitForTargetState(MembershipPb::ACTIVE, std::chrono::milliseconds(ISOLATION_TIMEOUT_MS)));
#endif
    }
};

class CoordinatorActiveFailureClientRecoveryGuardTest : public CoordinatorActiveFailureStopResumeTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorActiveFailureStopResumeTest::SetClusterSetupOptions(opts);
        opts.numWorkers = CLIENT_RECOVERY_GUARD_WORKER_COUNT;
        ASSERT_EQ(setenv(CLIENT_SD_HOST_ID_ENV3, CLIENT_SD_HOST_ID_VALUE3, 1), 0);
        opts.workerSpecifyGflagParams[3] = FormatString("-host_id_env_name=%s", CLIENT_SD_HOST_ID_ENV3);
    }

    void SetUp() override
    {
        CoordinatorActiveFailureStopResumeTest::SetUp();
        if (!::testing::Test::IsSkipped()) {
            InitHealthyWorkerClient(3);
        }
    }
};

class CoordinatorActiveFailureMultiKillBase : public CoordinatorActiveFailureStopResumeTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorActiveFailureStopResumeTest::SetClusterSetupOptions(opts);
        opts.numWorkers = MULTI_KILL_WORKER_COUNT;
        opts.numRpcThreads = 32;
        opts.workerGflagParams =
            " -shared_memory_size_mb=512 -ipc_through_shared_memory=false -arena_per_tenant=1"
            " -heartbeat_interval_ms=750 -node_timeout_s="
            + std::to_string(ONLINE_NODE_TIMEOUT_S)
            + " -node_dead_timeout_s=" + std::to_string(ONLINE_NODE_DEAD_TIMEOUT_S);
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        opts.coordinatorGflagParams = " -node_timeout_s=" + std::to_string(ONLINE_NODE_TIMEOUT_S)
                                      + " -node_dead_timeout_s=" + std::to_string(ONLINE_NODE_DEAD_TIMEOUT_S)
                                      + " -scale_in_collect_window_ms=0";
    }

    void SetUp() override
    {
        CoordinatorActiveFailureStopResumeTest::SetUp();
        if (::testing::Test::IsSkipped()) {
            return;
        }
        for (size_t index = 1; index < MULTI_KILL_REPORTER_INDICES.size(); ++index) {
            InitHealthyWorkerClient(MULTI_KILL_REPORTER_INDICES[index]);
        }
    }

protected:
    struct WorkerRingProbe {
        uint32_t workerIndex;
        std::string workerAddress;
        std::unique_ptr<client::WorkerRpcClient> rpcClient;
        int64_t convergedMs = -1;
        uint64_t version = 0;
    };

    MembershipPb::StatePb WorkerState(uint32_t workerIndex, uint64_t *topologyVersion = nullptr) const
    {
        ClusterTopologyPb topology;
        DS_EXPECT_OK(cluster_->ReadClusterTopology(topology));
        if (topologyVersion != nullptr) {
            *topologyVersion = topology.version();
        }
        HostPort worker;
        DS_EXPECT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
        auto found = topology.members().find(worker.ToString());
        return found == topology.members().end() ? MembershipPb::FAILED : found->second.state();
    }

    std::vector<std::shared_ptr<KVClient>> ReporterClients(size_t targetCount) const
    {
        std::vector<std::shared_ptr<KVClient>> reporters;
        for (size_t index = 0; index < MULTI_KILL_REPORTER_INDICES.size(); ++index) {
            const auto workerIndex = MULTI_KILL_REPORTER_INDICES[index];
            if (std::find(MULTI_KILL_TARGET_INDICES.begin(), MULTI_KILL_TARGET_INDICES.begin() + targetCount,
                          workerIndex)
                == MULTI_KILL_TARGET_INDICES.begin() + targetCount) {
                reporters.emplace_back(index == 0 ? clients_.front() : clients_[index + 1]);
            }
        }
        return reporters;
    }

    void InitSurvivorRingProbes(size_t targetCount, std::vector<WorkerRingProbe> &probes)
    {
        BrpcChannelConfig rpcConfig;
        rpcConfig.timeout_ms = REQUEST_TIMEOUT_MS;
        rpcConfig.connect_timeout_ms = CONNECT_TIMEOUT_MS;
        rpcConfig.max_retry = 0;
        for (uint32_t workerIndex = 0; workerIndex < MULTI_KILL_WORKER_COUNT; ++workerIndex) {
            if (std::find(MULTI_KILL_TARGET_INDICES.begin(), MULTI_KILL_TARGET_INDICES.begin() + targetCount,
                          workerIndex)
                != MULTI_KILL_TARGET_INDICES.begin() + targetCount) {
                continue;
            }
            HostPort workerAddress;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
            ConnectOptions options;
            InitConnectOpt(workerIndex, options);
            auto signature = std::make_shared<Signature>(options.accessKey, options.secretKey);
            auto rpcClient = std::make_unique<client::WorkerRpcClient>(workerAddress, signature, rpcConfig);
            DS_ASSERT_OK(rpcClient->Init());
            probes.emplace_back(WorkerRingProbe{ workerIndex, workerAddress.ToString(), std::move(rpcClient) });
        }
    }

    void ObserveSurvivorRings(const std::chrono::steady_clock::time_point &start,
                              const std::vector<std::string> &targetAddresses, std::vector<WorkerRingProbe> &probes,
                              int64_t &firstConvergedMs)
    {
        for (auto &probe : probes) {
            if (probe.convergedMs >= 0) {
                continue;
            }
            GetHashRingRspPb response;
            auto rc = probe.rpcClient->InvokeGetHashRing(0, response);
            if (rc.IsError() || !response.hash_ring_changed() || !response.has_hash_ring()) {
                continue;
            }
            const auto &members = response.hash_ring().members();
            const bool allTargetsExcluded =
                std::all_of(targetAddresses.begin(), targetAddresses.end(), [&](const auto &target) {
                    auto member = members.find(target);
                    return member == members.end() || member->second.state() != MembershipPb::ACTIVE;
                });
            if (!allTargetsExcluded) {
                continue;
            }
            probe.convergedMs = ElapsedMs(start);
            probe.version = response.version();
            if (firstConvergedMs < 0) {
                firstConvergedMs = probe.convergedMs;
            }
        }
    }

    void RunMultiKillScenario(size_t targetCount, int64_t killGapMs);
};

class CoordinatorActiveFailureMultiKillTest : public CoordinatorActiveFailureMultiKillBase,
                                              public ::testing::WithParamInterface<int64_t> {};

class CoordinatorActiveFailureManyKillTest : public CoordinatorActiveFailureMultiKillBase {};

TEST_F(CoordinatorActiveFailureStopResumeTest, DISABLED_LEVEL1_StopThenResumeWorkerMeasuresIsolationAndRejoin)
{
    std::vector<std::string> targetMetaKeys;
    DS_ASSERT_OK(FindRouteKeysToWorker(TARGET_WORKER_INDEX, ROUTE_KEY_PREFIX, targetMetaKeys));
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);

    const auto stopStart = std::chrono::steady_clock::now();
    DS_ASSERT_OK(cluster_->KillWorker(TARGET_WORKER_INDEX));
    std::string recoveredKey;
    auto stopMeasure = MeasureSetRecoveryUntilIsolated(stopStart, clients_, targetMetaKeys, recoveredKey);
    const bool isolated = TargetState() == MembershipPb::FAILED;
    const auto isolatedMs = ElapsedMs(stopStart);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=stop_resume_isolation isolated="
                               << isolated << " elapsed_ms=" << isolatedMs << " attempts=" << stopMeasure.attempts
                               << " failures=" << stopMeasure.failures
                               << " last_failure_ms=" << stopMeasure.lastFailureElapsedMs
                               << " no_more_fail_after_ms=" << stopMeasure.noMoreFailAfterMs);
    EXPECT_TRUE(isolated);
    ASSERT_LE(isolatedMs, SET_RECOVERY_EXPECT_MS);

    const auto resumeStart = std::chrono::steady_clock::now();
    DS_ASSERT_OK(externalCluster->StartWorker(TARGET_WORKER_INDEX, HostPort()));
    const bool rejoined = WaitForTargetState(MembershipPb::ACTIVE, std::chrono::milliseconds(REJOIN_TIMEOUT_MS));
    const auto rejoinMs = ElapsedMs(resumeStart);
    LOG_ACTIVE_FAILURE_MEASURE(
        "ACTIVE_FAILURE_MEASURE phase=stop_resume_rejoin rejoined=" << rejoined << " elapsed_ms=" << rejoinMs);
    ASSERT_TRUE(rejoined);
}

TEST_F(CoordinatorActiveFailureStopResumeTest, DISABLED_LEVEL1_StopThenResumeWorkerMeasuresAccessRecovery)
{
    std::shared_ptr<KVClient> client;
    InitCoordinatorServiceDiscoveryClient(CLIENT_SD_HOST_ID_VALUE1, client);
    std::vector<std::string> targetMetaKeys;
    DS_ASSERT_OK(FindRouteKeysToWorker(TARGET_WORKER_INDEX, ROUTE_KEY_PREFIX, targetMetaKeys));
    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);

    const auto stopStart = std::chrono::steady_clock::now();
    DS_ASSERT_OK(cluster_->KillWorker(TARGET_WORKER_INDEX));
    std::string recoveredKey;
    const auto isolatedMeasure = MeasureSetRecoveryUntilIsolated(stopStart, clients_, targetMetaKeys, recoveredKey);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=stop_resume_access_after_isolation recovered="
                               << isolatedMeasure.recovered << " elapsed_ms=" << isolatedMeasure.elapsedMs
                               << " attempts=" << isolatedMeasure.attempts << " failures=" << isolatedMeasure.failures
                               << " last_failure_ms=" << isolatedMeasure.lastFailureElapsedMs
                               << " no_more_fail_after_ms=" << isolatedMeasure.noMoreFailAfterMs
                               << " target_state=" << MembershipPb::StatePb_Name(TargetState())
                               << " last_status=" << isolatedMeasure.lastStatus);
    ASSERT_TRUE(isolatedMeasure.recovered);
    ASSERT_EQ(TargetState(), MembershipPb::FAILED);

    const auto resumeStart = std::chrono::steady_clock::now();
    DS_ASSERT_OK(externalCluster->StartWorker(TARGET_WORKER_INDEX, HostPort()));
    ASSERT_TRUE(WaitForTargetState(MembershipPb::ACTIVE, std::chrono::milliseconds(REJOIN_TIMEOUT_MS)));
    const auto recoveredMeasure = MeasureClientRecovery(resumeStart, [&](uint64_t attempt) {
        return client->Set(std::string(ROUTE_KEY_PREFIX) + "after_resume_" + std::to_string(attempt), "resumed-value");
    });
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=stop_resume_access_after_rejoin recovered="
                               << recoveredMeasure.recovered << " elapsed_ms=" << recoveredMeasure.elapsedMs
                               << " attempts=" << recoveredMeasure.attempts << " failures=" << recoveredMeasure.failures
                               << " last_failure_ms=" << recoveredMeasure.lastFailureElapsedMs
                               << " no_more_fail_after_ms=" << recoveredMeasure.noMoreFailAfterMs
                               << " target_state=" << MembershipPb::StatePb_Name(TargetState())
                               << " last_status=" << recoveredMeasure.lastStatus);
    ASSERT_TRUE(recoveredMeasure.recovered);
}

TEST_F(CoordinatorActiveFailureStopResumeTest, DISABLED_LEVEL1_ClientSetAndGetRecoverAfterKilledWorker)
{
    std::shared_ptr<KVClient> setClient0;
    std::shared_ptr<KVClient> setClient2;
    std::shared_ptr<KVClient> getClientWithLocalCache;
    std::shared_ptr<KVClient> getClientWithoutLocalCache;
    InitLocalCacheModeClient(0, true, setClient0);
    InitLocalCacheModeClient(2, false, setClient2);

    DS_ASSERT_OK(setClient0->Set(std::string(CLIENT_KILL_SET_KEY_PREFIX) + "warmup", "warmup-value"));
    std::vector<std::string> targetMetaKeys;
    DS_ASSERT_OK(FindRouteKeysToWorker(TARGET_WORKER_INDEX, CLIENT_KILL_SET_KEY_PREFIX, targetMetaKeys));
    InitLocalCacheModeClient(2, true, getClientWithLocalCache);
    InitLocalCacheModeClient(2, false, getClientWithoutLocalCache);

    const auto start = std::chrono::steady_clock::now();
    DS_ASSERT_OK(cluster_->KillWorker(TARGET_WORKER_INDEX));

    std::string recoveredKey;
    auto setMeasure = MeasureSetRecoveryAfterKill(start, { setClient0, setClient2 }, targetMetaKeys, recoveredKey);
    ASSERT_FALSE(recoveredKey.empty());
    auto getWithLocalCache = MeasureGetRecoveryAfterKill(start, getClientWithLocalCache, recoveredKey);
    auto getWithoutLocalCache = MeasureGetRecoveryAfterKill(start, getClientWithoutLocalCache, recoveredKey);
    const bool isolated = WaitForTargetState(MembershipPb::FAILED, std::chrono::milliseconds(ISOLATION_TIMEOUT_MS));
    const auto isolationElapsedMs = ElapsedMs(start);

    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=client_set_recover_after_kill recovered="
                               << (setMeasure.recovered ? "true" : "false") << " elapsed_ms=" << setMeasure.elapsedMs
                               << " attempts=" << setMeasure.attempts << " failures=" << setMeasure.failures
                               << " last_failure_ms=" << setMeasure.lastFailureElapsedMs
                               << " no_more_fail_after_ms=" << setMeasure.noMoreFailAfterMs
                               << " target_state=" << MembershipPb::StatePb_Name(TargetState())
                               << " last_status=" << setMeasure.lastStatus);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=client_metadata_isolation isolated="
                               << (isolated ? "true" : "false") << " elapsed_ms=" << isolationElapsedMs
                               << " target_state=" << MembershipPb::StatePb_Name(TargetState()));
    for (const auto &[enableLocalCache, measure] :
         { std::pair{ true, getWithLocalCache }, std::pair{ false, getWithoutLocalCache } }) {
        LOG_ACTIVE_FAILURE_MEASURE(
            "ACTIVE_FAILURE_MEASURE phase=client_get_recover_after_kill local_cache="
            << enableLocalCache << " recovered=" << (measure.recovered ? "true" : "false")
            << " elapsed_ms=" << measure.elapsedMs << " attempts=" << measure.attempts
            << " failures=" << measure.failures << " last_failure_ms=" << measure.lastFailureElapsedMs
            << " no_more_fail_after_ms=" << measure.noMoreFailAfterMs
            << " target_state=" << MembershipPb::StatePb_Name(TargetState()) << " last_status=" << measure.lastStatus);
    }

    ASSERT_TRUE(setMeasure.recovered);
    EXPECT_LE(setMeasure.elapsedMs, SET_RECOVERY_EXPECT_MS);
    EXPECT_LE(setMeasure.noMoreFailAfterMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(isolated);
    EXPECT_LE(isolationElapsedMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(getWithLocalCache.recovered);
    EXPECT_LE(getWithLocalCache.elapsedMs, GET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(getWithoutLocalCache.recovered);
    EXPECT_LE(getWithoutLocalCache.elapsedMs, GET_RECOVERY_EXPECT_MS);
}

TEST_F(CoordinatorActiveFailureTwoWorkerTest, DISABLED_LEVEL1_SingleReporterIsolatesKilledMetadataOwner)
{
    std::vector<std::string> targetKeys;
    DS_ASSERT_OK(FindRouteKeysToWorker(TARGET_WORKER_INDEX, "two_worker_target_", targetKeys));

    const auto start = std::chrono::steady_clock::now();
    DS_ASSERT_OK(cluster_->KillWorker(TARGET_WORKER_INDEX));
    const auto killCompletedMs = ElapsedMs(start);
    int64_t firstFailureMs = -1;
    int64_t lastFailureMs = -1;
    uint64_t attempts = 0;
    const auto deadline = start + std::chrono::milliseconds(SET_RECOVERY_EXPECT_MS);
    while (std::chrono::steady_clock::now() < deadline && TargetState() != MembershipPb::FAILED) {
        const auto &key = targetKeys[attempts % targetKeys.size()];
        auto rc = clients_.front()->Set(key, "two-worker-value-" + std::to_string(attempts++));
        if (rc.IsError()) {
            const auto failureMs = ElapsedMs(start);
            if (firstFailureMs < 0) {
                firstFailureMs = failureMs;
            }
            lastFailureMs = failureMs;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
    }
    const bool isolated = TargetState() == MembershipPb::FAILED;
    const auto isolationElapsedMs = ElapsedMs(start);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=two_worker_single_reporter_isolation"
                               << " isolated=" << isolated << " kill_completed_ms=" << killCompletedMs
                               << " isolation_elapsed_ms=" << isolationElapsedMs << " first_failure_ms="
                               << firstFailureMs << " last_failure_ms=" << lastFailureMs << " attempts=" << attempts);

    EXPECT_TRUE(isolated);
    EXPECT_LE(isolationElapsedMs, SET_RECOVERY_EXPECT_MS);

    uint64_t consecutiveSuccesses = 0;
    uint64_t recoveryAttempts = 0;
    const auto recoveryDeadline = start + std::chrono::milliseconds(ISOLATION_TIMEOUT_MS);
    while (std::chrono::steady_clock::now() < recoveryDeadline
           && consecutiveSuccesses < REQUIRED_CONSECUTIVE_SUCCESSES) {
        const auto &key = targetKeys[recoveryAttempts % targetKeys.size()];
        const auto value = "two-worker-recovered-" + std::to_string(recoveryAttempts++);
        auto rc = clients_.front()->Set(key, value);
        std::string observed;
        if (rc.IsOk()) {
            rc = clients_.back()->Get(key, observed);
        }
        if (rc.IsOk() && observed == value) {
            ++consecutiveSuccesses;
        } else {
            consecutiveSuccesses = 0;
            lastFailureMs = ElapsedMs(start);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
    }
    const auto trafficRecoveredMs = ElapsedMs(start);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=two_worker_client_recovery"
                               << " recovered=" << (consecutiveSuccesses >= REQUIRED_CONSECUTIVE_SUCCESSES)
                               << " elapsed_ms=" << trafficRecoveredMs << " last_failure_ms=" << lastFailureMs
                               << " attempts=" << recoveryAttempts);
    EXPECT_GE(consecutiveSuccesses, REQUIRED_CONSECUTIVE_SUCCESSES);
    EXPECT_LE(lastFailureMs, SET_RECOVERY_EXPECT_MS);
}

TEST_F(CoordinatorActiveFailureClientRecoveryGuardTest,
       LEVEL1_TwoWorkerFailureRecoversBoundClientsInBothLocalCacheModes)
{
    std::shared_ptr<KVClient> localCacheClient;
    std::shared_ptr<KVClient> routedClient;
    InitCoordinatorServiceDiscoveryClient(CLIENT_SD_HOST_ID_ENV1, localCacheClient, true);
    InitCoordinatorServiceDiscoveryClient(CLIENT_SD_HOST_ID_ENV1, routedClient, false);

    std::vector<std::string> firstFailedOwnerKeys;
    std::vector<std::string> secondFailedOwnerKeys;
    DS_ASSERT_OK(FindRouteKeysToWorker(TARGET_WORKER_INDEX, "client_recovery_guard_first_", firstFailedOwnerKeys));
    DS_ASSERT_OK(
        FindRouteKeysToWorker(SECOND_TARGET_WORKER_INDEX, "client_recovery_guard_second_", secondFailedOwnerKeys));
    DS_ASSERT_OK(localCacheClient->Set(secondFailedOwnerKeys[0], "local-cache-warmup"));
    DS_ASSERT_OK(routedClient->Set(secondFailedOwnerKeys[1], "routed-warmup"));
    std::string warmupValue;
    DS_ASSERT_OK(localCacheClient->Get(secondFailedOwnerKeys[0], warmupValue));
    ASSERT_EQ(warmupValue, "local-cache-warmup");
    DS_ASSERT_OK(routedClient->Get(secondFailedOwnerKeys[1], warmupValue));
    ASSERT_EQ(warmupValue, "routed-warmup");

    const auto start = std::chrono::steady_clock::now();
    const auto firstKillIssuedMs = ElapsedMs(start);
    DS_ASSERT_OK(cluster_->KillWorker(TARGET_WORKER_INDEX));
    const auto secondKillIssuedMs = ElapsedMs(start);
    DS_ASSERT_OK(cluster_->KillWorker(SECOND_TARGET_WORKER_INDEX));

    std::atomic<bool> stopSurvivorTraffic{ false };
    auto survivorTraffic = std::async(std::launch::async, [&]() {
        uint64_t attempt = 0;
        while (!stopSurvivorTraffic.load(std::memory_order_acquire)) {
            const auto &keys = attempt % 2 == 0 ? firstFailedOwnerKeys : secondFailedOwnerKeys;
            const auto &key = keys[(attempt / 2) % keys.size()];
            auto &client = (attempt / 2) % 2 == 0 ? clients_.front() : clients_.back();
            (void)client->Set(key, "survivor-traffic-" + std::to_string(attempt++));
            std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
        }
    });
    auto measureClient = [&](const std::shared_ptr<KVClient> &client, const std::string &mode) {
        return MeasureClientRecovery(start, [&](uint64_t attempt) {
            const auto &key = secondFailedOwnerKeys[attempt % secondFailedOwnerKeys.size()];
            const auto value = mode + "-recovered-" + std::to_string(attempt);
            RETURN_IF_NOT_OK(client->Set(key, value));
            std::string observed;
            RETURN_IF_NOT_OK(client->Get(key, observed));
            CHECK_FAIL_RETURN_STATUS(observed == value, K_RUNTIME_ERROR, "Recovered value mismatch");
            return Status::OK();
        });
    };
    auto localCacheRecovery =
        std::async(std::launch::async, [&]() { return measureClient(localCacheClient, "local-cache"); });
    auto routedRecovery = std::async(std::launch::async, [&]() { return measureClient(routedClient, "routed"); });

    int64_t firstIsolatedMs = -1;
    int64_t secondIsolatedMs = -1;
    const auto isolationDeadline = start + std::chrono::milliseconds(ISOLATION_TIMEOUT_MS);
    while (std::chrono::steady_clock::now() < isolationDeadline && (firstIsolatedMs < 0 || secondIsolatedMs < 0)) {
        if (firstIsolatedMs < 0 && WorkerState(TARGET_WORKER_INDEX) == MembershipPb::FAILED) {
            firstIsolatedMs = ElapsedMs(start);
        }
        if (secondIsolatedMs < 0 && WorkerState(SECOND_TARGET_WORKER_INDEX) == MembershipPb::FAILED) {
            secondIsolatedMs = ElapsedMs(start);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(POLL_INTERVAL_MS));
    }
    stopSurvivorTraffic.store(true, std::memory_order_release);
    survivorTraffic.get();
    const auto localCacheMeasure = localCacheRecovery.get();
    const auto routedMeasure = routedRecovery.get();

    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=two_worker_bound_client_guard_isolation"
                               << " first_isolation_after_kill_ms=" << firstIsolatedMs - firstKillIssuedMs
                               << " second_isolation_after_kill_ms=" << secondIsolatedMs - secondKillIssuedMs);
    for (const auto &[enableLocalCache, measure] :
         { std::pair{ true, localCacheMeasure }, std::pair{ false, routedMeasure } }) {
        LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=two_worker_bound_client_guard_recovery"
                                   << " local_cache=" << enableLocalCache
                                   << " recovered=" << (measure.recovered ? "true" : "false")
                                   << " elapsed_ms=" << measure.elapsedMs
                                   << " last_failure_ms=" << measure.lastFailureElapsedMs
                                   << " attempts=" << measure.attempts << " failures=" << measure.failures
                                   << " last_status=" << measure.lastStatus
                                   << " last_failure_status=" << measure.lastFailureStatus);
    }

    ASSERT_GE(firstIsolatedMs, 0);
    EXPECT_LE(firstIsolatedMs - firstKillIssuedMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_GE(secondIsolatedMs, 0);
    EXPECT_LE(secondIsolatedMs - secondKillIssuedMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(localCacheMeasure.recovered) << localCacheMeasure.lastStatus;
    EXPECT_LE(localCacheMeasure.lastFailureElapsedMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(routedMeasure.recovered) << routedMeasure.lastStatus;
    EXPECT_LE(routedMeasure.lastFailureElapsedMs, SET_RECOVERY_EXPECT_MS);
}

TEST_P(CoordinatorActiveFailureMultiKillTest, DISABLED_LEVEL2_TwoKilledMetadataOwnersMeasureActiveIsolation)
{
    RunMultiKillScenario(2, GetParam());
}

TEST_F(CoordinatorActiveFailureManyKillTest, DISABLED_LEVEL2_ThreeKilledMetadataOwnersMeasureActiveIsolation)
{
    RunMultiKillScenario(3, 0);
}

TEST_F(CoordinatorActiveFailureManyKillTest, DISABLED_LEVEL2_FourKilledMetadataOwnersMeasureActiveIsolation)
{
    RunMultiKillScenario(4, 500);
}

void CoordinatorActiveFailureMultiKillBase::RunMultiKillScenario(size_t targetCount, int64_t killGapMs)
{
    ASSERT_GE(targetCount, 2UL);
    ASSERT_LE(targetCount, MULTI_KILL_TARGET_INDICES.size());
    std::array<std::vector<std::string>, MULTI_KILL_WORKER_COUNT> keysByOwner;
    for (size_t ownerIndex = 0; ownerIndex < keysByOwner.size(); ++ownerIndex) {
        DS_ASSERT_OK(FindRouteKeysToWorker(ownerIndex, "multi_kill_owner_" + std::to_string(ownerIndex) + "_",
                                           keysByOwner[ownerIndex], MULTI_KILL_KEYS_PER_OWNER));
    }
    auto reporters = ReporterClients(targetCount);
    std::vector<std::string> targetAddresses;
    targetAddresses.reserve(targetCount);
    for (size_t targetIndex = 0; targetIndex < targetCount; ++targetIndex) {
        HostPort targetAddress;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(MULTI_KILL_TARGET_INDICES[targetIndex], targetAddress));
        targetAddresses.emplace_back(targetAddress.ToString());
    }
    std::vector<WorkerRingProbe> ringProbes;
    InitSurvivorRingProbes(targetCount, ringProbes);

    const auto start = std::chrono::steady_clock::now();
    std::array<std::atomic<int64_t>, MULTI_KILL_TARGET_INDICES.size()> killIssuedMs;
    std::array<std::atomic<int64_t>, MULTI_KILL_TARGET_INDICES.size()> killCompletedMs;
    for (size_t index = 0; index < targetCount; ++index) {
        killIssuedMs[index].store(-1);
        killCompletedMs[index].store(-1);
    }
    auto killTask = std::async(std::launch::async, [&]() {
        for (size_t targetIndex = 0; targetIndex < targetCount; ++targetIndex) {
            std::this_thread::sleep_until(start + std::chrono::milliseconds(targetIndex * killGapMs));
            killIssuedMs[targetIndex].store(ElapsedMs(start), std::memory_order_release);
            RETURN_IF_NOT_OK(cluster_->KillWorker(MULTI_KILL_TARGET_INDICES[targetIndex]));
            killCompletedMs[targetIndex].store(ElapsedMs(start), std::memory_order_release);
        }
        return Status::OK();
    });

    std::array<int64_t, MULTI_KILL_TARGET_INDICES.size()> isolatedMs;
    std::array<uint64_t, MULTI_KILL_TARGET_INDICES.size()> isolatedVersion = {};
    std::array<int64_t, MULTI_KILL_TARGET_INDICES.size()> firstFailureMs;
    std::array<int64_t, MULTI_KILL_TARGET_INDICES.size()> lastFailureMs;
    std::array<uint64_t, MULTI_KILL_TARGET_INDICES.size()> attempts = {};
    isolatedMs.fill(-1);
    firstFailureMs.fill(-1);
    lastFailureMs.fill(-1);
    std::array<uint64_t, MULTI_KILL_WORKER_COUNT> ownerAttempts = {};
    int64_t firstRingConvergedMs = -1;
    const auto deadline = start + std::chrono::milliseconds(MULTI_KILL_OBSERVATION_TIMEOUT_MS);
    auto allIsolated = [&] {
        return std::all_of(isolatedMs.begin(), isolatedMs.begin() + targetCount,
                           [](int64_t elapsedMs) { return elapsedMs >= 0; });
    };
    auto observeIsolation = [&] {
        uint64_t topologyVersion = 0;
        for (size_t targetIndex = 0; targetIndex < targetCount; ++targetIndex) {
            if (isolatedMs[targetIndex] < 0
                && WorkerState(MULTI_KILL_TARGET_INDICES[targetIndex], &topologyVersion) == MembershipPb::FAILED) {
                isolatedMs[targetIndex] = ElapsedMs(start);
                isolatedVersion[targetIndex] = topologyVersion;
            }
        }
    };
    while (std::chrono::steady_clock::now() < deadline && !allIsolated()) {
        observeIsolation();
        ObserveSurvivorRings(start, targetAddresses, ringProbes, firstRingConvergedMs);
        for (size_t reporterIndex = 0; reporterIndex < reporters.size(); ++reporterIndex) {
            for (size_t ownerIndex = 0; ownerIndex < keysByOwner.size(); ++ownerIndex) {
                const auto attempt = ownerAttempts[ownerIndex]++;
                const auto &key = keysByOwner[ownerIndex][attempt % keysByOwner[ownerIndex].size()];
                auto rc = reporters[reporterIndex]->Set(
                    key, "multi-kill-value-" + std::to_string(ownerIndex) + "-" + std::to_string(attempt));
                const auto target = std::find(MULTI_KILL_TARGET_INDICES.begin(),
                                              MULTI_KILL_TARGET_INDICES.begin() + targetCount, ownerIndex);
                if (target != MULTI_KILL_TARGET_INDICES.begin() + targetCount) {
                    const auto targetIndex =
                        static_cast<size_t>(std::distance(MULTI_KILL_TARGET_INDICES.begin(), target));
                    if (killIssuedMs[targetIndex].load(std::memory_order_acquire) < 0) {
                        continue;
                    }
                    ++attempts[targetIndex];
                    if (rc.IsOk()) {
                        continue;
                    }
                    const auto failureMs = ElapsedMs(start);
                    if (firstFailureMs[targetIndex] < 0) {
                        firstFailureMs[targetIndex] = failureMs;
                    }
                    lastFailureMs[targetIndex] = failureMs;
                }
            }
            observeIsolation();
            if (allIsolated()) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
    }

    DS_ASSERT_OK(killTask.get());
    auto allRingsConverged = [&] {
        return std::all_of(ringProbes.begin(), ringProbes.end(),
                           [](const auto &probe) { return probe.convergedMs >= 0; });
    };
    while (std::chrono::steady_clock::now() < deadline && !allRingsConverged()) {
        ObserveSurvivorRings(start, targetAddresses, ringProbes, firstRingConvergedMs);
        std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
    }
    const auto lastKillIssuedMs = killIssuedMs[targetCount - 1].load(std::memory_order_acquire);
    const auto lastTrafficFailureMs = *std::max_element(lastFailureMs.begin(), lastFailureMs.begin() + targetCount);
    std::ostringstream isolationLog;
    isolationLog << "ACTIVE_FAILURE_MEASURE phase=multi_kill_isolation" << " worker_count=" << MULTI_KILL_WORKER_COUNT
                 << " client_count=" << reporters.size()
                 << " configured_reporter_worker_count=" << MULTI_KILL_REPORTER_INDICES.size()
                 << " target_count=" << targetCount << " node_timeout_s=" << ONLINE_NODE_TIMEOUT_S
                 << " node_dead_timeout_s=" << ONLINE_NODE_DEAD_TIMEOUT_S
                 << " heartbeat_interval_ms=750 kill_gap_ms=" << killGapMs
                 << " last_failure_after_last_kill_ms=" << lastTrafficFailureMs - lastKillIssuedMs;
    for (size_t targetIndex = 0; targetIndex < targetCount; ++targetIndex) {
        const auto issuedMs = killIssuedMs[targetIndex].load(std::memory_order_acquire);
        const auto completedMs = killCompletedMs[targetIndex].load(std::memory_order_acquire);
        isolationLog << " target_" << targetIndex + 1 << "_worker_index=" << MULTI_KILL_TARGET_INDICES[targetIndex]
                     << " target_" << targetIndex + 1 << "_kill_issued_ms=" << issuedMs << " target_" << targetIndex + 1
                     << "_kill_completed_ms=" << completedMs << " target_" << targetIndex + 1
                     << "_isolated_ms=" << isolatedMs[targetIndex] << " target_" << targetIndex + 1
                     << "_isolation_after_kill_ms=" << isolatedMs[targetIndex] - issuedMs << " target_"
                     << targetIndex + 1 << "_first_failure_after_kill_ms=" << firstFailureMs[targetIndex] - issuedMs
                     << " target_" << targetIndex + 1
                     << "_last_failure_after_kill_ms=" << lastFailureMs[targetIndex] - issuedMs << " target_"
                     << targetIndex + 1 << "_isolated_version=" << isolatedVersion[targetIndex] << " target_"
                     << targetIndex + 1 << "_attempts=" << attempts[targetIndex];
    }
    LOG_ACTIVE_FAILURE_MEASURE(isolationLog.str());
    int64_t allRingsConvergedMs = -1;
    std::ostringstream ringLog;
    ringLog << "ACTIVE_FAILURE_MEASURE phase=multi_kill_ring_convergence" << " target_count=" << targetCount
            << " kill_gap_ms=" << killGapMs << " first_ring_converged_ms=" << firstRingConvergedMs;
    for (const auto &probe : ringProbes) {
        ringLog << " worker_" << probe.workerIndex << "_address=" << probe.workerAddress << " worker_"
                << probe.workerIndex << "_ring_converged_ms=" << probe.convergedMs << " worker_" << probe.workerIndex
                << "_ring_version=" << probe.version;
        allRingsConvergedMs = std::max(allRingsConvergedMs, probe.convergedMs);
    }
    ringLog << " all_rings_converged_ms=" << allRingsConvergedMs
            << " all_rings_after_last_kill_ms=" << allRingsConvergedMs - lastKillIssuedMs;
    LOG_ACTIVE_FAILURE_MEASURE(ringLog.str());
    for (size_t targetIndex = 0; targetIndex < targetCount; ++targetIndex) {
        const auto issuedMs = killIssuedMs[targetIndex].load(std::memory_order_acquire);
        ASSERT_GE(isolatedMs[targetIndex], 0);
        EXPECT_LE(isolatedMs[targetIndex] - issuedMs, SET_RECOVERY_EXPECT_MS);
    }
    EXPECT_LE(lastTrafficFailureMs - lastKillIssuedMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(allRingsConverged());
    EXPECT_LE(allRingsConvergedMs - lastKillIssuedMs, SET_RECOVERY_EXPECT_MS);

    for (uint32_t workerIndex = 0; workerIndex < MULTI_KILL_WORKER_COUNT; ++workerIndex) {
        if (std::find(MULTI_KILL_TARGET_INDICES.begin(), MULTI_KILL_TARGET_INDICES.begin() + targetCount, workerIndex)
            == MULTI_KILL_TARGET_INDICES.begin() + targetCount) {
            EXPECT_EQ(WorkerState(workerIndex), MembershipPb::ACTIVE) << "worker_index=" << workerIndex;
        }
    }

    uint64_t consecutiveSuccesses = 0;
    const auto recoveryDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    uint64_t recoveryAttempt = 0;
    while (std::chrono::steady_clock::now() < recoveryDeadline
           && consecutiveSuccesses < REQUIRED_CONSECUTIVE_SUCCESSES) {
        bool roundSucceeded = true;
        for (size_t reporterIndex = 0; reporterIndex < reporters.size(); ++reporterIndex) {
            const auto ownerIndex = MULTI_KILL_TARGET_INDICES[reporterIndex % targetCount];
            const auto &key = keysByOwner[ownerIndex][recoveryAttempt % keysByOwner[ownerIndex].size()];
            roundSucceeded =
                reporters[reporterIndex]->Set(key, "multi-kill-recovered-" + std::to_string(recoveryAttempt)).IsOk()
                && roundSucceeded;
        }
        consecutiveSuccesses = roundSucceeded ? consecutiveSuccesses + 1 : 0;
        ++recoveryAttempt;
        std::this_thread::sleep_for(std::chrono::milliseconds(TRAFFIC_INTERVAL_MS));
    }
    const auto trafficRecoveredMs = ElapsedMs(start);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=multi_kill_traffic_recovery"
                               << " recovered=" << (consecutiveSuccesses >= REQUIRED_CONSECUTIVE_SUCCESSES)
                               << " target_count=" << targetCount << " kill_gap_ms=" << killGapMs
                               << " elapsed_ms=" << trafficRecoveredMs << " attempts=" << recoveryAttempt);
    EXPECT_GE(consecutiveSuccesses, REQUIRED_CONSECUTIVE_SUCCESSES);
    EXPECT_LE(trafficRecoveredMs - *std::max_element(isolatedMs.begin(), isolatedMs.begin() + targetCount), 1'000);
}

INSTANTIATE_TEST_SUITE_P(KillIntervals, CoordinatorActiveFailureMultiKillTest,
                         ::testing::Values<int64_t>(0, 1'000, 2'000),
                         [](const ::testing::TestParamInfo<int64_t> &info) {
                             return "Gap" + std::to_string(info.param) + "ms";
                         });
}  // namespace datasystem::st
