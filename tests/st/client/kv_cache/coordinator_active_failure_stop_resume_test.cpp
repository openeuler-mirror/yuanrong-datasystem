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

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/utils/service_discovery.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem::st {
namespace {
constexpr uint32_t WORKER_COUNT = 3;
constexpr uint32_t TARGET_WORKER_INDEX = 1;
constexpr int32_t REQUEST_TIMEOUT_MS = 20;
constexpr int32_t CONNECT_TIMEOUT_MS = 2'000;
constexpr int64_t ISOLATION_TIMEOUT_MS = 10'000;
constexpr int64_t CLIENT_RECOVERY_TIMEOUT_MS = 12'000;
constexpr int64_t REJOIN_TIMEOUT_MS = CLIENT_RECOVERY_TIMEOUT_MS;
constexpr int64_t SET_RECOVERY_EXPECT_MS = 3'000;
constexpr int64_t GET_RECOVERY_EXPECT_MS = 3'000;
constexpr int64_t POLL_INTERVAL_MS = 50;
constexpr int64_t TRAFFIC_INTERVAL_MS = 20;
constexpr size_t KEY_SEARCH_LIMIT = 100'000;
constexpr size_t TARGET_META_KEY_COUNT = 16;
constexpr uint64_t REQUIRED_CONSECUTIVE_SUCCESSES = 3;
constexpr char ROUTE_KEY_PREFIX[] = "coordinator_active_failure_stop_";
constexpr char CLIENT_KILL_SET_KEY_PREFIX[] = "coordinator_active_failure_kill_set_";
constexpr char CLIENT_SD_HOST_ID_ENV0[] = "coordinator_active_failure_host_id_env0";
constexpr char CLIENT_SD_HOST_ID_ENV1[] = "coordinator_active_failure_host_id_env1";
constexpr char CLIENT_SD_HOST_ID_ENV2[] = "coordinator_active_failure_host_id_env2";
constexpr char CLIENT_SD_HOST_ID_VALUE0[] = "coordinator_active_failure_host0";
constexpr char CLIENT_SD_HOST_ID_VALUE1[] = "coordinator_active_failure_host1";
constexpr char CLIENT_SD_HOST_ID_VALUE2[] = "coordinator_active_failure_host2";

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
            " -use_brpc=true"
            " -node_timeout_s=3 -node_dead_timeout_s=30";
#ifdef USE_URMA
        opts.workerGflagParams += " -enable_urma=true";
#else
        opts.workerGflagParams += " -enable_urma=false";
#endif
        opts.coordinatorGflagParams = " -node_timeout_s=3 -node_dead_timeout_s=30 -scale_in_collect_window_ms=0";
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
        previousUseBrpc_ = FLAGS_use_brpc;
        FLAGS_use_brpc = true;
        ExternalClusterTest::SetUp();
        clusterStarted_ = true;
        InitHealthyWorkerClient(0);
        InitHealthyWorkerClient(2);
        ASSERT_TRUE(WaitForTargetState(MembershipPb::ACTIVE, std::chrono::milliseconds(ISOLATION_TIMEOUT_MS)));
        DS_ASSERT_OK(FindRouteKeyToWorker(TARGET_WORKER_INDEX, ROUTE_KEY_PREFIX, targetKey_));
        DS_ASSERT_OK(clients_.back()->Set(targetKey_, "active-failure-value"));
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
        FLAGS_use_brpc = previousUseBrpc_;
#endif
    }

protected:
    void InitHealthyWorkerClient(uint32_t workerIndex)
    {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(workerIndex, client, CONNECT_TIMEOUT_MS, false, REQUEST_TIMEOUT_MS);
        clients_.emplace_back(std::move(client));
    }

    void InitCoordinatorServiceDiscoveryClient(const std::string &hostIdEnvName, std::shared_ptr<KVClient> &client)
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
        connectOptions.serviceDiscovery = serviceDiscovery;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    MembershipPb::StatePb TargetState() const
    {
        ClusterTopologyPb topology;
        DS_EXPECT_OK(cluster_->ReadClusterTopology(topology));
        HostPort target;
        DS_EXPECT_OK(cluster_->GetWorkerAddr(TARGET_WORKER_INDEX, target));
        auto found = topology.members().find(target.ToString());
        if (found == topology.members().end()) {
            return MembershipPb::FAILED;
        }
        return found->second.state();
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
            for (const auto token : worker.second.tokens()) {
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

    Status FindRouteKeysToWorker(uint32_t workerIndex, const std::string &prefix, std::vector<std::string> &keys) const
    {
        keys.clear();
        keys.reserve(TARGET_META_KEY_COUNT);
        for (size_t i = 0; i < TARGET_META_KEY_COUNT; ++i) {
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
    bool previousUseBrpc_ = false;
};

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
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=stop_resume_isolation isolated=" << isolated
                                                                                              << " elapsed_ms="
                                                                                              << isolatedMs
                                                                                              << " attempts="
                                                                                              << stopMeasure.attempts
                                                                                              << " failures="
                                                                                              << stopMeasure.failures
                                                                                              << " last_failure_ms="
                                                                                              << stopMeasure
                                                                                                     .lastFailureElapsedMs
                                                                                              << " no_more_fail_after_ms="
                                                                                              << stopMeasure
                                                                                                     .noMoreFailAfterMs);
    EXPECT_TRUE(isolated);
    ASSERT_LE(isolatedMs, SET_RECOVERY_EXPECT_MS);

    const auto resumeStart = std::chrono::steady_clock::now();
    DS_ASSERT_OK(externalCluster->StartWorker(TARGET_WORKER_INDEX, HostPort()));
    const bool rejoined = WaitForTargetState(MembershipPb::ACTIVE, std::chrono::milliseconds(REJOIN_TIMEOUT_MS));
    const auto rejoinMs = ElapsedMs(resumeStart);
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=stop_resume_rejoin rejoined=" << rejoined
                                                                                           << " elapsed_ms="
                                                                                           << rejoinMs);
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
    std::shared_ptr<KVClient> getClient;
    InitTestKVClient(0, setClient0, CONNECT_TIMEOUT_MS, false, REQUEST_TIMEOUT_MS);
    InitTestKVClient(2, setClient2, CONNECT_TIMEOUT_MS, false, REQUEST_TIMEOUT_MS);
    InitTestKVClient(2, getClient, CONNECT_TIMEOUT_MS, false, REQUEST_TIMEOUT_MS);

    DS_ASSERT_OK(setClient0->Set(std::string(CLIENT_KILL_SET_KEY_PREFIX) + "warmup", "warmup-value"));
    std::string warmupValue;
    DS_ASSERT_OK(getClient->Get(targetKey_, warmupValue));
    std::vector<std::string> targetMetaKeys;
    DS_ASSERT_OK(FindRouteKeysToWorker(TARGET_WORKER_INDEX, CLIENT_KILL_SET_KEY_PREFIX, targetMetaKeys));

    const auto start = std::chrono::steady_clock::now();
    DS_ASSERT_OK(cluster_->KillWorker(TARGET_WORKER_INDEX));

    std::string recoveredKey;
    auto setMeasure = MeasureSetRecoveryAfterKill(start, { setClient0, setClient2 }, targetMetaKeys, recoveredKey);
    ASSERT_FALSE(recoveredKey.empty());
    const bool isolated = WaitForTargetState(MembershipPb::FAILED, std::chrono::milliseconds(ISOLATION_TIMEOUT_MS));
    const auto isolationElapsedMs = ElapsedMs(start);
    auto getMeasure = MeasureGetRecoveryAfterKill(start, getClient, recoveredKey);

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
    LOG_ACTIVE_FAILURE_MEASURE("ACTIVE_FAILURE_MEASURE phase=client_get_recover_after_kill recovered="
                               << (getMeasure.recovered ? "true" : "false") << " elapsed_ms=" << getMeasure.elapsedMs
                               << " attempts=" << getMeasure.attempts << " failures=" << getMeasure.failures
                               << " last_failure_ms=" << getMeasure.lastFailureElapsedMs
                               << " no_more_fail_after_ms=" << getMeasure.noMoreFailAfterMs
                               << " target_state=" << MembershipPb::StatePb_Name(TargetState())
                               << " last_status=" << getMeasure.lastStatus);

    ASSERT_TRUE(setMeasure.recovered);
    EXPECT_LE(setMeasure.elapsedMs, SET_RECOVERY_EXPECT_MS);
    EXPECT_LE(setMeasure.noMoreFailAfterMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(isolated);
    EXPECT_LE(isolationElapsedMs, SET_RECOVERY_EXPECT_MS);
    ASSERT_TRUE(getMeasure.recovered);
    EXPECT_LE(getMeasure.elapsedMs, GET_RECOVERY_EXPECT_MS);
}
}  // namespace datasystem::st
