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

/**
 * Description: KVClient ST for client-to-worker URMA data-plane failover.
 */
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/service_discovery.h"
#include "oc_client_common.h"

DS_DECLARE_uint32(urma_failover_min_sample_count);
DS_DECLARE_double(urma_failover_success_rate_ratio);

namespace datasystem {
namespace st {
namespace {
constexpr int WORKER_NUM = 3;
constexpr int CONNECT_TIMEOUT_MS = 2'000;
constexpr int REQUEST_TIMEOUT_MS = 10'000;
constexpr int SWITCH_TIMEOUT_S = 15;
constexpr int WINDOW_SETTLE_MS = 1'200;
constexpr int NODE_DEAD_TIMEOUT_S = 2;
constexpr int FAILURE_ISOLATION_WAIT_S = NODE_DEAD_TIMEOUT_S + 1;
constexpr uint64_t VALUE_SIZE = 512 * 1024;
constexpr uint32_t MIN_SAMPLE_COUNT = 2;
constexpr uint32_t HIGH_MIN_SAMPLE_COUNT = 100;
constexpr double SUCCESS_RATE_RATIO_EPSILON = 1e-9;
const std::string HOST_IP = "127.0.0.1";
const std::string URMA_WRITE_ERROR_INJECT = "UrmaManager.UrmaWriteError";
const std::string URMA_CQE_ERROR_INJECT = "UrmaManager.CheckCompletionRecordStatus";
const std::string SWITCH_END_INJECT = "client.switch_worker_end";
const std::string HEARTBEAT_INTERVAL_INJECT = "ListenWorker.CheckHeartbeat.heartbeat_interval_ms";
const std::string HEARTBEAT_TIMEOUT_INJECT = "ClientWorkerCommonApi.SendHeartbeat.timeoutMs";
const std::string CLIENT_CONFIG_PATH_ENV = "DATASYSTEM_CLIENT_CONFIG_PATH";
const std::string CLIENT_CONFIG_FILE = "urma_failover_client.config";
const std::string HOST_ID_ENV_PREFIX = "urma_failover_host_id_env";
const std::string HOST_ID_VALUE_PREFIX = "urma_failover_host_id";
constexpr char DUMMY_ACCESS_KEY[] = "datasystem_test_access_key";
constexpr char DUMMY_SECRET_KEY[] = "datasystem_test_secret_key";

struct GetTrafficData {
    std::vector<std::string> keys;
    std::string value;
};
}  // namespace

class KVClientUrmaFailoverTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.systemAccessKey = DUMMY_ACCESS_KEY;
        opts.systemSecretKey = DUMMY_SECRET_KEY;
        // Keep the initial master away from worker0 because several cases stop worker0 to force remote discovery.
        opts.masterIdx = 1;
        // Keep local IPC enabled so same-host service discovery exercises the UDS/shared-memory path.
        opts.workerGflagParams = FormatString(
            " -shared_memory_size_mb=5120 -payload_nocopy_threshold=65536"
            " -enable_worker_worker_batch_get=true -enable_transport_fallback=true"
            " -ipc_through_shared_memory=true -client_dead_timeout_s=1"
            " -node_timeout_s=1 -node_dead_timeout_s=%d -heartbeat_interval_ms=500"
            " -client_reconnect_wait_s=1",
            NODE_DEAD_TIMEOUT_S);
#ifdef USE_URMA
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_urma=true";
#else
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_urma=false";
#endif
        for (int i = 0; i < WORKER_NUM; ++i) {
            opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
            workerAddresses_.emplace_back(opts.workerConfigs.back());
            std::string envName = GetHostIdEnvName(i);
            std::string envValue = GetHostIdValue(i);
            ASSERT_EQ(setenv(envName.c_str(), envValue.c_str(), 1), 0);
            opts.workerSpecifyGflagParams[i] = FormatString("-host_id_env_name=%s", envName);
        }
    }

    void SetUp() override
    {
#ifndef USE_URMA
        GTEST_SKIP() << "URMA data-plane failover ST requires USE_URMA.";
#else
        oldSuccessRateRatio_ = FLAGS_urma_failover_success_rate_ratio;
        oldMinSampleCount_ = FLAGS_urma_failover_min_sample_count;
        FLAGS_urma_failover_success_rate_ratio = 0.5;
        FLAGS_urma_failover_min_sample_count = MIN_SAMPLE_COUNT;
        ClearClientInjects();
        SetupFastHeartbeat();
        ExternalClusterTest::SetUp();
        isClusterSetUp_ = true;
        clientConfigPath_ = GetTestCaseDataDir() + "/" + CLIENT_CONFIG_FILE;
        ASSERT_EQ(setenv(CLIENT_CONFIG_PATH_ENV.c_str(), clientConfigPath_.c_str(), 1), 0);
        WriteClientConfig(FLAGS_urma_failover_success_rate_ratio, FLAGS_urma_failover_min_sample_count);
#endif
    }

    void TearDown() override
    {
        clients_.clear();
        ClearWorkerInjects();
        ClearClientInjects();
        ClearHostIdEnvs();
        (void)unsetenv(CLIENT_CONFIG_PATH_ENV.c_str());
        FLAGS_urma_failover_success_rate_ratio = oldSuccessRateRatio_;
        FLAGS_urma_failover_min_sample_count = oldMinSampleCount_;
        if (isClusterSetUp_) {
            ExternalClusterTest::TearDown();
            isClusterSetUp_ = false;
        }
    }

protected:
    static std::string GetHostIdEnvName(uint32_t workerIndex)
    {
        return HOST_ID_ENV_PREFIX + std::to_string(workerIndex);
    }

    static std::string GetHostIdValue(uint32_t workerIndex)
    {
        return HOST_ID_VALUE_PREFIX + std::to_string(workerIndex);
    }

    void InitClientByServiceDiscovery(const std::string &hostIdEnvName, std::shared_ptr<KVClient> &client)
    {
        ServiceDiscoveryOptions sdOpts;
        sdOpts.etcdAddress = cluster_->GetEtcdAddrs();
        sdOpts.hostIdEnvName = hostIdEnvName;
        sdOpts.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(sdOpts);
        DS_ASSERT_OK(serviceDiscovery->Init());
        ConnectOptions connectOptions;
        connectOptions.connectTimeoutMs = CONNECT_TIMEOUT_MS;
        connectOptions.requestTimeoutMs = REQUEST_TIMEOUT_MS;
        connectOptions.accessKey = DUMMY_ACCESS_KEY;
        connectOptions.secretKey = DUMMY_SECRET_KEY;
        connectOptions.enableCrossNodeConnection = true;
        connectOptions.serviceDiscovery = serviceDiscovery;
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
        clients_.emplace_back(client);
    }

    void InitLocalPreferredClient(std::shared_ptr<KVClient> &client)
    {
        InitClientByServiceDiscovery(GetHostIdEnvName(0), client);
    }

    void InitRemoteFallbackClient(std::shared_ptr<KVClient> &client)
    {
        ShutdownWorkerAndWaitDiscoveryRemote(0);
        InitClientByServiceDiscovery(GetHostIdEnvName(0), client);
        uint32_t currentWorkerIndex = WORKER_NUM;
        DS_ASSERT_OK(GetCurrentWorkerIndex(client, currentWorkerIndex));
        ASSERT_NE(currentWorkerIndex, 0u);
    }

    void EnableSwitchEndCounter()
    {
        // Switch completion is asynchronous, so tests wait on this inject counter instead of sleeping blindly.
        DS_ASSERT_OK(inject::Clear(SWITCH_END_INJECT));
        DS_ASSERT_OK(inject::Set(SWITCH_END_INJECT, "call()"));
    }

    void InjectClientUrmaWriteError()
    {
        // This inject point makes the client URMA write fail while TCP fallback still carries the KV request.
        DS_ASSERT_OK(inject::Set(URMA_WRITE_ERROR_INJECT, "return()"));
    }

    void ClearClientUrmaWriteError()
    {
        DS_ASSERT_OK(inject::Clear(URMA_WRITE_ERROR_INJECT));
    }

    void SetupFastHeartbeat()
    {
        DS_ASSERT_OK(inject::Set(HEARTBEAT_INTERVAL_INJECT, "call(500)"));
        DS_ASSERT_OK(inject::Set(HEARTBEAT_TIMEOUT_INJECT, "call(500)"));
    }

    void RunTraffic(const std::shared_ptr<KVClient> &client, uint32_t count, const std::string &prefix,
                    uint64_t valueSize = VALUE_SIZE)
    {
        // Set/Get validates the externally visible promise: business traffic succeeds during data-plane recovery.
        std::string value(valueSize, 'x');
        for (uint32_t i = 0; i < count; ++i) {
            std::string key = FormatString("%s_%u_%llu", prefix, i, GetSteadyClockTimeStampMs());
            DS_ASSERT_OK(client->Set(key, value));
            std::string result;
            DS_ASSERT_OK(client->Get(key, result));
            ASSERT_EQ(result.size(), value.size());
            ASSERT_EQ(result, value);
        }
    }

    void RunSetTraffic(const std::shared_ptr<KVClient> &client, uint32_t count, const std::string &prefix,
                       uint64_t valueSize = VALUE_SIZE)
    {
        // Use Set-only traffic for failure windows so successful Get samples do not dilute the failure rate to 50%.
        std::string value(valueSize, 'x');
        for (uint32_t i = 0; i < count; ++i) {
            std::string key = FormatString("%s_%u_%llu", prefix, i, GetSteadyClockTimeStampMs());
            DS_ASSERT_OK(client->Set(key, value));
        }
    }

    void PrepareGetKeys(const std::shared_ptr<KVClient> &client, uint32_t count, const std::string &prefix,
                        GetTrafficData &data, uint64_t valueSize = VALUE_SIZE)
    {
        data.value.assign(valueSize, 'g');
        data.keys.clear();
        data.keys.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            std::string key = FormatString("%s_%u_%llu", prefix, i, GetSteadyClockTimeStampMs());
            DS_ASSERT_OK(client->Set(key, data.value));
            data.keys.emplace_back(std::move(key));
        }
    }

    void RunGetTraffic(const std::shared_ptr<KVClient> &client, const GetTrafficData &data, uint32_t begin,
                       uint32_t count)
    {
        ASSERT_LE(static_cast<uint64_t>(begin) + count, data.keys.size());
        for (uint32_t i = 0; i < count; ++i) {
            std::string result;
            DS_ASSERT_OK(client->Get(data.keys[begin + i], result));
            ASSERT_EQ(result.size(), data.value.size());
            ASSERT_EQ(result, data.value);
        }
    }

    void RunGetErrorTraffic(const std::shared_ptr<KVClient> &client, const GetTrafficData &data, uint32_t begin,
                            uint32_t count)
    {
        ASSERT_LE(static_cast<uint64_t>(begin) + count, data.keys.size());
        for (uint32_t i = 0; i < count; ++i) {
            std::string result;
            Status status = client->Get(data.keys[begin + i], result);
            ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR) << status.ToString();
            ASSERT_NE(status.GetMsg().find("fallback tcp payload rejected by limiter"), std::string::npos)
                << status.ToString();
        }
    }

    void TriggerUnhealthyWindow(const std::shared_ptr<KVClient> &client, const std::string &prefix)
    {
        // The tracker settles a window on the next request after the timeout, so the final request is intentional.
        RunSetTraffic(client, MIN_SAMPLE_COUNT, prefix);
        std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
        RunSetTraffic(client, 1, prefix + "_settle");
    }

    void TriggerUnhealthyGetWindow(const std::shared_ptr<KVClient> &client, const GetTrafficData &data)
    {
        // The final Get only settles the previous window; its sample starts the next window.
        RunGetTraffic(client, data, 0, MIN_SAMPLE_COUNT);
        std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
        RunGetTraffic(client, data, MIN_SAMPLE_COUNT, 1);
    }

    void TriggerUnhealthyGetErrorWindow(const std::shared_ptr<KVClient> &client, const GetTrafficData &data)
    {
        // K_URMA_ERROR is still a data-plane failure sample when the request carried URMA info.
        RunGetErrorTraffic(client, data, 0, MIN_SAMPLE_COUNT);
        std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
        std::string result;
        Status status = client->Get(data.keys[MIN_SAMPLE_COUNT], result);
        ASSERT_TRUE(status.IsOk() || status.GetCode() == StatusCode::K_URMA_ERROR) << status.ToString();
    }

    bool WaitSwitchCountAtLeast(uint64_t expectedCount, uint32_t timeoutSec = SWITCH_TIMEOUT_S)
    {
        Timer timer;
        do {
            uint64_t actualCount = inject::GetExecuteCount(SWITCH_END_INJECT);
            if (actualCount >= expectedCount) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } while (timer.ElapsedSecond() < timeoutSec);
        LOG(INFO) << "[URMA failover test] switch count did not reach " << expectedCount
                  << ", actual: " << inject::GetExecuteCount(SWITCH_END_INJECT);
        return false;
    }

    void InjectWorkerGetUrmaWriteError(uint32_t workerIndex, uint32_t count)
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, URMA_CQE_ERROR_INJECT,
                                               FormatString("%u*call(0, 9)", count)));
        injectedWorkerIndexes_.emplace(workerIndex);
    }

    void ClearWorkerUrmaWriteError(uint32_t workerIndex)
    {
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, URMA_CQE_ERROR_INJECT));
        injectedWorkerIndexes_.erase(workerIndex);
    }

    void WriteClientConfig(double successRateRatio, uint32_t minSampleCount)
    {
        std::string content = FormatString("-urma_failover_success_rate_ratio=%g\n"
                                           "-urma_failover_min_sample_count=%u\n",
                                           successRateRatio, minSampleCount);
        DS_ASSERT_OK(AtomicWriteTextFile(clientConfigPath_, content));
    }

    void WaitClientConfig(double successRateRatio, uint32_t minSampleCount)
    {
        DS_ASSERT_OK(cluster_->WaitForExpectedResult(
            [successRateRatio, minSampleCount]() {
                double actualSuccessRateRatio = FLAGS_urma_failover_success_rate_ratio;
                bool isExpectedSuccessRateRatio =
                    std::abs(actualSuccessRateRatio - successRateRatio) <= SUCCESS_RATE_RATIO_EPSILON;
                CHECK_FAIL_RETURN_STATUS(
                    isExpectedSuccessRateRatio, K_NOT_READY,
                    FormatString("success rate ratio is %g, expect %g", actualSuccessRateRatio, successRateRatio));
                CHECK_FAIL_RETURN_STATUS(FLAGS_urma_failover_min_sample_count == minSampleCount, K_NOT_READY,
                                         FormatString("min sample count is %u, expect %u",
                                                      FLAGS_urma_failover_min_sample_count, minSampleCount));
                return Status::OK();
            },
            SWITCH_TIMEOUT_S, K_OK));
    }

    Status GetWorkerUuid(uint32_t workerIndex, std::string &workerUuid)
    {
        auto db = InitTestEtcdInstance();
        std::unordered_map<HostPort, std::string> uuidMap;
        GetWorkerUuids(db.get(), uuidMap);
        HostPort workerAddr;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddr));
        auto iter = uuidMap.find(workerAddr);
        CHECK_FAIL_RETURN_STATUS(iter != uuidMap.end(), K_NOT_READY,
                                 FormatString("Cannot find worker %s uuid.", workerAddr.ToString()));
        workerUuid = iter->second;
        return Status::OK();
    }

    Status GetCurrentWorkerIndex(const std::shared_ptr<KVClient> &client, uint32_t &workerIndex)
    {
        std::string currentWorkerUuid;
        RETURN_IF_NOT_OK(GetClientWorkerUuid(client, currentWorkerUuid));
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            if (stoppedWorkers_.count(i) > 0) {
                continue;
            }
            std::string workerUuid;
            RETURN_IF_NOT_OK(GetWorkerUuid(i, workerUuid));
            if (workerUuid == currentWorkerUuid) {
                workerIndex = i;
                return Status::OK();
            }
        }
        return Status(K_NOT_READY, FormatString("Cannot map current worker uuid %s to a live worker.",
                                                currentWorkerUuid));
    }

    Status GetClientWorkerUuid(const std::shared_ptr<KVClient> &client, std::string &workerUuid)
    {
        // GenerateKey appends the current worker uuid after ';', which avoids reaching into client internals.
        std::string key;
        RETURN_IF_NOT_OK(client->GenerateKey("", key));
        auto pos = key.rfind(';');
        CHECK_FAIL_RETURN_STATUS(pos != std::string::npos && pos + 1 < key.size(), K_RUNTIME_ERROR,
                                 FormatString("Invalid generated key: %s", key));
        workerUuid = key.substr(pos + 1);
        return Status::OK();
    }

    void CheckClientUsesWorker(const std::shared_ptr<KVClient> &client, uint32_t workerIndex)
    {
        std::string expectedWorkerUuid;
        DS_ASSERT_OK(GetWorkerUuid(workerIndex, expectedWorkerUuid));
        std::string currentWorkerUuid;
        DS_ASSERT_OK(GetClientWorkerUuid(client, currentWorkerUuid));
        ASSERT_EQ(currentWorkerUuid, expectedWorkerUuid);
    }

    Status CheckClientUsesWorkerStatus(const std::shared_ptr<KVClient> &client, uint32_t workerIndex)
    {
        std::string expectedWorkerUuid;
        RETURN_IF_NOT_OK(GetWorkerUuid(workerIndex, expectedWorkerUuid));
        std::string currentWorkerUuid;
        RETURN_IF_NOT_OK(GetClientWorkerUuid(client, currentWorkerUuid));
        CHECK_FAIL_RETURN_STATUS(currentWorkerUuid == expectedWorkerUuid, K_NOT_READY,
                                 FormatString("Current worker uuid is %s, expect worker%u uuid %s.",
                                              currentWorkerUuid, workerIndex, expectedWorkerUuid));
        return Status::OK();
    }

    void WaitClientUsesWorker(const std::shared_ptr<KVClient> &client, uint32_t workerIndex)
    {
        DS_ASSERT_OK(cluster_->WaitForExpectedResult(
            [this, &client, workerIndex]() {
                RETURN_IF_NOT_OK(CheckClientUsesWorkerStatus(client, workerIndex));
                std::string key = FormatString("wait_worker_%u_%llu", workerIndex, GetSteadyClockTimeStampMs());
                std::string value = "service discovery failover keeps KV available";
                RETURN_IF_NOT_OK(client->Set(key, value));
                std::string result;
                RETURN_IF_NOT_OK(client->Get(key, result));
                CHECK_FAIL_RETURN_STATUS(result == value, K_RUNTIME_ERROR, "Unexpected value read from KVClient.");
                return Status::OK();
            },
            SWITCH_TIMEOUT_S, K_OK));
    }

    Status CheckServiceDiscoverySelectsRemoteWorker(const std::string &hostIdEnvName)
    {
        ServiceDiscoveryOptions sdOpts;
        sdOpts.etcdAddress = cluster_->GetEtcdAddrs();
        sdOpts.hostIdEnvName = hostIdEnvName;
        sdOpts.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(sdOpts);
        RETURN_IF_NOT_OK(serviceDiscovery->Init());
        std::string workerIp;
        int workerPort = 0;
        RETURN_IF_NOT_OK(serviceDiscovery->SelectWorker(workerIp, workerPort));
        HostPort selected(workerIp, workerPort);
        CHECK_FAIL_RETURN_STATUS(selected != workerAddresses_[0], K_NOT_READY,
                                 FormatString("Service discovery still selects local worker %s.",
                                              workerAddresses_[0].ToString()));
        return Status::OK();
    }

    void ShutdownWorkerAndWaitDiscoveryRemote(uint32_t workerIndex)
    {
        if (stoppedWorkers_.count(workerIndex) == 0) {
            DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, workerIndex));
            stoppedWorkers_.emplace(workerIndex);
            // After a worker is stopped, wait for node_dead_timeout_s plus a grace second so the failed node is
            // isolated from hash-master routing before the next KV Set creates metadata.
            std::this_thread::sleep_for(std::chrono::seconds(FAILURE_ISOLATION_WAIT_S));
        }
        if (workerIndex == 0) {
            DS_ASSERT_OK(cluster_->WaitForExpectedResult(
                [this]() { return CheckServiceDiscoverySelectsRemoteWorker(GetHostIdEnvName(0)); }, SWITCH_TIMEOUT_S,
                K_OK));
        }
    }

    void RestartWorkerAndWaitReady(uint32_t workerIndex)
    {
        if (stoppedWorkers_.count(workerIndex) == 0) {
            return;
        }
        DS_ASSERT_OK(cluster_->StartNode(WORKER, workerIndex, ""));
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, workerIndex));
        stoppedWorkers_.erase(workerIndex);
    }

    uint32_t GetOtherRemoteWorker(uint32_t workerIndex)
    {
        for (uint32_t i = 1; i < WORKER_NUM; ++i) {
            if (i != workerIndex) {
                return i;
            }
        }
        ADD_FAILURE() << "Cannot find another remote worker for worker " << workerIndex;
        return WORKER_NUM;
    }

    void ClearClientInjects()
    {
        (void)inject::Clear(URMA_WRITE_ERROR_INJECT);
        (void)inject::Clear(SWITCH_END_INJECT);
        (void)inject::Clear(HEARTBEAT_INTERVAL_INJECT);
        (void)inject::Clear(HEARTBEAT_TIMEOUT_INJECT);
    }

    void ClearWorkerInjects()
    {
        if (!isClusterSetUp_) {
            injectedWorkerIndexes_.clear();
            return;
        }
        for (auto workerIndex : injectedWorkerIndexes_) {
            (void)cluster_->ClearInjectAction(WORKER, workerIndex, URMA_CQE_ERROR_INJECT);
        }
        injectedWorkerIndexes_.clear();
    }

    void ClearHostIdEnvs()
    {
        for (uint32_t i = 0; i < WORKER_NUM; ++i) {
            (void)unsetenv(GetHostIdEnvName(i).c_str());
        }
    }

private:
    std::vector<HostPort> workerAddresses_;
    std::vector<std::shared_ptr<KVClient>> clients_;
    std::unordered_set<uint32_t> stoppedWorkers_;
    std::unordered_set<uint32_t> injectedWorkerIndexes_;
    std::string clientConfigPath_;
    double oldSuccessRateRatio_{ 0.5 };
    uint32_t oldMinSampleCount_{ MIN_SAMPLE_COUNT };
    bool isClusterSetUp_{ false };
};

TEST_F(KVClientUrmaFailoverTest, LocalDiscoveryUsesUdsAndDoesNotTriggerUrmaSwitch)
{
    LOG(INFO) << "[URMA failover test] verify local service discovery uses UDS and does not trigger URMA switch";
    std::shared_ptr<KVClient> client;
    InitLocalPreferredClient(client);
    CheckClientUsesWorker(client, 0);
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    TriggerUnhealthyWindow(client, "local_uds_no_switch");
    // Same-host service discovery should use the local UDS/shared-memory path, so a UB write fault is irrelevant.
    ASSERT_EQ(inject::GetExecuteCount(SWITCH_END_INJECT), 0u);
    CheckClientUsesWorker(client, 0);
    RunTraffic(client, 2, "local_uds_no_switch_after");
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_RemoteUbFallbackSwitchByDiscovery)
{
    LOG(INFO) << "[URMA failover test] verify remote service discovery uses UB fallback and switches";
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t initialWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, initialWorkerIndex));
    uint32_t expectedWorkerIndex = GetOtherRemoteWorker(initialWorkerIndex);
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    TriggerUnhealthyWindow(client, "remote_switch");
    // Remote fallback connections have no local UDS transport, so the UB write fault should trigger failover.
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    CheckClientUsesWorker(client, expectedWorkerIndex);
    RunTraffic(client, 2, "remote_switch_after");
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_RemoteGetTcpFallbackSwitchByDiscovery)
{
    LOG(INFO) << "[URMA failover test] verify remote Get TCP fallback switches worker";
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t initialWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, initialWorkerIndex));
    uint32_t expectedWorkerIndex = GetOtherRemoteWorker(initialWorkerIndex);
    GetTrafficData data;
    PrepareGetKeys(client, MIN_SAMPLE_COUNT + 1, "remote_get_tcp_fallback", data);
    // Keep Set samples from key preparation out of the measured worker-to-client Get failure window.
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    EnableSwitchEndCounter();
    InjectWorkerGetUrmaWriteError(initialWorkerIndex, MIN_SAMPLE_COUNT + 1);
    TriggerUnhealthyGetWindow(client, data);
    // Worker-to-client UB failure is hidden by TCP payload fallback, but it still marks the URMA data plane unhealthy.
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    CheckClientUsesWorker(client, expectedWorkerIndex);
    RunTraffic(client, 2, "remote_get_tcp_fallback_after");
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_RemoteGetUrmaErrorSwitchByDiscovery)
{
    LOG(INFO) << "[URMA failover test] verify remote Get K_URMA_ERROR switches worker";
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t initialWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, initialWorkerIndex));
    uint32_t expectedWorkerIndex = GetOtherRemoteWorker(initialWorkerIndex);
    const uint64_t rejectedFallbackSize = UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes;
    GetTrafficData data;
    PrepareGetKeys(client, MIN_SAMPLE_COUNT + 1, "remote_get_urma_error", data, rejectedFallbackSize);
    // Keep Set samples from key preparation out of the measured worker-to-client Get failure window.
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    EnableSwitchEndCounter();
    InjectWorkerGetUrmaWriteError(initialWorkerIndex, MIN_SAMPLE_COUNT + 1);
    TriggerUnhealthyGetErrorWindow(client, data);
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    CheckClientUsesWorker(client, expectedWorkerIndex);
    ClearWorkerUrmaWriteError(initialWorkerIndex);
    RunTraffic(client, 2, "remote_get_urma_error_after");
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_RemoteSwitchFailureKeepsCurrentWorkerAvailable)
{
    LOG(INFO) << "[URMA failover test] verify remote switch failure keeps current worker available";
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t currentWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, currentWorkerIndex));
    uint32_t otherWorkerIndex = GetOtherRemoteWorker(currentWorkerIndex);
    ShutdownWorkerAndWaitDiscoveryRemote(otherWorkerIndex);
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    TriggerUnhealthyWindow(client, "remote_switch_fail");
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    // With no remote candidate in service discovery, URMA_DATA_PLANE_FAILURE must not make the current worker unusable.
    CheckClientUsesWorker(client, currentWorkerIndex);
    RunTraffic(client, 2, "remote_switch_fail_after");
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_HealthyWindowResetsRemoteFailureBackoff)
{
    LOG(INFO) << "[URMA failover test] verify healthy remote UB window resets failed-switch backoff";
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t currentWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, currentWorkerIndex));
    uint32_t otherWorkerIndex = GetOtherRemoteWorker(currentWorkerIndex);
    ShutdownWorkerAndWaitDiscoveryRemote(otherWorkerIndex);
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    TriggerUnhealthyWindow(client, "healthy_reset_first");
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    ClearClientUrmaWriteError();
    // A full healthy remote UB window is required; empty or short windows must not reset the failed-switch backoff.
    RunSetTraffic(client, MIN_SAMPLE_COUNT, "healthy_reset_success_window");
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    RunSetTraffic(client, 1, "healthy_reset_success_settle");
    RestartWorkerAndWaitReady(otherWorkerIndex);
    InjectClientUrmaWriteError();
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    TriggerUnhealthyWindow(client, "healthy_reset_after");
    // If healthy traffic reset the backoff, the restored service-discovery candidate can be tried immediately.
    ASSERT_TRUE(WaitSwitchCountAtLeast(2));
    CheckClientUsesWorker(client, otherWorkerIndex);
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_MinSamplesNoSwitchOnRemoteUbFailure)
{
    LOG(INFO) << "[URMA failover test] verify insufficient remote UB samples do not trigger switch";
    FLAGS_urma_failover_min_sample_count = HIGH_MIN_SAMPLE_COUNT;
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t currentWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, currentWorkerIndex));
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    RunSetTraffic(client, MIN_SAMPLE_COUNT, "min_samples");
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    RunSetTraffic(client, 1, "min_samples_settle");
    // Even a 0% success rate is ignored until the settled window reaches the configured sample floor.
    ASSERT_EQ(inject::GetExecuteCount(SWITCH_END_INJECT), 0u);
    CheckClientUsesWorker(client, currentWorkerIndex);
    RunTraffic(client, 1, "min_samples_after");
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_ThresholdZeroDisablesRemoteUbSwitch)
{
    LOG(INFO) << "[URMA failover test] verify threshold zero disables remote UB failover and restore enables it";
    FLAGS_urma_failover_success_rate_ratio = 0.0;
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    uint32_t initialWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, initialWorkerIndex));
    uint32_t expectedWorkerIndex = GetOtherRemoteWorker(initialWorkerIndex);
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    TriggerUnhealthyWindow(client, "threshold_zero");
    // Threshold zero disables and clears the tracker even though the remote path is seeing UB write failures.
    ASSERT_EQ(inject::GetExecuteCount(SWITCH_END_INJECT), 0u);
    CheckClientUsesWorker(client, initialWorkerIndex);
    FLAGS_urma_failover_success_rate_ratio = 0.5;
    TriggerUnhealthyWindow(client, "threshold_restore");
    // Restoring the threshold starts fresh statistics; the next bad remote UB window may switch via discovery.
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    CheckClientUsesWorker(client, expectedWorkerIndex);
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_DynamicThresholdUpdateAppliesAtWindowSettlement)
{
    LOG(INFO) << "[URMA failover test] verify dynamic threshold update applies at window settlement";
    WriteClientConfig(0.5, MIN_SAMPLE_COUNT);
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    WaitClientConfig(0.5, MIN_SAMPLE_COUNT);
    uint32_t initialWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, initialWorkerIndex));
    uint32_t expectedWorkerIndex = GetOtherRemoteWorker(initialWorkerIndex);
    EnableSwitchEndCounter();
    RunSetTraffic(client, 1, "dynamic_threshold_success");
    InjectClientUrmaWriteError();
    RunSetTraffic(client, 1, "dynamic_threshold_failure");
    WriteClientConfig(0.6, MIN_SAMPLE_COUNT);
    WaitClientConfig(0.6, MIN_SAMPLE_COUNT);
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    RunSetTraffic(client, 1, "dynamic_threshold_settle");
    // The settled window is 50% successful: healthy at ratio 0.5, unhealthy after the hot update to 0.6.
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    CheckClientUsesWorker(client, expectedWorkerIndex);
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_DynamicMinSampleUpdateAppliesAtWindowSettlement)
{
    LOG(INFO) << "[URMA failover test] verify dynamic minimum sample update applies at window settlement";
    WriteClientConfig(0.5, 3);
    std::shared_ptr<KVClient> client;
    InitRemoteFallbackClient(client);
    WaitClientConfig(0.5, 3);
    uint32_t initialWorkerIndex = WORKER_NUM;
    DS_ASSERT_OK(GetCurrentWorkerIndex(client, initialWorkerIndex));
    uint32_t expectedWorkerIndex = GetOtherRemoteWorker(initialWorkerIndex);
    EnableSwitchEndCounter();
    InjectClientUrmaWriteError();
    RunSetTraffic(client, 2, "dynamic_min_first_window");
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    RunSetTraffic(client, 1, "dynamic_min_first_settle");
    ASSERT_EQ(inject::GetExecuteCount(SWITCH_END_INJECT), 0u);
    CheckClientUsesWorker(client, initialWorkerIndex);
    RunSetTraffic(client, 1, "dynamic_min_second_window");
    WriteClientConfig(0.5, MIN_SAMPLE_COUNT);
    WaitClientConfig(0.5, MIN_SAMPLE_COUNT);
    std::this_thread::sleep_for(std::chrono::milliseconds(WINDOW_SETTLE_MS));
    RunSetTraffic(client, 1, "dynamic_min_second_settle");
    // The second settled window has two failures and becomes eligible only after the min-sample hot update.
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    CheckClientUsesWorker(client, expectedWorkerIndex);
}

TEST_F(KVClientUrmaFailoverTest, LEVEL1_LocalFailRemoteFailThenSwitchBack)
{
    LOG(INFO) << "[URMA failover test] verify local worker failure, remote chain, and local switch back";
    std::shared_ptr<KVClient> client;
    InitLocalPreferredClient(client);
    CheckClientUsesWorker(client, 0);
    EnableSwitchEndCounter();
    RunTraffic(client, 1, "chain_local_before");
    // Keep worker2 out of service discovery so worker0 failure has exactly one remote candidate: worker1.
    ShutdownWorkerAndWaitDiscoveryRemote(2);
    ShutdownWorkerAndWaitDiscoveryRemote(0);
    WaitClientUsesWorker(client, 1);
    ASSERT_TRUE(WaitSwitchCountAtLeast(1));
    RunTraffic(client, 1, "chain_worker1_after");
    RestartWorkerAndWaitReady(2);
    ShutdownWorkerAndWaitDiscoveryRemote(1);
    // Worker0 is still down, so worker1 failure should drive the remote client to worker2.
    WaitClientUsesWorker(client, 2);
    ASSERT_TRUE(WaitSwitchCountAtLeast(2));
    RunTraffic(client, 1, "chain_worker2_after");
    RestartWorkerAndWaitReady(0);
    // Once the same-host worker is ready again, service discovery recovery should prefer it automatically.
    WaitClientUsesWorker(client, 0);
    RunTraffic(client, 1, "chain_local_after_recover");
}
}  // namespace st
}  // namespace datasystem
