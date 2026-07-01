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
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "cluster/external_cluster.h"
#include "oc_client_common.h"
#include "datasystem/utils/service_discovery.h"

namespace datasystem {
namespace st {
namespace {
constexpr char COORDINATOR_SD_HOST_ID_ENV0[] = "coordinator_sd_host_id_env0";
constexpr char COORDINATOR_SD_HOST_ID_ENV1[] = "coordinator_sd_host_id_env1";
constexpr char COORDINATOR_SD_HOST_ID_ENV_MISSING[] = "coordinator_sd_host_id_env_missing";
constexpr char COORDINATOR_SD_HOST_ID_VALUE0[] = "coordinator_sd_host_id0";
constexpr char COORDINATOR_SD_HOST_ID_VALUE1[] = "coordinator_sd_host_id1";
constexpr char COORDINATOR_SD_HOST_ID_VALUE_MISSING[] = "coordinator_sd_host_id_missing";
constexpr int COORDINATOR_SD_SELECT_LOOP_COUNT = 5;
constexpr int COORDINATOR_SD_CONNECT_TIMEOUT_MS = 60000;
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
        opts.coordinatorGflagParams = " -v=1";

        ASSERT_EQ(setenv(COORDINATOR_SD_HOST_ID_ENV0, COORDINATOR_SD_HOST_ID_VALUE0, 1), 0);
        ASSERT_EQ(setenv(COORDINATOR_SD_HOST_ID_ENV1, COORDINATOR_SD_HOST_ID_VALUE1, 1), 0);
        ASSERT_EQ(setenv(COORDINATOR_SD_HOST_ID_ENV_MISSING, COORDINATOR_SD_HOST_ID_VALUE_MISSING, 1), 0);
        opts.workerSpecifyGflagParams[0] = FormatString("-host_id_env_name=%s", COORDINATOR_SD_HOST_ID_ENV0);
        opts.workerSpecifyGflagParams[1] = FormatString("-host_id_env_name=%s", COORDINATOR_SD_HOST_ID_ENV1);
    }

protected:
    void GetCoordinatorServiceDiscovery(const std::string &hostIdEnvName, ServiceAffinityPolicy policy,
                                        std::shared_ptr<CoordinatorServiceDiscovery> &serviceDiscovery)
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster, nullptr);
        HostPort coordinatorAddr;
        DS_ASSERT_OK(externalCluster->GetCoordinatorAddr(0, coordinatorAddr));
        CoordinatorServiceDiscoveryOptions opts;
        opts.serviceAddress = coordinatorAddr.ToString();
        opts.hostIdEnvName = hostIdEnvName;
        opts.affinityPolicy = policy;
        serviceDiscovery = std::make_shared<CoordinatorServiceDiscovery>(opts);
        DS_ASSERT_OK(serviceDiscovery->Init());
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

TEST_F(CoordinatorServiceDiscoveryTest, RandomSelectsReadyWorker)
{
    AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV0, ServiceAffinityPolicy::RANDOM);
    AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV1, ServiceAffinityPolicy::RANDOM);
    AssertSelectedWorkerInCluster(COORDINATOR_SD_HOST_ID_ENV_MISSING, ServiceAffinityPolicy::RANDOM);
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
}  // namespace st
}  // namespace datasystem
