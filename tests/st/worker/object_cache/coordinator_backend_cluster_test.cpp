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
 * Description: ST for coordinator-backed worker cluster coordination.
 */
#include <unistd.h>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common.h"
#include "cluster/external_cluster.h"
#include "oc_client_common.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/worker/cluster_manager/cluster_constants.h"

namespace datasystem {
namespace st {
namespace {
constexpr int WAIT_RING_TIMEOUT_SEC = 10;
constexpr int WAIT_SCALE_TIMEOUT_SEC = 30;
constexpr int WAIT_RING_INTERVAL_MS = 100;
constexpr size_t TEST_KEY_COUNT = 100;

std::string WorkerStateToString(WorkerPb::StatePb state)
{
    switch (state) {
        case WorkerPb::INITIAL:
            return "INITIAL";
        case WorkerPb::JOINING:
            return "JOINING";
        case WorkerPb::ACTIVE:
            return "ACTIVE";
        case WorkerPb::LEAVING:
            return "LEAVING";
        default:
            return "UNKNOWN(" + std::to_string(static_cast<int>(state)) + ")";
    }
}

std::string WorkerStatesToString(const std::map<std::string, WorkerPb::StatePb> &workers)
{
    std::vector<std::string> workerStates;
    workerStates.reserve(workers.size());
    for (const auto &worker : workers) {
        workerStates.emplace_back(worker.first + ":" + WorkerStateToString(worker.second));
    }
    return VectorToString(workerStates);
}
}  // namespace

class CoordinatorBackendClusterTest : public OCClientCommon {
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
    }

protected:
    Status GetCoordinatorProxy()
    {
        if (coordinatorProxy_ != nullptr) {
            return Status::OK();
        }
        HostPort coordinatorAddr;
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
        RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(100));
        RETURN_IF_NOT_OK(externalCluster->GetCoordinatorAddr(0, coordinatorAddr));
        coordinatorProxy_ = std::make_unique<CoordinatorServiceProxyZmqImpl>(coordinatorAddr);
        return Status::OK();
    }

    Status GetHashRingWorkers(ICoordinatorServiceProxy &proxy, std::map<std::string, WorkerPb::StatePb> &outWorkers)
    {
        std::vector<KeyValueEntry> kvs;
        int64_t revision = 0;
        RETURN_IF_NOT_OK(
            proxy.Range(std::string(HASHRING_TABLE) + "/", "", kvs, revision, DEFAULT_COORDINATOR_RPC_TIMEOUT_MS));
        CHECK_FAIL_RETURN_STATUS(kvs.size() == 1, K_RUNTIME_ERROR,
                                 "Unexpected hash ring entry count: " + std::to_string(kvs.size()));

        HashRingPb ring;
        CHECK_FAIL_RETURN_STATUS(ring.ParseFromString(kvs.front().value), K_RUNTIME_ERROR,
                                 "Failed to parse HashRingPb from coordinator backend");

        outWorkers.clear();
        for (const auto &worker : ring.workers()) {
            outWorkers.emplace(worker.first, worker.second.state());
        }
        LOG(INFO) << "HashRingPb:" << ring.ShortDebugString();
        return Status::OK();
    }

    Status WaitWorkersInCluster(ICoordinatorServiceProxy &proxy, const std::set<std::string> &expectedWorkers,
                                int timeoutSec = WAIT_RING_TIMEOUT_SEC)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc = Status::OK();
        std::map<std::string, WorkerPb::StatePb> lastWorkers;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = GetHashRingWorkers(proxy, lastWorkers);
            if (lastRc.IsOk() && lastWorkers.size() == expectedWorkers.size()) {
                bool allExpectedWorkersActive = true;
                for (const auto &worker : expectedWorkers) {
                    auto iter = lastWorkers.find(worker);
                    if (iter == lastWorkers.end() || iter->second != WorkerPb::ACTIVE) {
                        allExpectedWorkersActive = false;
                        break;
                    }
                }
                if (allExpectedWorkersActive) {
                    return Status::OK();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_RING_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR,
                      "Timed out waiting for expected active workers in coordinator hash ring. "
                      "Expected: "
                          + VectorToString(std::vector<std::string>(expectedWorkers.begin(), expectedWorkers.end()))
                          + ", last workers: " + WorkerStatesToString(lastWorkers)
                          + ", last status: " + lastRc.ToString());
    }

    Status WaitWorkersNotInCluster(ICoordinatorServiceProxy &proxy, const std::set<std::string> &unexpectedWorkers,
                                   int timeoutSec = WAIT_RING_TIMEOUT_SEC)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeoutSec);
        Status lastRc = Status::OK();
        std::map<std::string, WorkerPb::StatePb> lastWorkers;
        while (std::chrono::steady_clock::now() < deadline) {
            lastRc = GetHashRingWorkers(proxy, lastWorkers);
            if (lastRc.IsOk()) {
                bool allUnexpectedWorkersInactive = true;
                for (const auto &worker : unexpectedWorkers) {
                    auto iter = lastWorkers.find(worker);
                    if (iter != lastWorkers.end() && iter->second == WorkerPb::ACTIVE) {
                        allUnexpectedWorkersInactive = false;
                        break;
                    }
                }
                if (allUnexpectedWorkersInactive) {
                    return Status::OK();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_RING_INTERVAL_MS));
        }
        return Status(K_RUNTIME_ERROR,
                      "Timed out waiting for unexpected workers to leave coordinator hash ring. "
                      "Unexpected: "
                          + VectorToString(std::vector<std::string>(unexpectedWorkers.begin(), unexpectedWorkers.end()))
                          + ", last workers: " + WorkerStatesToString(lastWorkers)
                          + ", last status: " + lastRc.ToString());
    }

    std::vector<std::string> BuildKeys(const std::string &prefix)
    {
        std::vector<std::string> keys;
        keys.reserve(TEST_KEY_COUNT);
        for (size_t i = 0; i < TEST_KEY_COUNT; ++i) {
            keys.emplace_back(prefix + "_key_" + std::to_string(i));
        }
        return keys;
    }

    std::unordered_map<std::string, std::string> BuildValues(const std::vector<std::string> &keys,
                                                             const std::string &prefix)
    {
        std::unordered_map<std::string, std::string> values;
        values.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            values.emplace(keys[i], prefix + "_value_" + std::to_string(i));
        }
        return values;
    }

    Status SetKeys(KVClient &client, const std::vector<std::string> &keys,
                   const std::unordered_map<std::string, std::string> &values)
    {
        for (const auto &key : keys) {
            auto iter = values.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != values.end(), K_RUNTIME_ERROR, "Missing expected value for key " + key);
            RETURN_IF_NOT_OK(client.Set(key, iter->second));
        }
        return Status::OK();
    }

    Status GetAndCheckKeys(KVClient &client, const std::vector<std::string> &keys,
                           const std::unordered_map<std::string, std::string> &values)
    {
        for (const auto &key : keys) {
            auto iter = values.find(key);
            CHECK_FAIL_RETURN_STATUS(iter != values.end(), K_RUNTIME_ERROR, "Missing expected value for key " + key);
            std::string value;
            RETURN_IF_NOT_OK(client.Get(key, value));
            std::string errorMsg = "Unexpected value for key ";
            errorMsg.append(key).append(", expected ").append(iter->second).append(", actual ").append(value);
            CHECK_FAIL_RETURN_STATUS(value == iter->second, K_RUNTIME_ERROR, errorMsg);
        }
        return Status::OK();
    }

    void AssertSetKeys(std::shared_ptr<KVClient> &client, const std::vector<std::string> &keys,
                       const std::unordered_map<std::string, std::string> &values)
    {
        ASSERT_NE(client, nullptr);
        DS_ASSERT_OK(SetKeys(*client, keys, values));
    }

    void AssertGetKeysEventually(std::shared_ptr<KVClient> &client, const std::vector<std::string> &keys,
                                 const std::unordered_map<std::string, std::string> &values)
    {
        ASSERT_NE(client, nullptr);
        DS_ASSERT_OK(cluster_->WaitForExpectedResult(
            [this, &client, &keys, &values]() {
                (void)this;
                return GetAndCheckKeys(*client, keys, values);
            },
            WAIT_SCALE_TIMEOUT_SEC, StatusCode::K_OK));
    }

    void InitKVClient(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        InitTestKVClient(workerIndex, client);
    }

    void AssertWorkersInCluster(const std::vector<int> &workerIndexes, int timeoutSec = WAIT_SCALE_TIMEOUT_SEC)
    {
        std::set<std::string> expectedWorkers;
        for (int workerIndex : workerIndexes) {
            HostPort worker;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
            expectedWorkers.insert(worker.ToString());
        }
        DS_ASSERT_OK(GetCoordinatorProxy());
        ASSERT_NE(coordinatorProxy_, nullptr);
        DS_ASSERT_OK(WaitWorkersInCluster(*coordinatorProxy_, expectedWorkers, timeoutSec));
    }

    void AssertWorkersNotInCluster(const std::vector<int> &workerIndexes, int timeoutSec = WAIT_SCALE_TIMEOUT_SEC)
    {
        std::set<std::string> unexpectedWorkers;
        for (int workerIndex : workerIndexes) {
            HostPort worker;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, worker));
            unexpectedWorkers.insert(worker.ToString());
        }
        DS_ASSERT_OK(GetCoordinatorProxy());
        ASSERT_NE(coordinatorProxy_, nullptr);
        DS_ASSERT_OK(WaitWorkersNotInCluster(*coordinatorProxy_, unexpectedWorkers, timeoutSec));
    }

    Status AddWorkerAndWaitReady(uint32_t workerIndex, HostPort &workerAddr)
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        CHECK_FAIL_RETURN_STATUS(externalCluster != nullptr, K_RUNTIME_ERROR, "Not an ExternalCluster");
        HostPort masterAddr;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(0, masterAddr));
        workerAddr = HostPort("127.0.0.1", GetFreePort());
        const int directPort = GetFreePort();
        RETURN_IF_NOT_OK(externalCluster->AddNode(masterAddr, workerAddr.ToString(), directPort));
        return cluster_->WaitNodeReady(WORKER, workerIndex, WAIT_SCALE_TIMEOUT_SEC);
    }

    std::unique_ptr<ICoordinatorServiceProxy> coordinatorProxy_;
};

class CoordinatorBackendClusterThreeWorkerTest : public CoordinatorBackendClusterTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        CoordinatorBackendClusterTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 3;
    }
};

TEST_F(CoordinatorBackendClusterTest, TwoWorkersCanReadKeysAcrossWorkers)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);

    const auto keys = BuildKeys("two_workers_cross_read");
    const auto values = BuildValues(keys, "two_workers_cross_read");
    AssertSetKeys(client0, keys, values);
    AssertGetKeysEventually(client1, keys, values);
}

TEST_F(CoordinatorBackendClusterTest, AddedWorkerCanWriteKeysReadableFromExistingWorker)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);

    const auto initialKeys = BuildKeys("scale_up_initial");
    const auto initialValues = BuildValues(initialKeys, "scale_up_initial");
    AssertSetKeys(client0, initialKeys, initialValues);
    AssertGetKeysEventually(client1, initialKeys, initialValues);

    HostPort worker2;
    DS_ASSERT_OK(AddWorkerAndWaitReady(2, worker2));

    AssertWorkersInCluster({ 0, 1, 2 }, WAIT_SCALE_TIMEOUT_SEC);

    std::shared_ptr<KVClient> client2;
    InitKVClient(2, client2);
    const auto scaleUpKeys = BuildKeys("scale_up_new_worker");
    const auto scaleUpValues = BuildValues(scaleUpKeys, "scale_up_new_worker");
    AssertSetKeys(client2, scaleUpKeys, scaleUpValues);
    AssertGetKeysEventually(client1, scaleUpKeys, scaleUpValues);
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, GracefulWorkerExitKeepsExistingKeysReadable)
{
    auto t0 = std::chrono::steady_clock::now();
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] InitKVClient done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    const auto keys = BuildKeys("graceful_scale_down");
    const auto values = BuildValues(keys, "graceful_scale_down");
    AssertSetKeys(client0, keys, values);
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] SetKeys before graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    AssertGetKeysEventually(client1, keys, values);
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] GetKeys before graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    auto t4 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] ShutdownNode graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count() << "ms";

    AssertWorkersInCluster({ 0, 1 });
    auto t5 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] AssertWorkersInCluster after graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t5 - t4).count() << "ms";

    AssertGetKeysEventually(client1, keys, values);
    auto t6 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] GetKeys after graceful exit done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t5).count() << "ms";
    LOG(INFO) << "[TIMING] GracefulWorkerExitKeepsExistingKeysReadable total test time: "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t0).count() << "ms";
}

TEST_F(CoordinatorBackendClusterThreeWorkerTest, KilledWorkerScaleDownAllowsNewWritesReadableFromOtherWorker)
{
    auto t0 = std::chrono::steady_clock::now();
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitKVClient(0, client0);
    InitKVClient(1, client1);
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] InitKVClient done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    const auto initialKeys = BuildKeys("passive_scale_down_initial");
    const auto initialValues = BuildValues(initialKeys, "passive_scale_down_initial");
    AssertSetKeys(client0, initialKeys, initialValues);
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Initial SetKeys done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    AssertGetKeysEventually(client1, initialKeys, initialValues);
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Initial GetKeys done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->KillWorker(2));
    auto t4 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] KillWorker done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count() << "ms";

    AssertWorkersNotInCluster({ 2 });
    sleep(3);
    auto t5 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] AssertWorkersInCluster done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t5 - t4).count() << "ms";

    const auto newKeys = BuildKeys("passive_scale_down_new_write");
    const auto newValues = BuildValues(newKeys, "passive_scale_down_new_write");
    AssertSetKeys(client0, newKeys, newValues);
    auto t6 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] New SetKeys after kill done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t6 - t5).count() << "ms";

    AssertGetKeysEventually(client1, newKeys, newValues);
    auto t7 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] New GetKeys after kill done in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t7 - t6).count() << "ms";
    LOG(INFO) << "[TIMING] Total test time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t7 - t0).count()
              << "ms";
}

TEST_F(CoordinatorBackendClusterTest, RestartWorkerPropagatesRingByCoordinatorWatch)
{
    auto t0 = std::chrono::steady_clock::now();
    AssertWorkersInCluster({ 0, 1 }, WAIT_RING_TIMEOUT_SEC);
    auto t1 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] First cluster state confirmed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() << "ms";

    auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    ASSERT_NE(externalCluster, nullptr);
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 1 }, SIGKILL, WAIT_RING_TIMEOUT_SEC));
    auto t2 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Worker restart + ready in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "ms";

    AssertWorkersInCluster({ 0, 1 }, WAIT_RING_TIMEOUT_SEC);
    auto t3 = std::chrono::steady_clock::now();
    LOG(INFO) << "[TIMING] Cluster state after restart confirmed in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count() << "ms";
}
}  // namespace st
}  // namespace datasystem
