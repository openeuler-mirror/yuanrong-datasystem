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

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <unistd.h>

#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/kv_client.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr char HOST_IP[] = "127.0.0.1";
constexpr char BALANCE_OVERRIDE_INJECT[] = "UrmaManager.SrcChipInflightBalanceOverride";
constexpr char CHIP_1_SELECTED_INJECT[] = "UrmaManager.SrcChipSelected.1";
constexpr char CHIP_2_SELECTED_INJECT[] = "UrmaManager.SrcChipSelected.2";
constexpr char FORCE_MOCK_AFFINITY_INJECT[] = "UrmaManager.ForceNumaAffinityForMock";
constexpr char ROUND_ROBIN_POLICY_INJECT[] = "UrmaManager.SrcChipPolicy.RoundRobin";
constexpr char ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT[] = "UrmaManager.SrcChipPolicy.RoundRobinWithAffinity";
constexpr char AFFINITY_OVERRIDE_INJECT[] = "UrmaManager.SrcChipAffinityOverride";
constexpr char POLICY_DECISION_INJECT[] = "UrmaManager.OverrideSrcChipPolicyDecision";
constexpr char GATHER_DOMINANT_CHIP_INJECT[] = "UrmaManager.OverrideGatherDominantSrcChip";
constexpr char GATHER_CHIP_1_SELECTED_INJECT[] = "UrmaManager.GatherSrcChipSelected.1";
constexpr char GATHER_CHIP_2_SELECTED_INJECT[] = "UrmaManager.GatherSrcChipSelected.2";
constexpr char GATHER_INFLIGHT_DRAINED_INJECT[] = "UrmaManager.GatherInflightCountersDrained";
constexpr size_t WORKER_COUNT = 3;
constexpr size_t CLIENT_COUNT = 8;
constexpr size_t THREADS_PER_CLIENT = 16;
constexpr size_t KEYS_PER_CLIENT = 4;
constexpr size_t READS_PER_KEY = 10;
constexpr size_t GATHER_KEY_COUNT = 128;
constexpr size_t GATHER_VALUE_SIZE = 8 * 1024;
constexpr size_t VALUE_SIZE = 8 * 1024 * 1024;
// All logical clients in this ST share the process-wide UrmaManager. The read gate releases 16 requests per client,
// each reserving one 8 MiB receive buffer (1 GiB total), so leave headroom for allocator fragmentation and write-side
// buffers retained by requests that are still retiring.
constexpr uint64_t CLIENT_TRANSPORT_MEMORY_SIZE = 2ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr uint32_t ROUND_ROBIN_POLICY = 0;
constexpr uint32_t ROUND_ROBIN_WITH_AFFINITY_POLICY = 1;
#ifndef URMA_NUMA_SRC_CHIP_POLICY_TEST_VALUE
#error "URMA_NUMA_SRC_CHIP_POLICY_TEST_VALUE must select one policy for this process"
#endif
static_assert(URMA_NUMA_SRC_CHIP_POLICY_TEST_VALUE == ROUND_ROBIN_POLICY
              || URMA_NUMA_SRC_CHIP_POLICY_TEST_VALUE == ROUND_ROBIN_WITH_AFFINITY_POLICY);

class LEVEL1_UrmaNumaInflightBalanceTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_COUNT;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        for (size_t i = 0; i < WORKER_COUNT; ++i) {
            opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        }
        opts.vLogLevel = 1;
        opts.workerGflagParams =
            " -shared_memory_size_mb=5120 -payload_nocopy_threshold=1000000"
            " -arena_per_tenant=1 -enable_urma=true -ipc_through_shared_memory=false"
            " -enable_transport_fallback=false -enable_ub_numa_affinity=true"
            " -ub_numa_rr_type=1 -ub_numa_inflight_wr_diff_threshold=15 -ub_numa_src_chip_policy="
            + std::to_string(sourceChipPolicy_) + " -enable_worker_worker_batch_get=true";
    }

    void SetUp() override
    {
#ifndef USE_URMA_MOCK
        GTEST_SKIP() << "This end-to-end test requires BUILD_WITH_URMA_MOCK.";
#else
        ImmutableStringPool::Instance().Init();
        intern::StringPool::InitAll();
        const std::string mockUdsDir = "/tmp/ds_urma_numa_balance_" + std::to_string(getpid());
        ASSERT_EQ(setenv("URMA_MOCK_UDS_BASE_DIR", mockUdsDir.c_str(), 1), 0);
        ASSERT_EQ(setenv("URMA_MOCK_THREAD_POOL_SIZE", "64", 1), 0);
        ASSERT_EQ(setenv("URMA_MOCK_QUEUE_CAP", "1024", 1), 0);
        ASSERT_EQ(setenv("URMA_MOCK_CHIP_1_LATENCY_US", "100", 1), 0);
        ASSERT_EQ(setenv("URMA_MOCK_CHIP_2_LATENCY_US", "0", 1), 0);
        ASSERT_EQ(setenv("DATASYSTEM_UB_TRANSPORT_ARENA_NUM", "2", 1), 0);
        ExternalClusterTest::SetUp();
#endif
    }

    void TearDown() override
    {
#ifdef USE_URMA_MOCK
        (void)inject::Clear(BALANCE_OVERRIDE_INJECT);
        (void)inject::Clear(CHIP_1_SELECTED_INJECT);
        (void)inject::Clear(CHIP_2_SELECTED_INJECT);
        (void)inject::Clear(FORCE_MOCK_AFFINITY_INJECT);
        (void)inject::Clear(ROUND_ROBIN_POLICY_INJECT);
        (void)inject::Clear(ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT);
        (void)inject::Clear(AFFINITY_OVERRIDE_INJECT);
        (void)inject::Clear(POLICY_DECISION_INJECT);
        (void)inject::Clear(GATHER_DOMINANT_CHIP_INJECT);
        (void)inject::Clear(GATHER_CHIP_1_SELECTED_INJECT);
        (void)inject::Clear(GATHER_CHIP_2_SELECTED_INJECT);
        (void)inject::Clear(GATHER_INFLIGHT_DRAINED_INJECT);
        ExternalClusterTest::TearDown();
        (void)unsetenv("URMA_MOCK_UDS_BASE_DIR");
        (void)unsetenv("URMA_MOCK_THREAD_POOL_SIZE");
        (void)unsetenv("URMA_MOCK_QUEUE_CAP");
        (void)unsetenv("URMA_MOCK_CHIP_1_LATENCY_US");
        (void)unsetenv("URMA_MOCK_CHIP_2_LATENCY_US");
        (void)unsetenv("DATASYSTEM_UB_TRANSPORT_ARENA_NUM");
#endif
    }

protected:
    void ExpectEveryWorkerExecuted(const std::string &name)
    {
        for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
            uint64_t count = 0;
            DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count));
            EXPECT_GT(count, 0u) << "worker index " << workerIndex << " did not execute " << name;
        }
    }

    void ExpectAnyWorkerExecuted(const std::string &name, uint64_t minimumCount = 1)
    {
        const uint64_t totalCount = GetWorkerExecuteCount(name);
        EXPECT_GE(totalCount, minimumCount) << "insufficient worker executions for " << name;
    }

    uint64_t GetWorkerExecuteCount(const std::string &name)
    {
        uint64_t totalCount = 0;
        for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
            uint64_t count = 0;
            DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count));
            totalCount += count;
        }
        return totalCount;
    }

    void ExpectNoWorkerExecuted(const std::string &name)
    {
        for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
            uint64_t count = 0;
            DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count));
            EXPECT_EQ(count, 0u) << "worker index " << workerIndex << " unexpectedly executed " << name;
        }
    }

    void RunMultiClientOneWriteTenConcurrentReadsBalanceBothSourceChips();

private:
    const uint32_t sourceChipPolicy_ = URMA_NUMA_SRC_CHIP_POLICY_TEST_VALUE;
};

void LEVEL1_UrmaNumaInflightBalanceTest::RunMultiClientOneWriteTenConcurrentReadsBalanceBothSourceChips()
{
#ifdef USE_URMA_MOCK
    DS_ASSERT_OK(inject::Set(BALANCE_OVERRIDE_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(CHIP_1_SELECTED_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(CHIP_2_SELECTED_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(FORCE_MOCK_AFFINITY_INJECT, "call(1)"));
    DS_ASSERT_OK(inject::Set(ROUND_ROBIN_POLICY_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(AFFINITY_OVERRIDE_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(POLICY_DECISION_INJECT, "1*call(1, 16, 0, 0)->1*call(2, 0, 2, 0)"));
    // Override two complete decisions atomically: the first forces hard depth correction and the second gives policy 1
    // a free affinity opportunity. Later decisions use live counters maintained by completions.
    for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, BALANCE_OVERRIDE_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, CHIP_1_SELECTED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, CHIP_2_SELECTED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, FORCE_MOCK_AFFINITY_INJECT, "call(1)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, ROUND_ROBIN_POLICY_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, AFFINITY_OVERRIDE_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, POLICY_DECISION_INJECT,
                                               "1*call(1, 16, 0, 0)->1*call(2, 0, 2, 0)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, GATHER_CHIP_1_SELECTED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, GATHER_CHIP_2_SELECTED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, GATHER_INFLIGHT_DRAINED_INJECT, "call()"));
    }

    std::vector<std::shared_ptr<KVClient>> clients(CLIENT_COUNT);
    std::vector<std::unique_ptr<ThreadPool>> clientPools;
    clientPools.reserve(CLIENT_COUNT);
    for (size_t i = 0; i < CLIENT_COUNT; ++i) {
        // A successful Init is also the end-to-end check that the two-arena client transport pool was created.
        ConnectOptions options;
        InitConnectOpt(i % WORKER_COUNT, options, 60000, true);
        options.fastTransportMemSize = CLIENT_TRANSPORT_MEMORY_SIZE;
        clients[i] = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(clients[i]->Init());
        clientPools.emplace_back(std::make_unique<ThreadPool>(THREADS_PER_CLIENT));
    }

    const std::string value(VALUE_SIZE, 'v');
    std::vector<std::vector<std::string>> keys(CLIENT_COUNT);
    std::promise<void> writeGatePromise;
    auto writeGate = writeGatePromise.get_future().share();
    std::vector<std::future<Status>> writeFutures;
    writeFutures.reserve(CLIENT_COUNT * KEYS_PER_CLIENT);
    for (size_t clientIndex = 0; clientIndex < CLIENT_COUNT; ++clientIndex) {
        keys[clientIndex].reserve(KEYS_PER_CLIENT);
        for (size_t keyIndex = 0; keyIndex < KEYS_PER_CLIENT; ++keyIndex) {
            keys[clientIndex].emplace_back("urma-numa-balance-" + GetStringUuid());
            writeFutures.emplace_back(clientPools[clientIndex]->Submit(
                [gate = writeGate, client = clients[clientIndex], key = keys[clientIndex].back(), &value]() mutable {
                    gate.wait();
                    return client->Set(key, value);
                }));
        }
    }
    writeGatePromise.set_value();
    bool allWritesSucceeded = true;
    for (auto &future : writeFutures) {
        auto rc = future.get();
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        allWritesSucceeded = allWritesSucceeded && rc.IsOk();
    }
    ASSERT_TRUE(allWritesSucceeded);

    std::promise<void> readGatePromise;
    auto readGate = readGatePromise.get_future().share();
    std::promise<void> allReadThreadsReadyPromise;
    auto allReadThreadsReady = allReadThreadsReadyPromise.get_future();
    std::atomic<size_t> readyReadThreadCount{ 0 };
    std::vector<std::future<Status>> readFutures;
    readFutures.reserve(CLIENT_COUNT * KEYS_PER_CLIENT * READS_PER_KEY);
    for (size_t clientIndex = 0; clientIndex < CLIENT_COUNT; ++clientIndex) {
        // Read each key through a client attached to a different worker so the owner must execute worker-worker
        // Get. Key-major submission puts all ten reads of the first key behind the same gate while filling all 16
        // threads in each Client pool.
        const size_t readerClientIndex = (clientIndex + 1) % CLIENT_COUNT;
        for (const auto &key : keys[clientIndex]) {
            for (size_t repeat = 0; repeat < READS_PER_KEY; ++repeat) {
                readFutures.emplace_back(clientPools[readerClientIndex]->Submit(
                    [gate = readGate, client = clients[readerClientIndex], key, &value, &readyReadThreadCount,
                     &allReadThreadsReadyPromise]() mutable {
                        if (readyReadThreadCount.fetch_add(1, std::memory_order_relaxed) + 1
                            == CLIENT_COUNT * THREADS_PER_CLIENT) {
                            allReadThreadsReadyPromise.set_value();
                        }
                        gate.wait();
                        std::vector<std::string> results;
                        auto rc = client->Get({ key }, results);
                        if (rc.IsError()) {
                            return rc;
                        }
                        if (results.size() != 1 || results.front() != value) {
                            return Status(K_RUNTIME_ERROR, "URMA Mock read payload mismatch");
                        }
                        return Status::OK();
                    }));
            }
        }
    }
    const auto allThreadsReadyStatus = allReadThreadsReady.wait_for(std::chrono::seconds(30));
    readGatePromise.set_value();
    EXPECT_EQ(allThreadsReadyStatus, std::future_status::ready);
    bool allReadsSucceeded = true;
    for (auto &future : readFutures) {
        auto rc = future.get();
        EXPECT_TRUE(rc.IsOk()) << rc.ToString();
        allReadsSucceeded = allReadsSucceeded && rc.IsOk();
    }
    ASSERT_TRUE(allReadsSucceeded);

    // The production aggregate path currently admits small objects by default. Exercise a batch larger than the
    // worker parallel threshold so the same end-to-end run proves that gather WRs carry chip affinity and retire
    // their inflight counters. The 8 MiB workload above remains the representative one-write/ten-read scenario.
    for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
        DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, workerIndex, POLICY_DECISION_INJECT));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, POLICY_DECISION_INJECT, "call(1, 2, 0, 0)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, GATHER_DOMINANT_CHIP_INJECT,
                                               "1*call(1)->1*call(2)->1*call(1)->1*call(2)->1*call(1)->"
                                               "1*call(2)->1*call(1)->1*call(2)->1*call(1)->1*call(2)"));
    }
    std::vector<std::string> gatherKeys;
    std::vector<std::string> gatherValues;
    gatherKeys.reserve(GATHER_KEY_COUNT);
    gatherValues.reserve(GATHER_KEY_COUNT);
    for (size_t i = 0; i < GATHER_KEY_COUNT; ++i) {
        gatherKeys.emplace_back("urma-numa-gather-" + GetStringUuid());
        gatherValues.emplace_back(GATHER_VALUE_SIZE, static_cast<char>('a' + i % 26));
        DS_ASSERT_OK(clients[0]->Set(gatherKeys.back(), gatherValues.back()));
    }
    std::vector<std::string> gatherResults;
    DS_ASSERT_OK(clients[1]->Get(gatherKeys, gatherResults));
    ASSERT_EQ(gatherResults, gatherValues);

    EXPECT_GT(inject::GetExecuteCount(BALANCE_OVERRIDE_INJECT), 0u);
    EXPECT_GT(inject::GetExecuteCount(CHIP_1_SELECTED_INJECT), 0u);
    EXPECT_GT(inject::GetExecuteCount(CHIP_2_SELECTED_INJECT), 0u);
    ExpectEveryWorkerExecuted(BALANCE_OVERRIDE_INJECT);
    ExpectEveryWorkerExecuted(CHIP_1_SELECTED_INJECT);
    ExpectEveryWorkerExecuted(CHIP_2_SELECTED_INJECT);
    if (sourceChipPolicy_ == ROUND_ROBIN_POLICY) {
        const auto gatherChip1Count = GetWorkerExecuteCount(GATHER_CHIP_1_SELECTED_INJECT);
        const auto gatherChip2Count = GetWorkerExecuteCount(GATHER_CHIP_2_SELECTED_INJECT);
        EXPECT_TRUE((gatherChip1Count == 0) != (gatherChip2Count == 0));
        ExpectAnyWorkerExecuted(GATHER_INFLIGHT_DRAINED_INJECT);
        EXPECT_GT(inject::GetExecuteCount(ROUND_ROBIN_POLICY_INJECT), 0u);
        EXPECT_EQ(inject::GetExecuteCount(ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT), 0u);
        ExpectEveryWorkerExecuted(ROUND_ROBIN_POLICY_INJECT);
        ExpectNoWorkerExecuted(ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT);
        EXPECT_EQ(inject::GetExecuteCount(AFFINITY_OVERRIDE_INJECT), 0u);
        ExpectNoWorkerExecuted(AFFINITY_OVERRIDE_INJECT);
    } else {
        ExpectAnyWorkerExecuted(GATHER_CHIP_1_SELECTED_INJECT);
        ExpectAnyWorkerExecuted(GATHER_CHIP_2_SELECTED_INJECT);
        ExpectAnyWorkerExecuted(GATHER_INFLIGHT_DRAINED_INJECT, 2);
        EXPECT_GT(inject::GetExecuteCount(ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT), 0u);
        EXPECT_EQ(inject::GetExecuteCount(ROUND_ROBIN_POLICY_INJECT), 0u);
        EXPECT_GT(inject::GetExecuteCount(AFFINITY_OVERRIDE_INJECT), 0u);
        ExpectEveryWorkerExecuted(ROUND_ROBIN_WITH_AFFINITY_POLICY_INJECT);
        ExpectEveryWorkerExecuted(AFFINITY_OVERRIDE_INJECT);
        ExpectNoWorkerExecuted(ROUND_ROBIN_POLICY_INJECT);
    }
#endif
}

TEST_F(LEVEL1_UrmaNumaInflightBalanceTest, MultiClientOneWriteTenConcurrentReadsBalanceBothSourceChips)
{
    RunMultiClientOneWriteTenConcurrentReadsBalanceBothSourceChips();
}
}  // namespace
}  // namespace st
}  // namespace datasystem
