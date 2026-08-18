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
constexpr char INFLIGHT_SNAPSHOT_INJECT[] = "UrmaManager.OverrideSrcChipInflightSnapshot";
constexpr char GATHER_CHIP_1_SELECTED_INJECT[] = "UrmaManager.GatherSrcChipSelected.1";
constexpr char GATHER_CHIP_2_SELECTED_INJECT[] = "UrmaManager.GatherSrcChipSelected.2";
constexpr char GATHER_INFLIGHT_DRAINED_INJECT[] = "UrmaManager.GatherInflightCountersDrained";
constexpr size_t WORKER_COUNT = 3;
constexpr size_t CLIENT_COUNT = 8;
constexpr size_t THREADS_PER_CLIENT = 16;
constexpr size_t KEYS_PER_CLIENT = 4;
constexpr size_t READS_PER_KEY = 10;
constexpr size_t CONCURRENT_READ_TASKS_PER_CLIENT = 5;
constexpr size_t GATHER_KEY_COUNT = 128;
constexpr size_t GATHER_VALUE_SIZE = 8 * 1024;
constexpr size_t VALUE_SIZE = 8 * 1024 * 1024;
// All logical clients in this ST share the process-wide UrmaManager. The read gate releases five batch requests per
// client, each reserving four 8 MiB receive buffers (1.25 GiB total), so leave headroom for allocator fragmentation
// and write-side buffers retained by requests that are still retiring.
constexpr uint64_t CLIENT_TRANSPORT_MEMORY_SIZE = 2ULL * 1024ULL * 1024ULL * 1024ULL;

class UrmaNumaInflightBalanceTest : public OCClientCommon {
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
            " -ub_numa_rr_type=2 -ub_numa_inflight_wr_diff_threshold=15"
            " -enable_worker_worker_batch_get=true";
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
        ASSERT_EQ(setenv("URMA_MOCK_CHIP_1_LATENCY_US", "20000", 1), 0);
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
        (void)inject::Clear(INFLIGHT_SNAPSHOT_INJECT);
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
        uint64_t totalCount = 0;
        for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
            uint64_t count = 0;
            DS_EXPECT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count));
            totalCount += count;
        }
        EXPECT_GE(totalCount, minimumCount) << "insufficient worker executions for " << name;
    }
};

TEST_F(UrmaNumaInflightBalanceTest, MultiClientOneWriteTenConcurrentReadsBalanceBothSourceChips)
{
#ifdef USE_URMA_MOCK
    DS_ASSERT_OK(inject::Set(BALANCE_OVERRIDE_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(CHIP_1_SELECTED_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(CHIP_2_SELECTED_INJECT, "call()"));
    DS_ASSERT_OK(inject::Set(FORCE_MOCK_AFFINITY_INJECT, "call(1)"));
    // Inject one observation just above the default boundary. The first real WR must override its RR candidate;
    // every subsequent decision uses the live per-chip counters maintained by completion processing.
    DS_ASSERT_OK(inject::Set(INFLIGHT_SNAPSHOT_INJECT, "1*call(16, 0)"));
    for (size_t workerIndex = 0; workerIndex < WORKER_COUNT; ++workerIndex) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, BALANCE_OVERRIDE_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, CHIP_1_SELECTED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, CHIP_2_SELECTED_INJECT, "call()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, FORCE_MOCK_AFFINITY_INJECT, "call(1)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, INFLIGHT_SNAPSHOT_INJECT, "1*call(16, 0)"));
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
    for (auto &future : writeFutures) {
        DS_ASSERT_OK(future.get());
    }

    std::promise<void> readGatePromise;
    auto readGate = readGatePromise.get_future().share();
    std::vector<std::future<Status>> readFutures;
    static_assert(READS_PER_KEY % CONCURRENT_READ_TASKS_PER_CLIENT == 0);
    readFutures.reserve(CLIENT_COUNT * CONCURRENT_READ_TASKS_PER_CLIENT);
    for (size_t clientIndex = 0; clientIndex < CLIENT_COUNT; ++clientIndex) {
        // Read each key through a client attached to a different worker so the owner must execute worker-worker
        // batch Get and its gather-write response path.
        const size_t readerClientIndex = (clientIndex + 1) % CLIENT_COUNT;
        for (size_t taskIndex = 0; taskIndex < CONCURRENT_READ_TASKS_PER_CLIENT; ++taskIndex) {
            readFutures.emplace_back(clientPools[readerClientIndex]->Submit(
                [gate = readGate, client = clients[readerClientIndex], readKeys = keys[clientIndex], &value]() mutable {
                    gate.wait();
                    for (size_t repeat = 0; repeat < READS_PER_KEY / CONCURRENT_READ_TASKS_PER_CLIENT; ++repeat) {
                        std::vector<std::string> results;
                        auto rc = client->Get(readKeys, results);
                        if (rc.IsError()) {
                            return rc;
                        }
                        if (results.size() != readKeys.size()
                            || std::any_of(results.begin(), results.end(), [&value](const auto &result) {
                                   return result != value;
                               })) {
                            return Status(K_RUNTIME_ERROR, "URMA Mock read payload mismatch");
                        }
                    }
                    return Status::OK();
                }));
        }
    }
    readGatePromise.set_value();
    for (auto &future : readFutures) {
        DS_ASSERT_OK(future.get());
    }

    // The production aggregate path currently admits small objects by default. Exercise a batch larger than the
    // worker parallel threshold so the same end-to-end run proves that gather WRs carry chip affinity and retire
    // their inflight counters. The 8 MiB workload above remains the representative one-write/ten-read scenario.
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
    ExpectAnyWorkerExecuted(GATHER_CHIP_1_SELECTED_INJECT);
    ExpectAnyWorkerExecuted(GATHER_CHIP_2_SELECTED_INJECT);
    // At least two zero transitions are required because Gather selected both source-chip counters above.
    ExpectAnyWorkerExecuted(GATHER_INFLIGHT_DRAINED_INJECT, 2);
#endif
}
}  // namespace
}  // namespace st
}  // namespace datasystem
