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
* Description: Memory rebalance end-to-end system tests.
*/

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/kv_cache/kv_client_common.h"
#include "cluster/external_cluster.h"
#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER0 = 0;
constexpr uint32_t WORKER1 = 1;
constexpr uint32_t WORKER2 = 2;
constexpr size_t VALUE_SIZE = 5 * 1024UL * 1024UL;
constexpr size_t SPILL_VALUE_SIZE = 4 * 1024UL * 1024UL;
constexpr int GET_TIMEOUT_MS = 30'000;
constexpr int REBALANCE_TIMEOUT_MS = 25'000;
constexpr int HASH_RING_TIMEOUT_MS = 60'000;
constexpr int SHORT_WAIT_MS = 3'000;
constexpr int POLL_INTERVAL_MS = 100;
constexpr int WORKER_RECEIVE_RING_DELAY_MS = 1'000;
constexpr char SCALE_UP_CASE_NAME[] = "ScaleUpNewWorkerParticipatesInRebalance";
constexpr char USAGE_GAP_BELOW_THRESHOLD_CASE_NAME[] = "UsageGapBelowThresholdDoesNotDispatchTask";

const std::string SOURCE_SEND_POINT = "TcpMigrateTransport.MigrateDataToRemote.delay";
const std::string TARGET_MIGRATE_POINT = "worker.migrate_service.return";
const std::string TARGET_MEMORY_AVAILABLE_POINT = "worker.migrate_service.memory_available";
const std::string ASSIGN_TASK_POINT = "MemoryRebalanceScheduler.AssignTask";
const std::string EXPIRE_TASK_POINT = "MemoryRebalanceScheduler.ExpireTask";
const std::string REPLACE_PRIMARY_POINT = "OCMetadataManager.ReplacePrimary";
const std::string REPORT_RESULT_POINT = "MasterOCServiceImpl.ReportRebalanceResult";

struct ObjectBatch {
   std::vector<std::string> keys;
   std::string value;
};

std::vector<uint32_t> AllWorkers()
{
   return { WORKER0, WORKER1, WORKER2 };
}

bool IsScaleUpCase()
{
   std::string caseName;
   std::string name;
   GetCurTestName(caseName, name);
   return name == SCALE_UP_CASE_NAME;
}

bool IsUsageGapBelowThresholdCase()
{
   std::string caseName;
   std::string name;
   GetCurTestName(caseName, name);
   return name == USAGE_GAP_BELOW_THRESHOLD_CASE_NAME;
}

std::string BuildRebalanceInjectActions()
{
   return "NodeSelector.setInterval:call(200);"
          "ResourceManager.setInterval:call(200);"
          "TcpMigrateTransport.MigrateDataToRemote.delay:100000*call();"
          "MemoryRebalanceScheduler.AssignTask:100000*call();"
          "MemoryRebalanceScheduler.ExpireTask:100000*call();"
          "worker.migrate_service.return:100000*call()";
}

void SleepMs(int timeoutMs)
{
   std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
}
}  // namespace

class LEVEL1_KVClientMemoryRebalanceTest : public KVClientCommon {
public:
   LEVEL1_KVClientMemoryRebalanceTest() = default;
   ~LEVEL1_KVClientMemoryRebalanceTest() override = default;

   void SetClusterSetupOptions(ExternalClusterOptions &opts) override
   {
       opts.numWorkers = IsScaleUpCase() ? 2 : 3;
       opts.numEtcd = 1;
       opts.numOBS = 0;
       std::string usageGapPercent = IsUsageGapBelowThresholdCase() ? "100" : "30";
       opts.workerGflagParams =
           "-shared_memory_size_mb=64 -log_monitor=true -enable_memory_rebalance=true "
           "-rebalance_usage_gap_percent=" + usageGapPercent + " -rebalance_source_usage_percent=70 "
           "-rebalance_cooldown_s=1 -rebalance_task_report_grace_ms=500 "
           "-data_migrate_rate_limit_mb=1024";
       opts.injectActions = BuildRebalanceInjectActions();
   }

   void SetUp() override
   {
       ExternalClusterTest::SetUp();
       InitTestKVClient(WORKER0, client0_);
       InitTestKVClient(WORKER1, client1_);
       if (!IsScaleUpCase()) {
           InitTestKVClient(WORKER2, client2_);
       }
   }

   void TearDown() override
   {
       client0_.reset();
       client1_.reset();
       client2_.reset();
       ExternalClusterTest::TearDown();
   }

protected:
   ObjectBatch WriteObjects(const std::shared_ptr<KVClient> &client, const std::string &prefix, int count,
                            char valueChar = 'a', size_t valueSize = VALUE_SIZE,
                            const SetParam *setParam = nullptr)
   {
       ObjectBatch batch;
       if (client == nullptr) {
           ADD_FAILURE() << "client is null";
           return batch;
       }
       batch.value.assign(valueSize, valueChar);
       batch.keys.reserve(count);
       for (int i = 0; i < count; ++i) {
           auto key = prefix + "_" + std::to_string(i);
           Status rc;
           if (setParam == nullptr) {
               rc = client->Set(key, batch.value);
           } else {
               rc = client->Set(key, batch.value, *setParam);
           }
           if (rc.IsError()) {
               ADD_FAILURE() << FormatString("Set %s failed: %s", key, rc.ToString());
               return batch;
           }
           batch.keys.emplace_back(std::move(key));
       }
       return batch;
   }

   void AssertReadable(const std::shared_ptr<KVClient> &client, const ObjectBatch &batch)
   {
       ASSERT_TRUE(client != nullptr);
       for (const auto &key : batch.keys) {
           std::string value;
           auto rc = client->Get(key, value, GET_TIMEOUT_MS);
           ASSERT_TRUE(rc.IsOk()) << FormatString("Get %s failed: %s", key, rc.ToString());
           ASSERT_EQ(value, batch.value) << key;
       }
   }

   uint64_t GetInjectCount(uint32_t workerIndex, const std::string &name)
   {
       uint64_t count = 0;
       auto rc = cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count);
       if (rc.IsError()) {
           ADD_FAILURE() << FormatString("Get inject count %s on worker %u failed: %s", name, workerIndex,
                                         rc.ToString());
           return 0;
       }
       return count;
   }

   uint64_t GetInjectCountIfAlive(uint32_t workerIndex, const std::string &name)
   {
       uint64_t count = 0;
       (void)cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, count);
       return count;
   }

   uint64_t GetTotalInjectCount(const std::string &name, const std::vector<uint32_t> &workers = AllWorkers())
   {
       uint64_t total = 0;
       for (auto workerIndex : workers) {
           total += GetInjectCount(workerIndex, name);
       }
       return total;
   }

   bool WaitFor(std::function<bool()> predicate, int timeoutMs = REBALANCE_TIMEOUT_MS)
   {
       auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
       do {
           if (predicate()) {
               return true;
           }
           SleepMs(POLL_INTERVAL_MS);
       } while (std::chrono::steady_clock::now() < deadline);
       return predicate();
   }

   void WaitForInjectCount(uint32_t workerIndex, const std::string &name, uint64_t expectedCount,
                           int timeoutMs = REBALANCE_TIMEOUT_MS)
   {
       ASSERT_TRUE(WaitFor([&] { return GetInjectCountIfAlive(workerIndex, name) >= expectedCount; }, timeoutMs))
           << FormatString("inject point %s on worker %u did not reach %lu, current=%lu", name, workerIndex,
                           expectedCount, GetInjectCountIfAlive(workerIndex, name));
   }

   void WaitForTotalInjectCount(const std::string &name, uint64_t expectedCount,
                                const std::vector<uint32_t> &workers = AllWorkers(),
                                int timeoutMs = REBALANCE_TIMEOUT_MS)
   {
       ASSERT_TRUE(WaitFor([&] {
           uint64_t total = 0;
           for (auto workerIndex : workers) {
               total += GetInjectCountIfAlive(workerIndex, name);
           }
           return total >= expectedCount;
       },
                           timeoutMs))
           << FormatString("inject point %s did not reach %lu", name, expectedCount);
   }

   void WaitAllNodesActiveInHashRing(uint32_t expectedWorkerNum, int timeoutMs = HASH_RING_TIMEOUT_MS)
   {
       if (!db_) {
           InitTestEtcdInstance();
       }
       Status lastStatus;
       HashRingPb lastRing;
       ASSERT_TRUE(WaitFor(
           [&] {
               std::string hashRingStr;
               lastStatus = db_->Get(ETCD_RING_PREFIX, "", hashRingStr);
               if (lastStatus.IsError()) {
                   return false;
               }
               lastRing.Clear();
               if (!lastRing.ParseFromString(hashRingStr)) {
                   return false;
               }
               if (lastRing.workers_size() != static_cast<int>(expectedWorkerNum)
                   || lastRing.add_node_info_size() != 0 || lastRing.del_node_info_size() != 0) {
                   return false;
               }
               for (const auto &worker : lastRing.workers()) {
                   if (worker.second.state() != WorkerPb::ACTIVE) {
                       return false;
                   }
               }
               return true;
           },
           timeoutMs))
           << FormatString("hash ring did not become stable, expectedWorkerNum=%u, lastStatus=%s, ring=%s",
                           expectedWorkerNum, lastStatus.ToString(), lastRing.ShortDebugString());
       SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
   }

   void SetInjectAction(uint32_t workerIndex, const std::string &name, const std::string &action)
   {
       DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, workerIndex, name, action));
   }

   void SetInjectActionForAll(const std::string &name, const std::string &action)
   {
       for (auto workerIndex : AllWorkers()) {
           SetInjectAction(workerIndex, name, action);
       }
   }

   std::shared_ptr<KVClient> client0_;
   std::shared_ptr<KVClient> client1_;
   std::shared_ptr<KVClient> client2_;
};

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_SingleWorkerHighWaterTriggersRebalance)
{
   auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
   auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

   auto sourceBatch = WriteObjects(client0_, "rebalance_single_source", 9, 's');

   WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
   WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
   AssertReadable(client1_, sourceBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_UsageGapTriggersRebalanceTowardLowerUsageWorker)
{
   auto lowWorkerBaseline = GetInjectCount(WORKER2, TARGET_MIGRATE_POINT);
   auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);

   SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
   auto lowPressureBatch = WriteObjects(client1_, "rebalance_direction_low_pressure", 1, 'l');
   SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
   auto sourceBatch = WriteObjects(client0_, "rebalance_direction_source", 9, 'h');

   WaitForInjectCount(WORKER2, TARGET_MIGRATE_POINT, lowWorkerBaseline + 1);
   WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
   SleepMs(SHORT_WAIT_MS);
   AssertReadable(client2_, sourceBatch);
   AssertReadable(client0_, lowPressureBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_UsageGapBelowThresholdDoesNotDispatchTask)
{
   auto worker1Batch = WriteObjects(client1_, "rebalance_threshold_worker1", 7, '1');
   auto worker2Batch = WriteObjects(client2_, "rebalance_threshold_worker2", 7, '2');
   ASSERT_EQ(GetTotalInjectCount(ASSIGN_TASK_POINT), 0ul);
   SleepMs(WORKER_RECEIVE_RING_DELAY_MS);

   auto sourceBatch = WriteObjects(client0_, "rebalance_threshold_source", 9, '0');

   SleepMs(SHORT_WAIT_MS);
   ASSERT_EQ(GetTotalInjectCount(ASSIGN_TASK_POINT), 0ul);
   AssertReadable(client0_, sourceBatch);
   AssertReadable(client1_, worker1Batch);
   AssertReadable(client2_, worker2Batch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_ScaleUpNewWorkerParticipatesInRebalance)
{
   auto oldWorkerBatch = WriteObjects(client1_, "rebalance_scale_old_worker", 7, 'o');
   auto sourceBatch = WriteObjects(client0_, "rebalance_scale_source", 9, 's');
   SleepMs(SHORT_WAIT_MS);
   ASSERT_EQ(GetInjectCount(WORKER0, SOURCE_SEND_POINT), 0ul);

   HostPort newWorkerAddr("127.0.0.1", GetFreePort());
   HostPort masterAddr;
   DS_ASSERT_OK(cluster_->GetWorkerAddr(WORKER0, masterAddr));
   DS_ASSERT_OK(cluster_->AddNode(masterAddr, newWorkerAddr.ToString(), GetFreePort()));
   DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, WORKER2, 20));
   WaitAllNodesActiveInHashRing(3);
   InitTestKVClient(WORKER2, client2_);

   WaitForInjectCount(WORKER2, TARGET_MIGRATE_POINT, 1);
   AssertReadable(client2_, sourceBatch);
   AssertReadable(client0_, oldWorkerBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_ForegroundReadWriteConcurrentWithRebalance)
{
   SetInjectAction(WORKER0, SOURCE_SEND_POINT, "5*sleep(500)");
   auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
   auto sourceBatch = WriteObjects(client0_, "rebalance_concurrent_source", 9, 'c');

   std::atomic<bool> stop{ false };
   std::atomic<uint32_t> failures{ 0 };
   std::vector<std::thread> threads;
   constexpr int threadCount = 4;
   for (int tid = 0; tid < threadCount; ++tid) {
       threads.emplace_back([&, tid] {
           uint32_t round = 0;
           while (!stop.load(std::memory_order_acquire)) {
               auto smallKey = "rebalance_concurrent_foreground_" + std::to_string(tid);
               std::string smallValue(4096, static_cast<char>('a' + tid + (round % 8)));
               if (client1_->Set(smallKey, smallValue).IsError()) {
                   failures.fetch_add(1, std::memory_order_relaxed);
                   continue;
               }
               std::string getSmallValue;
               if (client0_->Get(smallKey, getSmallValue, GET_TIMEOUT_MS).IsError() || getSmallValue != smallValue) {
                   failures.fetch_add(1, std::memory_order_relaxed);
               }

               std::string getLargeValue;
               const auto &largeKey = sourceBatch.keys[(round + tid) % sourceBatch.keys.size()];
               if (client1_->Get(largeKey, getLargeValue, GET_TIMEOUT_MS).IsError()
                   || getLargeValue != sourceBatch.value) {
                   failures.fetch_add(1, std::memory_order_relaxed);
               }
               ++round;
           }
       });
   }

   WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
   SleepMs(1'000);
   stop.store(true, std::memory_order_release);
   for (auto &thread : threads) {
       thread.join();
   }
   ASSERT_EQ(failures.load(std::memory_order_relaxed), 0u);
   AssertReadable(client1_, sourceBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_TargetNoSpaceFailsAndNextCycleSelectsAnotherTarget)
{
   SetInjectAction(WORKER1, TARGET_MEMORY_AVAILABLE_POINT, "100000*return()");
   auto blockedTargetBaseline = GetInjectCount(WORKER1, TARGET_MIGRATE_POINT);
   auto fallbackTargetBaseline = GetInjectCount(WORKER2, TARGET_MIGRATE_POINT);
   auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

   auto fallbackPressureBatch = WriteObjects(client2_, "rebalance_target_no_space_fallback_pressure", 2, 'f');
   auto sourceBatch = WriteObjects(client0_, "rebalance_target_no_space_source", 9, 'n');

   WaitForInjectCount(WORKER1, TARGET_MIGRATE_POINT, blockedTargetBaseline + 1);
   auto blockedTargetPressureBatch = WriteObjects(client1_, "rebalance_target_no_space_blocked_pressure", 7, 'b');
   WaitForInjectCount(WORKER2, TARGET_MIGRATE_POINT, fallbackTargetBaseline + 1);
   WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 2);
   AssertReadable(client2_, sourceBatch);
   AssertReadable(client0_, blockedTargetPressureBatch);
   AssertReadable(client0_, fallbackPressureBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_ReplacePrimaryFailureKeepsSourceReadable)
{
   SetInjectActionForAll(REPLACE_PRIMARY_POINT, "100000*return(K_RUNTIME_ERROR)");
   auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
   auto replacePrimaryBaseline = GetTotalInjectCount(REPLACE_PRIMARY_POINT);

   auto sourceBatch = WriteObjects(client0_, "rebalance_replace_primary_source", 9, 'r');

   WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
   WaitForTotalInjectCount(REPLACE_PRIMARY_POINT, replacePrimaryBaseline + 1);
   AssertReadable(client0_, sourceBatch);
   AssertReadable(client1_, sourceBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_ReportResultRpcFailureExpiresTaskWithoutDataLoss)
{
   SetInjectActionForAll(REPORT_RESULT_POINT, "100000*return(K_RPC_UNAVAILABLE)");
   auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
   auto reportBaseline = GetTotalInjectCount(REPORT_RESULT_POINT);
   auto expireBaseline = GetTotalInjectCount(EXPIRE_TASK_POINT);

   auto sourceBatch = WriteObjects(client0_, "rebalance_report_result_source", 9, 'p');

   WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
   WaitForTotalInjectCount(REPORT_RESULT_POINT, reportBaseline + 1);
   WaitForTotalInjectCount(EXPIRE_TASK_POINT, expireBaseline + 1);
   AssertReadable(client1_, sourceBatch);
}

class LEVEL1_KVClientMemoryRebalanceEvictSpillRegressionTest : public KVClientCommon {
public:
   void SetClusterSetupOptions(ExternalClusterOptions &opts) override
   {
       opts.enableSpill = true;
       opts.numWorkers = 1;
       opts.numEtcd = 1;
       opts.numOBS = 0;
       opts.enableDistributedMaster = "false";
       opts.workerGflagParams =
           "-shared_memory_size_mb=8 -log_monitor=true -v=1 -spill_size_limit=67108864 "
           "-enable_memory_rebalance=" + std::string(EnableRebalance() ? "true" : "false");
       opts.injectActions = "worker.Spill.Sync:return()";
       opts.workerSpecifyGflagParams[WORKER0] =
           FormatString("-shared_disk_directory=%s/worker0/shared_disk -shared_disk_size_mb=64",
                        GetTestCaseDataDir());
   }

   void SetUp() override
   {
       ExternalClusterTest::SetUp();
       InitTestKVClient(WORKER0, client_);
   }

   void TearDown() override
   {
       client_.reset();
       ExternalClusterTest::TearDown();
   }

protected:
   virtual bool EnableRebalance() const = 0;

   void VerifySpillRoundTrip()
   {
       SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
       ObjectBatch batch;
       batch.value.assign(SPILL_VALUE_SIZE, 'x');
       constexpr int objectCount = 6;
       for (int i = 0; i < objectCount; ++i) {
           auto key = "rebalance_spill_regression_" + std::to_string(i);
           DS_ASSERT_OK(client_->Set(key, batch.value, param));
           batch.keys.emplace_back(std::move(key));
       }
       for (const auto &key : batch.keys) {
           std::string value;
           auto rc = client_->Get(key, value, GET_TIMEOUT_MS);
           ASSERT_TRUE(rc.IsOk()) << FormatString("Get %s failed: %s", key, rc.ToString());
           ASSERT_EQ(value, batch.value) << key;
       }
   }

   std::shared_ptr<KVClient> client_;
};

class LEVEL1_KVClientMemoryRebalanceEnabledEvictSpillRegressionTest
   : public LEVEL1_KVClientMemoryRebalanceEvictSpillRegressionTest {
protected:
   bool EnableRebalance() const override
   {
       return true;
   }
};

class LEVEL1_KVClientMemoryRebalanceDisabledEvictSpillRegressionTest
   : public LEVEL1_KVClientMemoryRebalanceEvictSpillRegressionTest {
protected:
   bool EnableRebalance() const override
   {
       return false;
   }
};

TEST_F(LEVEL1_KVClientMemoryRebalanceEnabledEvictSpillRegressionTest, EvictAndSpillSemanticsRemainWithRebalanceOn)
{
   VerifySpillRoundTrip();
}

TEST_F(LEVEL1_KVClientMemoryRebalanceDisabledEvictSpillRegressionTest, EvictAndSpillSemanticsRemainWithRebalanceOff)
{
   VerifySpillRoundTrip();
}

}  // namespace st
}  // namespace datasystem
