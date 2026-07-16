/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/kv_cache/kv_client_common.h"
#include "cluster/external_cluster.h"
#include "common.h"
#include "common_distributed_ext.h"
#include "datasystem/common/flags/common_flags.h"  // FLAGS_use_brpc
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"
#include "datasystem/protos/cluster_topology.pb.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER0 = 0;
constexpr uint32_t WORKER1 = 1;
constexpr uint32_t WORKER2 = 2;
constexpr size_t VALUE_SIZE = 5 * 1024UL * 1024UL;
constexpr size_t MEMORY_LIMIT_SOURCE_VALUE_SIZE = 4 * 1024UL * 1024UL;
constexpr size_t SPILL_VALUE_SIZE = 4 * 1024UL * 1024UL;
constexpr int MEMORY_LIMIT_SOURCE_OBJECT_COUNT = 8;
constexpr int MEMORY_LIMIT_SOURCE_TRIGGER_OBJECT_COUNT = 2;
constexpr int GET_TIMEOUT_MS = 30'000;
constexpr int REBALANCE_TIMEOUT_MS = 25'000;
constexpr int HASH_RING_TIMEOUT_MS = 60'000;
constexpr int SHORT_WAIT_MS = 3'000;
constexpr int POLL_INTERVAL_MS = 100;
constexpr int WORKER_RECEIVE_RING_DELAY_MS = 1'000;
constexpr int64_t SOURCE_CLOCK_OFFSET_MS = 86'400'000;
constexpr char SCALE_UP_CASE_NAME[] = "ScaleUpNewWorkerParticipatesInRebalance";
constexpr char USAGE_GAP_BELOW_THRESHOLD_CASE_NAME[] = "UsageGapBelowThresholdDoesNotDispatchTask";
constexpr char MEMORY_LIMIT_SOURCE_THRESHOLD_CASE_NAME[] = "UsageRateUsesMemoryLimitForSourceThreshold";
constexpr char SOURCE_CLOCK_OFFSET_CASE_NAME[] = "SourceClockOffsetUsesRelativeTaskTimeout";
constexpr char DISABLED_TEST_PREFIX[] = "DISABLED_";
const std::string SOURCE_SEND_POINT = "TcpMigrateTransport.MigrateDataToRemote.delay";
const std::string FAST_MIGRATE_SEND_POINT = "FastMigrateTransport2.MigrateDataToRemote.delay";
const std::string TARGET_MIGRATE_POINT = "worker.migrate_service.return";
const std::string TARGET_MEMORY_AVAILABLE_POINT = "worker.migrate_service.memory_available";
const std::string ASSIGN_TASK_POINT = "MemoryRebalanceScheduler.AssignTask";
const std::string EXPIRE_TASK_POINT = "MemoryRebalanceScheduler.ExpireTask";
const std::string REPLACE_PRIMARY_POINT = "OCMetadataManager.ReplacePrimary";
const std::string REPORT_RESULT_POINT = "MasterOCServiceImpl.ReportRebalanceResult";
const std::string SOURCE_CLOCK_OFFSET_POINT = "RebalanceExecutor.NowMsForExpiryCheck.addOffsetMs";
const std::string ADMISSION_CLOSED_POINT = "WorkerOcServiceMigrateImpl.CloseIncomingMigrationAdmissionAndWait.closed";
const std::string DRAIN_TIMED_OUT_POINT = "WorkerOcServiceMigrateImpl.CloseIncomingMigrationAdmissionAndWait.timedOut";
const std::string MIGRATE_DATA_AFTER_ADMISSION = "WorkerOcServiceMigrateImpl.MigrateData.afterAdmission";

struct ObjectBatch {
   std::vector<std::string> keys;
   std::string value;
};

std::vector<uint32_t> AllWorkers()
{
   return { WORKER0, WORKER1, WORKER2 };
}

bool IsCurrentTestName(const std::string &expectedName)
{
   std::string caseName;
   std::string name;
   GetCurTestName(caseName, name);
   return name == expectedName || name == std::string(DISABLED_TEST_PREFIX) + expectedName;
}

bool IsScaleUpCase()
{
   return IsCurrentTestName(SCALE_UP_CASE_NAME);
}

bool IsUsageGapBelowThresholdCase()
{
   return IsCurrentTestName(USAGE_GAP_BELOW_THRESHOLD_CASE_NAME);
}

bool IsMemoryLimitSourceThresholdCase()
{
   std::string caseName;
   std::string name;
   GetCurTestName(caseName, name);
   return name == MEMORY_LIMIT_SOURCE_THRESHOLD_CASE_NAME;
}

bool IsSourceClockOffsetCase()
{
   return IsCurrentTestName(SOURCE_CLOCK_OFFSET_CASE_NAME);
}

bool IsUrmaScaleInCase()
{
    return IsCurrentTestName("RebalanceTargetActiveScaleInUrmaDoesNotLoseData");
}

std::string BuildRebalanceInjectActions()
{
   std::string actions = "NodeSelector.setInterval:call(200);"
                         "ResourceManager.setInterval:call(200);"
                         "TcpMigrateTransport.MigrateDataToRemote.delay:100000*call();"
                         "MemoryRebalanceScheduler.AssignTask:100000*call();"
                         "MemoryRebalanceScheduler.ExpireTask:100000*call();"
                         "worker.migrate_service.return:100000*call()";
   if (IsSourceClockOffsetCase()) {
       actions += ";" + SOURCE_CLOCK_OFFSET_POINT + ":"
                  + FormatString("call(%lld)", static_cast<long long>(SOURCE_CLOCK_OFFSET_MS));
   }
   return actions;
}

void SleepMs(int timeoutMs)
{
   std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
}
}  // namespace

class LEVEL1_KVClientMemoryRebalanceTest : public KVClientCommon, public CommonDistributedExt {
public:
   LEVEL1_KVClientMemoryRebalanceTest() = default;
   ~LEVEL1_KVClientMemoryRebalanceTest() override = default;

   void SetClusterSetupOptions(ExternalClusterOptions &opts) override
   {
       opts.numWorkers = IsScaleUpCase() ? 2 : 3;
       opts.numEtcd = 1;
       opts.numOBS = 0;
       bool memoryLimitSourceThresholdCase = IsMemoryLimitSourceThresholdCase();
       std::string usageGapPercent = IsUsageGapBelowThresholdCase() ? "100" : "30";
       if (memoryLimitSourceThresholdCase) {
           usageGapPercent = "1";
       }
       std::string sourceUsagePercent = memoryLimitSourceThresholdCase ? "55" : "70";
       std::string watermarkParams = memoryLimitSourceThresholdCase
                                         ? " -eviction_high_watermark_ratio=0.8"
                                           " -eviction_low_watermark_ratio=0.7"
                                         : "";
       opts.workerGflagParams =
           "-shared_memory_size_mb=64 -log_monitor=true -enable_memory_rebalance=true "
           "-rebalance_usage_gap_percent=" + usageGapPercent + " -rebalance_source_usage_percent=" +
           sourceUsagePercent + " "
           "-rebalance_cooldown_s=1 -rebalance_task_report_grace_ms=500 "
           "-data_migrate_rate_limit_mb=1024" +
           watermarkParams;
        if (IsUrmaScaleInCase()) {
#ifdef USE_URMA
            opts.workerGflagParams += " -enable_urma=true -enable_transport_fallback=false";
#else
            // Worker binary not built with URMA framework; test will GTEST_SKIP in the body.
            opts.workerGflagParams += " -enable_urma=false";
#endif
        }
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
           KVClientCommon::InitTestEtcdInstance();
       }
       Status lastStatus;
       ClusterTopologyPb lastRing;
       ASSERT_TRUE(WaitFor(
           [&] {
               std::string hashRingStr;
               lastStatus = db_->Get(GetTopologyTableName(), "", hashRingStr);
               if (lastStatus.IsError()) {
                   return false;
               }
               lastRing.Clear();
               if (!lastRing.ParseFromString(hashRingStr)) {
                   return false;
               }
               if (lastRing.members_size() != static_cast<int>(expectedWorkerNum)) {
                   return false;
               }
               for (const auto &worker : lastRing.members()) {
                   if (worker.second.state() != MembershipPb::ACTIVE) {
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

   // Reads the worker resource.log and returns the shared-memory usage rate as a percent
   // (memoryUsage / FootprintLimit * 100). Returns negative on transient error (partial line, file rotation);
   // WaitForWorkerMemRate retries silently so a transient read failure does not mark the test failed.
   // resource.log updates every ~10s, so use WaitForWorkerMemRate to poll until a fresh post-event sample is
   // observed instead of reading a stale startup line.
   double GetWorkerMemRatePercent(uint32_t workerIndex)
   {
       const std::string fullName =
           FormatString("%s/../worker%u/log/resource.log", FLAGS_log_dir.c_str(), workerIndex);
       std::ifstream ifs(fullName);
       if (!ifs.is_open()) {
           return -1.0;
       }
       std::string line;
       std::string lastLine;
       while (std::getline(ifs, line)) {
           lastLine = line;
       }
       auto tokens = Split(lastLine, " | ");
       const size_t ignoreCount = 7;
       if (tokens.size() <= ignoreCount) {
           return -1.0;  // partial line -- worker may be mid-write; caller retries
       }
       // tokens[ignoreCount] is SHARED_MEMORY = "memUsage/physUsage/totalLimit/rate/streamUsage/streamLimit";
       // field [3] is the usage rate as a fraction (memoryUsage / FootprintLimit).
       auto memFields = Split(tokens[ignoreCount], "/");
       if (memFields.size() < 4) {
           return -1.0;
       }
       try {
           return std::stod(memFields[3]) * 100.0;
       } catch (const std::exception &) {
           return -1.0;
       }
   }

   // Polls the worker resource.log until the sampled rate falls into [loPercent, hiPercent] (i.e. the monitor has
   // written a fresh post-event sample matching the expected post-rebalance band), or until timeoutMs elapses.
   // resource.log updates every ~10s, so the timeout must exceed that. Source drains from ~74% to ~37% (exits the
   // write-time high); target rises from ~0% to ~37% (enters the band). Returns the rate on success, or the last
   // sample (possibly stale/out-of-band) on timeout for the caller to assert against.
   double WaitForWorkerMemRate(uint32_t workerIndex, double loPercent, double hiPercent, int timeoutMs = 25'000)
   {
       auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
       double rate = -1.0;
       do {
           rate = GetWorkerMemRatePercent(workerIndex);
           if (rate >= 0.0 && rate >= loPercent && rate <= hiPercent) {
               return rate;
           }
           SleepMs(500);
       } while (std::chrono::steady_clock::now() < deadline);
       return rate;
   }

   std::shared_ptr<KVClient> client0_;
   std::shared_ptr<KVClient> client1_;
   std::shared_ptr<KVClient> client2_;

   BaseCluster *GetCluster() override
   {
       return cluster_.get();
   }
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

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_SourceClockOffsetUsesRelativeTaskTimeout)
{
   WaitAllNodesActiveInHashRing(3);
   auto clockOffsetBaseline = GetTotalInjectCount(SOURCE_CLOCK_OFFSET_POINT);
   auto sourceSendBaseline = GetTotalInjectCount(SOURCE_SEND_POINT);
   auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

   auto sourceBatch = WriteObjects(client0_, "rebalance_source_clock_offset", 9, 'd');

   WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
   WaitForTotalInjectCount(SOURCE_CLOCK_OFFSET_POINT, clockOffsetBaseline + 1);
   WaitForTotalInjectCount(SOURCE_SEND_POINT, sourceSendBaseline + 1);
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

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, DISABLED_UsageRateUsesMemoryLimitForSourceThreshold)
{
   auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

   // 8 * 4 MB is below 55% of memory_limit, but above 55% of 0.8 high-water capacity.
   auto sourceBatch =
       WriteObjects(client0_, "rebalance_memory_limit_source_threshold", MEMORY_LIMIT_SOURCE_OBJECT_COUNT, 'm',
                    MEMORY_LIMIT_SOURCE_VALUE_SIZE);

   SleepMs(SHORT_WAIT_MS);
   ASSERT_EQ(GetTotalInjectCount(ASSIGN_TASK_POINT), assignBaseline);

   auto triggerBatch = WriteObjects(client0_, "rebalance_memory_limit_source_trigger",
                                    MEMORY_LIMIT_SOURCE_TRIGGER_OBJECT_COUNT, 't', MEMORY_LIMIT_SOURCE_VALUE_SIZE);

   WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
   AssertReadable(client0_, sourceBatch);
   AssertReadable(client0_, triggerBatch);
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

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, BusyGuardSelfHealProbesAfterBudgetElapsed)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; real failure under brpc. Tracked separately.";
    }
    WaitAllNodesActiveInHashRing(3);
    constexpr char PROBE_POINT[] = "MigrateDataHandler.SelfHealBusyRate.probe";
    SetInjectActionForAll(PROBE_POINT, "100000*call()");
    SetInjectActionForAll("migrate.limiter.is_busy_node", "100000*return()");
    // Probe-only inject: 3 K_NOT_READY returns on MigrateDataProbe (not MigrateData).
    // worker.migrate_data_probe.return lives on the caller side (WorkerRemoteWorkerOCApi::MigrateDataProbe),
    // which runs on the source worker (WORKER0), so the action must be set on WORKER0, not the targets.
    // SelfHealBusyRate fires 3 probes, all return K_NOT_READY -> rate=0 -> budget elapsed -> give up
    // -> SendDataToRemote returns without sending MigrateData -> handler reports FAILED -> master cooldowns
    // source+target 1s -> next Schedule dispatches a new task with a fresh handler (selfHealAttempted_=false).
    // Probe 4: inject exhausted -> target responds -> rate>0 -> self-heal succeeds -> migration proceeds.
    SetInjectAction(WORKER0, "worker.migrate_data_probe.return", "3*return(K_NOT_READY)");

    auto probeBaseline = GetTotalInjectCount(PROBE_POINT);
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);
    auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
    auto sourceBatch = WriteObjects(client0_, "rebalance_busy_after_budget", 9, 'b');

    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    // 3 probes from handler 1 (budget elapsed) + 1 from retry handler 2 = 4 total.
    WaitForTotalInjectCount(PROBE_POINT, probeBaseline + 4, AllWorkers(), 15000);
    // Clear the busy-node inject so handler 2's subsequent batches skip self-heal.
    // Then wait for handler 2's MigrateData RPC to start sending (target is alive,
    // RPC completes in ~50ms) and sleep briefly to let it finish before TearDown
    // kills the target. Without this, TearDown kills the target mid-RPC, the RPC
    // retries for 120s, and StopRebalanceExecutor()'s join hangs to the 80s timeout.
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER0, "migrate.limiter.is_busy_node"));
    WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1, 5000);
    SleepMs(500);
    AssertReadable(client0_, sourceBatch);
}

// Verifies the rebalance accounting-unit fix end-to-end. Standalone 1MiB objects carry a 64B metadata header
// (default oc_metadata_header=true, max_client_num=200), so needSize=1MiB+64B and sallocx=1.25MiB (next jemalloc
// size class). Before the fix, migrated_bytes counted payload (GetDataSize) while max_bytes used RealUsage
// (salcx=1.25MiB), so the guard overshot: source undershot the midpoint and target overshot it (a ~16% gap).
// After the fix, source and target must converge near the usage midpoint.
TEST_F(LEVEL1_KVClientMemoryRebalanceTest, RebalanceConvergesToMidpointWithRealSizeAccounting)
{
    if (FLAGS_use_brpc) {
        GTEST_SKIP() << "brpc migration gap; real failure/flaky under brpc. Tracked separately.";
    }
    constexpr size_t ONE_MB = 1024UL * 1024UL;
    WaitAllNodesActiveInHashRing(3);
    auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

    // Pre-load worker2 so worker1 is the unambiguous lowest-usage target.
    auto pressureBatch = WriteObjects(client2_, "rebalance_midpoint_pressure", 1, 'p', ONE_MB);
    SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
    // shared_memory_size_mb=64, 70% source threshold -> ~44.8MiB real. 38 * 1.25MiB = 47.5MiB (~74.2%).
    auto sourceBatch = WriteObjects(client0_, "rebalance_midpoint_source", 38, 'm', ONE_MB);

    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
    // resource.log updates every ~10s, so poll until a fresh post-rebalance sample lands in the expected band
    // [25%, 50%] (both source and target converge near the midpoint ~37%). Source exits the write-time ~74%;
    // target enters from ~0%.
    double sourceRate = WaitForWorkerMemRate(WORKER0, 25.0, 50.0);
    double targetRate = WaitForWorkerMemRate(WORKER1, 25.0, 50.0);
    ASSERT_GE(sourceRate, 0.0);
    ASSERT_GE(targetRate, 0.0);
    // Convergence: the fix lands source and target both near the midpoint (~37%); the pre-fix payload-vs-realSize
    // mismatch left a ~16% gap (source ~29%, target ~45%). One sallocx object ~= 2% of 64MiB, so 5% is robust.
    EXPECT_NEAR(sourceRate, targetRate, 5.0);
    // Sanity bounds: neither worker should drain to nothing or overshoot past half the cache.
    EXPECT_GE(sourceRate, 25.0);
    EXPECT_LE(sourceRate, 50.0);
    EXPECT_GE(targetRate, 25.0);
    EXPECT_LE(targetRate, 50.0);
    AssertReadable(client1_, sourceBatch);
    AssertReadable(client2_, pressureBatch);
}

// Regression for rate=20 migration failure (5afc55ff regression): data_migrate_rate_limit_mb=20
// makes maxBandwidth=20MB/s and maxBatchSize=20MB. The first full 20MB batch saturates the
// remote sliding window, so the remote reports limit_rate=0 and the source enters
// SelfHealBusyRate. Before the prune-on-read fix, the sliding window only expired entries
// inside SlidingWindowUpdateRate (write path), but 5afc55ff's `bytes_send > 0` guard made
// probe requests (bytes_send==0) skip that call. The stale 20MB entry never drained, so all
// 3 probes returned rate=0, self-heal failed with K_NOT_READY, and only the first batch was
// ever sent (selfHealAttempted_ then cached the error for the handler's lifetime). After the
// fix, GetAvailableBandwidth prunes on read, so probe 2-3 sees the window drained after ~1s,
// self-heal recovers, and all batches complete. Asserting >=2 source-send inject hits
// distinguishes the two: old code stays at +1 (times out), fixed code reaches +2..3.
class LEVEL1_KVClientMemoryRebalanceLowRateTest : public LEVEL1_KVClientMemoryRebalanceTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.numOBS = 0;
        opts.workerGflagParams =
            "-shared_memory_size_mb=64 -log_monitor=true -enable_memory_rebalance=true "
            "-rebalance_usage_gap_percent=30 -rebalance_source_usage_percent=70 "
            "-rebalance_cooldown_s=1 -rebalance_task_report_grace_ms=500 "
            "-data_migrate_rate_limit_mb=20";
        opts.injectActions = BuildRebalanceInjectActions();
    }
};

TEST_F(LEVEL1_KVClientMemoryRebalanceLowRateTest, LowRateMigrateSucceedsAfterBusySelfHealRecovers)
{
    WaitAllNodesActiveInHashRing(3);
    auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

    // Pre-load worker2 so worker1 is the unambiguous lowest-usage target.
    auto pressureBatch = WriteObjects(client2_, "rebalance_low_rate_pressure", 1, 'p');
    SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
    // 9 * 5MB = 45MB > 70% of 64MB (~44.8MB) -> triggers rebalance to worker1.
    // With rate=20, maxBatchSize=20MB; the first full 20MB batch saturates the 20MB/s sliding
    // window and forces the self-heal path on the next batch.
    auto sourceBatch = WriteObjects(client0_, "rebalance_low_rate_source", 9, 'l');

    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    // Before the fix, only the first 20MB batch was sent (self-heal then failed and cached the
    // error for the handler's lifetime, blocking all subsequent batches). After the fix,
    // self-heal recovers within ~1s once the sliding window is pruned on read, so all 3
    // batches complete. Assert >=2 batches were sent within 30s.
    WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 2, 30'000);
    AssertReadable(client1_, sourceBatch);
    AssertReadable(client2_, pressureBatch);
}

TEST_F(LEVEL1_KVClientMemoryRebalanceTest, RebalanceTargetActiveScaleInDoesNotLoseData)
{
    SetInjectActionForAll(REPLACE_PRIMARY_POINT, "100000*call()");
    auto sourceSendBaseline = GetInjectCount(WORKER0, SOURCE_SEND_POINT);
    auto replacePrimaryBaseline = GetTotalInjectCount(REPLACE_PRIMARY_POINT);
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

    auto sourceBatch = WriteObjects(client0_, "rebalance_target_scalein", 9, 't');

    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    WaitForInjectCount(WORKER0, SOURCE_SEND_POINT, sourceSendBaseline + 1);
    WaitForTotalInjectCount(REPLACE_PRIMARY_POINT, replacePrimaryBaseline + 1);

    client1_.reset();
    VoluntaryScaleDownInject(static_cast<int>(WORKER1));
    WaitAllNodesActiveInHashRing(2);
    AssertReadable(client0_, sourceBatch);
    AssertReadable(client2_, sourceBatch);
}

// Verifies the NotifyRemoteGet admission fix on the URMA write path
// (FastMigrateTransport2).
//
// CI builds with -M on auto-fallback to URMA mock when liburma.so is absent,
// so this test runs as a normal level1 case. For local debugging with
// explicit mock: build.sh -d -t build -U on.
TEST_F(LEVEL1_KVClientMemoryRebalanceTest, RebalanceTargetActiveScaleInUrmaDoesNotLoseData)
{
#ifndef USE_URMA
    GTEST_SKIP() << "Worker not built with URMA framework; skip URMA write-path test";
#endif
    // Block Source at the send point so the test can order Target scale-down
    // before NotifyRemoteGet is sent.
    SetInjectAction(WORKER0, FAST_MIGRATE_SEND_POINT, "pause()");
    auto sourceSendBaseline = GetInjectCount(WORKER0, FAST_MIGRATE_SEND_POINT);
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);
    auto admissionClosedBaseline = GetInjectCount(WORKER1, ADMISSION_CLOSED_POINT);

    auto sourceBatch = WriteObjects(client0_, "rebalance_target_scalein_urma", 9, 'u');

    // T1: Source completed probe and is paused before NotifyRemoteGet.
    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    WaitForInjectCount(WORKER0, FAST_MIGRATE_SEND_POINT, sourceSendBaseline + 1);

    // T2: Target begins voluntary scale-down and closes admission.
    client1_.reset();
    SetInjectAction(WORKER1, ADMISSION_CLOSED_POINT, "100000*call()");
    VoluntaryScaleDownInject(static_cast<int>(WORKER1));
    WaitForInjectCount(WORKER1, ADMISSION_CLOSED_POINT, admissionClosedBaseline + 1);

    // T3: Release Source. NotifyRemoteGet arrives after admission is closed
    // and must be rejected with K_NOT_READY, so Source does not delete data.
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER0, FAST_MIGRATE_SEND_POINT));

    WaitAllNodesActiveInHashRing(2);
    AssertReadable(client0_, sourceBatch);
    AssertReadable(client2_, sourceBatch);
}

// Verifies that an admitted MigrateData whose drain times out returns
// K_NOT_READY to Source so Source keeps its local copy and no data is lost.
//
// T1: Source sends MigrateData to Target; Target acquires admission and
//     pauses at the afterAdmission inject point.
// T2: Target voluntary scale-down starts; admission closes and drain begins.
// T3: Drain times out (TOPOLOGY_STOP_GRACE = 10s) because the admitted RPC is
//     still paused. PreShutDown continues (does not early-return) to publish
//     EXITING and wait for topology removal.
// T4: Release the inject. MigrateData hits checkpoint A (admission closed)
//     or checkpoint B (drain timed out) and returns K_NOT_READY.
// T5: Source receives K_NOT_READY and keeps its local copy. Data is safe.
TEST_F(LEVEL1_KVClientMemoryRebalanceTest, RebalanceDrainTimeoutKeepsSourceData)
{
    SetInjectAction(WORKER1, MIGRATE_DATA_AFTER_ADMISSION, "pause()");
    SetInjectAction(WORKER2, MIGRATE_DATA_AFTER_ADMISSION, "pause()");
    SetInjectAction(WORKER1, ADMISSION_CLOSED_POINT, "100000*call()");
    SetInjectAction(WORKER2, ADMISSION_CLOSED_POINT, "100000*call()");
    SetInjectAction(WORKER1, DRAIN_TIMED_OUT_POINT, "100000*call()");
    SetInjectAction(WORKER2, DRAIN_TIMED_OUT_POINT, "100000*call()");
    auto baseline1 = GetInjectCount(WORKER1, MIGRATE_DATA_AFTER_ADMISSION);
    auto baseline2 = GetInjectCount(WORKER2, MIGRATE_DATA_AFTER_ADMISSION);
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);

    auto sourceBatch = WriteObjects(client0_, "rebalance_drain_timeout", 9, 'd');

    // T1: Source sends MigrateData; Target acquires admission and pauses.
    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    ASSERT_TRUE(WaitFor([&] {
        return GetInjectCountIfAlive(WORKER1, MIGRATE_DATA_AFTER_ADMISSION) > baseline1
            || GetInjectCountIfAlive(WORKER2, MIGRATE_DATA_AFTER_ADMISSION) > baseline2;
    })) << "Neither WORKER1 nor WORKER2 hit the afterAdmission inject point";

    uint32_t target = (GetInjectCountIfAlive(WORKER1, MIGRATE_DATA_AFTER_ADMISSION) > baseline1)
                          ? WORKER1
                          : WORKER2;
    // Clear the pause() inject on the non-target worker so that a subsequent
    // rebalance task dispatched to it (after the target exits) does not pin an
    // Execute task inside RebalanceExecutor::executorPool_ — which would block
    // StopRebalanceExecutor() during TearDown and hang worker Shutdown.
    uint32_t nonTarget = (target == WORKER1) ? WORKER2 : WORKER1;
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, nonTarget, MIGRATE_DATA_AFTER_ADMISSION));
    auto admissionClosedBaseline = GetInjectCount(target, ADMISSION_CLOSED_POINT);
    auto drainTimedOutBaseline = GetInjectCount(target, DRAIN_TIMED_OUT_POINT);

    // T2: Target voluntary scale-down; admission closes and drain begins.
    if (target == WORKER1) {
        client1_.reset();
    } else {
        client2_.reset();
    }
    VoluntaryScaleDownInject(static_cast<int>(target));
    WaitForInjectCount(target, ADMISSION_CLOSED_POINT, admissionClosedBaseline + 1);

    // T3: Wait for drain to time out (TOPOLOGY_STOP_GRACE = 10s).
    WaitForInjectCount(target, DRAIN_TIMED_OUT_POINT, drainTimedOutBaseline + 1);

    // T4: Release Target's MigrateData. It returns K_NOT_READY.
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, target, MIGRATE_DATA_AFTER_ADMISSION));

    // T5: Source keeps its local copy; data is readable after Target exits.
    WaitAllNodesActiveInHashRing(2);
    AssertReadable(client0_, sourceBatch);
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
