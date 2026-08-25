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
 * Description: End-to-end ST for the heat-driven rebalance strategy.
 *
 * Mirrors the memory rebalance trigger ST but adds a 3x Get warm-up so the source's primary copies
 * cross a test-only hot threshold, then waits for the heat scheduler to dispatch a task and the SPILL
 * migration to run, and asserts data stays readable after migration.
 *
 * The trigger case derives keys from the live hash ring so all primary copies are placed on one deterministic source
 * worker. This avoids relying on an unlikely random key clustering event.
 */

#include <algorithm>
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
#include "cluster/heat_watermark_collector.h"
#include "common.h"
#include "datasystem/common/util/format.h"
#include "datasystem/kv_client.h"

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER0 = 0;
constexpr uint32_t WORKER1 = 1;
constexpr uint32_t WORKER2 = 2;
// A heat task can move at most 10% of source capacity. Keep each object's real allocation below that budget while
// retaining enough aggregate bytes to cross the source usage and hot-ratio thresholds.
constexpr size_t VALUE_SIZE = 4 * 1024UL * 1024UL;  // 4MiB payload; 9 objects still trigger rebalance.
constexpr int GET_TIMEOUT_MS = 30'000;
constexpr int REBALANCE_TIMEOUT_MS = 30'000;
constexpr int SHORT_WAIT_MS = 3'000;
constexpr int POLL_INTERVAL_MS = 100;
constexpr int WORKER_RECEIVE_RING_DELAY_MS = 1'000;
constexpr int HOT_WARMUP_GETS = 3;

// Worker/transport-side inject points are shared with the memory strategy (same SPILL path).
const std::string SOURCE_SEND_POINT = "TcpMigrateTransport.MigrateDataToRemote.delay";
// Master-side inject points are heat-scheduler-specific.
const std::string ASSIGN_TASK_POINT = "HeatRebalanceScheduler.AssignTask";
const std::string EXPIRE_TASK_POINT = "HeatRebalanceScheduler.ExpireTask";

struct ObjectBatch {
    std::vector<std::string> keys;
    std::string value;
};

std::vector<uint32_t> AllWorkers()
{
    return { WORKER0, WORKER1, WORKER2 };
}

std::string BuildHeatRebalanceInjectActions()
{
    // WorkerOCServer.heatMaintenanceIntervalMs shortens HEAT_MAINTENANCE_INTERVAL_MS (default 30s) so heat decay
    // + hot-primary accounting keep pace with the 200ms NodeSelector report cycle. Without this the source-trigger
    // hot bytes ratio stays stale for up to 30s and the rebalance task is never dispatched within the 30s test
    // timeout (HEAT-REV-002 throttle).
    return "NodeSelector.setInterval:call(200);"
           "ResourceManager.setInterval:call(200);"
           "WorkerOCServer.heatMaintenanceIntervalMs:call(200);"
           "TcpMigrateTransport.MigrateDataToRemote.delay:100000*call();"
           "HeatRebalanceScheduler.AssignTask:100000*call();"
           "HeatRebalanceScheduler.ExpireTask:100000*call();"
           "worker.migrate_service.return:100000*call()";
}

void SleepMs(int timeoutMs)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
}
}  // namespace

TEST(HeatWatermarkCollectorTest, ParsesHeatPrimaryAndHotPrimaryWatermarks)
{
    HeatWatermarkSample sample;
    DS_ASSERT_OK(HeatWatermarkCollector::ParseMetricValue(
        "25/50/100/0.250000000/0.500000000/0.500000000/1 |", WORKER1, sample));
    EXPECT_EQ(sample.workerIndex, WORKER1);
    EXPECT_EQ(sample.hotPrimaryBytes, 25UL);
    EXPECT_EQ(sample.primaryBytes, 50UL);
    EXPECT_EQ(sample.copyCapacity, 100UL);
    EXPECT_DOUBLE_EQ(sample.heatDataWatermark, 0.5);
    EXPECT_DOUBLE_EQ(sample.primaryWatermark, 0.5);
    EXPECT_DOUBLE_EQ(sample.hotPrimaryWatermark, 0.25);
    EXPECT_TRUE(sample.valid);

    EXPECT_EQ(HeatWatermarkCollector::ParseMetricValue(
                  "51/50/100/0.510000000/0.500000000/1.000000000/1", WORKER1, sample)
                  .GetCode(),
              K_RUNTIME_ERROR);
}

class KVClientHeatRebalanceTest : public KVClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 3;
        opts.numEtcd = 1;
        opts.numOBS = 0;
        opts.workerGflagParams =
            "-shared_memory_size_mb=64 -log_monitor=true -log_monitor_interval_ms=1000 "
            "-enable_memory_rebalance=true "
            "-rebalance_strategy=heat -eviction_strategy=heat "
            "-eviction_heat_initial_counter=0 -eviction_heat_threshold=0 -eviction_heat_max_counter=32 "
            "-rebalance_heat_hot_counter_threshold=0.001 "
            "-rebalance_heat_source_usage_percent=60 -rebalance_heat_source_hot_ratio_percent=40 "
            "-rebalance_heat_source_usage_percent_low=50 "
            "-rebalance_heat_target_usage_percent=50 -rebalance_heat_target_hot_ratio_percent=30 "
            "-rebalance_task_report_grace_ms=500 -data_migrate_rate_limit_mb=1024";
        opts.injectActions = BuildHeatRebalanceInjectActions();
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestEtcdInstance();
        InitTestKVClient(WORKER0, client0_);
        InitTestKVClient(WORKER1, client1_);
        InitTestKVClient(WORKER2, client2_);
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
                              char valueChar = 'a', size_t valueSize = VALUE_SIZE)
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
            auto rc = client->Set(key, batch.value);
            if (rc.IsError()) {
                ADD_FAILURE() << FormatString("Set %s failed: %s", key, rc.ToString());
                return batch;
            }
            batch.keys.emplace_back(std::move(key));
        }
        return batch;
    }

    ObjectBatch WriteObjectsToWorker(const std::shared_ptr<KVClient> &client, uint32_t workerIndex, size_t count,
                                     char valueChar)
    {
        ObjectBatch batch;
        batch.value.assign(VALUE_SIZE, valueChar);
        GetObjectKeysHashToWorker(db_.get(), workerIndex, count, batch.keys);
        for (const auto &key : batch.keys) {
            auto rc = client->Set(key, batch.value);
            if (rc.IsError()) {
                ADD_FAILURE() << FormatString("Set %s failed: %s", key, rc.ToString());
                return batch;
            }
        }
        return batch;
    }

    // Warm cache hits so primary copies cross the hot threshold (heat = initial + hits > hot_threshold).
    void WarmCacheHits(const std::shared_ptr<KVClient> &client, const ObjectBatch &batch, int times)
    {
        for (int t = 0; t < times; ++t) {
            for (const auto &key : batch.keys) {
                std::string value;
                auto rc = client->Get(key, value, GET_TIMEOUT_MS);
                if (rc.IsError()) {
                    ADD_FAILURE() << FormatString("warm-up Get %s failed: %s", key, rc.ToString());
                    return;
                }
            }
        }
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
            total += GetInjectCountIfAlive(workerIndex, name);
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

    void WaitForTotalInjectCount(const std::string &name, uint64_t expectedCount, int timeoutMs = REBALANCE_TIMEOUT_MS)
    {
        ASSERT_TRUE(WaitFor([&] { return GetTotalInjectCount(name) >= expectedCount; }, timeoutMs))
            << FormatString("inject point %s did not reach %lu, current=%lu", name, expectedCount,
                             GetTotalInjectCount(name));
    }

    Status WaitForHeatWatermarks(bool requireHotPrimary, const std::string &phase,
                                 std::vector<HeatWatermarkSample> &samples)
    {
        HeatWatermarkCollector collector(cluster_->GetRootDir(), static_cast<uint32_t>(AllWorkers().size()));
        RETURN_IF_NOT_OK(collector.WaitFor(
            [requireHotPrimary](const std::vector<HeatWatermarkSample> &current) {
                return std::any_of(current.begin(), current.end(), [requireHotPrimary](const auto &sample) {
                    return sample.primaryBytes > 0 && (!requireHotPrimary || sample.hotPrimaryBytes > 0);
                });
            },
            REBALANCE_TIMEOUT_MS, POLL_INTERVAL_MS, samples));
        HeatWatermarkCollector::Log(phase, samples);
        return Status::OK();
    }

    std::shared_ptr<KVClient> client0_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
};

// A hot, high-usage worker triggers a heat rebalance task and migrates hot primary copies; data stays readable.
TEST_F(KVClientHeatRebalanceTest, HeatRebalanceTriggersAndMigratesHotPrimary)
{
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);
    auto sourceSendBaseline = GetInjectCountIfAlive(WORKER0, SOURCE_SEND_POINT);

    SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
    // Nine 4MiB payloads have 5MiB real allocations, so worker 0 usage exceeds 60% of the 64MiB shm while each
    // object remains below the scheduler's 10%-of-capacity per-task budget.
    auto batch = WriteObjectsToWorker(client0_, WORKER0, 9, 's');
    // Each 5 MiB real allocation receives roughly 4 KiB / 5 MiB heat per hit. Three hits cross the 0.001 test
    // threshold with decay margin, while untouched primaries remain at zero.
    WarmCacheHits(client0_, batch, HOT_WARMUP_GETS);
    std::vector<HeatWatermarkSample> watermarks;
    DS_ASSERT_OK(WaitForHeatWatermarks(true, "hot-before-rebalance", watermarks));

    // The 200ms report/swap inject speedup makes the heat scheduler dispatch within seconds.
    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    ASSERT_TRUE(WaitFor([&] { return GetInjectCountIfAlive(WORKER0, SOURCE_SEND_POINT) >= sourceSendBaseline + 1; }))
        << "the deterministic hot source worker did not send a rebalance batch";
    SleepMs(SHORT_WAIT_MS);

    // Data migrated to the target must still be readable (no loss / corruption).
    AssertReadable(client0_, batch);
}

// Without the warm-up Gets, primaries stay at heat=initial(0), which does not exceed the hot threshold. Keep usage
// between the low and high source watermarks so neither the pressure fallback nor the heat trigger should fire.
TEST_F(KVClientHeatRebalanceTest, NoTriggerWhenNoHotPrimary)
{
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);
    SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
    auto batch = WriteObjects(client0_, "heat_rebalance_cold", 6, 'c');  // no warm-up: heat stays 0, not hot
    (void)batch;
    std::vector<HeatWatermarkSample> watermarks;
    DS_ASSERT_OK(WaitForHeatWatermarks(false, "cold-before-rebalance", watermarks));
    for (const auto &sample : watermarks) {
        EXPECT_EQ(sample.hotPrimaryBytes, 0UL) << sample.ToString();
        EXPECT_DOUBLE_EQ(sample.hotPrimaryWatermark, 0.0) << sample.ToString();
        EXPECT_DOUBLE_EQ(sample.heatDataWatermark, 0.0) << sample.ToString();
    }
    SleepMs(SHORT_WAIT_MS * 2);  // give the fast 200ms report cycle time to (not) schedule
    ASSERT_EQ(GetTotalInjectCount(ASSIGN_TASK_POINT), assignBaseline)
        << "heat rebalance should not trigger when no primary is hot (hotBytes 0 / 64MB = 0% <= 40%)";
}
}  // namespace st
}  // namespace datasystem
