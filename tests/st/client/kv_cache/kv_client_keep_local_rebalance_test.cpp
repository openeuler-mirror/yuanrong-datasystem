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
 * Description: End-to-end ST for the keep-local-copy rebalance migration mode.
 *
 * Uses the heat rebalance strategy (for deterministic triggering via warm-up Gets) and explicitly enables
 * rebalance_keep_local_copy. After migration, the source keeps a local non-primary
 * copy (demoted, not erased) and data must remain readable from all clients.
 */

#include <algorithm>
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

const std::string SOURCE_SEND_POINT = "TcpMigrateTransport.MigrateDataToRemote.delay";
const std::string ASSIGN_TASK_POINT = "HeatRebalanceScheduler.AssignTask";

struct ObjectBatch {
    std::vector<std::string> keys;
    std::string value;
};

std::vector<uint32_t> AllWorkers()
{
    return { WORKER0, WORKER1, WORKER2 };
}

std::string BuildInjectActions()
{
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

class KVClientKeepLocalRebalanceTest : public KVClientCommon {
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
            "-rebalance_keep_local_copy=true "
            "-eviction_heat_initial_counter=0 -eviction_heat_threshold=0 -eviction_heat_max_counter=32 "
            "-rebalance_heat_hot_counter_threshold=0.001 "
            "-rebalance_heat_source_usage_percent=60 -rebalance_heat_source_hot_ratio_percent=40 "
            "-rebalance_heat_source_usage_percent_low=50 "
            "-rebalance_heat_target_usage_percent=50 -rebalance_heat_target_hot_ratio_percent=30 "
            "-rebalance_task_report_grace_ms=500 -data_migrate_rate_limit_mb=1024";
        opts.injectActions = BuildInjectActions();
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
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
                             char valueChar = 'a')
    {
        ObjectBatch batch;
        if (client == nullptr) {
            ADD_FAILURE() << "client is null";
            return batch;
        }
        batch.value.assign(VALUE_SIZE, valueChar);
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

    Status WaitForHeatWatermarks(const std::string &phase, std::vector<HeatWatermarkSample> &samples)
    {
        HeatWatermarkCollector collector(cluster_->GetRootDir(), static_cast<uint32_t>(AllWorkers().size()));
        RETURN_IF_NOT_OK(collector.WaitFor(
            [](const std::vector<HeatWatermarkSample> &current) {
                return std::any_of(current.begin(), current.end(), [](const auto &sample) {
                    return sample.primaryBytes > 0 && sample.hotPrimaryBytes > 0;
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

// With rebalance_keep_local_copy explicitly enabled, the heat rebalance migrates hot primaries to the target
// (which becomes the new primary) while the source keeps a local non-primary copy. Data must remain readable
// from the source client (routed to the new primary or served from local cache) — no data loss.
TEST_F(KVClientKeepLocalRebalanceTest, KeepLocalMigratesPrimaryAndKeepsLocalCopy)
{
    auto assignBaseline = GetTotalInjectCount(ASSIGN_TASK_POINT);
    auto sourceSendBaseline = GetTotalInjectCount(SOURCE_SEND_POINT);

    SleepMs(WORKER_RECEIVE_RING_DELAY_MS);
    auto batch = WriteObjects(client0_, "keep_local_source", 9, 's');
    WarmCacheHits(client0_, batch, HOT_WARMUP_GETS);
    std::vector<HeatWatermarkSample> watermarks;
    DS_ASSERT_OK(WaitForHeatWatermarks("keep-local-before-rebalance", watermarks));

    WaitForTotalInjectCount(ASSIGN_TASK_POINT, assignBaseline + 1);
    WaitForTotalInjectCount(SOURCE_SEND_POINT, sourceSendBaseline + 1);
    SleepMs(SHORT_WAIT_MS);
    DS_ASSERT_OK(WaitForHeatWatermarks("keep-local-after-rebalance", watermarks));

    // Data must still be readable — the source kept a local copy (demoted, not erased) and the target
    // became the new primary. Either path serves the correct data.
    AssertReadable(client0_, batch);
    // Also readable from a different client (routes to the new primary).
    AssertReadable(client1_, batch);
}
}  // namespace st
}  // namespace datasystem
