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
 * Description: End-to-end evict/spill parity tests for the heat-based eviction
 * strategy (FLAGS_eviction_strategy=heat). Reuses the clock-strategy evict
 * scenarios under the heat worker flags to confirm the new strategy still drives
 * memory-pressure eviction and spill round-trips correctly. Half-lives are set
 * long so the test exercises eviction selection (coldest/oldest) without coupling
 * to the 30s periodic-decay cadence; the decay math itself is covered by UT.
 */

#include "datasystem/kv_client.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "cluster/heat_watermark_collector.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
namespace {
const std::string HOST_IP = "127.0.0.1";
}  // namespace

// Mirrors KVCacheClientEvictTest (spill ON, 8MB shared memory) but switches the
// worker to the heat eviction strategy. Long half-lives keep heat ~capped during
// the fast test so selection reduces to oldest-lastAccess (LRU-like), exercising the
// full eviction + spill path under the new strategy without depending on decay timing.
class KVCacheClientHeatEvictTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableSpill = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=8 -log_monitor=true -log_monitor_interval_ms=1000 -v=1 -spill_size_limit="
            + std::to_string(maxSize_)
            + " -eviction_strategy=heat -eviction_heat_threshold=2"
              " -rebalance_heat_hot_counter_threshold=2"
              " -eviction_heat_half_life_primary_s=3600 -eviction_heat_half_life_local_s=1800";
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
        opts.injectActions = "worker.Spill.Sync:return();NodeSelector.setInterval:call(200);"
                             "WorkerOCServer.heatMaintenanceIntervalMs:call(200)";
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            opts.workerSpecifyGflagParams[i] = FormatString("-shared_disk_directory=%s -shared_disk_size_mb=8", dir);
        }
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClient(0, client_);
    }

protected:
    Status WaitForHeatWatermarks(bool requireHotPrimary, const std::string &phase,
                                 std::vector<HeatWatermarkSample> &samples)
    {
        HeatWatermarkCollector collector(cluster_->GetRootDir(), 1);
        RETURN_IF_NOT_OK(collector.WaitFor(
            [requireHotPrimary](const std::vector<HeatWatermarkSample> &current) {
                return std::any_of(current.begin(), current.end(), [requireHotPrimary](const auto &sample) {
                    return sample.primaryBytes > 0 && (!requireHotPrimary || sample.hotPrimaryBytes > 0);
                });
            },
            30'000, 100, samples));
        HeatWatermarkCollector::Log(phase, samples);
        return Status::OK();
    }

    std::shared_ptr<KVClient> client_;
    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
};

// Over-fill 8MB shared memory with NONE_L2_CACHE_EVICT objects; confirm the heat
// strategy triggers eviction (spill) under memory pressure and the newest key (still in
// memory) is readable. Mirrors the clock strategy's TestNoneL2CacheEvictTypeBasicFunction
// for parity. (Does not Get the oldest key: with 400MB into a 64MB spill limit, the oldest
// spilled objects are spill-evicted by EvictSpilledObjects, so they are intentionally gone.)
TEST_F(KVCacheClientHeatEvictTest, TestNoneL2CacheEvictBasicUnderHeatStrategy)
{
    LOG(INFO) << "Test None L2 cache evictable objects under heat eviction strategy";
    size_t count = 100;
    size_t dataSize = 4 * 1024ul * 1024ul;  // 4MB each -> 400MB into 8MB shm forces eviction
    std::string data(dataSize, '0');
    std::vector<std::string> keys;
    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    // The newest object should still be readable from memory.
    std::string val;
    DS_ASSERT_OK(client_->Get(keys.back(), val));
    ASSERT_FALSE(val.empty());
    ASSERT_EQ(val.size(), dataSize);
    std::vector<HeatWatermarkSample> watermarks;
    DS_ASSERT_OK(WaitForHeatWatermarks(false, "heat-evict-after-pressure", watermarks));
}

// Verify that cache hits under the heat strategy do not corrupt data: hit an object, then
// trigger its eviction with same-sized fillers, then re-Get it and confirm intact bytes.
// Uses 4MB homogeneous objects (matching the basic evict test) to avoid per-arena size
// fragmentation; few enough fillers that the spill limit (64MB) stays well below full, so
// the spilled hit key survives on disk.
TEST_F(KVCacheClientHeatEvictTest, TestCacheHitEvictDataIntegrityUnderHeatStrategy)
{
    LOG(INFO) << "Test cache-hit + evict data integrity under heat eviction strategy";
    size_t dataSize = 4 * 1024ul * 1024ul;  // 4MB, homogeneous to avoid arena fragmentation
    std::string data(dataSize, 'h');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    auto key = client_->Set(data, param);
    ASSERT_FALSE(key.empty());

    // Repeated cache hits bump the heat counter via OnCacheHit.
    for (int i = 0; i < 5; ++i) {
        std::string val;
        DS_ASSERT_OK(client_->Get(key, val));
        ASSERT_EQ(val, data);
    }
    std::vector<HeatWatermarkSample> watermarks;
    DS_ASSERT_OK(WaitForHeatWatermarks(true, "heat-evict-after-cache-hits", watermarks));

    // Write 4MB fillers to trigger eviction of the oldest (the hit key, added first).
    std::string filler(dataSize, 'x');
    for (int i = 0; i < 5; ++i) {
        SetParam p{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
        ASSERT_FALSE(client_->Set(filler, p).empty());
    }

    // The hit object must remain retrievable with intact data (in-memory or from spill).
    std::string val;
    DS_ASSERT_OK(client_->Get(key, val));
    ASSERT_EQ(val, data);
}

}  // namespace st
}  // namespace datasystem
