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

/** Description: Verifies cleanup of routed SHM allocations after ambiguous Create timeouts. */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "client/object_cache/oc_client_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/kv_client.h"

DS_DECLARE_string(host_id_env_name);

namespace datasystem {
namespace st {
namespace {
constexpr uint32_t WORKER_INDEX = 0;
constexpr size_t WORKER_SHM_SIZE_MB = 1 * 1024;
constexpr size_t VALUE_SIZE = 8 * 1024 * 1024;
constexpr size_t TIMEOUT_SET_COUNT = 130;
// Each cleanup task retries for about 500 ms. A 150 ms sequential request cadence keeps at most four tasks active,
// matching the dedicated four-thread cleanup pool without adding artificial sleeps between Set calls.
constexpr int32_t REQUEST_TIMEOUT_MS = 150;
constexpr int32_t WORKER_CREATE_DELAY_MS = 300;
constexpr int32_t AMBIGUOUS_CREATE_CLEANUP_WINDOW_MS = 500;
constexpr int32_t TEST_CASE_TIMEOUT_SEC = 180;
constexpr uint64_t HARD_RECLAIM_TIMEOUT_MS = 10 * 60 * 1000;
constexpr uint64_t FALLBACK_HARD_RECLAIM_TIMEOUT_MS = 1'000;
constexpr size_t CLEANUP_POOL_CAPACITY = 4;
constexpr size_t CLEANUP_POOL_OVERFLOW_SET_COUNT = CLEANUP_POOL_CAPACITY + 4;
constexpr int32_t BLOCKED_CLEANUP_MS = 3'000;
constexpr int32_t METRIC_WAIT_TIMEOUT_SEC = 20;
constexpr char CREATE_RESPONSE_DELAY_INJECT[] = "worker.Create.AllocateMemory";
constexpr char CREATE_BEFORE_ADD_INJECT[] = "worker.Create.BeforeAddShmUnit";
constexpr char FORCE_RECLAIMABLE_INJECT[] = "worker.Create.reclaimable";
constexpr char REMOVE_SHM_UNIT_INJECT[] = "RemoveShmUnit";
constexpr char REMOVE_BEFORE_LOOKUP_INJECT[] = "RemoveShmUnit.BeforeLookup";
constexpr char HOST_ID_ENV_NAME[] = "create_timeout_cleanup_host_id";
constexpr char HOST_ID_VALUE[] = "create-timeout-cleanup-host";
constexpr char KEY_PREFIX[] = "create_timeout_cleanup_";
constexpr char REF_ADD_METRIC[] = "worker_shm_ref_add_total";
constexpr char REF_REMOVE_METRIC[] = "worker_shm_ref_remove_total";
constexpr char REF_TABLE_SIZE_METRIC[] = "worker_shm_ref_table_size";
constexpr char HARD_RECLAIM_METRIC[] = "worker_shm_ref_hard_reclaim_total";
constexpr char CLEANUP_DROPPED_METRIC[] = "client_ambiguous_create_cleanup_dropped_total";
}  // namespace

class KVClientCreateTimeoutCleanupTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.numCoordinators = 0;
        opts.workerGflagParams =
            std::string(" -shared_memory_size_mb=") + std::to_string(WORKER_SHM_SIZE_MB)
            + " -ipc_through_shared_memory=true -arena_per_tenant=1 -enable_urma=false"
            + " -log_monitor=true -log_monitor_interval_ms=200"
            + " -shm_ref_hard_reclaim_timeout_ms=" + std::to_string(GetHardReclaimTimeoutMs())
            + " -host_id_env_name=" + HOST_ID_ENV_NAME;
    }

    void SetUp() override
    {
        previousHostIdEnvName_ = FLAGS_host_id_env_name;
        FLAGS_host_id_env_name = HOST_ID_ENV_NAME;
        ASSERT_EQ(setenv(HOST_ID_ENV_NAME, HOST_ID_VALUE, 1), 0);
        ExternalClusterTest::SetUp();
        ConnectOptions options;
        InitConnectOpt(WORKER_INDEX, options);
        options.enableLocalCache = false;
        options.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        options.requestTimeoutMs = REQUEST_TIMEOUT_MS;
        client_ = std::make_shared<KVClient>(options);
        DS_ASSERT_OK(client_->Init());
    }

    void TearDown() override
    {
        if (cluster_ != nullptr) {
            (void)cluster_->ClearInjectAction(WORKER, WORKER_INDEX, CREATE_RESPONSE_DELAY_INJECT);
            (void)cluster_->ClearInjectAction(WORKER, WORKER_INDEX, CREATE_BEFORE_ADD_INJECT);
            (void)cluster_->ClearInjectAction(WORKER, WORKER_INDEX, FORCE_RECLAIMABLE_INJECT);
            (void)cluster_->ClearInjectAction(WORKER, WORKER_INDEX, REMOVE_SHM_UNIT_INJECT);
            (void)cluster_->ClearInjectAction(WORKER, WORKER_INDEX, REMOVE_BEFORE_LOOKUP_INJECT);
        }
        client_.reset();
        ExternalClusterTest::TearDown();
        FLAGS_host_id_env_name = previousHostIdEnvName_;
        (void)unsetenv(HOST_ID_ENV_NAME);
    }

protected:
    struct WorkerRefMetrics {
        std::optional<int64_t> adds;
        std::optional<int64_t> removes;
        std::optional<int64_t> tableSize;
        std::optional<int64_t> hardReclaims;
    };

    virtual uint64_t GetHardReclaimTimeoutMs() const
    {
        return HARD_RECLAIM_TIMEOUT_MS;
    }

    int GetTestCaseTimeoutSecs() const override
    {
        return TEST_CASE_TIMEOUT_SEC;
    }

    std::shared_ptr<KVClient> client_;
    std::string previousHostIdEnvName_;

    WorkerRefMetrics ReadWorkerRefMetrics() const
    {
        const std::string path = FormatString("%s/worker%u/log/kv_metrics.log", cluster_->GetRootDir(), WORKER_INDEX);
        std::ifstream input(path);
        WorkerRefMetrics result;
        std::string line;
        while (std::getline(input, line)) {
            const auto summary = nlohmann::json::parse(line, nullptr, false);
            if (summary.is_discarded() || !summary.contains("metrics")) {
                continue;
            }
            for (const auto &metric : summary["metrics"]) {
                const auto name = metric.value("name", "");
                if (!metric.contains("total")) {
                    continue;
                }
                if (name == REF_ADD_METRIC) {
                    result.adds = metric["total"].get<int64_t>();
                } else if (name == REF_REMOVE_METRIC) {
                    result.removes = metric["total"].get<int64_t>();
                } else if (name == REF_TABLE_SIZE_METRIC) {
                    result.tableSize = metric["total"].get<int64_t>();
                } else if (name == HARD_RECLAIM_METRIC) {
                    result.hardReclaims = metric["total"].get<int64_t>();
                }
            }
        }
        return result;
    }

    Status WaitForWorkerRefsToConverge(size_t expectedAdds, int64_t minimumHardReclaims)
    {
        return cluster_->WaitForExpectedResult(
            [this, expectedAdds, minimumHardReclaims]() {
                const auto metrics = ReadWorkerRefMetrics();
                const bool refsConverged = metrics.adds.value_or(0) >= static_cast<int64_t>(expectedAdds)
                                           && metrics.removes == metrics.adds
                                           && metrics.tableSize.value_or(-1) == 0;
                const bool fallbackObserved = metrics.hardReclaims.value_or(0) >= minimumHardReclaims;
                return refsConverged && fallbackObserved ? Status::OK()
                                                        : Status(K_NOT_READY, "Worker SHM refs have not converged");
            },
            METRIC_WAIT_TIMEOUT_SEC, K_OK);
    }

    Status WaitForInjectCount(const std::string &injectPoint, uint64_t expectedCount)
    {
        return cluster_->WaitForExpectedResult(
            [this, &injectPoint, expectedCount]() {
                uint64_t executeCount = 0;
                RETURN_IF_NOT_OK(
                    cluster_->GetInjectActionExecuteCount(WORKER, WORKER_INDEX, injectPoint, executeCount));
                return executeCount >= expectedCount ? Status::OK()
                                                     : Status(K_NOT_READY, "Inject count has not reached target");
            },
            METRIC_WAIT_TIMEOUT_SEC, K_OK);
    }

    Status WaitForWorkerRefToAppear(const WorkerRefMetrics &baseline)
    {
        return cluster_->WaitForExpectedResult(
            [this, &baseline]() {
                const auto metrics = ReadWorkerRefMetrics();
                const bool added = metrics.adds.value_or(0) > baseline.adds.value_or(0);
                const bool retained = metrics.tableSize.value_or(0) > baseline.tableSize.value_or(0);
                const bool notHardReclaimed = metrics.hardReclaims.value_or(0) == baseline.hardReclaims.value_or(0);
                return added && retained && notHardReclaimed
                           ? Status::OK()
                           : Status(K_NOT_READY, "Late Create reference has not appeared");
            },
            METRIC_WAIT_TIMEOUT_SEC, K_OK);
    }

    Status WaitForWorkerRefsToReturn(const WorkerRefMetrics &baseline, int64_t expectedHardReclaims)
    {
        return cluster_->WaitForExpectedResult(
            [this, &baseline, expectedHardReclaims]() {
                const auto metrics = ReadWorkerRefMetrics();
                const bool added = metrics.adds.value_or(0) > baseline.adds.value_or(0);
                const bool removed = metrics.removes.value_or(0) > baseline.removes.value_or(0);
                const bool returned = metrics.tableSize.value_or(0) == baseline.tableSize.value_or(0);
                const bool hardReclaimed = metrics.hardReclaims.value_or(0)
                                           >= baseline.hardReclaims.value_or(0) + expectedHardReclaims;
                return added && removed && returned && hardReclaimed
                           ? Status::OK()
                           : Status(K_NOT_READY, "Worker hard reclaim has not restored the reference table");
            },
            METRIC_WAIT_TIMEOUT_SEC, K_OK);
    }

    static int64_t ReadClientMetric(const std::string &metricName)
    {
        for (const auto &text : metrics::DumpSummariesForTest()) {
            const auto summary = nlohmann::json::parse(text, nullptr, false);
            if (summary.is_discarded() || !summary.contains("metrics")) {
                continue;
            }
            for (const auto &metric : summary["metrics"]) {
                if (metric.value("name", "") == metricName && metric.contains("total")) {
                    return metric["total"].get<int64_t>();
                }
            }
        }
        return 0;
    }
};

class KVClientCreateTimeoutHardReclaimTest : public KVClientCreateTimeoutCleanupTest {
protected:
    uint64_t GetHardReclaimTimeoutMs() const override
    {
        return FALLBACK_HARD_RECLAIM_TIMEOUT_MS;
    }
};

TEST_F(KVClientCreateTimeoutCleanupTest, LEVEL2_AmbiguousCreateTimeoutsDoNotExhaustWorkerShm)
{
    const std::string value(VALUE_SIZE, 'a');
    const SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT, .ttlSecond = 10 };
    const std::string injectAction =
        std::to_string(TIMEOUT_SET_COUNT) + "*sleep(" + std::to_string(WORKER_CREATE_DELAY_MS) + ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, CREATE_RESPONSE_DELAY_INJECT, injectAction));

    for (size_t i = 0; i < TIMEOUT_SET_COUNT; ++i) {
        const Status rc = client_->Set(KEY_PREFIX + std::to_string(i), value, param);
        ASSERT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED) << "unexpected Set result at index " << i << ": " << rc;
    }

    uint64_t executeCount = 0;
    DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, WORKER_INDEX, CREATE_RESPONSE_DELAY_INJECT,
                                                       executeCount));
    ASSERT_EQ(executeCount, TIMEOUT_SET_COUNT)
        << "every timed-out Set must reach the post-AddShmUnit inject point";
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, CREATE_RESPONSE_DELAY_INJECT));

    DS_ASSERT_OK(WaitForWorkerRefsToConverge(TIMEOUT_SET_COUNT, 0));
    DS_ASSERT_OK(client_->Set(KEY_PREFIX + std::string("final"), value, param));
}

TEST_F(KVClientCreateTimeoutHardReclaimTest, LEVEL2_CleanupPoolOverflowFallsBackToWorkerHardReclaim)
{
    const std::string value(VALUE_SIZE, 'b');
    const SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT, .ttlSecond = 10 };
    const std::string createDelay = std::to_string(CLEANUP_POOL_OVERFLOW_SET_COUNT) + "*sleep("
                                    + std::to_string(WORKER_CREATE_DELAY_MS) + ")";
    const std::string cleanupDelay = std::to_string(CLEANUP_POOL_CAPACITY) + "*sleep("
                                     + std::to_string(BLOCKED_CLEANUP_MS) + ")";
    const int64_t droppedBefore = ReadClientMetric(CLEANUP_DROPPED_METRIC);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, FORCE_RECLAIMABLE_INJECT, "call(1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, REMOVE_SHM_UNIT_INJECT, cleanupDelay));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, CREATE_RESPONSE_DELAY_INJECT, createDelay));

    for (size_t i = 0; i < CLEANUP_POOL_OVERFLOW_SET_COUNT; ++i) {
        const Status rc = client_->Set(KEY_PREFIX + std::string("fallback_") + std::to_string(i), value, param);
        ASSERT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED) << "unexpected Set result at index " << i << ": " << rc;
    }

    const int64_t droppedCount = ReadClientMetric(CLEANUP_DROPPED_METRIC) - droppedBefore;
    ASSERT_GT(droppedCount, 0)
        << "overflow must exercise the bounded cleanup pool drop path";
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, CREATE_RESPONSE_DELAY_INJECT));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, REMOVE_SHM_UNIT_INJECT));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, FORCE_RECLAIMABLE_INJECT));
    DS_ASSERT_OK(WaitForWorkerRefsToConverge(CLEANUP_POOL_OVERFLOW_SET_COUNT, droppedCount));
    DS_ASSERT_OK(client_->Set(KEY_PREFIX + std::string("fallback_final"), value, param));
}

TEST_F(KVClientCreateTimeoutHardReclaimTest, LEVEL2_CreateAfterCleanupWindowFallsBackToWorkerHardReclaim)
{
    constexpr uint64_t cleanupAttemptCount = 3;
    const std::string value(VALUE_SIZE, 'c');
    const SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT, .ttlSecond = 10 };
    const auto baseline = ReadWorkerRefMetrics();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, FORCE_RECLAIMABLE_INJECT, "call(1)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, REMOVE_BEFORE_LOOKUP_INJECT,
                                           std::to_string(cleanupAttemptCount) + "*sleep(0)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, WORKER_INDEX, CREATE_BEFORE_ADD_INJECT, "1*pause()"));

    const auto createStart = std::chrono::steady_clock::now();
    const Status rc = client_->Set(KEY_PREFIX + std::string("late_create"), value, param);
    ASSERT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED) << "unexpected Set result: " << rc;
    DS_ASSERT_OK(WaitForInjectCount(CREATE_BEFORE_ADD_INJECT, 1));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, FORCE_RECLAIMABLE_INJECT));
    DS_ASSERT_OK(WaitForInjectCount(REMOVE_BEFORE_LOOKUP_INJECT, cleanupAttemptCount));
    const auto delayedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - createStart);
    ASSERT_GT(delayedMs.count(), AMBIGUOUS_CREATE_CLEANUP_WINDOW_MS);

    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, CREATE_BEFORE_ADD_INJECT));
    DS_ASSERT_OK(WaitForWorkerRefToAppear(baseline));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, WORKER_INDEX, REMOVE_BEFORE_LOOKUP_INJECT));
    DS_ASSERT_OK(WaitForWorkerRefsToReturn(baseline, 1));
    DS_ASSERT_OK(client_->Set(KEY_PREFIX + std::string("late_create_final"), value, param));
}
}  // namespace st
}  // namespace datasystem
