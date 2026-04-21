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
 * Description: Unit tests for the 8 master TTL chain + worker fallback metrics
 *   (worker_remove_client_refs_total, worker_object_erase_total,
 *    master_object_meta_table_size, master_ttl_{pending_size,fire,delete_success,delete_failed,retry}_total).
 */

#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "shm_leak_metrics_test_base.h"

#include <cstdint>
#include <string>

#include "gtest/gtest.h"

namespace datasystem {
namespace ut {
namespace {

class ShmLeakMetricsPhase2Test : public ShmLeakMetricsTestBase {};

// ── [BASIC] all 8 phase-2 metrics registered and zero ────────────────────────
TEST_F(ShmLeakMetricsPhase2Test, all_phase2_metrics_registered_and_zero)
{
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_remove_client_refs_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_object_erase_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_object_meta_table_size", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_ttl_pending_size", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_ttl_fire_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_ttl_delete_success_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_ttl_delete_failed_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_ttl_retry_total", "total"), 0);
}

// ── [BASIC] descriptor count covers phase-2 metrics ──────────────────────────
TEST_F(ShmLeakMetricsPhase2Test, metric_descs_count_includes_phase2)
{
    size_t count = 0;
    (void)metrics::GetKvMetricDescs(count);
    EXPECT_GE(count, static_cast<size_t>(metrics::KvMetricId::MASTER_TTL_RETRY_TOTAL) + 1);
}

// ── [WORKER] RemoveClient batch count + ClearObject count ────────────────────
TEST_F(ShmLeakMetricsPhase2Test, worker_remove_client_and_object_erase_counters)
{
    Cnt(metrics::KvMetricId::WORKER_REMOVE_CLIENT_REFS_TOTAL).Inc(7);
    Cnt(metrics::KvMetricId::WORKER_OBJECT_ERASE_TOTAL).Inc();
    Cnt(metrics::KvMetricId::WORKER_OBJECT_ERASE_TOTAL).Inc();
    Cnt(metrics::KvMetricId::WORKER_OBJECT_ERASE_TOTAL).Inc();
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_remove_client_refs_total", "total"), 7);
    EXPECT_EQ(Scalar(s, "worker_object_erase_total", "total"), 3);
}

// ── [MASTER-METATABLE] periodic Gauge.Set replay ─────────────────────────────
// Mirrors what ExpiredObjectManager::Run does once per second:
// metrics::GetGauge(...).Set(ocMetadataManager_->GetMetaTableSize()).
TEST_F(ShmLeakMetricsPhase2Test, master_meta_table_size_gauge_replays_size)
{
    auto g = Gge(metrics::KvMetricId::MASTER_OBJECT_META_TABLE_SIZE);
    g.Set(0);
    g.Set(150);
    g.Set(200);
    g.Set(180);
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "master_object_meta_table_size", "total"), 180);
}

// ── [MASTER-TTL] pending Gauge round-trip: insert N, fire all → returns to 0 ──
// Simulates the (InsertObjectUnlock × N) ↔ (GetExpiredObject N) symmetry.
TEST_F(ShmLeakMetricsPhase2Test, master_ttl_pending_round_trip)
{
    auto pending = Gge(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE);
    constexpr int kN = 50;
    for (int i = 0; i < kN; ++i) {
        pending.Inc();
    }
    {
        auto s = DumpSummaryJson();
        EXPECT_EQ(Scalar(s, "master_ttl_pending_size", "total"), kN);
    }
    // GetExpiredObject pulls them all out; FIRE counter increments by kN.
    Cnt(metrics::KvMetricId::MASTER_TTL_FIRE_TOTAL).Inc(kN);
    pending.Dec(kN);
    {
        auto s = DumpSummaryJson();
        EXPECT_EQ(Scalar(s, "master_ttl_pending_size", "total"), 0);
        EXPECT_EQ(Scalar(s, "master_ttl_fire_total", "total"), kN);
    }
}

// ── [MASTER-TTL] FIRE == SUCCESS + FAILED  invariant ─────────────────────────
TEST_F(ShmLeakMetricsPhase2Test, master_ttl_fire_equals_success_plus_failed)
{
    constexpr int kFire = 100;
    constexpr int kSuccess = 88;
    constexpr int kFailed = 12;
    static_assert(kSuccess + kFailed == kFire, "Invariant in test setup must hold");
    Cnt(metrics::KvMetricId::MASTER_TTL_FIRE_TOTAL).Inc(kFire);
    Cnt(metrics::KvMetricId::MASTER_TTL_DELETE_SUCCESS_TOTAL).Inc(kSuccess);
    Cnt(metrics::KvMetricId::MASTER_TTL_DELETE_FAILED_TOTAL).Inc(kFailed);
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "master_ttl_fire_total", "total"), kFire);
    EXPECT_EQ(Scalar(s, "master_ttl_delete_success_total", "total"), kSuccess);
    EXPECT_EQ(Scalar(s, "master_ttl_delete_failed_total", "total"), kFailed);
}

// ── [MASTER-TTL] retry counter + pending Gauge re-grow on failure backlog ────
// Simulates AddFailedObject re-queuing N objects: pending Inc(N) + retry Inc(N).
TEST_F(ShmLeakMetricsPhase2Test, master_ttl_retry_and_pending_grow_on_failure)
{
    auto pending = Gge(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE);
    constexpr int kFail = 25;
    Cnt(metrics::KvMetricId::MASTER_TTL_RETRY_TOTAL).Inc(kFail);
    pending.Inc(kFail);
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "master_ttl_retry_total", "total"), kFail);
    EXPECT_EQ(Scalar(s, "master_ttl_pending_size", "total"), kFail);
}

// ── [LEAK-SHAPE] TTL chain stuck pattern: pending grows, fire stagnant ───────
// Production signature: master_ttl_pending_size monotonic-up while
// master_ttl_fire_total delta = 0  →  scanner thread blocked / async pool full.
TEST_F(ShmLeakMetricsPhase2Test, ttl_chain_stuck_shape)
{
    auto pending = Gge(metrics::KvMetricId::MASTER_TTL_PENDING_SIZE);
    pending.Inc(500);
    // intentionally NO FIRE/SUCCESS/FAILED bumps
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "master_ttl_pending_size", "total"), 500);
    EXPECT_EQ(Scalar(s, "master_ttl_fire_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "master_ttl_delete_success_total", "total"), 0);
    pending.Dec(500);  // cleanup so test does not leak across cases
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
