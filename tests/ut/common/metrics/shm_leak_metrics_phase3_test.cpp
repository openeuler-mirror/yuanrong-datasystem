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
 * Description: Unit tests for the 2 client async-release metrics
 *   (client_async_release_queue_size, client_dec_ref_skipped_total).
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

class ShmLeakMetricsPhase3Test : public ShmLeakMetricsTestBase {};

// ── [BASIC] both phase-3 metrics registered and zero ─────────────────────────
TEST_F(ShmLeakMetricsPhase3Test, all_phase3_metrics_registered_and_zero)
{
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("client_async_release_queue_size=0"), std::string::npos);
    EXPECT_NE(s.find("client_dec_ref_skipped_total=0"), std::string::npos);
}

// ── [BASIC] descriptor count covers phase-3 metrics ──────────────────────────
TEST_F(ShmLeakMetricsPhase3Test, metric_descs_count_includes_phase3)
{
    size_t count = 0;
    (void)metrics::GetKvMetricDescs(count);
    EXPECT_GE(count, static_cast<size_t>(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL) + 1);
}

// ── [SKIP] 3 early-return sites all bump the same Counter ────────────────────
// Mirrors the 3 instrumented sites in DecreaseReferenceCnt(Impl):
//   (a) asyncReleasePool_ == nullptr || shmId.Empty()  — pool gone / shm empty
//   (b) !needDecreaseWorkerRef                          — local refcount > 0
//   (c) isShm && !IsBufferAlive(version)                — worker dead / version mismatch
TEST_F(ShmLeakMetricsPhase3Test, dec_ref_skipped_counter_aggregates_three_sites)
{
    constexpr int kPoolGone = 4;
    constexpr int kRefStillPositive = 7;
    constexpr int kBufferDead = 3;
    for (int i = 0; i < kPoolGone; ++i) {
        Cnt(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL).Inc();
    }
    for (int i = 0; i < kRefStillPositive; ++i) {
        Cnt(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL).Inc();
    }
    for (int i = 0; i < kBufferDead; ++i) {
        Cnt(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL).Inc();
    }
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("client_dec_ref_skipped_total="
                     + std::to_string(kPoolGone + kRefStillPositive + kBufferDead)),
              std::string::npos);
}

// ── [QUEUE] async release Gauge replays last Set() ───────────────────────────
// Mirrors what StartMetricsThread does once per second:
// metrics::GetGauge(...).Set(asyncReleasePool_->GetWaitingTasksNum()).
TEST_F(ShmLeakMetricsPhase3Test, async_release_queue_gauge_replays_size)
{
    auto g = Gge(metrics::KvMetricId::CLIENT_ASYNC_RELEASE_QUEUE_SIZE);
    g.Set(0);
    g.Set(13);
    g.Set(42);
    g.Set(7);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("client_async_release_queue_size=7"), std::string::npos);
}

// ── [LEAK-SHAPE] bugfix §3.3 production signature ────────────────────────────
// "Client switched to standby; old async-release tasks pile up; nothing actually frees."
// Pattern: client_dec_ref_skipped_total spike + client_async_release_queue_size up.
TEST_F(ShmLeakMetricsPhase3Test, switch_standby_silent_drop_shape)
{
    Cnt(metrics::KvMetricId::CLIENT_DEC_REF_SKIPPED_TOTAL).Inc(120);
    Gge(metrics::KvMetricId::CLIENT_ASYNC_RELEASE_QUEUE_SIZE).Set(85);
    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("client_dec_ref_skipped_total=120"), std::string::npos);
    EXPECT_NE(s.find("client_async_release_queue_size=85"), std::string::npos);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
