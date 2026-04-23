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
 * Description: Unit tests for the 8 worker SHM-release-accounting metrics
 *   (worker_allocator_{alloc,free}_bytes_total, worker_shm_unit_{created,destroyed}_total,
 *    worker_shm_ref_{add,remove}_total, worker_shm_ref_table_{size,bytes}).
 */

#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "shm_leak_metrics_test_base.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace datasystem {
namespace ut {
namespace {

class ShmLeakMetricsTest : public ShmLeakMetricsTestBase {};

// ── [BASIC] all 8 new metrics are registered and start at zero ────────────────
TEST_F(ShmLeakMetricsTest, all_metrics_registered_and_zero)
{
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_allocator_alloc_bytes_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_allocator_free_bytes_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_unit_created_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_unit_destroyed_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_add_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_remove_total", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_table_size", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_table_bytes", "total"), 0);
}

// ── [BASIC] descriptor count covers all new metrics ──────────────────────────
TEST_F(ShmLeakMetricsTest, metric_descs_count_includes_phase1)
{
    size_t count = 0;
    (void)metrics::GetKvMetricDescs(count);
    EXPECT_GE(count, static_cast<size_t>(metrics::KvMetricId::WORKER_SHM_REF_TABLE_BYTES) + 1);
}

// ── [BASIC] metric names contain the expected text in summary ────────────────
TEST_F(ShmLeakMetricsTest, metric_names_present_in_summary)
{
    // Bump each new counter once so that even total=0 metrics show up under "Compare with"
    Cnt(metrics::KvMetricId::WORKER_ALLOCATOR_ALLOC_BYTES_TOTAL).Inc(8 * 1024 * 1024);
    Cnt(metrics::KvMetricId::WORKER_ALLOCATOR_FREE_BYTES_TOTAL).Inc(8 * 1024 * 1024);
    Cnt(metrics::KvMetricId::WORKER_SHM_UNIT_CREATED_TOTAL).Inc();
    Cnt(metrics::KvMetricId::WORKER_SHM_UNIT_DESTROYED_TOTAL).Inc();
    Cnt(metrics::KvMetricId::WORKER_SHM_REF_ADD_TOTAL).Inc();
    Cnt(metrics::KvMetricId::WORKER_SHM_REF_REMOVE_TOTAL).Inc();
    Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_SIZE).Set(1);
    Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_BYTES).Set(1);

    auto s = metrics::DumpSummaryForTest();
    EXPECT_NE(s.find("worker_allocator_alloc_bytes_total"), std::string::npos);
    EXPECT_NE(s.find("worker_allocator_free_bytes_total"), std::string::npos);
    EXPECT_NE(s.find("worker_shm_unit_created_total"), std::string::npos);
    EXPECT_NE(s.find("worker_shm_unit_destroyed_total"), std::string::npos);
    EXPECT_NE(s.find("worker_shm_ref_add_total"), std::string::npos);
    EXPECT_NE(s.find("worker_shm_ref_remove_total"), std::string::npos);
    EXPECT_NE(s.find("worker_shm_ref_table_size"), std::string::npos);
    EXPECT_NE(s.find("worker_shm_ref_table_bytes"), std::string::npos);
}

// ── [ALLOC] alloc/free counter delta correctness ─────────────────────────────
TEST_F(ShmLeakMetricsTest, allocator_alloc_free_counter_delta)
{
    constexpr uint64_t kSize = 8 * 1024 * 1024;
    Cnt(metrics::KvMetricId::WORKER_ALLOCATOR_ALLOC_BYTES_TOTAL).Inc(kSize);
    Cnt(metrics::KvMetricId::WORKER_ALLOCATOR_ALLOC_BYTES_TOTAL).Inc(kSize);
    Cnt(metrics::KvMetricId::WORKER_ALLOCATOR_FREE_BYTES_TOTAL).Inc(kSize);

    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_allocator_alloc_bytes_total", "total"), 2 * kSize);
    EXPECT_EQ(Scalar(s, "worker_allocator_free_bytes_total", "total"), kSize);
    EXPECT_EQ(Scalar(s, "worker_allocator_alloc_bytes_total", "delta"), 2 * kSize);
    EXPECT_EQ(Scalar(s, "worker_allocator_free_bytes_total", "delta"), kSize);
}

// ── [SHMUNIT] ctor/dtor counter symmetry pattern ─────────────────────────────
TEST_F(ShmLeakMetricsTest, shm_unit_ctor_dtor_symmetry)
{
    constexpr int kN = 100;
    for (int i = 0; i < kN; ++i) {
        Cnt(metrics::KvMetricId::WORKER_SHM_UNIT_CREATED_TOTAL).Inc();
    }
    for (int i = 0; i < kN; ++i) {
        Cnt(metrics::KvMetricId::WORKER_SHM_UNIT_DESTROYED_TOTAL).Inc();
    }
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_shm_unit_created_total", "total"), kN);
    EXPECT_EQ(Scalar(s, "worker_shm_unit_destroyed_total", "total"), kN);
}

// ── [REF] simulate AddShmUnit + RemoveShmUnit pattern: counters + bytes Gauge ─
// Mirrors the "first ref → bytes Inc; last ref → bytes Dec" semantic that the
// instrumented code in object_ref_info.cpp implements via shmUnit->GetRefCount().
TEST_F(ShmLeakMetricsTest, simulated_add_remove_bytes_gauge_round_trip)
{
    constexpr uint64_t kSize = 4 * 1024 * 1024;
    auto bytesG = Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_BYTES);

    // simulate AddShmUnit (first ref globally → +size)
    Cnt(metrics::KvMetricId::WORKER_SHM_REF_ADD_TOTAL).Inc();
    bytesG.Inc(static_cast<int64_t>(kSize));
    {
        auto s = DumpSummaryJson();
        EXPECT_EQ(Scalar(s, "worker_shm_ref_table_bytes", "total"), kSize);
    }

    // simulate RemoveShmUnit (last ref dropped → -size)
    Cnt(metrics::KvMetricId::WORKER_SHM_REF_REMOVE_TOTAL).Inc();
    bytesG.Dec(static_cast<int64_t>(kSize));
    {
        auto s = DumpSummaryJson();
        EXPECT_EQ(Scalar(s, "worker_shm_ref_table_bytes", "total"), 0);
        EXPECT_EQ(Scalar(s, "worker_shm_ref_add_total", "total"), 1);
        EXPECT_EQ(Scalar(s, "worker_shm_ref_remove_total", "total"), 1);
    }
}

// ── [REF] partial remove leaves residual bytes (leak-shape signal) ───────────
TEST_F(ShmLeakMetricsTest, simulated_partial_remove_leaves_residual)
{
    auto bytesG = Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_BYTES);
    // Add 3 shms of different sizes
    bytesG.Inc(1000);
    bytesG.Inc(2000);
    bytesG.Inc(3000);
    Cnt(metrics::KvMetricId::WORKER_SHM_REF_ADD_TOTAL).Inc(3);
    // Remove only the middle one
    bytesG.Dec(2000);
    Cnt(metrics::KvMetricId::WORKER_SHM_REF_REMOVE_TOTAL).Inc();

    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_shm_ref_table_bytes", "total"), 4000);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_add_total", "total"), 3);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_remove_total", "total"), 1);
}

// ── [LEAK] the OOM-direct signal: add without remove keeps gauge positive ────
TEST_F(ShmLeakMetricsTest, leak_shape_bytes_gauge_stays_positive)
{
    constexpr int kN = 10;
    constexpr uint64_t kSize = 8 * 1024 * 1024;  // mimic 8 MB user-pin workload
    auto bytesG = Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_BYTES);
    for (int i = 0; i < kN; ++i) {
        bytesG.Inc(static_cast<int64_t>(kSize));
        Cnt(metrics::KvMetricId::WORKER_SHM_REF_ADD_TOTAL).Inc();
    }
    Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_SIZE).Set(kN);

    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_shm_ref_table_bytes", "total"), kN * kSize);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_table_size", "total"), kN);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_add_total", "total"), kN);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_remove_total", "total"), 0);
}

// ── [CONC] parallel add+remove: bytes Gauge converges back to 0 ──────────────
// Validates the atomicity of Counter::Inc / Gauge::Inc/Dec under contention,
// which is the exact pattern used by SharedMemoryRefTable::Add/RemoveShmUnit.
TEST_F(ShmLeakMetricsTest, parallel_add_remove_bytes_gauge_consistent)
{
    constexpr int kThreads = 4;
    constexpr int kPerThread = 200;     // small enough to stay <50ms
    constexpr uint64_t kSize = 4096;
    auto bytesG = Gge(metrics::KvMetricId::WORKER_SHM_REF_TABLE_BYTES);

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&]() {
            for (int i = 0; i < kPerThread; ++i) {
                bytesG.Inc(static_cast<int64_t>(kSize));
                Cnt(metrics::KvMetricId::WORKER_SHM_REF_ADD_TOTAL).Inc();
                bytesG.Dec(static_cast<int64_t>(kSize));
                Cnt(metrics::KvMetricId::WORKER_SHM_REF_REMOVE_TOTAL).Inc();
            }
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    auto s = DumpSummaryJson();
    EXPECT_EQ(Scalar(s, "worker_shm_ref_table_bytes", "total"), 0);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_add_total", "total"), kThreads * kPerThread);
    EXPECT_EQ(Scalar(s, "worker_shm_ref_remove_total", "total"), kThreads * kPerThread);
}

}  // namespace
}  // namespace ut
}  // namespace datasystem
