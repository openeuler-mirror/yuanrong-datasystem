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

#ifndef DATASYSTEM_COMMON_METRICS_METRICS_H
#define DATASYSTEM_COMMON_METRICS_METRICS_H

#include <array>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "datasystem/utils/status.h"

namespace datasystem::metrics {
struct MetricSlot;

enum class MetricType : uint8_t { COUNTER, GAUGE, HISTOGRAM };

struct MetricDesc {
    uint16_t id;
    const char *name;
    MetricType type;
    const char *unit;
};

Status Init(const MetricDesc *descs, size_t count);
void Tick();
void PrintSummary();

class Counter {
public:
    ~Counter() = default;
    void Inc(uint64_t delta = 1) const;

private:
    friend Counter GetCounter(uint16_t id);
    explicit Counter(MetricSlot *slot) : slot_(slot) {}
    MetricSlot *slot_;
};
class Gauge {
public:
    ~Gauge() = default;
    void Set(int64_t value) const;
    void Inc(int64_t delta = 1) const;
    void Dec(int64_t delta = 1) const;

private:
    friend Gauge GetGauge(uint16_t id);
    explicit Gauge(MetricSlot *slot) : slot_(slot) {}
    MetricSlot *slot_;
};
class Histogram {
public:
    ~Histogram() = default;
    void Observe(uint64_t value) const;

private:
    friend Histogram GetHistogram(uint16_t id);
    explicit Histogram(MetricSlot *slot) : slot_(slot) {}
    MetricSlot *slot_;
};

// P99 histogram: 20 fixed buckets (1us ~ 60s, optimized for 0-5ms). Summary JSON includes p50, p90, p99.
constexpr std::array<uint64_t, 20> HIST_BUCKET_UPPER = {
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000,
    2000, 3000, 4000, 5000,
    10000, 20000, 50000, 100000, 1000000, 60000000
};
constexpr size_t HIST_BUCKET_NUM = HIST_BUCKET_UPPER.size();
using HistBuckets = std::array<uint64_t, HIST_BUCKET_NUM>;

// upper_bound: first bucket > value -> P99 returns bucket upper bound
inline size_t BucketIndex(uint64_t value)
{
    auto it = std::upper_bound(HIST_BUCKET_UPPER.begin(), HIST_BUCKET_UPPER.end(), value);
    size_t idx = static_cast<size_t>(it - HIST_BUCKET_UPPER.begin());
    if (idx >= HIST_BUCKET_NUM) {
        idx = HIST_BUCKET_NUM - 1;
    }
    return idx;
}

uint64_t PercentileFromBuckets(const HistBuckets &buckets, uint64_t count, uint32_t percentile,
    uint64_t overflowMax = 0);

Counter GetCounter(uint16_t id);
Gauge GetGauge(uint16_t id);
Histogram GetHistogram(uint16_t id);

class ScopedTimer {
public:
    explicit ScopedTimer(uint16_t id);
    ~ScopedTimer();

private:
    uint16_t id_;
    std::chrono::steady_clock::time_point start_;
};

std::string DumpSummaryForTest(int intervalMs = 10000);
void ResetForTest();
}  // namespace datasystem::metrics

#endif  // DATASYSTEM_COMMON_METRICS_METRICS_H
