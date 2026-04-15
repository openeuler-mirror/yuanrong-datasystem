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
