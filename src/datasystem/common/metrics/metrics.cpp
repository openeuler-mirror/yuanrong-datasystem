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

#include "datasystem/common/metrics/metrics.h"

#include <array>
#include <atomic>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
DS_DECLARE_bool(log_monitor);
DS_DECLARE_int32(log_monitor_interval_ms);
namespace datasystem::metrics {
namespace {
constexpr size_t MAX_METRIC_NUM = 1024;
constexpr size_t MAX_METRICS_LOG_BYTES = 24000;
constexpr const char *VERSION = "v0";

std::string BuildSuffix(const char *unit)
{
    if (unit == nullptr || std::strcmp(unit, "count") == 0) {
        return "";
    }
    if (std::strcmp(unit, "bytes") == 0) {
        return "B";
    }
    return unit;
}
}  // namespace

struct alignas(64) MetricSlot {
    uint16_t id = 0;
    MetricType type = MetricType::COUNTER;
    std::string name;
    std::string suffix;
    std::atomic<uint64_t> u64Value{ 0 };
    std::atomic<int64_t> i64Value{ 0 };
    std::atomic<uint64_t> sum{ 0 };
    std::atomic<uint64_t> max{ 0 };
    std::atomic<uint64_t> periodMax{ 0 };
    std::mutex histMutex;
    bool used = false;
};

namespace {
struct LastSnapshot {
    uint64_t u64Value = 0;
    int64_t i64Value = 0;
    uint64_t sum = 0;
};

std::array<MetricSlot, MAX_METRIC_NUM> g_slots;
std::array<LastSnapshot, MAX_METRIC_NUM> g_last;
std::vector<uint16_t> g_ids;
std::mutex g_stateMutex;
std::mutex g_tickMutex;
std::chrono::steady_clock::time_point g_lastLogTime = std::chrono::steady_clock::now();
std::atomic<bool> g_inited{ false };
uint64_t g_cycle = 0;
void ClearAll()
{
    g_inited.store(false, std::memory_order_release);
    for (auto &slot : g_slots) {
        slot.id = 0;
        slot.type = MetricType::COUNTER;
        slot.name.clear();
        slot.suffix.clear();
        slot.u64Value.store(0, std::memory_order_relaxed);
        slot.i64Value.store(0, std::memory_order_relaxed);
        slot.sum.store(0, std::memory_order_relaxed);
        slot.max.store(0, std::memory_order_relaxed);
        slot.periodMax.store(0, std::memory_order_relaxed);
        slot.used = false;
    }
    for (auto &last : g_last) {
        last = {};
    }
    g_ids.clear();
    g_cycle = 0;
}

MetricSlot *FindSlot(uint16_t id, MetricType type)
{
    if (!g_inited.load(std::memory_order_acquire) || id >= MAX_METRIC_NUM || !g_slots[id].used
        || g_slots[id].type != type) {
        return nullptr;
    }
    return &g_slots[id];
}

void UpdateMax(std::atomic<uint64_t> &target, uint64_t value)
{
    for (uint64_t cur = target.load(std::memory_order_relaxed); cur < value;
         cur = target.load(std::memory_order_relaxed)) {
        if (target.compare_exchange_weak(cur, value, std::memory_order_relaxed)) {
            break;
        }
    }
}

std::string RenderJsonSummary(
    uint64_t cycle, int intervalMs, size_t partIndex, size_t partCount, const std::string &body)
{
    std::ostringstream os;
    os << "{\"event\":\"metrics_summary\",\"version\":\"" << VERSION << "\",\"cycle\":" << cycle
       << ",\"interval_ms\":" << intervalMs << ",\"part_index\":" << partIndex
       << ",\"part_count\":" << partCount << ",\"metrics\":[" << body << "]}";
    return os.str();
}

std::vector<std::string> BuildSummary(int intervalMs)
{
    std::lock_guard<std::mutex> lock(g_stateMutex);
    if (!g_inited.load(std::memory_order_acquire)) {
        return {};
    }
    std::vector<std::string> metrics;
    for (auto id : g_ids) {
        auto &slot = g_slots[id];
        auto &last = g_last[id];
        std::ostringstream item;
        bool needAdd = false;
        if (slot.type == MetricType::COUNTER) {
            auto value = slot.u64Value.load(std::memory_order_relaxed);
            if (value > 0) {
                needAdd = true;
                item << "{\"name\":\"" << slot.name << "\",\"total\":"
                     << value << ",\"delta\":" << static_cast<int64_t>(value - last.u64Value) << '}';
            }
            last.u64Value = value;
        } else if (slot.type == MetricType::GAUGE) {
            auto value = slot.i64Value.load(std::memory_order_relaxed);
            if (value != 0 || last.i64Value != 0) {
                needAdd = true;
                item << "{\"name\":\"" << slot.name << "\",\"total\":"
                     << value << ",\"delta\":" << (value - last.i64Value) << '}';
            }
            last.i64Value = value;
        } else {
            std::lock_guard<std::mutex> histLock(slot.histMutex);
            auto count = slot.u64Value.load(std::memory_order_relaxed);
            auto sum = slot.sum.load(std::memory_order_relaxed);
            auto max = slot.max.load(std::memory_order_relaxed);
            auto dCount = count - last.u64Value;
            auto dSum = sum - last.sum;
            auto dMax = slot.periodMax.exchange(0, std::memory_order_relaxed);
            if (count > 0) {
                needAdd = true;
                item << "{\"name\":\"" << slot.name << "\",\"total\":"
                     << "{\"count\":" << count << ",\"avg_us\":" << (count == 0 ? 0 : sum / count)
                     << ",\"max_us\":" << max << "},\"delta\":{\"count\":" << dCount
                     << ",\"avg_us\":" << (dCount == 0 ? 0 : dSum / dCount) << ",\"max_us\":" << dMax << "}}";
            }
            last.u64Value = count;
            last.sum = sum;
        }
        if (needAdd) {
            metrics.emplace_back(item.str());
        }
    }
    const auto cycle = ++g_cycle;
    std::vector<std::string> bodies(1);
    for (const auto &metric : metrics) {
        auto merged = bodies.back().empty() ? metric : bodies.back() + ',' + metric;
        if (!bodies.back().empty() &&
            RenderJsonSummary(cycle, intervalMs, 1, 1, merged).size() > MAX_METRICS_LOG_BYTES) {
            bodies.emplace_back(metric);
        } else {
            bodies.back() = std::move(merged);
        }
    }
    std::vector<std::string> summaries;
    for (size_t i = 0; i < bodies.size(); ++i) {
        const auto partIndex = i + 1;
        summaries.emplace_back(RenderJsonSummary(cycle, intervalMs, partIndex, bodies.size(), bodies[i]));
    }
    return summaries;
}

void LogSummary(int intervalMs)
{
    auto summaries = BuildSummary(intervalMs);
    const auto traceId = (Logging::PodName() + "-metrics").substr(0, Trace::TRACEID_MAX_SIZE);
    for (const auto &summary : summaries) {
        auto guard = Trace::Instance().SetTraceNewID(traceId);
        LOG(INFO) << summary;
    }
}
}  // namespace

Status Init(const MetricDesc *descs, size_t count)
{
    if (descs == nullptr && count != 0) {
        return Status(K_INVALID, "Invalid metric descriptor.");
    }
    {
        std::lock_guard<std::mutex> lock(g_stateMutex);
        ClearAll();
        for (size_t i = 0; i < count; ++i) {
            auto id = descs[i].id;
            if (id >= MAX_METRIC_NUM || descs[i].name == nullptr || g_slots[id].used) {
                return Status(K_INVALID, "Invalid metric descriptor.");
            }
            g_slots[id].id = id;
            g_slots[id].type = descs[i].type;
            g_slots[id].name = descs[i].name;
            g_slots[id].suffix = BuildSuffix(descs[i].unit);
            g_slots[id].used = true;
            g_ids.emplace_back(id);
        }
        g_inited.store(true, std::memory_order_release);
    }
    {
        std::lock_guard<std::mutex> lock(g_tickMutex);
        g_lastLogTime = std::chrono::steady_clock::now();
    }
    return Status::OK();
}

void Tick()
{
    if (!FLAGS_log_monitor) {
        return;
    }
    int interval = FLAGS_log_monitor_interval_ms;
    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(g_tickMutex);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - g_lastLogTime).count();
        if (elapsed < interval) {
            return;
        }
        g_lastLogTime = now;
    }
    LogSummary(interval);
}

void PrintSummary()
{
    if (!FLAGS_log_monitor) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(g_tickMutex);
        g_lastLogTime = std::chrono::steady_clock::now();
    }
    LogSummary(FLAGS_log_monitor_interval_ms);
}

void Counter::Inc(uint64_t delta) const
{
    if (slot_ != nullptr) {
        slot_->u64Value.fetch_add(delta, std::memory_order_relaxed);
    }
}

void Gauge::Set(int64_t value) const
{
    if (slot_ != nullptr) {
        slot_->i64Value.store(value, std::memory_order_relaxed);
    }
}

void Gauge::Inc(int64_t delta) const
{
    if (slot_ != nullptr) {
        slot_->i64Value.fetch_add(delta, std::memory_order_relaxed);
    }
}

void Gauge::Dec(int64_t delta) const
{
    Inc(-delta);
}

void Histogram::Observe(uint64_t value) const
{
    if (slot_ != nullptr) {
        std::lock_guard<std::mutex> lock(slot_->histMutex);
        slot_->u64Value.fetch_add(1, std::memory_order_relaxed);
        slot_->sum.fetch_add(value, std::memory_order_relaxed);
        UpdateMax(slot_->max, value);
        UpdateMax(slot_->periodMax, value);
    }
}

Counter GetCounter(uint16_t id)
{
    return Counter(FindSlot(id, MetricType::COUNTER));
}

Gauge GetGauge(uint16_t id)
{
    return Gauge(FindSlot(id, MetricType::GAUGE));
}

Histogram GetHistogram(uint16_t id)
{
    return Histogram(FindSlot(id, MetricType::HISTOGRAM));
}

ScopedTimer::ScopedTimer(uint16_t id) : id_(id), start_(std::chrono::steady_clock::now())
{
}

ScopedTimer::~ScopedTimer()
{
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start_);
    GetHistogram(id_).Observe(static_cast<uint64_t>(elapsed.count()));
}

std::string DumpSummaryForTest(int intervalMs)
{
    auto summaries = BuildSummary(intervalMs);
    return summaries.empty() ? "" : summaries.front();
}

void ResetForTest()
{
    {
        std::lock_guard<std::mutex> lock(g_stateMutex);
        ClearAll();
    }
    {
        std::lock_guard<std::mutex> lock(g_tickMutex);
        g_lastLogTime = std::chrono::steady_clock::now();
    }
}
}  // namespace datasystem::metrics
