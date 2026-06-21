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
 * Description: Performance smoke test for coordinator store.
 */

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <vector>

#include "ut/common.h"
#include "datasystem/common/coordinator/coordinator_store.h"
#include "datasystem/common/coordinator/memory_kv_store.h"
#include "datasystem/common/coordinator/steady_clock.h"
#include "datasystem/common/coordinator/ttl_manager.h"
#include "datasystem/common/coordinator/watch_dispatcher.h"
#include "datasystem/common/coordinator/watch_registry.h"

namespace datasystem {
namespace ut {
namespace {
constexpr int PERF_OPS = 8000;
constexpr int WARM_UP_OPS = 1000;
constexpr int WATCHERS_10 = 10;
constexpr int WATCHERS_100 = 100;
constexpr int64_t PERF_TTL_MS = 60000;
constexpr uint64_t WATCH_NOTIFY_TIMEOUT_MS = 60000;
constexpr int PERCENT_SCALE = 100;
constexpr int P90 = 90;
constexpr int P99 = 99;
constexpr size_t SCENARIO_COUNT = 6;
constexpr double NANOSECONDS_PER_MICROSECOND = 1000.0;
constexpr double NANOSECONDS_PER_SECOND = 1000000000.0;

struct PerfStats {
    std::string scenario;
    size_t ops = 0;
    size_t success = 0;
    double avgUs = 0.0;
    double p90Us = 0.0;
    double p99Us = 0.0;
    double pmaxUs = 0.0;
    double tps = 0.0;
};

class CountingWatchDispatcher : public WatchDispatcher {
public:
    explicit CountingWatchDispatcher(WatchRegistry *watchRegistry) : WatchDispatcher(watchRegistry)
    {
    }
    ~CountingWatchDispatcher() override = default;

    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        (void)watchId;
        (void)watcherAddr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            notifiedEvents_ += events.size();
        }
        cv_.notify_all();
        return Status::OK();
    }

    bool WaitNotifiedAtLeast(size_t expected, uint64_t timeoutMs = 10000)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                            [this, expected] { return notifiedEvents_ >= expected; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t notifiedEvents_ = 0;
};

class PerfStoreFixture {
public:
    PerfStoreFixture()
    {
        memStore_ = std::make_shared<MemoryKvStore>();
        registry_ = std::make_shared<WatchRegistry>();
        dispatcher_ = std::make_shared<CountingWatchDispatcher>(registry_.get());
        clock_ = std::make_shared<SteadyClockMock>();
        ttlManager_ = std::make_shared<TtlManager>(clock_);
        store_ = std::make_unique<CoordinatorStore>(memStore_, registry_, dispatcher_, ttlManager_);
        dispatcher_->Start();
        ttlManager_->Start();
    }

    ~PerfStoreFixture()
    {
        ttlManager_->Stop();
        dispatcher_->Stop();
    }

    CoordinatorStore &Store()
    {
        return *store_;
    }

    WatchRegistry &Registry()
    {
        return *registry_;
    }

    CountingWatchDispatcher &Dispatcher()
    {
        return *dispatcher_;
    }

private:
    std::shared_ptr<MemoryKvStore> memStore_;
    std::shared_ptr<WatchRegistry> registry_;
    std::shared_ptr<CountingWatchDispatcher> dispatcher_;
    std::shared_ptr<SteadyClockMock> clock_;
    std::shared_ptr<TtlManager> ttlManager_;
    std::unique_ptr<CoordinatorStore> store_;
};

double ToUs(std::chrono::nanoseconds duration)
{
    return static_cast<double>(duration.count()) / NANOSECONDS_PER_MICROSECOND;
}

size_t PercentileIndex(size_t size, int percentile)
{
    if (size == 0) {
        return 0;
    }
    size_t index = ((size * static_cast<size_t>(percentile)) + PERCENT_SCALE - 1) / PERCENT_SCALE;
    return std::min(index == 0 ? 0 : index - 1, size - 1);
}

PerfStats BuildStats(const std::string &scenario, size_t ops, size_t success,
                     std::vector<std::chrono::nanoseconds> latencies, std::chrono::nanoseconds totalDuration)
{
    PerfStats stats;
    stats.scenario = scenario;
    stats.ops = ops;
    stats.success = success;
    if (latencies.empty()) {
        return stats;
    }

    std::sort(latencies.begin(), latencies.end());
    auto totalLatency = std::accumulate(latencies.begin(), latencies.end(), std::chrono::nanoseconds(0));
    stats.avgUs = ToUs(totalLatency) / static_cast<double>(latencies.size());
    stats.p90Us = ToUs(latencies[PercentileIndex(latencies.size(), P90)]);
    stats.p99Us = ToUs(latencies[PercentileIndex(latencies.size(), P99)]);
    stats.pmaxUs = ToUs(latencies.back());

    double totalSeconds = static_cast<double>(totalDuration.count()) / NANOSECONDS_PER_SECOND;
    if (totalSeconds > 0.0) {
        stats.tps = static_cast<double>(success) / totalSeconds;
    }
    return stats;
}

void PrintStats(const PerfStats &stats)
{
    std::cout << std::fixed << std::setprecision(2) << "[CoordinatorStorePerf] scenario=" << stats.scenario
              << " ops=" << stats.ops << " success=" << stats.success << " avg_us=" << stats.avgUs
              << " p90_us=" << stats.p90Us << " p99_us=" << stats.p99Us << " pmax_us=" << stats.pmaxUs
              << " tps=" << stats.tps << "\n";
}

template <typename Func>
PerfStats MeasureScenario(const std::string &scenario, size_t ops, Func &&func)
{
    for (size_t i = 0; i < WARM_UP_OPS; ++i) {
        Status status = func(i);
        EXPECT_TRUE(status.IsOk()) << status.ToString();
    }

    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(ops);
    size_t success = 0;

    auto totalStart = std::chrono::steady_clock::now();
    for (size_t i = 0; i < ops; ++i) {
        auto opStart = std::chrono::steady_clock::now();
        Status status = func(i + WARM_UP_OPS);
        auto opEnd = std::chrono::steady_clock::now();
        if (status.IsOk()) {
            ++success;
        }
        latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(opEnd - opStart));
    }
    auto totalEnd = std::chrono::steady_clock::now();

    return BuildStats(scenario, ops, success, std::move(latencies),
                      std::chrono::duration_cast<std::chrono::nanoseconds>(totalEnd - totalStart));
}

void AddWatchers(CoordinatorStore &store, int watcherCount)
{
    for (int i = 0; i < watcherCount; ++i) {
        int64_t watchId = 0;
        std::vector<KeyValueEntry> initial;
        DS_ASSERT_OK(store.WatchRange("/perf/watch/", "/perf/watch0", "addr" + std::to_string(i), watchId, initial));
        ASSERT_TRUE(initial.empty());
    }
}

void RunWarmUp()
{
    PerfStoreFixture fixture;
    auto stats = MeasureScenario("WarmUp", WARM_UP_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/warm-up/" + std::to_string(i), "v", 0, 0, version, revision);
    });
    PrintStats(stats);
}

PerfStats MeasurePutNoWatchNoTtl()
{
    PerfStoreFixture fixture;
    return MeasureScenario("PutNoWatchNoTtl", PERF_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/no-watch/" + std::to_string(i), "v", 0, 0, version, revision);
    });
}

PerfStats MeasurePutWithWatchers(int watcherCount)
{
    PerfStoreFixture fixture;
    AddWatchers(fixture.Store(), watcherCount);
    auto stats = MeasureScenario("PutWith" + std::to_string(watcherCount) + "Watchers", PERF_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/watch/" + std::to_string(i), "v", 0, 0, version, revision);
    });
    EXPECT_TRUE(fixture.Dispatcher().WaitNotifiedAtLeast(static_cast<size_t>(watcherCount) * PERF_OPS,
                                                         WATCH_NOTIFY_TIMEOUT_MS));
    return stats;
}

PerfStats MeasurePutWithTtl()
{
    PerfStoreFixture fixture;
    return MeasureScenario("PutWithTtl", PERF_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/ttl/" + std::to_string(i), "v", PERF_TTL_MS, 0, version, revision);
    });
}

PerfStats MeasureRangeSingleKey()
{
    PerfStoreFixture fixture;
    int64_t version = 0;
    int64_t revision = 0;
    Status status = fixture.Store().Put("/perf/range/key", "v", 0, 0, version, revision);
    EXPECT_TRUE(status.IsOk()) << status.ToString();
    if (status.IsError()) {
        return PerfStats{ "RangeSingleKey", 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0 };
    }

    return MeasureScenario("RangeSingleKey", PERF_OPS, [&fixture](size_t i) {
        (void)i;
        std::vector<KeyValueEntry> kvs;
        int64_t rangeRevision = 0;
        return fixture.Store().Range("/perf/range/key", "", kvs, rangeRevision);
    });
}

PerfStats MeasureDeleteSingleKey()
{
    PerfStoreFixture fixture;
    for (int i = 0; i < WARM_UP_OPS + PERF_OPS; ++i) {
        int64_t version = 0;
        int64_t revision = 0;
        Status status = fixture.Store().Put("/perf/delete/" + std::to_string(i), "v", 0, 0, version, revision);
        EXPECT_TRUE(status.IsOk()) << status.ToString();
        if (status.IsError()) {
            return PerfStats{ "DeleteSingleKey", 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0 };
        }
    }

    return MeasureScenario("DeleteSingleKey", PERF_OPS, [&fixture](size_t i) {
        int64_t deleted = 0;
        int64_t revision = 0;
        return fixture.Store().DeleteRange("/perf/delete/" + std::to_string(i), "", deleted, revision);
    });
}
}  // namespace

class CoordinatorStorePerfTest : public CommonTest {};

TEST_F(CoordinatorStorePerfTest, ReportCoordinatorStorePerformance)
{
    RunWarmUp();

    std::vector<PerfStats> results;
    results.reserve(SCENARIO_COUNT);
    results.push_back(MeasurePutNoWatchNoTtl());
    results.push_back(MeasurePutWithWatchers(WATCHERS_10));
    results.push_back(MeasurePutWithWatchers(WATCHERS_100));
    results.push_back(MeasurePutWithTtl());
    results.push_back(MeasureRangeSingleKey());
    results.push_back(MeasureDeleteSingleKey());

    for (const auto &stats : results) {
        PrintStats(stats);
        ASSERT_EQ(stats.ops, stats.success) << stats.scenario;
    }
}
}  // namespace ut
}  // namespace datasystem
