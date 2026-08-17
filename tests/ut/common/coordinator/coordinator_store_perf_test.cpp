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
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <bthread/bthread.h>

#include "ut/common.h"
#include "datasystem/common/flags/common_flags.h"  // FLAGS_use_brpc
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
constexpr size_t WATCH_DISPATCH_CHANNELS = 64;
constexpr size_t WATCH_DISPATCH_LARGE_CHANNELS = 1024;
constexpr size_t WATCH_DISPATCH_CONCURRENCY_SLOTS = WATCH_DISPATCH_LARGE_CHANNELS + 1;
constexpr size_t WATCH_DISPATCH_DEFAULT_THREADS = 4;
constexpr size_t WATCH_DISPATCH_LARGE_THREADS = 32;
constexpr size_t WATCH_DISPATCH_MAX_HEALTHY_INFLIGHT = 4096;
constexpr uint64_t WATCH_DISPATCH_HEALTHY_RPC_DELAY_US = 100;
constexpr uint64_t WATCH_DISPATCH_TIMEOUT_US = 1000000;
constexpr auto WATCH_DISPATCH_PERF_DURATION = std::chrono::seconds(5);
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

struct WatchDispatchPerfStats {
    std::string scenario;
    size_t submittedEvents = 0;
    size_t notifiedEvents = 0;
    size_t pendingEvents = 0;
    size_t healthyNotifyCalls = 0;
    size_t slowNotifyAttempts = 0;
    size_t maxConcurrency = 0;
    double avgBatchSize = 0.0;
    double elapsedMs = 0.0;
    double submittedEventsPerSecond = 0.0;
    double notifiedEventsPerSecond = 0.0;
    bool slowNotifyInflight = false;
};

class DispatchPerfWatchDispatcher : public WatchDispatcher {
public:
    DispatchPerfWatchDispatcher(WatchRegistry *watchRegistry, std::optional<int64_t> slowWatchId,
                                uint64_t healthyRpcDelayUs, size_t dispatchThreadCount)
        : WatchDispatcher(watchRegistry, BTHREAD_TAG_DEFAULT, dispatchThreadCount),
          slowWatchId_(slowWatchId),
          healthyRpcDelayUs_(healthyRpcDelayUs)
    {
    }

    Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                    std::vector<std::shared_ptr<WatchEvent>> &events) override
    {
        (void)watcherAddr;
        const size_t concurrency = currentConcurrency_.fetch_add(1, std::memory_order_acq_rel) + 1;
        size_t observed = maxConcurrency_.load(std::memory_order_relaxed);
        while (concurrency > observed
               && !maxConcurrency_.compare_exchange_weak(observed, concurrency, std::memory_order_relaxed)) {
        }
        if (watchId >= 0 && static_cast<size_t>(watchId) < activeWatchIds_.size()
            && activeWatchIds_[static_cast<size_t>(watchId)].exchange(true, std::memory_order_acq_rel)) {
            sameChannelConcurrent_.store(true, std::memory_order_relaxed);
        }

        if (slowWatchId_.has_value() && watchId == *slowWatchId_) {
            slowNotifyAttempts_.fetch_add(1, std::memory_order_relaxed);
            slowNotifyInflight_.store(true, std::memory_order_release);
            slowStartedCv_.notify_all();
            (void)bthread_usleep(WATCH_DISPATCH_TIMEOUT_US);
            slowNotifyInflight_.store(false, std::memory_order_release);
            FinishNotify(watchId);
            return Status(K_RPC_DEADLINE_EXCEEDED, "injected one-second timeout");
        }

        if (healthyRpcDelayUs_ > 0) {
            (void)bthread_usleep(healthyRpcDelayUs_);
        }
        healthyNotifiedEvents_.fetch_add(events.size(), std::memory_order_relaxed);
        healthyNotifyCalls_.fetch_add(1, std::memory_order_relaxed);
        FinishNotify(watchId);
        return Status::OK();
    }

    bool WaitSlowNotifyStarted(uint64_t timeoutMs = WATCH_NOTIFY_TIMEOUT_MS)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return slowStartedCv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                                       [this] { return slowNotifyAttempts_.load(std::memory_order_relaxed) > 0; });
    }

    size_t HealthyNotifiedEvents()
    {
        return healthyNotifiedEvents_.load(std::memory_order_relaxed);
    }

    WatchDispatchPerfStats BuildStats(const std::string &scenario, size_t submittedEvents,
                                      std::chrono::steady_clock::duration elapsed)
    {
        const size_t healthyNotifiedEvents = healthyNotifiedEvents_.load(std::memory_order_relaxed);
        const size_t healthyNotifyCalls = healthyNotifyCalls_.load(std::memory_order_relaxed);
        WatchDispatchPerfStats stats;
        stats.scenario = scenario;
        stats.submittedEvents = submittedEvents;
        stats.notifiedEvents = healthyNotifiedEvents;
        stats.pendingEvents = submittedEvents - healthyNotifiedEvents;
        stats.healthyNotifyCalls = healthyNotifyCalls;
        stats.slowNotifyAttempts = slowNotifyAttempts_.load(std::memory_order_relaxed);
        stats.maxConcurrency = maxConcurrency_.load(std::memory_order_relaxed);
        stats.avgBatchSize = healthyNotifyCalls == 0
                                 ? 0.0
                                 : static_cast<double>(healthyNotifiedEvents) / static_cast<double>(healthyNotifyCalls);
        stats.elapsedMs = std::chrono::duration<double, std::milli>(elapsed).count();
        stats.slowNotifyInflight = slowNotifyInflight_.load(std::memory_order_acquire);
        if (elapsed.count() > 0) {
            const double elapsedSeconds = std::chrono::duration<double>(elapsed).count();
            stats.submittedEventsPerSecond = static_cast<double>(submittedEvents) / elapsedSeconds;
            stats.notifiedEventsPerSecond = static_cast<double>(healthyNotifiedEvents) / elapsedSeconds;
        }
        return stats;
    }

    bool SameChannelConcurrent()
    {
        return sameChannelConcurrent_.load(std::memory_order_relaxed);
    }

private:
    void FinishNotify(int64_t watchId)
    {
        if (watchId >= 0 && static_cast<size_t>(watchId) < activeWatchIds_.size()) {
            activeWatchIds_[static_cast<size_t>(watchId)].store(false, std::memory_order_release);
        }
        currentConcurrency_.fetch_sub(1, std::memory_order_acq_rel);
    }

    const std::optional<int64_t> slowWatchId_;
    const uint64_t healthyRpcDelayUs_;
    std::atomic<size_t> currentConcurrency_{ 0 };
    std::atomic<size_t> maxConcurrency_{ 0 };
    std::atomic<size_t> slowNotifyAttempts_{ 0 };
    std::atomic<bool> slowNotifyInflight_{ false };
    std::mutex mutex_;
    std::condition_variable slowStartedCv_;
    std::array<std::atomic<bool>, WATCH_DISPATCH_CONCURRENCY_SLOTS> activeWatchIds_{};
    std::atomic<size_t> healthyNotifiedEvents_{ 0 };
    std::atomic<size_t> healthyNotifyCalls_{ 0 };
    std::atomic<bool> sameChannelConcurrent_{ false };
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
        EXPECT_TRUE(store_->Start().IsOk());
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

void PrintWatchDispatchStats(const WatchDispatchPerfStats &stats)
{
    std::cout << std::fixed << std::setprecision(2) << "[WatchDispatcherPerf] scenario=" << stats.scenario
              << " submitted_events=" << stats.submittedEvents << " notified_events=" << stats.notifiedEvents
              << " pending_events=" << stats.pendingEvents << " healthy_notify_calls=" << stats.healthyNotifyCalls
              << " slow_notify_attempts=" << stats.slowNotifyAttempts
              << " avg_batch_size=" << stats.avgBatchSize << " max_concurrency=" << stats.maxConcurrency
              << " elapsed_ms=" << stats.elapsedMs
              << " submitted_events_per_second=" << stats.submittedEventsPerSecond
              << " notified_events_per_second=" << stats.notifiedEventsPerSecond
              << " slow_notify_inflight=" << stats.slowNotifyInflight << "\n";
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
        DS_ASSERT_OK(
            store.WatchRange("/perf/watch/", "/perf/watch0", "addr" + std::to_string(i), "", watchId, initial));
        ASSERT_TRUE(initial.empty());
    }
}

void RunWarmUp()
{
    PerfStoreFixture fixture;
    auto stats = MeasureScenario("WarmUp", WARM_UP_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/warm-up/" + std::to_string(i), "v", 0, COORDINATOR_NO_VERSION_CHECK, version,
                                   revision);
    });
    PrintStats(stats);
}

PerfStats MeasurePutNoWatchNoTtl()
{
    PerfStoreFixture fixture;
    return MeasureScenario("PutNoWatchNoTtl", PERF_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/no-watch/" + std::to_string(i), "v", 0, COORDINATOR_NO_VERSION_CHECK, version,
                                   revision);
    });
}

PerfStats MeasurePutWithWatchers(int watcherCount)
{
    PerfStoreFixture fixture;
    AddWatchers(fixture.Store(), watcherCount);
    auto stats = MeasureScenario("PutWith" + std::to_string(watcherCount) + "Watchers", PERF_OPS, [&fixture](size_t i) {
        int64_t version = 0;
        int64_t revision = 0;
        return fixture.Store().Put("/perf/watch/" + std::to_string(i), "v", 0, COORDINATOR_NO_VERSION_CHECK, version,
                                   revision);
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
        return fixture.Store().Put("/perf/ttl/" + std::to_string(i), "v", PERF_TTL_MS, COORDINATOR_NO_VERSION_CHECK,
                                   version, revision);
    });
}

PerfStats MeasureRangeSingleKey()
{
    PerfStoreFixture fixture;
    int64_t version = 0;
    int64_t revision = 0;
    Status status = fixture.Store().Put("/perf/range/key", "v", 0, COORDINATOR_NO_VERSION_CHECK, version, revision);
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
        Status status = fixture.Store().Put("/perf/delete/" + std::to_string(i), "v", 0, COORDINATOR_NO_VERSION_CHECK,
                                            version, revision);
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

WatchDispatchPerfStats MeasureWatchDispatch(const std::string &scenario, bool injectSlowTimeout,
                                            uint64_t healthyRpcDelayUs, size_t channelCount,
                                            size_t dispatchThreadCount)
{
    auto registry = std::make_shared<WatchRegistry>();
    std::vector<std::string> keys;
    keys.reserve(channelCount);
    std::vector<int64_t> watchIds;
    watchIds.reserve(channelCount);
    for (size_t i = 0; i < channelCount; ++i) {
        auto key = "/perf/dispatch/" + std::to_string(i);
        const auto workerAddr = "worker-" + std::to_string(i);
        const int64_t watchId = registry->Register(key, "", workerAddr);
        watchIds.push_back(watchId);
        keys.push_back(std::move(key));
    }
    const std::optional<int64_t> slowWatchId = injectSlowTimeout ? std::make_optional(watchIds.front()) : std::nullopt;
    auto dispatcher =
        std::make_shared<DispatchPerfWatchDispatcher>(registry.get(), slowWatchId, healthyRpcDelayUs,
                                                      dispatchThreadCount);
    dispatcher->Start();
    for (size_t i = 0; i < channelCount; ++i) {
        const auto workerAddr = "worker-" + std::to_string(i);
        const int64_t watchId = watchIds[i];
        dispatcher->AddChannel(watchId, workerAddr);
        dispatcher->SetSnapshotRevision(watchId, 1);
    }

    int64_t revision = 2;
    if (injectSlowTimeout) {
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ keys.front(), "slow-value", 1, revision };
        event->revision = revision++;
        dispatcher->Enqueue(std::move(event));
        EXPECT_TRUE(dispatcher->WaitSlowNotifyStarted());
    }

    const size_t firstHealthyKey = injectSlowTimeout ? 1 : 0;
    const size_t healthyKeyCount = keys.size() - firstHealthyKey;
    size_t submittedEvents = 0;
    const auto start = std::chrono::steady_clock::now();
    const auto deadline = start + WATCH_DISPATCH_PERF_DURATION;
    while (std::chrono::steady_clock::now() < deadline) {
        const size_t notifiedEvents = dispatcher->HealthyNotifiedEvents();
        if (submittedEvents - notifiedEvents >= WATCH_DISPATCH_MAX_HEALTHY_INFLIGHT) {
            std::this_thread::yield();
            continue;
        }
        const size_t keyIndex = firstHealthyKey + (submittedEvents % healthyKeyCount);
        auto event = std::make_shared<WatchEvent>();
        event->type = WatchEvent::Type::PUT;
        event->entry = KeyValueEntry{ keys[keyIndex], "value", 1, revision };
        event->revision = revision++;
        dispatcher->Enqueue(std::move(event));
        ++submittedEvents;
    }
    const auto end = std::chrono::steady_clock::now();

    auto stats = dispatcher->BuildStats(scenario, submittedEvents, end - start);
    EXPECT_FALSE(dispatcher->SameChannelConcurrent());
    EXPECT_GT(stats.healthyNotifyCalls, 0UL);
    EXPECT_LE(stats.pendingEvents, WATCH_DISPATCH_MAX_HEALTHY_INFLIGHT);
    dispatcher->Stop();
    return stats;
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

TEST_F(CoordinatorStorePerfTest, ReportWatchDispatcherPerformance)
{
    for (const auto healthyRpcDelayUs : { 0UL, WATCH_DISPATCH_HEALTHY_RPC_DELAY_US }) {
        const auto delayName = std::to_string(healthyRpcDelayUs) + "us";
        auto baseline = MeasureWatchDispatch("HealthyRpc" + delayName + "Notify5s", false, healthyRpcDelayUs,
                                             WATCH_DISPATCH_CHANNELS, WATCH_DISPATCH_DEFAULT_THREADS);
        PrintWatchDispatchStats(baseline);
        EXPECT_GT(baseline.notifiedEvents, 0UL);
        EXPECT_GT(baseline.notifiedEventsPerSecond, 0.0);

        auto timeout = MeasureWatchDispatch("HealthyRpc" + delayName + "OneChannelTimeout1s5s", true,
                                            healthyRpcDelayUs, WATCH_DISPATCH_CHANNELS,
                                            WATCH_DISPATCH_DEFAULT_THREADS);
        PrintWatchDispatchStats(timeout);
        EXPECT_GT(timeout.notifiedEvents, 0UL);
        EXPECT_GT(timeout.notifiedEventsPerSecond, 0.0);
        EXPECT_GE(timeout.slowNotifyAttempts, 2UL);
        EXPECT_GT(timeout.maxConcurrency, 1UL);
    }
}

TEST_F(CoordinatorStorePerfTest, ReportLargeWatchDispatcherPerformance)
{
    for (const auto healthyRpcDelayUs : { 0UL, WATCH_DISPATCH_HEALTHY_RPC_DELAY_US }) {
        const auto delayName = std::to_string(healthyRpcDelayUs) + "us";
        auto baseline = MeasureWatchDispatch("LargeHealthyRpc" + delayName + "Notify5s", false,
                                             healthyRpcDelayUs, WATCH_DISPATCH_LARGE_CHANNELS,
                                             WATCH_DISPATCH_LARGE_THREADS);
        PrintWatchDispatchStats(baseline);
        EXPECT_GT(baseline.notifiedEvents, 0UL);
        EXPECT_GT(baseline.notifiedEventsPerSecond, 0.0);

        auto timeout = MeasureWatchDispatch("LargeHealthyRpc" + delayName + "OneChannelTimeout1s5s", true,
                                            healthyRpcDelayUs, WATCH_DISPATCH_LARGE_CHANNELS,
                                            WATCH_DISPATCH_LARGE_THREADS);
        PrintWatchDispatchStats(timeout);
        EXPECT_GT(timeout.notifiedEvents, 0UL);
        EXPECT_GT(timeout.notifiedEventsPerSecond, 0.0);
        EXPECT_GE(timeout.slowNotifyAttempts, 2UL);
        EXPECT_GT(timeout.maxConcurrency, 1UL);
    }
}
}  // namespace ut
}  // namespace datasystem
