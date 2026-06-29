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
 * BrpcAsyncContext Mutex Contention Microbenchmark
 * =================================================
 *
 * Measures whether the single std::mutex in BrpcAsyncContext is a bottleneck
 * for high-QPS async RPC workloads. Replicates the exact AllocateTag/TakeCall/
 * ForgetCall pattern with instrumented lock timing.
 *
 * Variant A (baseline):  single std::mutex (current BrpcAsyncContext)
 * Variant B (sharded16): 16-bucket sharded mutex (tagId % 16)
 * Variant C (sharded64): 64-bucket sharded mutex (tagId % 64)
 *
 * Test matrix:
 *   threads: 1, 8, 32, 64, 128
 *   op types: allocate, take, forget, mixed(1:1:1)
 *   duration: 30s per combination
 *
 * Metrics:
 *   - throughput (ops/sec)
 *   - avg/p50/p99/max latency per operation
 *   - mutex wait time and wait% = sum(wait) / sum(total)
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// ============================================================================
// Timing helpers
// ============================================================================

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

/**
 * RAII lock that records wait time (before acquiring) and hold time.
 * Accumulates into thread-local references.
 */
class TimedLockGuard {
public:
    TimedLockGuard(std::mutex &mtx, Nanos &waitAcc, Nanos &holdAcc)
        : mtx_(mtx), holdAcc_(holdAcc), waitAcc_(waitAcc)
    {
        auto waitStart = Clock::now();
        mtx_.lock();
        auto holdStart = Clock::now();
        waitAcc_ += holdStart - waitStart;
        holdStart_ = holdStart;
    }

    ~TimedLockGuard()
    {
        auto holdEnd = Clock::now();
        holdAcc_ += holdEnd - holdStart_;
        mtx_.unlock();
    }

    TimedLockGuard(const TimedLockGuard &) = delete;
    TimedLockGuard &operator=(const TimedLockGuard &) = delete;

private:
    std::mutex &mtx_;
    Nanos &holdAcc_;
    Nanos &waitAcc_;
    Clock::time_point holdStart_;
};

// ============================================================================
// Per-thread timing accumulators
// ============================================================================

struct ThreadTiming {
    Nanos waitNs{0};
    Nanos holdNs{0};
    uint64_t ops{0};
    std::vector<double> latencyUs;  // per-operation latency samples

    void Reserve(size_t cap) { latencyUs.reserve(cap); }

    void Merge(const ThreadTiming &other)
    {
        waitNs += other.waitNs;
        holdNs += other.holdNs;
        ops += other.ops;
        latencyUs.insert(latencyUs.end(), other.latencyUs.begin(), other.latencyUs.end());
    }
};

// ============================================================================
// Variant A: Baseline — single std::mutex (current BrpcAsyncContext)
// ============================================================================

class BaselineContext {
public:
    int64_t AllocateTag(ThreadTiming &tim)
    {
        auto t0 = Clock::now();
        int64_t tagId;
        {
            TimedLockGuard lock(mapMtx_, tim.waitNs, tim.holdNs);
            tagId = nextTagId_++;
            pendingCalls_[tagId] = tagId;  // store tag as value for simplicity
        }
        auto t1 = Clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        tim.latencyUs.push_back(us);
        tim.ops++;
        return tagId;
    }

    bool TakeCall(int64_t tagId, ThreadTiming &tim)
    {
        auto t0 = Clock::now();
        bool found = false;
        {
            TimedLockGuard lock(mapMtx_, tim.waitNs, tim.holdNs);
            auto it = pendingCalls_.find(tagId);
            if (it != pendingCalls_.end()) {
                pendingCalls_.erase(it);
                found = true;
            }
        }
        auto t1 = Clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        tim.latencyUs.push_back(us);
        tim.ops++;
        return found;
    }

    void ForgetCall(int64_t tagId, ThreadTiming &tim)
    {
        auto t0 = Clock::now();
        {
            TimedLockGuard lock(mapMtx_, tim.waitNs, tim.holdNs);
            pendingCalls_.erase(tagId);
        }
        auto t1 = Clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        tim.latencyUs.push_back(us);
        tim.ops++;
    }

    size_t Size()
    {
        std::lock_guard<std::mutex> lock(mapMtx_);
        return pendingCalls_.size();
    }

private:
    std::mutex mapMtx_;
    std::map<int64_t, int64_t> pendingCalls_;
    int64_t nextTagId_{1};
};

// ============================================================================
// Variant B: Sharded 16-bucket mutex
// ============================================================================

static constexpr size_t kShardCount16 = 16;
static constexpr size_t kShardCount32 = 32;
static constexpr size_t kShardCount64 = 64;

template <size_t N>
class ShardedContext {
public:
    int64_t AllocateTag(ThreadTiming &tim)
    {
        auto t0 = Clock::now();
        int64_t tagId = nextTagId_.fetch_add(1, std::memory_order_relaxed);
        {
            size_t bucket = static_cast<size_t>(tagId) & (N - 1);  // power-of-2, matches production GetShardIndex
            TimedLockGuard lock(bucketMtx_[bucket], tim.waitNs, tim.holdNs);
            bucketMap_[bucket][tagId] = tagId;
        }
        auto t1 = Clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        tim.latencyUs.push_back(us);
        tim.ops++;
        return tagId;
    }

    bool TakeCall(int64_t tagId, ThreadTiming &tim)
    {
        auto t0 = Clock::now();
        bool found = false;
        {
            size_t bucket = static_cast<size_t>(tagId) & (N - 1);  // power-of-2, matches production GetShardIndex
            TimedLockGuard lock(bucketMtx_[bucket], tim.waitNs, tim.holdNs);
            auto it = bucketMap_[bucket].find(tagId);
            if (it != bucketMap_[bucket].end()) {
                bucketMap_[bucket].erase(it);
                found = true;
            }
        }
        auto t1 = Clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        tim.latencyUs.push_back(us);
        tim.ops++;
        return found;
    }

    void ForgetCall(int64_t tagId, ThreadTiming &tim)
    {
        auto t0 = Clock::now();
        {
            size_t bucket = static_cast<size_t>(tagId) & (N - 1);  // power-of-2, matches production GetShardIndex
            TimedLockGuard lock(bucketMtx_[bucket], tim.waitNs, tim.holdNs);
            bucketMap_[bucket].erase(tagId);
        }
        auto t1 = Clock::now();
        double us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        tim.latencyUs.push_back(us);
        tim.ops++;
    }

    size_t Size()
    {
        size_t total = 0;
        for (size_t i = 0; i < N; ++i) {
            std::lock_guard<std::mutex> lock(bucketMtx_[i]);
            total += bucketMap_[i].size();
        }
        return total;
    }

private:
    std::array<std::mutex, N> bucketMtx_;
    std::array<std::map<int64_t, int64_t>, N> bucketMap_;
    std::atomic<int64_t> nextTagId_{1};
};

// ============================================================================
// Latency statistics
// ============================================================================

struct LatencyStats {
    double avgUs;
    double p50Us;
    double p99Us;
    double p999Us;
    double maxUs;
    double minUs;

    static LatencyStats Compute(std::vector<double> &samples)
    {
        LatencyStats s{};
        if (samples.empty()) return s;

        std::sort(samples.begin(), samples.end());
        size_t n = samples.size();

        double sum = 0.0;
        for (double v : samples) sum += v;
        s.avgUs = sum / n;
        s.minUs = samples[0];
        s.maxUs = samples[n - 1];
        s.p50Us = samples[PercentileIdx(n, 50.0)];
        s.p99Us = samples[PercentileIdx(n, 99.0)];
        s.p999Us = samples[PercentileIdx(n, 99.9)];
        return s;
    }

private:
    static size_t PercentileIdx(size_t n, double pct)
    {
        size_t idx = static_cast<size_t>(pct / 100.0 * (n - 1));
        return idx >= n ? n - 1 : idx;
    }
};

// ============================================================================
// Benchmark runner
// ============================================================================

enum class OpType { ALLOCATE, TAKE, FORGET, MIXED };

struct BenchResult {
    std::string variant;
    int threads;
    std::string opType;
    double durationSec;
    uint64_t totalOps;
    double qps;
    LatencyStats latency;
    double waitUsPerOp;
    double holdUsPerOp;
    double totalUsPerOp;
    double waitPct;
};

/**
 * Mixed workload: simulates complete async RPC lifecycle.
 * Each cycle = AllocateTag(tagA) → TakeCall(tagA) + AllocateTag(tagB) → ForgetCall(tagB).
 * Ratio: 2 allocate : 1 take : 1 forget (4 lock acquisitions per cycle).
 *
 * TakeCall and ForgetCall operate on DIFFERENT tags because in production
 * a given tag goes through either TakeCall OR ForgetCall, not both.
 */
template <typename Context>
static void MixedWorker(Context &ctx, std::atomic<bool> &startFlag, std::atomic<bool> &stopFlag,
                        ThreadTiming &tim)
{
    while (!startFlag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    while (!stopFlag.load(std::memory_order_acquire)) {
        // Path A: allocate → take (simulates successful RPC)
        int64_t tagA = ctx.AllocateTag(tim);
        ctx.TakeCall(tagA, tim);

        // Path B: allocate → forget (simulates cancelled/forgotten RPC)
        int64_t tagB = ctx.AllocateTag(tim);
        ctx.ForgetCall(tagB, tim);
    }
}

/**
 * Allocate-only workload.
 */
template <typename Context>
static void AllocateWorker(Context &ctx, std::atomic<bool> &startFlag, std::atomic<bool> &stopFlag,
                           ThreadTiming &tim)
{
    while (!startFlag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    while (!stopFlag.load(std::memory_order_acquire)) {
        ctx.AllocateTag(tim);
    }
}

/**
 * Take-only workload: batch-allocate (untimed) then consume via TakeCall (timed).
 * Each iteration = allocate batch → take each, ensuring fresh tags always available.
 */
template <typename Context>
static void TakeWorker(Context &ctx, std::atomic<bool> &startFlag, std::atomic<bool> &stopFlag,
                       ThreadTiming &tim)
{
    std::vector<int64_t> batch;
    batch.reserve(256);

    while (!startFlag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    while (!stopFlag.load(std::memory_order_acquire)) {
        // Allocate batch (untimed)
        batch.clear();
        {
            ThreadTiming dummy;
            for (int i = 0; i < 128; ++i) {
                batch.push_back(ctx.AllocateTag(dummy));
            }
        }
        // Take all (timed)
        for (int64_t tag : batch) {
            ctx.TakeCall(tag, tim);
        }
    }
}

/**
 * Forget-only workload: batch-allocate (untimed) then consume via ForgetCall (timed).
 */
template <typename Context>
static void ForgetWorker(Context &ctx, std::atomic<bool> &startFlag, std::atomic<bool> &stopFlag,
                         ThreadTiming &tim)
{
    std::vector<int64_t> batch;
    batch.reserve(256);

    while (!startFlag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    while (!stopFlag.load(std::memory_order_acquire)) {
        // Allocate batch (untimed)
        batch.clear();
        {
            ThreadTiming dummy;
            for (int i = 0; i < 128; ++i) {
                batch.push_back(ctx.AllocateTag(dummy));
            }
        }
        // Forget all (timed)
        for (int64_t tag : batch) {
            ctx.ForgetCall(tag, tim);
        }
    }
}

template <typename Context>
static BenchResult RunBench(Context &ctx, const std::string &variantName, int numThreads, OpType opType,
                            int durationSec)
{
    std::vector<std::thread> workers;
    std::vector<ThreadTiming> threadTims(numThreads);
    std::atomic<bool> startFlag{false};
    std::atomic<bool> stopFlag{false};

    // Reserve sample capacity per thread
    // Mixed: 3 ops per cycle; allocate/take/forget: 1 op per cycle
    size_t sampleCap = opType == OpType::MIXED
                           ? static_cast<size_t>(durationSec) * 600'000   // 3x ops per cycle
                           : static_cast<size_t>(durationSec) * 2'000'000;
    for (auto &tim : threadTims) {
        tim.Reserve(sampleCap);
    }

    // Spawn worker threads
    auto wallStart = Clock::now();

    for (int t = 0; t < numThreads; ++t) {
        switch (opType) {
        case OpType::MIXED:
            workers.emplace_back([&ctx, &startFlag, &stopFlag, &tim = threadTims[t]]() {
                MixedWorker(ctx, startFlag, stopFlag, tim);
            });
            break;
        case OpType::ALLOCATE:
            workers.emplace_back([&ctx, &startFlag, &stopFlag, &tim = threadTims[t]]() {
                AllocateWorker(ctx, startFlag, stopFlag, tim);
            });
            break;
        case OpType::TAKE:
            workers.emplace_back([&ctx, &startFlag, &stopFlag, &tim = threadTims[t]]() {
                TakeWorker(ctx, startFlag, stopFlag, tim);
            });
            break;
        case OpType::FORGET:
            workers.emplace_back([&ctx, &startFlag, &stopFlag, &tim = threadTims[t]]() {
                ForgetWorker(ctx, startFlag, stopFlag, tim);
            });
            break;
        }
    }

    // Start all threads
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    startFlag.store(true, std::memory_order_release);

    // Run for duration
    std::this_thread::sleep_for(std::chrono::seconds(durationSec));
    stopFlag.store(true, std::memory_order_release);

    // Join all threads
    for (auto &w : workers) {
        w.join();
    }

    auto wallEnd = Clock::now();
    double wallSec = std::chrono::duration<double>(wallEnd - wallStart).count();

    // Merge thread timings
    ThreadTiming merged;
    for (auto &tim : threadTims) {
        merged.Merge(tim);
    }

    // Compute stats
    BenchResult result;
    result.variant = variantName;
    result.threads = numThreads;
    result.opType = [&]() -> std::string {
        switch (opType) {
        case OpType::ALLOCATE: return "allocate";
        case OpType::TAKE:    return "take";
        case OpType::FORGET:  return "forget";
        case OpType::MIXED:   return "mixed";
        }
        return "unknown";
    }();
    result.durationSec = wallSec;
    result.totalOps = merged.ops;
    result.qps = wallSec > 0 ? merged.ops / wallSec : 0.0;

    if (!merged.latencyUs.empty()) {
        result.latency = LatencyStats::Compute(merged.latencyUs);
    }

    if (merged.ops > 0) {
        double waitUs = std::chrono::duration<double, std::micro>(merged.waitNs).count();
        double holdUs = std::chrono::duration<double, std::micro>(merged.holdNs).count();
        result.waitUsPerOp = waitUs / merged.ops;
        result.holdUsPerOp = holdUs / merged.ops;
        result.totalUsPerOp = result.waitUsPerOp + result.holdUsPerOp;
        result.waitPct = result.totalUsPerOp > 0 ? (result.waitUsPerOp / result.totalUsPerOp * 100.0) : 0.0;
    }

    return result;
}

// ============================================================================
// CSV output
// ============================================================================

static void PrintHeader()
{
    std::cout << "variant,threads,op_type,duration_sec,total_ops,qps,"
              << "avg_us,p50_us,p99_us,p999_us,min_us,max_us,"
              << "wait_us_per_op,hold_us_per_op,total_us_per_op,wait_pct\n";
}

static void PrintResult(const BenchResult &r)
{
    std::cout << std::fixed << std::setprecision(2);
    std::cout << r.variant << "," << r.threads << "," << r.opType << "," << std::setprecision(1) << r.durationSec
              << "," << r.totalOps << "," << std::setprecision(0) << r.qps << "," << std::setprecision(3)
              << r.latency.avgUs << "," << r.latency.p50Us << "," << r.latency.p99Us << "," << r.latency.p999Us
              << "," << r.latency.minUs << "," << r.latency.maxUs << "," << std::setprecision(3) << r.waitUsPerOp
              << "," << r.holdUsPerOp << "," << r.totalUsPerOp << "," << std::setprecision(1) << r.waitPct << "\n";
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv)
{
    int durationSec = 10;  // default 10s, use 30s for final run
    std::vector<int> threadCounts = {1, 8, 32, 64, 128};
    std::vector<OpType> opTypes = {OpType::MIXED, OpType::ALLOCATE, OpType::TAKE, OpType::FORGET};
    std::vector<std::string> opNames = {"allocate", "take", "forget", "mixed"};

    // Parse args: --duration=N --threads=a,b,c --ops=allocate,take,forget,mixed
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg.rfind("--duration=", 0) == 0) {
            durationSec = std::stoi(arg.substr(11));
        } else if (arg.rfind("--threads=", 0) == 0) {
            threadCounts.clear();
            std::string vals = arg.substr(10);
            std::istringstream iss(vals);
            std::string tok;
            while (std::getline(iss, tok, ',')) {
                threadCounts.push_back(std::stoi(tok));
            }
        } else if (arg.rfind("--ops=", 0) == 0) {
            opTypes.clear();
            std::string vals = arg.substr(6);
            std::istringstream iss(vals);
            std::string tok;
            while (std::getline(iss, tok, ',')) {
                if (tok == "allocate") opTypes.push_back(OpType::ALLOCATE);
                else if (tok == "take") opTypes.push_back(OpType::TAKE);
                else if (tok == "forget") opTypes.push_back(OpType::FORGET);
                else if (tok == "mixed") opTypes.push_back(OpType::MIXED);
            }
        }
    }

    std::cout << "=== BrpcAsyncContext Mutex Contention Benchmark ===\n";
    std::cout << "Duration: " << durationSec << "s per combination\n";
    std::cout << "Thread counts: ";
    for (size_t i = 0; i < threadCounts.size(); ++i) {
        if (i > 0) std::cout << ",";
        std::cout << threadCounts[i];
    }
    std::cout << "\nOp types: ";
    for (size_t i = 0; i < opTypes.size(); ++i) {
        if (i > 0) std::cout << ",";
        std::cout << opNames[static_cast<int>(opTypes[i])];
    }
    std::cout << "\nVariants: baseline, sharded16, sharded32, sharded64\n\n";

    std::vector<BenchResult> results;

    auto runVariant = [&](const std::string &name, auto &ctx) {
        for (auto op : opTypes) {
            for (int n : threadCounts) {
                std::cout << "  [" << name << "] " << opNames[static_cast<int>(op)] << " threads=" << n
                          << " ... " << std::flush;
                auto r = RunBench(ctx, name, n, op, durationSec);
                results.push_back(r);
                std::cout << std::fixed << std::setprecision(0) << "QPS=" << r.qps << std::setprecision(3)
                          << " avg=" << r.latency.avgUs << "us p99=" << r.latency.p99Us
                          << "us wait=" << std::setprecision(1) << r.waitPct << "%\n";
            }
        }
    };

    // Variant A: Baseline
    {
        BaselineContext ctx;
        runVariant("baseline", ctx);
    }

    // Variant B: Sharded 16
    {
        ShardedContext<kShardCount16> ctx;
        runVariant("sharded16", ctx);
    }

    // Variant C: Sharded 32
    {
        ShardedContext<kShardCount32> ctx;
        runVariant("sharded32", ctx);
    }

    // Variant D: Sharded 64
    {
        ShardedContext<kShardCount64> ctx;
        runVariant("sharded64", ctx);
    }

    // Output CSV
    std::cout << "\n[RESULT_CSV]\n";
    PrintHeader();
    for (const auto &r : results) {
        PrintResult(r);
    }
    std::cout << "\n";

    // Summary: wait% comparison for mixed workload at highest thread count
    std::cout << "[WAIT_PCT_SUMMARY] Mixed workload mutex wait %:\n";
    std::cout << "threads,baseline,sharded16,sharded32,sharded64\n";
    for (int n : threadCounts) {
        std::cout << n;
        for (const auto &r : results) {
            if (r.opType == "mixed" && r.threads == n) {
                std::cout << "," << std::fixed << std::setprecision(1) << r.waitPct;
            }
        }
        std::cout << "\n";
    }

    std::cout << "\n[INFO] Benchmark complete.\n";
    return 0;
}
