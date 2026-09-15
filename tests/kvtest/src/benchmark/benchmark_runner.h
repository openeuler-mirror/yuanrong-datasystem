#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "common/simple_log.h"
#include "common/config.h"
#include "benchmark/benchmark_result.h"
#include "vendor/TDigest.h"

// Key calculation utilities
int CalcKeysPerRound(int workerMemoryMb, uint64_t dataSize);

/**
 * @brief Calculate the cleanup wait before the next benchmark round.
 * @param[in] configuredWaitMs Configured cleanup wait in milliseconds.
 * @param[in] maxDurationMs Benchmark duration limit in milliseconds, or zero when unlimited.
 * @param[in] elapsedMs Elapsed benchmark time in milliseconds.
 * @return Cleanup wait capped by the remaining benchmark duration.
 */
int64_t CalcRoundCleanupWaitMs(int configuredWaitMs, int64_t maxDurationMs, int64_t elapsedMs);

std::string MakeBenchKey(int instanceId, int round, int index);
std::pair<int, int> ThreadKeyRange(int totalKeys, int numThreads, int threadId);

// Per-phase result with per-request latency tracking
struct PhaseResult {
    int successCount = 0;
    int failureCount = 0;
    std::vector<double> latenciesMs;  // per-request latency for successful ops
};

// Compute percentiles from a sorted latency vector
struct Percentiles {
    double avg = 0;
    double min = 0;
    double p50 = 0;
    double p90 = 0;
    double p99 = 0;
    double p999 = 0;
    double p9999 = 0;
    double max = 0;
};

inline constexpr size_t BENCHMARK_TDIGEST_COMPRESSION = 10000;
inline constexpr int64_t BENCHMARK_MILLISECONDS_PER_SECOND = 1000;
inline constexpr int64_t BENCHMARK_NANOSECONDS_PER_MICROSECOND = 1000;
inline constexpr int64_t BENCHMARK_NANOSECONDS_PER_MILLISECOND = 1'000'000;
inline constexpr int64_t BENCHMARK_NANOSECONDS_PER_SECOND = 1'000'000'000;
inline constexpr uint64_t BENCHMARK_BYTES_PER_MIB = 1024 * 1024;

/** @brief Calculate successful operations per second from a measured wall-clock span. */
inline double CalcBenchmarkQps(int64_t successCount, double elapsedMs)
{
    return elapsedMs > 0
               ? static_cast<double>(successCount) * BENCHMARK_MILLISECONDS_PER_SECOND / elapsedMs
               : 0;
}

/** @brief Calculate successful payload throughput in MiB/s from the same measured span. */
inline double CalcBenchmarkThroughputMiBps(int64_t successCount, uint64_t dataSize, double elapsedMs)
{
    return elapsedMs > 0
               ? static_cast<double>(successCount) * dataSize * BENCHMARK_MILLISECONDS_PER_SECOND
                     / (elapsedMs * BENCHMARK_BYTES_PER_MIB)
               : 0;
}

/** @brief Accumulates bounded-memory latency and operation counts for one measured phase. */
struct StreamingPhaseResult {
    int64_t successCount = 0;
    int64_t failureCount = 0;
    int64_t notFoundCount = 0;
    int64_t timeoutCount = 0;
    double totalLatencyMs = 0;
    double minLatencyMs = std::numeric_limits<double>::max();
    double maxLatencyMs = 0;
    int64_t firstStartNs = 0;
    int64_t lastEndNs = 0;
    tdigest::TDigest latencyDigest{ BENCHMARK_TDIGEST_COMPRESSION };

    StreamingPhaseResult() = default;
    StreamingPhaseResult(const StreamingPhaseResult &) = delete;
    StreamingPhaseResult &operator=(const StreamingPhaseResult &) = delete;
    StreamingPhaseResult(StreamingPhaseResult &&) = default;
    StreamingPhaseResult &operator=(StreamingPhaseResult &&) = default;

    /** @brief Record one SDK operation. */
    void Record(const BenchmarkOpResult &op, double latencyMs, int64_t startNs, int64_t endNs)
    {
        if (firstStartNs == 0 || startNs < firstStartNs) {
            firstStartNs = startNs;
        }
        lastEndNs = std::max(lastEndNs, endNs);
        if (!op.success) {
            ++failureCount;
            if (op.notFound) {
                ++notFoundCount;
            }
            if (op.timeout) {
                ++timeoutCount;
            }
            return;
        }
        ++successCount;
        totalLatencyMs += latencyMs;
        minLatencyMs = std::min(minLatencyMs, latencyMs);
        maxLatencyMs = std::max(maxLatencyMs, latencyMs);
        latencyDigest.add(latencyMs);
    }

    /** @brief Record one batch as per-key samples. */
    void RecordBatch(const BenchmarkOpResult &op, int64_t count, double batchLatencyMs, int64_t startNs, int64_t endNs)
    {
        if (count <= 0) {
            return;
        }
        if (firstStartNs == 0 || startNs < firstStartNs) {
            firstStartNs = startNs;
        }
        lastEndNs = std::max(lastEndNs, endNs);
        if (!op.success) {
            failureCount += count;
            return;
        }
        const double perKeyLatencyMs = batchLatencyMs / static_cast<double>(count);
        successCount += count;
        totalLatencyMs += batchLatencyMs;
        minLatencyMs = std::min(minLatencyMs, perKeyLatencyMs);
        maxLatencyMs = std::max(maxLatencyMs, perKeyLatencyMs);
        latencyDigest.add(perKeyLatencyMs, static_cast<double>(count));
    }

    /** @brief Merge another phase result into this result. */
    void Merge(StreamingPhaseResult &other)
    {
        successCount += other.successCount;
        failureCount += other.failureCount;
        notFoundCount += other.notFoundCount;
        timeoutCount += other.timeoutCount;
        totalLatencyMs += other.totalLatencyMs;
        if (other.successCount > 0) {
            minLatencyMs = std::min(minLatencyMs, other.minLatencyMs);
            maxLatencyMs = std::max(maxLatencyMs, other.maxLatencyMs);
        }
        if (firstStartNs == 0 || (other.firstStartNs != 0 && other.firstStartNs < firstStartNs)) {
            firstStartNs = other.firstStartNs;
        }
        lastEndNs = std::max(lastEndNs, other.lastEndNs);
        other.latencyDigest.compress();
        latencyDigest.merge(&other.latencyDigest);
    }

    /** @brief Return the wall-clock span from the first request start to the last request end. */
    double ElapsedMs() const
    {
        return firstStartNs == 0
                   ? 0
                   : static_cast<double>(lastEndNs - firstStartNs) / BENCHMARK_NANOSECONDS_PER_MILLISECOND;
    }

    /** @brief Return latency statistics for successful operations. */
    Percentiles GetPercentiles()
    {
        Percentiles result;
        if (successCount == 0) {
            return result;
        }
        latencyDigest.compress();
        result.avg = totalLatencyMs / static_cast<double>(successCount);
        result.min = minLatencyMs;
        result.p50 = latencyDigest.quantile(0.5);
        result.p90 = latencyDigest.quantile(0.9);
        result.p99 = latencyDigest.quantile(0.99);
        result.p999 = latencyDigest.quantile(0.999);
        result.p9999 = latencyDigest.quantile(0.9999);
        result.max = maxLatencyMs;
        return result;
    }
};

inline Percentiles ComputePercentiles(std::vector<double> latencies) {
    Percentiles p;
    if (latencies.empty()) return p;
    double sum = 0;
    for (auto v : latencies) sum += v;
    p.avg = sum / latencies.size();
    std::sort(latencies.begin(), latencies.end());
    p.min = latencies.front();
    auto rank = [&](double pct) -> double {
        size_t idx = static_cast<size_t>(std::ceil(pct / 100.0 * latencies.size()));
        return latencies[std::min(idx, latencies.size()) - 1];
    };
    p.p50 = rank(50);
    p.p90 = rank(90);
    p.p99 = rank(99);
    p.p999 = rank(99.9);
    p.p9999 = rank(99.99);
    p.max = latencies.back();
    return p;
}

// Phase execution functions (template on client type for testability)
template<typename Client>
PhaseResult RunSetPhase(Client *client, int instanceId, int round, int startKey, int numKeys,
                        const std::string &setApi, const std::string &data);

template<typename Client>
PhaseResult RunGetPhase(Client *client, int instanceId, int round, int startKey, int numKeys);

template<typename Client>
PhaseResult RunDelPhase(Client *client, int instanceId, int round, int startKey, int numKeys);

template<typename Client>
PhaseResult RunMSetPhase(Client *client, int instanceId, int round, int startKey, int numKeys,
                         int batchSize, const std::string &data) {
    PhaseResult result;
    for (int offset = 0; offset < numKeys; offset += batchSize) {
        int batchEnd = std::min(offset + batchSize, numKeys);
        std::vector<std::string> keys;
        keys.reserve(batchEnd - offset);
        for (int i = offset; i < batchEnd; i++) {
            keys.push_back(MakeBenchKey(instanceId, round, startKey + i));
        }
        auto start = std::chrono::steady_clock::now();
        bool ok = client->MSet(keys, data);
        auto end = std::chrono::steady_clock::now();
        if (ok) {
            result.successCount += keys.size();
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            result.latenciesMs.push_back(ms);
        } else {
            result.failureCount++;
        }
    }
    return result;
}

template<typename Client>
PhaseResult RunMGetPhase(Client *client, int instanceId, int round, int startKey, int numKeys,
                         int batchSize) {
    PhaseResult result;
    for (int offset = 0; offset < numKeys; offset += batchSize) {
        int batchEnd = std::min(offset + batchSize, numKeys);
        std::vector<std::string> keys;
        keys.reserve(batchEnd - offset);
        for (int i = offset; i < batchEnd; i++) {
            keys.push_back(MakeBenchKey(instanceId, round, startKey + i));
        }
        auto start = std::chrono::steady_clock::now();
        bool ok = client->MGetVerify(keys);
        auto end = std::chrono::steady_clock::now();
        if (ok) {
            result.successCount += keys.size();
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            result.latenciesMs.push_back(ms);
        } else {
            result.failureCount++;
        }
    }
    return result;
}

// Thread barrier for phase synchronization
class Barrier {
public:
    explicit Barrier(int count) : threshold_(count), count_(count) {}
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (--count_ == 0) {
            count_ = threshold_;
            generation_++;
            cv_.notify_all();
        } else {
            auto gen = generation_;
            cv_.wait(lock, [&] { return gen != generation_; });
        }
    }
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int threshold_;
    int count_;
    int generation_ = 0;
};

struct BenchmarkStats {
    std::atomic<int> roundsCompleted{0};
    std::atomic<int> totalSet{0};
    std::atomic<int> totalGet{0};
    std::atomic<int> totalDel{0};
};

struct BenchmarkParams {
    int numThreads = 1;
    int keysPerRound = 0;
    std::string setApi = "string_view";
    bool isGetMode = false;
    std::string cleanupMethod = "del";
    uint32_t ttlSeconds = 0;
    int maxRounds = 0;      // 0 = infinite
    int64_t maxDurationMs = 0;  // 0 = infinite
    uint64_t dataSize = 0;
    std::string data;
};

// --- Template implementations (header-only for template linkage) ---

template<typename Client>
PhaseResult RunSetPhase(Client *client, int instanceId, int round, int startKey, int numKeys,
                        const std::string &setApi, const std::string &data) {
    PhaseResult result;
    for (int i = 0; i < numKeys; i++) {
        std::string key = MakeBenchKey(instanceId, round, startKey + i);
        auto start = std::chrono::steady_clock::now();
        bool ok = false;
        if (setApi == "string_view") {
            ok = client->Set(key, data);
        } else if (setApi == "create_buffer") {
            ok = client->CreateAndSet(key, data.size(), data);
        } else if (setApi == "create_buffer_raw") {
            ok = client->CreateAndSetRaw(key, data.size(), data);
        }
        auto end = std::chrono::steady_clock::now();
        if (ok) {
            result.successCount++;
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            result.latenciesMs.push_back(ms);
        } else {
            result.failureCount++;
        }
    }
    return result;
}

template<typename Client>
PhaseResult RunGetPhase(Client *client, int instanceId, int round, int startKey, int numKeys) {
    PhaseResult result;
    for (int i = 0; i < numKeys; i++) {
        std::string key = MakeBenchKey(instanceId, round, startKey + i);
        auto start = std::chrono::steady_clock::now();
        bool ok = client->GetVerify(key);
        auto end = std::chrono::steady_clock::now();
        if (ok) {
            result.successCount++;
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            result.latenciesMs.push_back(ms);
        } else {
            result.failureCount++;
        }
    }
    return result;
}

template<typename Client>
PhaseResult RunDelPhase(Client *client, int instanceId, int round, int startKey, int numKeys) {
    PhaseResult result;
    constexpr int kBatchSize = 1000;
    constexpr int kMaxRetries = 3;
    if (numKeys <= 0) return result;

    for (int offset = 0; offset < numKeys; offset += kBatchSize) {
        int batchEnd = std::min(offset + kBatchSize, numKeys);
        std::vector<std::string> keys;
        keys.reserve(batchEnd - offset);
        for (int i = offset; i < batchEnd; i++) {
            keys.push_back(MakeBenchKey(instanceId, round, startKey + i));
        }
        bool ok = false;
        double delMs = 0;
        for (int attempt = 0; attempt < kMaxRetries; attempt++) {
            auto start = std::chrono::steady_clock::now();
            if (client->Del(keys)) {
                auto end = std::chrono::steady_clock::now();
                delMs = std::chrono::duration<double, std::milli>(end - start).count();
                ok = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (ok) {
            result.successCount += keys.size();
            result.latenciesMs.push_back(delMs / keys.size());
        } else {
            result.failureCount++;
            SLOG_WARN("RunDelPhase: batch " << (offset / kBatchSize)
                      << " failed after " << kMaxRetries << " retries"
                      << " (round=" << round << ", keys=" << keys.size() << ")");
        }
    }
    return result;
}


// Get the round number for get threads based on key strategy
inline int GetRoundForGet(MixedKeyStrategy strategy, int round) {
    switch (strategy) {
        case MixedKeyStrategy::SAME_KEYS: return round;
        case MixedKeyStrategy::READ_PREV: return round > 0 ? round - 1 : -1;
        case MixedKeyStrategy::INDEPENDENT: return 0;
    }
    return round;
}


// Multi-threaded round execution (test helper)
template<typename Client>
void RunBenchmarkRounds(Client *client, BenchmarkStats *stats, const BenchmarkParams &params) {
    for (int round = 0; params.maxRounds == 0 || round < params.maxRounds; round++) {
        // Set phase
        {
            std::vector<PhaseResult> threadResults(params.numThreads);
            std::vector<std::thread> threads;
            for (int t = 0; t < params.numThreads; t++) {
                threads.emplace_back([&, t]() {
                    auto range = ThreadKeyRange(params.keysPerRound, params.numThreads, t);
                    if (range.second == 0) return;
                    threadResults[t] = RunSetPhase(client, 0, round, range.first, range.second,
                                                   params.setApi, params.data);
                });
            }
            for (auto &t : threads) t.join();
            for (auto &r : threadResults) stats->totalSet += r.successCount;
        }

        // Get phase
        if (params.isGetMode) {
            std::vector<PhaseResult> threadResults(params.numThreads);
            std::vector<std::thread> threads;
            for (int t = 0; t < params.numThreads; t++) {
                threads.emplace_back([&, t]() {
                    auto range = ThreadKeyRange(params.keysPerRound, params.numThreads, t);
                    if (range.second == 0) return;
                    threadResults[t] = RunGetPhase(client, 0, round, range.first, range.second);
                });
            }
            for (auto &t : threads) t.join();
            for (auto &r : threadResults) stats->totalGet += r.successCount;
        }

        // Del phase
        if (params.cleanupMethod == "del") {
            std::vector<PhaseResult> threadResults(params.numThreads);
            std::vector<std::thread> threads;
            for (int t = 0; t < params.numThreads; t++) {
                threads.emplace_back([&, t]() {
                    auto range = ThreadKeyRange(params.keysPerRound, params.numThreads, t);
                    if (range.second == 0) return;
                    threadResults[t] = RunDelPhase(client, 0, round, range.first, range.second);
                });
            }
            for (auto &t : threads) t.join();
            for (auto &r : threadResults) stats->totalDel += r.successCount;
        }

        stats->roundsCompleted++;
    }
}
