#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "common/simple_log.h"

// Key calculation utilities
int CalcKeysPerRound(int workerMemoryMb, uint64_t dataSize);
std::string MakeBenchKey(int instanceId, int round, int index);
std::pair<int, int> ThreadKeyRange(int totalKeys, int numThreads, int threadId);

// Per-phase result with per-request latency tracking
struct PhaseResult {
    int successCount = 0;
    std::vector<double> latenciesMs;  // per-request latency for successful ops
};

// Compute percentiles from a sorted latency vector
struct Percentiles {
    double avg = 0;
    double p50 = 0;
    double p90 = 0;
    double p99 = 0;
    double max = 0;
};

inline Percentiles ComputePercentiles(std::vector<double> latencies) {
    Percentiles p;
    if (latencies.empty()) return p;
    double sum = 0;
    for (auto v : latencies) sum += v;
    p.avg = sum / latencies.size();
    std::sort(latencies.begin(), latencies.end());
    auto rank = [&](double pct) -> double {
        size_t idx = static_cast<size_t>(std::ceil(pct / 100.0 * latencies.size()));
        return latencies[std::min(idx, latencies.size()) - 1];
    };
    p.p50 = rank(50);
    p.p90 = rank(90);
    p.p99 = rank(99);
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
        }
    }
    return result;
}

template<typename Client>
PhaseResult RunGetPhase(Client *client, int instanceId, int round, int startKey, int numKeys) {
    PhaseResult result;
    for (int i = 0; i < numKeys; i++) {
        std::string key = MakeBenchKey(instanceId, round, startKey + i);
        std::string out;
        auto start = std::chrono::steady_clock::now();
        bool ok = client->Get(key, out);
        auto end = std::chrono::steady_clock::now();
        if (ok) {
            result.successCount++;
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            result.latenciesMs.push_back(ms);
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
        auto start = std::chrono::steady_clock::now();
        for (int attempt = 0; attempt < kMaxRetries; attempt++) {
            if (client->Del(keys)) { ok = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        auto end = std::chrono::steady_clock::now();
        if (ok) {
            result.successCount += keys.size();
            double ms = std::chrono::duration<double, std::milli>(end - start).count();
            result.latenciesMs.push_back(ms / keys.size());
        } else {
            SLOG_WARN("RunDelPhase: batch " << (offset / kBatchSize)
                      << " failed after " << kMaxRetries << " retries"
                      << " (round=" << round << ", keys=" << keys.size() << ")");
        }
    }
    return result;
}
