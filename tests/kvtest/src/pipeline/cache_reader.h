#pragma once

#include "common/config.h"
#include "data_pattern.h"
#include "metrics/metrics.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class CacheReader {
public:
    CacheReader(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
                MetricsCollector &metrics);
    ~CacheReader();

    void OnWarmupDone(int senderId, const std::vector<std::string> &warmupKeys);
    void OnEvictKeys(const std::vector<std::string> &keys);

    void Start();
    void Stop();

private:
    void ReaderLoop(int threadId);
    bool CacheGetOrFill(const std::string &key, uint64_t size);
    std::string RandomKey(std::mt19937 &rng);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    VerifyConfig verifyCfg_;

    // Key pool: read-heavy, shared_mutex
    mutable std::shared_mutex keyPoolMutex_;
    std::vector<std::string> keyPool_;
    size_t maxKeyPoolSize_;

    // Warmup synchronization
    std::mutex warmupMutex_;
    std::condition_variable warmupCv_;
    std::set<int> pendingWarmupWriters_;

    // Thread management
    std::atomic<bool> running_{false};
    std::atomic<bool> warmupDone_{false};
    std::vector<std::thread> threads_;

    // Pre-generated data (constructor-time, read-only after)
    std::unordered_map<uint64_t, std::string> pregenData_;
};
