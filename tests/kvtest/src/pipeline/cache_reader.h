#pragma once

#include "common/bthread_compat.h"
#include "common/config.h"
#include "common/thread_pool.h"
#include "data_pattern.h"
#include "metrics/metrics.h"
#include "rpc/peer_client.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

class CacheReader {
public:
    CacheReader(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
                MetricsCollector &metrics);
    ~CacheReader();

    void OnWarmupDone(int senderId, const std::vector<std::string> &warmupKeys);
    void OnEvictKeys(const std::vector<std::string> &keys);
    // Cache update broadcast: record the latest-known version stamp for the
    // given keys (also ensures they are present in the key pool).
    void OnUpdateVersion(const std::vector<std::string> &keys, uint64_t version);

    void Start();
    void Stop();

    // Signal reader threads to stop without joining. Safe to call from the
    // HTTP/bRPC control-plane thread; the join happens in Stop() on shutdown.
    void RequestStop() {
        running_ = false;
        warmupCv_.notify_all();
    }

    // Advance to the next QPS stage / re-sample random QPS based on elapsed
    // time. Called periodically from the main loop (mirrors KVWorker).
    void AdvanceStage();

    int CurrentTargetQps() { return currentTargetQps_.load(); }

    // Snapshot of the reader key-pool size (present keys), for logging.
    size_t KeyPoolSize() const {
        std::shared_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
        return keyPool_.size();
    }

    // Snapshot of the per-key max-version map size, for logging.
    size_t MaxSeenVersionSize() const {
        std::lock_guard<kvtest::mutex> lock(versionMutex_);
        return maxSeenVersion_.size();
    }

private:
    void ReaderLoop(int threadId);
    bool CacheGetOrFill(const std::string &key, uint64_t size);
    bool CacheGetOrFillBatch(const std::vector<std::string> &keys,
                             const std::vector<uint64_t> &sizes);
    std::string RandomKey(std::mt19937 &rng);
    // Fire-and-forget broadcast of an update (key, version) to reader peers.
    void BroadcastUpdate(const std::vector<std::string> &keys, uint64_t version);
    int ResampleRandomQps();

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    VerifyConfig verifyCfg_;

    // Key pool: read-heavy, kvtest::shared_mutex (bthread::RWLock in bazel
    // mode so a bthread reader waiting on a writer yields the bthread instead
    // of blocking a pthread worker).
    mutable kvtest::shared_mutex keyPoolMutex_;
    std::vector<std::string> keyPool_;
    size_t maxKeyPoolSize_;
    size_t warmupKeyCount_ = 0;  // total warmup keys across all writers; never trimmed
    // Hash-based dedup: tracks every key ever added (warmup + backfilled) so a
    // re-broadcast of an already-present key is skipped. Stores only hash
    // values (uint64_t) rather than full strings — far less memory than a
    // string set, at the cost of a negligible hash-collision risk.
    std::unordered_set<uint64_t> seenKeys_;

    // Warmup synchronization — kvtest::mutex/condition_variable alias to
    // bthread primitives in bazel mode so wait_for inside the reader bthread
    // yields the bthread while waiting for warmup_done.
    kvtest::mutex warmupMutex_;
    kvtest::condition_variable warmupCv_;
    std::set<int> pendingWarmupWriters_;

    // Thread management
    std::atomic<bool> running_{false};
    std::atomic<bool> warmupDone_{false};
    std::vector<kvtest::thread> threads_;

    // Dynamic QPS (stages / random min-max), mirrors KVWorker.
    std::atomic<int> currentTargetQps_{0};
    std::chrono::steady_clock::time_point stageStartTime_;
    int currentStageIndex_ = 0;

    // Pre-generated data (constructor-time, read-only after)
    std::unordered_map<uint64_t, std::string> pregenData_;

    // Per-key max version seen (from reads + broadcasts), for stale-read
    // detection. Guards by versionMutex_. Key is the full string (correct: no
    // hash collisions); memory is not a concern for the intended scale.
    mutable kvtest::mutex versionMutex_;
    std::unordered_map<std::string, uint64_t> maxSeenVersion_;
    // Broadcast of update versions to other readers (fire-and-forget).
    std::unique_ptr<PeerControlClient> peerClient_;
    ThreadPool notifyPool_;
};
