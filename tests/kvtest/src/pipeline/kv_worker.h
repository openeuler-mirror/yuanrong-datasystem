#pragma once

#include "common/bthread_compat.h"
#include "common/config.h"
#include "common/thread_pool.h"
#include "metrics/metrics.h"
#include "pipeline.h"
#include "rpc/peer_client.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>

class KVWorker {
public:
    KVWorker(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
             MetricsCollector &metrics);
    ~KVWorker();

    void Start();
    void Stop();

    // Signal pipeline threads and the notify pool to stop without joining.
    // Safe to call from the HTTP/bRPC control-plane thread: it flips the
    // pipeline loop flag and drops queued notify tasks so no new Set/notify
    // requests are issued, instead of waiting for main's shutdown delay.
    // Mid-flight RPCs finish (cannot be killed) then threads exit; the join
    // happens in Stop() on shutdown.
    void RequestStop();

    size_t NotifyQueueSize() { return notifyPool_.QueueSize(); }
    uint64_t NotifyDroppedCount() { return notifyPool_.DroppedCount(); }
    uint64_t NotifySuppressedCount() const { return notifySuppressed_.load(std::memory_order_relaxed); }
    PeerNotifySkipCounts NotifyPeerSkipCounts() const { return peerClient_->GetNotifySkipCounts(); }
    uint64_t CurrentPoolSize() { return currentPoolSize_.load(); }

    // Adjust pool size based on current hit rate vs target.
    // Called periodically from main loop when targetHitRate > 0.
    void AdjustPoolSize();

    // Advance to next QPS stage based on elapsed time.
    // Called periodically from main loop when targetQpsStages is non-empty.
    void AdvanceStage();

    int CurrentTargetQps() { return currentTargetQps_.load(); }

    // Record the latest-known version stamp for the given keys (from cache
    // update broadcasts), so the writer can detect stale reads on hit.
    void OnUpdateVersion(const std::vector<std::string> &keys, uint64_t version);

    // Snapshot of the per-key max-version map size, for logging.
    size_t MaxSeenVersionSize() const;

private:
    void PipelineLoop(int threadId);
    void NotifyPeers(const std::vector<std::string> &keys, uint64_t size, uint64_t version);
    void NotifyWarmupDone(const std::vector<std::string> &warmupKeys);
    int ResampleRandomQps();

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::unique_ptr<PeerControlClient> peerClient_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> currentPoolSize_{0};
    std::atomic<int> currentTargetQps_{0};
    std::atomic<uint64_t> notifySuppressed_{0};
    std::atomic<double> missBackfillProb_{1.0};  // probability to backfill a miss; 0 = leave absent
    std::chrono::steady_clock::time_point stageStartTime_;
    // Per-key max version seen (from broadcast updates + own hits), for
    // stale-read detection. Guards by versionMutex_.
    mutable kvtest::mutex versionMutex_;
    std::unordered_map<std::string, uint64_t> maxSeenVersion_;
    int currentStageIndex_ = 0;
    std::vector<kvtest::thread> threads_;
    ThreadPool notifyPool_;
    std::vector<std::pair<std::string, OpFunc>> pipelineOps_;
    bool pipelineNeedsData_ = true;
    std::unordered_map<uint64_t, std::string> pregenData_;
};
