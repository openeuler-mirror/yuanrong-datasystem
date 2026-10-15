#include "kv_worker.h"
#include "data_pattern.h"
#include <datasystem/utils/string_view.h>
#include "common/rate_gate.h"
#include "common/simple_log.h"
#include <chrono>
#include <iomanip>
#include <random>
#include <algorithm>
#include <numeric>

using namespace datasystem;

// Incremental warmup-broadcast batch size: keys accumulate and are sent to
// peers in batches of this size (version=0 → OnEvictKeys on receivers).
namespace {
constexpr size_t kWarmupBroadcastBatch = 50;
}

KVWorker::KVWorker(const Config &cfg, std::shared_ptr<KVClient> client,
                   MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics),
      currentPoolSize_(static_cast<uint64_t>(cfg.keyPoolSize)),
      currentTargetQps_(cfg.targetQps), notifyPool_(100, cfg.notifyQueueMax),
      peerClient_(MakePeerControlClient()) {
    if (cfg_.cuda.transferEnabled) {
        pipelineNeedsData_ = std::find(cfg_.pipeline.begin(), cfg_.pipeline.end(), kOpSetStringView) != cfg_.pipeline.end();
    }
    for (auto &name : cfg_.pipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown pipeline op: " << name << ", skipping");
            continue;
        }
        pipelineOps_.emplace_back(name, fn);
    }

    for (auto size : cfg_.dataSizes) {
        if (pipelineNeedsData_) pregenData_[size] = GeneratePatternData(size, cfg_.instanceId);
    }
}

KVWorker::~KVWorker() { Stop(); }

void KVWorker::Start() {
    running_ = true;

    // Spawn the warmup Set producer first. Keys enter the local (and remote)
    // key pool only after their Set completes, so the pipeline (Get) never
    // samples a key the warmup has not set yet — no backfill race with the
    // staggered Set schedule.
    if (cfg_.keyPoolSize > 0) {
        SLOG_INFO("Starting warmup: " << cfg_.keyPoolSize << " keys");
        std::vector<std::string> warmupKeys;
        warmupKeys.reserve(cfg_.keyPoolSize);
        for (int i = 0; i < cfg_.keyPoolSize; i++) {
            warmupKeys.push_back("cache_pool_" + std::to_string(cfg_.instanceId)
                                 + "_" + std::to_string(i));
        }
        warmupThread_ = std::thread([this, keys = std::move(warmupKeys)]() {
            WarmupSet(keys);
        });
    }

    // Give the warmup producer a head start before Get begins sampling, so the
    // pool is not (near-)empty when the pipeline threads start. With no TTL the
    // warmup bursts in well under a second, so the delay is skipped.
    if (cfg_.keyPoolSize > 0 && cfg_.ttlSeconds > 0) {
        SLOG_INFO("Warmup head start: Get starts 5s after Set begins");
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    SLOG_INFO("Starting " << cfg_.numThreads << " pipeline threads"
              << (cfg_.targetQps > 0 ? "" : " (unlimited QPS)")
              << (cfg_.targetQpsStages.empty() ? "" :
                  " [" + std::to_string(cfg_.targetQpsStages.size()) + " stages x "
                  + std::to_string(cfg_.stageDurationSeconds) + "s]"));
    if (!cfg_.targetQpsStages.empty()) {
        stageStartTime_ = std::chrono::steady_clock::now();
    }
    if (cfg_.targetQpsMin > 0 && cfg_.targetQpsMax > 0) {
        currentTargetQps_.store(ResampleRandomQps());
        SLOG_INFO("Random QPS: [" << cfg_.targetQpsMin << ", " << cfg_.targetQpsMax
                  << "], re-sample every " << cfg_.randomQpsIntervalSeconds << "s, initial="
                  << currentTargetQps_.load());
        stageStartTime_ = std::chrono::steady_clock::now();
    }

    for (int i = 0; i < cfg_.numThreads; i++) {
        threads_.emplace_back(&KVWorker::PipelineLoop, this, i);
    }
}

void KVWorker::WarmupSet(const std::vector<std::string> &keys) {
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    param.ttlSecond = cfg_.ttlSeconds;
    param.existence = cfg_.setNx ? ExistenceOpt::NX : ExistenceOpt::NONE;

    // Spread the Sets across the TTL window so expiry does not synchronize into
    // a burst: key idx should land at start + idx * (ttl/size). Deadline
    // scheduling (not a fixed sleep) so the Set RPC latency itself does not
    // stretch the window. The effective interval is the LARGER of the TTL
    // spread and the target_qps pacing, so the Set rate never exceeds
    // target_qps (prevents Set-timeout bursts when the spread is fast or the
    // TTL is off). Without either, burst (no spacing).
    uint64_t intervalUs = 0;
    if (cfg_.ttlSeconds > 0 && !keys.empty()) {
        intervalUs = static_cast<uint64_t>(cfg_.ttlSeconds) * 1000000ULL / keys.size();
    }
    if (cfg_.targetQps > 0) {
        uint64_t qpsIntervalUs = 1000000ULL / static_cast<uint64_t>(cfg_.targetQps);
        if (qpsIntervalUs > intervalUs) {
            intervalUs = qpsIntervalUs;
        }
    }
    const int64_t startUs = kvtest::SteadyNowUs();

    int warmupOk = 0, warmupFail = 0;
    std::vector<std::string> setKeys;      // successfully Set keys (for the final catch-up)
    std::vector<std::string> pendingBcast; // batched keys awaiting incremental broadcast
    setKeys.reserve(keys.size());
    for (size_t idx = 0; idx < keys.size() && running_.load(); idx++) {
        const std::string &key = keys[idx];
        uint64_t size = CacheKeySize(static_cast<uint64_t>(idx), cfg_.dataSizes);
        auto it = pregenData_.find(size);
        if (it == pregenData_.end()) {
            warmupFail++;
            continue;
        }
        std::string data = GenerateVersionedData(size, NowNanos());

        bool ok = false;
        for (int retry = 0; retry <= cfg_.warmupRetryCount; retry++) {
            Status rc = client_->Set(key, StringView(data), param);
            if (rc.IsOk()) {
                ok = true;
                break;
            }
            if (retry < cfg_.warmupRetryCount) {
                SLOG_WARN("Warmup Set failed for key=" << key
                          << " retry=" << retry << ": " << rc.GetMsg());
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(cfg_.warmupRetryDelayMs));
            }
        }
        if (ok) {
            warmupOk++;
            setKeys.push_back(key);
            // Enter the LOCAL key pool only after the Set completes, so Get
            // never samples a key the warmup has not set yet.
            {
                std::unique_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
                if (seenKeys_.insert(std::hash<std::string>{}(key)).second) {
                    keyPool_.push_back(key);
                    warmupKeyCount_++;
                }
            }
            pendingBcast.push_back(key);
            // Incremental broadcast in batches: receivers route version=0 to
            // OnEvictKeys and add the keys to their pools right away.
            if (pendingBcast.size() >= kWarmupBroadcastBatch) {
                NotifyPeers(pendingBcast, /*size=*/0, /*version=*/0);
                pendingBcast.clear();
            }
        } else {
            warmupFail++;
            SLOG_WARN("Warmup Set failed permanently for key=" << key);
        }

        // Sleep only until the next key's deadline; the Set time already spent
        // counts toward it (no drift).
        if (intervalUs > 0 && idx + 1 < keys.size()) {
            int64_t deadlineUs = startUs
                + static_cast<int64_t>((idx + 1) * intervalUs);
            int64_t nowUs = kvtest::SteadyNowUs();
            if (deadlineUs > nowUs) {
                std::this_thread::sleep_for(
                    std::chrono::microseconds(deadlineUs - nowUs));
            }
        }
    }
    // Flush the trailing incremental batch.
    if (!pendingBcast.empty()) {
        NotifyPeers(pendingBcast, /*size=*/0, /*version=*/0);
        pendingBcast.clear();
    }
    // Final catch-up with retry: covers incremental batches that were dropped,
    // and doubles as the reader-role start signal (readers wait for warmup_done).
    if (!setKeys.empty()) {
        NotifyWarmupDone(setKeys);
    }
    SLOG_INFO("Warmup done: " << warmupOk << " ok, " << warmupFail << " fail");
}

void KVWorker::Stop() {
    running_ = false;
    if (warmupThread_.joinable()) warmupThread_.join();
    for (auto &t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
    notifyPool_.Stop();
}

void KVWorker::RequestStop() {
    running_ = false;
    notifyPool_.StopNow();
}

void KVWorker::PipelineLoop(int threadId) {
    auto calcMyQps = [&](int totalQps) -> int {
        if (totalQps <= 0 || cfg_.numThreads <= 0) return 0;
        int base = totalQps / cfg_.numThreads;
        int rem = totalQps % cfg_.numThreads;
        return base + (threadId < rem ? 1 : 0);
    };

    int64_t intervalUs = 0;
    int myQps = 0;
    int lastKnownQps = currentTargetQps_.load();

    myQps = calcMyQps(lastKnownQps);
    if (myQps > 0) {
        intervalUs = 1000000 / myQps;
    } else if (lastKnownQps > 0) {
        intervalUs = 1000000000;  // effectively stopped
    }

    std::mt19937 rng(threadId + cfg_.instanceId * 1000);
    if (cfg_.dataSizes.empty()) {
        SLOG_WARN("Thread " << threadId << ": no data sizes configured, exiting");
        return;
    }
    auto sizeDist = std::uniform_int_distribution<size_t>(0, cfg_.dataSizes.size() - 1);

    SLOG_INFO("Thread " << threadId << " started"
              << (intervalUs > 0 ? "" : " (unlimited)"));

    kvtest::RateGate gate;
    gate.Configure(intervalUs, threadId, cfg_.numThreads, cfg_.enableJitter);
    gate.ResetGrid(kvtest::SteadyNowUs());

    while (running_) {
        // Check for QPS stage change
        int currentQps = currentTargetQps_.load();
        if (currentQps != lastKnownQps) {
            lastKnownQps = currentQps;
            myQps = calcMyQps(currentQps);
            if (myQps > 0) {
                intervalUs = 1000000 / myQps;
            } else if (currentQps > 0) {
                intervalUs = 1000000000;
            } else {
                intervalUs = 0;
            }
            gate.Configure(intervalUs, threadId, cfg_.numThreads, cfg_.enableJitter);
            gate.ResetGrid(kvtest::SteadyNowUs());
            SLOG_INFO("Thread " << threadId << " QPS → " << currentQps
                      << " (myQps=" << myQps << ", interval=" << intervalUs << "us)");
        }

        if (gate.Active()) {
            int64_t nowUs = kvtest::SteadyNowUs();
            int64_t fireUs = gate.NextFireUs(nowUs, rng);
            if (fireUs > nowUs) {
                // Yield the bthread (brpc mode) or sleep the std::thread
                // (cmake mode); kvtest::sleep_until picks the right primitive
                // so a long fire-time gap doesn't hold a pthread worker
                // when running on a bthread.
                kvtest::sleep_until(kvtest::SteadyFromUs(fireUs));
            }
        }

        PipelineContext ctx;

        if (cfg_.keyPoolSize > 0) {
            // Cache mode: sample keys from the shared key pool (own warmup keys
            // + other clients' keys via broadcast), honoring the hot-set
            // distribution. With mget distribution enabled, sample a batch of
            // distinct keys; otherwise a single key. Empty pool skips.
            bool mget = cfg_.mgetSizeDist.enabled;
            int target = mget ? SampleMgetBatchSize(cfg_.mgetSizeDist, rng) : 1;
            std::unordered_set<std::string> sampled;
            sampled.reserve(static_cast<size_t>(target));
            ctx.batchKeys.clear();
            for (int i = 0; i < target; i++) {
                std::string k;
                int guard = 0;
                do {
                    k = RandomKey(rng);
                    if (k.empty()) break;
                } while (!sampled.insert(k).second && ++guard < 16);
                if (k.empty()) break;
                ctx.batchKeys.push_back(k);
            }
            if (ctx.batchKeys.empty()) {
                kvtest::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
        } else {
            // Original: timestamp-based keys
            ctx.batchKeys.resize(cfg_.batchKeysCount);
            auto now = std::chrono::steady_clock::now().time_since_epoch().count();
            for (int i = 0; i < cfg_.batchKeysCount; i++) {
                ctx.batchKeys[i] = "kv_test_" + std::to_string(cfg_.instanceId)
                                 + "_" + std::to_string(threadId)
                                 + "_" + std::to_string(now) + "_" + std::to_string(i);
            }
        }
        ctx.key = ctx.batchKeys[0];
        // Cache mode: deterministic per-key size (idx % sizes) so a key's
        // value size never changes across warmup/backfill/update. Pipeline
        // mode keeps the random size draw.
        ctx.size = (cfg_.keyPoolSize > 0)
            ? CacheKeySize(ParseCacheKeyIdx(ctx.key), cfg_.dataSizes)
            : cfg_.dataSizes[sizeDist(rng)];
        // Batch sizes for mget mode (per sampled key); empty for single-key.
        ctx.batchSizes.clear();
        if (cfg_.keyPoolSize > 0 && ctx.batchKeys.size() > 1) {
            ctx.batchSizes.reserve(ctx.batchKeys.size());
            for (const auto &k : ctx.batchKeys) {
                ctx.batchSizes.push_back(CacheKeySize(ParseCacheKeyIdx(k), cfg_.dataSizes));
            }
        }
        uint64_t size = ctx.size;
        ctx.senderId = cfg_.instanceId;
        if (pipelineNeedsData_) ctx.data = pregenData_[size];
        ctx.client = client_;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        ctx.param.ttlSecond = cfg_.ttlSeconds;
        // NX applies to cache mode only; pipeline mode always overwrites (NONE).
        ctx.param.existence = (cfg_.keyPoolSize > 0 && cfg_.setNx)
            ? ExistenceOpt::NX : ExistenceOpt::NONE;
        ctx.verifyFailCount = &metrics_.VerifyFailCounter();
        ctx.verifyCfg = BuildVerifyConfig(cfg_.verifyLevel, cfg_.verifySampleBytes,
                                          cfg_.verifySampleStepBytes, cfg_.verifyFailOp);
        // Cache-mode values carry an 8-byte version stamp; content verify skips it.
        if (cfg_.keyPoolSize > 0) ctx.verifyCfg.versionPrefixBytes = 8;
        ctx.metrics = &metrics_;
        ctx.cacheUpdateProb = cfg_.cacheUpdateProb;
        ctx.missBackfillProb = missBackfillProb_.load();

        bool allOk = ExecutePipeline(pipelineOps_, ctx, metrics_,
                                     metrics_.VerifyFailCounter(), cfg_.instanceId);

        // Pipeline mode keeps the legacy every-op notify. Cache mode no longer
        // broadcasts on backfill: the backfilled key was sampled from the shared
        // key pool and is already in every client's pool (expand already
        // broadcast it), so a backfill notify is redundant.
        if (allOk && cfg_.keyPoolSize == 0) {
            NotifyPeers(ctx.batchKeys, size, /*version=*/0);
        }
        // Cache update (re-Set on hit) broadcast: carry each updated key's new
        // version stamp to all peers. Skipped with set_nx (NX re-Set is a no-op,
        // value unchanged, so the new stamp never landed).
        if (allOk && cfg_.keyPoolSize > 0 && !cfg_.setNx) {
            for (size_t i = 0; i < ctx.batchUpdatedKeys.size(); i++) {
                NotifyPeers({ctx.batchUpdatedKeys[i]}, size, ctx.batchUpdatedVersions[i]);
            }
        }

        // Cache hit stale-read check (writer-side), one per hit key in the batch.
        if (cfg_.keyPoolSize > 0 && !cfg_.setNx) {
            for (size_t i = 0; i < ctx.batchHitKeys.size(); i++) {
                const std::string &key = ctx.batchHitKeys[i];
                uint64_t readVer = (i < ctx.batchHitVersions.size()) ? ctx.batchHitVersions[i] : 0;
                uint64_t updVer = 0;
                for (size_t j = 0; j < ctx.batchUpdatedKeys.size(); j++) {
                    if (ctx.batchUpdatedKeys[j] == key) {
                        updVer = ctx.batchUpdatedVersions[j];
                        break;
                    }
                }
                bool stale = false;
                uint64_t expectedVer = 0;
                {
                    std::lock_guard<kvtest::mutex> lock(versionMutex_);
                    auto vit = maxSeenVersion_.find(key);
                    if (vit != maxSeenVersion_.end()) expectedVer = vit->second;
                    stale = (expectedVer > 0) && (readVer < expectedVer);
                    uint64_t latest = std::max(readVer, updVer);
                    if (latest > expectedVer) {
                        if (vit != maxSeenVersion_.end()) vit->second = latest;
                        else maxSeenVersion_[key] = latest;
                    }
                }
                if (stale) {
                    metrics_.RecordCacheStale();
                    SLOG_WARN("cacheGetOrCreate stale read: key=" << key
                              << " expected=" << expectedVer
                              << " actual=" << readVer);
                }
            }
        }

        gate.Advance();
    }

    SLOG_INFO("Thread " << threadId << " stopped");
}

void KVWorker::NotifyPeers(const std::vector<std::string> &keys, uint64_t size, uint64_t version) {
    if (cfg_.peers.empty()) return;
    // Cache mode broadcasts to ALL peers (update-version and expand broadcasts
    // must reach every client); notify_count only caps the fan-out in pipeline
    // mode.
    if (cfg_.keyPoolSize == 0 && cfg_.notifyCount <= 0) return;

    // Skip the whole fan-out (shuffle, address parsing, one allocation per
    // target) when the notify pool is already at its bound; ThreadPool would
    // drop those tasks anyway. Counted separately from the pool's own drops
    // so the loss is attributable to the producer rather than the queue.
    if (cfg_.notifyQueueMax != 0 && notifyPool_.QueueSize() >= cfg_.notifyQueueMax) {
        notifySuppressed_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    int total = static_cast<int>(cfg_.peers.size());
    // Cache mode: fan out to every peer (no sampling); pipeline mode caps at
    // notify_count.
    int count = (cfg_.keyPoolSize > 0) ? total : std::min(cfg_.notifyCount, total);

    // Fisher-Yates partial shuffle (cache mode: count == total, full shuffle).
    std::vector<int> indices(total);
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937 rng(std::random_device{}());
    for (int i = 0; i < count; i++) {
        std::uniform_int_distribution<int> dist(i, total - 1);
        std::swap(indices[i], indices[dist(rng)]);
    }

    // Resolve peer addresses
    struct PeerAddr { std::string host; int port; };
    std::vector<PeerAddr> targets;
    targets.reserve(count);
    for (int i = 0; i < count; i++) {
        std::string hostPort = cfg_.peers[indices[i]];
        if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
            hostPort = hostPort.substr(7);
        }
        auto colonPos = hostPort.find(':');
        if (colonPos == std::string::npos) continue;
        try {
            targets.push_back({hostPort.substr(0, colonPos),
                              std::stoi(hostPort.substr(colonPos + 1))});
        } catch (...) { continue; }
    }

    int sender = cfg_.instanceId;
    // Capture a copy of the keys for the async tasks; the transport (httplib
    // JSON body or brpc NotifyReq) is built inside PeerControlClient.
    if (cfg_.notifyIntervalUs > 0) {
        // Sequential: offload to thread pool, notify one by one with interval.
        // In bazel mode the pool worker is a bthread — kvtest::sleep_for
        // yields it so the inter-notify gap doesn't hold a pthread worker.
        int intervalUs = cfg_.notifyIntervalUs;
        notifyPool_.Submit([this, targets = std::move(targets), keys, sender, size, intervalUs, version]() {
            for (size_t i = 0; i < targets.size(); i++) {
                if (i > 0) {
                    kvtest::sleep_for(std::chrono::microseconds(intervalUs));
                }
                peerClient_->Notify(targets[i].host, targets[i].port,
                                     /*action=*/"", sender, keys, size, version);
            }
        });
    } else {
        // Parallel: fire-and-forget via thread pool
        for (auto &t : targets) {
            notifyPool_.Submit([this, h = t.host, p = t.port, keys, sender, size, version]() {
                peerClient_->Notify(h, p, /*action=*/"", sender, keys, size, version);
            });
        }
    }
}

void KVWorker::NotifyWarmupDone(const std::vector<std::string> &warmupKeys) {
    if (cfg_.peers.empty()) return;

    struct PeerAddr { std::string host; int port; };
    std::vector<PeerAddr> targets;
    for (auto &peer : cfg_.peers) {
        std::string hostPort = peer;
        if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
            hostPort = hostPort.substr(7);
        }
        auto colonPos = hostPort.find(':');
        if (colonPos == std::string::npos) continue;
        try {
            targets.push_back({hostPort.substr(0, colonPos),
                              std::stoi(hostPort.substr(colonPos + 1))});
        } catch (...) { continue; }
    }
    if (targets.empty()) return;

    int sender = cfg_.instanceId;
    // warmup_done is critical: a reader that misses it waits warmup_timeout and
    // then exits (it has no other way to learn the warmup keys). Under startup
    // skew a writer may finish warmup before its readers are up, so retry the
    // broadcast until every reader acks, up to 100 attempts at 2s intervals.
    notifyPool_.Submit([this, targets = std::move(targets), warmupKeys, sender]() {
        constexpr int kMaxWarmupNotifyRetries = 100;
        constexpr int kWarmupNotifyRetryDelayMs = 2000;
        for (int attempt = 0; attempt < kMaxWarmupNotifyRetries; attempt++) {
            bool allAcked = true;
            for (const auto &t : targets) {
                if (!peerClient_->Notify(t.host, t.port, /*action=*/"warmup_done",
                                         sender, warmupKeys, /*size=*/0, /*version=*/0)) {
                    allAcked = false;
                }
            }
            if (allAcked) return;
            if (attempt + 1 < kMaxWarmupNotifyRetries) {
                kvtest::sleep_for(std::chrono::milliseconds(kWarmupNotifyRetryDelayMs));
            }
        }
        SLOG_WARN("warmup_done not acknowledged by all readers after "
                  << kMaxWarmupNotifyRetries << " retries, giving up");
    });
}

void KVWorker::OnUpdateVersion(const std::vector<std::string> &keys, uint64_t version) {
    std::lock_guard<kvtest::mutex> lock(versionMutex_);
    for (const auto &k : keys) {
        auto &v = maxSeenVersion_[k];
        if (version > v) v = version;
    }
}

size_t KVWorker::MaxSeenVersionSize() const {
    std::lock_guard<kvtest::mutex> lock(versionMutex_);
    return maxSeenVersion_.size();
}

size_t KVWorker::KeyPoolSize() const {
    std::shared_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
    return keyPool_.size();
}

void KVWorker::OnWarmupDone(int senderId, const std::vector<std::string> &warmupKeys) {
    (void)senderId;
    std::unique_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
    for (const auto &k : warmupKeys) {
        if (seenKeys_.insert(std::hash<std::string>{}(k)).second) {
            keyPool_.push_back(k);
            warmupKeyCount_++;
        }
    }
}

void KVWorker::OnEvictKeys(const std::vector<std::string> &keys) {
    const size_t maxPool = static_cast<size_t>(cfg_.maxKeyPoolSize);
    std::unique_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
    for (const auto &k : keys) {
        if (seenKeys_.insert(std::hash<std::string>{}(k)).second) {
            keyPool_.push_back(k);
        }
    }
    // Capacity control: trim the oldest eviction keys (after the warmup
    // segment) when the pool exceeds the budget.
    if (keyPool_.size() <= maxPool) return;
    if (warmupKeyCount_ > maxPool) return;  // keep all warmup keys
    size_t eraseCount = keyPool_.size() - maxPool;
    keyPool_.erase(keyPool_.begin() + warmupKeyCount_,
                   keyPool_.begin() + warmupKeyCount_ + eraseCount);
}

std::string KVWorker::RandomKey(std::mt19937 &rng) {
    std::shared_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
    if (keyPool_.empty()) return "";
    uint64_t idx = SamplePoolIndex(rng, static_cast<uint64_t>(keyPool_.size()),
                                   cfg_.hotKeyRatio, cfg_.hotAccessProb);
    return keyPool_[static_cast<size_t>(idx)];
}

void KVWorker::AdjustPoolSize() {
    if (cfg_.keyPoolSize <= 0) return;
    if (cfg_.targetHitRate <= 0.0 || cfg_.targetHitRate > 1.0) return;

    // Windowed hit rate (last flush interval), so the controller reacts to the
    // recent traffic instead of the cumulative total.
    double currentRate = metrics_.RecentHitRate();
    uint64_t poolSize = currentPoolSize_.load();
    const uint64_t maxSize = static_cast<uint64_t>(cfg_.maxKeyPoolSize);

    if (currentRate > cfg_.targetHitRate + 0.02) {
        // Too hot: grow the pool with brand-new (never-seen) keys. The hit rate
        // is driven down by expansion + TTL expiry + cache eviction: the grown
        // key set eventually exceeds the cache capacity, LRU eviction kicks in
        // and lowers the hit rate, so the pool does not expand forever. The pool
        // only grows — indices are never reused (backfill=1.0).
        missBackfillProb_.store(1.0);
        uint64_t newSize = poolSize + std::max<uint64_t>(1, poolSize / 20);
        if (newSize <= maxSize) {
            currentPoolSize_.store(newSize);
            // Materialize the new OWN keys into the shared pool and broadcast
            // them so every client can sample them (they stay absent until a
            // miss backfills them).
            std::vector<std::string> newKeys;
            {
                std::unique_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
                for (uint64_t idx = poolSize; idx < newSize; idx++) {
                    std::string k = "cache_pool_" + std::to_string(cfg_.instanceId)
                                  + "_" + std::to_string(idx);
                    if (seenKeys_.insert(std::hash<std::string>{}(k)).second) {
                        keyPool_.push_back(k);
                        newKeys.push_back(std::move(k));
                    }
                }
            }
            if (!newKeys.empty()) {
                NotifyPeers(newKeys, /*size=*/0, /*version=*/0);
            }
            SLOG_INFO("Pool expand: hit_rate=" << std::fixed << std::setprecision(3)
                      << currentRate << " > target=" << cfg_.targetHitRate
                      << " → pool " << poolSize << "→" << newSize
                      << " (+" << newKeys.size() << " keys, backfill=1.0)");
        } else {
            SLOG_INFO("Pool at cap: hit_rate=" << std::fixed << std::setprecision(3)
                      << currentRate << " > target=" << cfg_.targetHitRate
                      << ", pool=" << poolSize << " (max " << maxSize << ", backfill=1.0)");
        }
    } else if (currentRate < cfg_.targetHitRate - 0.02) {
        // Too cold: always backfill misses to warm the cache back up.
        // No pool shrink (indices are never reused).
        missBackfillProb_.store(1.0);
        SLOG_INFO("Pool warm: hit_rate=" << std::fixed << std::setprecision(3)
                  << currentRate << " < target=" << cfg_.targetHitRate
                  << " → backfill=1.0 (pool " << poolSize << ")");
    } else {
        missBackfillProb_.store(1.0);
        SLOG_INFO("Pool hold: hit_rate=" << std::fixed << std::setprecision(3)
                  << currentRate << " ~ target=" << cfg_.targetHitRate
                  << " (pool " << poolSize << ", backfill=1.0)");
     }
}

int KVWorker::ResampleRandomQps() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(cfg_.targetQpsMin, cfg_.targetQpsMax);
    return dist(rng);
}

void KVWorker::AdvanceStage() {
    // Random QPS mode: re-sample uniformly in [min, max] every interval.
    if (cfg_.targetQpsMin > 0 && cfg_.targetQpsMax > 0) {
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - stageStartTime_).count();
        if (elapsed >= cfg_.randomQpsIntervalSeconds) {
            int newQps = ResampleRandomQps();
            currentTargetQps_.store(newQps);
            stageStartTime_ = std::chrono::steady_clock::now();
            SLOG_INFO("Random QPS re-sampled: " << newQps
                      << " [" << cfg_.targetQpsMin << ", " << cfg_.targetQpsMax << "]");
        }
        return;
    }

    if (cfg_.targetQpsStages.empty() || cfg_.stageDurationSeconds <= 0) return;

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - stageStartTime_).count();

    int targetStage = static_cast<int>(elapsed / cfg_.stageDurationSeconds);
    int lastStage = static_cast<int>(cfg_.targetQpsStages.size()) - 1;
    if (targetStage > lastStage) {
        targetStage = lastStage;
    }

    if (targetStage != currentStageIndex_) {
        currentStageIndex_ = targetStage;
        int newQps = cfg_.targetQpsStages[targetStage];
        currentTargetQps_.store(newQps);
        SLOG_INFO("QPS stage " << targetStage << "/" << lastStage
                  << ": " << newQps << " QPS"
                  << (targetStage == lastStage ? " (final stage)" : ""));
    }
}
