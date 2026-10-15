#include "cache_reader.h"
#include "data_pattern.h"
#include "pipeline.h"
#include "common/rate_gate.h"
#include "common/simple_log.h"
#include <datasystem/utils/string_view.h>
#include <algorithm>
#include <chrono>

using namespace datasystem;

CacheReader::CacheReader(const Config &cfg,
                         std::shared_ptr<KVClient> client,
                         MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics),
      verifyCfg_(BuildVerifyConfig(cfg.verifyLevel, cfg.verifySampleBytes,
                                   cfg.verifySampleStepBytes, cfg.verifyFailOp)),
      maxKeyPoolSize_(static_cast<size_t>(cfg.maxKeyPoolSize)),
      peerClient_(MakePeerControlClient()),
      notifyPool_(100, cfg.notifyQueueMax) {
    // Cache-mode values carry an 8-byte version stamp; content verify skips it.
    verifyCfg_.versionPrefixBytes = 8;
    // Pre-generate data for all sizes (read-only after constructor)
    for (auto size : cfg_.dataSizes) {
        pregenData_[size] = GeneratePatternData(size, cfg_.instanceId);
    }

    // Initialize pending warmup writers from nodes with role == "writer"
    for (auto &n : cfg_.nodes) {
        if (n.role == "writer") {
            pendingWarmupWriters_.insert(n.instanceId);
        }
    }
}

CacheReader::~CacheReader() { Stop(); }

void CacheReader::Start() {
    running_ = true;

    {
        std::lock_guard<kvtest::mutex> lock(warmupMutex_);
        if (pendingWarmupWriters_.empty()) {
            SLOG_WARN("CacheReader: no writers configured, warmup done immediately");
            warmupDone_ = true;
        }
        SLOG_INFO("CacheReader starting " << cfg_.numThreads << " reader threads"
                  << ", waiting for " << pendingWarmupWriters_.size() << " writers");
    }

    for (int i = 0; i < cfg_.numThreads; i++) {
        threads_.emplace_back(&CacheReader::ReaderLoop, this, i);
    }
}

void CacheReader::Stop() {
    running_ = false;
    warmupCv_.notify_all();
    for (auto &t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
    notifyPool_.Stop();
}

void CacheReader::OnWarmupDone(int senderId, const std::vector<std::string> &warmupKeys) {
    {
        std::unique_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
        for (const auto &k : warmupKeys) {
            if (seenKeys_.insert(std::hash<std::string>{}(k)).second) {
                keyPool_.push_back(k);
                warmupKeyCount_++;
            }
        }
    }
    size_t remaining = 0;
    {
        std::lock_guard<kvtest::mutex> lock(warmupMutex_);
        pendingWarmupWriters_.erase(senderId);
        if (pendingWarmupWriters_.empty()) {
            warmupDone_ = true;
        }
        remaining = pendingWarmupWriters_.size();
    }
    warmupCv_.notify_all();
    SLOG_INFO("CacheReader: warmup_done from writer " << senderId
              << ", keys=" << warmupKeys.size()
              << ", remaining_writers=" << remaining);
}

void CacheReader::OnEvictKeys(const std::vector<std::string> &keys) {
    std::unique_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
    for (const auto &k : keys) {
        if (seenKeys_.insert(std::hash<std::string>{}(k)).second) {
            keyPool_.push_back(k);
        }
    }
    // Capacity control: trim the oldest eviction keys (everything after the
    // warmup segment) when the pool exceeds the budget. Warmup keys span
    // [0, warmupKeyCount_) across ALL writers and are never trimmed.
    if (keyPool_.size() <= maxKeyPoolSize_) return;
    if (warmupKeyCount_ > maxKeyPoolSize_) {
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            SLOG_WARN("CacheReader: warmup keys (" << warmupKeyCount_
                      << ") exceed max_key_pool_size (" << maxKeyPoolSize_
                      << "); keeping all warmup keys, evict window disabled. "
                      << "Set max_key_pool_size >= writer_count * key_pool_size");
        }
        return;
    }
    size_t eraseCount = keyPool_.size() - maxKeyPoolSize_;
    keyPool_.erase(keyPool_.begin() + warmupKeyCount_,
                   keyPool_.begin() + warmupKeyCount_ + eraseCount);
}

void CacheReader::OnUpdateVersion(const std::vector<std::string> &keys, uint64_t version) {
    {
        std::lock_guard<kvtest::mutex> lock(versionMutex_);
        for (const auto &k : keys) {
            auto &v = maxSeenVersion_[k];
            if (version > v) v = version;
        }
    }
    // Also ensure the keys are in the pool (dedup + capacity control).
    OnEvictKeys(keys);
}

void CacheReader::BroadcastUpdate(const std::vector<std::string> &keys, uint64_t version) {
    if (cfg_.peers.empty() || version == 0) return;
    notifyPool_.Submit([this, keys, version]() {
        for (const auto &peer : cfg_.peers) {
            std::string hostPort = peer;
            if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
                hostPort = hostPort.substr(7);
            }
            auto colonPos = hostPort.find(':');
            if (colonPos == std::string::npos) continue;
            std::string host = hostPort.substr(0, colonPos);
            int port = 0;
            try {
                port = std::stoi(hostPort.substr(colonPos + 1));
            } catch (...) {
                continue;
            }
            peerClient_->Notify(host, port, /*action=*/"", cfg_.instanceId, keys,
                                /*size=*/0, version);
        }
    });
}

std::string CacheReader::RandomKey(std::mt19937 &rng) {
    std::shared_lock<kvtest::shared_mutex> lock(keyPoolMutex_);
    if (keyPool_.empty()) return "";
    uint64_t idx = SamplePoolIndex(rng, static_cast<uint64_t>(keyPool_.size()),
                                   cfg_.hotKeyRatio, cfg_.hotAccessProb);
    return keyPool_[static_cast<size_t>(idx)];
}

void CacheReader::ReaderLoop(int threadId) {
    // Per-thread independent RNG (matches KVWorker::PipelineLoop pattern)
    std::mt19937 rng(threadId + cfg_.instanceId * 1000);

    // Wait for warmup completion with timeout
    {
        std::unique_lock<kvtest::mutex> lock(warmupMutex_);
        if (!warmupCv_.wait_for(lock,
                std::chrono::seconds(cfg_.warmupTimeoutSeconds),
                [this] { return warmupDone_.load() || !running_.load(); })) {
            SLOG_ERROR("Thread " << threadId << ": warmup timeout after "
                       << cfg_.warmupTimeoutSeconds << "s, exiting");
            return;
        }
    }
    if (!running_) return;

    // QPS control (same logic as KVWorker::PipelineLoop)
    int64_t intervalUs = 0;
    if (cfg_.targetQps > 0 && cfg_.numThreads > 0) {
        int base = cfg_.targetQps / cfg_.numThreads;
        int rem = cfg_.targetQps % cfg_.numThreads;
        int myQps = base + (threadId < rem ? 1 : 0);
        intervalUs = myQps > 0 ? 1000000 / myQps : 1000000000;
    }
    kvtest::RateGate gate;
    gate.Configure(intervalUs, threadId, cfg_.numThreads, cfg_.enableJitter);
    gate.ResetGrid(kvtest::SteadyNowUs());

    SLOG_INFO("Reader thread " << threadId << " started"
              << (intervalUs > 0 ? "" : " (unlimited)"));

    while (running_) {
        if (gate.Active()) {
            int64_t nowUs = kvtest::SteadyNowUs();
            int64_t fireUs = gate.NextFireUs(nowUs, rng);
            if (fireUs > nowUs) {
                // Yield the bthread (brpc mode) or sleep the std::thread
                // (cmake mode); kvtest::sleep_until picks the right primitive
                // so the QPS-rate idle doesn't hold a pthread worker.
                kvtest::sleep_until(kvtest::SteadyFromUs(fireUs));
            }
        }

        std::string key = RandomKey(rng);
        if (key.empty()) {
            kvtest::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        if (!IsKvtestClientInitialized()) {
            kvtest::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        // Deterministic per-key size (idx % sizes) — matches what the writer
        // used for this key, so verify never sees a size mismatch.
        uint64_t size = CacheKeySize(ParseCacheKeyIdx(key), cfg_.dataSizes);
        CacheGetOrFill(key, size);

        gate.Advance();
    }

    SLOG_INFO("Reader thread " << threadId << " stopped");
}

bool CacheReader::CacheGetOrFill(const std::string &key, uint64_t size) {
    // pregenData_ is read-only after constructor, no lock needed
    auto it = pregenData_.find(size);
    if (it == pregenData_.end()) {
        SLOG_WARN("CacheGetOrFill: size " << size << " not in pregenData_");
        return false;
    }

    // Step 1: Get
    Optional<Buffer> optBuf;
    double getLatency = 0;
    Status hitRc = Measure([&]() {
        return client_->Get(key, optBuf);
    }, getLatency);

    if (hitRc.IsOk() && optBuf) {
        // Verify the cached payload. A corrupted hit still counts as a cache
        // hit (key was present); failOp governs op success only.
        VerifyFailReason reason = VerifyFailReason::NONE;
        bool vok = VerifyBuffer(optBuf->ImmutableData(),
                                static_cast<uint64_t>(optBuf->GetSize()),
                                size, /*senderId=*/0, verifyCfg_, &reason);
        if (!vok) {
            if (reason == VerifyFailReason::SIZE) {
                SLOG_WARN("cacheGetOrFill size mismatch on hit: key=" << key
                          << " expected=" << size
                          << " got=" << optBuf->GetSize());
            } else {
                SLOG_WARN("cacheGetOrFill content mismatch on hit: key=" << key
                          << " level=" << static_cast<int>(verifyCfg_.level)
                          << " senderId=" << cfg_.instanceId);
            }
            metrics_.RecordVerifyFail();
        }
        metrics_.Record(kOpCacheGetOrFillHit, getLatency, hitRc.GetCode(), size);
        metrics_.RecordCacheHit();

        uint64_t actualVersion = ReadVersion(optBuf->ImmutableData(),
                                             static_cast<uint64_t>(optBuf->GetSize()));

        // Cache-update-on-hit: with probability cacheUpdateProb, re-Set a
        // timestamped value (overwrite path). Disabled by default (prob 0).
        uint64_t newVersion = 0;
        if (cfg_.cacheUpdateProb > 0.0) {
            static thread_local std::mt19937 updateRng(std::random_device{}());
            static thread_local std::uniform_real_distribution<double> updateDist(0.0, 1.0);
            if (updateDist(updateRng) < cfg_.cacheUpdateProb) {
                newVersion = NowNanos();
                std::string newData = GenerateVersionedData(size, newVersion);
                SetParam param;
                param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
                param.ttlSecond = cfg_.ttlSeconds;
                param.existence = cfg_.setNx ? ExistenceOpt::NX : ExistenceOpt::NONE;
                double setLatency = 0;
                Status setRc = Measure([&]() {
                    return client_->Set(key, StringView(newData), param);
                }, setLatency);
                metrics_.Record(kOpCacheSetUpdate, setLatency, setRc.GetCode(), size);
                // A failed Set must not advance the tracked version nor be
                // broadcast — readers would expect a stamp that isn't there.
                if (!setRc.IsOk()) newVersion = 0;
            }
        }

        // Version monotonicity check: a read-back version older than the newest
        // we've seen (from reads or broadcasts) is a stale read. Skipped with
        // set_nx (the value is write-once; its stamp is the first writer's).
        if (!cfg_.setNx) {
            bool stale = false;
            uint64_t expectedVer = 0;
            {
                std::lock_guard<kvtest::mutex> lock(versionMutex_);
                auto vit = maxSeenVersion_.find(key);
                if (vit != maxSeenVersion_.end()) expectedVer = vit->second;
                stale = (expectedVer > 0) && (actualVersion < expectedVer);
                uint64_t latest = std::max(actualVersion, newVersion);
                if (latest > expectedVer) {
                    if (vit != maxSeenVersion_.end()) vit->second = latest;
                    else maxSeenVersion_[key] = latest;
                }
            }
            if (stale) {
                metrics_.RecordCacheStale();
                SLOG_WARN("cacheGetOrFill stale read: key=" << key
                          << " expected=" << expectedVer
                          << " actual=" << actualVersion);
            }
        }

        if (!cfg_.setNx && newVersion > 0) {
            BroadcastUpdate({key}, newVersion);
        }

        if (!vok && verifyCfg_.failOp) return false;
        return true;
    }

    if (hitRc.GetCode() != StatusCode::K_NOT_FOUND) {
        // Get error (timeout / connection / etc.): not a cache miss, so do not
        // backfill. Count separately and warn throttled (once per 1000).
        metrics_.RecordCacheGetError();
        static thread_local uint64_t getErrThrottle = 0;
        if (++getErrThrottle % 1000 == 1) {
            SLOG_WARN("cacheGetOrFill Get error: key=" << key
                      << " code=" << hitRc.GetCode() << " msg=" << hitRc.GetMsg());
        }
        return false;
    }

    // Step 2: Miss -> Exist
    double existLatency = 0;
    std::vector<bool> exists;
    Status existRc = Measure([&]() {
        return client_->Exist({key}, exists);
    }, existLatency);
    metrics_.Record(kOpCacheExist, existLatency, existRc.GetCode(), 0);

    // Step 3: Simulate inference (not counted in latency). Same primitive
    // selection as the reader loop — yields the bthread in brpc mode.
    kvtest::sleep_for(std::chrono::milliseconds(cfg_.inferenceDelayMs));

    // Step 4: Set backfill (timestamped value so version monotonicity holds)
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    param.ttlSecond = cfg_.ttlSeconds;
    param.existence = cfg_.setNx ? ExistenceOpt::NX : ExistenceOpt::NONE;
    std::string fillData = GenerateVersionedData(size, NowNanos());
    double setLatency = 0;
    Status setRc = Measure([&]() {
        return client_->Set(key, StringView(fillData), param);
    }, setLatency);
    metrics_.Record(kOpCacheSetFill, setLatency, setRc.GetCode(), size);

    double missLatency = getLatency + existLatency + setLatency;
    metrics_.Record(kOpCacheGetOrFillMiss, missLatency, setRc.GetCode(), size);
    metrics_.RecordCacheMiss();
    return setRc.IsOk();
}
