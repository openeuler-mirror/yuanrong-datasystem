#include "cache_reader.h"
#include "data_pattern.h"
#include "pipeline.h"
#include "common/simple_log.h"
#include <datasystem/utils/string_view.h>
#include <algorithm>
#include <chrono>

using namespace datasystem;

CacheReader::CacheReader(const Config &cfg,
                         std::shared_ptr<KVClient> client,
                         MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics),
      maxKeyPoolSize_(static_cast<size_t>(cfg.maxKeyPoolSize)) {
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
        std::lock_guard<std::mutex> lock(warmupMutex_);
        if (pendingWarmupWriters_.empty()) {
            SLOG_WARN("CacheReader: no writers configured, warmup done immediately");
            warmupDone_ = true;
        }
        SLOG_INFO("CacheReader starting " << cfg_.numSetThreads << " reader threads"
                  << ", waiting for " << pendingWarmupWriters_.size() << " writers");
    }

    for (int i = 0; i < cfg_.numSetThreads; i++) {
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
}

void CacheReader::OnWarmupDone(int senderId, const std::vector<std::string> &warmupKeys) {
    {
        std::unique_lock<std::shared_mutex> lock(keyPoolMutex_);
        keyPool_.insert(keyPool_.end(), warmupKeys.begin(), warmupKeys.end());
    }
    size_t remaining = 0;
    {
        std::lock_guard<std::mutex> lock(warmupMutex_);
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
    std::unique_lock<std::shared_mutex> lock(keyPoolMutex_);
    size_t prevSize = keyPool_.size();
    keyPool_.insert(keyPool_.end(), keys.begin(), keys.end());
    // Capacity control: remove oldest eviction keys if over limit
    // Warmup keys are at [0, warmupKeyCount), evict keys after that
    if (keyPool_.size() > maxKeyPoolSize_ && prevSize >= static_cast<size_t>(cfg_.keyPoolSize)) {
        size_t excess = keyPool_.size() - maxKeyPoolSize_;
        size_t evictStart = static_cast<size_t>(cfg_.keyPoolSize);
        keyPool_.erase(keyPool_.begin() + evictStart,
                       keyPool_.begin() + evictStart + excess);
    }
}

std::string CacheReader::RandomKey(std::mt19937 &rng) {
    std::shared_lock<std::shared_mutex> lock(keyPoolMutex_);
    if (keyPool_.empty()) return "";
    auto dist = std::uniform_int_distribution<size_t>(0, keyPool_.size() - 1);
    return keyPool_[dist(rng)];
}

void CacheReader::ReaderLoop(int threadId) {
    // Per-thread independent RNG (matches KVWorker::PipelineLoop pattern)
    std::mt19937 rng(threadId + cfg_.instanceId * 1000);
    auto sizeDist = std::uniform_int_distribution<size_t>(0, cfg_.dataSizes.size() - 1);

    // Wait for warmup completion with timeout
    {
        std::unique_lock<std::mutex> lock(warmupMutex_);
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
    if (cfg_.targetQps > 0 && cfg_.numSetThreads > 0) {
        int base = cfg_.targetQps / cfg_.numSetThreads;
        int rem = cfg_.targetQps % cfg_.numSetThreads;
        int myQps = base + (threadId < rem ? 1 : 0);
        intervalUs = myQps > 0 ? 1000000 / myQps : 1000000000;
    }
    int64_t maxOffset = (cfg_.enableJitter && intervalUs > 0) ? intervalUs : 0;
    std::uniform_int_distribution<int64_t> offsetDist(0, maxOffset > 0 ? maxOffset : 1);

    int64_t phaseUs = intervalUs > 0
        ? static_cast<int64_t>(
            static_cast<double>(threadId) / cfg_.numSetThreads * intervalUs)
        : 0;
    auto nextSlot = std::chrono::steady_clock::now()
                   + std::chrono::microseconds(phaseUs);

    SLOG_INFO("Reader thread " << threadId << " started"
              << (intervalUs > 0 ? "" : " (unlimited)"));

    while (running_) {
        if (intervalUs > 0) {
            auto fireTime = nextSlot + std::chrono::microseconds(offsetDist(rng));
            auto now = std::chrono::steady_clock::now();
            if (fireTime > now) {
                std::this_thread::sleep_until(fireTime);
            }
        }

        std::string key = RandomKey(rng);
        if (key.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        uint64_t size = cfg_.dataSizes[sizeDist(rng)];
        CacheGetOrFill(key, size);

        if (intervalUs > 0) {
            nextSlot += std::chrono::microseconds(intervalUs);
            auto now2 = std::chrono::steady_clock::now();
            if (nextSlot <= now2) nextSlot = now2;
        }
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
    const auto &data = it->second;

    // Step 1: Get
    Optional<Buffer> optBuf;
    double getLatency = 0;
    bool hit = Measure([&]() {
        return client_->Get(key, optBuf);
    }, getLatency);

    if (hit && optBuf) {
        if (static_cast<uint64_t>(optBuf->GetSize()) != size) {
            SLOG_WARN("cacheGetOrFill size mismatch: key=" << key
                      << " expected=" << size << " got=" << optBuf->GetSize());
            metrics_.RecordVerifyFail();
        }
        metrics_.Record(kOpCacheGetOrFillHit, getLatency, true, size);
        metrics_.RecordCacheHit();
        return true;
    }

    // Step 2: Miss -> Exist
    double existLatency = 0;
    std::vector<bool> exists;
    bool existOk = Measure([&]() {
        return client_->Exist({key}, exists);
    }, existLatency);
    metrics_.Record(kOpCacheExist, existLatency, existOk, 0);

    // Step 3: Simulate inference (not counted in latency)
    std::this_thread::sleep_for(std::chrono::milliseconds(cfg_.inferenceDelayMs));

    // Step 4: Set backfill
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    param.ttlSecond = cfg_.ttlSeconds;
    double setLatency = 0;
    bool setOk = Measure([&]() {
        return client_->Set(key, StringView(data), param);
    }, setLatency);
    metrics_.Record(kOpCacheSetFill, setLatency, setOk, size);

    double missLatency = getLatency + existLatency + setLatency;
    metrics_.Record(kOpCacheGetOrFillMiss, missLatency, setOk, size);
    metrics_.RecordCacheMiss();
    return setOk;
}
