#include "kv_worker.h"
#include "data_pattern.h"
#include <datasystem/utils/string_view.h>
#include "common/simple_log.h"
#include <chrono>
#include <iomanip>
#include <random>
#include <algorithm>
#include <numeric>

using namespace datasystem;

KVWorker::KVWorker(const Config &cfg, std::shared_ptr<KVClient> client,
                   MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics),
      currentPoolSize_(static_cast<uint64_t>(cfg.keyPoolSize)),
      currentTargetQps_(cfg.targetQps), notifyPool_(100),
      peerClient_(MakePeerControlClient()) {
    for (auto &name : cfg_.pipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown pipeline op: " << name << ", skipping");
            continue;
        }
        pipelineOps_.emplace_back(name, fn);
    }

    for (auto size : cfg_.dataSizes) {
        pregenData_[size] = GeneratePatternData(size, cfg_.instanceId);
    }
}

KVWorker::~KVWorker() { Stop(); }

void KVWorker::Start() {
    running_ = true;

    // Cache mode warmup phase
    if (cfg_.keyPoolSize > 0) {
        SLOG_INFO("Starting warmup: " << cfg_.keyPoolSize << " keys");

        SetParam param;
        param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        param.ttlSecond = cfg_.ttlSeconds;

        int warmupOk = 0, warmupFail = 0;
        std::vector<std::string> warmupKeys;
        warmupKeys.reserve(cfg_.keyPoolSize);

        for (int i = 0; i < cfg_.keyPoolSize; i++) {
            std::string key = "cache_pool_" + std::to_string(cfg_.instanceId)
                            + "_" + std::to_string(i);

            uint64_t size = cfg_.dataSizes[0];
            auto it = pregenData_.find(size);
            if (it == pregenData_.end()) {
                warmupFail++;
                continue;
            }
            const auto &data = it->second;

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
                warmupKeys.push_back(key);
            } else {
                warmupFail++;
                SLOG_WARN("Warmup Set failed permanently for key=" << key);
            }
        }

        SLOG_INFO("Warmup done: " << warmupOk << " ok, " << warmupFail << " fail");

        if (!warmupKeys.empty()) {
            NotifyWarmupDone(warmupKeys);
        }
    }

    SLOG_INFO("Starting " << cfg_.numThreads << " pipeline threads"
              << (cfg_.targetQps > 0 ? "" : " (unlimited QPS)")
              << (cfg_.targetQpsStages.empty() ? "" :
                  " [" + std::to_string(cfg_.targetQpsStages.size()) + " stages x "
                  + std::to_string(cfg_.stageDurationSeconds) + "s]"));
    if (!cfg_.targetQpsStages.empty()) {
        stageStartTime_ = std::chrono::steady_clock::now();
    }

    for (int i = 0; i < cfg_.numThreads; i++) {
        threads_.emplace_back(&KVWorker::PipelineLoop, this, i);
    }
}

void KVWorker::Stop() {
    running_ = false;
    for (auto &t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
    notifyPool_.Stop();
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

    int64_t maxOffset = (cfg_.enableJitter && intervalUs > 0) ? intervalUs : 0;
    std::uniform_int_distribution<int64_t> offsetDist(0, maxOffset > 0 ? maxOffset : 1);

    SLOG_INFO("Thread " << threadId << " started"
              << (intervalUs > 0 ? "" : " (unlimited)"));

    int64_t phaseUs = intervalUs > 0
        ? static_cast<int64_t>(
            static_cast<double>(threadId) / cfg_.numThreads * intervalUs)
        : 0;
    auto nextSlot = std::chrono::steady_clock::now()
                   + std::chrono::microseconds(phaseUs);

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
            maxOffset = (cfg_.enableJitter && intervalUs > 0) ? intervalUs : 0;
            offsetDist = std::uniform_int_distribution<int64_t>(0, maxOffset > 0 ? maxOffset : 1);
            nextSlot = std::chrono::steady_clock::now();
            SLOG_INFO("Thread " << threadId << " QPS → " << currentQps
                      << " (myQps=" << myQps << ", interval=" << intervalUs << "us)");
        }

        if (intervalUs > 0) {
            auto fireTime = nextSlot + std::chrono::microseconds(offsetDist(rng));
            auto now = std::chrono::steady_clock::now();
            if (fireTime > now) {
                // Yield the bthread (brpc mode) or sleep the std::thread
                // (cmake mode); kvtest::sleep_until picks the right primitive
                // so a long fire-time gap doesn't hold a pthread worker
                // when running on a bthread.
                kvtest::sleep_until(fireTime);
            }
        }

        uint64_t size = cfg_.dataSizes[sizeDist(rng)];

        PipelineContext ctx;
        ctx.batchKeys.resize(cfg_.batchKeysCount);

        if (cfg_.keyPoolSize > 0) {
            // Cache mode: hash-modulo keys from pool for reuse
            uint64_t poolSize = currentPoolSize_.load();
            if (poolSize > 0) {
                auto dist = std::uniform_int_distribution<uint64_t>(0, poolSize - 1);
                for (int i = 0; i < cfg_.batchKeysCount; i++) {
                    ctx.batchKeys[i] = "cache_pool_" + std::to_string(cfg_.instanceId)
                                     + "_" + std::to_string(dist(rng));
                }
            }
        } else {
            // Original: timestamp-based keys
            auto now = std::chrono::steady_clock::now().time_since_epoch().count();
            for (int i = 0; i < cfg_.batchKeysCount; i++) {
                ctx.batchKeys[i] = "kv_test_" + std::to_string(cfg_.instanceId)
                                 + "_" + std::to_string(threadId)
                                 + "_" + std::to_string(now) + "_" + std::to_string(i);
            }
        }
        ctx.key = ctx.batchKeys[0];
        ctx.size = size;
        ctx.senderId = cfg_.instanceId;
        ctx.data = pregenData_[size];
        ctx.client = client_;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        ctx.param.ttlSecond = cfg_.ttlSeconds;
        ctx.verifyFailCount = &metrics_.VerifyFailCounter();
        ctx.verifyCfg = BuildVerifyConfig(cfg_.verifyLevel, cfg_.verifySampleBytes,
                                          cfg_.verifySampleStepBytes, cfg_.verifyFailOp);
        ctx.metrics = &metrics_;

        bool allOk = ExecutePipeline(pipelineOps_, ctx, metrics_,
                                     metrics_.VerifyFailCounter());

        if (allOk) {
            NotifyPeers(ctx.batchKeys, size);
        }

        if (intervalUs > 0) {
            // Advance to next slot boundary
            nextSlot += std::chrono::microseconds(intervalUs);
            auto now2 = std::chrono::steady_clock::now();
            if (nextSlot <= now2) {
                nextSlot = now2;
            }
        }
    }

    SLOG_INFO("Thread " << threadId << " stopped");
}

void KVWorker::NotifyPeers(const std::vector<std::string> &keys, uint64_t size) {
    if (cfg_.peers.empty() || cfg_.notifyCount <= 0) return;

    int total = static_cast<int>(cfg_.peers.size());
    int count = std::min(cfg_.notifyCount, total);

    // Fisher-Yates partial shuffle
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
        notifyPool_.Submit([this, targets = std::move(targets), keys, sender, size, intervalUs]() {
            for (size_t i = 0; i < targets.size(); i++) {
                if (i > 0) {
                    kvtest::sleep_for(std::chrono::microseconds(intervalUs));
                }
                peerClient_->Notify(targets[i].host, targets[i].port,
                                     /*action=*/"", sender, keys, size);
            }
        });
    } else {
        // Parallel: fire-and-forget via thread pool
        for (auto &t : targets) {
            notifyPool_.Submit([this, h = t.host, p = t.port, keys, sender, size]() {
                peerClient_->Notify(h, p, /*action=*/"", sender, keys, size);
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

    int sender = cfg_.instanceId;
    for (auto &t : targets) {
        notifyPool_.Submit([this, h = t.host, p = t.port, warmupKeys, sender]() {
            peerClient_->Notify(h, p, /*action=*/"warmup_done", sender, warmupKeys, /*size=*/0);
        });
    }
}

void KVWorker::AdjustPoolSize() {
    if (cfg_.targetHitRate <= 0.0 || cfg_.targetHitRate > 1.0) return;

    double currentRate = metrics_.CacheHitRate();
    uint64_t poolSize = currentPoolSize_.load();
    uint64_t minSize = std::max<uint64_t>(10, cfg_.keyPoolSize / 10);
    uint64_t maxSize = static_cast<uint64_t>(cfg_.maxKeyPoolSize);

    if (currentRate > cfg_.targetHitRate + 0.02) {
        // Hit rate too high → increase pool to add pressure
        uint64_t newSize = poolSize + std::max<uint64_t>(1, poolSize / 20);
        if (newSize <= maxSize) {
            currentPoolSize_.store(newSize);
            SLOG_INFO("Pool adjust: hit_rate=" << std::fixed << std::setprecision(3)
                      << currentRate << " > target=" << cfg_.targetHitRate
                      << " → pool " << poolSize << "→" << newSize);
        }
    } else if (currentRate < cfg_.targetHitRate - 0.02) {
        // Hit rate too low → decrease pool to reduce pressure
        uint64_t newSize = poolSize - std::max<uint64_t>(1, poolSize / 20);
        if (newSize >= minSize) {
            currentPoolSize_.store(newSize);
            SLOG_INFO("Pool adjust: hit_rate=" << std::fixed << std::setprecision(3)
                      << currentRate << " < target=" << cfg_.targetHitRate
                      << " → pool " << poolSize << "→" << newSize);
        }
    }
}

void KVWorker::AdvanceStage() {
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
