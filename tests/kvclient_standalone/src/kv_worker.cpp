#include "kv_worker.h"
#include "data_pattern.h"
#include "httplib.h"
#include <datasystem/utils/string_view.h>
#include "simple_log.h"
#include <chrono>
#include <random>
#include <algorithm>
#include <numeric>

using namespace datasystem;

KVWorker::KVWorker(const Config &cfg, std::shared_ptr<KVClient> client,
                   MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics), notifyPool_(100) {
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
    qpsPerThread_ = (cfg_.targetQps > 0 && cfg_.numSetThreads > 0)
                    ? cfg_.targetQps / cfg_.numSetThreads : 0;
    if (qpsPerThread_ <= 0) qpsPerThread_ = 0;

    SLOG_INFO("Starting " << cfg_.numSetThreads << " pipeline threads"
              << (qpsPerThread_ > 0 ? "" : " (unlimited QPS)"));

    for (int i = 0; i < cfg_.numSetThreads; i++) {
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
    double intervalMs = qpsPerThread_ > 0 ? 1000.0 / qpsPerThread_ : 0;

    std::mt19937 rng(threadId + cfg_.instanceId * 1000);
    if (cfg_.dataSizes.empty()) {
        SLOG_WARN("Thread " << threadId << ": no data sizes configured, exiting");
        return;
    }
    auto sizeDist = std::uniform_int_distribution<size_t>(0, cfg_.dataSizes.size() - 1);

    SLOG_INFO("Thread " << threadId << " started"
              << (qpsPerThread_ > 0 ? "" : " (unlimited)"));

    while (running_) {
        uint64_t size = cfg_.dataSizes[sizeDist(rng)];
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();

        PipelineContext ctx;
        ctx.batchKeys.resize(cfg_.batchKeysCount);
        for (int i = 0; i < cfg_.batchKeysCount; i++) {
            ctx.batchKeys[i] = "kv_test_" + std::to_string(cfg_.instanceId)
                             + "_" + std::to_string(threadId)
                             + "_" + std::to_string(now) + "_" + std::to_string(i);
        }
        ctx.key = ctx.batchKeys[0];
        ctx.size = size;
        ctx.senderId = cfg_.instanceId;
        ctx.data = pregenData_[size];
        ctx.client = client_;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE;
        ctx.param.ttlSecond = cfg_.ttlSeconds;
        ctx.verifyFailCount = &metrics_.VerifyFailCounter();

        auto start = std::chrono::steady_clock::now();
        bool allOk = ExecutePipeline(pipelineOps_, ctx, metrics_,
                                     metrics_.VerifyFailCounter());
        auto end = std::chrono::steady_clock::now();
        double elapsedMs = std::chrono::duration<double, std::milli>(end - start).count();

        if (allOk) {
            NotifyPeers(ctx.batchKeys, size);
        }

        double sleepMs = intervalMs - elapsedMs;
        if (sleepMs > 0) {
            if (cfg_.enableJitter) {
                std::uniform_real_distribution<double> dist(0, 2.0 * sleepMs);
                sleepMs = dist(rng);
            }
            std::this_thread::sleep_for(
                std::chrono::microseconds(static_cast<int64_t>(sleepMs * 1000)));
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

    // JSON body: {"keys":["...","..."],"sender":0,"size":8388608}
    std::string body = "{\"keys\":[";
    for (size_t i = 0; i < keys.size(); i++) {
        if (i > 0) body += ",";
        body += "\"" + keys[i] + "\"";
    }
    body += "],\"sender\":" + std::to_string(cfg_.instanceId)
          + ",\"size\":" + std::to_string(size) + "}";

    for (int i = 0; i < count; i++) {
        std::string hostPort = cfg_.peers[indices[i]];
        if (hostPort.size() > 7 && hostPort.compare(0, 7, "http://") == 0) {
            hostPort = hostPort.substr(7);
        }

        auto colonPos = hostPort.find(':');
        if (colonPos == std::string::npos) continue;

        std::string host;
        int port;
        try {
            host = hostPort.substr(0, colonPos);
            port = std::stoi(hostPort.substr(colonPos + 1));
        } catch (...) {
            continue;
        }

        notifyPool_.Submit([host, port, body]() {
            try {
                thread_local std::unordered_map<std::string,
                    std::unique_ptr<httplib::Client>> clientCache;
                std::string ckey = host + ":" + std::to_string(port);
                auto &ref = clientCache[ckey];
                if (!ref) {
                    ref = std::make_unique<httplib::Client>(host, port);
                    ref->set_connection_timeout(2);
                    ref->set_read_timeout(2);
                }
                ref->Post("/notify", body, "application/json");
            } catch (...) {}
        });
    }
}
