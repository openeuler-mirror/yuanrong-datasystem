#include "kv_worker.h"
#include "data_pattern.h"
#include "httplib.h"
#include <datasystem/utils/string_view.h>
#include "simple_log.h"
#include <chrono>
#include <random>
#include <algorithm>

using namespace datasystem;

KVWorker::KVWorker(const Config &cfg, std::shared_ptr<KVClient> client,
                   MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics) {
    // Build pipeline ops from config
    for (auto &name : cfg_.pipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown pipeline op: " << name << ", skipping");
            continue;
        }
        pipelineOps_.emplace_back(name, fn);
    }
}

KVWorker::~KVWorker() { Stop(); }

void KVWorker::Start() {
    running_ = true;
    int qpsPerThread = cfg_.targetQps / cfg_.numSetThreads;
    if (qpsPerThread <= 0) qpsPerThread = 1;

    SLOG_INFO("Starting " << cfg_.numSetThreads << " pipeline threads, "
              << qpsPerThread << " QPS each (total target: " << cfg_.targetQps << ")");

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
}

void KVWorker::PipelineLoop(int threadId) {
    int qpsPerThread = cfg_.targetQps / cfg_.numSetThreads;
    if (qpsPerThread <= 0) qpsPerThread = 1;
    double intervalMs = 1000.0 / qpsPerThread;

    std::mt19937 rng(threadId + cfg_.instanceId * 1000);
    auto sizeDist = std::uniform_int_distribution<size_t>(0, cfg_.dataSizes.size() - 1);

    SLOG_INFO("Thread " << threadId << " started: " << qpsPerThread << " QPS");

    while (running_) {
        uint64_t size = cfg_.dataSizes[sizeDist(rng)];
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::string key = "kv_test_" + std::to_string(cfg_.instanceId)
                        + "_" + std::to_string(threadId)
                        + "_" + std::to_string(now);

        PipelineContext ctx;
        ctx.key = key;
        ctx.size = size;
        ctx.senderId = cfg_.instanceId;
        ctx.data = GeneratePatternData(size, cfg_.instanceId);
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
            NotifyPeers(key, size);
        }

        double sleepMs = intervalMs - elapsedMs;
        if (sleepMs > 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(static_cast<int64_t>(sleepMs * 1000)));
        }
    }

    SLOG_INFO("Thread " << threadId << " stopped");
}

void KVWorker::NotifyPeers(const std::string &key, uint64_t size) {
    if (cfg_.peers.empty() || cfg_.notifyCount <= 0) return;

    std::vector<std::string> candidates = cfg_.peers;
    std::mt19937 rng(std::random_device{}());
    std::shuffle(candidates.begin(), candidates.end(), rng);

    int count = std::min(cfg_.notifyCount, static_cast<int>(candidates.size()));

    std::string body = "{\"key\":\"" + key + "\",\"sender\":"
                     + std::to_string(cfg_.instanceId) + ",\"size\":"
                     + std::to_string(size) + "}";

    for (int i = 0; i < count; i++) {
        std::string hostPort = candidates[i];
        if (hostPort.substr(0, 7) == "http://") hostPort = hostPort.substr(7);

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

        try {
            httplib::Client cli(host, port);
            cli.set_connection_timeout(2);
            cli.set_read_timeout(2);
            cli.Post("/notify", body, "application/json");
        } catch (...) {}
    }
}
