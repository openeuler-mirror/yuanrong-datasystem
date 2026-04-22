#include "kv_worker.h"
#include "data_pattern.h"
#include "httplib.h"
#include <datasystem/utils/string_view.h>
#include <spdlog/spdlog.h>
#include <chrono>
#include <random>
#include <algorithm>

using namespace datasystem;

KVWorker::KVWorker(const Config &cfg, std::shared_ptr<KVClient> client,
                   MetricsCollector &metrics)
    : cfg_(cfg), client_(client), metrics_(metrics) {}

KVWorker::~KVWorker() { Stop(); }

void KVWorker::Start() {
    running_ = true;
    int qpsPerThread = cfg_.targetQps / cfg_.numSetThreads;
    if (qpsPerThread <= 0) qpsPerThread = 1;

    spdlog::info("Starting {} set threads, {} QPS each (total target: {})",
                 cfg_.numSetThreads, qpsPerThread, cfg_.targetQps);

    for (int i = 0; i < cfg_.numSetThreads; i++) {
        threads_.emplace_back(&KVWorker::SetLoop, this, i);
    }
}

void KVWorker::Stop() {
    running_ = false;
    for (auto &t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
}

std::string KVWorker::GenerateData(uint64_t size, int senderId) {
    return GeneratePatternData(size, senderId);
}

void KVWorker::SetLoop(int threadId) {
    int qpsPerThread = cfg_.targetQps / cfg_.numSetThreads;
    if (qpsPerThread <= 0) qpsPerThread = 1;
    double intervalMs = 1000.0 / qpsPerThread;

    std::mt19937 rng(threadId + cfg_.instanceId * 1000);
    auto sizeDist = std::uniform_int_distribution<size_t>(0, cfg_.dataSizes.size() - 1);

    spdlog::info("Thread {} started: {} QPS, interval {:.1f}ms", threadId, qpsPerThread, intervalMs);

    while (running_) {
        uint64_t size = cfg_.dataSizes[sizeDist(rng)];

        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::string key = "kv_test_" + std::to_string(cfg_.instanceId)
                        + "_" + std::to_string(threadId)
                        + "_" + std::to_string(now);

        std::string data = GenerateData(size, cfg_.instanceId);

        SetParam param;
        param.writeMode = WriteMode::NONE_L2_CACHE;
        param.ttlSecond = cfg_.ttlSeconds;

        auto start = std::chrono::steady_clock::now();
        Status rc = client_->Set(key, StringView(data), param);
        auto end = std::chrono::steady_clock::now();
        double latencyMs = std::chrono::duration<double, std::milli>(end - start).count();

        metrics_.Record("set", latencyMs, rc.IsOk());

        if (!rc.IsOk()) {
            spdlog::warn("Set failed: key={}, error={}", key, rc.GetMsg());
        }

        if (rc.IsOk()) {
            NotifyPeers(key, size);
        }

        double elapsedMs = latencyMs;
        double sleepMs = intervalMs - elapsedMs;
        if (sleepMs > 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(static_cast<int64_t>(sleepMs * 1000)));
        }
    }

    spdlog::info("Thread {} stopped", threadId);
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

        std::thread([host, port, body]() {
            try {
                httplib::Client cli(host, port);
                cli.set_connection_timeout(2);
                cli.set_read_timeout(2);
                cli.Post("/notify", body, "application/json");
            } catch (...) {}
        }).detach();
    }
}
