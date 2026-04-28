#pragma once

#include "config.h"
#include "metrics.h"
#include "pipeline.h"
#include "thread_pool.h"
#include "httplib.h"
#include <datasystem/kv_client.h>
#include <memory>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <mutex>

class HttpServer {
public:
    HttpServer(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
               MetricsCollector &metrics, std::atomic<bool> &running);
    ~HttpServer();

    void Start();
    void Stop();

    size_t NotifyQueueSize() { return notifyPool_.QueueSize(); }

private:
    void HandleNotify(const std::string &body);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::atomic<bool> &running_;
    std::unique_ptr<httplib::Server> server_;
    std::thread serverThread_;
    ThreadPool notifyPool_;
    std::vector<std::pair<std::string, OpFunc>> notifyOps_;
    bool notifyNeedsData_ = false;
    std::mutex pregenMutex_;
    std::unordered_map<std::string, std::string> pregenData_;
};
