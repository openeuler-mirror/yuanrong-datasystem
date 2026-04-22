#pragma once

#include "config.h"
#include "metrics.h"
#include "httplib.h"
#include <datasystem/kv_client.h>
#include <memory>
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>

class HttpServer {
public:
    HttpServer(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
               MetricsCollector &metrics, std::atomic<bool> &running);
    ~HttpServer();

    void Start();
    void Stop();

private:
    void HandleNotify(const std::string &body);
    std::string GenerateExpectedData(uint64_t size, int senderId);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::atomic<bool> &running_;
    std::unique_ptr<httplib::Server> server_;
    std::thread serverThread_;
    std::vector<std::thread> notifyThreads_;
    std::mutex notifyMutex_;
};
