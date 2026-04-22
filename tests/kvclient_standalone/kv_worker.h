#pragma once

#include "config.h"
#include "metrics.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <memory>
#include <vector>
#include <thread>

class KVWorker {
public:
    KVWorker(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
             MetricsCollector &metrics);
    ~KVWorker();

    void Start();
    void Stop();

private:
    void SetLoop(int threadId);
    void NotifyPeers(const std::string &key, uint64_t size);
    std::string GenerateData(uint64_t size, int senderId);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;
};
