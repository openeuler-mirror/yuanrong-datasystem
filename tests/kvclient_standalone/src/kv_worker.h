#pragma once

#include "config.h"
#include "metrics.h"
#include "pipeline.h"
#include "thread_pool.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>

class KVWorker {
public:
    KVWorker(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
             MetricsCollector &metrics);
    ~KVWorker();

    void Start();
    void Stop();

    size_t NotifyQueueSize() { return notifyPool_.QueueSize(); }

private:
    void PipelineLoop(int threadId);
    void NotifyPeers(const std::string &key, uint64_t size);

    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> threads_;
    ThreadPool notifyPool_;
    std::vector<std::pair<std::string, OpFunc>> pipelineOps_;
    std::unordered_map<uint64_t, std::string> pregenData_;
    int qpsPerThread_ = 0;
};
