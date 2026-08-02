#pragma once

#include "common/config.h"
#include "metrics/metrics.h"
#include "notify_dispatcher.h"
#include "vendor/httplib.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <memory>
#include <thread>

class CacheReader;

// httplib-backed control plane (cmake mode). The brpc mode (bazel) uses
// BrpcControlServer instead; both compose a NotifyDispatcher so the notify
// protocol semantics stay identical across build systems.
class HttpServer {
public:
    HttpServer(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
               MetricsCollector &metrics, std::atomic<bool> &running);
    ~HttpServer();

    void Start();
    void Stop();

    size_t NotifyQueueSize() { return dispatcher_.QueueSize(); }

    void SetCacheReader(CacheReader *reader) { dispatcher_.SetCacheReader(reader); }

private:
    void HandleNotify(const std::string &body);

    Config cfg_;
    std::atomic<bool> &running_;
    MetricsCollector &metrics_;
    NotifyDispatcher dispatcher_;
    std::unique_ptr<httplib::Server> server_;
    std::thread serverThread_;
};
