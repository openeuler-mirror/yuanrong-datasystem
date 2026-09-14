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
class KVWorker;

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

    // Drop queued notify work + stop the cache reader without joining, same
    // as the /stop handler does. Called from main's shutdown path when the
    // process was stopped via SIGTERM so the TERM path mirrors /stop (the
    // calls must run on the main thread: StopNow takes a mutex and is not
    // async-signal-safe, so the signal handler only flips an atomic).
    void StopNow() { dispatcher_.StopNow(); }

    size_t NotifyQueueSize() { return dispatcher_.QueueSize(); }
    uint64_t NotifyDroppedCount() { return dispatcher_.DroppedCount(); }

    void SetCacheReader(CacheReader *reader) { dispatcher_.SetCacheReader(reader); }

    // Inject the writer so /stop can flip its pipeline loop flag immediately
    // instead of waiting for main's shutdown delay. Mirrors SetCacheReader.
    void SetWorker(KVWorker *worker) { worker_ = worker; }

private:
    void HandleNotify(const std::string &body);

    Config cfg_;
    std::atomic<bool> &running_;
    MetricsCollector &metrics_;
    NotifyDispatcher dispatcher_;
    std::unique_ptr<httplib::Server> server_;
    std::thread serverThread_;
    KVWorker *worker_ = nullptr;
};
