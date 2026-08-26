#pragma once

#include "common/config.h"
#include "metrics/metrics.h"
#include "notify_dispatcher.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <memory>

#include <google/protobuf/service.h>

class CacheReader;
class KVWorker;

// brpc-backed control plane (bazel mode, KVTEST_USE_BRPC). Mirrors the
// HttpServer interface (Start/Stop/NotifyQueueSize/SetCacheReader/SetWorker) so
// main.cpp can select between them with a single #ifdef. Composes a
// NotifyDispatcher so the notify protocol semantics are identical to the
// httplib path.
class BrpcControlServer {
public:
    BrpcControlServer(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
                      MetricsCollector &metrics, std::atomic<bool> &running);
    ~BrpcControlServer();

    void Start();
    void Stop();

    size_t NotifyQueueSize() { return dispatcher_.QueueSize(); }

    void SetCacheReader(CacheReader *reader) { dispatcher_.SetCacheReader(reader); }

    // Inject the writer so the Stop RPC can flip its pipeline loop flag
    // immediately instead of waiting for main's shutdown delay.
    void SetWorker(KVWorker *worker);

private:
    Config cfg_;
    std::atomic<bool> &running_;
    MetricsCollector &metrics_;
    NotifyDispatcher dispatcher_;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
