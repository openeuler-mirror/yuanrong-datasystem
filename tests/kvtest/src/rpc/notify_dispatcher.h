#pragma once

#include "common/bthread_compat.h"
#include "common/config.h"
#include "common/thread_pool.h"
#include "metrics/metrics.h"
#include "pipeline/pipeline.h"
#include <datasystem/kv_client.h>
#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

class CacheReader;

// NotifyDispatcher owns the former httplib POST /notify handling logic:
// dispatching warmup_done / evict / normal notify_pipeline actions to the
// cache reader or the async notify pool. Extracted so both the httplib
// control server (cmake mode) and the brpc control server (bazel mode) share
// a single source of truth for the notify protocol semantics.
//
// Threading contract: DispatchNotify may submit work to an internal
// ThreadPool; callers (httplib handler thread, brpc bthread) must not hold
// the request lock while dispatching. The pool is bounded (100) and stopped
// explicitly via Stop() on server shutdown.
class NotifyDispatcher {
public:
    NotifyDispatcher(const Config &cfg, std::shared_ptr<datasystem::KVClient> client,
                     MetricsCollector &metrics);

    void SetCacheReader(CacheReader *reader) { cacheReader_ = reader; }

    // Dispatch a parsed notify request. action="warmup_done" takes the cache
    // warmup path; an empty action takes the normal notify_pipeline path.
    // keys/size semantics mirror the former httplib /notify JSON body.
    void DispatchNotify(const std::string &action, int sender,
                        const std::vector<std::string> &keys, uint64_t size);

    size_t QueueSize() { return notifyPool_.QueueSize(); }

    // Stop the internal notify pool. Must be called on server shutdown so
    // in-flight notify tasks drain before the client/metrics are torn down.
    void Stop() { notifyPool_.Stop(); }

    // Stop without draining: drop queued notify-pipeline tasks so no new Get
    // requests are issued from received notifies, and signal the cache reader
    // (if any) to stop its loops. Called from /stop so the control thread
    // replies at once; joins happen in Stop() on shutdown.
    void StopNow();

private:
    Config cfg_;
    std::shared_ptr<datasystem::KVClient> client_;
    MetricsCollector &metrics_;
    ThreadPool notifyPool_;
    std::vector<std::pair<std::string, OpFunc>> notifyOps_;
    bool notifyNeedsData_ = false;
    // Protects pregenData_ from concurrent notify-pipeline tasks. Acquired
    // inside notifyPool_ workers which are bthreads in bazel mode, so use
    // kvtest::mutex (bthread::Mutex) to avoid blocking a pthread on contention.
    kvtest::mutex pregenMutex_;
    std::unordered_map<std::string, std::string> pregenData_;
    CacheReader *cacheReader_ = nullptr;
};
