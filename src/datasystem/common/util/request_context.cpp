/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Per-request context implementation using brpc bthread_key_t.
 *
 * Design: GetRequestContext() never returns nullptr.
 * - In brpc handlers: bthread_getspecific returns the pointer set by SetRequestContext().
 * - In ZMQ/pthread handlers: bthread_getspecific returns nullptr, fall back to a
 *   per-pthread static thread_local RequestContext.
 *
 * Callers simply write GetRequestContext()->xxx without any transport-mode checks.
 */

#include "datasystem/common/util/request_context.h"

#include <mutex>

#include <bthread/bthread.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/trace.h"

DS_DECLARE_bool(use_brpc);

namespace datasystem {

// Rate-limit the "no active ScopedRequestContext on brpc bthread" warning:
// background std::thread / async pool / client SDK paths hit this branch on
// every Trace::Instance() / GetWorkerTimeCost() / AccessTransportTracker call.
constexpr int K_MISSING_CONTEXT_WARN_INTERVAL = 1000;

static bthread_key_t g_requestContextKey = INVALID_BTHREAD_KEY;
static std::once_flag g_initFlag;

// Strong override of the weak symbol declared in trace.h. Returns the
// per-bthread Trace when a handler is active (ScopedRequestContext on the
// current bthread), or nullptr otherwise so Trace::Instance() can fall back to
// its thread_local instance. This keeps weak/strong semantics aligned (both may
// return nullptr) and prevents background std::thread / async thread pool / ZMQ
// handler paths from accidentally sharing the per-pthread fallback
// RequestContext's Trace across tasks (would let traceID/latencyTicks leak). We
// intentionally do NOT call GetRequestContext() here, because its NEVER-nullptr
// contract would always return a (fallback) Trace and make the nullptr signal
// impossible to express.
Trace* GetBthreadTrace()
{
    // ZMQ mode (usercode_in_pthread=true): handlers run on plain pthreads, not bthreads.
    // bthread_setspecific/getspecific on plain pthreads may route through different
    // keytable pools than the ScopedRequestContext registered on, causing
    // SetLatencySummary/AddLatencyTick to write to one Trace instance while
    // access_recorder reads another (data loss). Force thread_local fallback in
    // ZMQ mode to preserve master's behavior (Trace::Instance is per-pthread,
    // shared across the entire handler call chain on the same pthread).
    if (!FLAGS_use_brpc) {
        return nullptr;
    }
    if (g_requestContextKey == INVALID_BTHREAD_KEY) {
        return nullptr;  // InitRequestContext() not called (minimal test binary).
    }
    auto* ctx = static_cast<RequestContext*>(bthread_getspecific(g_requestContextKey));
    return ctx ? &ctx->trace : nullptr;
}

void InitRequestContext()
{
    std::call_once(g_initFlag, []() {
        // Pass nullptr as destructor: RequestContext instances are always
        // stack-allocated (via ScopedRequestContext), never heap-allocated.
        // The bthread_key stores a bare pointer for the lifetime of a single
        // request; ScopedRequestContext explicitly clears it via
        // SetRequestContext(nullptr) before the stack object goes out of scope.
        bthread_key_create(&g_requestContextKey, nullptr);
    });
}

void SetRequestContext(RequestContext* ctx)
{
    if (g_requestContextKey == INVALID_BTHREAD_KEY) {
        return;
    }
    bthread_setspecific(g_requestContextKey, ctx);
}

RequestContext* GetRequestContext(const char* file, int line)
{
    // brpc path: handler called SetRequestContext(), bthread_getspecific returns the pointer.
    if (g_requestContextKey != INVALID_BTHREAD_KEY) {
        auto* p = static_cast<RequestContext*>(bthread_getspecific(g_requestContextKey));
        if (p != nullptr) {
            return p;
        }
        // bthread_getspecific returned nullptr — no active ScopedRequestContext on this bthread.
        // This is expected on background std::thread / client SDK / async thread pool paths
        // (EvictionTask, AsyncSendManager::Sender, EtcdWatch, RocksStore::asyncThreadPool_,
        // client application pthreads, etc.) that never enter a brpc handler. Such paths
        // correctly fall through to the per-pthread fallback below.
        // Only log when it might actually indicate a missing ScopedRequestContext in a
        // brpc handler, and rate-limit to avoid log storms: every Trace::Instance() /
        // GetWorkerTimeCost() / AccessTransportTracker call on those threads would
        // otherwise emit one ERROR.
        if (FLAGS_use_brpc) {
            LOG_EVERY_N(WARNING, K_MISSING_CONTEXT_WARN_INTERVAL)
                << "GetRequestContext(): no active ScopedRequestContext on this bthread"
                << " (called from " << file << ":" << line << "). "
                << "Expected on background/client threads; for brpc handlers, declare "
                << "ScopedRequestContext as the first line of the handler.";
        }
        // ZMQ / test: fall through to thread_local fallback (safe under usercode_in_pthread=true).
    }
    // ZMQ / pthread / test path (InitRequestContext never called, or no active context):
    // use per-pthread fallback.  ZMQ uses usercode_in_pthread=true, so each handler
    // has a dedicated pthread and thread_local is safe.
    static thread_local RequestContext fallbackCtx;
    return &fallbackCtx;
}

}  // namespace datasystem
