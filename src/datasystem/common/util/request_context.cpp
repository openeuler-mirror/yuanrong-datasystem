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
 * - In pthread handlers / background threads: bthread_getspecific returns nullptr,
 *   fall back to a per-pthread static thread_local RequestContext.
 *
 * Callers simply write GetRequestContext()->xxx without any transport-mode checks.
 */

#include "datasystem/common/util/request_context.h"

#include <cstdio>
#include <cstdlib>
#include <mutex>

#include <bthread/bthread.h>

#include "datasystem/common/log/trace.h"

namespace datasystem {

// Rate-limit the "no active ScopedRequestContext on brpc bthread" warning:
// background std::thread / async pool / client SDK paths hit this branch on
// every Trace::Instance() / GetWorkerTimeCost() / AccessTransportTracker call.
constexpr int K_MISSING_CONTEXT_WARN_INTERVAL = 1000;

static bthread_key_t g_requestContextKey = INVALID_BTHREAD_KEY;
static bthread_key_t g_clientAccessTransportKey = INVALID_BTHREAD_KEY;
static std::once_flag g_initFlag;

constexpr uint8_t K_ACCESS_TRANSPORT_KIND_COUNT = 4;
constexpr uint8_t K_UNKNOWN_ACCESS_TRANSPORT_KIND = 3;
static const AccessTransportKind g_clientAccessTransportKinds[K_ACCESS_TRANSPORT_KIND_COUNT] = {
    static_cast<AccessTransportKind>(0), static_cast<AccessTransportKind>(1), static_cast<AccessTransportKind>(2),
    static_cast<AccessTransportKind>(3)
};

static void AbortOnBthreadKeyError(int rc, const char *operation)
{
    if (rc == 0) {
        return;
    }
    std::fprintf(stderr, "FATAL: %s failed with rc=%d for client access transport context.\n", operation, rc);
    std::abort();
}

static RequestContext* GetFallbackRequestContext()
{
    static thread_local RequestContext fallbackCtx;
    fallbackCtx.isFallbackContext = true;
    return &fallbackCtx;
}

// Strong override of the weak symbol declared in trace.h. Returns the
// per-bthread Trace when a handler is active (ScopedRequestContext on the
// current bthread), or nullptr otherwise so Trace::Instance() can fall back to
// its thread_local instance. This keeps weak/strong semantics aligned (both may
// return nullptr) and prevents background std::thread / async thread pool paths
// from accidentally sharing the per-pthread fallback RequestContext's Trace
// across tasks (would let traceID/latencyTicks leak). We
// intentionally do NOT call GetRequestContext() here, because its NEVER-nullptr
// contract would always return a (fallback) Trace and make the nullptr signal
// impossible to express.
ApiDeadline *GetBthreadApiDeadline()
{
    // Ensure the bthread key is created (with call_once synchronization)
    // before reading g_requestContextKey. See GetBthreadTrace for the
    // TSAN rationale.
    InitRequestContext();
    if (g_requestContextKey == INVALID_BTHREAD_KEY) {
        return nullptr;
    }
    auto *ctx = static_cast<RequestContext *>(bthread_getspecific(g_requestContextKey));
    return ctx == nullptr || ctx->isFallbackContext ? nullptr : &ctx->apiDeadline;
}

Trace* GetBthreadTrace()
{
    // Ensure the bthread key is created before reading it. InitRequestContext
    // is idempotent (std::call_once) and provides the release/acquire pair
    // that synchronizes the key write in the once body with this read.
    // Without this, a background thread started by Logging::Start (e.g.
    // MetricsFlush) can call GetBthreadTrace -> reads g_requestContextKey
    // before CommonServer::Init -> InitRequestContext creates it on the main
    // thread, and TSAN reports a data race on the global key. Calling
    // InitRequestContext here lets whichever thread reaches this first do
    // the once-init, and all later callers synchronize via call_once.
    InitRequestContext();
    if (g_requestContextKey == INVALID_BTHREAD_KEY) {
        return nullptr;  // bthread_key_create failed
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
        AbortOnBthreadKeyError(bthread_key_create(&g_requestContextKey, nullptr), "bthread_key_create");
        AbortOnBthreadKeyError(bthread_key_create(&g_clientAccessTransportKey, nullptr), "bthread_key_create");
    });
}

void SetRequestContext(RequestContext* ctx)
{
    if (g_requestContextKey == INVALID_BTHREAD_KEY) {
        return;
    }
    bthread_setspecific(g_requestContextKey, ctx);
}

RequestContext* GetActiveRequestContext()
{
    if (g_requestContextKey == INVALID_BTHREAD_KEY) {
        return nullptr;
    }
    auto *context = static_cast<RequestContext*>(bthread_getspecific(g_requestContextKey));
    return context == nullptr || context->isFallbackContext ? nullptr : context;
}

void PublishClientAccessTransportKind(AccessTransportKind kind)
{
    InitRequestContext();
    uint8_t index = static_cast<uint8_t>(kind);
    if (index >= K_ACCESS_TRANSPORT_KIND_COUNT) {
        index = K_UNKNOWN_ACCESS_TRANSPORT_KIND;
    }
    auto *slot = const_cast<AccessTransportKind*>(&g_clientAccessTransportKinds[index]);
    AbortOnBthreadKeyError(bthread_setspecific(g_clientAccessTransportKey, slot), "bthread_setspecific");
}

bool TryGetClientAccessTransportKind(AccessTransportKind &kind)
{
    if (g_clientAccessTransportKey == INVALID_BTHREAD_KEY) {
        return false;
    }
    auto *slot = static_cast<AccessTransportKind*>(bthread_getspecific(g_clientAccessTransportKey));
    if (slot == nullptr) {
        return false;
    }
    kind = *slot;
    return true;
}

void ClearClientAccessTransportKind()
{
    if (g_clientAccessTransportKey == INVALID_BTHREAD_KEY) {
        return;
    }
    AbortOnBthreadKeyError(bthread_setspecific(g_clientAccessTransportKey, nullptr), "bthread_setspecific");
}

ScopedClientRequestContext::ScopedClientRequestContext()
{
    InitRequestContext();
    saved_ = static_cast<RequestContext*>(bthread_getspecific(g_requestContextKey));
    if (saved_ != nullptr && !saved_->isFallbackContext) {
        return;
    }
    RequestContext *callerContext = saved_ == nullptr ? GetFallbackRequestContext() : saved_;
    Trace &callerTrace = Trace::Instance();
    context_.emplace();
    context_->tenantId = callerContext->tenantId;
    context_->trace.CopyPrefixFrom(callerTrace);
    SetRequestContext(&context_.value());
}

ScopedClientRequestContext::~ScopedClientRequestContext()
{
    if (context_.has_value()) {
        const bool accessTransportTracked = context_->accessTransportTracked;
        const AccessTransportKind accessTransportKind = context_->accessTransportKind;
        SetRequestContext(saved_);
        if (accessTransportTracked) {
            PublishClientAccessTransportKind(accessTransportKind);
        }
    }
}

RequestContext* GetRequestContext(const char* file, int line)
{
    // Ensure the bthread key is created before reading it. Idempotent via
    // std::call_once. See GetBthreadTrace for the TSAN rationale: without
    // this, a background thread (ExpiredObject, MetricsFlush, etc.) can
    // call GetRequestContext -> reads g_requestContextKey before
    // CommonServer::Init -> InitRequestContext creates it on the main
    // thread, and TSAN reports a data race on the global key.
    InitRequestContext();
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
        VLOG_EVERY_N(1, K_MISSING_CONTEXT_WARN_INTERVAL) << "GetRequestContext(): no active "
            "ScopedRequestContext on this bthread (called from " << file << ":" << line << "). "
            "Expected on background/client threads; for brpc handlers, declare "
            "ScopedRequestContext as the first line of the handler.";
    }
    // No active ScopedRequestContext on this thread: use per-pthread fallback.
    // brpc background threads (EvictionTask, etc.) land here — they never set a
    // ScopedRequestContext — and client/utility threads each own their pthread,
    // so a per-pthread thread_local fallback is safe.
    return GetFallbackRequestContext();
}

}  // namespace datasystem
