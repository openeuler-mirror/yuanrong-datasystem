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
 * Description: Transport-agnostic per-request context.
 *
 * Under brpc M:N, multiple bthreads share a single pthread, making C++
 * thread_local variables unsafe. RequestContext provides per-bthread
 * storage via bthread_key_t.
 *
 * Utility code that runs outside a brpc handler falls back to a per-pthread
 * static thread_local instance, so the same API works without caller-side
 * branching.
 *
 * Usage:
 *   // Server init (once)
 *   InitRequestContext();
 *
 *   // brpc handler entry
 *   RequestContext ctx;
 *   SetRequestContext(&ctx);
 *   // ... handler logic ...
 *   GetRequestContext()->workerTimeCost.Append("op", ms);  // always works
 *
 *   // Utility code (no active ScopedRequestContext) — no entry change needed
 *   GetRequestContext()->workerTimeCost.Append("op", ms);  // auto-fallback
 */

#ifndef DATASYSTEM_COMMON_UTIL_REQUEST_CONTEXT_H
#define DATASYSTEM_COMMON_UTIL_REQUEST_CONTEXT_H

#include <optional>

// thread_local.h pulls <bthread/bthread.h> -> bvar, whose percentile.h uses
// CHECK_EQ << "...". That must be parsed with butil's CHECK_EQ (supports <<) BEFORE
// log.h (via time_cost.h) redefines CHECK_EQ as a do{}while(0) that breaks the <<.
// So thread_local.h must come before time_cost.h.
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/log/time_cost.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/rpc_message.h"

namespace datasystem {

// Forward-declare to avoid pulling access_recorder.h into all RequestContext users.
enum class AccessTransportKind : uint8_t;

struct RequestContext {
    TimeCost workerTimeCost;
    TimeCost masterTimeCost;
    Trace trace;
    // Per-request transport kind tracked by AccessTransportTracker.
    // Default SHM=0; full enum definition in access_recorder.h.
    AccessTransportKind accessTransportKind = static_cast<AccessTransportKind>(0);
    bool accessTransportTracked = false;

    // API-level deadline owned by this request. ApiDeadline::Instance() resolves to this object
    // while the RequestContext is active, so it remains stable across BRPC bthread migration.
    ApiDeadline apiDeadline;
    bool isFallbackContext = false;

    // Per-request timeout trackers (replaces ScopedBthreadLocal<TimeoutDuration>).
    // Each request lazily inits its own TimeoutDuration (default RPC_TIMEOUT) at
    // entry; subsequent reads via GetRequestContext()->xxxTimeoutDuration are
    // zero-overhead after taking a reference. Under brpc M:N this is the only
    // correct place for per-request deadline state — the old globals leaked
    // across requests when a bthread yielded and the pthread picked up another.
    TimeoutDuration reqTimeoutDuration;
    TimeoutDuration scTimeoutDuration;
    TimeoutDuration timeoutDuration;

    // Per-request auth context. Set at client SDK signature generation and consumed
    // by authenticate.cpp / ak_sk_manager.h before any yield.
    RpcMessage serializedMessage;
    std::string tenantId;
    std::string reqAk;
    std::string reqSignature;
    uint64_t reqTimestamp = 0;
};

// Initialize the per-bthread key. Must be called once during server init.
// Safe to call multiple times (idempotent via std::call_once).
void InitRequestContext();

// Store RequestContext pointer for current bthread (brpc handlers).
// The pointer must remain valid for the duration of the request.
void SetRequestContext(RequestContext* ctx);


// Get the RequestContext for the current execution context.
// NEVER returns nullptr — falls back to a per-pthread static instance
// when bthread_getspecific returns nullptr (user-thread / background-thread paths).
// file/line parameters use __builtin_FILE()/__builtin_LINE() defaults so
// error logs identify the exact call site when no ScopedRequestContext is active.
RequestContext* GetRequestContext(const char* file = __builtin_FILE(), int line = __builtin_LINE());

// Return the active non-fallback BRPC request context, or nullptr when the
// current execution context only has the unsafe pthread fallback.
RequestContext* GetActiveRequestContext();

// Preserve only the completed standalone KVClient call's transport result in
// bthread-local storage. Other request-scoped fields remain isolated in the
// temporary ScopedClientRequestContext.
void PublishClientAccessTransportKind(AccessTransportKind kind);
bool TryGetClientAccessTransportKind(AccessTransportKind &kind);
void ClearClientAccessTransportKind();

// RAII wrapper that creates a RequestContext on the stack, registers it via
// SetRequestContext(), and restores the previous context on destruction.
// Nested ScopedRequestContext is safe: the destructor restores the outer
// context rather than clearing to nullptr.
//
// When to use ScopedRequestContext vs TraceGuard SetTraceNewID:
//   - Use SetRequestContext(nullptr) + ScopedRequestContext(traceID) when the
//     thread pool task body sends brpc RPCs. brpc may borrow the calling
//     pthread to run other bthreads (M:N work-stealing), leaving a stale
//     RequestContext pointer in the bthread-local keytable. Installing a
//     fresh ScopedRequestContext protects against this pollution.
//   - Use TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID)
//     when the task body does NOT send brpc RPCs (e.g. local RocksDB ops,
//     fire-and-forget logging, pure computation). The simpler TraceGuard is
//     sufficient because there is no brpc borrowing to pollute the slot.
// Usage in brpc handlers:
//   ScopedRequestContext ctx;
//   // ... handler logic, may return early ...
//   // Destructor automatically calls SetRequestContext(saved).
class ScopedRequestContext {
public:
    // When traceID is provided (brpc adapter extracts it from the request attachment),
    // use it directly — bypassing the unreliable brpc M:N thread_local fallback.
    // When empty (nested ScopedRequestContext in handler code, or non-adapter paths),
    // inherit traceID from Trace::Instance().GetTraceID() as before.
    explicit ScopedRequestContext(const std::string& traceID = "")
        : saved_(GetRequestContext())
    {
        // Snapshot the inherited traceID BEFORE SetRequestContext overwrites
        // the bthread-local pointer. Under brpc M:N, Trace::Instance() routes
        // through GetBthreadTrace() -> bthread_getspecific, which returns the
        // outer (saved) RequestContext's trace. After SetRequestContext, the
        // key points to ctx_ which has an empty trace.
        std::string inheritedTraceID;
        if (!traceID.empty()) {
            inheritedTraceID = traceID;
        } else {
            inheritedTraceID = Trace::Instance().GetTraceID();
        }
        // Inherit per-request timeout/auth state from the outer context so that
        // nested ScopedRequestContext (created by business-layer handlers after
        // the transport-layer entry already set deadlines) does NOT reset
        // timeouts to defaults. Without this, a handler creating an inner scope
        // would read default-constructed timeouts — a regression vs the old
        // globals which were independent of ScopedRequestContext.
        // Note: serializedMessage (RpcMessage) has deleted copy semantics, and
        // the auth strings are consumed (std::move'd out) before business
        // handlers create nested scopes, so they are intentionally NOT inherited.
        if (saved_ != nullptr) {
            ApiDeadline *activeDeadline = GetBthreadApiDeadline();
            if (activeDeadline != nullptr) {
                ctx_.apiDeadline.InheritStateFrom(*activeDeadline);
            } else {
                ctx_.apiDeadline.InheritDeadlineFrom(ApiDeadline::Instance());
            }
            ctx_.reqTimeoutDuration = saved_->reqTimeoutDuration;
            ctx_.scTimeoutDuration = saved_->scTimeoutDuration;
            ctx_.timeoutDuration = saved_->timeoutDuration;
            // serializedMessage: not inherited (RpcMessage copy is deleted;
            // consumed before nested scope creation)
            ctx_.tenantId = saved_->tenantId;
            ctx_.reqAk = saved_->reqAk;
            ctx_.reqSignature = saved_->reqSignature;
            ctx_.reqTimestamp = saved_->reqTimestamp;
        }
        SetRequestContext(&ctx_);
        if (!inheritedTraceID.empty()) {
            TraceGuard guard = ctx_.trace.SetTraceNewID(inheritedTraceID, true);
            (void)guard;
            // Inherit the request-log sampling state from the outer scope so a
            // nested handler ScopedRequestContext does not drop the transport
            // prologue's ApplyLogSampleState(). SetTraceNewID above unconditionally
            // clears requestLogTrace + sampleDecision; restore them from the saved
            // (outer) trace. Under brpc M:N, Trace::Instance() routes to this ctx_.trace
            // via bthread_getspecific, so without this restore the access recorder
            // (constructed inside the nested scope) sees requestLogTrace=false and
            // sampling is bypassed.
            if (saved_ != nullptr) {
                bool outerAdmitted = false;
                const bool outerHasDecision = saved_->trace.GetRequestSampleDecision(outerAdmitted);
                ctx_.trace.SetRequestLogTrace(saved_->trace.IsRequestLogTrace());
                ctx_.trace.SetRequestSampleDecision(outerHasDecision, outerAdmitted);
            }
        }
    }
    ~ScopedRequestContext() { SetRequestContext(saved_); }

    // Non-copyable, non-movable.
    ScopedRequestContext(const ScopedRequestContext&) = delete;
    ScopedRequestContext& operator=(const ScopedRequestContext&) = delete;
    ScopedRequestContext(ScopedRequestContext&&) = delete;
    ScopedRequestContext& operator=(ScopedRequestContext&&) = delete;

private:
    RequestContext ctx_;
    RequestContext* saved_;
};

// Client API boundary for BRPC mode. It keeps an already-active request context
// unchanged for nested calls, but installs a fresh context when the caller only
// has the unsafe per-pthread fallback.
class ScopedClientRequestContext {
public:
    ScopedClientRequestContext();
    ~ScopedClientRequestContext();

    ScopedClientRequestContext(const ScopedClientRequestContext&) = delete;
    ScopedClientRequestContext& operator=(const ScopedClientRequestContext&) = delete;
    ScopedClientRequestContext(ScopedClientRequestContext&&) = delete;
    ScopedClientRequestContext& operator=(ScopedClientRequestContext&&) = delete;

private:
    std::optional<RequestContext> context_;
    RequestContext* saved_ = nullptr;
};

// Convenience: get worker TimeCost (always valid, never nullptr).
inline TimeCost& GetWorkerTimeCost(const char* file = __builtin_FILE(), int line = __builtin_LINE())
{
    return GetRequestContext(file, line)->workerTimeCost;
}

// Convenience: get master TimeCost (always valid, never nullptr).
inline TimeCost& GetMasterTimeCost(const char* file = __builtin_FILE(), int line = __builtin_LINE())
{
    return GetRequestContext(file, line)->masterTimeCost;
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_REQUEST_CONTEXT_H
