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
 * Under ZMQ (usercode_in_pthread=true), GetRequestContext() falls back
 * to a per-pthread static thread_local instance, so the same API works
 * for both transports without caller-side branching.
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
 *   // ZMQ handler or utility code — no entry change needed
 *   GetRequestContext()->workerTimeCost.Append("op", ms);  // auto-fallback
 */

#ifndef DATASYSTEM_COMMON_UTIL_REQUEST_CONTEXT_H
#define DATASYSTEM_COMMON_UTIL_REQUEST_CONTEXT_H

#include "datasystem/common/log/time_cost.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/thread_local.h"

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
};

// Initialize the per-bthread key. Must be called once during server init.
// Safe to call multiple times (idempotent via std::call_once).
void InitRequestContext();

// Store RequestContext pointer for current bthread (brpc handlers).
// The pointer must remain valid for the duration of the request.
void SetRequestContext(RequestContext* ctx);

// Get the RequestContext for the current execution context.
// NEVER returns nullptr — falls back to a per-pthread static instance
// when bthread_getspecific returns nullptr (ZMQ / user-thread paths).
// file/line parameters use __builtin_FILE()/__builtin_LINE() defaults so
// error logs identify the exact call site when no ScopedRequestContext is active.
RequestContext* GetRequestContext(const char* file = __builtin_FILE(), int line = __builtin_LINE());

// RAII wrapper that creates a RequestContext on the stack, registers it via
// SetRequestContext(), and clears the bthread_key on destruction before the
// stack object is destroyed. This prevents dangling-pointer bugs when
// handlers exit early (e.g. via RETURN_IF_NOT_OK).
//
// Usage in brpc handlers:
//   ScopedRequestContext ctx;
//   // ... handler logic, may return early ...
//   // Destructor automatically calls SetRequestContext(nullptr).
class ScopedRequestContext {
public:
    ScopedRequestContext()
    {
        // Snapshot the dispatcher's traceID, then re-set it onto ctx_.trace so LOG/access
        // records inside the handler scope keep emitting the inherited traceID.
        std::string inheritedTraceID = Trace::Instance().GetTraceID();
        SetRequestContext(&ctx_);
        if (!inheritedTraceID.empty()) {
            TraceGuard guard = ctx_.trace.SetTraceNewID(inheritedTraceID, true);
            (void)guard;
        }
    }
    ~ScopedRequestContext() { SetRequestContext(nullptr); }

    // Non-copyable, non-movable.
    ScopedRequestContext(const ScopedRequestContext&) = delete;
    ScopedRequestContext& operator=(const ScopedRequestContext&) = delete;
    ScopedRequestContext(ScopedRequestContext&&) = delete;
    ScopedRequestContext& operator=(ScopedRequestContext&&) = delete;

private:
    RequestContext ctx_;
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
