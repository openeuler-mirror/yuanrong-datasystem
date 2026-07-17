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
 * Description: Helper for closing brpc streams with bounded wait + deferred
 * cleanup on timeout. Consolidates the "StreamClose + wait_for + leak-or-reset"
 * pattern previously duplicated across BrpcClientStreamWriterReader and
 * BrpcClientReaderImpl (7 call sites).
 *
 * Deferred cleanup strategy (Plan A+D):
 *   1. StreamCloseAndWait blocks up to kStreamCloseTimeoutSec (5s) for
 *      on_closed/on_failed callbacks.
 *   2. If callbacks fire in time (kClosed): Controller is reset normally.
 *   3. If timeout (kTimeout) with closeNotifier: Controller is enqueued into
 *      a deferred cleanup queue. A lazily-started reaper thread periodically
 *      checks a shared atomic flag set by on_closed/on_failed callbacks.
 *      - If the flag is true: controller is safely reset (no leak).
 *      - If maxDeferredWaitSec elapses without the flag: controller is leaked
 *        as a safety fallback (Plan A), and the leak counter is incremented.
 *   4. If timeout without closeNotifier: immediate leak (backward compatible).
 *
 * KNOWN LIMITATION (Controller only): This deferred-cleanup strategy protects
 * the Controller from UAF. It does NOT by itself protect the StreamInputHandler
 * object: brpc borrows the raw handler pointer and fires on_closed (its final
 * callback) on a brpc bthread after the owner may have destroyed it.
 * The handler UAF is fixed at the handler layer: each BrpcClientReaderImpl /
 * BrpcClientStreamWriterReader / BrpcServerReaderImpl keeps itself alive via a
 * shared_ptr keepalive cleared at the tail of on_closed, so the object outlives
 * brpc's final callback. See those headers.
 *
 * Predicate: StreamCloseAndWait waits for on_closed (streamEnd) only, NOT
 * on_failed (readError). brpc always fires on_failed then on_closed during
 * recycle (stream.cpp:594 -> :596); returning on on_failed alone was a UAF root
 * cause (caller destroyed the handler before the guaranteed on_closed).
 *
 * maxDeferredWaitSec calculation:
 *   When brpc StreamOptions::idle_timeout_ms is set (>0), brpc detects the
 *   dead stream within idle_timeout_ms and fires on_failed. In this case we
 *   use 3x the idle timeout as the deferred wait (min 60s):
 *     maxDeferredWait = max(idle_timeout_ms / 1000 * 3, 60)
 *   When idle_timeout_ms is -1 (default/disabled), brpc relies on TCP
 *   keepalive to detect dead connections (Linux default ~7200s). We match
 *   this with kDefaultDeferredWaitSec = 7200s (2h).
 */

#ifndef DATASYSTEM_COMMON_RPC_BRPC_STREAM_CLOSE_HELPER_H
#define DATASYSTEM_COMMON_RPC_BRPC_STREAM_CLOSE_HELPER_H

#include <atomic>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <chrono>
#include <condition_variable>
#include <bthread/mutex.h>
#include <bthread/condition_variable.h>
#include <cstdint>
#include <memory>
#include <mutex>

#include "datasystem/common/rpc/brpc_status_util.h"  // kStreamCloseTimeoutSec

namespace datasystem {

// Default max wait for deferred cleanup when idle_timeout_ms is not set.
// Matches Linux default TCP keepalive time (tcp_keepalive_time = 7200s).
// When idle_timeout_ms > 0, callers should compute a shorter value:
//   max(idle_timeout_ms / 1000 * 3, 60)
inline constexpr int kDefaultDeferredWaitSec = 7200;

enum class StreamCloseResult {
    kClosed,    // on_closed/on_failed fired within timeout
    kTimeout,   // Timed out waiting for callbacks
};

// Aggregate state parameters to keep helper signatures short.
// All fields are references to the caller's state; lifetime must outlive the
// helper call (i.e., the caller's members must outlive this call).
//
// IMPORTANT: This struct uses aggregate initialization ({...}).  When adding
// or reordering fields, ALL call sites must be updated with the new field
// order.  C++14 aggregate init does not use default member initializers for
// omitted fields (they are copy-initialized from {} instead), so a missing
// field silently becomes 0/nullptr rather than the declared default.
struct StreamCloseState {
    brpc::StreamId& streamId;
    bthread::Mutex& mtx;
    bthread::ConditionVariable& cv;
    bool& streamEnd;
    bool& readError;
    // Shared flag set by on_closed/on_failed callbacks to notify the deferred
    // cleanup reaper that the Controller is safe to delete.
    std::shared_ptr<std::atomic<bool>> closeNotifier;
    // Max time to keep the Controller in the deferred queue before falling
    // back to intentional leak.  Derived from idle_timeout_ms or default
    // kDefaultDeferredWaitSec.  See file-level comment for formula.
    int maxDeferredWaitSec = kDefaultDeferredWaitSec;
};

// Close a brpc stream and wait bounded time for on_closed/on_failed.
// Sets streamId to INVALID_STREAM_ID before returning.
// Never blocks forever — bounded by timeoutSec.
StreamCloseResult StreamCloseAndWait(StreamCloseState state,
                                     int timeoutSec = kStreamCloseTimeoutSec);

// Convenience: close + wait + deferred-cleanup-or-reset Controller.
// - On kClosed: cntl.reset(), returns true.
// - On kTimeout with closeNotifier: enqueues cntl for deferred cleanup,
//   returns false.  The reaper thread will either reset (safe) or release
//   (leak fallback) later.
// - On kTimeout without closeNotifier: cntl.release() immediately (old
//   behavior), leak counter incremented, returns false.
bool StreamCloseAndDrain(StreamCloseState state,
                         std::unique_ptr<brpc::Controller>& cntl,
                         const char* logContext = nullptr);

// Item in the deferred cleanup queue.
struct DeferredCleanupItem {
    std::unique_ptr<brpc::Controller> cntl;
    std::chrono::steady_clock::time_point enqueueTime;
    std::shared_ptr<std::atomic<bool>> closedFlag;
    int maxDeferredWaitSec;
};

// Enqueue a Controller for deferred cleanup.  The reaper thread is lazily
// started on first enqueue and stops when the queue drains to empty.
void EnqueueDeferredCleanup(std::unique_ptr<brpc::Controller> cntl,
                            std::shared_ptr<std::atomic<bool>> closedFlag,
                            int maxDeferredWaitSec = kDefaultDeferredWaitSec);

// Lifecycle management for the deferred cleanup reaper thread.
// StartDeferredCleanupReaper is idempotent (called internally by Enqueue).
// StopDeferredCleanupReaper drains the queue and joins the reaper.
// SAFETY PRECONDITION: caller must have joined all brpc IO bthreads
// (e.g. via rpcServer->Shutdown()) before calling this.  Controllers
// in the queue may still be referenced by brpc if the server is running.
void StartDeferredCleanupReaper();
void StopDeferredCleanupReaper();

// Metric accessors.
uint64_t GetBrpcStreamLeakCount() noexcept;
uint64_t GetDeferredCleanupQueueSize();

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_BRPC_STREAM_CLOSE_HELPER_H
