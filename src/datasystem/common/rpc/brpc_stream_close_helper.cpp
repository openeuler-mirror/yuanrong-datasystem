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

#include "datasystem/common/rpc/brpc_stream_close_helper.h"

#include <chrono>
#include <deque>
#include <thread>
#include <vector>

#include "datasystem/common/log/log.h"

namespace datasystem {

// --- Leak counter -----------------------------------------------------------

static std::atomic<uint64_t> g_brpcStreamLeakCount{0};

uint64_t GetBrpcStreamLeakCount() noexcept
{
    return g_brpcStreamLeakCount.load(std::memory_order_relaxed);
}

// --- Stream close helpers ---------------------------------------------------

StreamCloseResult StreamCloseAndWait(StreamCloseState state, int timeoutSec)
{
    if (state.streamId == brpc::INVALID_STREAM_ID) {
        return StreamCloseResult::kClosed;
    }
    brpc::StreamClose(state.streamId);
    std::unique_lock<std::mutex> lk(state.mtx);
    // Wait for on_closed ONLY (streamEnd), not on_failed (readError). brpc fires
    // on_failed then ALWAYS on_closed as the final callback during stream recycle
    // (stream.cpp:594 -> :596). Returning on on_failed alone let the caller destroy
    // the StreamInputHandler while brpc immediately fired on_closed on the dangling
    // pointer -> UAF. readError_ still unblocks the Read() loop separately.
    bool closed = state.cv.wait_for(lk, std::chrono::seconds(timeoutSec),
        [&state] { return state.streamEnd; });
    state.streamId = brpc::INVALID_STREAM_ID;
    return closed ? StreamCloseResult::kClosed : StreamCloseResult::kTimeout;
}

bool StreamCloseAndDrain(StreamCloseState state,
                         std::unique_ptr<brpc::Controller>& cntl,
                         const char* logContext)
{
    auto result = StreamCloseAndWait(state);
    if (result == StreamCloseResult::kClosed) {
        cntl.reset();
        return true;
    }
    if (state.closeNotifier) {
        EnqueueDeferredCleanup(std::move(cntl), state.closeNotifier,
                               state.maxDeferredWaitSec);
        return false;
    }
    LOG(ERROR) << (logContext != nullptr ? logContext : "StreamClose")
               << ": stream close timed out after " << kStreamCloseTimeoutSec
               << "s; intentionally leaking Controller to avoid UAF on in-flight "
               << "brpc IO bthread";
    (void)cntl.release();
    g_brpcStreamLeakCount.fetch_add(1, std::memory_order_relaxed);
    return false;
}

// --- Deferred cleanup queue + reaper ----------------------------------------

// maxDeferredWaitSec formula (see header file-level comment):
//   idle_timeout_ms > 0 ? max(idle_timeout_ms / 1000 * 3, 60)
//                       : kDefaultDeferredWaitSec (7200s, matches TCP keepalive)

namespace {

std::mutex g_deferredMutex;
std::condition_variable g_deferredCv;
std::deque<DeferredCleanupItem> g_deferredQueue;
std::thread g_reaperThread;
bool g_reaperRunning = false;  // protected by g_deferredMutex

constexpr auto K_REAPER_POLL_INTERVAL = std::chrono::seconds(30);

// Process one batch of deferred cleanup items.  Returns true if the queue
// was non-empty (caller should continue polling), false if the queue is
// empty and the reaper should stop.
// Must be called with g_deferredMutex NOT held.
bool ProcessDeferredQueueBatch()
{
    std::vector<std::unique_ptr<brpc::Controller>> toDelete;
    {
        std::unique_lock<std::mutex> lk(g_deferredMutex);
        g_deferredCv.wait_for(lk, K_REAPER_POLL_INTERVAL,
            [] { return !g_deferredQueue.empty() || !g_reaperRunning; });

        if (g_deferredQueue.empty() || !g_reaperRunning) {
            g_reaperRunning = false;
            return false;
        }

        auto now = std::chrono::steady_clock::now();
        auto it = g_deferredQueue.begin();
        while (it != g_deferredQueue.end()) {
            bool safe = it->closedFlag
                && it->closedFlag->load(std::memory_order_relaxed);
            bool expired = std::chrono::duration_cast<std::chrono::seconds>(
                now - it->enqueueTime).count() >= it->maxDeferredWaitSec;

            if (safe) {
                toDelete.push_back(std::move(it->cntl));
                it = g_deferredQueue.erase(it);
            } else if (expired) {
                LOG(ERROR) << "Deferred Controller cleanup: max wait of "
                           << it->maxDeferredWaitSec
                           << "s exceeded; intentionally leaking Controller"
                           << " to avoid UAF on in-flight brpc IO bthread";
                (void)it->cntl.release();
                g_brpcStreamLeakCount.fetch_add(1, std::memory_order_relaxed);
                it = g_deferredQueue.erase(it);
            } else {
                ++it;
            }
        }
    }
    toDelete.clear();
    return true;
}

// Ensure reaper thread is running.  Must be called with g_deferredMutex held.
void EnsureReaperLocked()
{
    if (!g_reaperRunning) {
        if (g_reaperThread.joinable()) {
            g_reaperThread.join();
        }
        g_reaperRunning = true;
        g_reaperThread = std::thread([] {
            while (ProcessDeferredQueueBatch()) {
                // Continue polling until the queue is empty.
            }
        });
    }
}

}  // namespace

void EnqueueDeferredCleanup(std::unique_ptr<brpc::Controller> cntl,
                            std::shared_ptr<std::atomic<bool>> closedFlag,
                            int maxDeferredWaitSec)
{
    {
        std::lock_guard<std::mutex> lk(g_deferredMutex);
        g_deferredQueue.push_back({std::move(cntl), std::chrono::steady_clock::now(),
                                   std::move(closedFlag), maxDeferredWaitSec});
        EnsureReaperLocked();
    }
    g_deferredCv.notify_one();
}

void StartDeferredCleanupReaper()
{
    std::lock_guard<std::mutex> lk(g_deferredMutex);
    if (!g_deferredQueue.empty()) {
        EnsureReaperLocked();
    }
}

void StopDeferredCleanupReaper()
{
    // SAFETY PRECONDITION: caller must have joined all brpc IO bthreads
    // (e.g. rpcServer->Shutdown()) before calling this function.  Any
    // remaining Controllers in the queue may still be referenced by
    // in-flight brpc IO bthreads if brpc has not been shut down, and
    // deleting them here would UAF.
    {
        std::lock_guard<std::mutex> lk(g_deferredMutex);
        g_reaperRunning = false;
        g_deferredQueue.clear();
    }
    g_deferredCv.notify_one();
    // Unconditional join: the reaper thread may have already exited
    // (running was false from a prior lazy-stop), but g_reaperThread is
    // still joinable.  A joinable std::thread that is not joined before
    // destruction triggers std::terminate() at process exit.
    if (g_reaperThread.joinable()) {
        g_reaperThread.join();
    }
}

uint64_t GetDeferredCleanupQueueSize()
{
    std::lock_guard<std::mutex> lk(g_deferredMutex);
    return g_deferredQueue.size();
}

}  // namespace datasystem
