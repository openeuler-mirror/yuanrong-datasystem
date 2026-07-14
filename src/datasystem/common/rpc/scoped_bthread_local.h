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
 * Description: Per-bthread storage for brpc M:N scheduling.
 *
 * Motivation: brpc uses M:N scheduling — N bthreads multiplexed on M pthread
 * workers. C++ thread_local variables are per-pthread, which causes cross-request
 * contamination when a bthread yields (e.g. on RPC) and the underlying pthread
 * picks up a different bthread: bthread B reads bthread A's tenant_id, AK/SK,
 * timeout deadline, or serialized message.
 *
 * ScopedBthreadLocal<T> replaces thread_local T with per-bthread storage via
 * bthread_key_t (bthread_getspecific / bthread_setspecific). Each bthread
 * lazily allocates its own T instance on first access.
 *
 * Constraints:
 *  - Designed for global / static storage duration. Do NOT use on the stack
 *    or as a function-local static; the destructor must run only after all
 *    bthreads have exited, otherwise per-bthread T instances may leak.
 *  - NOT async-signal-safe. std::call_once and bthread_getspecific may
 *    deadlock or corrupt state if called from a signal handler.
 *  - Per-access overhead: bthread_getspecific (O(1) keytable lookup, ~10-20
 *    cycles) vs thread_local (~1-2 cycles). The correctness gain eliminates
 *    cross-request auth/timeout/serialization bugs, which far outweighs the
 *    microsecond-level overhead.
 *
 * Usage:
 *   Before: extern thread_local TimeoutDuration timeoutDuration;
 *           timeoutDuration.Init(ms);           // dot access
 *   After:  extern ScopedBthreadLocal<TimeoutDuration> timeoutDuration;
 *           timeoutDuration->Init(ms);          // arrow access via operator->
 *           *timeoutDuration = other;           // deref for value access
 *           timeoutDuration = other;            // assignment via forwarding op=
 */

#pragma once

#include <bthread/bthread.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <new>
#include <utility>

namespace datasystem {

template <typename T>
class ScopedBthreadLocal {
public:
    ScopedBthreadLocal() = default;

    ~ScopedBthreadLocal()
    {
        // Mark destroyed so debug-mode Get() can detect use-after-destructor.
        // Release ordering ensures all prior writes to per-bthread T values
        // are visible before the key is deleted.
#ifndef NDEBUG
        destroyed_.store(true, std::memory_order_release);
#endif
        if (key_ != INVALID_BTHREAD_KEY) {
            // bthread_key_delete does NOT invoke the key destructor for
            // existing per-bthread values. At process exit all bthreads
            // should have already exited (and their destructors run). Any
            // remaining values at this point are leaked intentionally —
            // the OS reclaims all memory on process exit.
            bthread_key_delete(key_);
        }
    }

    // Non-copyable, non-movable (owns a bthread_key_t).
    ScopedBthreadLocal(const ScopedBthreadLocal&) = delete;
    ScopedBthreadLocal& operator=(const ScopedBthreadLocal&) = delete;
    ScopedBthreadLocal(ScopedBthreadLocal&&) = delete;
    ScopedBthreadLocal& operator=(ScopedBthreadLocal&&) = delete;

    // Forwarding assignment: g_ReqTimestamp = 42u or scTimeoutDuration = parent.
    ScopedBthreadLocal& operator=(const T& val)
    {
        *Get() = val;
        return *this;
    }
    ScopedBthreadLocal& operator=(T&& val)
    {
        *Get() = std::move(val);
        return *this;
    }

    // Returns pointer to the per-bthread T instance. Allocates one on first
    // access for the current bthread. Never returns nullptr.
    // Thread-safety: safe to call concurrently from multiple bthreads.
    // Each bthread gets its own T* via bthread_getspecific, so there is
    // no data race on the T values.
    T* Get()
    {
#ifndef NDEBUG
        if (destroyed_.load(std::memory_order_acquire)) {
            fprintf(stderr,
                    "FATAL: ScopedBthreadLocal::Get() called after destruction. "
                    "ScopedBthreadLocal must have static storage duration and "
                    "all bthreads must exit before static destructors run.\n");
            std::abort();
        }
#endif
        EnsureKeyCreated();
        void* data = bthread_getspecific(key_);
        if (data == nullptr) {
            auto* ptr = new T();
            int rc = bthread_setspecific(key_, ptr);
            // bthread_setspecific fails if the key is invalid (destructor
            // already ran) or if the bthread keytable/subkeytable allocation
            // returns ENOMEM (brpc uses nothrow new for internal tables). In
            // either case the process cannot safely continue with per-bthread
            // storage — the returned T* would not be tracked for cleanup, and
            // the next Get() would allocate another T (leak). Fail fast.
            if (rc != 0) {
                delete ptr;
                fprintf(stderr,
                        "FATAL: bthread_setspecific failed with rc=%d. "
                        "bthread keytable allocation failed or key deleted.\n",
                        rc);
                std::abort();
            }
            return ptr;
        }
        return static_cast<T*>(data);
    }

    // Pointer-like access: timeoutDuration->Init(ms)
    T* operator->() { return Get(); }

    // Dereference access: *timeoutDuration = otherTimeoutDuration
    T& operator*() { return *Get(); }

    // Implicit conversion to T&. Not universally applied by GCC in C++14 mode
    // (member access and operator expressions may not trigger it), so call
    // sites should prefer explicit -> and * syntax. This operator remains for
    // contexts where implicit conversion works reliably (e.g. function arguments).
    operator T&() { return *Get(); }  // NOLINT(google-explicit-constructor,hicpp-explicit-conversions)

private:
    void EnsureKeyCreated()
    {
        // std::call_once synchronizes with itself: only the first caller
        // executes the init lambda; all other callers block until it completes.
        // The init lambda does NOT call bthread_yield or any blocking operation
        // (bthread_key_create is a simple atomic counter increment), so it
        // cannot cause a bthread-level deadlock.
        // Note: std::call_once blocks the underlying pthread (not just the
        // bthread). The blocking window is ~nanoseconds (bthread_key_create
        // is O(1)), so this does not cause meaningful pthread starvation.
        std::call_once(once_, [this]() {
            int rc = bthread_key_create(&key_, [](void* data) {
                // Called by brpc when a bthread exits and its keytable is
                // cleaned up. NOT called by bthread_key_delete() — see
                // brpc documentation: "No destructor is invoked by this
                // function."
                delete static_cast<T*>(data);
            });
            if (rc != 0) {
                // bthread_key_create failure means the bthread key pool is
                // exhausted. brpc's BTHREAD_MAX_KEYS default is large (~1024),
                // and we use only 8 keys across the entire process. If this
                // fires, the process is fundamentally broken — fail fast
                // rather than continuing with undefined bthread TLS behavior.
                fprintf(stderr,
                        "FATAL: bthread_key_create failed with rc=%d. "
                        "bthread key pool exhausted? (BTHREAD_MAX_KEYS exceeded)\n",
                        rc);
                std::abort();
            }
        });
    }

    bthread_key_t key_ = INVALID_BTHREAD_KEY;
    std::once_flag once_;

#ifndef NDEBUG
    // Set to true in the destructor. Debug-mode Get() checks this to catch
    // use-after-destructor bugs (e.g. non-static ScopedBthreadLocal instances).
    // In release builds this check is compiled out to avoid hot-path overhead.
    std::atomic<bool> destroyed_{false};
#endif
};

}  // namespace datasystem
