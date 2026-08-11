#pragma once

// Thin std-style primitives that swap to brpc/bthread in bazel mode.
//
// Why: datasystem KVClient is synchronous (Set/Get return Status), but its
// internal RPC/transport paths are bthread-aware (bthread::Mutex, bthread::
// RWLock, bthread::ConditionVariable, brpc::Channel). When a kvtest pipeline
// thread is a std::thread, a SDK RPC wait blocks the whole pthread worker.
// When the same call runs inside a bthread, the SDK's internal contention and
// RPC completion naturally yield the bthread and let the brpc worker pthread
// serve other bthreads — the M:N benefit brpc is built around.
//
// Selection: in bazel mode (KVTEST_USE_BRPC, defined by tests/kvtest/BUILD.
// bazel) the primitives wrap bthread_t / bthread::Mutex / bthread_rwlock_t /
// bthread::ConditionVariable. In cmake mode (httplib, links the pre-installed
// SDK which does not ship brpc/bthread headers) they alias to std primitives.
// Callers (KVWorker, CacheReader) reference kvtest::thread / kvtest::mutex /
// kvtest::condition_variable / kvtest::shared_mutex / kvtest::sleep_for /
// kvtest::sleep_until unconditionally, keeping the pipeline source identical
// across both build modes.
//
// The bthread::ConditionVariable API lacks the std predicate overloads
// (wait(lock, pred) / wait_for(lock, dur, pred)); the wrapper here implements
// those by hand so callers keep std-style predicate usage. The wait_for
// duration form takes int64 microseconds in bthread (see SDK
// cluster/runtime/topology_snapshot_state.cpp:143), so the wrapper converts
// from std::chrono here, centralizing the unit handling.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <utility>

#ifdef KVTEST_USE_BRPC
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/rwlock.h>
#endif

namespace kvtest {

#ifdef KVTEST_USE_BRPC

// std::thread-compatible bthread wrapper. bthreads are M:N scheduled on the
// brpc worker pthread pool — a SDK RPC wait yields the bthread instead of
// holding a pthread. Mirrors the SDK's datasystem::StartBackgroundTask
// pattern (src/datasystem/common/rpc/bthread_utils.h) but local to kvtest so
// the cmake-mode path stays free of bthread headers.
//
// Lifetime mirrors std::thread exactly: a non-joined, non-detached thread at
// destruction calls std::terminate() — the loud surface for forgotten
// shutdown, instead of silently blocking the destructor or letting a
// captured `this` dangle. Existing callers (KVWorker::Stop, CacheReader::
// Stop) join every thread before the container is cleared, so the normal
// path never trips this guard.
class thread {
public:
    thread() = default;
    thread(thread &&o) noexcept : tid_(o.tid_)
    {
        o.tid_ = kInvalid;
    }
    thread &operator=(thread &&o) noexcept
    {
        if (this != &o) {
            if (joinable()) {
                std::terminate();
            }
            tid_ = o.tid_;
            o.tid_ = kInvalid;
        }
        return *this;
    }
    thread(const thread &) = delete;
    thread &operator=(const thread &) = delete;

    template <typename F, typename... Args>
    explicit thread(F &&f, Args &&...args)
    {
        using BoundFn = std::function<void()>;
        auto *ctx =
            new BoundFn([fn = std::forward<F>(f), args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
                std::apply(std::move(fn), std::move(args));
            });
        bthread_t tid = kInvalid;
        const int rc = bthread_start_background(
            &tid, nullptr,
            [](void *p) -> void * {
                auto *fn = static_cast<BoundFn *>(p);
                (*fn)();
                delete fn;
                return nullptr;
            },
            ctx);
        if (rc != 0) {
            delete ctx;
            // Match std::thread failure surface: nothing to join (joinable()
            // is false). Caller is expected to detect via joinable().
            return;
        }
        tid_ = tid;
    }

    bool joinable() const
    {
        return tid_ != kInvalid;
    }
    void join()
    {
        if (tid_ != kInvalid) {
            (void)bthread_join(tid_, nullptr);
            tid_ = kInvalid;
        }
    }
    void detach()
    {
        tid_ = kInvalid;
    }

    ~thread()
    {
        if (joinable()) {
            std::terminate();
        }
    }

private:
    // bthread_t is an integer id; 0 is the conventional "no bthread" sentinel
    // and is what bthread_start_background writes to *tid on failure paths
    // when it does not crash. Keep 0 as our invalid marker to stay consistent
    // with bthread_t default-initialization.
    static constexpr bthread_t kInvalid{ 0 };
    bthread_t tid_ = kInvalid;
};

using mutex = bthread::Mutex;

// std::condition_variable-compatible wrapper. bthread::ConditionVariable has
// wait(lock) and wait_for(lock, int64 micros) -> int (0 = notified,
// ETIMEDOUT = timeout), but no predicate overloads. Implement the predicate
// forms by hand so call sites can keep std-style usage.
class condition_variable {
public:
    void notify_one()
    {
        cv_.notify_one();
    }
    void notify_all()
    {
        cv_.notify_all();
    }

    template <typename Lock>
    void wait(Lock &lock)
    {
        cv_.wait(lock);
    }

    template <typename Lock, typename Pred>
    void wait(Lock &lock, Pred pred)
    {
        while (!pred()) {
            cv_.wait(lock);
        }
    }

    template <typename Lock, typename Rep, typename Period>
    std::cv_status wait_for(Lock &lock, std::chrono::duration<Rep, Period> d)
    {
        const auto us = RoundUpMicros(d);
        const int rc = (us.count() > 0) ? cv_.wait_for(lock, us.count()) : cv_.wait_for(lock, 0);
        return (rc == 0) ? std::cv_status::no_timeout : std::cv_status::timeout;
    }

    template <typename Lock, typename Rep, typename Period, typename Pred>
    bool wait_for(Lock &lock, std::chrono::duration<Rep, Period> d, Pred pred)
    {
        const auto us = RoundUpMicros(d);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(us);
        while (!pred()) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return pred();
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - now);
            const int rc = cv_.wait_for(lock, remaining.count());
            // rc == ETIMEDOUT: loop test will re-check pred against deadline.
            // rc == 0 (notified): re-check pred at top of loop.
            (void)rc;
        }
        return true;
    }

private:
    template <typename Rep, typename Period>
    static std::chrono::microseconds RoundUpMicros(std::chrono::duration<Rep, Period> d)
    {
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(d);
        if (us < d) {
            us += std::chrono::microseconds(1);
        }
        return us;
    }

    bthread::ConditionVariable cv_;
};

// std::shared_mutex-compatible wrapper using bthread_rwlock_t. Exposes
// lock/unlock/lock_shared/unlock_shared/try_lock/try_lock_shared so both
// std::unique_lock and std::shared_lock work against this type, matching the
// SDK's bthread_rwlock_t usage in src/datasystem/common/object_cache/
// safe_object.h.
class shared_mutex {
public:
    shared_mutex()
    {
        (void)bthread_rwlock_init(&lock_, nullptr);
    }
    ~shared_mutex()
    {
        (void)bthread_rwlock_destroy(&lock_);
    }
    shared_mutex(const shared_mutex &) = delete;
    shared_mutex &operator=(const shared_mutex &) = delete;

    void lock()
    {
        (void)bthread_rwlock_wrlock(&lock_);
    }
    void unlock()
    {
        (void)bthread_rwlock_unlock(&lock_);
    }
    bool try_lock()
    {
        return bthread_rwlock_trywrlock(&lock_) == 0;
    }

    void lock_shared()
    {
        (void)bthread_rwlock_rdlock(&lock_);
    }
    void unlock_shared()
    {
        (void)bthread_rwlock_unlock(&lock_);
    }
    bool try_lock_shared()
    {
        return bthread_rwlock_tryrdlock(&lock_) == 0;
    }

private:
    bthread_rwlock_t lock_;
};

template <typename Rep, typename Period>
inline void sleep_for(std::chrono::duration<Rep, Period> d)
{
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(d);
    if (us < d) {
        us += std::chrono::microseconds(1);
    }
    if (us.count() > 0) {
        (void)bthread_usleep(us.count());
    }
}

template <typename Clock, typename Dur>
inline void sleep_until(std::chrono::time_point<Clock, Dur> tp)
{
    const auto now = Clock::now();
    if (tp > now) {
        sleep_for(tp - now);
    }
}

#else  // cmake mode: KVClient SDK ships without brpc/bthread headers, so keep
       // the std primitives untouched. The kvtest:: aliases let pipeline code
       // stay source-identical across both build modes.

using thread = std::thread;
using mutex = std::mutex;
using condition_variable = std::condition_variable;
using shared_mutex = std::shared_mutex;

template <typename Rep, typename Period>
inline void sleep_for(std::chrono::duration<Rep, Period> d)
{
    std::this_thread::sleep_for(d);
}

template <typename Clock, typename Dur>
inline void sleep_until(std::chrono::time_point<Clock, Dur> tp)
{
    std::this_thread::sleep_until(tp);
}

#endif

}  // namespace kvtest
