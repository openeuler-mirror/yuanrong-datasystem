#pragma once

#include "bthread_compat.h"
#include "simple_log.h"
#include <functional>
#include <queue>
#include <vector>

// Bounded worker-pool with std::thread-compatible API. Used by
// KVWorker::notifyPool_ (peer-notify offload) and NotifyDispatcher::
// notifyPool_ (async notify-pipeline execution).
//
// Selection: in bazel mode (KVTEST_USE_BRPC) the workers are bthreads
// (kvtest::thread -> bthread_start_background) and the queue mutex/cv are
// bthread-aware (kvtest::mutex -> bthread::Mutex, kvtest::condition_variable
// -> bthread::ConditionVariable). A worker waiting on the queue, or running
// a task that calls SDK Set/Get / brpc::Channel::CallMethod, yields its
// bthread instead of holding a pthread — the brpc M:N benefit. In cmake
// mode the kvtest:: aliases resolve to std primitives so the pre-installed
// SDK (no brpc headers) still builds unchanged. Submit / Stop / QueueSize
// and the bounded-concurrency contract are preserved verbatim in both
// modes — Submit never blocks, Stop drains the queue then joins workers,
// QueueSize reports pending (un-started) tasks under the pool mutex.
class ThreadPool {
public:
    explicit ThreadPool(int numThreads) {
        for (int i = 0; i < numThreads; i++) {
            workers_.emplace_back([this]() { WorkerLoop(); });
        }
    }

    ~ThreadPool() { Stop(); }

    void Submit(std::function<void()> task) {
        {
            std::lock_guard<kvtest::mutex> lock(mutex_);
            if (stopped_) return;
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }

    size_t QueueSize() {
        std::lock_guard<kvtest::mutex> lock(mutex_);
        return tasks_.size();
    }

    void Stop() {
        {
            std::lock_guard<kvtest::mutex> lock(mutex_);
            if (stopped_) return;
            stopped_ = true;
        }
        cv_.notify_all();
        for (auto &w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();
    }

    // Stop without draining: flip the stopped flag, drop queued (not-yet-
    // started) tasks, wake idle workers, and return immediately without
    // joining. A worker mid-task finishes that one task (cannot be forcibly
    // killed) then sees stopped_ and exits. Used by the /stop path so the
    // HTTP/bRPC control thread replies at once; the eventual join happens in
    // the normal Stop()/destructor on shutdown. Unlike Stop(), this does not
    // wait for in-flight tasks or queued tasks to drain.
    void StopNow() {
        {
            std::lock_guard<kvtest::mutex> lock(mutex_);
            if (stopped_) return;
            stopped_ = true;
            while (!tasks_.empty()) tasks_.pop();
        }
        cv_.notify_all();
    }

private:
    void WorkerLoop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<kvtest::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stopped_ || !tasks_.empty(); });
                if (stopped_) return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            try {
                task();
            } catch (const std::exception &e) {
                SLOG_ERROR("ThreadPool task threw: " << e.what());
            } catch (...) {
                SLOG_ERROR("ThreadPool task threw unknown exception");
            }
        }
    }

    std::vector<kvtest::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    kvtest::mutex mutex_;
    kvtest::condition_variable cv_;
    bool stopped_ = false;
};
