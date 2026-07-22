/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Cooperative cancellation token used by topology executor callbacks.
 */
#ifndef DATASYSTEM_CLUSTER_EXECUTOR_CANCELLATION_TOKEN_H
#define DATASYSTEM_CLUSTER_EXECUTOR_CANCELLATION_TOKEN_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace datasystem::cluster {

/**
 * @brief Executor-owned cooperative cancellation signal.
 */
class CancellationToken final {
public:
    /**
     * @brief Destroy the token.
     */
    ~CancellationToken() = default;

    /**
     * @brief Return true after cancellation.
     * @return Cooperative cancellation state.
     */
    bool IsCancelled() const noexcept
    {
        return cancelled_.load();
    }

    /**
     * @brief Wait until cancellation or deadline.
     * @param[in] deadline Absolute deadline.
     * @return True when cancelled; false on deadline.
     */
    bool WaitUntil(std::chrono::steady_clock::time_point deadline) const
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_until(lock, deadline, [this] { return cancelled_.load(); });
    }

private:
    friend class TopologyTaskExecutor;

    /**
     * @brief Request cooperative cancellation and wake waiters.
     */
    void Cancel() noexcept
    {
        cancelled_.store(true);
        changed_.notify_all();
    }

    std::atomic<bool> cancelled_{ false };
    // Coordinates changed_ waiters with cancelled_; cancelled_ remains the authoritative atomic state.
    mutable std::mutex mutex_;
    // Uses mutex_ to wake WaitUntil() when cancelled_ becomes true.
    mutable std::condition_variable changed_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_EXECUTOR_CANCELLATION_TOKEN_H
