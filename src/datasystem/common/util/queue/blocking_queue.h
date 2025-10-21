/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Simple and inefficient blocking queue_.
 */
#ifndef DATASYSTEM_BLOCKING_QUEUE_H
#define DATASYSTEM_BLOCKING_QUEUE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <set>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
template <typename T>
class BlockingQueue {
public:
    BlockingQueue()
    {
        abortFlag_ = false;
    }
    void Abort()
    {
        {
            std::lock_guard<std::mutex> locker(mtx_);
            abortFlag_ = true;
        }
        this->emptyCond_.notify_all();
    }

    template <typename T2>
    void Push(T2 &&value)
    {
        std::unique_lock<std::mutex> lock(this->mtx_);
        queue_.push_front(std::forward<T>(value));
        this->emptyCond_.notify_one();
    }

    template <typename T2>
    void Push(std::set<T, T2> &&value)
    {
        std::unique_lock<std::mutex> lock(this->mtx_);
        queue_.insert(queue_.cbegin(), value.begin(), value.end());
        this->emptyCond_.notify_one();
    }

    Status Pop(T &retValue, double &totalIoSleepTime)
    {
        Timer timer;
        std::unique_lock<std::mutex> lock(this->mtx_);
        this->emptyCond_.wait(lock, [=] { return !this->queue_.empty() || abortFlag_; });
        if (abortFlag_) {
            RETURN_STATUS(StatusCode::K_INTERRUPTED, "Pop aborted");
        }
        T rc(std::move(this->queue_.back()));
        this->queue_.pop_back();
        totalIoSleepTime = timer.ElapsedSecond();
        retValue = rc;
        return Status::OK();
    }

    Status Pop(T &retValue)
    {
        std::unique_lock<std::mutex> lock(this->mtx_);
        this->emptyCond_.wait(lock, [=] { return !this->queue_.empty() || abortFlag_; });
        if (abortFlag_) {
            RETURN_STATUS(StatusCode::K_INTERRUPTED, "Pop aborted");
        }
        T rc(std::move(this->queue_.back()));
        this->queue_.pop_back();
        retValue = rc;
        return Status::OK();
    }

    size_t Size()
    {
        std::unique_lock<std::mutex> lock(this->mtx_);
        return queue_.size();
    }

    bool Empty()
    {
        std::unique_lock<std::mutex> lock(this->mtx_);
        return queue_.size() == 0;
    }

private:
    std::atomic<bool> abortFlag_{ false };
    std::deque<T> queue_;
    std::condition_variable emptyCond_;  // Signaled if queue_ is not empty.
    std::mutex mtx_;                     // For emptyCond_.
};
}  // namespace datasystem
#endif
