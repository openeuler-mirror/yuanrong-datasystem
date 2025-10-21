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
 * Description: Circular queue Management.
 */

#ifndef DATASYSTEM_SHARED_MEMORY_CIRCULAR_QUEUE_H
#define DATASYSTEM_SHARED_MEMORY_CIRCULAR_QUEUE_H

#include <exception>
#include <mutex>
#include <shared_mutex>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
// Thread-safe circular queue.
template <typename T>
class CircularQueue {
public:
    // Shared Memory Usage Only, user invokes by placement new.
    explicit CircularQueue(T *arrAddr) : arr_(arrAddr), head_(0), len_(0), cap_(0)
    {
        if (arr_ == nullptr) {
            LOG(ERROR) << "circular queue is nullptr";
        }
    }

    // Construct circular queue from preallocated memory for the underlying array.
    CircularQueue(size_t capacity, T *arrAddr, bool clear = true) : arr_(arrAddr), cap_(capacity)
    {
        // Recovery requires to not clear the previous state.
        if (clear) {
            len_ = 0;
            head_ = 0;
        }
    }

    virtual ~CircularQueue() = default;

    void Clear()
    {
        std::unique_lock<std::shared_mutex> lock(qMutex_);
        head_ = 0;
        len_ = 0;
    }

    template <typename T2>
    bool Push(T2 &&value)
    {
        std::unique_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        // If full, return bool to the caller to indicate that the queue is full.
        if (len_ == cap_) {
            LOG(ERROR) << "circular queue is full";
            return false;
        }

        // Assign the value to the right position in the array.
        arr_[(head_ + len_) % cap_] = std::forward<T2>(value);

        // Increment tail and len_.
        len_++;

        return true;
    }

    template <typename T2>
    bool BatchPush(const std::vector<T2> &valueList)
    {
        std::unique_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        // If full, return bool to the caller to indicate that the queue is full.
        if (cap_ - len_ < valueList.size()) {
            LOG(ERROR) << "circular queue is full";
            return false;
        }

        for (const auto &value : valueList) {
            // Assign the value to the right position in the array.
            arr_[(head_ + len_) % cap_] = value;

            // Increment tail and len_.
            len_++;
        }

        return true;
    }

    T Front()
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        // Error trying to pop an empty queue.
        if (len_ == 0) {
            std::string msg = "circular queue is empty";
            LOG(ERROR) << msg;
            throw std::out_of_range(msg);
        }

        // head_ points to the position to read.
        return arr_[head_];
    }

    void Print(std::string prefix = "")
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        LOG(INFO) << FormatString("Printing in %s, head_: %zu. tail: %zu. len_: %zu. cap_: %zu.", std::move(prefix),
                                  head_, (head_ + len_) % cap_, len_, cap_);
    }

    T Back()
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        if (len_ == 0) {
            std::string msg = "circular queue is empty";
            LOG(ERROR) << msg;
            throw std::out_of_range(msg);
        }
        return arr_[(head_ + len_ - 1) % cap_];
    }

    bool Pop(size_t count = 1)
    {
        std::unique_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        // Error trying to pop an empty queue.
        if (len_ < count) {
            LOG(ERROR) << "circular queue is empty";
            return false;
        }
        // Decrement len_ and increment head_.
        len_ -= count;
        head_ = (head_ + count) % cap_;
        return true;
    }

    template <typename T2>
    bool BatchFetchAndPop(std::vector<T2> &valueList, size_t count)
    {
        std::unique_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        if (len_ < count) {
            LOG(ERROR) << "circular queue is empty";
            return false;
        }
        while (count > 0) {
            valueList.push_back(arr_[head_]);  // save the front element to output vector
            len_ -= 1;                         // single element pop step 1
            head_ = (head_ + 1) % cap_;        // single element pop step 2
            count--;                           // loop counter
        }
        return true;
    }

    size_t Length()
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        return len_;
    }

    size_t Capacity()
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        return cap_;
    }

    size_t Remaining()
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        return cap_ - len_;
    }

    bool IsFull()
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        return len_ == cap_;
    }

    const T operator[](size_t idx) const
    {
        return AccessElement(idx);
    }

    T operator[](size_t idx)
    {
        return AccessElement(idx);
    }

protected:
    T *arr_;

private:
    void CheckCapNotEmpty() const
    {
        if (cap_ == 0) {
            std::string msg = "The capacity is zero";
            LOG(ERROR) << msg;
            throw std::out_of_range(msg);
        }
    }

    T AccessElement(size_t idx) const
    {
        std::shared_lock<std::shared_mutex> lock(qMutex_);
        CheckCapNotEmpty();
        if (idx >= len_) {
            std::string msg = "index out of bound";
            LOG(ERROR) << msg;
            throw std::out_of_range(msg);
        }
        return arr_[(head_ + idx) % cap_];
    }

    size_t head_;
    size_t len_;
    const size_t cap_;
    mutable std::shared_mutex qMutex_;
};

template <typename T>
class HeapCircularQ : public CircularQueue<T> {
public:
    explicit HeapCircularQ(size_t capacity) : CircularQueue<T>(capacity, nullptr, true), vec_(capacity)
    {
        this->arr_ = vec_.data();
    }

private:
    std::vector<T> vec_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_SHARED_MEMORY_CIRCULAR_QUEUE_H
