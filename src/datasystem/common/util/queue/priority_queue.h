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
 * Description: Priority queue based on heap.
 */
#ifndef DATASYSTEM_COMMON_UTIL_PRIORITY_QUEUE_H
#define DATASYSTEM_COMMON_UTIL_PRIORITY_QUEUE_H

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <type_traits>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
template <typename Func, typename T>
struct FunctorChecker : std::false_type {
};

template <typename Obj, typename T>
struct FunctorChecker<bool (Obj::*)(const T &, const T &), T> : std::true_type {
};

template <typename Obj, typename T>
struct FunctorChecker<bool (Obj::*)(const T &, const T &) const, T> : std::true_type {
};

template <typename T, typename C = std::less<T>>
class PriorityQueue {
public:
    static_assert(FunctorChecker<decltype(&C::operator()), T>::value, "Functor type does not match");

    explicit PriorityQueue(size_t capacity) : capacity_(capacity), size_(0), timeStamp_(0)
    {
        LOG(INFO) << "PriorityQueue capacity is :" << capacity_;
        buf_ = std::make_unique<Node[]>(capacity_ + 1);
    }
    ~PriorityQueue() = default;
    PriorityQueue(const PriorityQueue &) = delete;
    PriorityQueue &operator=(const PriorityQueue &) = delete;
    PriorityQueue(PriorityQueue &&) noexcept = delete;
    PriorityQueue &operator=(PriorityQueue &&) noexcept = delete;

    bool Empty()
    {
        std::unique_lock<std::mutex> lock(mux_);
        return size_ == 0;
    }

    bool Full()
    {
        std::unique_lock<std::mutex> lock(mux_);
        return size_ == capacity_;
    }

    size_t Size()
    {
        std::unique_lock<std::mutex> lock(mux_);
        return size_;
    }

    size_t Capacity()
    {
        std::unique_lock<std::mutex> lock(mux_);
        return capacity_;
    }

    /**
     * @brief Push an element to the end of the priority queue.
     * It is a blocking call if the queue is full.
     * @param[in] ele Element to be pushed.
     * @return Status of the call.
     */
    Status Put(const T &ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait(lock, IsNotFull);
        buf_[++size_] = std::move(Node(ele, timeStamp_++));
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the priority queue.
     * Return error if the queue is full.
     * @param[in] ele Element to be added.
     * @return Status K_OK or K_TRY_AGAIN (if queue is full)
     */
    Status Add(const T &ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (size_ == capacity_) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is full");
        }
        buf_[++size_] = std::move(Node(ele, timeStamp_++));
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the priority queue with a timeout.
     * @param[in] ele Element to be pushed.
     * @param[in] timeout (in milliseconds).
     * @return Status K_OK or K_Try_Again (if timeout expires).
     */
    Status Offer(const T &ele, uint64_t timeout)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotFull);
        if (size_ == capacity_) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is full");
        }
        buf_[++size_] = std::move(Node(ele, timeStamp_++));
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the priority queue.
     * It is a blocking call if the queue is full.
     * @param[in] ele Element to be put.
     * @return Status of the call.
     */
    Status Put(T &&ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait(lock, IsNotFull);
        buf_[++size_] = std::move(Node(std::forward<T>(ele), timeStamp_++));
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the priority queue.
     * Return error if the queue is full.
     * @param[in] ele Element to be pushed.
     * @return Status K_OK or K_Try_Again (if queue is full).
     */
    Status Add(T &&ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (size_ == capacity_) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is full");
        }
        buf_[++size_] = std::move(Node(std::forward<T>(ele), timeStamp_++));
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the priority queue with a timeout.
     * @param[in] ele Element to be pushed.
     * @param[in] timeout (in milliseconds).
     * @return Status OK or TryAgain (if timeout expires).
     */
    Status Offer(T &&ele, uint64_t timeout)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotFull);
        if (size_ == capacity_) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is full");
        }
        buf_[++size_] = std::move(Node(std::forward<T>(ele), timeStamp_++));
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the priority queue by simply calling the constructor of T in place.
     * It is a blocking call if the queue is full.
     * @param[in/out] args Args of the call.
     * @return Status of the call.
     */
    template <typename... Ts>
    Status EmplaceBack(Ts &&... args) noexcept
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait(lock, IsNotFull);
        new (&(buf_[size_ + 1].element)) T(std::forward<Ts>(args)...);
        buf_[size_ + 1].timeStamp = timeStamp_++;
        ++size_;
        RETURN_IF_NOT_OK(SiftUp(size_));
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the priority queue.
     * It is a blocking call if the queue is empty.
     * @param[out] ptr Pointer used to take in the popped element.
     * @return Status object.
     */
    Status Take(T *ptr)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(ptr);
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait(lock, IsNotEmpty);
        RETURN_IF_NOT_OK(PopAndSiftDown(ptr));
        cvFull_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the priority queue. Return error if the queue is empty.
     * @param[out] ptr Pointer used to take in the popped element.
     * @return Status K_OK or K_TRY_AGAIN (if queue is empty).
     */
    Status Remove(T *ptr)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(ptr);
        std::unique_lock<std::mutex> lock(mux_);
        if (size_ == 0) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is empty");
        }
        RETURN_IF_NOT_OK(PopAndSiftDown(ptr));
        cvFull_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the priority queue. Return error if timeout expires.
     * @param[out] ptr Pointer used to take in the popped element.
     * @param[in] timeout (in milliseconds).
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Poll(T *ptr, uint64_t timeout)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(ptr);
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotEmpty);
        if (size_ == 0) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is empty");
        }
        RETURN_IF_NOT_OK(PopAndSiftDown(ptr));
        cvFull_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Peek the head of priority queue.
     * @param[out] ptr Reference to pointer to the const head element.
     * @return Status OK or TryAgain.
     */
    Status Peek(const T *&ptr)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (size_ == 0) {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is empty");
        }
        ptr = &buf_[1].element;
        return Status::OK();
    }

private:
    /**
     * @brief Pop the first element and sift queue down by 1.
     * @param[out] ptr Pointer to take in the popped element.
     * @return Status of the call.
     */
    Status PopAndSiftDown(T *ptr)
    {
        *ptr = std::move(buf_[1].element);
        if (size_-- > 1) {
            buf_[1] = std::move(buf_[size_ + 1]);
            RETURN_IF_NOT_OK(SiftDown(1));
        }
        return Status::OK();
    }

    /**
     * @brief Heap operation. Let a node with higher priority float.
     * @param[in] i Index (ranges from 1 to size_).
     * @return K_RUNTIME_ERROR: if i is less than 1 or larger than size of queue.
     *         K_OK: else.
     */
    Status SiftUp(size_t i)
    {
        if (i < 1 || i > size_) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Sifting up element " + std::to_string(i)
                                                           + " in priority queue of size " + std::to_string(size_));
        }
        // Heap is placed in a queue.
        // buf_[i / 2] is the parent of buf_[i].
        while (size_ > 1 && i > 1 && cmp_(buf_[i / 2], buf_[i])) {
            std::swap(buf_[i], buf_[i / 2]);
            i /= 2;
        }
        return Status::OK();
    }

    /**
     * @brief Heap operation. Let a node with lower priority sink.
     * @param[in] i Index (ranges from 1 to size_).
     * @return K_RUNTIME_ERROR: if i is less than 1 or larger than size of queue.
     *         K_OK: else.
     */
    Status SiftDown(size_t i)
    {
        if (i < 1 || i > size_) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Sifting down element " + std::to_string(i)
                                                           + " in priority queue of size " + std::to_string(size_));
        }
        while (size_ > 1) {
            size_t j = 2 * i;
            if (j > size_) {
                break;
            }
            if (j + 1 <= size_ && cmp_(buf_[j], buf_[j + 1])) {
                ++j;
            }
            if (!cmp_(buf_[i], buf_[j])) {
                break;
            }
            std::swap(buf_[i], buf_[j]);
            i = j;
        }
        return Status::OK();
    }

    // Data structure holding the element, and its priority and timestamp.
    struct Node {
        Node() = default;

        Node(const T &ele, uint64_t ti) : element(ele), timeStamp(ti)
        {
        }

        Node(T &&ele, uint64_t ti) : element(std::forward<T>(ele)), timeStamp(ti)
        {
        }

        ~Node() = default;

        // No copy constructor because T could be unique_ptr.
        Node(const Node &) = delete;
        Node &operator=(const Node &) = delete;

        Node(Node &&other) noexcept : element(std::move(other.element)), timeStamp(other.timeStamp)
        {
        }

        Node &operator=(Node &&other) noexcept
        {
            if (this != &other) {
                element = std::move(other.element);
                timeStamp = other.timeStamp;
            }
            return *this;
        }

        T element;
        uint64_t timeStamp{0};
    };

    class Comparator {
    public:
        Comparator() = default;
        ~Comparator() = default;
        bool operator()(const Node &n1, const Node &n2)
        {
            if (rawComparator_(n1.element, n2.element)) {
                return true;
            } else if (rawComparator_(n2.element, n1.element)) {
                return false;
            } else {
                return n1.timeStamp > n2.timeStamp;
            }
        }

    private:
        C rawComparator_;
    };

    std::function<bool()> IsNotFull = [this]() -> bool { return size_ != capacity_; };
    std::function<bool()> IsNotEmpty = [this]() -> bool { return size_ != 0; };

    std::unique_ptr<Node[]> buf_;  // The beginning with index = 0 is not part of the queue.
    Comparator cmp_;
    size_t capacity_;
    size_t size_;
    std::mutex mux_;
    std::condition_variable cvEmpty_;
    std::condition_variable cvFull_;
    uint64_t timeStamp_;  // The order in time when an element was pushed in.
};
}  // namespace datasystem

#endif