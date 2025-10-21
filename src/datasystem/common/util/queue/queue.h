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
 * Description: Simple queue to pass data among threads.
 */
#ifndef DATASYSTEM_COMMON_UTIL_QUEUE_H
#define DATASYSTEM_COMMON_UTIL_QUEUE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
static constexpr int QUE_NO_TIMEOUT = -1;  // No time out. Must match epoll_wait.
/**
 * @brief It is a simple thread safe fixed size queue for the producer/consumer problem.
 * The name of the APIs are mostly taken from the Java BlockingQueue counterpart.
 * <ol>
 * <li> Put -- Push an element into the back of the queue. The operation blocks if the queue is full.
 * <li> Add -- Like Put but return error if the queue is full.
 * <li> Offer -- Like Put but with a timeout. Return error if the insert is not successful before the timeout expires..
 * <li> Take -- Pop an element from the front of the queue The operation blocks if the queue is empty.
 * <li> Remove -- Like Take but return error if the queue is empty.
 * <li> Poll -- Like Take but with a timeout. Return error if the pop is not successful before the timeout expires.
 * </ol>
 * @tparam T Type of element of the queue.
 */
template <typename T>
class Queue final {
public:
    typedef T value_type;
    typedef T *pointer;
    typedef const T *const_pointer;
    typedef T &reference;
    typedef const T &const_reference;

    explicit Queue(size_t capacity) : capacity_(capacity), head_(0), tail_(0), hwm_(0)
    {
        buf_ = std::make_unique<T[]>(capacity_);
    }

    void Reset(size_t capacity)
    {
        capacity_ = capacity;
        buf_ = std::make_unique<T[]>(capacity_);
        head_ = 0;
        tail_ = 0;
        hwm_ = 0;
    }

    ~Queue() = default;
    // Disable all copy and move constructors.
    Queue(const Queue &) = delete;
    Queue(Queue &&other) noexcept = delete;
    Queue &operator=(const Queue &) = delete;
    Queue &operator=(Queue &&other) noexcept = delete;

    /**
     *
     * @return Capacity of the queue.
     */
    auto Capacity() const
    {
        return capacity_;
    }

    /**
     *
     * @return Number of elements in the queue.
     */
    auto Size() const
    {
        return tail_ - head_;
    }

    /**
     *
     * @return If the queue is empty.
     */
    auto Empty() const
    {
        return head_ == tail_;
    }

    /**
     * @return the high water mark ratio.
     */
    float GetHWMRatio()
    {
        if (capacity_ == 0) {
            LOG(ERROR) << "GetHWMRatio failed, capacity is 0";
            return 0.0;
        }
        float ratio = static_cast<float>(hwm_.load()) / static_cast<float>(capacity_);
        hwm_ = 0;
        return ratio;
    }

    /**
     * @brief Push an element to the end of the queue.
     * It is a blocking call if the queue is full.
     * @param[in] ele Element to be pushed.
     * @return Status of the call, only K_OK as of now.
     */
    Status Put(const_reference ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "Put failed, capacity is 0");
        cvFull_.wait(lock, IsNotFull);
        auto k = tail_++ % capacity_;
        buf_[k] = ele;
        // Update HWM
        hwm_ = std::max(hwm_.load(), Size());
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the queue. Return error if the queue is full.
     * @param[in] ele Element to be added.
     * @return Status K_OK or K_TRY_AGAIN (if queue is full).
     */
    Status Add(const_reference ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (IsNotFull()) {
            CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero, add failed");
            auto k = tail_++ % capacity_;
            buf_[k] = ele;
            // Update HWM
            hwm_ = std::max(hwm_.load(), Size());
            cvEmpty_.notify_all();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is full");
        }
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the queue with a timeout.
     * @param[in] ele Element to be pushed.
     * @param[in] timeout in milliseconds
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Offer(const_reference ele, uint64_t timeout)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotFull);
        if (IsNotFull()) {
            CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero, offer failed");
            auto k = tail_++ % capacity_;
            buf_[k] = ele;
            // Update HWM
            hwm_ = std::max(hwm_.load(), Size());
            cvEmpty_.notify_all();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                          "The queue is full within allowed time: " + std::to_string(timeout) + " ms");
        }
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the queue.
     * It is a blocking call if the queue is full.
     * @param[in] ele Element to be pushed.
     * @return Status object.
     */
    Status Put(T &&ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero, put failed");
        cvFull_.wait(lock, IsNotFull);
        auto k = tail_++ % capacity_;
        buf_[k] = std::forward<T>(ele);
        // Update HWM
        hwm_ = std::max(hwm_.load(), Size());
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the queue. Return error if the queue is full.
     * @param[in] ele Element to be pushed.
     * @return Status K_OK or K_TRY_AGAIN (if queue is full).
     */
    Status Add(T &&ele)
    {
        std::unique_lock<std::mutex> lock(mux_);
        if (IsNotFull()) {
            CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero failed");
            auto k = tail_++ % capacity_;
            buf_[k] = std::forward<T>(ele);
            // Update HWM
            hwm_ = std::max(hwm_.load(), Size());
            cvEmpty_.notify_all();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is full");
        }
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the queue with a timeout.
     * @param[in] ele Element to be pushed.
     * @param[in] timeout (in milliseconds).
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Offer(T &&ele, uint64_t timeout)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotFull);
        if (IsNotFull()) {
            CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "offer failed, division by zero");
            auto k = tail_++ % capacity_;
            buf_[k] = std::forward<T>(ele);
            // Update HWM
            hwm_ = std::max(hwm_.load(), Size());
            cvEmpty_.notify_all();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                          "The queue is full within allowed time: " + std::to_string(timeout) + " ms");
        }
        return Status::OK();
    }

    /**
     * @brief Push an element to the end of the queue by simply calling the constructor of T in place.
     * It is a blocking call if the queue is full.
     * @param[in] ele Element to be pushed.
     * @return Status object.
     */
    template <typename... Ts>
    Status EmplaceBack(Ts &&...args) noexcept
    {
        std::unique_lock<std::mutex> lock(mux_);
        CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero, emplace failed");
        cvFull_.wait(lock, IsNotFull);
        auto k = tail_++ % capacity_;
        new (&(buf_[k])) T(std::forward<Ts>(args)...);
        // Update HWM
        hwm_ = std::max(hwm_.load(), Size());
        cvEmpty_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the queue.
     * It is a blocking call if the queue is empty.
     * @param[out] p The pointer to take in popped element.
     * @return Status object.
     */
    Status Take(pointer p)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(p);
        std::unique_lock<std::mutex> lock(mux_);
        CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero, take failed");
        cvEmpty_.wait(lock, IsNotEmpty);
        auto k = head_++ % capacity_;
        *p = std::move(buf_[k]);
        cvFull_.notify_all();
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the queue. Return error if the queue is empty.
     * @param[out] p The pointer to take in popped element.
     * @return Status K_OK or K_TRY_AGAIN (if queue is empty).
     */
    Status Remove(pointer p)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(p);
        std::unique_lock<std::mutex> lock(mux_);
        if (IsNotEmpty()) {
            CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero, remove failed");
            auto k = head_++ % capacity_;
            *p = std::move(buf_[k]);
            cvFull_.notify_all();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The queue is empty");
        }
        return Status::OK();
    }

    /**
     * @brief Pop an element from the front of the queue. Return error if timeout expires.
     * @param[out] p The pointer to take in popped element.
     * @param[in] timeout in milliseconds.
     * @return Status K_OK or K_TRY_AGAIN (if timeout expires).
     */
    Status Poll(pointer p, uint64_t timeout)
    {
        RETURN_RUNTIME_ERROR_IF_NULL(p);
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotEmpty);
        if (IsNotEmpty()) {
            CHECK_FAIL_RETURN_STATUS(capacity_ != 0, K_RUNTIME_ERROR, "division by zero");
            auto k = head_++ % capacity_;
            *p = std::move(buf_[k]);
            cvFull_.notify_all();
        } else {
            RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                          "The queue is empty within allowed time: " + std::to_string(timeout) + " ms");
        }
        return Status::OK();
    }

    /**
     * @brief A wrapper to call different variation of send
     * @param[in] ele
     * @param[in] timeout
     * @return Status
     */
    Status Send(const_reference ele, int timeout)
    {
        if (timeout > 0) {
            return Offer(ele, timeout);
        } else if (timeout == QUE_NO_TIMEOUT) {
            return Put(ele);
        } else {
            return Add(ele);
        }
    }

    /**
     * @brief A wrapper to call different variation of send
     * @param[in] ele
     * @param[in] timeout
     * @return Status
     */
    Status Send(T &&ele, int timeout)
    {
        if (timeout > 0) {
            return Offer(std::forward<T>(ele), timeout);
        } else if (timeout == QUE_NO_TIMEOUT) {
            return Put(std::forward<T>(ele));
        } else {
            return Add(std::forward<T>(ele));
        }
    }

    /**
     * @brief A wrapper to call different variation of receive
     * @param ele
     * @param timeout
     * @return
     */
    Status Recv(reference ele, int timeout)
    {
        if (timeout > 0) {
            return Poll(&ele, timeout);
        } else if (timeout == QUE_NO_TIMEOUT) {
            return Take(&ele);
        } else {
            return Remove(&ele);
        }
    }

    /**
     * @brief Same as Poll but not popping any element.
     * @param[in] timeout Timeout.
     * @return Status of the call.
     */
    Status PollRecv(uint64_t timeout)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvEmpty_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotEmpty);
        RETURN_OK_IF_TRUE(IsNotEmpty());
        RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                      "The queue is empty within allowed time: " + std::to_string(timeout) + " ms");
    }

    /**
     * @brief Same as Offer but not inserting any element.
     * @param[in] timeout Timeout of the call.
     * @return Status of the call.
     */
    Status PollSend(uint64_t timeout)
    {
        std::unique_lock<std::mutex> lock(mux_);
        cvFull_.wait_for(lock, std::chrono::milliseconds(timeout), IsNotFull);
        RETURN_OK_IF_TRUE(IsNotFull());
        RETURN_STATUS(StatusCode::K_TRY_AGAIN,
                      "The queue is full within allowed time: " + std::to_string(timeout) + " ms");
    }

private:
    std::unique_ptr<T[]> buf_;
    size_t capacity_;
    size_t head_;
    size_t tail_;
    std::atomic<size_t> hwm_;
    std::mutex mux_;
    std::condition_variable cvEmpty_;
    std::condition_variable cvFull_;
    std::function<bool()> IsNotFull = [this]() -> bool { return Size() != Capacity(); };
    std::function<bool()> IsNotEmpty = [this]() -> bool { return !Empty(); };
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_QUEUE_H
