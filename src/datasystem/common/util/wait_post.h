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
 * Description: Simple event semaphore to synchronize threads.
 */
#ifndef DATASYSTEM_COMMON_UTIL_WAIT_POST_H
#define DATASYSTEM_COMMON_UTIL_WAIT_POST_H

#include <condition_variable>
#include <mutex>

#include "datasystem/utils/status.h"

namespace datasystem {
/**
 * A WaitPost is an implementation of <a
 * href="https://en.wikipedia.org/wiki/Event_(synchronization_primitive)">Event</a>.
 * In brief, it consists of a boolean state and provides methods to synchronize running threads.
 * <ol>
 * <li> Wait(). If the boolean state is false, the calling threads will block until
 * the boolean state becomes true.
 * <li> WaitFor(uint64_t timeout). Similar to Wait but with a timeout in millisecond. Return true if
 * the boolean state becomes true before timeout.
 * <li> Set(). Change the boolean state to true. All blocking threads will be released.
 * <li> Clear(). Reset the boolean state back to false.
 * </ol>
 */
class WaitPost final {
public:
    WaitPost();
    ~WaitPost() = default;
    /**
     * @brief Wait for the event to happen.
     */
    void Wait();

    /**
     * @brief Wait for the event to happen, then reset for next wait.
     */
    void WaitAndClear();

    /**
     * @brief Wait for the event to happen.
     * @param[in] timeoutMs In milliseconds.
     * @return true if the event happen before timeout. False otherwise.
     */
    bool WaitFor(uint64_t timeoutMs);

    /**
     * @brief Wait for the event to happen, if event has happened, reset for next wait.
     * @param[in] timeoutMs In milliseconds.
     * @return true if the event happen before timeout. False otherwise.
     */
    bool WaitForAndClear(uint64_t timeoutMs);

    /**
     * @brief Notify all blocking threads the event has happened.
     */
    void Set();

    /**
     * @brief Reset the event for reuse.
     */
    void Clear();

    /**
     * @brief Set the event as happened and attach a status information.
     *        This will wake up all threads waiting on this event and provide them with the status.
     * @param[in] status The status information to be passed to waiting threads, typically contains
     *                   success/failure information of an asynchronous operation.
     * @note This function is thread-safe and can be called from any thread.
     * @note After calling this function, all waiting threads will be unblocked and can retrieve
     *       the status information through WaitAndGetStatus().
     */
    void SetWithStatus(const Status &status);

    /**
     * @brief Wait for the event to happen and retrieve the associated status information.
     *        This function will block the calling thread until SetWithStatus() is called.
     * @return Status The status information that was set by SetWithStatus() call.
     *         Returns Status::OK() if no specific status was set.
     * @note This function is thread-safe and can be called from multiple threads.
     * @note If SetWithStatus() was already called before this wait, the function will return
     *       immediately with the stored status.
     * @warning The status information is only stored once per SetWithStatus() call. Subsequent
     *          calls to WaitAndGetStatus() will return the same status until SetWithStatus() is
     *          called again with a new status.
     */
    Status WaitAndGetStatus();

private:
    int val_;
    std::mutex mux_;
    std::condition_variable cv_;

    Status status_ = Status::OK();
};

class Barrier {
public:
    explicit Barrier(size_t count) : expectCount_(count)
    {
    }

    /**
     * @brief Wait until the count meets condition.
     */
    void Wait();

private:
    const size_t expectCount_;
    size_t currCount_{ 0 };
    uint32_t generation_{ 0 };
    std::mutex mux_;
    std::condition_variable cv_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_WAIT_POST_H
