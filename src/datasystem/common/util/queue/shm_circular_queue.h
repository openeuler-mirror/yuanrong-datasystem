/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Based on shared memory circular queue management.
 */

#ifndef DATASYSTEM_COMMON_UTIL_SHM_CIRCULAR_QUEUE_H
#define DATASYSTEM_COMMON_UTIL_SHM_CIRCULAR_QUEUE_H

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/strings_util.h"

#define FUTEX_RETRY_ON_EINTR(result, statement)                   \
    do {                                                          \
        int cnt_ = 0;                                             \
        do {                                                      \
            (result) = (statement);                               \
            cnt_++;                                               \
        } while ((result) == -1 && errno == EINTR && cnt_ <= 10); \
    } while (0)

namespace datasystem {

/**
 * @brief The struct describes what the circular queue looks like.
 */
struct QueueInfo {
    uint32_t capacity = 2048;
    uint32_t elementFlagSize = sizeof(uint32_t);  // futex flag size
    uint32_t elementDataSize = 16;                // length of uuid
};

/**
 * The structure of circularQueueUnit_ :
 *
 * | qLock_(uint32_t) | lockId_(uint32_t)  | qSize_(uint32_t) | qHead_(uint32_t) | element(User-defined size) * n|
 *
 *      circularQueueUnit_: Pointer to this queue.
 *      queueLock_: Used for futex lock, protect the whole queue.
 *      lockId_(bitmap) : To mark which client is holding the lock.
 *      queueSize_: Stores the length of the queue and serves as a futex notification that the queue is empty and
                    the queue is full.
 *      queueHead_: The head of the circular queue.
 *      element: Circular queue of elements, the size and structure of the elements is user-defined, but all elements
                 are the same size.
 */

class ShmCircularQueue {
public:
    ShmCircularQueue(size_t capacity, uint32_t elementSize, std::shared_ptr<ShmUnitInfo> shmUnit, uint32_t lockId = 0,
                     bool isClient = false)
        : circularQueueUnit_(shmUnit),
          lockId_(lockId),
          isClient_(isClient),
          elementSize_(elementSize),
          cap_(capacity),
          head_(0),
          len_(0)
    {
    }

    ~ShmCircularQueue() = default;

    /**
     * @brief Do some init worker before use.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief To set the handler when user pop the element.
     * @param[in] callback The function of the data handler and the function needs to handle a uint8_t data type.
     * @return Status of the call.
     */
    Status SetGetDataHandler(std::function<void(uint8_t *)> callback);

    /**
     * @brief Load the data atomic from shared memory.
     */
    void UpdateQueueMeta();

    /**
     * @brief Use a Futex to notify the other process that it is ready to continue writing data.
     */
    void NotifyQueueNotFull();

    /**
     * @brief Use a Futex to notify the other process that it is ready to continue deal data.
     */
    void NotifyNotEmpty();

    /**
     * @brief Notifies an empty waiting process to end the while thread.
     */
    void WakeUpQueueProcessAndFinish();

    /**
     * @brief Wake up client process while server not exit.
     */
    void WakeUpClientProcessAndFinish();

    /**
     * @brief Check if the queue is in the process of being destroyed.
     * @return True means it is being destroyed.
     */
    bool CheckQueueDestroyed()
    {
        return destroyFlag_;
    }

    /**
     * @brief If the queue is empty it falls into a wait
     * @param[in] timeoutSec Futex wait timeout setting, usually 60s
     * @return Status of the call.
     */
    Status WaitForQueueEmpty(const timespec &timeoutSec);

    /**
     * @brief If the queue is full it falls into a wait
     * @param[in] timeoutSec Futex wait timeout setting, usually 60s
     * @return Status of the call.
     */
    Status WaitForQueueFull(const timespec &timeoutSec);

    /**
     * @brief Clear the queue meta info.
     */
    void Clear();

    /**
     * @brief Support multiple process to occupy a slot in this queue first.
     * @param[out] slotIndex User can set an index on the data that will not be set by other process.
     * @return True if we get the slot, False if the queue is full and not get the slot.
     */
    bool GetSlotUntilSuccess(uint32_t &slotIndex);

    /**
     * @brief Push data to the specified slot.
     * @param[in] slotIndex The slot that need to be push.
     * @param[in] value The value need to pushed.
     * @param[in] valueSize The size of the value to be pushed.
     * @param[out] outShmPtr The shm ptr that wrote the incoming data.
     * @return Status OK if success, otherwise is failed.
     */
    Status PushBySlot(uint32_t slotIndex, const char *value, uint32_t valueSize, uint8_t **outShmPtr);

    /**
     * @brief Get the front data and submit it to the user handler then pop it and loop until the queue runs out of
     * data.
     * @return Number of pop data, empty is zero.
     */
    uint32_t GetAndPopAll();

    /**
     * @brief Push data to queue, Not thread/process safe operate, only support one user push the data.
     * @param[in] value The value need to pushed.
     * @param[in] valueSize The size of the value to be pushed.
     * @return True if success, otherwise is failed.
     */
    bool Push(const char *value, uint32_t valueSize);

    /**
     * @brief Get the front data and submit it to the user handler, Not thread/process safe operate
     * @return True if success, otherwise is failed.
     */
    bool Front();

    /**
     * @brief Get the back data and submit it to the user handler, Not thread/process safe operate
     * @return True if success, otherwise is failed.
     */
    bool Back();

    /**
     * @brief Pop a data form the queue, Not thread/process safe operate, only support one user pop the data.
     * @param[in] count Number of pop-up data, default is 1.
     * @return True if success, otherwise is failed.
     */
    bool Pop(size_t count = 1);

    /**
     * @brief Get the length of the queue.
     * @return The size of length which stored in sys-memory.
     */
    uint32_t Length()
    {
        return len_;
    }

    /**
     * @brief Get the capacity of the queue.
     * @return The size of capacity.
     */
    uint32_t Capacity()
    {
        return cap_;
    }

    /**
     * @brief Check if the queue is full, compare by sys-memory data.
     * @return True if check success.
     */
    bool IsFull()
    {
        return len_ == cap_;
    }

    /**
     * @brief Shared lock for this shm queue.
     * @param[in] timeoutSec Timeout second for lock wait time.
     * @return Status of the call.
     */
    Status SharedLock(uint64_t timeoutSec = 60);

    /**
     * @brief Unlock shared lock for this shm queue.
     */
    void SharedUnlock();

    /**
     * @brief Unlock shared lock for specific lockId.
     * @param[in] lockId The id which may locked by shared lock.
     */
    void TrySharedUnlockByLockId(uint32_t lockId);

    /**
     * @brief Write lock for this shm queue.
     * @param[in] timeoutSec Timeout second for lock wait time.
     * @return Status of the call.
     */
    Status WriteLock(uint64_t timeoutSec = 60);

    /**
     * @brief Unlock write lock for this shm queue.
     */
    void WriteUnlock();

    /**
     * @brief Get the pointer information for shared memory communication
     * @param[out] fd File descriptor of the allocated shared memory segments.
     * @param[out] mmapSize Total size of shared memory segments.
     * @param[out] offset Offset from the base of the shared memory mmap.
     * @param[out] id The id of this shmUnit.
     * @return Status of the call.
     */
    Status GetQueueShmUnit(int &fd, uint64_t &mmapSize, ptrdiff_t &offset, ShmKey &id);

    /**
     * @brief Check futex result.
     * @param[in] res The result of futex.
     * @return Status of the call.
     */
    static Status CheckFutexErrno(long res);

private:
    /**
     * @brief Set the queue info to shm.
     * @param[in] needSetHead Whether or not to set the head's pos to shared memory.
     * @param[in] verifyHeadPos The verify pos for atomic compare.
     * @param[in] skipOne The number from last change, zero means change is 1.
     * @return Ture if it is success.
     */
    bool SetPosToShm(bool needSetHead = false, uint32_t verifyHeadPos = 0, uint32_t skipOne = 0);

    /**
     * @brief Set the queue length to shm.
     * @param[in] isAddChange The change is add or sub.
     * @param[in] skipOne The number from last change, zero means change is 1.
     * @return Ture if it is success.
     */
    bool SetQueueSizeToShm(bool isAddChange = true, uint32_t skipOne = 0);

    /**
     * @brief Set the queue head to shm.
     * @param[in] verifyHeadPos The verify pos for atomic compare.
     * @return Ture if it is success.
     */
    bool SetHeadPosToShm(uint32_t verifyHeadPos);

    std::shared_ptr<ShmUnitInfo> circularQueueUnit_{ nullptr };
    std::shared_ptr<object_cache::PackageLock> queueLock_;  // Protect head changed in different process
    uint32_t *queueSize_{ nullptr };
    uint32_t *queueHead_{ nullptr };
    uint8_t *arr_{ nullptr };
    // This callback needs to handle a pointer to uint8_t
    std::function<void(uint8_t *)> dataHandler_{ nullptr };

    uint32_t lockId_;
    bool isClient_ = false;
    uint32_t elementSize_;
    uint32_t cap_;
    uint32_t head_;
    uint32_t len_;
    bool destroyFlag_ = false;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_UTIL_SHM_CIRCULAR_QUEUE_H
