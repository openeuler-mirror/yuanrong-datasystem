/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Lock Map for fine grained locking without tbb
 */

#ifndef DATASYSTEM_LOCK_MAP_H
#define DATASYSTEM_LOCK_MAP_H

#include <algorithm>
#include <mutex>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {

template <typename W, typename R>
class LockMap {
public:
    typedef struct {
        std::shared_mutex mux;
        std::atomic<uint64_t> ref {0}; // number of threads waiting on mux
        R data;
        bool pendingDelete = false;
        std::condition_variable_any cv;
    } Entry;

    // Exclusive access
    class Accessor {
    public:
        ~Accessor()
        {
            Release();
        }
        /**
         * @brief Release access
         */
        void Release()
        {
            if (lock) {
                if (--entry->ref == 1 && entry->pendingDelete) {
                    entry->cv.notify_one(); // notify pending erase if entry is the last one
                }
                lock.reset();
                entry = nullptr;
            }
        }
        
        Entry *entry = nullptr;
    private:
        std::unique_ptr<std::unique_lock<std::shared_mutex>> lock;
        W key;
        friend class LockMap;
    };

    // Shared access
    class ConstAccessor {
    public:
        ~ConstAccessor()
        {
            Release();
        }
        /**
         * @brief Release access
         */
        void Release()
        {
            if (lock) {
                if (--entry->ref == 1 && entry->pendingDelete) {
                    entry->cv.notify_one(); // notify pending erase if entry is the last one
                }
                lock.reset();
                entry = nullptr;
            }
        }

        Entry *entry = nullptr;
    private:
        std::unique_ptr<std::shared_lock<std::shared_mutex>> lock;
        W key;
        friend class LockMap;
    };

    /**
     * @brief Find the entry in the map. Locks the entry with exclusive lock.
     * @return True if entry found, False if entry not found.
     */
    bool Find(Accessor &accessor, const W &key)
    {
        std::unique_lock<std::mutex> glock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto &entry = it->second;
            entry.ref++;
            glock.unlock();
            accessor.entry = &entry;
            accessor.key = key;
            accessor.lock = std::make_unique<std::unique_lock<std::shared_mutex>>(entry.mux);
            return true;
        }
        return false;
    }

    /**
     * @brief Find or insert the entry to the map. Locks the entry with exclusive lock.
     *        Guarantee to be the first to hold the lock when entry is inserted.
     * @return True if entry is inserted, False if entry is not inserted
     */
    bool Insert(Accessor &accessor, const W &key)
    {
        std::unique_lock<std::mutex> glock(mutex_);
        auto res = map_.try_emplace(key);
        auto &entry = res.first->second;
        entry.ref++;
        if (res.second) {
            accessor.lock = std::make_unique<std::unique_lock<std::shared_mutex>>(entry.mux);
        }
        glock.unlock();
        accessor.entry = &entry;
        accessor.key = key;
        if (!res.second) {
            accessor.lock = std::make_unique<std::unique_lock<std::shared_mutex>>(entry.mux);
        }
        return res.second;
    }

    /**
     * @brief Find the entry in the map. Locks the entry with shared lock.
     * @return True if entry is found, False if entry is not found.
     */
    bool Find(ConstAccessor &accessor, const W &key)
    {
        std::unique_lock<std::mutex> glock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto &entry = it->second;
            entry.ref++;
            glock.unlock();
            accessor.entry = &entry;
            accessor.key = key;
            accessor.lock = std::make_unique<std::shared_lock<std::shared_mutex>>(entry.mux);
            return true;
        }
        return false;
    }

    /**
     * @brief Find or insert the entry to the map. Locks the entry with shared lock.
     *        Guarantee to be the first to hold the lock when entry is inserted.
     * @return True if entry is inserted, False if entry is not inserted
     */
    bool Insert(ConstAccessor &accessor, const W &key)
    {
        std::unique_lock<std::mutex> glock(mutex_);
        auto res = map_.try_emplace(key);
        auto &entry = res.first->second;
        entry.ref++;
        if (res.second) {
            accessor.lock = std::make_unique<std::shared_lock<std::shared_mutex>>(entry.mux);
        }
        glock.unlock();
        accessor.entry = &entry;
        accessor.key = key;
        if (!res.second) {
            accessor.lock = std::make_unique<std::shared_lock<std::shared_mutex>>(entry.mux);
        }
        return res.second;
    }

    /**
     * @brief Try to erase the entry from the map. Releases the accessor if the entry is erased.
     * @return True if entry erased, False if entry is waiting to be accessed by other threads.
     */
    bool TryErase(Accessor &accessor)
    {
        // Check accessor is valid
        if (accessor.lock && !accessor.entry->pendingDelete) {
            std::lock_guard<std::mutex> glock(mutex_);
            // Check if there are other threads waiting on it
            if (accessor.entry->ref == 1) {
                accessor.Release();
                map_.erase(accessor.key);
                return true;
            }
        }
        return false;
    }
    
    /**
     * @brief Erase the entry from the map. Blocks until entry is erased. Releases the accessor if the entry is erased.
     * @return True if entry erased, False if entry is waiting to be accessed by other threads.
     */
    void BlockingErase(Accessor &accessor)
    {
        // Check accessor is valid and not being deleted
        if (accessor.lock && !accessor.entry->pendingDelete) {
            accessor.entry->pendingDelete = true;
            std::unique_lock<std::mutex> glock(mutex_);
            // Try to remove the entry
            if (accessor.entry->ref == 1) {
                accessor.Release();
                map_.erase(accessor.key);
                return;
            }
            // Wait for other threads to finish
            while (accessor.entry->ref != 1) {
                glock.unlock();
                accessor.entry->cv.wait(*accessor.lock);
                glock.lock();
            }
            // No more threads waiting, remove the entry
            accessor.Release();
            map_.erase(accessor.key);
        }
    }

    /**
     * @brief Get the size of the map
     * @return size
     */
    size_t Size()
    {
        std::lock_guard<std::mutex> glock(mutex_);
        return map_.size();
    }

    /**
     * @brief begin iterator
     * @return begin iterator of the inner map
     */
    auto begin()
    {
        return map_.begin();
    }
    
    /**
     * @brief end iterator
     * @return end iterator of the inner map
     */
    auto end()
    {
        return map_.end();
    }

private:
    std::mutex mutex_;
    std::unordered_map<W, Entry> map_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_LOCK_MAP_H
