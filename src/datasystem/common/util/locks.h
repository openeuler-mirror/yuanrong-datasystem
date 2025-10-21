/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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
 * Description: A writer preference lock implementation.
 */
#ifndef DATASYSTEM_LOCKS_H
#define DATASYSTEM_LOCKS_H

#include <atomic>
#include <shared_mutex>
#include <thread>
#include <unordered_set>

namespace datasystem {
/**
 * @brief Write-preferring RW locks. The writer will block new readers while waiting for lock.
 */
class WriterPrefRWLock {
public:
    WriterPrefRWLock()
    {
        lockWord_ = 0;
    }

    ~WriterPrefRWLock() = default;

    inline void ReadLock()
    {
        CheckReader();
        while (true) {
            while (lockWord_ & WRITER) {
                // Block if there is a writer.
            };
            if (((lockWord_.fetch_add(READER) + READER) & WRITER) == 0) {
                AddReader();
                return;  // There was no writer when we read locked.
            }
            (void)lockWord_.fetch_sub(READER);  // There was a writer who beat us, retry.
        }
    }

    inline bool TryReadLock()
    {
        if (lockWord_ & WRITER) {
            return false;  // Return false if there is a writer.
        }
        if (((lockWord_.fetch_add(READER) + READER) & WRITER) == 0) {
            AddReader();
            return true;  // There was no writer when we read locked.
        }
        (void)lockWord_.fetch_sub(READER);  // There was a writer who beat us, undo read bit and return false.
        return false;
    }

    inline void ReadUnlock()
    {
        DelReader();
        (void)lockWord_.fetch_sub(READER);
    }

    inline void WriteLock()
    {
        while (true) {
            auto val = lockWord_.load();
            auto expected = val & ~WRITER;
            if (lockWord_.compare_exchange_strong(expected, val | WRITER)) {
                while (val & ~WRITER) {  // While there are still readers.
                    val = lockWord_.load();
                }

                return;
            }
        }
    }

    inline bool TryWriteLock()
    {
        auto val = lockWord_.load();
        auto expected = val & ~WRITER;
        if (lockWord_.compare_exchange_strong(expected, val | WRITER)) {
            val = lockWord_.load();
            if (!(val & ~WRITER)) {
                return true;
            }
            // Got the write bit, but there were readers. Undo the write bit and return false.
            (void)lockWord_.fetch_sub(WRITER);
        }
        return false;
    }

    inline bool IsWLocked() const
    {
        return (lockWord_.load(std::memory_order_relaxed) & WRITER);
    }

    inline void WriteUnlock()
    {
        (void)lockWord_.fetch_sub(WRITER);
    }

private:
#ifdef WITH_TESTS
    void AddReader()
    {
        std::lock_guard<std::shared_timed_mutex> locker(mutex_);
        readerThreadIds_.insert(std::this_thread::get_id());
    }

    void DelReader()
    {
        std::lock_guard<std::shared_timed_mutex> locker(mutex_);
        readerThreadIds_.erase(std::this_thread::get_id());
    }

    void CheckReader();

    std::unordered_set<std::thread::id> readerThreadIds_;
    std::shared_timed_mutex mutex_;
#else
    void AddReader()
    {
    }
    void DelReader()
    {
    }
    void CheckReader()
    {
    }
#endif
    // LSB is the write lock, higher order bits are a reader counter.
    std::atomic<unsigned long long> lockWord_ = { 0 };
    static const long long WRITER = 1LL;
    static const long long READER = 2LL;
};

// RAII interfaces for RWLock - create base lock class later for read preference.
class ReadLock {
public:
    explicit ReadLock(WriterPrefRWLock *lock) : lock_(lock)
    {
        lock_->ReadLock();
        locked_ = true;
    }

    ReadLock() : lock_(nullptr), locked_(false)
    {
    }

    ~ReadLock()
    {
        if (locked_) {
            lock_->ReadUnlock();
        }
    }

    bool UnlockIfLocked()
    {
        if (locked_) {
            lock_->ReadUnlock();
            locked_ = false;
            return true;
        }
        return false;
    }

    bool LockIfUnlocked()
    {
        if (!locked_) {
            lock_->ReadLock();
            locked_ = true;
            return true;
        }
        return false;
    }

    void Assign(WriterPrefRWLock *lock)
    {
        lock_ = lock;
    }

    bool TryLockIfUnlocked()
    {
        if (!locked_) {
            bool rc = lock_->TryReadLock();
            if (rc) {
                locked_ = true;
            }
            return rc;
        }
        return false;
    }

private:
    WriterPrefRWLock *lock_;
    bool locked_;
};

class WriteLock {
public:
    explicit WriteLock(WriterPrefRWLock *lock) : lock_(lock)
    {
        if (lock_ != nullptr) {
            lock_->WriteLock();
            locked_ = true;
        }
    }

    WriteLock() : lock_(nullptr), locked_(false)
    {
    }

    ~WriteLock()
    {
        if (locked_) {
            lock_->WriteUnlock();
        }
    }

    bool UnlockIfLocked()
    {
        if (locked_) {
            lock_->WriteUnlock();
            locked_ = false;
            return true;
        }
        return false;
    }

    bool LockIfUnlocked()
    {
        if (!locked_) {
            lock_->WriteLock();
            locked_ = true;
            return true;
        }
        return false;
    }

    void Assign(WriterPrefRWLock *lock)
    {
        lock_ = lock;
    }

    bool TryLockIfUnlocked()
    {
        if (!locked_) {
            bool rc = lock_->TryWriteLock();
            if (rc) {
                locked_ = true;
            }
            return rc;
        }
        return false;
    }

private:
    WriterPrefRWLock *lock_ = nullptr;
    bool locked_ = false;
};
}  // namespace datasystem
#endif
