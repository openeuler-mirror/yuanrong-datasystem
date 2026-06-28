/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Lock adapters shared by safe_object_brpc_migration_test and
// safe_object_contention_test. They mirror the production rwState_
// algorithm in safe_object.h so that the tests can exercise the
// lock primitive in isolation, without dragging in the SafeObject
// business logic (which would force linking common_object_cache
// and indirectly the rdma subsystem).
//
// **Maintenance contract**: SpinlockAdapter MUST stay byte-for-byte
// consistent with SafeObject's rwState_ algorithm (WLock / AcquireReadSpinlock).
// If you change one, change the other in the same commit. The CI cannot
// enforce this automatically; reviewer must check both files together.

#ifndef TESTS_ST_WORKER_OBJECT_CACHE_SAFE_OBJECT_LOCK_ADAPTERS_H
#define TESTS_ST_WORKER_OBJECT_CACHE_SAFE_OBJECT_LOCK_ADAPTERS_H

#include <atomic>

#include <bthread/bthread.h>
#include <bthread/rwlock.h>

namespace datasystem {
namespace st {

// Constants must match safe_object_rwlock namespace in safe_object.h.
namespace adapter_rwlock {
constexpr uint32_t WRITER_HELD = 0x80000000;
constexpr uint32_t READER_MASK = 0x1FFFFFFF;
constexpr int SPIN_BEFORE_YIELD = 64;
}  // namespace adapter_rwlock

class SpinlockAdapter {
public:
    void WLock()
    {
        // Phase 1: CAS WRITER_HELD bit on (allows reader_count > 0; new
        // readers will see WRITER_HELD=1 and spin).
        int spin = 0;
        while (true) {
            uint32_t cur = state_.load(std::memory_order_acquire);
            if ((cur & adapter_rwlock::WRITER_HELD) != 0) {
                if (++spin >= adapter_rwlock::SPIN_BEFORE_YIELD) {
                    bthread_yield();
                    spin = 0;
                }
                continue;
            }
            uint32_t desired = cur | adapter_rwlock::WRITER_HELD;
            if (state_.compare_exchange_weak(cur, desired,
                                             std::memory_order_acquire,
                                             std::memory_order_relaxed)) {
                break;
            }
            if (++spin >= adapter_rwlock::SPIN_BEFORE_YIELD) {
                bthread_yield();
                spin = 0;
            }
        }
        // Phase 2: wait for in-flight readers to drain.
        spin = 0;
        while ((state_.load(std::memory_order_acquire) & adapter_rwlock::READER_MASK) != 0) {
            if (++spin >= adapter_rwlock::SPIN_BEFORE_YIELD) {
                bthread_yield();
                spin = 0;
            }
        }
    }
    void WUnlock() { state_.fetch_and(~adapter_rwlock::WRITER_HELD, std::memory_order_release); }
    void RLock()
    {
        bool acquired = false;
        int spin = 0;
        while (!acquired) {
            uint32_t cur = state_.load(std::memory_order_acquire);
            if ((cur & adapter_rwlock::WRITER_HELD) != 0) {
                if (++spin >= adapter_rwlock::SPIN_BEFORE_YIELD) {
                    bthread_yield();
                    spin = 0;
                }
                continue;
            }
            if (state_.compare_exchange_weak(cur, cur + 1,
                                             std::memory_order_acquire,
                                             std::memory_order_relaxed)) {
                acquired = true;
            } else {
                if (++spin >= adapter_rwlock::SPIN_BEFORE_YIELD) {
                    bthread_yield();
                    spin = 0;
                }
            }
        }
    }
    void RUnlock() { state_.fetch_sub(1, std::memory_order_release); }

private:
    std::atomic<uint32_t> state_{ 0 };
};

class BthreadRwlockAdapter {
public:
    BthreadRwlockAdapter() { bthread_rwlock_init(&lock_, nullptr); }
    ~BthreadRwlockAdapter() { bthread_rwlock_destroy(&lock_); }
    BthreadRwlockAdapter(const BthreadRwlockAdapter &) = delete;
    BthreadRwlockAdapter &operator=(const BthreadRwlockAdapter &) = delete;
    void WLock() { bthread_rwlock_wrlock(&lock_); }
    void WUnlock() { bthread_rwlock_unlock(&lock_); }
    void RLock() { bthread_rwlock_rdlock(&lock_); }
    void RUnlock() { bthread_rwlock_unlock(&lock_); }

private:
    bthread_rwlock_t lock_;
};

}  // namespace st
}  // namespace datasystem

#endif  // TESTS_ST_WORKER_OBJECT_CACHE_SAFE_OBJECT_LOCK_ADAPTERS_H
