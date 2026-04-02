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
 * Description: Testing SharedMemViewLock
 */
#include "datasystem/common/stream_cache/cursor.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {
class SharedMemViewLockTest : public CommonTest {
public:
    SharedMemView view_;
};

TEST_F(SharedMemViewLockTest, WriteLockTimeoutTest)
{
    // Protect the lock size from changes.
    const uint EXPECTED_SIZE_OF_SHAREDMEMVIEWLOCK = 4;
    ASSERT_EQ(sizeof(view_.lock_), EXPECTED_SIZE_OF_SHAREDMEMVIEWLOCK);

    // Simulate the following:
    // 1. Thread A holding the read lock for a long period of time.
    // 2. Thread B try to get a write lock but timeout.
    // 3. Read lock is obtainable after Thread A and Thread B finish.

    // Thread A
    const uint TWO_SECS = 2;
    ThreadPool pool(1);
    auto func = [this]() {
        SharedMemViewLock lock(&view_.lock_);
        return lock.LockSharedAndExec([]() { sleep(TWO_SECS); }, ONE_THOUSAND);
    };
    std::future<Status> fut = pool.Submit(func);

    sleep(1);

    // Thread B
    SharedMemViewLock lock(&view_.lock_);
    ASSERT_EQ(lock.LockExclusiveAndExec([]() { sleep(1); }, ONE_THOUSAND).GetCode(), K_TRY_AGAIN);

    // Wait for Thread A to finish.
    DS_ASSERT_OK(fut.get());

    // Should be able to get the read lock again as no one is waiting for the write lock.
    DS_ASSERT_OK(lock.LockSharedAndExec([]() { sleep(1); }, ONE_THOUSAND));
}

}  // namespace ut
}  // namespace datasystem