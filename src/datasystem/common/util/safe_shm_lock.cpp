/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#include "datasystem/common/util/safe_shm_lock.h"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {
namespace {
timespec MilliSecondsToTimeSpec(uint64_t timeoutMs)
{
    static constexpr uint64_t ONE_K = 1'000ul;
    static constexpr uint64_t ONE_M = 1'000'000ul;
    timespec t{ .tv_sec = static_cast<__time_t>(timeoutMs / ONE_K),
                .tv_nsec = static_cast<__syscall_slong_t>((timeoutMs % ONE_K) * ONE_M) };
    return t;
}
}  // namespace

SafeShmLock::SafeShmLock(uint32_t *lockWord, uint32_t lockId) : lockWord_(lockWord), lockId_(lockId | LOCK_ID_TAG)
{
}

Status SafeShmLock::Lock(uint64_t timeoutMs)
{
    Timer timer;
    Status rc;
    bool isFirstTimeout = false;
    uint32_t expectedVal = (lockId_ << LOCK_ID_SHIFT) | WRITE_FLAG;
    uint64_t tryLockCounter = 0;
    const uint64_t futexThreshold = 32;
    do {
        uint32_t val = __atomic_load_n(lockWord_, __ATOMIC_SEQ_CST);
        if (val == 0) {
            // Compare and set lock word
            if (__atomic_compare_exchange_n(lockWord_, &val, expectedVal, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
                break;
            }
        }
        tryLockCounter++;
        if (tryLockCounter <= futexThreshold && timeoutMs > 0) {
            continue;
        }
        auto elapsedMs = timer.ElapsedMilliSecond();
        if (elapsedMs > TIMEOUT_WARNING_LIMIT_MS && !isFirstTimeout) {
            isFirstTimeout = true;
            LOG(WARNING) << "Fetching lock on shared memory takes more than " << TIMEOUT_WARNING_LIMIT_MS
                         << " ms, current lockWord:" << val;
        }

        CHECK_FAIL_RETURN_STATUS(elapsedMs < timeoutMs, K_TRY_AGAIN, FormatString("Timeout after %zu ms", timeoutMs));
        auto remainingMs = std::min<uint64_t>(timeoutMs - elapsedMs, TIMEOUT_WARNING_LIMIT_MS);
        LOG_IF_ERROR(FutexWait(lockWord_, remainingMs), "FutexWait failed");
        // Reset tryLockCounter, so we get to retry a bit more.
        tryLockCounter = 0;
    } while (true);
    if (isFirstTimeout) {
        LOG(WARNING) << "Fetching lock on shared memory takes " << timer.ElapsedMilliSecond() << " ms";
    }
    return Status::OK();
}

void SafeShmLock::UnLock()
{
    (void)UnlockImpl(lockWord_, lockId_, false);
}

bool SafeShmLock::ForceUnlock(uint32_t *lockWord, uint32_t lockId)
{
    return UnlockImpl(lockWord, lockId | LOCK_ID_TAG, true);
}

bool SafeShmLock::UnlockImpl(uint32_t *lockWord, uint32_t lockId, bool force)
{
    uint32_t expectedContext = (lockId << LOCK_ID_SHIFT) | WRITE_FLAG;
    do {
        uint32_t val = __atomic_load_n(lockWord, __ATOMIC_SEQ_CST);
        uint32_t expectedVal = val & WAIT_MASK;
        uint32_t currContext = val & (~WAIT_MASK);
        // check the lockId and try unlock.
        if (expectedContext == currContext) {
            if (__atomic_compare_exchange_n(lockWord, &val, expectedVal, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
                LOG_IF_ERROR(FutexWake(lockWord), "FutexWake failed");
                return true;
            }
        } else {
            LOG_IF(WARNING, !force) << FormatString("Shared memory lock context not match, expect %zu, curr %zu",
                                                    expectedContext, currContext);
            return false;
        }
    } while (true);
}

Status SafeShmLock::FutexWait(uint32_t *lockWord, uint64_t timeoutMs)
{
    // If lock is not currently acquired, we do not need to futex wait.
    uint32_t val = __atomic_load_n(lockWord, __ATOMIC_SEQ_CST);
    RETURN_OK_IF_TRUE((val & WRITE_FLAG) == 0 || (val & (LOCK_ID_TAG << LOCK_ID_SHIFT)) == 0);

    auto t = MilliSecondsToTimeSpec(timeoutMs);
    auto beforeAdd = __atomic_fetch_add(lockWord, WAIT_NUM, __ATOMIC_SEQ_CST);
    int ret = 0;
    // call futex wait if the lock hold by the new shm lock.
    if ((beforeAdd & WRITE_FLAG) && (beforeAdd & (LOCK_ID_TAG << LOCK_ID_SHIFT))) {
        ret = syscall(SYS_futex, lockWord, FUTEX_WAIT, beforeAdd + WAIT_NUM, &t, nullptr, 0);
    }
    INJECT_POINT("FutexWait.wake");
    __atomic_fetch_sub(lockWord, WAIT_NUM, __ATOMIC_SEQ_CST);
    if (ret == 0 || errno == EAGAIN || errno == EINTR || errno == ETIMEDOUT) {
        return Status::OK();
    }
    RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Futex wait error. Errno = %d. Message %s", errno, StrErr(errno)));
}

Status SafeShmLock::FutexWake(uint32_t *lockWord, int numToWakeUp)
{
    auto waitFlag = __atomic_load_n(lockWord, __ATOMIC_SEQ_CST) & WAIT_MASK;
    RETURN_OK_IF_TRUE(waitFlag == 0);
    int ret = 0;
    RETRY_ON_EINTR_WITH_RET(syscall(SYS_futex, lockWord, FUTEX_WAKE, numToWakeUp, nullptr, nullptr, 0), ret);
    RETURN_OK_IF_TRUE(ret >= 0);
    RETURN_STATUS(K_RUNTIME_ERROR, FormatString("futex wake error. Errno = %d. Message %s", errno, StrErr(errno)));
}
}  // namespace datasystem
