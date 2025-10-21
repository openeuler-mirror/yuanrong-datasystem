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

#ifndef DATASYSTEM_COMMON_UTIL_SAFE_SHM_LOCK_H
#define DATASYSTEM_COMMON_UTIL_SAFE_SHM_LOCK_H

#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdint>

#include "datasystem/utils/status.h"

namespace datasystem {
class SafeShmLock {
public:
    explicit SafeShmLock(uint32_t *lockWord, uint32_t lockId);

    Status Lock(uint64_t timeoutMs);
    void UnLock();

    static bool ForceUnlock(uint32_t *lockWord, uint32_t lockId);

private:
    static bool UnlockImpl(uint32_t *lockWord, uint32_t lockId, bool force);
    static Status FutexWait(uint32_t *lockWord, uint64_t timeoutMs);
    static Status FutexWake(uint32_t *lockWord, int numToWakeUp = INT_MAX);

    // bit0(WRITE_FLAG), bit1~bit15(wait count), bit16~bit31(lockId)
    uint32_t *lockWord_;
    uint32_t lockId_;
    constexpr static const uint32_t WRITE_FLAG = 1;
    constexpr static const uint32_t WAIT_NUM = 2;
    constexpr static const uint32_t LOCK_ID_SHIFT = 16;
    constexpr static const uint32_t WAIT_MASK = 0x0000FFFE;
    constexpr static const uint32_t LOCK_ID_TAG = 0x8000;
    constexpr static const uint32_t LOCK_ID_MASK = 0xFFFF0000;
    constexpr static const int TIMEOUT_WARNING_LIMIT_MS = 3000;
};
}  // namespace datasystem
#endif
