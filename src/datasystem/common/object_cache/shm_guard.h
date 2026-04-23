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
 * Description: Define of ShmGuard.
 */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_SHM_GUARD_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_SHM_GUARD_H

#include <atomic>
#include <cstdint>
#include <memory>

#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/shm_unit.h"

namespace datasystem {

class ShmGuard {
public:
    ShmGuard(std::shared_ptr<ShmUnit> shmUnit, size_t dataSize, size_t metaSize);
    void EnableSlowFreeObserve();

    /**
     * @brief Try to acquire read lock.
     * @param[in] retry Retry if failed.
     * @return Status of the call.
     */
    Status TryRLatch(bool retry = true);

    /**
     * @brief Transfer ShmUnit and locker to RpcMessage, ensure the ShmUnit will not be freed and modified while
     * the payload is being sent
     * @param[out] messages The rpc messages.
     * @param[in] offset Read the contents of the offset.
     * @param[in] size Read the contents of the size.
     * @return Status of the call.
     */
    Status TransferTo(std::vector<RpcMessage> &messages, const uint64_t offset = 0, const uint64_t size = 0);

    /**
     * @brief Call after RpcMessage release.
     * @param[in] data The data pointer.
     * @param[in] hint The pointer of Impl instance.
     */
    static void Free(void *data, void *hint);

    /**
     * @brief Check whether zero-copy TCP payload should be rejected because recent ShmGuard release is slow.
     * @param[out] remainingMs The remaining reject duration in milliseconds.
     * @return true if the circuit breaker is open.
     */
    static bool IsSlowFreeCircuitOpen(int64_t &remainingMs);

private:
    struct Impl {
        explicit Impl(std::shared_ptr<ShmUnit> shm);
        ~Impl();
        bool IsLastFrameOnRelease();
        // Keep the shared-memory allocation alive while any payload frame is still in flight.
        std::shared_ptr<ShmUnit> shmUnit;
        // Protect the shared-memory payload from concurrent modification while it is exposed to RPC.
        std::shared_ptr<object_cache::ShmLock> lock;
        // Thread id that acquired the read lock.
        std::thread::id tid;
        // Start timestamp in ms for the current zero-copy transfer.
        std::atomic<int64_t> transferStartTimeMs{ 0 };
        // Number of payload frames that still need to release this impl.
        std::atomic<uint32_t> remainingFrames{ 0 };
        // Whether this transfer should contribute to slow-free circuit statistics.
        std::atomic<bool> trackSlowFree{ false };
    };

    std::shared_ptr<Impl> impl_;
    size_t dataSize_;
    size_t metaSize_;
};
}  // namespace datasystem
#endif
