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

/**
 * Description: Define shared memory resource stats class.
 */
#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_RESOURCE_POOL_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_RESOURCE_POOL_H

#include <atomic>
#include <cstdint>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace memory {

class ResourcePool {
public:
    explicit ResourcePool(uint64_t limit = UINT64_MAX) : footprintLimit_(limit){};

    ~ResourcePool() = default;

    /**
     * @brief Add the usage with CAS.
     * @param[in] size The resource size.
     * @param[in] dynamicLimit The dynamic limit.
     * @return Status K_OK if succeed.
     */
    Status AddUsageCAS(uint64_t size, uint64_t dynamicLimit = UINT64_MAX);

    /**
     * @brief Add the real usage.
     * @param[in] size The resource size.
     * @return bool True if succeed.
     */
    bool AddRealUsage(uint64_t size);

    /**
     * @brief Add the real usage without limitation check.
     * @param[in] size The resource size.
     */
    void AddRealUsageNoCheck(uint64_t size);

    /**
     * @brief Sub the usage.
     * @param[in] size The resource size.
     */
    void SubUsage(uint64_t size)
    {
        (void)usage_.fetch_sub(size, std::memory_order_relaxed);
    }

    /**
     * @brief Sub the real usage.
     * @param[in] size The resource size.
     */
    void SubRealUsage(uint64_t size);

    /**
     * @brief Sub the usage with CAS.
     * @param[in] size The resource size.
     * @return bool True if succeed.
     */
    bool SubRealUsageCAS(uint64_t size);

    /**
     * @brief Get or update realUsage_.
     * @param[in] newUsage Update to newUsage if realUsage_ > 2^48 - 1.
     * @return uint64_t The realUsage_ value.
     */
    uint64_t GetOrUpdateRealUsage(uint64_t newUsage);

    /**
     * @brief Get the footprintLimit_.
     * @return uint64_t The footprintLimit_.
     */
    uint64_t FootprintLimit() const
    {
        return footprintLimit_;
    }

    /**
     * @brief Get the usage_.
     * @return uint64_t The usage_.
     */
    uint64_t Usage() const
    {
        return usage_.load(std::memory_order_acquire);
    }

    /**
     * @brief Get the realUsage_.
     * @return uint64_t The realUsage_.
     */
    uint64_t RealUsage() const
    {
        return realUsage_.load(std::memory_order_acquire);
    }

#ifdef WITH_TESTS
    /**
     * @brief Set the usage_ for test.
     * @param[in] usage The value to set.
     */
    void SetUsage(uint64_t usage)
    {
        usage_.store(usage, std::memory_order_release);
    }

    /**
     * @brief Set the realUsage_ for test.
     * @param[in] realUsage The value to set.
     */
    void SetRealUsage(uint64_t realUsage)
    {
        realUsage_.store(realUsage, std::memory_order_release);
    }
#endif

private:
    // The footprintLimit_ of the resource.
    uint64_t footprintLimit_ = UINT64_MAX;

    // The usage of the resource.
    std::atomic<uint64_t> usage_{ 0 };

    // The real usage of the resource.
    std::atomic<uint64_t> realUsage_{ 0 };
};

}  // namespace memory
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_RESOURCE_POOL_H
