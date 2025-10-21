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
 * Description: Implement shared memory resource stats class.
 */

#include "datasystem/common/shared_memory/resource_pool.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace memory {

Status ResourcePool::AddUsageCAS(uint64_t size, uint64_t dynamicLimit)
{
    auto threshold = std::min(footprintLimit_, dynamicLimit);
    uint64_t preSize = usage_.load(std::memory_order_acquire);
    while (true) {
        CHECK_FAIL_RETURN_STATUS(threshold > preSize && threshold - preSize >= size, StatusCode::K_OUT_OF_MEMORY,
                                 FormatString("Resource allocation has reached the threshold limit."
                                              "Total allocated %u, required size %u, threshold size: %u",
                                              preSize, size, threshold));
        if (usage_.compare_exchange_weak(preSize, preSize + size)) {
            break;
        }
    }
    return Status::OK();
}

bool ResourcePool::AddRealUsage(uint64_t size)
{
    auto prevSize = realUsage_.fetch_add(size, std::memory_order_relaxed);
    if (prevSize > UINT64_MAX - size || prevSize + size > footprintLimit_) {
        (void)realUsage_.fetch_sub(size, std::memory_order_relaxed);
        return false;
    }
    return true;
}

void ResourcePool::AddRealUsageNoCheck(uint64_t size)
{
    (void)realUsage_.fetch_add(size, std::memory_order_relaxed);
    return;
}

uint64_t ResourcePool::GetOrUpdateRealUsage(uint64_t newUsage)
{
    // The amd64/arm64 architecture implemented 48-bit physical addresses space.
    const uint64_t maxUsageLimit = 0xFFFFFFFFFFFF;  // 2^48 - 1
    uint64_t usage = realUsage_.load(std::memory_order_acquire);
    while (usage > maxUsageLimit) {
        if (realUsage_.compare_exchange_weak(usage, newUsage, std::memory_order_release, std::memory_order_acquire)) {
            LOG(WARNING) << "Invalid realUsage_:" << usage << ", update to " << newUsage;
            return newUsage;
        }
    }
    return usage;
}

void ResourcePool::SubRealUsage(uint64_t size)
{
    auto prevSize = realUsage_.fetch_sub(size, std::memory_order_relaxed);
    LOG_IF(WARNING, prevSize < size) << FormatString(
        "The released resources(%uB) are more than the total resources(%uB).", size, prevSize);
}

bool ResourcePool::SubRealUsageCAS(uint64_t size)
{
    uint64_t prevSize = realUsage_.load(std::memory_order_acquire);
    while (true) {
        uint64_t newSize = prevSize >= size ? prevSize - size : 0;
        if (realUsage_.compare_exchange_weak(prevSize, newSize, std::memory_order_release, std::memory_order_acquire)) {
            break;
        }
    }
    if (prevSize < size) {
        LOG(WARNING) << FormatString("The released resources(%uB) are more than the total resources(%uB), update to 0.",
                                     size, prevSize);
        return false;
    }
    return true;
}

}  // namespace memory
}  // namespace datasystem
