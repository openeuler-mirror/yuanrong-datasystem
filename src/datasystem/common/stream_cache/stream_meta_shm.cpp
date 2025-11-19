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
 * Description: Record meta for a stream.
 */

#include "datasystem/common/stream_cache/stream_meta_shm.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/stream/stream_config.h"

namespace datasystem {
Status StreamMetaShm::Init(std::shared_ptr<client::MmapTableEntry> mmapTableEntry)
{
    RETURN_RUNTIME_ERROR_IF_NULL(shmPtr_);
    auto *data = shmPtr_;
    usage_ = reinterpret_cast<decltype(usage_)>(shmPtr_);
    data += sizeof(*(usage_));
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>((data) - (shmPtr_)) <= shmSz_, K_RUNTIME_ERROR,
                             "Work area size too small");
    if (mmapTableEntry != nullptr) {
        mmapTableEntry_ = std::move(mmapTableEntry);
    }
    return Status::OK();
}

Status StreamMetaShm::TryIncUsage(uint64_t size)
{
    INJECT_POINT("StreamMetaShm.TryIncUsage");
    bool success = false;
    uint64_t currUsage = 0;
    do {
        currUsage = __atomic_load_n(usage_, __ATOMIC_RELAXED);
        CHECK_FAIL_RETURN_STATUS(UINT64_MAX - currUsage >= size, K_OUT_OF_RANGE,
                                 "The usage of shared memory reached UINT64_MAX");
        uint64_t desiredVal = currUsage + size;
        CHECK_FAIL_RETURN_STATUS(desiredVal <= maxStreamSize_, K_OUT_OF_MEMORY,
                                 FormatString("stream: %s, currUsage: %llu, tryIncUsage: %llu, maxStreamSize_: %llu",
                                              streamName_, currUsage, size, maxStreamSize_));
        success =
            __atomic_compare_exchange_n(usage_, &currUsage, desiredVal, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    } while (!success);

    VLOG(SC_NORMAL_LOG_LEVEL) << "TryIncUsage for streamName:" << streamName_ << ", size: " << size
                              << ", before: " << currUsage << ", after: " << (currUsage + size);
    return Status::OK();
}

Status StreamMetaShm::TryDecUsage(uint64_t size)
{
    bool success = false;
    uint64_t currUsage = 0;
    do {
        currUsage = __atomic_load_n(usage_, __ATOMIC_RELAXED);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            currUsage >= size, K_RUNTIME_ERROR,
            FormatString("[TryDecUsage error] stream: %s, currUsage: %llu, tryDecUsage: %llu", streamName_, currUsage,
                         size));
        uint64_t desiredVal = currUsage - size;
        success =
            __atomic_compare_exchange_n(usage_, &currUsage, desiredVal, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
    } while (!success);
    VLOG(SC_NORMAL_LOG_LEVEL) << "TryDecUsage for streamName:" << streamName_ << ", size: " << size
                              << ", before: " << currUsage << ", after: " << (currUsage - size);
    return Status::OK();
}
}  // namespace datasystem
