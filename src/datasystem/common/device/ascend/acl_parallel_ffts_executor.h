/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Object-level parallel executor for Ascend FFTS copies.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_FFTS_EXECUTOR_H
#define DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_FFTS_EXECUTOR_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/device/device_batch_copy_helper.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/device_resource_manager.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/status.h"

namespace datasystem {

// Optional object-level parallelism for the FFTS H2D pipeline. One preserves the serial path.
struct ParallelFftsH2DConfig {
    ParallelFftsH2DConfig();
    ~ParallelFftsH2DConfig() = default;

    Status LoadFromEnv();
    Status Validate() const;
    std::string ToString() const;

    size_t workerNum;
    uint64_t minBytes;
};

// Optional object-level parallelism for the FFTS D2H pipeline. One preserves the serial path.
struct ParallelFftsD2HConfig {
    ParallelFftsD2HConfig();
    ~ParallelFftsD2HConfig() = default;

    Status LoadFromEnv();
    Status Validate() const;
    std::string ToString() const;

    size_t workerNum;
    uint64_t minBytes;
};

/**
 * @brief Partition complete FFTS objects into byte-balanced shards.
 * @note Returned helpers own metadata and buffer views only. Underlying buffers remain caller-owned.
 */
Status BuildParallelFftsShards(const DeviceBatchCopyHelper &helper, size_t workerNum, MemcpyKind kind,
                               std::vector<DeviceBatchCopyHelper> &shards, uint64_t &deviceStagingBytes);

/**
 * @brief Direction-specific executor that runs complete FFTS objects in parallel.
 * @note Memcpy drains every submitted shard before returning, so caller-owned buffers remain valid.
 * @note Resource and device managers plus any owner captured by copyFunction must outlive the executor.
 */
class AclParallelFftsExecutor {
public:
    using FftsCopyFunction =
        std::function<Status(uint32_t, DeviceBatchCopyHelper &, bool, ThreadPool *deviceSubmitPool)>;

    AclParallelFftsExecutor(DeviceResourceManager *resourceManager, DeviceManagerBase *deviceManager,
                            FftsCopyFunction copyFunction, ParallelFftsH2DConfig config);
    AclParallelFftsExecutor(DeviceResourceManager *resourceManager, DeviceManagerBase *deviceManager,
                            FftsCopyFunction copyFunction, ParallelFftsD2HConfig config);
    ~AclParallelFftsExecutor();

    AclParallelFftsExecutor(const AclParallelFftsExecutor &) = delete;
    AclParallelFftsExecutor &operator=(const AclParallelFftsExecutor &) = delete;

    Status Memcpy(uint32_t deviceId, DeviceBatchCopyHelper &helper);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_FFTS_EXECUTOR_H
