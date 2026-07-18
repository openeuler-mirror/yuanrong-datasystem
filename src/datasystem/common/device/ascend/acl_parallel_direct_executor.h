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
 * Description: Bounded parallel executor for Ascend direct batch copies.
 */
#ifndef DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_DIRECT_EXECUTOR_H
#define DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_DIRECT_EXECUTOR_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "datasystem/common/device/device_batch_copy_helper.h"
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/utils/status.h"

namespace datasystem {

constexpr size_t ACL_MEMCPY_BATCH_LIMIT = 4096;

// Immutable after an executor is initialized. Values are loaded once when AclResourceManager is constructed.
struct ParallelH2DConfig {
    ParallelH2DConfig();
    ~ParallelH2DConfig() = default;

    Status LoadFromEnv();
    Status Validate() const;
    std::string ToString() const;

    size_t workerNum;
    size_t aggregateNum;
    size_t maxPendingTaskNum;
    uint64_t minBytes;
    size_t maxInflightBatchNum;
};

// Direct D2H uses a separate environment namespace so Set and Get can be tuned independently.
struct ParallelD2HConfig {
    ParallelD2HConfig();
    ~ParallelD2HConfig() = default;

    Status LoadFromEnv();
    Status Validate() const;
    std::string ToString() const;

    size_t workerNum;
    size_t aggregateNum;
    size_t maxPendingTaskNum;
    uint64_t minBytes;
    size_t maxInflightBatchNum;
};

/**
 * @brief Per-device bounded executor for synchronous ACL direct H2D or D2H batches.
 * @note The caller owns descriptor arrays and source/destination buffers. MemcpyBatch drains every accepted task
 * before returning, so those views remain valid for the complete asynchronous worker interval.
 */
class AclParallelDirectExecutor {
public:
    AclParallelDirectExecutor(uint32_t deviceId, DeviceManagerBase *deviceManager, ParallelH2DConfig config);
    AclParallelDirectExecutor(uint32_t deviceId, DeviceManagerBase *deviceManager, ParallelD2HConfig config);
    ~AclParallelDirectExecutor();

    AclParallelDirectExecutor(const AclParallelDirectExecutor &) = delete;
    AclParallelDirectExecutor &operator=(const AclParallelDirectExecutor &) = delete;

    Status Init();
    Status MemcpyBatch(DeviceBatchCopyHelper &helper);
    void Shutdown();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_DEVICE_ASCEND_ACL_PARALLEL_DIRECT_EXECUTOR_H
