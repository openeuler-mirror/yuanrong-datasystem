/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"

#include <memory>
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/shared_memory/arena.h"
#include "datasystem/common/util/format.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
WorkerSCAllocateMemory::WorkerSCAllocateMemory(std::shared_ptr<object_cache::WorkerOcEvictionManager> manager)
    : ocEvictManager_(std::move(manager))
{
    streamMaxSize_ = datasystem::memory::Allocator::Instance()->GetMaxMemorySize(ServiceType::STREAM);
}

Status WorkerSCAllocateMemory::AllocateMemoryForStream(const std::string &tenantId, const std::string &streamId,
                                                       const uint64_t needSize, bool populate, ShmUnit &shmUnit,
                                                       bool retryOnOOM)
{
    Timer timer;
    PerfPoint point(PerfKey::WORKER_MEMORY_ALLOCATE);
    auto streamMemoryUsageSize =
        datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::STREAM);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        UINT64_MAX - needSize >= streamMemoryUsageSize, K_OUT_OF_RANGE,
        FormatString("The size is overflow, stream cache memory use size:%d + add:%d > UINT64_MAX:%d",
                     streamMemoryUsageSize, needSize, UINT64_MAX));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        streamMemoryUsageSize + needSize <= streamMaxSize_, K_OUT_OF_MEMORY,
        FormatString(
            "Stream cache memory size overflow, maxStreamSize is: %d, need size is: %d, stream cache use size: %d",
            streamMaxSize_, needSize, streamMemoryUsageSize));
    bool evict = false;
    if (datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::OBJECT) > 0) {
        evict = EvictWhenMemoryExceedThrehold(streamId, needSize, ocEvictManager_, ServiceType::STREAM);
    }
    // Allocate some memory into this shmUnit
    // if object used size = 0, no need to evict object, return OOM, if stream size is max return OOM
    Status rc = shmUnit.AllocateMemory(tenantId, needSize, populate, ServiceType::STREAM);
    static const std::vector<int> WAIT_MSECOND = { 1, 10, 50, 100, 200, 400, 800, 1600, 3200 };
    if (rc.GetCode() == K_OUT_OF_MEMORY && evict && retryOnOOM) {
        for (int t : WAIT_MSECOND) {
            auto remainingTime = reqTimeoutDuration.CalcRealRemainingTime();
            auto sleepTime = std::min<int64_t>(remainingTime, t);
            streamMemoryUsageSize =
                datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(ServiceType::STREAM);
            if (streamMemoryUsageSize + needSize > streamMaxSize_) {
                return Status(K_OUT_OF_MEMORY, FormatString("Stream cache memory size overflow, maxStreamSize is: %d, "
                                                            "need size is: %d, stream cache use size: %d",
                                                            streamMaxSize_, needSize, streamMemoryUsageSize));
            }
            if (remainingTime <= 0) {
                break;
            }
            VLOG(1) << FormatString("OOM, sleep time: %ld, streamId: %s, needSize %ld", sleepTime, streamId,
                                    needSize);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            rc = shmUnit.AllocateMemory(tenantId, needSize, populate, ServiceType::STREAM);
            if (rc.GetCode() != K_OUT_OF_MEMORY) {
                break;
            }

            (void)EvictWhenMemoryExceedThrehold(streamId, needSize, ocEvictManager_, ServiceType::STREAM);
        }
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rc, FormatString("[stream %s] Error while allocating memory size %ld", streamId, needSize));
    VLOG(DEBUG_LOG_LEVEL) << "allocate for stream success, allocate size: " << needSize << " stream cache use size: "
                          << datasystem::memory::Allocator::Instance()->GetTotalRealMemoryUsage(
                                 ServiceType::STREAM);
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem