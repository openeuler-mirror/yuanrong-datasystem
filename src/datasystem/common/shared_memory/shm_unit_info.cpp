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
 * Description: Define the basic unit information of the shared memory in client side.
 * Don't support allocate or free shared memory.
 */

#include "datasystem/common/shared_memory/shm_unit_info.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/memory.h"

namespace datasystem {

ShmUnitInfo::ShmUnitInfo(int fd, uint64_t mmapSz) : fd(fd), mmapSize(mmapSz)
{
}

ShmUnitInfo::ShmUnitInfo(ShmKey id, ShmView shmView, void *pointer)
    : fd(shmView.fd),
      mmapSize(shmView.mmapSz),
      pointer(pointer),
      offset(shmView.off),
      size(shmView.sz),
      refCount(0),
      id(std::move(id)),
      numaId(INVALID_NUMA_ID)
{
}

Status ShmUnitInfo::MemoryCopy(const uint8_t *src, uint64_t srcSize,
                               const std::shared_ptr<ThreadPool> &threadPool) const
{
    Status rc = datasystem::MemoryCopy(static_cast<uint8_t *>(pointer), size, src, srcSize, threadPool);
    if (rc.IsError()) {
        LOG(ERROR) << "MemoryCopy Failed."
                   << "\n  target size: " << size << "\n  source size: " << srcSize;
    }
    return rc;
}

Status ShmUnitInfo::MemoryCopy(const std::vector<std::pair<const uint8_t *, uint64_t>> &payloads,
                               const std::shared_ptr<ThreadPool> &threadPool, uint64_t off)
{
    // The starting point for the copy is the beginning of the ShmUnitInfo's memory at "pointer", and it has "size" for
    // the amount of bytes that we are allowed to copy in to.
    uint8_t *targetPtr = reinterpret_cast<uint8_t *>(pointer) + off;
    CHECK_FAIL_RETURN_STATUS(size > off, StatusCode::K_RUNTIME_ERROR,
                             FormatString("size %zu is less than off %zu", size, off));
    uint64_t remainingSpace = size - off;
    Status rc;
    for (auto &msg : payloads) {
        rc = datasystem::MemoryCopy(targetPtr, remainingSpace, msg.first, msg.second, threadPool);
        if (rc.IsError()) {
            LOG(ERROR) << "MemoryCopy Failed:"
                       << "\n  ShmUnitInfo size: " << size
                       << "\n  target size: " << remainingSpace << "\n  source size: " << msg.second;
            return rc;
        }
        targetPtr += msg.second;
        remainingSpace = remainingSpace > msg.second ? remainingSpace - msg.second : 0;
    }
    return Status::OK();
}

}  // namespace datasystem
