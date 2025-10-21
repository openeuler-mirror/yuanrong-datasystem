/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Define the basic unit of the shared memory in the server side.
 * Support allocate and free shared memory.
 */

#include "datasystem/common/shared_memory/shm_unit.h"

#include <utility>

#include <securec.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {
ShmUnit::ShmUnit(int fd, uint64_t mmapSz) : ShmUnitInfo(fd, mmapSz)
{
}

ShmUnit::ShmUnit(std::string id, ShmView shmView, void *pointer) : ShmUnitInfo(std::move(id), shmView, pointer)
{
}

ShmUnit::~ShmUnit()
{
    VLOG(1) << "Release memory of " << id << " Size: " << size << " Off: " << offset;
    Status rc = this->FreeMemory();
    if (rc.IsError()) {
        LOG(WARNING) << "Destructor for a ShmUnit failed to free memory.";
    }
}

std::string ShmUnit::GetTenantId()
{
    return tenantId_;
}

ShmView ShmUnit::GetShmView()
{
    return { fd, mmapSize, offset, size };
}

void ShmUnit::SetHardFreeMemory()
{
    needHardFree_ = true;
}

Status ShmUnit::FreeMemory()
{
    RETURN_OK_IF_TRUE(pointer == nullptr);
    // This call will set pointer to nullptr on success.
    VLOG(1) << "[ShmUnit] Arena FreeMemory, Tenant:" << (tenantId_.empty() ? "Default" : tenantId_)
            << ", needHardFree: " << needHardFree_;
    INJECT_POINT("ShmUnit.FreeMemory", [this]() {
        needHardFree_ = true;
        return Status::OK();
    });
    if (needHardFree_) {
        int ret = memset_s(pointer, size, 0, size);
        if (ret != EOK) {
            LOG(WARNING) << FormatString("[ShmId %s] memset failed, error code: %d.", id, ret);
        }
    }
    return datasystem::memory::Allocator::Instance()->FreeMemory(tenantId_, pointer, cacheType_);
}

Status ShmUnit::AllocateMemory(const std::string &tenantId, uint64_t needSize, bool populate,
                               memory::CacheType cacheType)
{
    VLOG(1) << "[ShmUnit] AllocateMemory, Tenant: " << (tenantId.empty() ? "Default" : tenantId)
            << ", size: " << needSize << ", cachetype: " << static_cast<int>(cacheType);
    cacheType_ = cacheType;
    RETURN_IF_NOT_OK(datasystem::memory::Allocator::Instance()->AllocateMemory(tenantId, needSize, populate, pointer,
                                                                               fd, offset, mmapSize, cacheType_));
    size = needSize;
    tenantId_ = tenantId;
    return Status::OK();
}
}  // namespace datasystem
