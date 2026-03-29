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
 * Description: The client memory ref table implementation.
 */
#include "datasystem/client/object_cache/client_memory_ref_table.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {

void ClientMemoryRefTable::IncreaseRef(const ShmKey &shmId)
{
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    TbbMemoryRefTable::accessor accessor;
    auto isNew = table_.insert(accessor, shmId);
    accessor->second = (isNew ? 1 : accessor->second + 1);
    VLOG(1) << "IncreaseRef: " << shmId << " after increase ref count=" << accessor->second;
}

bool ClientMemoryRefTable::DecreaseRef(const ShmKey &shmId)
{
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    TbbMemoryRefTable::accessor accessor;
    auto found = table_.find(accessor, shmId);
    VLOG(1) << "DecreaseRef: " << shmId << " before decrease ref count=" << (found ? accessor->second : 0);
    if (found) {
        accessor->second--;
        if (accessor->second <= 0) {
            table_.erase(accessor);
            return true;
        }
    }
    // Worker supports multiple shared memory references.
    // The client must call DecreaseShmRef even if the reference count is greater than 0.
    return workerSupportMultiShmRefCount_;
}

int ClientMemoryRefTable::RefCount(const ShmKey &shmId)
{
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    TbbMemoryRefTable::accessor accessor;
    auto found = table_.find(accessor, shmId);
    return found ? accessor->second : 0;
}

size_t ClientMemoryRefTable::Size() const
{
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    return table_.size();
}

void ClientMemoryRefTable::SetSupportMultiShmRefCount(bool value)
{
    workerSupportMultiShmRefCount_ = value;
}

void ClientMemoryRefTable::Clear()
{
    std::lock_guard<std::shared_mutex> wlocker(mutex_);
    table_.clear();
}
}  // namespace object_cache
}  // namespace datasystem
