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

Status ClientMemoryRefTable::CreateRef(const ShmKey &shmId)
{
    RETURN_OK_IF_TRUE(workerSupportMultiShmRefCount_);
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    CHECK_FAIL_RETURN_STATUS(table_.emplace(shmId, 1), StatusCode::K_RUNTIME_ERROR,
                             FormatString("shmId %s already exists", shmId));
    return Status::OK();
}

Status ClientMemoryRefTable::CreateRef(const ShmKey &shmId, TbbMemoryRefTable::accessor &accessor)
{
    RETURN_OK_IF_TRUE(workerSupportMultiShmRefCount_);
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    CHECK_FAIL_RETURN_STATUS(table_.emplace(accessor, shmId, 1), StatusCode::K_RUNTIME_ERROR,
                             FormatString("shmId %s already exists", shmId));
    return Status::OK();
}

void ClientMemoryRefTable::DeleteRef(const ShmKey &shmId)
{
    if (workerSupportMultiShmRefCount_) {
        return;
    }
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    table_.erase(shmId);
}

void ClientMemoryRefTable::DeleteRef(TbbMemoryRefTable::accessor &accessor)
{
    if (workerSupportMultiShmRefCount_) {
        return;
    }
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    table_.erase(accessor);
}

Status ClientMemoryRefTable::Find(const ShmKey &shmId, TbbMemoryRefTable::accessor &accessor)
{
    RETURN_OK_IF_TRUE(workerSupportMultiShmRefCount_);
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    auto found = table_.find(accessor, shmId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(found, K_NOT_FOUND,
                                         FormatString("[shmId %s] Cannot find shm in memoryRef table.", shmId));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        accessor->second > 0, K_UNKNOWN_ERROR,
        FormatString("[shmId %s] Ref count must be positive integer, cur is : %d", shmId, accessor->second));
    return Status::OK();
}

void ClientMemoryRefTable::IncreaseRef(const ShmKey &shmId)
{
    if (workerSupportMultiShmRefCount_) {
        return;
    }
    std::shared_lock<std::shared_mutex> rlocker(mutex_);
    TbbMemoryRefTable::accessor accessor;
    auto found = table_.insert(accessor, shmId);
    accessor->second = (found ? 1 : accessor->second + 1);
}

Status ClientMemoryRefTable::DecreaseRef(TbbMemoryRefTable::accessor &accessor, bool &needDecreaseWorkerRef)
{
    if (workerSupportMultiShmRefCount_) {
        needDecreaseWorkerRef = true;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!accessor.empty(), K_UNKNOWN_ERROR,
                                         FormatString("ShmId %s not exists in memoryRef table.", accessor->first));
    accessor->second--;
    needDecreaseWorkerRef = accessor->second == 0;
    return Status::OK();
}

void ClientMemoryRefTable::SetSupportMultiShmRefCount(bool value)
{
    workerSupportMultiShmRefCount_ = value;
}

void ClientMemoryRefTable::Clear()
{
    if (workerSupportMultiShmRefCount_) {
        return;
    }
    std::lock_guard<std::shared_mutex> wlocker(mutex_);
    table_.clear();
}

}  // namespace object_cache
}  // namespace datasystem
