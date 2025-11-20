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
 * Description: Defines object reference info class ObjectMemoryRefTable class and ObjectGlobalRefTable.
 */
#include "datasystem/common/object_cache/object_ref_info.h"

#include <algorithm>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
Status SharedMemoryRefTable::GetShmUnit(const ShmKey &shmId, std::shared_ptr<ShmUnit> &shmUnit)
{
    TbbMemoryObjectRefTable::const_accessor shmAccessor;
    auto found = shmRefTable_.find(shmAccessor, shmId);
    if (!found) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("Get a not found shm: %s ", shmId));
    }
    shmUnit = shmAccessor->second.first;
    return Status::OK();
}

void SharedMemoryRefTable::AddShmUnit(const std::string &clientId, std::shared_ptr<ShmUnit> &shmUnit)
{
    const auto &shmId = shmUnit->GetId();
    TbbMemoryClientRefTable::accessor clientAccessor;
    TbbMemoryObjectRefTable::accessor objectAccessor;

    if (!clientRefTable_.find(clientAccessor, clientId)) {
        auto clientInfo = std::make_shared<ObjectRefInfo<ShmKey>>();
        clientRefTable_.emplace(clientAccessor, clientId, std::move(clientInfo));
    }
    if (clientAccessor->second->AddRef(shmId)) {
        shmUnit->IncrementRefCount();
    }
    if (!shmRefTable_.find(objectAccessor, shmId)) {
        shmRefTable_.emplace(objectAccessor, shmId, std::make_pair(shmUnit, std::unordered_set<ImmutableString>()));
    }

    objectAccessor->second.second.emplace(clientId);

    if (shmUnit->GetRefCount() == 1) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
    }

    VLOG(1) << "AddShmUnit for shmid: " << shmUnit->id << " client id: " << clientId;
}

void SharedMemoryRefTable::AddShmUnits(const std::string &clientId, std::vector<std::shared_ptr<ShmUnit>> &shmUnits)
{
    TbbMemoryClientRefTable::accessor clientAccessor;
    if (!clientRefTable_.find(clientAccessor, clientId)) {
        auto clientInfo = std::make_shared<ObjectRefInfo<ShmKey>>();
        clientRefTable_.emplace(clientAccessor, clientId, std::move(clientInfo));
    }
    TbbMemoryObjectRefTable::accessor objectAccessor;
    for (auto &shmUnit : shmUnits) {
        if (shmUnit == nullptr) {
            continue;
        }
        const auto &shmId = shmUnit->GetId();
        if (clientAccessor->second->AddRef(shmId)) {
            shmUnit->IncrementRefCount();
        }
        if (!shmRefTable_.find(objectAccessor, shmId)) {
            shmRefTable_.emplace(objectAccessor, shmId,
                                 std::make_pair(shmUnit, std::unordered_set<ImmutableString>{ clientId }));
        } else {
            objectAccessor->second.second.emplace(clientId);
        }
        objectAccessor.release();

        if (shmUnit->GetRefCount() == 1) {
            datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
            datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
        }
        VLOG(1) << "AddShmUnit for shmid: " << shmUnit->id << " client id: " << clientId;
    }
}

Status SharedMemoryRefTable::RemoveShmUnit(const std::string &clientId, const ShmKey &shmId)
{
    TbbMemoryClientRefTable::accessor clientAccessor;
    TbbMemoryObjectRefTable::accessor shmAccessor;

    // first lock clientAccessor, then lock shmAccessor.
    auto found = clientRefTable_.find(clientAccessor, clientId);
    if (!found) {
        LOG(WARNING) << FormatString("RemoveShmUnit: The key shmId : %s not exit in shmRefTable_.", shmId);
        return Status::OK();
    }
    if (!shmRefTable_.find(shmAccessor, shmId)) {
        LOG(WARNING) << FormatString("RemoveShmUnit: The key objectKey : %s not exit in shmRefTable_.", shmId);
        return Status::OK();
    }
    auto shmUnit = shmAccessor->second.first;
    INJECT_POINT("RemoveShmUnit");
    RemoveShmUnitDetail(clientId, shmAccessor, clientAccessor);
    VLOG(1) << "RemoveShmUnit for shmid: " << shmId << " client id: " << clientId;
    return Status::OK();
}

void SharedMemoryRefTable::RemoveShmUnitDetail(const std::string &clientId,
                                               TbbMemoryObjectRefTable::accessor &shmAccessor,
                                               TbbMemoryClientRefTable::accessor &clientAccessor)
{
    const auto &shmUnit = shmAccessor->second.first;
    const auto shmId = shmUnit->GetId();
    // Step 1: Update Client Table.
    if (clientAccessor->second->RemoveRef(shmId)) {
        if (shmUnit->GetRefCount() > 0) {
            shmUnit->DecrementRefCount();
        } else {
            LOG(WARNING) << "RemoveShmUnit: The value of refCount is 0 and cannot be decreased. id:"
                         << BytesUuidToString(shmId.ToString());
        }
    }
    if (clientAccessor->second->CheckIsRefIdsEmpty()) {
        if (!clientRefTable_.erase(clientAccessor)) {
            LOG(ERROR) << FormatString("Fail to erase clientId: %s", clientAccessor->first);
        }
    }
    // Step 2: Update Object Table.
    if (shmUnit->GetRefCount() == 0) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(-1);
    }
    auto &clientIds = shmAccessor->second.second;
    (void)clientIds.erase(clientId);
    if (clientIds.empty()) {
        VLOG(1) << FormatString("Erase shmId: %s", shmAccessor->first);
        if (!shmRefTable_.erase(shmAccessor)) {
            LOG(ERROR) << FormatString("Fail to erase shmId: %s", shmAccessor->first);
        }
    }
}

#ifdef WITH_TESTS
bool SharedMemoryRefTable::Contains(const std::string &clientId, const ShmKey &shmId) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        return accessor->second->Contains(shmId);
    }
    return false;
}
#endif

void SharedMemoryRefTable::GetClientRefIds(const std::string &clientId, std::vector<ShmKey> &shmIds) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        accessor->second->GetRefIds(shmIds);
    }
}

Status SharedMemoryRefTable::RemoveClient(const std::string &clientId)
{
    TbbMemoryClientRefTable::accessor clientAccessor;
    if (!clientRefTable_.find(clientAccessor, clientId)) {
        return Status::OK();
    }
    std::vector<ShmKey> shmIds;
    clientAccessor->second->GetRefIds(shmIds);
    for (const auto &shmId : shmIds) {
        TbbMemoryObjectRefTable::accessor shmAccessor;
        if (!shmRefTable_.find(shmAccessor, shmId)) {
            continue;
        }
        RemoveShmUnitDetail(clientId, shmAccessor, clientAccessor);
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
