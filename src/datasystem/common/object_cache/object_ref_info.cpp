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
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
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

void SharedMemoryRefTable::ClientTableGetOrInsert(const ClientKey &clientId,
                                                  TbbMemoryClientRefTable::const_accessor &accessor)
{
    // return read lock until the end of scope.
    while (!clientRefTable_.find(accessor, clientId)) {
        TbbMemoryClientRefTable::accessor writeAccessor;
        // Add write lock to avoid multiple insertions.
        if (clientRefTable_.insert(writeAccessor, clientId)) {
            auto clientInfo = std::make_shared<ObjectRefInfo<ShmKey>>();
            writeAccessor->second = std::move(clientInfo);
        }
    }
}

void SharedMemoryRefTable::InitShmRefForClient(const ClientKey &clientId, bool supportMultiShmRefCount)
{
    LOG(INFO) << "Initializing shared memory reference for client " << clientId
              << ", supportMultiShmRefCount: " << supportMultiShmRefCount;
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.insert(accessor, clientId)) {
        // Set isUniqueCnt to false if the client supports multiple shared memory reference counts,
        auto clientInfo = std::make_shared<ObjectRefInfo<ShmKey>>(!supportMultiShmRefCount);
        accessor->second = std::move(clientInfo);
    } else {
        LOG(WARNING) << "Client " << clientId << " already exists";
    }
}

void SharedMemoryRefTable::AddShmUnit(const ClientKey &clientId, std::shared_ptr<ShmUnit> &shmUnit)
{
    const auto &shmId = shmUnit->GetId();
    TbbMemoryClientRefTable::const_accessor clientAccessor;
    TbbMemoryObjectRefTable::accessor objectAccessor;

    ClientTableGetOrInsert(clientId, clientAccessor);
    if (clientAccessor->second->AddRef(shmId)) {
        shmUnit->IncrementRefCount();
    }
    if (!shmRefTable_.find(objectAccessor, shmId)) {
        shmRefTable_.emplace(objectAccessor, shmId, std::make_pair(shmUnit, std::unordered_set<ClientKey>()));
    }

    objectAccessor->second.second.emplace(clientId);

    if (shmUnit->GetRefCount() == 1) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(-1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(1);
    }

    VLOG(1) << "AddShmUnit for shmid: " << shmUnit->id << " client id: " << clientId;
}

void SharedMemoryRefTable::AddShmUnits(TbbMemoryClientRefTable::const_accessor &clientAccessor,
                                       std::vector<std::shared_ptr<ShmUnit>> &shmUnits)
{
    TbbMemoryObjectRefTable::accessor objectAccessor;
    const ClientKey &clientId = clientAccessor->first;
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
                                 std::make_pair(shmUnit, std::unordered_set<ClientKey>{ clientId }));
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

Status SharedMemoryRefTable::RemoveShmUnit(const ClientKey &clientId, const ShmKey &shmId)
{
    TbbMemoryClientRefTable::accessor clientAccessor;
    TbbMemoryObjectRefTable::accessor shmAccessor;

    // first lock clientAccessor, then lock shmAccessor.
    auto found = clientRefTable_.find(clientAccessor, clientId);
    if (!found) {
        LOG(WARNING) << FormatString("The clientId not exists in clientRefTable_, clientId: %s, shmId: %s", clientId,
                                     shmId);
        return Status::OK();
    }
    if (!shmRefTable_.find(shmAccessor, shmId)) {
        LOG(WARNING) << FormatString("The shmId not exists in shmRefTable_, clientId: %s, shmId: %s", clientId, shmId);
        return Status::OK();
    }
    auto shmUnit = shmAccessor->second.first;
    INJECT_POINT("RemoveShmUnit");
    RemoveShmUnitDetail(clientId, false, shmAccessor, clientAccessor);
    VLOG(1) << "RemoveShmUnit for shmid: " << shmId << " client id: " << clientId;
    return Status::OK();
}

void SharedMemoryRefTable::RemoveShmUnitDetail(const ClientKey &clientId, bool forceRemoveClient,
                                               TbbMemoryObjectRefTable::accessor &shmAccessor,
                                               TbbMemoryClientRefTable::accessor &clientAccessor)
{
    const auto &shmUnit = shmAccessor->second.first;
    const auto shmId = shmUnit->GetId();
    // Step 1: Update Client Table.
    uint32_t refCount = 0;
    if (clientAccessor->second->RemoveAndGetRefCnt(shmId, refCount)) {
        if (shmUnit->GetRefCount() > 0) {
            shmUnit->DecrementRefCount();
        } else {
            LOG(WARNING) << "RemoveShmUnit: The value of refCount is 0 and cannot be decreased. id:"
                         << BytesUuidToString(shmId.ToString());
        }
    }

    // Step 2: Update Object Table.
    if (shmUnit->GetRefCount() == 0) {
        datasystem::memory::Allocator::Instance()->ChangeNoRefPageCount(1);
        datasystem::memory::Allocator::Instance()->ChangeRefPageCount(-1);
    }

    // skip if the ref count for shmId is not zero.
    if (!forceRemoveClient && refCount > 0) {
        return;
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
bool SharedMemoryRefTable::Contains(const ClientKey &clientId, const ShmKey &shmId) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        return accessor->second->Contains(shmId);
    }
    return false;
}
#endif

void SharedMemoryRefTable::GetClientRefIds(const ClientKey &clientId, std::vector<ShmKey> &shmIds) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        accessor->second->GetRefIds(shmIds);
    }
}

Status SharedMemoryRefTable::RemoveClient(const ClientKey &clientId)
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
        RemoveShmUnitDetail(clientId, true, shmAccessor, clientAccessor);
    }
    clientRefTable_.erase(clientAccessor);
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
