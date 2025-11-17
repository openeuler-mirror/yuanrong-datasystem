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

#include "datasystem/common/log/log.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
bool ObjectRefInfo::AddRef(const std::string &objectKey, uint32_t ref)
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    TbbObjKeyTable::accessor objAccessor;
    VLOG(1) << "add object key " << objectKey << " ref:" << ref;
    bool res = objectKeys_.emplace(objAccessor, objectKey, ref);
    if (res) {
        return true;
    }
    if (isUniqueCnt_) {
        return false;
    }
    objAccessor->second += ref;
    return true;
}

uint32_t ObjectRefInfo::GetRefCount(const std::string &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    TbbObjKeyTable::const_accessor objAccessor;
    if (objectKeys_.find(objAccessor, objectKey)) {
        return objAccessor->second;
    }
    return 0;
}

Status ObjectRefInfo::UpdateRefCount(const std::string &objectKey, int count)
{
    if (count < 0) {
        RETURN_STATUS(StatusCode::K_INVALID, FormatString("[ObjectId %s] Invalid count: %d", objectKey, count));
    }
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    TbbObjKeyTable::accessor objAccessor;
    if (objectKeys_.find(objAccessor, objectKey)) {
        if (isUniqueCnt_ && count > 1) {
            RETURN_STATUS(StatusCode::K_DUPLICATED, "object key is marked to be unique");
        }
        objAccessor->second = static_cast<uint32_t>(count);
        return Status::OK();
    }
    auto result = objectKeys_.emplace(objAccessor, objectKey, count);
    if (!result) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "emplace on objectKeys_ failed.");
    }
    return Status::OK();
}

bool ObjectRefInfo::RemoveRef(const std::string &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    TbbObjKeyTable::accessor objAccessor;
    if (!objectKeys_.find(objAccessor, objectKey)) {
        return false;
    }
    if (isUniqueCnt_) {
        auto result = objectKeys_.erase(objAccessor);
        return result > 0;
    }
    objAccessor->second -= 1;
    if (objAccessor->second == 0) {
        (void)objectKeys_.erase(objAccessor);
    }
    return true;
}

bool ObjectRefInfo::Contains(const std::string &objectKey) const
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    return objectKeys_.count(objectKey) == 1;
}

void ObjectRefInfo::GetRefIds(std::vector<std::string> &objectKeys) const
{
    std::lock_guard<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    std::transform(objectKeys_.begin(), objectKeys_.end(), std::back_inserter(objectKeys),
                   [](auto &kv) { return kv.first; });
}

bool ObjectRefInfo::CheckIsNoneRef(const std::string &objectKey) const
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    TbbObjKeyTable::const_accessor objAccessor;
    if (!objectKeys_.find(objAccessor, objectKey)) {
        return true;
    } else if (objAccessor->second == 0) {
        return true;
    }
    return false;
}

bool ObjectRefInfo::CheckIsRefIdsEmpty() const
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    return objectKeys_.empty();
}

Status ObjectGlobalRefTable::GIncreaseRef(const std::string &clientId, const std::vector<std::string> &objectKeys,
                                          std::vector<std::string> &failedIncIds, std::vector<std::string> &firstIncIds,
                                          bool isRemoteClient)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientRefTable::const_accessor clientAccessor;
    while (!clientRefTable_.find(clientAccessor, clientId)) {
        TbbClientRefTable::accessor accessor;
        auto clientInfo = std::make_shared<ObjectRefInfo>();
        clientRefTable_.emplace(accessor, clientId, std::move(clientInfo));
        // In the off-cloud reference counting scenario, if remoteClient appears for the first time, record it to
        // remoteClientIdTable_.
        if (isRemoteClient) {
            (void)remoteClientIdTable_.insert({ clientId, nullptr });
        }
    }
    std::vector<std::string> successVec;
    Status rc = Status::OK();
    for (const auto &objectKey : objectKeys) {
        if (!clientAccessor->second->AddRef(objectKey)) {
            LOG(WARNING) << FormatString("GIncreaseRef is being processed, but the object key(%s) duplicate.",
                                         objectKey);
            continue;
        }
        if (saveToKvStore_) {
            rc = saveToKvStore_(clientId, objectKey, isRemoteClient);
            if (rc.IsError()) {
                (void)clientAccessor->second->RemoveRef(objectKey);
                failedIncIds.emplace_back(objectKey);
                LOG(ERROR) << "Save global reference to kv store failed. status:" << rc.ToString();
            }
        }
        if (rc.IsOk()) {
            successVec.emplace_back(objectKey);
        }
    }
    std::sort(successVec.begin(), successVec.end());

    for (const auto &objectKey : successVec) {
        TbbObjRefTable::accessor objAccessor;
        if (!objectRefTable_.find(objAccessor, objectKey)) {
            (void)objectRefTable_.insert(objAccessor, objectKey);
        }
        if (objAccessor->second.empty()) {
            firstIncIds.emplace_back(objectKey);
        }
        objAccessor->second.emplace(clientId);
    }
    return rc;
}

Status ObjectGlobalRefTable::GDecreaseRef(const std::string &clientId, const std::vector<std::string> &objectKeys,
                                          std::vector<std::string> &failedDecIds,
                                          std::vector<std::string> &finishDecIds, bool isRemoteClient)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    std::vector<std::string> successVec;
    Status rc = Status::OK();
    {
        TbbClientRefTable::const_accessor clientAccessor;
        if (!clientRefTable_.find(clientAccessor, clientId)) {
            LOG(WARNING) << FormatString("GDecreaseRef is being processed, but the client id(%s) does not exist.",
                                         clientId);
            return Status::OK();
        }
        for (const auto &objectKey : objectKeys) {
            if (!clientAccessor->second->RemoveRef(objectKey)) {
                LOG(WARNING) << FormatString("GDecreaseRef is being processed, but the object key(%s) does not exist.",
                                             objectKey);
                continue;
            }
            successVec.emplace_back(objectKey);
            if (removeFromKvStore_) {
                rc = removeFromKvStore_(clientId, objectKey, isRemoteClient);
                if (rc.IsError()) {
                    (void)clientAccessor->second->AddRef(objectKey);
                    failedDecIds.emplace_back(objectKey);
                    LOG(ERROR) << "Remove global reference from kv store failed. status:" << rc.ToString();
                }
            }
        }
    }
    std::sort(successVec.begin(), successVec.end());

    RETURN_IF_NOT_OK(TryEraseClientId(clientId, isRemoteClient));
    for (const auto &objectKey : successVec) {
        TbbObjRefTable::accessor objAccessor;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectRefTable_.find(objAccessor, objectKey), StatusCode::K_RUNTIME_ERROR,
                                             FormatString("Fail to find objectKey: %s", objectKey));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objAccessor->second.erase(clientId), StatusCode::K_RUNTIME_ERROR,
                                             FormatString("Fail to erase clientId: %s", clientId));
        if (objAccessor->second.empty()) {
            finishDecIds.emplace_back(objectKey);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectRefTable_.erase(objAccessor), StatusCode::K_RUNTIME_ERROR,
                                                 FormatString("Fail to erase objectKey: %s", objAccessor->first));
        }
    }
    return rc;
}

Status ObjectGlobalRefTable::TryEraseClientId(const std::string &clientId, bool isRemoteClient)
{
    TbbClientRefTable::accessor clientAccessor;
    bool isRemoteClientIdRefEmpty = false;
    if (!clientRefTable_.find(clientAccessor, clientId)) {
        LOG(INFO) << FormatString("GDecreaseRef is being processed, but the client id(%s) does not exist.", clientId);
        isRemoteClientIdRefEmpty = true;
    } else if (clientAccessor->second->CheckIsRefIdsEmpty()) {
        // erase clientId from clientRefTable_.
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            clientRefTable_.erase(clientAccessor), StatusCode::K_RUNTIME_ERROR,
            FormatString("Fail to erase clientId %s from clientRefTable_", clientAccessor->first));
        isRemoteClientIdRefEmpty = true;
    }
    // erase remoteClientId from remoteClientIdTable_.
    if (isRemoteClient && isRemoteClientIdRefEmpty) {
        (void)remoteClientIdTable_.erase(clientId);
    }
    return Status::OK();
}

void ObjectGlobalRefTable::GetAllRef(std::unordered_map<std::string, std::unordered_set<std::string>> &refTable) const
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    for (const auto &kv : objectRefTable_) {
        std::unordered_set<std::string> set(kv.second.begin(), kv.second.end());
        refTable.emplace(kv.first, std::move(set));
    }
}

void ObjectGlobalRefTable::GetAllClientRef(std::unordered_map<std::string, std::vector<std::string>> &refTable) const
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    for (const auto &kv : clientRefTable_) {
        std::vector<std::string> objKeys;
        kv.second->GetRefIds(objKeys);
        refTable.emplace(kv.first, objKeys);
    }
}

void ObjectGlobalRefTable::GetRemoteClientIds(std::unordered_set<std::string> &remoteClientIds) const
{
    remoteClientIds.clear();
    for (TbbFirstRemoteClientTable::const_iterator it = remoteClientIdTable_.begin(); it != remoteClientIdTable_.end();
         ++it) {
        (void)remoteClientIds.insert(it->first);
    }
}

void ObjectGlobalRefTable::GetClientRefIds(const std::string &clientId, std::vector<std::string> &objectKeys) const
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbClientRefTable::const_accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        accessor->second->GetRefIds(objectKeys);
    }
}

bool ObjectGlobalRefTable::IsNotExistRemoteClientId(const std::string &clientId) const
{
    TbbFirstRemoteClientTable::const_accessor accessor;
    auto exists = remoteClientIdTable_.find(accessor, clientId);
    return !exists;
}

uint32_t ObjectGlobalRefTable::GetRefWorkerCount(const std::string &objectKey) const
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbObjRefTable::accessor accessor;
    if (objectRefTable_.find(accessor, objectKey)) {
        return accessor->second.size();
    }
    return 0;
}

void ObjectGlobalRefTable::GetRefWorkerCounts(const std::vector<std::string> &objectKeys,
                                              std::vector<uint32_t> &refCounts) const
{
    for (const auto &objectKey : objectKeys) {
        refCounts.emplace_back(GetRefWorkerCount(objectKey));
    }
}

void ObjectGlobalRefTable::GetObjRefIds(const std::string &objectKey, std::vector<std::string> &clientIds) const
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    TbbObjRefTable::accessor accessor;
    if (objectRefTable_.find(accessor, objectKey)) {
        clientIds.insert(clientIds.end(), accessor->second.begin(), accessor->second.end());
    }
}

Status SharedMemoryRefTable::GetShmUnit(const std::string &shmId, std::shared_ptr<ShmUnit> &shmUnit)
{
    TbbMemoryObjectRefTable::accessor shmAccessor;
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
        auto clientInfo = std::make_shared<ObjectRefInfo>();
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
        auto clientInfo = std::make_shared<ObjectRefInfo>();
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

Status SharedMemoryRefTable::RemoveShmUnit(const std::string &clientId, const std::string &shmId)
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
                         << BytesUuidToString(shmId);
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

bool SharedMemoryRefTable::Contains(const std::string &clientId, const std::string &shmId) const
{
    TbbMemoryClientRefTable::accessor accessor;
    if (clientRefTable_.find(accessor, clientId)) {
        return accessor->second->Contains(shmId);
    }
    return false;
}

void SharedMemoryRefTable::GetClientRefIds(const std::string &clientId, std::vector<std::string> &shmIds) const
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
    std::vector<std::string> shmIds;
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
