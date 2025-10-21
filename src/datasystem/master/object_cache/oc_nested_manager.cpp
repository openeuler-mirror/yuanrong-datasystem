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
 * Description: Module responsible for managing the object's nested relationship on the master.
 */
#include "datasystem/common/util/strings_util.h"
#include "datasystem/master/object_cache/oc_nested_manager.h"

namespace datasystem {
namespace master {
bool OCNestedManager::CheckIsNoneNestedRefById(const std::string &childObjectKey)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("CheckIsNoneNestedRefById get lock", timer.ElapsedMilliSecond());
    return nestedRef_->CheckIsNoneRef(childObjectKey);
}

Status OCNestedManager::IncreaseNestedRefCnt(const std::string &parentObjectKey,
                                             const std::set<ImmutableString> &nestedObjectKeys)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("IncreaseNestedRefCnt 2 params get lock", timer.ElapsedMilliSecond());
    auto iter = dependencyTable_.find(parentObjectKey);
    if (iter == dependencyTable_.end()) {
        dependencyTable_.emplace(parentObjectKey, nestedObjectKeys);
    } else {
        iter->second = nestedObjectKeys;
    }

    for (const auto &id : nestedObjectKeys) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->AddNestedRelationship(parentObjectKey, id),
                                         "Add nested dependency to RocksDb failed.");
    }
    return Status::OK();
}

Status OCNestedManager::IncreaseNestedRefCnt(const std::string &nestedObjectKey, uint32_t ref)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("IncreaseNestedRefCnt get lock", timer.ElapsedMilliSecond());
    LOG(INFO) << "Increasing nested ref count for ObjectKey: " << nestedObjectKey << ", ref is " << ref;
    if (nestedRef_->AddRef(nestedObjectKey, ref)) {
        objectStore_->UpdateNestedRefCount(nestedObjectKey, nestedRef_->GetRefCount(nestedObjectKey));
        return Status::OK();
    }

    RETURN_STATUS(StatusCode::K_DUPLICATED, "object key is marked to be unique");
}

void OCNestedManager::GetAllParentIds(std::vector<std::string> &objKeys)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    for (const auto &dependencyInfo : dependencyTable_) {
        objKeys.emplace_back(dependencyInfo.first);
    }
}

void OCNestedManager::GetAllNestedKeys(std::vector<std::string> &nestedKeys)
{
    nestedRef_->GetRefIds(nestedKeys);
}

uint32_t OCNestedManager::GetNestedKeyRef(const std::string &objKey)
{
    return nestedRef_->GetRefCount(objKey);
}

void OCNestedManager::GetNestedRelationship(const std::string &parentId, std::vector<std::string> &nestedKeys)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    auto iter = dependencyTable_.find(parentId);
    if (iter != dependencyTable_.end()) {
        nestedKeys = { iter->second.begin(), iter->second.end() };
    }
}

void OCNestedManager::RemoveRelationshipData(const std::string &parentObjectKey)
{
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    auto iter = dependencyTable_.find(parentObjectKey);
    if (iter == dependencyTable_.end()) {
        return;
    }
    for (const auto &id : iter->second) {
        LOG_IF_ERROR(objectStore_->RemoveNestedRelationship(parentObjectKey, id),
                     FormatString("remove from rocksdb failed, parentId: %s, id: %s", parentObjectKey, id));
    }
    dependencyTable_.erase(parentObjectKey);
}

void OCNestedManager::RemoveNestIdsRef(const std::vector<std::string> &nestedKeys)
{
    VLOG(1) << "remove nested ref ids: " << VectorToString(nestedKeys);
    for (const auto &id : nestedKeys) {
        if (nestedRef_->RemoveRef(id)) {
            LOG_IF_ERROR(objectStore_->RemoveNestedRefCount(id),
                         FormatString("remove from rocksdb fialed, objectKey: %s", id));
        }
    }
}

bool OCNestedManager::CheckIsNotParentId(const std::string &objKey)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    auto iter = dependencyTable_.find(objKey);
    return iter == dependencyTable_.end();
}

Status OCNestedManager::DecreaseNestedRefCnt(const std::string &nestedObjectKey)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("DecreaseNestedRefCnt get lock", timer.ElapsedMilliSecond());
    LOG(INFO) << "Decreasing nested ref count for ObjectKey: " << nestedObjectKey;
    if (nestedRef_->RemoveRef(nestedObjectKey)) {
        uint32_t count = nestedRef_->GetRefCount(nestedObjectKey);
        if (count) {
            objectStore_->UpdateNestedRefCount(nestedObjectKey, count);
        } else {
            objectStore_->RemoveNestedRefCount(nestedObjectKey);  // remove if count is 0
        }
        return Status::OK();
    }
    RETURN_STATUS(StatusCode::K_NOT_FOUND, "This Object had no dependencies");
}

Status OCNestedManager::DecreaseNestedRefCnt(const std::string &parentObjectKey, std::vector<std::string> &zeroRefIds)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("DecreaseNestedRefCnt 2 params get lock", timer.ElapsedMilliSecond());
    auto iter = dependencyTable_.find(parentObjectKey);
    if (iter == dependencyTable_.end()) {
        return Status::OK();
    }
    for (const auto &id : iter->second) {
        if (!nestedRef_->RemoveRef(id)) {  // This only works for ids that are owned by this master
            LOG(WARNING) << FormatString("Remove nested reference relationship, error to decrease %s", id);
        }
        if (!nestedRef_->Contains(id)) {
            zeroRefIds.emplace_back(id);  // All ids that are not owned by the master will be come here
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->RemoveNestedRelationship(parentObjectKey, id),
                                         "Remove nested dependency from rocksdb failed.");
    }
    (void)dependencyTable_.erase(iter);
    return Status::OK();
}

Status OCNestedManager::RecoverRelationshipData(const std::string &refTable, const std::string &countTable)
{
    std::vector<std::pair<std::string, std::string>> metas;
    if (objectStore_->IsRocksdbRunning() || etcdCM_->IsCentralized()) {
        RETURN_IF_NOT_OK(objectStore_->GetAllFromRocks(refTable, metas));
    } else {
        LOG(WARNING) << FormatString("Table[%s] does not support using etcd as a l2_cache, just ignore it", refTable);
    };
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("RecoverRelationshipData", timer.ElapsedMilliSecond());
    for (const auto &meta : metas) {
        auto parentIdPos = meta.first.rfind("_");
        if (parentIdPos != std::string::npos) {
            dependencyTable_[meta.first.substr(0, parentIdPos)].emplace(meta.second);
        } else {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("Parse key to parentId failed : %s", meta.first));
        }
    }
    metas.clear();
    if (objectStore_->IsRocksdbRunning() || etcdCM_->IsCentralized()) {
        RETURN_IF_NOT_OK(objectStore_->GetAllFromRocks(countTable, metas));
    } else {
        LOG(WARNING) << FormatString("Table[%s] does not support using etcd as a l2_cache, just ignore it", countTable);
    };

    int count = 0;
    for (const auto &meta : metas) {
        if (!StringToInt(meta.second, count)) {
            RETURN_STATUS(StatusCode::K_INVALID, "Parse failed with count " + meta.second);
        }
        RETURN_IF_NOT_OK(nestedRef_->UpdateRefCount(meta.first, count));
    }
    return Status::OK();
}

bool OCNestedManager::NestedKeysCanSet(const std::string &parentObjectKey,
                                      const std::set<ImmutableString> &nestedObjectKeys)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("NestedKeysCanSet", timer.ElapsedMilliSecond());
    auto iter = dependencyTable_.find(parentObjectKey);
    if (iter == dependencyTable_.end() || iter->second.empty() || iter->second == nestedObjectKeys) {
        return true;
    } else {
        return false;
    }
}

bool OCNestedManager::IsNestedKeysDiff(const std::string &parentObjectKey,
                                      const std::set<ImmutableString> &nestedObjectKeys)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    masterOperationTimeCost.Append("IsNestedKeysDiff", timer.ElapsedMilliSecond());
    auto iter = dependencyTable_.find(parentObjectKey);
    if (iter == dependencyTable_.end() || iter->second != nestedObjectKeys) {
        return true;
    } else {
        return false;
    }
}
}  // namespace master
}  // namespace datasystem
