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
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_OBJECTREFINFO_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_OBJECTREFINFO_H

#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
template  <typename T>
class ObjectRefInfo {
using TbbObjKeyTable = tbb::concurrent_hash_map<T, uint32_t>;
public:
    explicit ObjectRefInfo(bool isUniqueCnt = true) : isUniqueCnt_(isUniqueCnt)
    {
    }

    ~ObjectRefInfo() = default;

    /**
     * @brief Add the reference to the object.
     * @param[in] objectKey The object key to add, it cannot be empty.
     * @param[in] ref ref num need to add.
     * @return True on success, false otherwise.
     */
    bool AddRef(const T &objectKey, uint32_t ref = 1);

    /**
     * @brief Remove the reference to the object.
     * @param[in] objectKey The object key to remove, it cannot be empty.
     * @return True on success, false otherwise.
     */
    bool RemoveRef(const T &objectKey);

    /**
     * @brief Check if the id is contains.
     * @param[in] objectKey The id of object.
     * @return True on success, false otherwise.
     */
    bool Contains(const T &objectKey) const;

    /**
     * @brief Get number of references for a object key.
     * @param[in] objectKey The id of object.
     * @return reference count if Id is present, 0 otherwise.
     */
    uint32_t GetRefCount(const T &objectKey);

    /**
     * @brief Used to update reference count for a objectKey during recovery
     * @param[in] objectKey The id of object.
     * @param[in] count The reference count for the object.
     * @return Status of the call.
     */
    Status UpdateRefCount(const T &objectKey, int count);

    /**
     * @brief Get all ref ids.
     * @param[out] objectKeys The object keys.
     */
    void GetRefIds(std::vector<T> &objectKeys) const;

    /**
     * @brief Check if the obj is dependent on other objs.
     * @param[in] objectKey The id of object.
     * @return Whether it is no ref.
     */
    bool CheckIsNoneRef(const T &objectKey) const;

    /**
     * @brief Check is objectKeys_ are empty.
     * @return True if ids set empty.
     */
    bool CheckIsRefIdsEmpty() const;

private:
    // In this client map is object key and objects referenced cnt by this object.
    TbbObjKeyTable objectKeys_;
    // protect the objectKeys_ map
    mutable std::shared_timed_mutex objectKeyMapMutex_;
    bool isUniqueCnt_;
};

template <typename KeyType>
class ObjectGlobalRefTable {
    using TbbRefTable = tbb::concurrent_hash_map<KeyType, std::shared_ptr<ObjectRefInfo<std::string>>>;
    using TbbObjRefTable = tbb::concurrent_hash_map<ImmutableString, std::unordered_set<KeyType>>;
    using TbbFirstRemoteClientTable = tbb::concurrent_hash_map<KeyType, std::nullptr_t>;

public:
    explicit ObjectGlobalRefTable() = default;

    ~ObjectGlobalRefTable() = default;

    /**
     * @brief Increase the global reference count and construct object-address mapping.
     * @param[in] refId The ref key, clientId(in worker side) or address (in master side).
     * @param[in] objectKeys Client references' ids.
     * @param[out] failedIncIds Failed increase ids.
     * @param[out] firstIncIds The first time to increase ids.
     * @param[in] isRemoteClient Identifies whether the reference counting request is in-cloud or out-of-cloud.
     * @return Status of the call.
     */
    Status GIncreaseRef(const KeyType &refId, const std::vector<std::string> &objectKeys,
                        std::vector<std::string> &failedIncIds, std::vector<std::string> &firstIncIds,
                        bool isRemoteClient = false)
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        typename TbbRefTable::const_accessor clientAccessor;
        while (!clientRefTable_.find(clientAccessor, refId)) {
            typename TbbRefTable::accessor accessor;
            auto clientInfo = std::make_shared<ObjectRefInfo<std::string>>();  // std::string -> ObjectKey
            clientRefTable_.emplace(accessor, refId, std::move(clientInfo));
            // In the off-cloud reference counting scenario, if remoteClient appears for the first time, record it to
            // remoteClientIdTable_.
            if (isRemoteClient) {
                (void)remoteClientIdTable_.insert({ refId, nullptr });
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
                rc = saveToKvStore_(refId, objectKey, isRemoteClient);
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
            typename TbbObjRefTable::accessor objAccessor;
            if (!objectRefTable_.find(objAccessor, objectKey)) {
                (void)objectRefTable_.insert(objAccessor, objectKey);
            }
            if (objAccessor->second.empty()) {
                firstIncIds.emplace_back(objectKey);
            }
            objAccessor->second.emplace(refId);
        }
        return rc;
    }

    /**
     * @brief Decrease the global reference count and construct object-address mapping.
     * @param[in] refId The ref key, clientId(in worker side) or address (in master side).
     * @param[in] objectKeys Client references' ids.
     * @param[out] failDecIds Fail decrease ids.
     * @param[out] finishDecIds The last time to decrease ids.
     * @param[in] isRemoteClient Identifies whether the reference counting request is in-cloud or out-of-cloud.
     * @return Status of the call.
     */
    Status GDecreaseRef(const KeyType &refId, const std::vector<std::string> &objectKeys,
                        std::vector<std::string> &failedDecIds, std::vector<std::string> &finishDecIds,
                        bool isRemoteClient = false)
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        std::vector<std::string> successVec;
        Status rc = Status::OK();
        {
            typename TbbRefTable::const_accessor clientAccessor;
            if (!clientRefTable_.find(clientAccessor, refId)) {
                LOG(WARNING) << FormatString("GDecreaseRef is being processed, but the client id(%s) does not exist.",
                                             refId);
                return Status::OK();
            }
            for (const auto &objectKey : objectKeys) {
                if (!clientAccessor->second->RemoveRef(objectKey)) {
                    LOG(WARNING) << FormatString(
                        "GDecreaseRef is being processed, but the object key(%s) does not exist.", objectKey);
                    continue;
                }
                successVec.emplace_back(objectKey);
                if (removeFromKvStore_) {
                    rc = removeFromKvStore_(refId, objectKey, isRemoteClient);
                    if (rc.IsError()) {
                        (void)clientAccessor->second->AddRef(objectKey);
                        failedDecIds.emplace_back(objectKey);
                        LOG(ERROR) << "Remove global reference from kv store failed. status:" << rc.ToString();
                    }
                }
            }
        }
        std::sort(successVec.begin(), successVec.end());

        RETURN_IF_NOT_OK(TryEraseClientId(refId, isRemoteClient));
        for (const auto &objectKey : successVec) {
            typename TbbObjRefTable::accessor objAccessor;
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectRefTable_.find(objAccessor, objectKey),
                                                 StatusCode::K_RUNTIME_ERROR,
                                                 FormatString("Fail to find objectKey: %s", objectKey));
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objAccessor->second.erase(refId), StatusCode::K_RUNTIME_ERROR,
                                                 FormatString("Fail to erase refId: %s", refId));
            if (objAccessor->second.empty()) {
                finishDecIds.emplace_back(objectKey);
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(objectRefTable_.erase(objAccessor), StatusCode::K_RUNTIME_ERROR,
                                                     FormatString("Fail to erase objectKey: %s", objAccessor->first));
            }
        }
        return rc;
    }

    /**
     * @brief Get the mapping of object-address.
     * @param[out] refTable The relationship of object and address.
     */
    void GetAllRef(std::unordered_map<std::string, std::unordered_set<KeyType>> &refTable) const
    {
        std::lock_guard<std::shared_timed_mutex> lck(mutex_);
        for (const auto &kv : objectRefTable_) {
            std::unordered_set<KeyType> set(kv.second.begin(), kv.second.end());
            refTable.emplace(kv.first, std::move(set));
        }
    }

    /**
     * @brief Get a copy of global reference table with client as the key and objects as the value.
     * @param[out] refTable The relationship of client and objects.
     */
    void GetAllClientRef(std::unordered_map<KeyType, std::vector<std::string>> &refTable) const
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        for (const auto &kv : clientRefTable_) {
            std::vector<std::string> objKeys;
            kv.second->GetRefIds(objKeys);
            refTable.emplace(kv.first, objKeys);
        }
    };

    /**
     * @brief Get a copy of the remoteClientIdSet_ set of the global reference table.
     * @param[out] remoteClientIds The value specifies which meta is ot-cloud reference counting.
     */
    void GetRemoteClientIds(std::unordered_set<KeyType> &remoteClientIds) const
    {
        remoteClientIds.clear();
        for (typename TbbFirstRemoteClientTable::const_iterator it = remoteClientIdTable_.begin();
             it != remoteClientIdTable_.end(); ++it) {
            (void)remoteClientIds.insert(it->first);
        }
    };

    /**
     * @brief Get all object keys by this client id
     * @param[in] refId The ref key, clientId(in worker side) or address (in master side).
     * @param[out] objectKeys The object keys.
     */
    void GetClientRefIds(const KeyType &refId, std::vector<std::string> &objectKeys) const
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        typename TbbRefTable::const_accessor accessor;
        if (clientRefTable_.find(accessor, refId)) {
            accessor->second->GetRefIds(objectKeys);
        }
    };

    /**
     * @brief Check whether the refId exists in the key of the clientRefTable_ table.
     * @param[in] refId The ref key, clientId(in worker side) or address (in master side).
     * @return false if it exists, true if it doesn't
     */
    bool IsNotExistRemoteClientId(const KeyType &refId) const
    {
        typename TbbFirstRemoteClientTable::const_accessor accessor;
        auto exists = remoteClientIdTable_.find(accessor, refId);
        return !exists;
    }

    /**
     * @brief Get the Ref Worker Count for object key.
     * @param[in] objectKey The list of object key.
     * @return The reference count.
     */
    uint32_t GetRefWorkerCount(const std::string &objectKey) const
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        typename TbbObjRefTable::accessor accessor;
        if (objectRefTable_.find(accessor, objectKey)) {
            return accessor->second.size();
        }
        return 0;
    }

    /**
     * @brief Get the Ref Worker Count for each object key.
     * @param[in] objectKeys The list of object key.
     * @param[out] refCounts The reference count.
     */
    void GetRefWorkerCounts(const std::vector<std::string> &objectKeys, std::vector<uint32_t> &refCounts) const
    {
        for (const auto &objectKey : objectKeys) {
            refCounts.emplace_back(GetRefWorkerCount(objectKey));
        }
    }

    /**
     * @brief Get all client ids by object key.
     * @param[in] objectKey The object key.
     * @param[out] refIds The list of refId.
     */
    void GetObjRefIds(const std::string &objectKey, std::vector<KeyType> &refIds) const
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        typename TbbObjRefTable::accessor accessor;
        if (objectRefTable_.find(accessor, objectKey)) {
            refIds.insert(refIds.end(), accessor->second.begin(), accessor->second.end());
        }
    };

    /**
     * @brief Register Persistence Functions.
     * @param[in] saveToKvStore Function for save to kvstore.
     * @param[out] removeFromKvStore Function for remove from kvstore.
     */
    void RegisterPersistenceFunc(
        const std::function<Status(const std::string &, const std::string &, bool)> &saveToKvStore,
        const std::function<Status(const std::string &, const std::string &, bool)> &removeFromKvStore)
    {
        saveToKvStore_ = saveToKvStore;
        removeFromKvStore_ = removeFromKvStore;
    }

    size_t GetClientRefCount()
    {
        return clientRefTable_.size();
    }

    size_t GetObjectRefCount()
    {
        return objectRefTable_.size();
    }

    size_t GetRemoteClientCount()
    {
        return remoteClientIdTable_.size();
    }

private:
    /**
     * @brief Try to erase the refId from clientRefTable And remoteClientIdSet_.
     * @param[in] refId The ref key, clientId(in worker side) or address (in master side).
     * @return Status of the call.
     */
    Status TryEraseClientId(const KeyType &refId, bool isRemoteClient)
    {
        typename TbbRefTable::accessor clientAccessor;
        bool isRemoteClientIdRefEmpty = false;
        if (!clientRefTable_.find(clientAccessor, refId)) {
            LOG(INFO) << FormatString("GDecreaseRef is being processed, but the client id(%s) does not exist.", refId);
            isRemoteClientIdRefEmpty = true;
        } else if (clientAccessor->second->CheckIsRefIdsEmpty()) {
            // erase refId from clientRefTable_.
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                clientRefTable_.erase(clientAccessor), StatusCode::K_RUNTIME_ERROR,
                FormatString("Fail to erase refId %s from clientRefTable_", clientAccessor->first));
            isRemoteClientIdRefEmpty = true;
        }
        // erase remoteClientId from remoteClientIdTable_.
        if (isRemoteClient && isRemoteClientIdRefEmpty) {
            (void)remoteClientIdTable_.erase(refId);
        }
        return Status::OK();
    };

    // The map is [client id(in worker side) or address(in master side)] -> ObjectInfo
    TbbRefTable clientRefTable_;
    // The map is object key -> [client id(in worker side) or address(in master side)]
    TbbObjRefTable objectRefTable_;
    // The table of remote client id.
    // Use concurrent_hash_map as a set to achieve thread security. The key is remoteClientId, and the value is nullptr.
    TbbFirstRemoteClientTable remoteClientIdTable_;

    mutable std::shared_timed_mutex mutex_;

    std::function<Status(const std::string &, const std::string &, bool)> saveToKvStore_;
    std::function<Status(const std::string &, const std::string &, bool)> removeFromKvStore_;
};

using TbbMemoryClientRefTable = tbb::concurrent_hash_map<ClientKey, std::shared_ptr<ObjectRefInfo<ShmKey>>>;
using TbbMemoryObjectRefTable =
    tbb::concurrent_hash_map<ShmKey, std::pair<std::shared_ptr<ShmUnit>, std::unordered_set<ClientKey>>>;
class SharedMemoryRefTable {
public:
    SharedMemoryRefTable() = default;

    ~SharedMemoryRefTable() = default;

    /**
     * @brief Add shared memory unit reference to the client table.
     * @param[in] clientId uuid of client.
     * @param[in] shmUnit The safe object.
     */
    void AddShmUnit(const ClientKey &clientId, std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Add shared memory units reference to the client table.
     * @param[in] clientId uuid of client.
     * @param[in] shmUnits The safe objects.
     */
    void AddShmUnits(const ClientKey &clientId, std::vector<std::shared_ptr<ShmUnit>> &shmUnits);

    /**
     * @brief Check one shared memory unit whether be referred by client.
     * @param[in] objectKey Shared memory unit id of object.
     * @param[out] shmUnit Shared memory unit shared ptr.
     * @return true on success, false otherwise.
     */
    Status GetShmUnit(const ShmKey &shmId, std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Remove a client from the client table.
     * @param[in] clientId uuid of client.
     * @param[in] shmId The shared memory id.
     * @return Status of the call
     */
    Status RemoveShmUnit(const ClientKey &clientId, const ShmKey &shmId);

    /**
     * @brief Remove a client from the client table.
     * @param[in] clientId uuid of client.
     * @return Status of the call
     */
    Status RemoveClient(const ClientKey &clientId);

#ifdef WITH_TESTS
    /**
     * @brief Check one object whether be referred by client.
     * @param[in] clientId uuid of client.
     * @param[in] shmId Shared memory unit id of object.
     * @return true on success, false otherwise.
     */
    bool Contains(const ClientKey &clientId, const ShmKey &shmId) const;
#endif

    /**
     * @brief Get all share memory unit ids referred by client.
     * @param[in] clientId uuid of client.
     * @param[out] shmIds Shared memory unit id of object.
     */
    void GetClientRefIds(const ClientKey &clientId, std::vector<ShmKey> &shmIds) const;

private:
    /**
     * @brief Remove shared memory unit reference from the client table and shm table.
     * @param[in] clientId uuid of client.
     * @param[in/out] shmAccessor the tbb accessor of object reference table.
     * @param[in/out] clientAccessor the tbb accessor of client table.
     */
    void RemoveShmUnitDetail(const ClientKey &clientId, TbbMemoryObjectRefTable::accessor &shmAccessor,
                             TbbMemoryClientRefTable::accessor &clientAccessor);

    // The map is client id and the shared memory ids referenced by this client.
    TbbMemoryClientRefTable clientRefTable_;
    // The map is shared memory id and the client ids referenced by this object.
    TbbMemoryObjectRefTable shmRefTable_;

    mutable std::shared_timed_mutex mutex_;
};

template <typename T>
bool ObjectRefInfo<T>::AddRef(const T &objectKey, uint32_t ref)
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    typename TbbObjKeyTable::accessor objAccessor;
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

template <typename T>
uint32_t ObjectRefInfo<T>::GetRefCount(const T &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    typename TbbObjKeyTable::const_accessor objAccessor;
    if (objectKeys_.find(objAccessor, objectKey)) {
        return objAccessor->second;
    }
    return 0;
}

template <typename T>
Status ObjectRefInfo<T>::UpdateRefCount(const T &objectKey, int count)
{
    if (count < 0) {
        RETURN_STATUS(StatusCode::K_INVALID, FormatString("[ObjectId %s] Invalid count: %d", objectKey, count));
    }
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    typename TbbObjKeyTable::accessor objAccessor;
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

template <typename T>
bool ObjectRefInfo<T>::RemoveRef(const T &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    typename TbbObjKeyTable::accessor objAccessor;
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

template <typename T>
bool ObjectRefInfo<T>::Contains(const T &objectKey) const
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    return objectKeys_.count(objectKey) == 1;
}

template <typename T>
void ObjectRefInfo<T>::GetRefIds(std::vector<T> &objectKeys) const
{
    std::lock_guard<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    std::transform(objectKeys_.begin(), objectKeys_.end(), std::back_inserter(objectKeys),
                   [](auto &kv) { return kv.first; });
}

template <typename T>
bool ObjectRefInfo<T>::CheckIsNoneRef(const T &objectKey) const
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    typename TbbObjKeyTable::const_accessor objAccessor;
    if (!objectKeys_.find(objAccessor, objectKey)) {
        return true;
    } else if (objAccessor->second == 0) {
        return true;
    }
    return false;
}

template <typename T>
bool ObjectRefInfo<T>::CheckIsRefIdsEmpty() const
{
    std::shared_lock<std::shared_timed_mutex> lock(objectKeyMapMutex_);
    return objectKeys_.empty();
}
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_OBJECTREFINFO_H
