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
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

using TbbObjKeyTable = tbb::concurrent_hash_map<ImmutableString, uint32_t>;
class ObjectRefInfo {
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
    bool AddRef(const std::string &objectKey, uint32_t ref = 1);

    /**
     * @brief Remove the reference to the object.
     * @param[in] objectKey The object key to remove, it cannot be empty.
     * @return True on success, false otherwise.
     */
    bool RemoveRef(const std::string &objectKey);

    /**
     * @brief Check if the id is contains.
     * @param[in] objectKey The id of object.
     * @return True on success, false otherwise.
     */
    bool Contains(const std::string &objectKey) const;

    /**
     * @brief Get number of references for a object key.
     * @param[in] objectKey The id of object.
     * @return reference count if Id is present, 0 otherwise.
     */
    uint32_t GetRefCount(const std::string &objectKey);

    /**
     * @brief Used to update reference count for a objectKey during recovery
     * @param[in] objectKey The id of object.
     * @param[in] count The reference count for the object.
     * @return Status of the call.
     */
    Status UpdateRefCount(const std::string &objectKey, int count);

    /**
     * @brief Get all ref ids.
     * @param[out] objectKeys The object keys.
     */
    void GetRefIds(std::vector<std::string> &objectKeys) const;

    /**
     * @brief Check if the obj is dependent on other objs.
     * @param[in] objectKey The id of object.
     * @return Whether it is no ref.
     */
    bool CheckIsNoneRef(const std::string &objectKey) const;

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

using TbbClientRefTable = tbb::concurrent_hash_map<ImmutableString, std::shared_ptr<ObjectRefInfo>>;
using TbbObjRefTable = tbb::concurrent_hash_map<ImmutableString, std::unordered_set<ImmutableString>>;
using TbbFirstRemoteClientTable = tbb::concurrent_hash_map<ImmutableString, std::nullptr_t>;
class ObjectGlobalRefTable {
public:
    explicit ObjectGlobalRefTable() = default;

    ~ObjectGlobalRefTable() = default;

    /**
     * @brief Increase the global reference count and construct object-address mapping.
     * @param[in] clientId Uuid of client.
     * @param[in] objectKeys Client references' ids.
     * @param[out] failedIncIds Failed increase ids.
     * @param[out] firstIncIds The first time to increase ids.
     * @param[in] isRemoteClient Identifies whether the reference counting request is in-cloud or out-of-cloud.
     * @return Status of the call.
     */
    Status GIncreaseRef(const std::string &clientId, const std::vector<std::string> &objectKeys,
        std::vector<std::string> &failedIncIds, std::vector<std::string> &firstIncIds, bool isRemoteClient = false);

    /**
     * @brief Decrease the global reference count and construct object-address mapping.
     * @param[in] clientId Uuid of client.
     * @param[in] objectKeys Client references' ids.
     * @param[out] failDecIds Fail decrease ids.
     * @param[out] finishDecIds The last time to decrease ids.
     * @param[in] isRemoteClient Identifies whether the reference counting request is in-cloud or out-of-cloud.
     * @return Status of the call.
     */
    Status GDecreaseRef(const std::string &clientId, const std::vector<std::string> &objectKeys,
        std::vector<std::string> &failedDecIds, std::vector<std::string> &finishDecIds, bool isRemoteClient = false);

    /**
     * @brief Get the mapping of object-address.
     * @param[out] refTable The relationship of object and address.
     */
    void GetAllRef(std::unordered_map<std::string, std::unordered_set<std::string>> &refTable) const;

    /**
     * @brief Get a copy of global reference table with client as the key and objects as the value.
     * @param[out] refTable The relationship of client and objects.
     */
    void GetAllClientRef(std::unordered_map<std::string, std::vector<std::string>> &refTable) const;

    /**
     * @brief Get a copy of the remoteClientIdSet_ set of the global reference table.
     * @param[out] remoteClientIds The value specifies which meta is ot-cloud reference counting.
     */
    void GetRemoteClientIds(std::unordered_set<std::string> &remoteClientIds) const;

    /**
     * @brief Get all object keys by this client id
     * @param[in] clientId Uuid of client.
     * @param[out] objectKeys The object keys.
     */
    void GetClientRefIds(const std::string &clientId, std::vector<std::string> &objectKeys) const;

    /**
     * @brief Check whether the clientId exists in the key of the clientRefTable_ table.
     * @param[in] clientId Uuid of client.
     * @return false if it exists, true if it doesn't
     */
    bool IsNotExistRemoteClientId(const std::string &clientId) const;

    /**
     * @brief Get the Ref Worker Count for object key.
     * @param[in] objectKey The list of object key.
     * @return The reference count.
     */
    uint32_t GetRefWorkerCount(const std::string &objectKey) const;

    /**
     * @brief Get the Ref Worker Count for each object key.
     * @param[in] objectKeys The list of object key.
     * @param[out] refCounts The reference count.
     */
    void GetRefWorkerCounts(const std::vector<std::string> &objectKeys, std::vector<uint32_t> &refCounts) const;

    /**
     * @brief Get all client ids by object key.
     * @param[in] objectKey The object key.
     * @param[out] clientIds The list of clientId.
     */
    void GetObjRefIds(const std::string &objectKey, std::vector<std::string> &clientIds) const;

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
     * @brief Try to erase the clientId from clientRefTable And remoteClientIdSet_.
     * @param[in] clientId Uuid of client.
     * @return Status of the call.
     */
    Status TryEraseClientId(const std::string &clientId, bool isRemoteClient);

    // The map is client id and the objects referenced by this client.
    TbbClientRefTable clientRefTable_;
    // The map is object key and the address referenced by this object.
    TbbObjRefTable objectRefTable_;
    // The table of remote client id.
    // Use concurrent_hash_map as a set to achieve thread security. The key is remoteClientId, and the value is nullptr.
    TbbFirstRemoteClientTable remoteClientIdTable_;

    mutable std::shared_timed_mutex mutex_;

    std::function<Status(const std::string &, const std::string &, bool)> saveToKvStore_;
    std::function<Status(const std::string &, const std::string &, bool)> removeFromKvStore_;
};

using TbbMemoryClientRefTable = tbb::concurrent_hash_map<ImmutableString, std::shared_ptr<ObjectRefInfo>>;
using TbbMemoryObjectRefTable =
    tbb::concurrent_hash_map<ImmutableString, std::pair<std::shared_ptr<ShmUnit>, std::unordered_set<ImmutableString>>>;
class SharedMemoryRefTable {
public:
    SharedMemoryRefTable() = default;

    ~SharedMemoryRefTable() = default;

    /**
     * @brief Add shared memory unit reference to the client table.
     * @param[in] clientId uuid of client.
     * @param[in] shmUnit The safe object.
     */
    void AddShmUnit(const std::string &clientId, std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Check one shared memory unit whether be referred by client.
     * @param[in] objectKey Shared memory unit id of object.
     * @param[out] shmUnit Shared memory unit shared ptr.
     * @return true on success, false otherwise.
     */
    Status GetShmUnit(const std::string &shmId, std::shared_ptr<ShmUnit> &shmUnit);

    /**
     * @brief Remove a client from the client table.
     * @param[in] clientId uuid of client.
     * @param[in] shmId The shared memory id.
     * @return Status of the call
     */
    Status RemoveShmUnit(const std::string &clientId, const std::string &shmId);

    /**
     * @brief Remove a client from the client table.
     * @param[in] clientId uuid of client.
     * @return Status of the call
     */
    Status RemoveClient(const std::string &clientId);

    /**
     * @brief Check one object whether be referred by client.
     * @param[in] clientId uuid of client.
     * @param[in] shmId Shared memory unit id of object.
     * @return true on success, false otherwise.
     */
    bool Contains(const std::string &clientId, const std::string &shmId) const;

    /**
     * @brief Get all share memory unit ids referred by client.
     * @param[in] clientId uuid of client.
     * @param[out] shmIds Shared memory unit id of object.
     */
    void GetClientRefIds(const std::string &clientId, std::vector<std::string> &shmIds) const;

private:
    /**
     * @brief Remove shared memory unit reference from the client table and shm table.
     * @param[in] clientId uuid of client.
     * @param[in/out] shmAccessor the tbb accessor of object reference table.
     * @param[in/out] clientAccessor the tbb accessor of client table.
     */
    void RemoveShmUnitDetail(const std::string &clientId, TbbMemoryObjectRefTable::accessor &shmAccessor,
                             TbbMemoryClientRefTable::accessor &clientAccessor);

    // The map is client id and the shared memory ids referenced by this client.
    TbbMemoryClientRefTable clientRefTable_;
    // The map is shared memory id and the client ids referenced by this object.
    TbbMemoryObjectRefTable shmRefTable_;

    mutable std::shared_timed_mutex mutex_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_OBJECTREFINFO_H
