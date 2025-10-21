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
 * Description: Module responsible for managing global cache deletion.
 */
#ifndef MASTER_OC_GLOBAL_CACHE_DELETE_MANAGER_H
#define MASTER_OC_GLOBAL_CACHE_DELETE_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/l2cache/l2_storage.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"

namespace datasystem {
namespace master {
using GlobalDeleteInfoMap = std::unordered_map<std::string, std::unordered_map<uint64_t, uint64_t>>;

class OCGlobalCacheDeleteManager {
    struct DeleteMeta {
        uint64_t version;
        bool failed;
    };

public:
    /**
     * @brief Construct OCGlobalCacheDeleteManager for notifying global cache to delete object.
     */
    OCGlobalCacheDeleteManager(std::shared_ptr<ObjectMetaStore> objectStore, std::shared_ptr<PersistenceApi> persistApi,
                               bool backendStoreExist);

    ~OCGlobalCacheDeleteManager();

    /**
     * @brief Initialization.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Shutdown the oc global cache manager module.
     */
    void Shutdown();

    /**
     * @brief Insert data to global cache table.
     * @param[in] objectKey The object key to be deleted.
     * @param[in] objectVersion The object version.
     * @param[in] delVersion The version to be deleted.
     * @param[in] needPersist Indicates whether to persist to rocksdb.
     * @return Status of the call.
     */
    Status InsertDeletedObject(const std::string &objectKey, uint64_t objectVersion, uint64_t delVersion,
                               bool needPersist = false,
                               ObjectMetaStore::WriteType type = ObjectMetaStore::WriteType::ROCKS_ONLY);

    /**
     * @brief Remove data from global cache table.
     * @param[in] objectKeys The object keys to be remove.
     * @note The structure of std::unordered_map<std::string, uint64_t> is std::unordered_map<objectKey, version>
     * @return Status of the call.
     */
    Status RemoveDeletedObject(const std::unordered_map<std::string, uint64_t> &objectKeys);

    /**
     * @brief Recover global cache info table.
     * @param[in] isFromRocksdb Specifies whether to obtain data from rocksdb.
     * @param[in] workerUuids Obtains the data of specified worker uuids. If the value is empty, obtains the data of the
     * current worker.
     * @param[in] extraRanges Obtains the data of specified hash ranges if not empty.
     * @return Status of the call.
     */
    Status RecoverDeletedIds(bool isFromRocksdb, const std::vector<std::string> &workerUuids = {},
                             const worker::HashRange &extraRanges = {});

    /**
     * @brief Get the version of the deleting objects.
     * @param[in] objectKeys The object keys.
     * @param[out] deletingVersions The version of the deleting objects.
     */
    void GetDeletingVersions(const std::list<std::string> &objectKeys, std::vector<uint64_t> &deletingVersions);

    /**
     * @brief Get delete infos via obejct id.
     * @param[in] objectKey Object key.
     * @return object version and delete version pair list.
     */
    std::vector<std::pair<uint64_t, uint64_t>> GetDeletedInfos(const std::string &objectKey);

    /**
     * @brief Get global cache delete infos via match function.
     * @param[in] matchFunc Match function.
     * @return Global cache delete infos.
     */
    GlobalDeleteInfoMap GetDeletedInfosMatch(const std::function<bool(const std::string &)> &matchFunc);

    /**
     * @brief Insert delte object from migrate node.
     * @param[in] objMeta Migrate object meta.
     */
    void InsertDeletedObjectFromMigrateNode(const MetaForMigrationPb &objMeta);

    /**
     * @brief Get deleting object count.
     * @return The deleting object count.
     */
    size_t GetDeletingObjectCount()
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        return deleteIds_.size();
    }

private:
    /**
     * @brief Start the process of deleting global cache object.
     */
    void ProcessDeleteObjects();

    /**
     * @brief Delete object from global cache.
     * @param[in] objectKeys The object list need delete from global cache.
     */
    void DeleteObjects(const std::map<std::string, DeleteMeta> &objectKeys);

    /**
     * @brief Get deleted object keys.
     * @param[out] objectKeys The object list need delete from global cache.
     */
    void GetDeletedObjects(std::map<std::string, DeleteMeta> &deleteIds);

    /**
     * @brief delete the object in persistence api
     * @param[in] objectKey the object path in cloud storage, equal to objectKey
     * @param[in] objectVersion the object version
     * @param[in] maxVersionToDel indicate delete all the versions which <= maxVersionToDel
     */
    Status DelPersistenceObj(const std::string &objectKey, uint64_t objectVersion, uint64_t maxVersionToDel);

    /**
     * @brief Version string to number
     * @param[in] key The object key.
     * @param[in] versionStr The string type version number.
     * @param[out] version The version number.
     * @return true for success
     */
    static bool VersionFromString(const std::string &key, const std::string &versionStr, uint64_t &version);

    std::shared_ptr<PersistenceApi> persistenceApi_{ nullptr };
    std::shared_ptr<ObjectMetaStore> objectStore_;

    const int SEND_DELETE_L2CACHE_TIME_MS = 100;  // Time interval between two delete l2cache object.
    WaitPost cvLock_;                           // Wait to send cache update to worker.
    std::unique_ptr<Thread> thread_{ nullptr };
    std::shared_timed_mutex mutex_;  // For deleteIds_ and deleteFailedIds_.
    std::unique_ptr<ThreadPool> threadPool_{ nullptr };
    // The table deleteIds_ is used to store objects ready to be deleted, and another asynchronous thread polls and
    // deletes objects in the table. deleteIds_ is: std::unordered_map<objectKey+"/"+version, version>
    std::map<std::string, DeleteMeta> deleteIds_;
    std::unordered_map<std::string, std::unordered_map<uint64_t, uint64_t>> objDeleteMap_;
    std::atomic<bool> interruptFlag_;

    /**
     * in object persistence to obs scenarios, when datasystem delete the object:
     * we need to make sure the object in the l2cache must be delete, when the given version of the object is not
     * found in the persistence (i.e. in upload process or failed to upload), we need a delay and try again.
     * when the delay time elapse, we try to delete, in this time, if we can't find the given version of the object in
     * l2 storage once again, we consider this version has failed to upload to persistence, we will complete the
     * deletion, erase the key in the map.
     *
     * the map is store the stopwatch to calculate delay time, key is:<objectKey>/<version>
     */
    std::unordered_map<std::string, Timer> waitToCompleteDeleteObjMap_;
    // protect waitToCompleteDeleteObjMap_
    std::mutex waitDelMutex_;

    // support leve2 storage type
    L2StorageType supportL2Storage_;

    const bool backendStoreExist_;
};
}  // namespace master
}  // namespace datasystem
#endif  // MASTER_OC_GLOBAL_CACHE_DELETE_MANAGER_H
