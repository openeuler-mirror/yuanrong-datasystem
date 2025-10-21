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
#include "datasystem/master/object_cache/oc_global_cache_delete_manager.h"

#include <future>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/strings_util.h"

DS_DECLARE_string(obs_endpoint);
DS_DECLARE_string(sfs_path);
DS_DECLARE_bool(oc_io_from_l2cache_need_metadata);
/**
 * the purpose is:
 * take in consider that, when we delete the object which is in uploading process, in this scenarios we need
 * delay some time to retry.
 *
 * the perfect delay time is a little larger than object upload time, but it is hard to calculate that time, so we
 * set to a hour
 */
DS_DEFINE_int32(
    object_del_retry_delay_sec, 3600,
    "when we delete the object which is in uploading process, in this scenarios we need delay some time to retry.");
DS_DEFINE_uint32(l2_cache_delete_thread_num, 32, "Global cache delete thread number.");
DS_DEFINE_validator(l2_cache_delete_thread_num, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});

namespace datasystem {
namespace master {
OCGlobalCacheDeleteManager::OCGlobalCacheDeleteManager(std::shared_ptr<ObjectMetaStore> objectStore,
                                                       std::shared_ptr<PersistenceApi> persistApi,
                                                       bool backendStoreExist)
    : persistenceApi_(persistApi),
      objectStore_(std::move(objectStore)),
      interruptFlag_(false),
      backendStoreExist_(backendStoreExist)
{
}

OCGlobalCacheDeleteManager::~OCGlobalCacheDeleteManager()
{
    if (!interruptFlag_) {
        Shutdown();
    }
    LOG(INFO) << "Remaining object count:" << deleteIds_.size();
}

Status OCGlobalCacheDeleteManager::Init()
{
    RETURN_IF_NOT_OK_APPEND_MSG(RecoverDeletedIds(backendStoreExist_),
                                "Recover global cache delete ids from rocksdb failed");
    supportL2Storage_ = GetCurrentStorageType();
    if (!IsSupportL2Storage(supportL2Storage_)) {
        LOG(INFO) << "Start without l2cache, l2 cache address is empty.";
        return Status::OK();
    }
    RETURN_IF_EXCEPTION_OCCURS(
        threadPool_ = std::make_unique<ThreadPool>(0, FLAGS_l2_cache_delete_thread_num, "OcGlobalCacheDelete"));
    thread_ = std::make_unique<Thread>(&OCGlobalCacheDeleteManager::ProcessDeleteObjects, this);
    thread_->set_name("OcScanDeleteObject");
    return Status::OK();
}

void OCGlobalCacheDeleteManager::Shutdown()
{
    if (!thread_) {
        return;
    }
    interruptFlag_ = true;
    cvLock_.Set();
    thread_->join();
}

void OCGlobalCacheDeleteManager::ProcessDeleteObjects()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "Starting delete global cache object thread.";
    const size_t countLimit = 64;
    while (!interruptFlag_) {
        INJECT_POINT("master.ProcessDeleteObjects", [] {});
        std::map<std::string, DeleteMeta> allObjectKeys;
        GetDeletedObjects(allObjectKeys);
        // Process in current thread if object count less than countLimit.
        if (allObjectKeys.size() < countLimit) {
            DeleteObjects(allObjectKeys);
            cvLock_.WaitFor(SEND_DELETE_L2CACHE_TIME_MS);
            continue;
        }

        std::vector<std::map<std::string, DeleteMeta>> parts(FLAGS_l2_cache_delete_thread_num);
        size_t index = 0;
        for (auto &kv : allObjectKeys) {
            parts[index % FLAGS_l2_cache_delete_thread_num].insert(std::move(kv));
            index += 1;
        }

        std::vector<std::future<void>> futures;
        for (const auto &objectKeys : parts) {
            if (!objectKeys.empty()) {
                futures.emplace_back(threadPool_->Submit([this, &objectKeys] { DeleteObjects(objectKeys); }));
            }
        }

        for (auto &fut : futures) {
            fut.get();
        }
        cvLock_.WaitFor(SEND_DELETE_L2CACHE_TIME_MS);
    }
    LOG(INFO) << "Terminating global cache delete thread.";
}

Status OCGlobalCacheDeleteManager::InsertDeletedObject(const std::string &objectKey, uint64_t objectVersion,
                                                       uint64_t delVersion, bool needPersist,
                                                       ObjectMetaStore::WriteType type)
{
    if (needPersist) {
        VLOG(2) << "Insert global cache delete to etcd, obejct: " << objectKey
                << ", type: " << static_cast<uint32_t>(type);
        RETURN_IF_NOT_OK(objectStore_->AddDeletedObjectWithDelVersion(objectKey, objectVersion, delVersion, type));
    }
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    // The objectKey of the inserted deleteIds_ map is combined with the version to prevent objects of the old version
    // from being deleted asynchronously and objects of the new version from being inserted into the deletion list.
    // As a result, the cloudstrorage data remains.
    std::string key = objectKey + "/" + std::to_string(objectVersion);
    (void)deleteIds_.emplace(std::move(key), DeleteMeta{ .version = delVersion, .failed = false });
    (void)objDeleteMap_[objectKey].emplace(std::make_pair(objectVersion, delVersion));
    return Status::OK();
}

void OCGlobalCacheDeleteManager::DeleteObjects(const std::map<std::string, DeleteMeta> &objectKeys)
{
    INJECT_POINT("global_cache_delete.delete_objects", []() { return; });
    Timer timer;
    std::unordered_map<std::string, uint64_t> successIds;
    const size_t batchCount = 64;
    size_t count = 0;
    for (const auto &info : objectKeys) {
        if (interruptFlag_) {
            break;
        }
        const std::string &key = info.first;
        const auto &meta = info.second;
        auto pos = key.find_last_of('/');
        if (pos == std::string::npos) {
            LOG(WARNING) << "invalid key:" << key;
            continue;
        }
        std::string objectKey = key.substr(0, pos);
        std::string objectVersionStr = key.substr(pos + 1);
        uint64_t objectVersion = 0;
        uint64_t delVersion = meta.version;
        if (!VersionFromString(key, objectVersionStr, objectVersion)) {
            continue;
        }
        Status status;
        if (IsSupportL2Storage(supportL2Storage_)) {
            status = DelPersistenceObj(objectKey, objectVersion, delVersion);
        }
        if (status.IsOk() || status.GetCode() == K_NOT_FOUND) {
            LOG(INFO) << "del object success. objectKey:" << key << ", version:" << delVersion;
            (void)successIds.emplace(objectKey, objectVersion);
        } else {
            if (!meta.failed) {
                LOG(WARNING) << "del object failed. objectKey:" << key << ", version:" << delVersion
                             << ", status:" << status.ToString();
                std::lock_guard<std::shared_timed_mutex> lck(mutex_);
                auto iter = deleteIds_.find(key);
                if (iter != deleteIds_.end()) {
                    iter->second.failed = true;
                }
            }
        }
        count += 1;
        if (count >= batchCount) {
            (void)RemoveDeletedObject(successIds);
            count = 0;
            successIds.clear();
        }
    }
    if (!successIds.empty()) {
        (void)RemoveDeletedObject(successIds);
    }

    const size_t logTimeLimit = 60;  // Log the object count if the delete operation takes more than 60s.
    LOG_IF(INFO, timer.ElapsedSecond() > logTimeLimit)
        << "It takes " << timer.ElapsedSecond() << "s to delete " << objectKeys.size() << " objects from global cache.";
}

Status OCGlobalCacheDeleteManager::RemoveDeletedObject(const std::unordered_map<std::string, uint64_t> &objectKeys)
{
    for (const auto &info : objectKeys) {
        RETURN_IF_NOT_OK(objectStore_->RemoveDeletedObject(info.first, info.second));
    }
    // If delete operation was successfully executed, remove the key from deleteFailedIds_.
    std::lock_guard<std::shared_timed_mutex> lck(mutex_);
    for (const auto &info : objectKeys) {
        std::string key = info.first + "/" + std::to_string(info.second);
        (void)deleteIds_.erase(key);
        auto it = objDeleteMap_.find(info.first);
        if (it != objDeleteMap_.end()) {
            (void)it->second.erase(info.second);
            if (it->second.empty()) {
                (void)objDeleteMap_.erase(it);
            }
        }
    }
    return Status::OK();
}

Status OCGlobalCacheDeleteManager::DelPersistenceObj(const std::string &objectKey, uint64_t objectVersion,
                                                     uint64_t maxVersionToDel)
{
    INJECT_POINT("worker.DelPersistenceObj.beforeDel");
    std::string key = objectKey + "/" + std::to_string(objectVersion);
    std::unique_lock<std::mutex> locker(waitDelMutex_);
    auto search = waitToCompleteDeleteObjMap_.find(key);
    Status status;
    int delayTimeSec = FLAGS_object_del_retry_delay_sec;
    INJECT_POINT("worker.DelPersistence.delay", [&delayTimeSec](int timeSec) {
        delayTimeSec = timeSec;
        return Status::OK();
    });
    bool listIncompleteVersions = false;
    if (search != waitToCompleteDeleteObjMap_.end()) {
        listIncompleteVersions = true;
        if (search->second.ElapsedSecond() < delayTimeSec) {
            // the delay time not elapse, not process it
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The version of the object is waiting a delay to try delete again.");
        }
    }
    // unlock when sending delete request to persistence api.
    locker.unlock();

    PerfPoint point(PerfKey::MASTER_L2_CACHE_DEL);
    status = persistenceApi_->Del(objectKey, maxVersionToDel, true, 0, &objectVersion, listIncompleteVersions);
    point.Record();
    RETURN_OK_IF_TRUE(status.IsOk());

    if (status.GetCode() == StatusCode::K_NOT_FOUND) {
        if (objectVersion == UINT64_MAX) {
            LOG(INFO) << "The max version not found for object " << objectKey << ", no need delay the deletion task.";
            return Status::OK();
        }
        locker.lock();
        search = waitToCompleteDeleteObjMap_.find(key);
        if (search != waitToCompleteDeleteObjMap_.end()) {
            LOG(INFO) << FormatString(
                "after the delay %s second, we can't find an object %s with version %s in persistence api, we consider "
                "the version is not exist in persistence api any more, so not need to delete it, and this deletion "
                "task is success complete.",
                search->second.ElapsedSecond(), objectKey, maxVersionToDel);
            waitToCompleteDeleteObjMap_.erase(key);
            return Status::OK();
        } else {
            Timer timer;
            waitToCompleteDeleteObjMap_.emplace(key, timer);
            RETURN_STATUS(StatusCode::K_TRY_AGAIN, "The version of the object is waiting a delay to try delete again.");
        }
    }
    return status;
}

void OCGlobalCacheDeleteManager::GetDeletedObjects(std::map<std::string, DeleteMeta> &deleteIds)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    deleteIds = deleteIds_;
}

Status OCGlobalCacheDeleteManager::RecoverDeletedIds(bool isFromRocksdb, const std::vector<std::string> &workerUuids,
                                                     const worker::HashRange &extraRanges)
{
    std::vector<std::pair<std::string, std::string>> deleteObjects;
    if (!objectStore_->IsRocksdbRunning()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetFromEtcd(ETCD_GLOBAL_CACHE_TABLE_PREFIX, GLOBAL_CACHE_TABLE,
                                                                   workerUuids, extraRanges, deleteObjects),
                                         "Load global cache delete objects from etcd failed.");
        for (const auto &iter : deleteObjects) {
            RETURN_IF_NOT_OK(objectStore_->PutToRocksStore(GLOBAL_CACHE_TABLE, iter.first, iter.second));
        }
    } else {
        if (isFromRocksdb) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetAllFromRocks(GLOBAL_CACHE_TABLE, deleteObjects),
                                             "Load global cache delete objects from rocksdb failed.");
        } else {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                objectStore_->GetFromEtcd(ETCD_GLOBAL_CACHE_TABLE_PREFIX, GLOBAL_CACHE_TABLE, workerUuids, extraRanges,
                                          deleteObjects),
                "Load global cache delete objects from etcd failed.");
        }
    }

    LOG(INFO) << "Recover delete object count:" << deleteObjects.size();
    for (const auto &info : deleteObjects) {
        const std::string &key = info.first;
        // The key structure in tables GLOBAL_CACHE_TABLE and ETCD_GLOBAL_CACHE_TABLE_PREFIX is:
        // key = objectKey+"/"+version.
        auto pos = key.find_last_of('/');
        if (pos == std::string::npos) {
            LOG(WARNING) << "invalid key:" << key;
            continue;
        }
        std::string objectKey = key.substr(0, pos);
        std::string objectVersionStr = key.substr(pos + 1);
        uint64_t objectVersion = 0;
        if (!VersionFromString(key, objectVersionStr, objectVersion)) {
            continue;
        }
        uint64_t delVersion = 0;
        if (!VersionFromString(key, info.second, delVersion)) {
            continue;
        }
        Status status = InsertDeletedObject(objectKey, objectVersion, delVersion, false);
        uint32_t hash;
        std::string table;
        objectStore_->GetHashAndTable(objectKey, ETCD_GLOBAL_CACHE_TABLE_PREFIX, hash, table);
        objectStore_->InsertToEtcdKeyMap(table, key, hash, true);
        if (status.IsError()) {
            LOG(WARNING) << "Load global cache delete objects failed.";
        }
    }
    return Status::OK();
}

bool OCGlobalCacheDeleteManager::VersionFromString(const std::string &key, const std::string &versionStr,
                                                   uint64_t &version)
{
    try {
        version = StrToUnsignedLongLong(versionStr);
    } catch (std::invalid_argument &e) {
        LOG(WARNING) << "Cannot convert version to uint64_t. objectKey:" << key << ", version str:" << versionStr;
        return false;
    } catch (const std::out_of_range &e) {
        LOG(WARNING) << "The string is out of range. objectKey:" << key << ", version str:" << versionStr;
        return false;
    }
    return true;
}

void OCGlobalCacheDeleteManager::GetDeletingVersions(const std::list<std::string> &objectKeys,
                                                     std::vector<uint64_t> &deletingVersions)
{
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    for (const auto &id : objectKeys) {
        std::string prefix = id + "/";
        auto it = deleteIds_.lower_bound(prefix);
        uint64_t maxVersion = 0;
        while (it != deleteIds_.end() && it->first.find(prefix) == 0) {
            if (it->second.version > maxVersion) {
                maxVersion = it->second.version;
            }
            LOG(INFO) << "object " << id << " exsits pending task to delete global cache, version "
                      << it->second.version;
            ++it;
        }
        deletingVersions.emplace_back(maxVersion);
    }
}

std::vector<std::pair<uint64_t, uint64_t>> OCGlobalCacheDeleteManager::GetDeletedInfos(const std::string &objectKey)
{
    std::vector<std::pair<uint64_t, uint64_t>> ret;
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    auto it = objDeleteMap_.find(objectKey);
    if (it == objDeleteMap_.end()) {
        return ret;
    }
    for (const auto &entry : it->second) {
        ret.emplace_back(std::make_pair(entry.first, entry.second));
    }
    return ret;
}

GlobalDeleteInfoMap OCGlobalCacheDeleteManager::GetDeletedInfosMatch(
    const std::function<bool(const std::string &)> &matchFunc)
{
    GlobalDeleteInfoMap ret;
    std::shared_lock<std::shared_timed_mutex> lck(mutex_);
    for (const auto &entry : objDeleteMap_) {
        if (!matchFunc(entry.first)) {
            continue;
        }
        (void)ret.emplace(entry.first, entry.second);
    }
    return ret;
}

void OCGlobalCacheDeleteManager::InsertDeletedObjectFromMigrateNode(const MetaForMigrationPb &objMeta)
{
    for (const auto &delInfo : objMeta.global_cache_dels()) {
        const auto &objectKey = objMeta.object_key();
        auto objectVersion = delInfo.object_version();
        auto delVersion = delInfo.delete_version();
        VLOG(1) << FormatString("Save one global cache delete, object key: %s, object version: %d, delete version: %d",
                                objectKey, objectVersion, delVersion);
        (void)InsertDeletedObject(objMeta.object_key(), objectVersion, delVersion, false);
        uint32_t hash;
        std::string table;
        objectStore_->GetHashAndTable(objMeta.object_key(), ETCD_GLOBAL_CACHE_TABLE_PREFIX, hash, table);
        objectStore_->InsertToEtcdKeyMap(table, objectKey + "/" + std::to_string(objectVersion), hash, true);
    }
}

}  // namespace master
}  // namespace datasystem
