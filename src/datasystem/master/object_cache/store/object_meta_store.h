/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Declare interface to store object meta in RocksDB.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_STORE_OBJECTMETA_STORE_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_STORE_OBJECTMETA_STORE_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <securec.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/master/object_cache/store/meta_async_queue.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

namespace datasystem {
enum ObjectMetaStoreEventType : uint32_t {
    GET_HASH_RANGE_NON_BLOCK,
    GET_LOCAL_WORKER_UUID,
};

using GetHashRangeNonBlockEvent = EventSubscribers<GET_HASH_RANGE_NON_BLOCK, std::function<void(worker::HashRange &)>>;

using GetLocalWorkerUuidEvent = EventSubscribers<GET_LOCAL_WORKER_UUID, std::function<void(std::string &)>>;

namespace master {
static const std::string HEALTH_STATUS = "ready";
// Mark whether the reference counting metadata stored in rocksdb is in or out of the cloud.
static const std::string REMOTE_CLIENT_FLAG = "?remoteClient";

std::string Hash2Str(uint32_t hash);

const int QUEUE_CAPACITY = 10000;

// The odd-numbered bits are used to store notifications to the masters, and the even-numbered bits are used to store
// notifications to the workers.
enum class NotifyWorkerOpType : uint32_t {
    // notify worker op
    PRIMARY_COPY_INVALID = 1u,
    CACHE_INVALID = 1u << 2,
    DELETE = 1u << 4,
    // notify master op
    REMOVE_META = 1u << 1,           // Migrate metadata owner
    DELETE_ALL_COPY_META = 1u << 3,  // Delete metadata directly
};
ENABLE_BITMASK_ENUM_OPS(NotifyWorkerOpType);

struct NotifyWorkerOp {
    NotifyWorkerOpType type;
    int64_t removeMetaVersion = 0;
    std::unordered_set<std::string> removeMetaAzNames = {};
    int64_t deleteAllCopyMetaVersion = 0;
    std::unordered_set<std::string> deleteAllCopyMetaAzNames = {};
    int64_t delObjectVersion = 0;
};

class ObjectMetaStore {
public:
    enum WriteType : uint8_t { ROCKS_ONLY = 0, ROCKS_ASYNC_ETCD = 1, ROCKS_SYNC_ETCD = 2 };

    /**
     * @brief Construct ObjectMetaStore.
     */
    ObjectMetaStore(RocksStore *rocksStore, EtcdStore *etcdStore, bool isEnable = true);

    ~ObjectMetaStore();

    /**
     * @brief Used path backStorePath to start rocksdb in rocksStore.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Is persistence enabled or not.
     * @return true If persistence enable.
     */
    bool IsPersistenceEnabled() const
    {
        return isPersistenceEnabled_;
    }

    /**
     * @brief Create the serialized string of object meta.
     * @param[in] objectKey id of the object meta.
     * @param[in] meta The object meta to be serialized.
     * @param[out] serializedStr the serialized string of the object meta.
     * @return Status of call.
     */
    Status CreateSerializedStringForMeta(const std::string &objectKey, ObjectMetaPb &meta, std::string &serializedStr);

    /**
     * @brief Create object meta in Rocksdb.
     * @param[in] objectKey object key parameters.
     * @param[in] serializedStr serializedStr of the object meta.
     * @param[in] type Kv write type.
     * @return Status of the call.
     */
    Status CreateOrUpdateMeta(const std::string &objectKey, const std::string &serializedStr,
                              WriteType type = ROCKS_ONLY);

    /**
     * @brief Put  a batch of objects into rocks and etcd.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @param[in] type write type for object.
     * @return Status of the call.
     */
    Status CreateOrUpdateBatchMeta(std::unordered_map<std::string, std::string> &metaInfos, WriteType type);

    /**
     * @brief Put  a batch of objects into rocks and etcd.
     * @param[in] table the KvStore table name to get all pairs
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @param[in] type write type for object.
     * @param[in] needSaveToEtcd if need to save in etcd.
     * @return Status of the call.
     */
    Status BatchPutToEtcdStore(const std::string &tablePrefix, std::unordered_map<std::string, std::string> &metaInfos,
                               WriteType type, bool needSaveToEtcd);

    /**
     * @brief Remove object meta from Rocksdb.
     * @param[in] key The object meta key to be removed, in format of objectKey_workerAddr.
     * @param[in] needRemoveEtcdData Indicates whether to delete etcd data.
     * @return Status of the call.
     */
    Status RemoveMeta(const std::string &key, bool needRemoveEtcdData = true);

    /**
     * @brief Add object location to rocksdb.
     * @param[in] objectKey Object key to be added.
     * @param[in] workerAddr Location of the object.
     * @param[in] ackPersistenceVal Persistence value to be added.
     * @return Status of the call.
     */
    Status AddObjectLocation(const std::string &objectKey, const std::string &workerAddr,
                             const std::string &ackPersistenceVal = "");

    /**
     * @brief Add object location to rocksdb.
     * @param[in] keyLocations The key and location address pair of the object to be added.
     * @param[in] ackState Ack state, all keyLocations use the same ack state.
     * @return Status of the call.
     */
    Status AddObjectLocations(const std::unordered_map<std::string, std::string> &keyLocations,
                              const std::string &ackPersistenceVal);

    /**
     * @brief Remove object location from rocksdb.
     * @param[in] objectKey Object key to be removed.
     * @param[in] workerAddr Location of the object.
     * @return Status of the call.
     */
    Status RemoveObjectLocation(const std::string &objectKey, const std::string &workerAddr);

    /**
     * @brief Get all pairs from KvStore table
     * @param[in] table the KvStore table name to get all pairs
     * @param[out] outMetas The output metas in table
     * @return Status of the call
     */
    Status GetAllFromRocks(const std::string &table, std::vector<std::pair<std::string, std::string>> &outMetas);

    /**
     * @brief Get pairs from ETCD table and write to rocksdb table.
     * @param[in] tablePrefix ETCD table prefix.
     * @param[in] rocksTable Need write rocksdb table.
     * @param[in] workerUuids Obtains the data of specified worker uuids. If the value is empty, obtains the data of the
     * current worker.
     * @param[in] extraRanges Obtains the data of specified hash ranges if not empty.
     * @param[out] outMetas KV paris.
     * @return Status of the call
     */
    Status GetFromEtcd(const std::string &tablePrefix, const std::string &rocksTable,
                       const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges,
                       std::vector<std::pair<std::string, std::string>> &outMetas);

    /**
     * @brief Add nested object relationship to nested object table.
     * @param[in] parentObjKey The parent object key in nested relationship.
     * @param[in] childObjKey The child object key depend on parentObjKey.
     * @return Status of the call.
     */
    Status AddNestedRelationship(const std::string &parentObjKey, const std::string &childObjKey);

    /**
     * @brief Remove nested relationship from nested object table.
     * @param[in] parentObjKey The parent object key in nested relationship.
     * @param[in] childObjKey The child object key depend on parentObjKey.
     * @return Status of the call.
     */
    Status RemoveNestedRelationship(const std::string &parentObjKey, const std::string &childObjKey);

    /**
     * @brief Update count of objects that depends on objKey.
     * @param[in] objKey The parent object key in nested relationship.
     * @param[in] count Number of objects that depend on parent object key.
     * @return Status of the call.
     */
    Status UpdateNestedRefCount(const std::string &objKey, uint32_t count);

    /**
     * @brief Remove count objects.
     * @param[in] objKey The parent object key in nested relationship.
     * @return Status of the call.
     */
    Status RemoveNestedRefCount(const std::string &objKey);

    /**
     * @brief Add an operation that requires asynchronous notification to the worker.
     * @param[in] workerAddr Address of worker.
     * @param[in] objectKey The id of object.
     * @param[in] op To be add operation.
     * @param[in] type Kv write type.
     * @return Status of the call.
     */
    Status AddAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey, const NotifyWorkerOp &op,
                            WriteType type = ROCKS_ONLY);

    /**
     * @brief Remove an operation that requires asynchronous notification to the worker.
     * @param[in] workerAddr Address of worker.
     * @param[in] objectKey The id of object.
     * @param[in] needRemoveEtcdData Indicates whether to delete etcd data.
     * @return Status of the call.
     */
    Status RemoveAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey,
                               bool needRemoveEtcdData = true);

    /**
     * @brief Remove op from Rocksdb by worker address.
     * @param[in] workerAddr Address of worker.
     * @return Status of the call.
     */
    Status RemoveAsyncWorkerOpByWorker(const std::string &workerAddr);

    /**
     * @brief Add remoteClientId obj reference to Rocksdb.
     * @param[in] remoteClientId remote clientId.
     * @param[in] masterAddr The master address.
     * @return Status of the call.
     */
    Status AddRemoteClientRef(const std::string &remoteClientId, const std::string &masterAddr);

    /**
     * @brief Remove remoteClientId obj reference to Rocksdb.
     * @param[in] remoteClientId remote clientId.
     * @param[in] masterAddr The master address.
     * @return Status of the call.
     */
    Status RemoveRemoteClientRef(const std::string &remoteClientId, const std::string &masterAddr);

    /**
     * @brief Add global reference to Rocksdb.
     * @param[in] key It is the worker address in the in-cloud reference case, and it is the remoteClientId in the
     * in the out-cloud reference case.
     * @param[in] objectKey The id of object ref.
     * @return Status of the call.
     */
    Status AddGlobalRef(const std::string &key, const std::string &objectKey, bool isRemoteClient);

    /**
     * @brief Remove global reference from Rocksdb.
     * @param[in] key It is the worker address in the in-cloud reference case, and it is the remoteClientId in the
     * in the out-cloud reference case.
     * @param[in] objectKey The id of object ref.
     * @return Status of the call.
     */
    Status RemoveGlobalRef(const std::string &key, const std::string &objectKey, bool isRemoteClient);

    /**
     * @brief Add global cache deleted object to Rocksdb.
     * @param[in] objectKey The id of global cache to delete.
     * @param[in] version The version of global cache to delete.
     * @param[in] type Kv write type.
     * @return Status of the call.
     */
    Status AddDeletedObject(const std::string &objectKey, uint64_t version, WriteType type = ROCKS_ONLY);

    /**
     * @brief Add global cache deleted object to Rocksdb.
     * @param[in] objectKey The id of global cache to delete.
     * @param[in] version The version of the object.
     * @param[in] delVersion The version of global cache to delete.
     * @param[in] type Kv write type.
     * @return Status of the call.
     */
    Status AddDeletedObjectWithDelVersion(const std::string &key, uint64_t version, uint64_t delVersion,
                                          const std::string &targetWorkerAddress = "", WriteType type = ROCKS_ONLY);

    /**
     * @brief Remove the id of global cache to delete.
     * @param[in] objectKey The id of object.
     * @param[in] version The version of global cache to delete.
     * @return Status of the call.
     */
    Status RemoveDeletedObject(const std::string &objectKey, uint64_t version);

    static std::string EncodeDeletedObjectValue(uint64_t delVersion, const std::string &targetWorkerAddress);

    static bool DecodeDeletedObjectValue(const std::string &objectKey, const std::string &encodedValue,
                                         uint64_t &delVersion, std::string &targetWorkerAddress);

    /**
     * @brief Get hash and table name.
     * @param[in] objKey Object key.
     * @param[in] tablePrefix table prefix name.
     * @param[out] hash The hash value of the key.
     * @param[out] table Table name.
     */
    void GetHashAndTable(const std::string &objKey, const std::string &tablePrefix, uint32_t &hash, std::string &table);

    /**
     * @brief Insert the id to etcdKeyMap.
     * @param[in] table etcd table name.
     * @param[in] key object key.
     * @param[in] hash hash value.
     * @param[in] isAsync is Async or not.
     */
    void InsertToEtcdKeyMap(const std::string &table, const std::string &key, uint32_t hash, bool isAsync);

    /**
     * @brief For test purpose only.
     * @return True is queue is empty.
     */
    bool AsyncQueueEmpty();

    /**
     * @brief Obtains the usage of the queue for asynchronously writing ETCD data.
     * @note currentSize: the number of tasks in the current queue.
     *       totalLimit:  the maximum queue capacity
     * @return The Usage: "currentSize/totalLimit/workerL2CacheQueueUsag"
     */
    std::string GetETCDAsyncQueueUsage();

    /**
     * @brief Check if rocksdb is trustworthy or not.
     * @return True if rocksdb is trustworthy.
     */
    bool IsRocksdbRunning()
    {
        return isRocksdbRunning_;
    }

    /**
     * @brief Check whether to support metadata written to rocksdb.
     * @return True if support metadata written to rocksdb.
     */
    bool IsRocksdbEnableWriteMeta();

    /**
     * @brief Change rocksdb to running.
     */
    void ChangeRocksdbToRunning()
    {
        isRocksdbRunning_ = true;
    }

    /**
     * @brief Put a new key-value into a table in rocks store.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @return Status of the call.
     */
    Status PutToRocksStore(const std::string &tableName, const std::string &key, const std::string &value)
    {
        return rocksStore_->Put(tableName, key, value);
    }

    /**
     * @brief Add rocksdb health tag.
     * @return Status of the call.
     */
    Status AddRocksdbHealthTag();

    /**
     * @brief Rocksdb health check.
     * @return true Rocksdb health check success.
     */
    bool CheckHealth();

    /**
     * @brief Get metas match function.
     * @param[in] matchFunc Match function.
     * @param[out] objAsyncMap Object async element map.
     */
    void GetMetasMatch(std::function<bool(const std::string &)> &&matchFunc,
                       std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &objAsyncMap);

    /**
     * @brief Poll async elements by object key.
     * @param[in] objectKey Object key.
     * @param[out] elements Async elements.
     */
    void PollAsyncElementsByObjectKey(const std::string &objectKey,
                                      std::unordered_set<std::shared_ptr<AsyncElement>> &elements);

    /**
     * @brief Insert wait async elements to object meta store.
     * @param[in] objectKey Object key.
     * @param[in] table Table name.
     * @param[in] key Etcd key.
     * @param[in] value Etcd value.
     * @param[in] reqType Request type.
     * @param[in] timestamp Begin timestamp.
     * @param[in] traceId Trace ID.
     */
    void InsertWaitAsyncElements(const std::string &objectKey, const std::string &table, const std::string &key,
                                 const std::string &value, AsyncElement::ReqType reqType, uint64_t timestamp,
                                 const std::string &traceId);

private:
    /**
     * @brief Check if etcd enable.
     * @return Status of the call.
     */
    bool EtcdEnable() const
    {
        return (etcdStore_ != nullptr) && enableEtcdMetaStore_;
    }

    /**
     * @brief Init Etcd store.
     * @return Status of the call.
     */
    Status InitEtcdStore();

    /**
     * @brief Init rocksdb store.
     * @return Status of the call.
     */
    Status InitRocksStore();

    /**
     * @brief Print warn log if need.
     */
    void WarnIfNeed();

    /**
     * @brief Hash function.
     * @param[in] key Object key.
     * @return hash code and is special key or not.
     */
    std::pair<uint32_t, bool> HashFunction(const std::string &key);

    /**
     * @brief Add KV to etcd store.
     * @param[in] tablePrefix ETCD table prefix.
     * @param[in] key Key need to store.
     * @param[in] value Value need to store
     * @param[in] type Kv write type.
     * @param[in] needSaveToEtcd Default is true. If false, only write to memory, not write through etcd.
     * @return Status of the call.
     */
    Status PutToEtcdStore(const std::string &tablePrefix, const std::string &objKey, const std::string &key,
                          const std::string &value, WriteType type);

    /**
     * @brief Remove key from etcd store.
     * @param[in] objectKey Object key.
     * @param[in] key Key need to store.
     * @param[in] tablePrefix ETCD table prefix.
     * @param[in] postHandler To ensure consistency, some things can only be done after successfully deleting the key in
     * etcd.
     * @return Status of the call.
     */
    Status RemoveEtcdKey(const std::string &objectKey, const std::string &key, const std::string &tablePrefix,
                         std::function<Status()> &&postHandler = nullptr);

    /**
     * @brief Prefix search key and erase them from etcdKeyMap_
     * @param[in] table ETCD table.
     * @param[in] prefixKey Preifx search key.
     * @param[out] keys Keys that match the prefix
     */
    void PrefixSearchAndErase(const std::string &table, const std::string &prefixKey,
                              std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> &keys);

    /**
     * @brief Prefix remove keys impl.
     * @param[in] table ETCD table.
     * @param[in] keys Keys that match the prefix.
     * @param[out] failedKeys Failed keys that need to rollback.
     * @return Status of the call.
     */
    Status PrefixRemoveEtcdKeyImpl(const std::string &table,
                                   const std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> &keys,
                                   std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> &failedKeys);

    /**
     * @brief Remove some Keys by Prefix from etcd store in KvStore.
     * @param[in] prefixKey Prefix key pattern.
     * @param[in] tablePrefix ETCD table prefix.
     * @return Status of the call.
     */
    Status PrefixRemoveEtcdKey(const std::string &prefixKey, const std::string &tablePrefix);

    /**
     * @brief Remove key from rocks table in KvStore.
     * @param[in] key The key to remove.
     * @param[in] table The KvStore table name.
     * @return Status of the call.
     */
    Status RemoveRocksKey(const std::string &key, const std::string &table);

    /**
     * @brief Remove some Keys by Prefix from rocksdb.
     * @param[in] prefixKey The keys with the specific prefix that should be remove.
     * @param[in] table The KvStore table name.
     * @return Status of the call.
     */
    Status PrefixRemoveRocksKey(const std::string &prefixKey, const std::string &table);

    /**
     * @brief Check if etcd async addable.
     * @return Status of the call.
     */
    Status EtcdAsyncAddable() const;

    /**
     * @brief Get pairs from ETCD table and write to rocksdb table.
     * @param[in] tablePrefix ETCD table prefix.
     * @param[in] rocksTable Need write rocksdb table.
     * @param[in] suffix ETCD table suffix.
     * @param[in] range The range to be obtained.
     * @param[out] metas KV paris.
     * @return Status of the call
     */
    Status GetRangeFromEtcd(const std::string &tablePrefix, const std::string &rocksTable, const std::string &suffix,
                            const std::pair<uint32_t, uint32_t> &range,
                            std::vector<std::pair<std::string, std::string>> &metas);

    /**
     * @brief Sender thread responsible for sending meta to Etcd asynchronously.
     * @param[in] threadNum The thread ID.
     * @param[in] queue Queue corresponding to sender, it save asynchronous task.
     */
    void AsyncMetaOpToEtcdStorageHandler(int threadNum, const std::shared_ptr<MetaAsyncQueue> &queue);

    /**
     * @brief Add one async task to etcd store.
     * @param[in] objectKey Object key.
     * @param[in] table ETCD table.
     * @param[in] etcdKey Key need to remove.
     * @param[in] value Value need to store
     * @param[in] requestType The request's type, see the AsyncEtcdOpElement::RequestType for details.
     * @param[in] postHandler To ensure consistency, some things can only be done after successfully deleting the key in
     * etcd.
     * @return Status of the call
     */
    Status AddOneAsyncTaskToEtcdStore(const std::string &objectKey, const std::string &table,
                                      const std::string &etcdKey, const std::string &value,
                                      AsyncElement::ReqType requestType, uint64_t timestamp = 0,
                                      const std::string &traceId = "", std::function<Status()> &&postHandler = nullptr);

    // The backend rocksdb storage.
    RocksStore *rocksStore_;

    // In scenarios where etcd is more reliable than rocksdb, we need this parameter,
    // the meaning of the parameter is as follows:
    // If etcd is used to store metadata, in some scenarios where rocksdb may not be trusted,
    // we can set this variable to false, and the metadata will be restored from etcd.
    std::atomic<bool> isRocksdbRunning_{ true };

    // The backend etcd storage.
    EtcdStore *etcdStore_;

    // Async request max size.
    const uint64_t maxRequestSize_;

    // Current async request size.
    std::atomic<uint64_t> asyncReqSize_{ 0 };

    // Async meta op to ETCD storage
    std::vector<std::shared_ptr<MetaAsyncQueue>> queues_;
    std::vector<Thread> threadPool_;

    // Is persistence enabled or not.
    bool isPersistenceEnabled_;

    // Protects 'etcdKeyMap_'
    std::shared_timed_mutex etcdMtx_;

    // Save etcd key put type, <table - <key - <hash, async>>>/
    std::unordered_map<std::string, std::map<std::string, std::pair<uint32_t, bool>>> etcdKeyMap_;

    // Indicate the object meta store is running or not.
    std::atomic<bool> running_{ true };

    // Timer for warn log.
    Timer timer_;

    // Should data read and write from the L2CacheDaemon depend on metadata.
    bool enableEtcdMetaStore_{ true };
};
}  // namespace master
}  // namespace datasystem
#endif
