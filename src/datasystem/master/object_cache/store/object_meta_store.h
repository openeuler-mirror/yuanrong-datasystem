/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace master {
// Mark whether the reference counting metadata stored in rocksdb is in or out of the cloud.
static const std::string REMOTE_CLIENT_FLAG = "?remoteClient";

static const std::string LIVENESS_CHECK = "liveness_check";
static const std::string LIVENESS_OK = "ok";

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

    static NotifyWorkerOp Delete(int64_t version)
    {
        NotifyWorkerOp op{ .type = NotifyWorkerOpType::DELETE };
        op.delObjectVersion = version;
        return op;
    }
};

void FillNotifyWorkerOpDetailPb(const NotifyWorkerOp &op, ObjectAsyncOpDetailPb &pb);

class ObjectMetaStore {
public:

    /**
     * @brief Construct ObjectMetaStore.
     */
    explicit ObjectMetaStore(RocksStore *rocksStore, bool isEnable = true);

    ~ObjectMetaStore() = default;

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
     * @return Status of the call.
     */
    Status CreateOrUpdateMeta(const std::string &objectKey, const std::string &serializedStr);

    /**
     * @brief Put a batch of objects into RocksDB.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @return Status of the call.
     */
    Status CreateOrUpdateBatchMeta(std::unordered_map<std::string, std::string> &metaInfos);

    /**
     * @brief Remove object meta from Rocksdb.
     * @param[in] key The object meta key to be removed, in format of objectKey_workerAddr.
     * @return Status of the call.
     */
    Status RemoveMeta(const std::string &key);

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
     * @brief Remove a rollback location after the ordered RocksDB operation completes.
     */
    Status RemoveObjectLocationForRollbackAndWait(const std::string &objectKey, const std::string &workerAddr);

    /**
     * @brief Atomically remove one object location and the object's metadata from RocksDB.
     */
    Status RemoveObjectLocationAndMeta(const std::string &objectKey, const std::string &workerAddr);

    /**
     * @brief Get all pairs from KvStore table
     * @param[in] table the KvStore table name to get all pairs
     * @param[out] outMetas The output metas in table
     * @return Status of the call
     */
    Status GetAllFromRocks(const std::string &table, std::vector<std::pair<std::string, std::string>> &outMetas);

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
     * @return Status of the call.
     */
    Status AddAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey,
                            const NotifyWorkerOp &op, bool waitForCompletion = false);

    /**
     * @brief Remove an operation that requires asynchronous notification to the worker.
     * @param[in] workerAddr Address of worker.
     * @param[in] objectKey The id of object.
     * @return Status of the call.
     */
    Status RemoveAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey);

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
     * @return Status of the call.
     */
    Status AddDeletedObject(const std::string &objectKey, uint64_t version);

    /**
     * @brief Add global cache deleted object to Rocksdb.
     * @param[in] objectKey The id of global cache to delete.
     * @param[in] version The version of the object.
     * @param[in] delVersion The version of global cache to delete.
     * @return Status of the call.
     */
    Status AddDeletedObjectWithDelVersion(const std::string &key, uint64_t version, uint64_t delVersion);

    /**
     * @brief Remove the id of global cache to delete.
     * @param[in] objectKey The id of object.
     * @param[in] version The version of global cache to delete.
     * @return Status of the call.
     */
    Status RemoveDeletedObject(const std::string &objectKey, uint64_t version);

    static std::string EncodeDeletedObjectValue(uint64_t delVersion, const std::string &targetWorkerAddress);

    // Older delete records may contain a worker suffix; it is decoded but no longer used for routing.
    static bool DecodeDeletedObjectValue(const std::string &objectKey, const std::string &encodedValue,
                                         uint64_t &delVersion, std::string &targetWorkerAddress);

    /**
     * @brief Liveness check rocksdb.
     * @return Status of the call.
     */
    Status LivenessCheckRocksdb();

private:

    /**
     * @brief Init rocksdb store.
     * @return Status of the call.
     */
    Status InitRocksStore();

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

    RocksStore *rocksStore_;
    bool isPersistenceEnabled_;
};
}  // namespace master
}  // namespace datasystem
#endif
