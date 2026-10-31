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
 * Description: Define interface to store object meta in RocksDB.
 */
#include "datasystem/master/object_cache/store/object_meta_store.h"

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/locks.h"


namespace datasystem {
namespace master {

void FillNotifyWorkerOpDetailPb(const NotifyWorkerOp &op, ObjectAsyncOpDetailPb &pb)
{
    pb.set_op_type(static_cast<uint32_t>(op.type));
    pb.set_remove_meta_version(op.removeMetaVersion);
    *pb.mutable_remove_meta_az_names() = { op.removeMetaAzNames.begin(), op.removeMetaAzNames.end() };
    pb.set_delete_all_copy_version(op.deleteAllCopyMetaVersion);
    *pb.mutable_delete_all_copy_az_names() = { op.deleteAllCopyMetaAzNames.begin(), op.deleteAllCopyMetaAzNames.end() };
    pb.set_delete_object_version(op.delObjectVersion);
}

ObjectMetaStore::ObjectMetaStore(RocksStore *rocksStore, bool isEnable)
    : rocksStore_(rocksStore), isPersistenceEnabled_(isEnable)
{
}

Status ObjectMetaStore::Init()
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    return InitRocksStore();
}

Status ObjectMetaStore::InitRocksStore()
{
    RETURN_RUNTIME_ERROR_IF_NULL(rocksStore_);
    return Replica::CreateOcTable(rocksStore_);
}

Status ObjectMetaStore::LivenessCheckRocksdb()
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    auto rc = rocksStore_->Put(HEALTH_TABLE, LIVENESS_CHECK, LIVENESS_OK);
    if (rc.IsError()) {
        RETURN_STATUS(StatusCode::K_KVSTORE_ERROR, "Liveness check rocksdb failed");
    }
    return Status::OK();
}

Status ObjectMetaStore::CreateSerializedStringForMeta(const std::string &objectKey, ObjectMetaPb &meta,
                                                      std::string &serializedStr)
{
    meta.set_object_key(objectKey);
    auto serialized = meta.SerializeToString(&serializedStr);
    // Object key is the key in a key/value pair for the metadata table.
    // Storing the same object key in the "value" part of the kv is redundant and
    // deprecated. Save memory and resources by removing this from the value.
    // The field itself cannot be removed due to down-level support since this ObjectMeta pb
    // is stored on disk (rocksdb). In future it could be fully removed since its not used
    // anymore.
    meta.set_allocated_object_key(NULL);
    if (!serialized) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("Failed to Serialize object[%s] meta", objectKey));
    }
    return Status::OK();
}

Status ObjectMetaStore::CreateOrUpdateMeta(const std::string &objectKey, const std::string &serializedStr)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_CREATE_META);
    return rocksStore_->Put(META_TABLE, objectKey, serializedStr);
}

Status ObjectMetaStore::CreateOrUpdateBatchMeta(std::unordered_map<std::string, std::string> &metaInfos)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_CREATE_META);
    return rocksStore_->BatchPut(META_TABLE, metaInfos);
}

Status ObjectMetaStore::RemoveMeta(const std::string &key)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_CREATE_META);
    return RemoveRocksKey(key, META_TABLE);
}

Status ObjectMetaStore::AddObjectLocation(const std::string &objectKey, const std::string &workerAddr,
                                          const std::string &ackPersistenceVal)
{
    INJECT_POINT("ObjectMetaStore.AddObjectLocation");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_ADD_OBJ_LOCATION);
    std::string key = workerAddr + "_" + objectKey;
    // for compatibility: empty string "" stands for ACK, "0" stands for UNACK
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->PutWithOrderingKey(LOCATION_TABLE, key, ackPersistenceVal, objectKey),
                                     FormatString("Failed to add global ref to rocksdb: %s", key));
    return Status::OK();
}

Status ObjectMetaStore::AddObjectLocations(const std::unordered_map<std::string, std::string> &keyLocations,
                                           const std::string &ackPersistenceVal)
{
    RETURN_OK_IF_TRUE(keyLocations.empty());
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_ADD_OBJ_LOCATIONS);
    // for compatibility: empty string "" stands for ACK, "0" stands for UNACK
    std::unordered_map<std::string, std::string> locationInfos;
    for (const auto &keyLocation : keyLocations) {
        std::string key = keyLocation.second + "_" + keyLocation.first;  // workerAddr + "_" + objectKey
        locationInfos[key] = ackPersistenceVal;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->BatchPut(LOCATION_TABLE, locationInfos),
        FormatString("Failed to batch add global ref to rocksdb: key is %s", keyLocations.begin()->first));
    return Status::OK();
}

Status ObjectMetaStore::RemoveObjectLocation(const std::string &objectKey, const std::string &workerAddr)
{
    INJECT_POINT("ObjectMetaStore.RemoveObjectLocation");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_REMOVE_OBJ_LOCATION);
    std::string key = workerAddr + "_" + objectKey;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->DeleteWithOrderingKey(LOCATION_TABLE, key, objectKey),
                                     FormatString("Failed to delete location from rocksdb: %s", key));
    return Status::OK();
}

Status ObjectMetaStore::RemoveObjectLocationForRollbackAndWait(const std::string &objectKey,
                                                               const std::string &workerAddr)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_REMOVE_OBJ_LOCATION);
    const std::string locationKey = workerAddr + "_" + objectKey;
    const std::vector<RocksStore::TableKey> tableKeys = { { LOCATION_TABLE, locationKey } };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->DeleteBatchAcrossTables(tableKeys, objectKey),
        FormatString("Failed to commit rollback location deletion to rocksdb: %s", locationKey));
    return Status::OK();
}

Status ObjectMetaStore::RemoveObjectLocationAndMeta(const std::string &objectKey, const std::string &workerAddr)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_REMOVE_OBJ_LOCATION);
    const std::string locationKey = workerAddr + "_" + objectKey;
    const std::vector<RocksStore::TableKey> tableKeys = {
        { LOCATION_TABLE, locationKey }, { META_TABLE, objectKey }
    };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->DeleteBatchAcrossTables(tableKeys, objectKey),
        FormatString("Failed to delete object metadata and location from rocksdb: %s", objectKey));
    return Status::OK();
}

Status ObjectMetaStore::RemoveRocksKey(const std::string &key, const std::string &table)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Delete(table, key),
                                     FormatString("Failed to delete object key: %s", key));
    return Status::OK();
}

Status ObjectMetaStore::PrefixRemoveRocksKey(const std::string &prefixKey, const std::string &table)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->PrefixDelete(table, prefixKey),
                                     FormatString("Failed to delete object key: %s", prefixKey));
    return Status::OK();
}

Status ObjectMetaStore::AddNestedRelationship(const std::string &parentObjKey, const std::string &childObjKey)
{
    INJECT_POINT("ObjectMetaStore.AddNestedRelationship");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = parentObjKey + "_" + childObjKey;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(NESTED_TABLE, key, childObjKey),
                                     FormatString("Failed to add nested relationship, objectKey: %s", childObjKey));
    return Status::OK();
}

Status ObjectMetaStore::RemoveNestedRelationship(const std::string &parentObjKey, const std::string &childObjKey)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = parentObjKey + "_" + childObjKey;
    return RemoveRocksKey(key, NESTED_TABLE);
}

Status ObjectMetaStore::UpdateNestedRefCount(const std::string &objKey, uint32_t count)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = objKey;
    std::string strCount = std::to_string(count);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(NESTED_COUNT_TABLE, key, strCount),
                                     FormatString("Failed to add nested relationship, objectKey: %s", objKey));
    return Status::OK();
}

Status ObjectMetaStore::RemoveNestedRefCount(const std::string &objKey)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = objKey;
    return RemoveRocksKey(key, NESTED_COUNT_TABLE);
}

Status ObjectMetaStore::GetAllFromRocks(const std::string &table,
                                        std::vector<std::pair<std::string, std::string>> &outMetas)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->GetAll(table, outMetas),
                                     FormatString("Failed to get all pairs from table: %s", table));
    VLOG(1) << FormatString("Succeed to get all pairs from rocksdb, table %s", table);
    return Status::OK();
}

Status ObjectMetaStore::AddAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey,
                                         const NotifyWorkerOp &op, bool waitForCompletion)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = workerAddr + "_" + objectKey;
    ObjectAsyncOpDetailPb pb;
    FillNotifyWorkerOpDetailPb(op, pb);
    auto val = pb.SerializeAsString();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->PutWithOrderingKey(ASYNC_WORKER_OP_TABLE, key, val, key, waitForCompletion),
        FormatString("Failed to add async worker op to rocksdb: %s, op: %d", key, static_cast<uint32_t>(op.type)));
    return Status::OK();
}

Status ObjectMetaStore::RemoveAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    return RemoveRocksKey(workerAddr + "_" + objectKey, ASYNC_WORKER_OP_TABLE);
}

Status ObjectMetaStore::RemoveAsyncWorkerOpByWorker(const std::string &workerAddr)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    return PrefixRemoveRocksKey(workerAddr, ASYNC_WORKER_OP_TABLE);
}

Status ObjectMetaStore::AddRemoteClientRef(const std::string &remoteClientId, const std::string &masterAddr)
{
    INJECT_POINT("master.rocksdb.AddRemoteClientObjRef");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = remoteClientId + "_" + masterAddr;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(REMOTE_CLIENT_REF_TABLE, key, ""),
                                     FormatString("Failed to add remote client ref to rocksdb: %s", key));
    return Status::OK();
}

Status ObjectMetaStore::RemoveRemoteClientRef(const std::string &remoteClientId, const std::string &masterAddr)
{
    INJECT_POINT("master.rocksdb.RemoveRemoteClientObjRef");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = remoteClientId + "_" + masterAddr;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveRocksKey(key, REMOTE_CLIENT_REF_TABLE),
                                     FormatString("Failed to delete remote client ref from rocksdb: %s", key));
    return Status::OK();
}

Status ObjectMetaStore::AddGlobalRef(const std::string &key, const std::string &objectKey, bool isRemoteClient)
{
    INJECT_POINT("master.rocksdb.AddGlobalRef");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string mixKey = key + "_" + objectKey;
    if (isRemoteClient) {
        mixKey += REMOTE_CLIENT_FLAG;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(GLOBAL_REF_TABLE, mixKey, ""),
                                     FormatString("Failed to add global ref to rocksdb: %s", mixKey));
    return Status::OK();
}

Status ObjectMetaStore::RemoveGlobalRef(const std::string &key, const std::string &objectKey, bool isRemoteClient)
{
    INJECT_POINT("master.rocksdb.RemoveGlobalRef");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string mixKey = key + "_" + objectKey;
    if (isRemoteClient) {
        mixKey += REMOTE_CLIENT_FLAG;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveRocksKey(mixKey, GLOBAL_REF_TABLE),
                                     FormatString("Failed to delete global ref from rocksdb: %s", mixKey));
    return Status::OK();
}

std::string ObjectMetaStore::EncodeDeletedObjectValue(uint64_t delVersion, const std::string &targetWorkerAddress)
{
    if (targetWorkerAddress.empty()) {
        return std::to_string(delVersion);
    }
    return std::to_string(delVersion) + "|" + targetWorkerAddress;
}

bool ObjectMetaStore::DecodeDeletedObjectValue(const std::string &objectKey, const std::string &encodedValue,
                                               uint64_t &delVersion, std::string &targetWorkerAddress)
{
    auto delimiter = encodedValue.find('|');
    auto versionStr = delimiter == std::string::npos ? encodedValue : encodedValue.substr(0, delimiter);
    try {
        delVersion = StrToUnsignedLongLong(versionStr);
    } catch (std::invalid_argument &) {
        LOG(WARNING) << "Cannot convert delete version to uint64_t. objectKey:" << objectKey
                     << ", encoded value:" << encodedValue;
        return false;
    } catch (const std::out_of_range &) {
        LOG(WARNING) << "Delete version is out of range. objectKey:" << objectKey << ", encoded value:" << encodedValue;
        return false;
    }
    targetWorkerAddress = delimiter == std::string::npos ? "" : encodedValue.substr(delimiter + 1);
    return true;
}

Status ObjectMetaStore::AddDeletedObjectWithDelVersion(const std::string &objectKey, uint64_t version,
                                                       uint64_t delVersion)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string versionStr = EncodeDeletedObjectValue(delVersion, "");
    std::string key = objectKey + "/" + std::to_string(version);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->Put(GLOBAL_CACHE_TABLE, key, versionStr),
        FormatString("Failed to add l2 cache delete to rocksdb: objectKey=%s, version=%s", objectKey, versionStr));
    return Status::OK();
}

Status ObjectMetaStore::AddDeletedObject(const std::string &objectKey, uint64_t version)
{
    return AddDeletedObjectWithDelVersion(objectKey, version, version);
}

Status ObjectMetaStore::RemoveDeletedObject(const std::string &objectKey, uint64_t version)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    return RemoveRocksKey(objectKey + "/" + std::to_string(version), GLOBAL_CACHE_TABLE);
}

}  // namespace master
}  // namespace datasystem
