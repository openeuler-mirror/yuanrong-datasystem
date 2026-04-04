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
 * Description: Define interface to store object meta in RocksDB.
 */
#include "datasystem/master/object_cache/store/object_meta_store.h"

#include <atomic>
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
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/log/log.h"
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
#include "datasystem/master/object_cache/store/meta_async_queue.h"
#include "datasystem/utils/status.h"

DS_DEFINE_uint32(etcd_meta_pool_size, 8, "ETCD metadata async pool size");
DS_DECLARE_bool(oc_io_from_l2cache_need_metadata);
DS_DECLARE_string(rocksdb_write_mode);

static bool ValidateEtcdPoolSize(const char *flagName, uint32_t value)
{
    const uint32_t maxSize = 128;
    if (value <= 0 || value > maxSize) {
        LOG(ERROR) << "Flag " << flagName << " must be greater than 0 and less than " << maxSize;
        return false;
    }
    return true;
}
DS_DEFINE_validator(etcd_meta_pool_size, ValidateEtcdPoolSize);

#define EXEC_UTIL_SUCCESS(_statement, _quietCode, _exitFlag)                 \
    do {                                                                     \
        Status rc = (_statement);                                            \
        if (!(_exitFlag) && rc.GetCode() == StatusCode::K_RPC_UNAVAILABLE) { \
            continue;                                                        \
        }                                                                    \
        if (rc.IsError() && rc.GetCode() != _quietCode) {                    \
            LOG(ERROR) << "Execute ETCD op failed: " << rc.ToString();       \
        }                                                                    \
        break;                                                               \
    } while (true)

namespace datasystem {
namespace master {

ObjectMetaStore::ObjectMetaStore(RocksStore *rocksStore, EtcdStore *etcdStore, bool isEnable)
    : rocksStore_(rocksStore),
      etcdStore_(etcdStore),
      maxRequestSize_(static_cast<uint64_t>(QUEUE_CAPACITY) * FLAGS_etcd_meta_pool_size),
      isPersistenceEnabled_(isEnable)
{
}

ObjectMetaStore::~ObjectMetaStore()
{
    running_ = false;
    for (auto &thread : threadPool_) {
        thread.join();
    }
}

Status ObjectMetaStore::Init()
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    ioFromL2CacheNeedMeta_ = FLAGS_oc_io_from_l2cache_need_metadata;
    RETURN_IF_NOT_OK(InitEtcdStore());
    RETURN_IF_NOT_OK(InitRocksStore());
    return Status::OK();
}

Status ObjectMetaStore::InitEtcdStore()
{
    if (!EtcdEnable()) {
        LOG(INFO) << "Not enable etcd for meta store";
        return Status::OK();
    }

    for (uint32_t i = 0; i < FLAGS_etcd_meta_pool_size; i++) {
        queues_.emplace_back(std::make_shared<MetaAsyncQueue>(QUEUE_CAPACITY));
    }
    for (uint32_t i = 0; i < FLAGS_etcd_meta_pool_size; i++) {
        threadPool_.emplace_back(Thread(&ObjectMetaStore::AsyncMetaOpToEtcdStorageHandler, this, i, queues_[i]));
    }

    // Hash table for normal key.
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                             std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                             std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                             std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX));

    // Worker table for key with worker id.
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                             std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    RETURN_IF_NOT_OK(etcdStore_->CreateTable(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                             std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    return etcdStore_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
}

std::string Hash2Str(uint32_t hash)
{
    const uint32_t width = 10;
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(width) << hash;
    return ss.str();
}

Status DecodeEtcdKey(const std::string &etcdKey, std::string &outKey, std::string &hash)
{
    const uint32_t minSize = 2;
    auto res = Split(etcdKey, "/");
    CHECK_FAIL_RETURN_STATUS(res.size() >= minSize, StatusCode::K_RUNTIME_ERROR,
                             FormatString("%s not an ETCD key.", etcdKey));
    hash = res.front();
    outKey = etcdKey.substr(hash.size() + 1);
    return Status::OK();
}

std::pair<uint32_t, bool> ObjectMetaStore::HashFunction(const std::string &key)
{
    auto res = Split(key, ";");
    uint32_t hash;
    bool specKey;

    const size_t num = 2;
    if (res.size() > num && Validator::IsUuid(res[res.size() - num])) {
        // key with worker id and az name.
        hash = MurmurHash3_32(res[res.size() - num]);
        specKey = true;
    } else if (res.size() > 1 && Validator::IsUuid(res.back())) {
        // key with worker id.
        hash = MurmurHash3_32(res.back());
        specKey = true;
    } else {
        hash = MurmurHash3_32(key);
        specKey = false;
    }
    return { hash, specKey };
}

void ObjectMetaStore::WarnIfNeed()
{
    const float oneHundredPercent = 100.0;
    const float sixtyPercentThreshold = 60.0;
    const float eightyPercentThreshold = 80.0;
    const int oneHundredFreq = 10;
    const int eightyPercentFreq = 50;
    const int sixtyPercentFreq = 100;
    float ratio = static_cast<float>(asyncReqSize_) / maxRequestSize_ * oneHundredPercent;
    std::string msg = FormatString("The thread of ETCD async queue is: [%.3lf]", ratio);
    if (ratio >= oneHundredPercent) {
        LOG_EVERY_N(WARNING, oneHundredFreq) << msg << "%, thread full";
    } else if (ratio >= eightyPercentThreshold) {
        LOG_EVERY_N(WARNING, eightyPercentFreq) << msg << "%, reaches 80% threshold";
    } else if (ratio >= sixtyPercentThreshold) {
        LOG_EVERY_N(WARNING, sixtyPercentFreq) << msg << "%, reaches 60% threshold";
    }
}

void ObjectMetaStore::InsertToEtcdKeyMap(const std::string &table, const std::string &key, uint32_t hash, bool isAsync)
{
    if (!EtcdEnable()) {
        return;
    }
    std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
    auto tableIter = etcdKeyMap_.find(table);
    if (tableIter == etcdKeyMap_.end()) {
        std::map<std::string, std::pair<uint32_t, bool>> item{ { key, { hash, isAsync } } };
        (void)etcdKeyMap_.emplace(table, std::move(item));
        return;
    }
    auto itemIter = tableIter->second.find(key);
    if (itemIter == tableIter->second.end()) {
        (void)tableIter->second.emplace(key, std::make_pair(hash, isAsync));
    }
}

void ObjectMetaStore::GetHashAndTable(const std::string &objKey, const std::string &tablePrefix, uint32_t &hash,
                                      std::string &table)
{
    // Our rules:
    // 1. If objKey like 'KEY', we will calculate its hash code and put it into etcd table like: ${hash}/key
    // 2. If objKey like 'KEY:ID', we will calculate ID's hash code and put it into etcd table like: ${id_hash}/key
    auto res = HashFunction(objKey);
    hash = res.first;
    bool specKey = res.second;
    table = tablePrefix + (specKey ? ETCD_WORKER_SUFFIX : ETCD_HASH_SUFFIX);
}

void ObjectMetaStore::AsyncMetaOpToEtcdStorageHandler(int threadNum, const std::shared_ptr<MetaAsyncQueue> &queue)
{
    while (running_) {
        INJECT_POINT("AsyncMetaOpToEtcdStorageHandler", [] { return; });
        std::shared_ptr<AsyncElement> element;
        constexpr int timeoutMs = 1000;
        bool ret = queue->Poll(element, timeoutMs);
        if (!ret || element == nullptr) {
            continue;
        }
#ifdef WITH_TESTS
        static const auto injectFunc = [&element, this](int delayMs, const std::string &tableName, bool passAdd = false,
                                                        bool passDel = false) {
            if (passAdd && element->RequestType() == AsyncElement::ReqType::ADD) {
                return;
            }
            if (passDel && element->RequestType() == AsyncElement::ReqType::DEL) {
                return;
            }
            if (element->Table().find(tableName) != std::string::npos) {
                Timer timer;
                while (running_ && timer.ElapsedMilliSecond() < delayMs) {
                    const int checkIntervalMs = 100;
                    std::this_thread::sleep_for(std::chrono::milliseconds(checkIntervalMs));
                }
            }
        };
#endif
        INJECT_POINT("ObjectMetaStore.AsyncMetaOpToEtcdStorageHandler.Delay.MetaTable",
                     [](int delayS) { injectFunc(delayS, ETCD_META_TABLE_PREFIX); });
        INJECT_POINT("ObjectMetaStore.AsyncMetaOpToEtcdStorageHandler.Delay.GlobalCacheTable.PassAdd",
                     [](int delayS) { injectFunc(delayS, ETCD_GLOBAL_CACHE_TABLE_PREFIX, true); });
        const auto &etcdKey = element->Key();
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(element->TraceID());
        VLOG(1) << FormatString("handler %d get key: %s", threadNum, etcdKey);
        uint64_t asyncElapse = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                         std::chrono::steady_clock::now() - element->BeginTimestamp())
                                                         .count());
        switch (element->RequestType()) {
            case AsyncElement::ReqType::ADD:
                EXEC_UTIL_SUCCESS(etcdStore_->Put(element->Table(), etcdKey, element->Value(), nullptr,
                                                  SEND_RPC_TIMEOUT_MS_DEFAULT, asyncElapse),
                                  K_OK, !running_);
                break;
            case AsyncElement::ReqType::DEL:
                EXEC_UTIL_SUCCESS(etcdStore_->Delete(element->Table(), etcdKey, asyncElapse), K_NOT_FOUND, !running_);
                LOG_IF_ERROR(element->ExcutePostHandler(), "Excute post handler failed, etcd key: " + etcdKey);
                break;
            default:
                LOG(WARNING) << "unknown operation: " << static_cast<uint8_t>(element->RequestType());
                break;
        }
        (void)asyncReqSize_.fetch_sub(1, std::memory_order_relaxed);
    }
}

Status ObjectMetaStore::AddOneAsyncTaskToEtcdStore(const std::string &objectKey, const std::string &table,
                                                   const std::string &etcdKey, const std::string &value,
                                                   AsyncElement::ReqType requestType, uint64_t timestamp,
                                                   const std::string &traceId, std::function<Status()> &&postHandler)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!queues_.empty(), K_NOT_READY,
                                         "It does not support executing etcd asynchronous tasks currently.");
    WarnIfNeed();
    auto element = timestamp == 0 ? std::make_shared<AsyncElement>(objectKey, table, etcdKey, value, requestType)
                                  : std::make_shared<AsyncElement>(objectKey, table, etcdKey, value, requestType,
                                                                   timestamp, traceId);
    if (postHandler) {
        element->SetPostHandler(std::move(postHandler));
    }
    auto threadIdx = MurmurHash3_32(objectKey) % FLAGS_etcd_meta_pool_size;
    std::shared_ptr<AsyncElement> elderElement;
    int incrCnt = 0;
    queues_[threadIdx]->AppendAsyncTask(element, elderElement, incrCnt);
    if (incrCnt > 0) {
        (void)asyncReqSize_.fetch_add(incrCnt, std::memory_order_relaxed);
    } else if (incrCnt < 0) {
        (void)asyncReqSize_.fetch_sub(-incrCnt, std::memory_order_relaxed);
    }
    RequestCounter::GetInstance().ResetLastArrivalTime("AddOneAsyncTaskToEtcdStore");
    if (elderElement != nullptr) {
        LOG(WARNING) << FormatString("The queue of 'Async meta op to ETCD storage' is full, remove an operation for %s",
                                     elderElement->Key());
    }
    return Status::OK();
}

Status ObjectMetaStore::PutToEtcdStore(const std::string &tablePrefix, const std::string &objKey,
                                       const std::string &key, const std::string &value, WriteType type)
{
    RETURN_OK_IF_TRUE(!EtcdEnable());
    RETURN_OK_IF_TRUE(type == WriteType::ROCKS_ONLY);

    auto isAsync = [](WriteType type) { return type == WriteType::ROCKS_ASYNC_ETCD; };

    uint32_t hash;
    std::string table;
    GetHashAndTable(objKey, tablePrefix, hash, table);
    std::string etcdKey = Hash2Str(hash) + "/" + key;
    if (type == WriteType::ROCKS_SYNC_ETCD) {
        INJECT_POINT("PutToEtcdStore.failed");
        RETURN_IF_NOT_OK(etcdStore_->Put(table, etcdKey, value, timeoutDuration.CalcRemainingTime()));
    } else if (isAsync(type)) {
        INJECT_POINT("master.before_sub_async_send_etcd_req");
        RETURN_IF_NOT_OK(AddOneAsyncTaskToEtcdStore(objKey, table, etcdKey, value, AsyncElement::ReqType::ADD));
    }
    bool isAsyncEtcd = isAsync(type);
    InsertToEtcdKeyMap(table, key, hash, isAsyncEtcd);
    return Status::OK();
}

Status ObjectMetaStore::BatchPutToEtcdStore(const std::string &tablePrefix,
                                            std::unordered_map<std::string, std::string> &metaInfos, WriteType type,
                                            bool needSaveToEtcd)
{
    RETURN_OK_IF_TRUE(!EtcdEnable());
    RETURN_OK_IF_TRUE(type == WriteType::ROCKS_ONLY);

    auto isAsync = [](WriteType type) { return type == WriteType::ROCKS_ASYNC_ETCD; };
    std::unordered_map<std::string, EtcdStore::BatchInfoPutToEtcd> newMetaInfos;
    std::string tableName;
    for (const auto &info : metaInfos) {
        uint32_t hash = 0;
        std::string table;
        GetHashAndTable(info.first, tablePrefix, hash, table);
        std::string etcdKey = Hash2Str(hash) + "/" + info.first;
        newMetaInfos[info.first].meta = info.second;
        newMetaInfos[info.first].hashVal = hash;
        newMetaInfos[info.first].tableName = table;
        newMetaInfos[info.first].etcdKey = etcdKey;
    }
    if (type == WriteType::ROCKS_SYNC_ETCD && needSaveToEtcd) {
        RETURN_IF_NOT_OK(etcdStore_->BatchPut(newMetaInfos));
    }
    bool isAsyncEtcd = isAsync(type);
    for (const auto &info : newMetaInfos) {
        if (isAsyncEtcd) {
            RETURN_IF_NOT_OK(AddOneAsyncTaskToEtcdStore(info.first, info.second.tableName, info.second.etcdKey,
                                                        info.second.meta, AsyncElement::ReqType::ADD));
        }
        InsertToEtcdKeyMap(info.second.tableName, info.first, info.second.hashVal, isAsyncEtcd);
    }
    return Status::OK();
}

Status ObjectMetaStore::RemoveEtcdKey(const std::string &objectKey, const std::string &key,
                                      const std::string &tablePrefix, std::function<Status()> &&postHandler)
{
    RETURN_OK_IF_TRUE(!EtcdEnable());
    auto res = Split(objectKey, ";");
    bool specKey = false;
    const size_t num = 2;
    if ((res.size() > num && Validator::IsUuid(res[res.size() - num]))
        || (res.size() > 1 && Validator::IsUuid(res.back()))) {
        specKey = true;
    }
    uint32_t hash;
    bool async;
    std::string table = tablePrefix + (specKey ? ETCD_WORKER_SUFFIX : ETCD_HASH_SUFFIX);
    {
        std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
        auto tableIter = etcdKeyMap_.find(table);
        if (tableIter == etcdKeyMap_.end()) {
            VLOG(1) << table << " not find, skip deleting the key in etcd";
            return Status::OK();
        }
        auto itemIter = tableIter->second.find(key);
        if (itemIter == tableIter->second.end()) {
            LOG(INFO) << key << " not in etcdKeyMap_, skip deleting the key in etcd";
            return Status::OK();
        }
        hash = itemIter->second.first;
        async = itemIter->second.second;
        (void)tableIter->second.erase(itemIter);
    }
    Status rc;
    std::string etcdKey = Hash2Str(hash) + "/" + key;
    if (async) {
        RETURN_IF_NOT_OK(AddOneAsyncTaskToEtcdStore(objectKey, table, etcdKey, "", AsyncElement::ReqType::DEL, 0, "",
                                                    std::move(postHandler)));
    } else {
        rc = etcdStore_->Delete(table, etcdKey);
    }

    if (rc.IsError() && rc.GetCode() != StatusCode::K_NOT_FOUND) {
        std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
        (void)etcdKeyMap_[table].emplace(key, std::make_pair(hash, async));
        return rc;
    }
    return postHandler != nullptr && !async ? postHandler() : Status::OK();
}

void ObjectMetaStore::PrefixSearchAndErase(const std::string &table, const std::string &prefixKey,
                                           std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> &keys)
{
    auto tableIter = etcdKeyMap_.find(table);
    if (tableIter == etcdKeyMap_.end()) {
        return;
    }
    auto keyBegIter = tableIter->second.upper_bound(prefixKey);
    if (keyBegIter == tableIter->second.end()) {
        return;
    }
    auto keyEndIter = tableIter->second.upper_bound(StringPlusOne(prefixKey));
    for (auto iter = keyBegIter; iter != keyEndIter; ++iter) {
        keys.emplace_back(iter->first, std::make_pair(iter->second.first, iter->second.second));
    }
    (void)tableIter->second.erase(keyBegIter, keyEndIter);
}

Status ObjectMetaStore::PrefixRemoveEtcdKeyImpl(
    const std::string &table, const std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> &keys,
    std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> &failedKeys)
{
    Status rc;
    for (const auto &item : keys) {
        uint32_t hash = item.second.first;
        bool async = item.second.second;
        std::string etcdKey = Hash2Str(hash) + "/" + item.first;
        if (async) {
            RETURN_IF_NOT_OK(AddOneAsyncTaskToEtcdStore(item.first, table, etcdKey, "", AsyncElement::ReqType::DEL));
        } else {
            Status s = etcdStore_->Delete(table, etcdKey);
            if (s.IsError() && s.GetCode() != StatusCode::K_NOT_FOUND) {
                LOG(ERROR) << FormatString("Delete key [%s] failed, error: %s", item.first, s.ToString());
                rc = s;
                failedKeys.emplace_back(item);
            }
        }
    }
    RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
    return rc;
}

Status ObjectMetaStore::PrefixRemoveEtcdKey(const std::string &prefixKey, const std::string &tablePrefix)
{
    RETURN_OK_IF_TRUE(!EtcdEnable());

    std::string hashTable = tablePrefix + ETCD_HASH_SUFFIX;
    std::string workerIdTable = tablePrefix + ETCD_WORKER_SUFFIX;
    std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> hashKeys, workerIdKeys;
    {
        std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
        PrefixSearchAndErase(hashTable, prefixKey, hashKeys);
        PrefixSearchAndErase(workerIdTable, prefixKey, workerIdKeys);
    }

    RETURN_OK_IF_TRUE(hashKeys.empty() && workerIdKeys.empty());

    std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> failedHashKeys;
    std::vector<std::pair<std::string, std::pair<uint32_t, bool>>> failedWorkerIdKeys;
    Status rc = PrefixRemoveEtcdKeyImpl(hashTable, hashKeys, failedHashKeys);
    Status s = PrefixRemoveEtcdKeyImpl(workerIdTable, workerIdKeys, failedWorkerIdKeys);
    rc = s.IsError() ? s : rc;

    std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
    for (const auto &item : failedHashKeys) {
        (void)etcdKeyMap_[hashTable].emplace(item.first, item.second);
    }
    for (const auto &item : failedWorkerIdKeys) {
        (void)etcdKeyMap_[workerIdTable].emplace(item.first, item.second);
    }
    return rc;
}

Status ObjectMetaStore::InitRocksStore()
{
    RETURN_RUNTIME_ERROR_IF_NULL(rocksStore_);
    return Replica::CreateOcTable(rocksStore_);
}

bool ObjectMetaStore::IsRocksdbEnableWriteMeta()
{
    return FLAGS_rocksdb_write_mode != "none";
}

Status ObjectMetaStore::AddRocksdbHealthTag()
{
    RETURN_IF_NOT_OK(PutToRocksStore(HEALTH_TABLE, "status", HEALTH_STATUS));
    ChangeRocksdbToRunning();
    return Status::OK();
}

bool ObjectMetaStore::CheckHealth()
{
    std::string value;
    Status rc = rocksStore_->Get(HEALTH_TABLE, "status", value);
    if (rc.IsError() || value != HEALTH_STATUS) {
        LOG_IF_ERROR(rc, "check HEALTH_TABLE in rocksDb failed");
        isRocksdbRunning_ = false;
        return false;
    }
    return true;
}

void ObjectMetaStore::GetMetasMatch(
    std::function<bool(const std::string &)> &&matchFunc,
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &objAsyncMap)
{
    for (size_t i = 0; i < queues_.size(); ++i) {
        uint64_t count = 0;
        queues_[i]->PollMetasByObjectKey(std::forward<std::function<bool(const std::string &)>>(matchFunc), objAsyncMap,
                                         count);
        (void)asyncReqSize_.fetch_sub(count, std::memory_order_relaxed);
    }
    {
        std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
        for (const auto &entry : objAsyncMap) {
            (void)etcdKeyMap_.erase(entry.first);
        }
    }
}

void ObjectMetaStore::PollAsyncElementsByObjectKey(const std::string &objectKey,
                                                   std::unordered_set<std::shared_ptr<AsyncElement>> &elements)
{
    if (queues_.empty()) {
        LOG(WARNING) << "It does not support executing etcd asynchronous tasks currently.";
        return;
    }
    auto idx = MurmurHash3_32(objectKey) % FLAGS_etcd_meta_pool_size;
    queues_[idx]->PollAsyncElementsByObjectKey(objectKey, elements);
    (void)asyncReqSize_.fetch_sub(elements.size(), std::memory_order_relaxed);
    {
        std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
        (void)etcdKeyMap_.erase(objectKey);
    }
}

void ObjectMetaStore::InsertWaitAsyncElements(const std::string &objectKey, const std::string &table,
                                              const std::string &key, const std::string &value,
                                              AsyncElement::ReqType reqType, uint64_t timestamp,
                                              const std::string &traceId)
{
    AddOneAsyncTaskToEtcdStore(objectKey, table, key, value, reqType, timestamp, traceId);
    InsertToEtcdKeyMap(table, key, HashFunction(objectKey).first, true);
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

Status ObjectMetaStore::CreateOrUpdateMeta(const std::string &objectKey, const std::string &serializedStr,
                                           WriteType type)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_CREATE_META);
    const std::string &key = objectKey;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(META_TABLE, key, serializedStr),
                                     FormatString("Failed to add object meta: %s", key));
    point.Record();
    Status rc = PutToEtcdStore(ETCD_META_TABLE_PREFIX, key, key, serializedStr, type);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Failed to add object meta to etcd store: %s", key);
    }
    return rc;
}

Status ObjectMetaStore::CreateOrUpdateBatchMeta(std::unordered_map<std::string, std::string> &metaInfos, WriteType type)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_CREATE_META);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->BatchPut(META_TABLE, metaInfos),
                                     FormatString("Failed to add object meta: %s", MapToString(metaInfos)));

    Status rc = BatchPutToEtcdStore(ETCD_META_TABLE_PREFIX, metaInfos, type, true);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Failed to add object meta to etcd store: %s", MapToString(metaInfos));
        (void)rocksStore_->BatchDelete(META_TABLE, metaInfos);
    }
    return Status::OK();
}

Status ObjectMetaStore::RemoveMeta(const std::string &key, bool needRemoveEtcdData)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_CREATE_META);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveRocksKey(key, META_TABLE),
                                     FormatString("Failed to delete meta from rocksdb: %s", key));
    if (needRemoveEtcdData) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveEtcdKey(key, key, ETCD_META_TABLE_PREFIX),
                                         FormatString("Failed to delete meta from etcd: %s", key));
    }
    return Status::OK();
}

Status ObjectMetaStore::AddObjectLocation(const std::string &objectKey, const std::string &workerAddr,
                                         const std::string &ackPersistenceVal)
{
    INJECT_POINT("ObjectMetaStore.AddObjectLocation");
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_ADD_OBJ_LOCATION);
    std::string key = workerAddr + "_" + objectKey;
    // for compatibility: empty string "" stands for ACK, "0" stands for UNACK
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->Put(LOCATION_TABLE, key, ackPersistenceVal),
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
        std::string key = keyLocation.second + "_" + keyLocation.first; // workerAddr + "_" + objectKey
        locationInfos[key] = ackPersistenceVal;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rocksStore_->BatchPut(LOCATION_TABLE, locationInfos),
        FormatString("Failed to batch add global ref to rocksdb: key is %s", keyLocations.begin()->first));
    return Status::OK();
}

Status ObjectMetaStore::RemoveObjectLocation(const std::string &objectKey, const std::string &workerAddr)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    PerfPoint point(PerfKey::MASTER_ROCKSDB_REMOVE_OBJ_LOCATION);
    std::string key = workerAddr + "_" + objectKey;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveRocksKey(key, LOCATION_TABLE),
                                     FormatString("Failed to delete location from rocksdb: %s", key));
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

Status ObjectMetaStore::GetRangeFromEtcd(const std::string &tablePrefix, const std::string &rocksTable,
                                         const std::string &suffix, const std::pair<uint32_t, uint32_t> &range,
                                         std::vector<std::pair<std::string, std::string>> &metas)
{
    std::string table = tablePrefix + suffix;
    std::string keyBegin = Hash2Str(range.first);
    std::string keyEnd = Hash2Str(range.second);
    VLOG(1) << FormatString("[ETCD] Recovery %s from etcd, begin %s, end %s", table, keyBegin, keyEnd);
    RETURN_IF_NOT_OK_EXCEPT(etcdStore_->RangeSearch(table, keyBegin, keyEnd, metas), StatusCode::K_NOT_FOUND);
    for (auto &kv : metas) {
        std::string hash;
        RETURN_IF_NOT_OK(DecodeEtcdKey(kv.first, kv.first, hash));
        uint32_t hashNum = 0;
        try {
            hashNum = StrToUnsignedLong(hash);
        } catch (const std::exception &e) {
            RETURN_STATUS(StatusCode::K_INVALID, "Parse failed with hash: " + hash);
        }
        RETURN_IF_NOT_OK(rocksStore_->Put(rocksTable, kv.first, kv.second));
        std::lock_guard<std::shared_timed_mutex> l(etcdMtx_);
        auto iter = etcdKeyMap_.find(table);
        if (iter == etcdKeyMap_.end()) {
            std::map<std::string, std::pair<uint32_t, bool>> item{ { kv.first, { hashNum, false } } };
            (void)etcdKeyMap_.emplace(table, std::move(item));
        } else {
            (void)iter->second.emplace(kv.first, std::make_pair(hashNum, false));
        }
    }
    return Status::OK();
}

Status ObjectMetaStore::GetFromEtcd(const std::string &tablePrefix, const std::string &rocksTable,
                                    const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges,
                                    std::vector<std::pair<std::string, std::string>> &outMetas)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    RETURN_OK_IF_TRUE(!EtcdEnable());
    bool isGetAll = workerUuids.empty() && extraRanges.empty();
    auto getFunc = [this, &tablePrefix, &rocksTable](const std::string &suffix,
                                                     const std::pair<uint32_t, uint32_t> &range,
                                                     std::vector<std::pair<std::string, std::string>> &metas) {
        return GetRangeFromEtcd(tablePrefix, rocksTable, suffix, range, metas);
    };

    std::vector<std::pair<std::string, std::string>> metas;
    if (isGetAll) {
        worker::HashRange hashRange;
        GetHashRangeNonBlockEvent::GetInstance().NotifyAll(hashRange);
        for (const auto &range : hashRange) {
            RETURN_IF_NOT_OK(getFunc(ETCD_HASH_SUFFIX, range, metas));
            (void)outMetas.insert(outMetas.end(), metas.begin(), metas.end());
            metas.clear();
        }
        std::string workerId;
        GetLocalWorkerUuidEvent::GetInstance().NotifyAll(workerId);
        uint32_t workerHash = MurmurHash3_32(workerId);
        RETURN_IF_NOT_OK(getFunc(ETCD_WORKER_SUFFIX, { workerHash, workerHash }, metas));
    } else {
        for (const auto &uuid : workerUuids) {
            uint32_t workerHash = MurmurHash3_32(uuid);
            RETURN_IF_NOT_OK(getFunc(ETCD_WORKER_SUFFIX, { workerHash, workerHash }, metas));
            (void)outMetas.insert(outMetas.end(), metas.begin(), metas.end());
            metas.clear();
        }
        for (const auto &range : extraRanges) {
            RETURN_IF_NOT_OK(getFunc(ETCD_HASH_SUFFIX, range, metas));
            (void)outMetas.insert(outMetas.end(), metas.begin(), metas.end());
            metas.clear();
        }
    }
    (void)outMetas.insert(outMetas.end(), metas.begin(), metas.end());
    VLOG(1) << FormatString("Succeed to get all pairs, table %s, size: %ld", tablePrefix, outMetas.size());
    return Status::OK();
}

Status ObjectMetaStore::AddAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey,
                                         const NotifyWorkerOp &op, WriteType type)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = workerAddr + "_" + objectKey;
    ObjectAsyncOpDetailPb pb;
    pb.set_op_type(static_cast<uint32_t>(op.type));
    pb.set_remove_meta_version(op.removeMetaVersion);
    *pb.mutable_remove_meta_az_names() = { op.removeMetaAzNames.begin(), op.removeMetaAzNames.end() };
    pb.set_delete_all_copy_version(op.deleteAllCopyMetaVersion);
    *pb.mutable_delete_all_copy_az_names() = { op.deleteAllCopyMetaAzNames.begin(), op.deleteAllCopyMetaAzNames.end() };
    pb.set_delete_object_version(op.delObjectVersion);
    auto val = pb.SerializeAsString();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->Put(ASYNC_WORKER_OP_TABLE, key, val),
        FormatString("Failed to add async worker op to rocksdb: %s, op: %d", key, static_cast<uint32_t>(op.type)));
    Status rc = PutToEtcdStore(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX, objectKey, key, val, type);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Failed to add async worker op to etcd store: %s", key);
        (void)rocksStore_->Delete(ASYNC_WORKER_OP_TABLE, key);
    }
    return rc;
}

Status ObjectMetaStore::RemoveAsyncWorkerOp(const std::string &workerAddr, const std::string &objectKey,
                                            bool needRemoveEtcdData)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string key = workerAddr + "_" + objectKey;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveRocksKey(key, ASYNC_WORKER_OP_TABLE),
                                     FormatString("Failed to delete async worker op from rocksdb: %s", key));
    if (needRemoveEtcdData) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveEtcdKey(objectKey, key, ETCD_ASYNC_WORKER_OP_TABLE_PREFIX),
                                         FormatString("Failed to delete async worker op from etcd: %s", key));
    }
    return Status::OK();
}

Status ObjectMetaStore::RemoveAsyncWorkerOpByWorker(const std::string &workerAddr)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        PrefixRemoveRocksKey(workerAddr, ASYNC_WORKER_OP_TABLE),
        FormatString("Failed to delete async worker op from rocksdb, prefix key %s", workerAddr));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        PrefixRemoveEtcdKey(workerAddr, ETCD_ASYNC_WORKER_OP_TABLE_PREFIX),
        FormatString("Failed to delete async worker op from etcd, prefix key %s", workerAddr));
    return Status::OK();
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

Status ObjectMetaStore::AddDeletedObjectWithDelVersion(const std::string &objectKey, uint64_t version,
                                                       uint64_t delVersion, WriteType type)
{
    RETURN_OK_IF_TRUE(!isPersistenceEnabled_);
    std::string versionStr = std::to_string(delVersion);
    std::string key = objectKey + "/" + std::to_string(version);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        rocksStore_->Put(GLOBAL_CACHE_TABLE, key, versionStr),
        FormatString("Failed to add l2 cache delete to rocksdb: objectKey=%s, version=%s", objectKey, versionStr));
    INJECT_POINT("etcd.failed.add.deleteobjecttoetcd", [&] {
        return Status(K_RPC_UNAVAILABLE, "Failed to add l2 cache delete to etcd store: objectKey" + objectKey);
    });
    Status rc = PutToEtcdStore(ETCD_GLOBAL_CACHE_TABLE_PREFIX, objectKey, key, versionStr, type);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Failed to add l2 cache delete to etcd store: objectKey=%s, version=%s", objectKey,
                                   versionStr);
        (void)rocksStore_->Delete(GLOBAL_CACHE_TABLE, key);
    }
    return rc;
}

Status ObjectMetaStore::AddDeletedObject(const std::string &objectKey, uint64_t version, WriteType type)
{
    return AddDeletedObjectWithDelVersion(objectKey, version, version, type);
}

Status ObjectMetaStore::RemoveDeletedObject(const std::string &objectKey, uint64_t version)
{
    std::string versionStr = std::to_string(version);
    std::string key = objectKey + "/" + versionStr;

    auto postRemoveEtcdKeyFunc = [this, key, objectKey, versionStr]() -> Status {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RemoveRocksKey(key, GLOBAL_CACHE_TABLE),
            FormatString("Failed to delete l2 cache from rocksdb: objectKey=%s,version=%s", objectKey, versionStr));
        return Status::OK();
    };

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RemoveEtcdKey(objectKey, key, ETCD_GLOBAL_CACHE_TABLE_PREFIX, std::move(postRemoveEtcdKeyFunc)),
        FormatString("Failed to delete l2 cache from etcd: objectKey=%s,version=%s", objectKey, versionStr));
    return Status::OK();
}

bool ObjectMetaStore::AsyncQueueEmpty()
{
    VLOG(2) << "Async queue size: " << asyncReqSize_;
    return asyncReqSize_.load(std::memory_order_relaxed) == 0;
}

std::string ObjectMetaStore::GetETCDAsyncQueueUsage()
{
    if (maxRequestSize_ == 0) {
        return "";
    }
    uint64_t currentSize = asyncReqSize_.load(std::memory_order_relaxed);
    auto masterEtcdQueueUsage = currentSize / static_cast<float>(maxRequestSize_);
    return FormatString("%llu/%llu/%.3f", currentSize, maxRequestSize_, masterEtcdQueueUsage);
}
}  // namespace master
}  // namespace datasystem
