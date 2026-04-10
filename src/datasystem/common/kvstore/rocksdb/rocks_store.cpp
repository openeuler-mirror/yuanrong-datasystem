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
 * Description: Interface to RocksDB.
 */
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"

#include <sys/stat.h>
#include <iostream>
#include <sstream>

#include <rocksdb/slice.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#ifdef WITH_TESTS
#include "datasystem/common/inject/inject_point.h"
#endif
#include "datasystem/common/log/log.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"

DS_DEFINE_bool(rocksdb_sync_write, false, "Controls whether rocksdb sets sync to true when writing data.");
DS_DEFINE_int32(rocksdb_max_open_file, 128, "Number of open files that can be used by the rocksdb");
DS_DEFINE_int32(rocksdb_background_threads, 16,
                "Number of background threads rocksdb can use for flushing and compacting.");
DS_DEFINE_string(rocksdb_write_mode, "async",
                 "Config the rocksdb support none, sync or async, async by default. Optional value: "
                 "'none', 'sync', 'async'. This represents the method of writing metadata to rocksdb.");
DS_DEFINE_validator(rocksdb_write_mode, &Validator::ValidateRocksdbModeType);

namespace datasystem {
std::mutex RocksStore::lck;
static constexpr size_t ROCKSDB_MAX_LOG_FILE_SIZE = 10u * 1024u * 1024u;  // 10MB
static constexpr size_t ROCKSDB_MAX_LOG_FILE_NUM = 3;
bool RocksStore::disableRocksDB = false;

namespace {
void InitRocksOptions(rocksdb::Options &options)
{
    options.IncreaseParallelism(FLAGS_rocksdb_background_threads);
    options.OptimizeLevelStyleCompaction();

    // Create the DB if it's not already present.
    options.create_if_missing = true;
    options.create_missing_column_families = false;
    // Open if the db already exists.
    options.error_if_exists = false;
    // Disable compression in every level.
    options.compression = rocksdb::CompressionType::kNoCompression;
    options.compression_per_level = { rocksdb::CompressionType::kNoCompression,
                                      rocksdb::CompressionType::kNoCompression,
                                      rocksdb::CompressionType::kNoCompression,
                                      rocksdb::CompressionType::kNoCompression,
                                      rocksdb::CompressionType::kNoCompression };
    options.env->SetAllowNonOwnerAccess(false);
    options.max_open_files = FLAGS_rocksdb_max_open_file;
    options.max_log_file_size = ROCKSDB_MAX_LOG_FILE_SIZE;
    options.keep_log_file_num = ROCKSDB_MAX_LOG_FILE_NUM;
}
}  // namespace

RocksStore::RocksStore()
{
    mode_ = ParseRocksdbWriteMode();
    InitializeAsyncThreadPool();
}

std::shared_ptr<RocksStore> RocksStore::GetInstance(
    const std::string &dbPath, const std::unordered_map<std::string, rocksdb::ColumnFamilyOptions> &tableOptions)
{
#ifdef WITH_TESTS
    INJECT_POINT("master.disableRocksDb", []() {
        RocksStore::disableRocksDB = true;
        return nullptr;
    });
#endif
    LOG(INFO) << "Rocksdb get instance, dbPath:" << dbPath;
    rocksdb::Options options;
    InitRocksOptions(options);

    std::lock_guard<std::mutex> lock(lck);
    std::shared_ptr<RocksStore> instance(new RocksStore);
    if (disableRocksDB) {
        LOG(INFO) << "Rocksdb is disabled";
        return instance;
    }
    std::vector<rocksdb::ColumnFamilyHandle *> columnFamilyHandles;
    std::vector<std::string> columnFamilies;

    instance->dbPath_ = dbPath;
    rocksdb::Status rc = rocksdb::DB::Open(options, instance->dbPath_, &instance->db_);
    if (rc.IsInvalidArgument()) {
        // Db may already exist and need to open with all its column families.
        rc = rocksdb::DB::ListColumnFamilies(options, instance->dbPath_, &columnFamilies);
        if (!rc.ok()) {
            LOG(ERROR) << "Cannot create/open database: " + std::string(rc.getState());
            return nullptr;
        }
        std::vector<rocksdb::ColumnFamilyDescriptor> columnDescriptors;
        for (const auto &item : columnFamilies) {
            auto columnOption = rocksdb::ColumnFamilyOptions();
            auto iter = tableOptions.find(item);
            if (iter != tableOptions.end()) {
                columnOption = iter->second;
            }
            columnDescriptors.emplace_back(rocksdb::ColumnFamilyDescriptor(item, columnOption));
        }
        rc = rocksdb::DB::Open(options, instance->dbPath_, columnDescriptors, &columnFamilyHandles, &instance->db_);
    }
    if (instance->db_ != nullptr) {
        if (columnFamilyHandles.size() != columnFamilies.size()) {
            LOG(ERROR) << FormatString(
                "Size not equal between columnFamilyHandles(%zu) and columnFamilies(%zu)."
                " Db::Open status: %s",
                columnFamilyHandles.size(), columnFamilies.size(), rc.getState());
            return nullptr;
        }
        for (size_t i = 0; i < columnFamilyHandles.size(); ++i) {
            instance->tables_.emplace(columnFamilies[i], columnFamilyHandles[i]);
        }
    }
    if (!rc.ok()) {
        LOG(ERROR) << "Cannot create/open database: " + std::string(rc.getState());
        return nullptr;
    }
    LOG(INFO) << "Rocksdb get instance finished, dbPath:" << dbPath << "write mode: " << FLAGS_rocksdb_write_mode;
    return instance;
}

RocksStore::~RocksStore()
{
    Close();
}

void RocksStore::Close()
{
    if (db_ == nullptr) {
        return;
    }
    LOG(INFO) << "Close rocksdb, dbPath:" << dbPath_;
    for (auto &handle : tables_) {
        db_->DestroyColumnFamilyHandle(handle.second);
    }
    tables_.clear();
    db_->Close();
    delete db_;
    db_ = nullptr;
    if (asyncThreadPool_) {
        asyncThreadPool_.reset();
    }
}

RocksdbWriteMode RocksStore::ParseRocksdbWriteMode()
{
    if (FLAGS_rocksdb_write_mode == "async") {
        return RocksdbWriteMode::ASYNC;
    } else if (FLAGS_rocksdb_write_mode == "sync") {
        return RocksdbWriteMode::SYNC;
    } else if (FLAGS_rocksdb_write_mode == "none") {
        return RocksdbWriteMode::NONE;
    } else {
        LOG(INFO) << FormatString("Rocksdb write mode is : % s, will use none mode instead.", FLAGS_rocksdb_write_mode);
        return RocksdbWriteMode::NONE;
    }
}

bool RocksStore::IsClusterInfoTable(const std::string &tableName)
{
    auto it = std::find(clusterInfoTable_.begin(), clusterInfoTable_.end(), tableName);
    if (it != clusterInfoTable_.end()) {
        return true;
    }
    return false;
}

void RocksStore::InitializeAsyncThreadPool(size_t threadCount)
{
    if (!asyncThreadPool_ && mode_ == RocksdbWriteMode::ASYNC) {
        asyncThreadPool_ = std::make_unique<OrderedThreadPool>(threadCount);
        LOG(INFO) << "Init rocksdb async thread pool.";
    }
}

Status RocksStore::CreateTable(const std::string &tableName, const rocksdb::ColumnFamilyOptions &tableOptions,
                               rocksdb::ColumnFamilyHandle **tableHandle)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::ColumnFamilyHandle *cf = nullptr;
    rocksdb::Status rc;
    auto item = tables_.find(tableName);
    // If the table already exists, it's no op. Return OK.
    if (item == tables_.end()) {
        // Create the table and add it to the list
        CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
        rc = db_->CreateColumnFamily(tableOptions, tableName, &cf);
        if (!rc.ok()) {
            // Log an error
            RETURN_STATUS_LOG_ERROR(StatusCode::K_KVSTORE_ERROR,
                                    "Cannot create table: " + tableName + " Error: " + rc.ToString());
        } else {
            tables_.emplace(tableName, cf);
            if (tableHandle != nullptr) {
                *tableHandle = cf;
            }
        }
    }
    return Status::OK();
}

Status RocksStore::DropTable(const std::string &tableName)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::Status rc;
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;

    auto item = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(item != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");

    tableHandle = item->second;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rc = db_->DropColumnFamily(tableHandle);
    CHECK_FAIL_RETURN_STATUS(rc.ok(), StatusCode::K_KVSTORE_ERROR,
                             "Error dropping the table " + tableName + " Error: " + rc.ToString());
    rc = db_->DestroyColumnFamilyHandle(tableHandle);
    CHECK_FAIL_RETURN_STATUS(rc.ok(), StatusCode::K_KVSTORE_ERROR,
                             "Error destroy the table " + tableName + " Error: " + rc.ToString());
    tables_.erase(item);

    return Status::OK();
}

Status RocksStore::Put(const std::string &tableName, const std::string &key, const std::string &value)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::Status rc;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;
#ifdef WITH_TESTS
    INJECT_POINT("master.rocksdb.put");
#endif
    PerfPoint point(PerfKey::ROCKSDB_PUT);
    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    tableHandle = iter->second;
    Timer timer;
    if (mode_ == RocksdbWriteMode::SYNC) {
        rc = Put(tableHandle, key, value, FLAGS_rocksdb_sync_write);
        masterOperationTimeCost.Append("RocksDB Put", timer.ElapsedMilliSecond());
        return CheckAndRemoveDbPath(rc);
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(key, [this, tableHandle, key, value]() {
            rocksdb::Status rc = Put(tableHandle, key, value, FLAGS_rocksdb_sync_write);
            if (!rc.ok()) {
                LOG(ERROR) << FormatString("Async Put key %s failed: %s", key, rc.getState());
            }
        });
        return Status::OK();
    }
    masterOperationTimeCost.Append("RocksDB Put", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status RocksStore::BatchPut(const std::string &tableName, std::unordered_map<std::string, std::string> &metaInfos)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::Status rc;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_RUNTIME_ERROR, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;
    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    tableHandle = iter->second;
    if (mode_ == RocksdbWriteMode::SYNC) {
        rc = BatchPut(metaInfos, tableHandle, FLAGS_rocksdb_sync_write);
        return CheckAndRemoveDbPath(rc);
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(tableName, [this, tableHandle, metaInfos]() {
            rocksdb::Status rc = BatchPut(metaInfos, tableHandle, FLAGS_rocksdb_sync_write);
            if (!rc.ok()) {
                FormatString("Async BatchPut failed: %s", rc.getState());
            }
        });

        return Status::OK();
    }
    return Status::OK();
}

Status RocksStore::BatchDelete(const std::string &tableName, std::unordered_map<std::string, std::string> &metaInfos)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;
    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    tableHandle = iter->second;
    if (mode_ == RocksdbWriteMode::SYNC) {
        rocksdb::Status rc = BatchDelete(metaInfos, tableHandle, FLAGS_rocksdb_sync_write);
        return CheckAndRemoveDbPath(rc);
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(tableName, [this, tableHandle, metaInfos]() {
            sleep(10);
            rocksdb::Status rc = BatchDelete(metaInfos, tableHandle, FLAGS_rocksdb_sync_write);
            if (!rc.ok()) {
                LOG(ERROR) << FormatString("Async BatchDelete failed: %s", rc.getState());
            }
        });
        return Status::OK();
    }
    return Status::OK();
}

Status RocksStore::CheckAndRemoveDbPath(rocksdb::Status rc)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    if (rc != rocksdb::Status::OK()) {
        const size_t noFind = -1;
        std::string rcMsg = rc.ToString();
        auto pos = rcMsg.find(dbPath_);
        if (pos != noFind) {
            rcMsg.erase(pos, dbPath_.size());
            rcMsg.insert(pos, "rocksdbpath");
        }
        std::stringstream errMsg;
        errMsg << "Cannot add a new key-value: " << rcMsg;
        return Status(StatusCode::K_KVSTORE_ERROR, errMsg.str());
    }
    return Status::OK();
}

Status RocksStore::ListTables(std::vector<std::string> &tables)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    rocksdb::Status rc;
    rocksdb::Options options;
    rc = rocksdb::DB::ListColumnFamilies(options, dbPath_, &tables);
    if (!rc.ok()) {
        // Set the database handle to a null pointer.
        db_ = nullptr;
        RETURN_STATUS_LOG_ERROR(StatusCode::K_KVSTORE_ERROR, "Error when listing table names: " + rc.ToString());
    }
    return Status::OK();
}

Status RocksStore::Get(const std::string &tableName, const std::string &key, std::string &value)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_RUNTIME_ERROR, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;

    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    PerfPoint point(PerfKey::ROCKSDB_GET);
    tableHandle = iter->second;
    Timer timer;
    rocksdb::Status rc;
    if (mode_ == RocksdbWriteMode::SYNC) {
        rc = Get(tableHandle, key, value);
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(key, [this, key, tableHandle, &rc, &value]() {
            rc = Get(tableHandle, key, value);
            if (!rc.ok()) {
                LOG(ERROR) << FormatString("Async Get key %s failed: %s", key, rc.getState());
            }
        });
        future.wait();
    }
    masterOperationTimeCost.Append("RocksDB Get", timer.ElapsedMilliSecond());
    if (!rc.ok()) {
        if (rc == rocksdb::Status::NotFound()) {
            RETURN_STATUS(StatusCode::K_NOT_FOUND, "Key not found " + key);
        } else {
            RETURN_STATUS(StatusCode::K_KVSTORE_ERROR, "Internal error: " + std::string(rc.getState()));
        }
    }
    return Status::OK();
}

Status RocksStore::GetAll(const std::string &tableName, std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    outKeyValues.clear();
    rocksdb::Status rc;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;

    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    PerfPoint point(PerfKey::ROCKSDB_GET_ALL);
    tableHandle = iter->second;
    auto readOptions = rocksdb::ReadOptions();
    Timer timer;
    if (mode_ == RocksdbWriteMode::SYNC) {
        std::unique_ptr<rocksdb::Iterator> iter2(db_->NewIterator(readOptions, tableHandle));
        iter2->SeekToFirst();
        while (iter2->Valid()) {
            outKeyValues.emplace_back(std::make_pair(iter2->key().ToString(), iter2->value().ToString()));
            iter2->Next();
        }
        masterOperationTimeCost.Append("RocksDB GetAll", timer.ElapsedMilliSecond());
        return Status::OK();
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(tableName, [this, readOptions, tableHandle, &outKeyValues]() {
            std::unique_ptr<rocksdb::Iterator> iter2(db_->NewIterator(readOptions, tableHandle));
            iter2->SeekToFirst();
            while (iter2->Valid()) {
                outKeyValues.emplace_back(std::make_pair(iter2->key().ToString(), iter2->value().ToString()));
                iter2->Next();
            }
        });
        future.wait();
        return Status::OK();
    }
    masterOperationTimeCost.Append("RocksDB GetAll", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status RocksStore::PrefixSearch(const std::string &tableName, const std::string &prefixKey,
                                std::vector<std::pair<std::string, std::string>> &outKeyValues)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::Status rc;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;

    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    PerfPoint point(PerfKey::ROCKSDB_PREFIX_SEARCH);
    tableHandle = iter->second;
    rocksdb::ColumnFamilyDescriptor cfDesc;
    rc = tableHandle->GetDescriptor(&cfDesc);
    CHECK_FAIL_RETURN_STATUS(rc.ok(), StatusCode::K_KVSTORE_ERROR,
                             "Internal error while getting table descriptor: " + rc.ToString());
    rocksdb::ColumnFamilyOptions const cfOptions = cfDesc.options;
    std::shared_ptr<const rocksdb::SliceTransform> prefixExtractor = cfOptions.prefix_extractor;
    CHECK_FAIL_RETURN_STATUS(prefixExtractor != nullptr, StatusCode::K_KVSTORE_ERROR,
                             "Table " + tableName + " was not created with prefix search option");
    const bool result = prefixExtractor->SameResultWhenAppended(prefixKey);
    CHECK_FAIL_RETURN_STATUS(result, StatusCode::K_KVSTORE_ERROR,
                             "Table " + tableName + " was created with a prefix search pattern longer than "
                                 + "the input pattern \"" + prefixKey + "\"");
    if (mode_ == RocksdbWriteMode::SYNC) {
        PrefixSearch(tableHandle, prefixKey, outKeyValues);
        return Status::OK();
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(tableName, [this, prefixKey, tableHandle, &outKeyValues]() {
            PrefixSearch(tableHandle, prefixKey, outKeyValues);
        });
        future.wait();
        return Status::OK();
    }
    return Status::OK();
}

Status RocksStore::Delete(const std::string &tableName, const std::string &key)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::Status rc;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;
#ifdef WITH_TESTS
    INJECT_POINT("master.rocksdb.delete");
#endif
    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    tableHandle = iter->second;
    Timer timer;
    PerfPoint point(PerfKey::ROCKSDB_DELETE);
    if (mode_ == RocksdbWriteMode::SYNC) {
        rc = Delete(tableHandle, key, FLAGS_rocksdb_sync_write);
        masterOperationTimeCost.Append("RocksDB Delete", timer.ElapsedMilliSecond());
        // Deleting a key that does not exist in the database will NOT yield an error.
        return CheckAndRemoveDbPath(rc);
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(key, [this, tableHandle, key]() {
            rocksdb::Status rc = Delete(tableHandle, key, FLAGS_rocksdb_sync_write);
            if (!rc.ok()) {
                LOG(ERROR) << FormatString("Async Delete key %s failed: %s", key, rc.getState());
            }
        });
        return Status::OK();
    }
    masterOperationTimeCost.Append("RocksDB Delete", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status RocksStore::PrefixDelete(const std::string &tableName, const std::string &prefixKey)
{
    RETURN_OK_IF_TRUE(disableRocksDB);
    RETURN_OK_IF_TRUE(mode_ == RocksdbWriteMode::NONE);
    rocksdb::Status rc;
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    rocksdb::ColumnFamilyHandle *tableHandle = nullptr;

    auto iter = tables_.find(tableName);
    CHECK_FAIL_RETURN_STATUS(iter != tables_.end(), StatusCode::K_NOT_FOUND, "Table " + tableName + " does not exist");
    tableHandle = iter->second;
    PerfPoint point(PerfKey::ROCKSDB_PREFIX_DELETE);
    std::string endKey = StringPlusOne(prefixKey);
    rocksdb::WriteOptions options;
    options.sync = FLAGS_rocksdb_sync_write;
    Timer timer;
    if (mode_ == RocksdbWriteMode::SYNC) {
        rc = db_->DeleteRange(options, tableHandle, rocksdb::Slice(prefixKey), rocksdb::Slice(endKey));
        masterOperationTimeCost.Append("RocksDB DeleteRange", timer.ElapsedMilliSecond());
        if (rc != rocksdb::Status::OK()) {
            RETURN_STATUS(StatusCode::K_KVSTORE_ERROR,
                          "Cannot delete prefix key: " + prefixKey + " Error: " + rc.ToString());
        }
        return Status::OK();
    } else if (mode_ == RocksdbWriteMode::ASYNC) {
        auto future = asyncThreadPool_->Submit(tableName, [this, tableHandle, options, prefixKey, endKey]() {
            rocksdb::Status rc =
                db_->DeleteRange(options, tableHandle, rocksdb::Slice(prefixKey), rocksdb::Slice(endKey));
            if (!rc.ok()) {
                LOG(ERROR) << FormatString("Async PrefixDelete prefixKey %s to endKey %s failed: %s", prefixKey, endKey,
                                           rc.getState());
            }
        });
        return Status::OK();
    }
    masterOperationTimeCost.Append("RocksDB DeleteRange", timer.ElapsedMilliSecond());
    return Status::OK();
}
}  // namespace datasystem
