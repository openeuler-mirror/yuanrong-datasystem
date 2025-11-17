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
#ifndef DATASYSTEM_COMMON_KVSTORE_ROCKS_STORE_H
#define DATASYSTEM_COMMON_KVSTORE_ROCKS_STORE_H

#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <type_traits>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class RocksdbWriteMode { ASYNC, SYNC, NONE };
class RocksStore : public KvStore {
public:
    /**
     * @brief Create a new rocksdb database in the input path.
     * @param[in] dbPath Directory to store the database.
     * @param[in] tableOptions Map of the tables and it's options to open, if create table with prefix search,
     * you must specific ColumnFamilyOptions for this table.
     * @return dbInstance handle to the database, nullptr if an error is raised.
     */
    static std::shared_ptr<RocksStore> GetInstance(
        const std::string &dbPath, const std::unordered_map<std::string, rocksdb::ColumnFamilyOptions> &tableOptions =
                                       std::unordered_map<std::string, rocksdb::ColumnFamilyOptions>());

    ~RocksStore();

    /**
     * @brief Close the database
     */
    void Close() override;

    /**
     * @brief Create a new table (aka column family in RocksDB's term).
     * @param[in] tableName The table name to create.
     * @param[in] tableOptions The option to create RocksDB's column family,
     *   need to set tableOptions.prefix_extractor to support prefix search.
     * @param[out] tableHandle If provided, pointer to the handle to the created table name.
     * @return Status of the call.
     */
    Status CreateTable(const std::string &tableName,
                       const rocksdb::ColumnFamilyOptions &tableOptions = rocksdb::ColumnFamilyOptions(),
                       rocksdb::ColumnFamilyHandle **tableHandle = nullptr);

    /**
     * @brief Drop a table (aka column family in RocksDB's term).
     * @param[in] tableName The table name to create.
     * @return Status of the call.
     */
    Status DropTable(const std::string &tableName) override;

    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @return Status of the call.
     */
    Status Put(const std::string &tableName, const std::string &key, const std::string &value) override;

    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @return Status of the call.
     */
    Status BatchPut(const std::string &tableName, std::unordered_map<std::string, std::string> &metaInfos);

    /**
     * @brief Deleta a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @return Status of the call.
     */
    Status BatchDelete(const std::string &tableName, std::unordered_map<std::string, std::string> &metaInfos);

    /**
     * @brief Put a new key-value into a table (inline, high performance version).
     * @param[in] tableHandle The table handle for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @param[in] sync The sync mode to delete or not.
     * @return Status of the call.
     */
    inline rocksdb::Status Put(rocksdb::ColumnFamilyHandle *tableHandle, const std::string &key,
                               const std::string &value, bool sync = false)
    {
        rocksdb::WriteOptions options;
        options.sync = sync;
        return db_->Put(options, tableHandle, rocksdb::Slice(key), rocksdb::Slice(value));
    }

    /**
     * @brief Put a batch of meta into a table.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @param[in] tableHandle The table handle for the <key,value> to insert.
     * @param[in] sync The sync mode to delete or not.
     * @return Status of the call.
     */
    inline rocksdb::Status BatchPut(const std::unordered_map<std::string, std::string> &metaInfos,
                                    rocksdb::ColumnFamilyHandle *tableHandle, bool sync = false)
    {
        rocksdb::WriteOptions options;
        options.sync = sync;
        rocksdb::WriteBatch batch;
        for (const auto &info : metaInfos) {
            batch.Put(tableHandle, info.first, info.second);
        }
        return db_->Write(options, &batch);
    }

    /**
     * @brief Deleta a a batch of meta from the table.
     * @param[in] metaInfos The metaInfos of a batch of objects to insert.
     * @param[in] tableHandle The table handle for the <key,value> to insert.
     * @param[in] sync The sync mode to delete or not.
     * @return Status of the call.
     */
    inline rocksdb::Status BatchDelete(const std::unordered_map<std::string, std::string> &metaInfos,
                                       rocksdb::ColumnFamilyHandle *tableHandle, bool sync = false)
    {
        rocksdb::WriteOptions options;
        options.sync = sync;
        rocksdb::WriteBatch batch;
        for (const auto &info : metaInfos) {
            batch.Delete(tableHandle, rocksdb::Slice(info.first));
        }
        return db_->Write(options, &batch);
    }

    /**
     * @brief List all the tables.
     * @param[out] tables A vector of all the tables in this database.
     * @return Status of the call.
     */
    Status ListTables(std::vector<std::string> &tables) override;

    /**
     * @brief Get the value of the given key.
     * @param[in] tableName The table to search for the key.
     * @param[in] key The key to search.
     * @param[out] value The value of the given key. If not found, the content is unchanged.
     * @return Status of the call, Status::KVStoreError() if the given key does not exist.
     */
    Status Get(const std::string &tableName, const std::string &key, std::string &value) override;

    /**
     * @brief Check and remove rocks db path in error message.
     * @param[in] rc The rocks status.
     * @return Status of the call.
     */
    Status CheckAndRemoveDbPath(rocksdb::Status rc);

    /**
     * @brief Get the value of the given key (inline, high performance version).
     * @param[in] tableHandle The table handle to search for the key.
     * @param[in] key The key to search.
     * @param[out] value The value of the given key. If not found, the content is unchanged.
     * @return Status of the call.
     */
    inline rocksdb::Status Get(rocksdb::ColumnFamilyHandle *tableHandle, const std::string &key, std::string &value)
    {
        return db_->Get(rocksdb::ReadOptions(), tableHandle, rocksdb::Slice(key), &value);
    }

    /**
     * @brief Get all key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[out] outKeyValues The output key-values in table.
     * @return Status of the call.
     */
    Status GetAll(const std::string &tableName,
                  std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    /**
     * @brief Prefix search in specific table.
     * @param[in] tableName The table where the key to be search.
     * @param[in] prefixKey The prefix key to search.
     * @param[out] outKeyValues Array of the output key-value pairs.
     * @return Status of the call, Status::KVStoreError() if the given prefixKey does not exist.
     */
    Status PrefixSearch(const std::string &tableName, const std::string &prefixKey,
                        std::vector<std::pair<std::string, std::string>> &outKeyValues) override;

    /**
     * @brief Prefix search in specific table (inline, high performance version).
     * @param[in] tableHandle The table handle where the key to be search.
     * @param[in] prefixKey The prefix key to search.
     * @param[out] outKeyValues Array of the output key-value pairs.
     */
    inline void PrefixSearch(rocksdb::ColumnFamilyHandle *tableHandle, const std::string &prefixKey,
                             std::vector<std::pair<std::string, std::string>> &outKeyValues)
    {
        auto readOptions = rocksdb::ReadOptions();
        readOptions.prefix_same_as_start = true;
        auto searchIter = db_->NewIterator(readOptions, tableHandle);
        searchIter->Seek(rocksdb::Slice(prefixKey));
        while (searchIter->Valid()) {
            outKeyValues.emplace_back(searchIter->key().ToString(), searchIter->value().ToString());
            searchIter->Next();
        }
        delete searchIter;
    }

    /**
     * @brief Delete the given key (inline, high performance version).
     * @param[in] tableName The table where the key to be deleted.
     * @param[in] key The key to delete.
     * @return Status of the call, Status::KVStoreError() if the operation fails.
     */
    Status Delete(const std::string &tableName, const std::string &key) override;

    /**
     * @brief Delete some keys by prefix.
     * @param[in] tableName The table where the key to be deleted.
     * @param[in] prefixKey The prefix key to delete.
     * @return Status of the call, Status::KVStoreError() if the operation fails.
     */
    Status PrefixDelete(const std::string &tableName, const std::string &prefixKey);

    /**
     * @brief Delete the given key.
     * @param[in] tableHandle The table handle where the key to be deleted.
     * @param[in] key The key to delete.
     * @param[in] sync The sync mode to delete or not.
     * @return Status of the call.
     */
    inline rocksdb::Status Delete(rocksdb::ColumnFamilyHandle *tableHandle, const std::string &key, bool sync = false)
    {
        rocksdb::WriteOptions options;
        options.sync = sync;
        return db_->Delete(options, tableHandle, rocksdb::Slice(key));
    }

    rocksdb::DB *GetRawDB()
    {
        return db_;
    }

    // No copy allowed.
    RocksStore(const RocksStore &) = delete;
    RocksStore &operator=(const RocksStore &) = delete;
    // No move allowed.
    RocksStore(const RocksStore &&) = delete;
    RocksStore &operator=(const RocksStore &&) = delete;

    bool IsAsyncQueueEmpty()
    {
        if (!asyncThreadPool_) {
            return true;
        }
        return asyncThreadPool_->AreAllQueuesEmpty();
    }

private:
    // Make the constructor private to force the user to call GetInstance to open a read-only RocksDB database.
    RocksStore();

    /**
     * @brief Check whether the table is a cluster table.
     * @param[in] tableName The table need to check.
     * @return True if the table is a cluster table.
     */
    bool IsClusterInfoTable(const std::string &tableName);

    /**
     * @brief Init async thread pool.
     * @param[in] threadCount The thread numer of async thread pool.
     */
    void InitializeAsyncThreadPool(size_t threadCount = 16);

    /**
     * @brief Parse rocksdb write mode.
     * @return RocksdbWriteMode enum.
     */
    RocksdbWriteMode ParseRocksdbWriteMode();

    rocksdb::DB *db_ = nullptr;
    std::string dbPath_;
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle *> tables_;
    static std::mutex lck;
    static bool disableRocksDB;
    std::vector<std::string> clusterInfoTable_ = { CLUSTER_TABLE, HASHRING_TABLE, REPLICA_GROUP_TABLE, HEALTH_TABLE };
    std::unique_ptr<OrderedThreadPool> asyncThreadPool_;
    RocksdbWriteMode mode_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_KVSTORE_ROCKS_STORE_H
