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
#ifndef DATASYSTEM_COMMON_KVSTORE_KV_STORE_H
#define DATASYSTEM_COMMON_KVSTORE_KV_STORE_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class KvStore {
public:
    virtual ~KvStore() = default;

    /**
     * @brief Close the database.
     */
    virtual void Close() = 0;

    /**
     * @brief Drop a table (aka column family in RocksDB's term).
     * @param[in] tableName The table name to create.
     * @return Status of the call.
     */
    virtual Status DropTable(const std::string &tableName) = 0;

    /**
     * @brief Put a new key-value into a table.
     * @param[in] tableName The table name for the <key,value> to insert.
     * @param[in] key The key to add to the table.
     * @param[in] value The value to add to the table.
     * @return Status of the call.
     */
    virtual Status Put(const std::string &tableName, const std::string &key, const std::string &value) = 0;

    /**
     * @brief List all the tables.
     * @param[out] tables A vector of all the tables in this database.
     * @return Status of the call.
     */
    virtual Status ListTables(std::vector<std::string> &tables) = 0;

    /**
     * @brief Get the value of the given key.
     * @param[in] tableName The table to search for the key.
     * @param[in] key The key to search.
     * @param[out] value The value of the given key, if not found, the content is unchanged.
     * @return Status of the call, Status::KVStoreError() if the given key does not exist.
     */
    virtual Status Get(const std::string &tableName, const std::string &key, std::string &value) = 0;

    /**
     * @brief Get all key-values from specific table.
     * @param[in] tableName The table to get key-values.
     * @param[out] outKeyValues The output key-values in table.
     * @return Status of the call.
     */
    virtual Status GetAll(const std::string &tableName,
                          std::vector<std::pair<std::string, std::string>> &outKeyValues) = 0;

    /**
     * @brief Prefix search in specific table.
     * @param[in] tableName The table where the key to be search.
     * @param[in] prefixKey The prefix key to search.
     * @param[out] outKeyValues Array of the output key-value pairs.
     * @return Status of the call, Status::KVStoreError() if the given prefixKey does not exist.
     */
    virtual Status PrefixSearch(const std::string &tableName, const std::string &prefixKey,
                                std::vector<std::pair<std::string, std::string>> &outKeyValues) = 0;

    /**
     * @brief Delete the given key (inline, high performance version).
     * @param[in] tableName The table where the key to be deleted.
     * @param[in] key The key to delete.
     * @return Status of the call, Status::KVStoreError() if the operation fails.
     */
    virtual Status Delete(const std::string &tableName, const std::string &key) = 0;

    // No copy allowed.
    KvStore(const KvStore &) = delete;
    KvStore &operator=(const KvStore &) = delete;
    // No move allowed.
    KvStore(const KvStore &&) = delete;
    KvStore &operator=(const KvStore &&) = delete;

protected:
    // Make the constructor private to force the user to call GetInstance to open a kv store.
    KvStore() = default;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_KVSTORE_KV_STORE_H
