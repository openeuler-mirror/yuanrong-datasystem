/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Local metadata RocksDB helper.
 */

#ifndef DATASYSTEM_COMMON_KVSTORE_REPLICA_H
#define DATASYSTEM_COMMON_KVSTORE_REPLICA_H

#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class Replica {
public:
    Replica() = delete;
    ~Replica() = delete;

    /**
     * @brief Get the instance of rocks store.
     * @param[in] dbPath Directory to store the database.
     * @return The rocks store instance.
     */
    static Status CreateRocksStoreInstance(const std::string &dbPath, std::shared_ptr<RocksStore> &store);

    /**
     * @brief Get the instance of rocks store and create object/stream metadata tables.
     * @param[in] dbPath Directory to store the database.
     * @return The rocks store instance.
     */
    static Status CreateRocksStoreInstanceAndTable(const std::string &dbPath, std::shared_ptr<RocksStore> &store);

    /**
     * @brief Create object cache metadata tables.
     * @param[in] store The rocks store pointer.
     * @return Status of this call.
     */
    static Status CreateOcTable(RocksStore *store);

    /**
     * @brief Create stream cache metadata tables.
     * @param[in] store The rocks store pointer.
     * @return Status of this call.
     */
    static Status CreateScTable(RocksStore *store);

    /**
     * @brief Create table in rocksdb.
     * @param[in] tableName The RocksDB table name to be created.
     * @param[in] store The rocks store pointer.
     * @param[in] isSc Create with stream-cache table options.
     * @param[in] tables The tables currently in rocksdb.
     * @return Status of the call.
     */
    static Status CreateTable(const std::string &tableName, RocksStore *store, bool isSc,
                              std::vector<std::string> &tables);

    /**
     * @brief Remove local object/stream metadata RocksDB directories from filesystem.
     * @param[in] dbRootPath The db root path.
     * @return Status of this call.
     */
    static Status RemoveRocksFromFileSystem(const std::string &dbRootPath);
};
}  // namespace datasystem
#endif
