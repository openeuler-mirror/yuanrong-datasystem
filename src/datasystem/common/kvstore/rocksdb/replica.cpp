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
#include "datasystem/common/kvstore/rocksdb/replica.h"

#include <algorithm>
#include <unordered_map>

#include "rocksdb/options.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/log/log.h"
#include "datasystem/master/stream_cache/store/stream_transform.h"

namespace datasystem {
namespace {
const std::string STREAM_META_NAME = "stream_meta_data";
const std::string OBJECT_META_NAME = "object_metadata";

std::unordered_map<std::string, rocksdb::ColumnFamilyOptions> GetTableOptions()
{
    auto tableList = { STREAM_TABLE_NAME, PUB_TABLE_NAME, SUB_TABLE_NAME };
    rocksdb::ColumnFamilyOptions prefixOption;
    prefixOption.prefix_extractor = std::make_shared<master::stream_cache::StreamTransform>();
    std::unordered_map<std::string, rocksdb::ColumnFamilyOptions> tableOptions;
    for (auto table : tableList) {
        tableOptions[table] = prefixOption;
    }
    return tableOptions;
}
}  // namespace

Status Replica::CreateOcTable(RocksStore *store)
{
    std::vector<std::string> tables;
    RETURN_IF_NOT_OK(store->ListTables(tables));
    VLOG(1) << "Existing tables in rocksdb: " << VectorToString(tables);
    RETURN_IF_NOT_OK(CreateTable(META_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(LOCATION_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(NESTED_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(NESTED_COUNT_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(ASYNC_WORKER_OP_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(GLOBAL_REF_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(GLOBAL_CACHE_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(REMOTE_CLIENT_OBJ_REF_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(REMOTE_CLIENT_REF_TABLE, store, false, tables));
    RETURN_IF_NOT_OK(CreateTable(HEALTH_TABLE, store, false, tables));
    LOG(INFO) << FormatString("CreateOcTable success.");
    return Status::OK();
}

Status Replica::CreateScTable(RocksStore *store)
{
    std::vector<std::string> tables;
    RETURN_IF_NOT_OK(store->ListTables(tables));
    VLOG(1) << "Existing tables in rocksdb: " << VectorToString(tables);
    RETURN_IF_NOT_OK(CreateTable(STREAM_TABLE_NAME, store, true, tables));
    RETURN_IF_NOT_OK(CreateTable(PUB_TABLE_NAME, store, true, tables));
    RETURN_IF_NOT_OK(CreateTable(SUB_TABLE_NAME, store, true, tables));
    RETURN_IF_NOT_OK(CreateTable(NOTIFY_PUB_TABLE_NAME, store, true, tables));
    RETURN_IF_NOT_OK(CreateTable(NOTIFY_SUB_TABLE_NAME, store, true, tables));
    RETURN_IF_NOT_OK(CreateTable(STREAM_CON_CNT_TABLE_NAME, store, true, tables));
    RETURN_IF_NOT_OK(CreateTable(STREAM_PRODUCER_COUNT, store, true, tables));
    LOG(INFO) << FormatString("CreateScTable success.");
    return Status::OK();
}

Status Replica::CreateTable(const std::string &tableName, RocksStore *store, bool isSc,
                            std::vector<std::string> &tables)
{
    VLOG(1) << "tableName:" << tableName;
    bool exits = (std::find(tables.begin(), tables.end(), tableName) != tables.end());
    if (!exits) {
        tables.emplace_back(tableName);
        auto options = rocksdb::ColumnFamilyOptions();
        if (isSc) {
            options.prefix_extractor = std::make_shared<master::stream_cache::StreamTransform>();
        }
        RETURN_IF_NOT_OK(store->CreateTable(tableName, options));
        LOG(INFO) << FormatString("Create table { %s } successfully.", tableName);
    }
    return Status::OK();
}

Status Replica::CreateRocksStoreInstance(const std::string &dbPath, std::shared_ptr<RocksStore> &store)
{
    auto options = GetTableOptions();
    store = RocksStore::GetInstance(dbPath, options);
    CHECK_FAIL_RETURN_STATUS(store != nullptr, K_KVSTORE_ERROR, "Cannot open the key/value store");
    return Status::OK();
}

Status Replica::CreateRocksStoreInstanceAndTable(const std::string &dbPath, std::shared_ptr<RocksStore> &store)
{
    RETURN_IF_NOT_OK(CreateRocksStoreInstance(dbPath, store));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateOcTable(store.get()), "Create object metadata table failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateScTable(store.get()), "Create stream metadata table failed");
    return Status::OK();
}

Status Replica::RemoveRocksFromFileSystem(const std::string &dbRootPath)
{
    std::string objectPath = dbRootPath + "/" + OBJECT_META_NAME;
    std::string streamPath = dbRootPath + "/" + STREAM_META_NAME;
    LOG(INFO) << "try remove path " << objectPath << " and " << streamPath;
    RETURN_IF_NOT_OK(RemoveAll(objectPath));
    RETURN_IF_NOT_OK(RemoveAll(streamPath));
    return Status::OK();
}
}  // namespace datasystem
