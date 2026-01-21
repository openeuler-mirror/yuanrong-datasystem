/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "se");
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
 * Description: 定义了工作服务处理主类。
 */
#include "datasystem/client/emb_cache/emb_table_meta.h"

// 仅在源文件中包含具体头文件，避免头文件循环依赖
// #include "datasystem/emb_cache/emb_client.h"
#include "datasystem/kv_client.h"
namespace emb_cache {
class EmbServiceClientImpl;
} 
namespace datasystem {
namespace emb_cache {

EmbTableMeta::EmbTableMeta(std::shared_ptr<KVClient> kvClient) : kvClient_(kvClient) {}

// ========== 创建表元数据实现 ==========
Status EmbTableMeta::CreateTableMeta(const InitParams& params) {
    // 创建TableMetaPb对象
    TableMetaPb metaPb;
    metaPb.set_table_key(params.tableKey);
    metaPb.set_table_index(params.tableIndex);
    metaPb.set_table_name(params.tableName);
    metaPb.set_dim_size(params.dimSize);
    metaPb.set_table_capacity(params.tableCapacity);
    metaPb.set_bucket_num(params.bucketNum);
    metaPb.set_hashfunction(static_cast<::datasystem::HashFunctionPb>(params.hashFunction));
    metaPb.set_bucket_capacity(params.bucketCapacity);

    // 序列化
    std::string serializedStr;
    auto serialized = metaPb.SerializeToString(&serializedStr);
    if (!serialized) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("Failed to Serialize table[%s] meta", params.tableKey));
    }
    RETURN_IF_NOT_OK(kvClient_->Set(params.tableKey, serializedStr));

    // 更新tableMeta_成员变量
    tableMetaInfo_.tableKey = params.tableKey;
    tableMetaInfo_.tableIndex = params.tableIndex;
    tableMetaInfo_.tableName = params.tableName;
    tableMetaInfo_.dimSize = params.dimSize;
    tableMetaInfo_.tableCapacity = params.tableCapacity;
    tableMetaInfo_.bucketNum = params.bucketNum;
    tableMetaInfo_.hashFunction = params.hashFunction;
    tableMetaInfo_.bucketCapacity = params.bucketCapacity;

    return Status::OK();
}

// ========== 查询表元数据实现 ==========
Status EmbTableMeta::QueryTableMetaInfo(const std::string& tableKey, TableMetaInfo& meta) {
    // 如果tableMeta_已经有数据，直接返回
    if (!tableMetaInfo_.tableKey.empty() || !tableMetaInfo_.tableName.empty()) {
        meta = tableMetaInfo_;
        return Status::OK();
    }

    // 如果没有数据，从KV客户端获取
    std::string serializedData;
    if (kvClient_ == nullptr) {
        LOG(ERROR) << "kvClient is null";
    } 
    Status rc = kvClient_->Get(tableKey, serializedData);
    if (rc.GetCode() == StatusCode::K_NOT_FOUND) {
        std::string err = "Table meta not found, tableKey: " + tableMetaInfo_.tableKey;
        LOG(ERROR) << err;
        RETURN_STATUS(K_NOT_FOUND, err);
    }

    // 反序列化
    TableMetaPb metaPb;
    if (!metaPb.ParseFromString(serializedData)) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Parse to TableMetaPb failed");
    }

    // 更新tableMeta_成员变量
    tableMetaInfo_.tableKey = metaPb.table_key();
    tableMetaInfo_.tableIndex = metaPb.table_index();
    tableMetaInfo_.tableName = metaPb.table_name();
    tableMetaInfo_.dimSize = metaPb.dim_size();
    tableMetaInfo_.tableCapacity = metaPb.table_capacity();
    tableMetaInfo_.bucketNum = metaPb.bucket_num();
    tableMetaInfo_.hashFunction = static_cast<datasystem::HashFunction>(metaPb.hashfunction());
    tableMetaInfo_.bucketCapacity = metaPb.bucket_capacity();

    // 更新返回的meta
    meta = tableMetaInfo_;
    return Status::OK();
}

} // namespace emb_cache
} // namespace datasystem