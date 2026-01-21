/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Lic under the Apache License, Version 2.0 (the "License");
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
#ifndef EMB_TABLE_META_H
#define EMB_TABLE_META_H

#include <string>
#include <utility>

#include "datasystem/utils/status.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/emb_service.pb.h"
#include "datasystem/emb_client.h"

namespace datasystem {
    class KVClient; // 假设在 datasystem 下
}
namespace datasystem {
namespace emb_cache {
struct TableMetaInfo {
    std::string tableKey;
    int tableIndex;
    std::string tableName;
    uint64_t dimSize;
    uint64_t tableCapacity;
    uint64_t bucketNum;
    HashFunction hashFunction;
    uint64_t bucketCapacity;
};

class EmbTableMeta {
public:
    // ========== 构造函数：无参数，符合需求 ==========
    EmbTableMeta() = default;
    EmbTableMeta(std::shared_ptr<KVClient> kvClient);

    // ========== 核心结构体：创建表元数据的参数封装 ==========
    // 聚合所有必要参数，搭配默认值，提升易用性
    // ========== 核心方法1：创建并存储表元数据 ==========
    // 入参：封装后的参数结构体；功能：构造PB→序列化→KV存储
    Status CreateTableMeta(const InitParams& params);

    // ========== 核心方法2：查询表元数据 ==========
    // 入参：tableIndex（查询键）；出参：meta（反序列化后的PB对象）
    // 返回：查询状态（成功/失败原因）
    Status QueryTableMetaInfo(const std::string& tableKey, TableMetaInfo& meta);

private:
    // 当前实例的表元数据
    TableMetaInfo tableMetaInfo_;
    // KV客户端
    std::shared_ptr<KVClient> kvClient_;
};

} // namespace emb_cache
} // namespace datasystem
#endif // EMB_TABLE_META_H