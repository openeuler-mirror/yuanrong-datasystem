/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Data system state cache client management.
 */

#ifndef DATASYSTEM_EMB_CLIENT_H
#define DATASYSTEM_EMB_CLIENT_H

#include <memory>
#include <vector>
#include <optional>
#include <string>

#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/connection.h"
#include "datasystem/kv_client.h"

namespace datasystem {
    class KVClient;
    namespace emb_cache {
    class EmbClientImpl;
    class EmbTableBucket;
    class EmbTableMeta;
    struct TableMetaInfo;
    }  // namespace emb_cache
}  // namespace datasystem

namespace datasystem {

static constexpr uint64_t kDefaultBucketNum = 1024;

enum class HashFunction {
    XXHASH64 = 0,        // 性能最高，推荐默认使用
    MURMURHASH3_X64 = 1,  // 经典算法，适合通用场景
    CITYHASH64 = 2        // 适合长字符串 Key
};

using HashFuncPtr = uint64_t (*)(uint64_t);

struct InitParams {
    std::string tableKey;
    int tableIndex;
    std::string tableName;
    uint64_t dimSize;
    uint64_t tableCapacity;
    uint64_t bucketNum = kDefaultBucketNum;
    HashFunction hashFunction = HashFunction::XXHASH64;
    uint64_t bucketCapacity;

    InitParams() = default;
    explicit InitParams(const emb_cache::TableMetaInfo& info);
};

class __attribute((visibility("default"))) EmbClient : public std::enable_shared_from_this<EmbClient>{
    friend class datasystem::emb_cache::EmbClientImpl;
public:
    /// \brief Construct EmbClient.
    ///
    /// \param[in] connectOptions The connection options.
    explicit EmbClient(const ConnectOptions &connectOptions = {});

    ~EmbClient();

    // 禁用拷贝，防止多个 Client 抢夺同一个 Impl
    // EmbClient(const Client&) = delete;
    // EmbClient& operator=(const Client&) = delete;

    /// \brief get client according to the tableIndex
    ///
    /// \param[in] tableIndex table index
    /// \param[in] embClient The client corresponding to the tableIndex
    ///
    /// \return K_OK on success; the error code otherwise.
    // static Status GetClient(std::string tableKey, std::shared_ptr<EmbClient> &embClient);

    /// \brief Shutdown the embClient.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status ShutDown();

    /// \brief Init KVClient object.
    ///
    /// \param[in] params Table Structure Information.
    ///
    /// \return Status of the call.
    Status Init(const InitParams &params, const std::string &etcdserver, const std::string &localPath);

    /// \brief Insert pairs of keys and values into dataSystem
    ///
    /// \param[in] keys keys vector
    /// \param[in] values values vector
    ///
    /// \return Status of the call.
    Status Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values);

    /// \brief Notify the ShareObject that all the kvs of the table has been inserted
    ///        and every ShareObjectMap can start building index;
    ///
    /// \param[in] keys keys to find the corresponding values
    /// \param[in]
    ///
    /// \return Status of the call.
    Status BuildIndex();

    /// \brief Get the values for the keys and put them into the buffer
    ///
    /// \param[in] keys keys to find the corresponding values
    /// \param[out] memory of values
    ///
    /// \return Status of the call.
    Status Find(const std::vector<uint64_t> &keys, std::vector<StringView> &buffer);
    // Status Find(const std::vector<uint32_t> &keys, Optional<Buffer> &buffer);

    // 不同格式的文件解析方式也不同，所以需要用户告诉文件格式，传递给prase
    // 一个路径对应一个文件
    Status Load(const std::vector<std::string> embKeyFilesPath, const std::vector<std::string> embValueFilesPath, const std::string fileFormat);

    // todo：重载：文件形式，只有一个路径
    // Status Load(const std::vector<std::string> embFilesPath);

private:
    static inline const char* const DEFAULT_TABLE_KEY = "defaultTableKey";
    std::string tableKey_ = DEFAULT_TABLE_KEY;
    std::shared_ptr<KVClient> kvClientImpl_;
    std::shared_ptr<emb_cache::EmbClientImpl> embClientImpl_;
    // 强引用交给client来管理
    std::shared_ptr<emb_cache::EmbTableBucket> tableBucket_ = nullptr;
    std::shared_ptr<emb_cache::EmbTableMeta> tableMeta_ = nullptr;
};

}  // namespace datasystem

#endif  // DATASYSTEM_EmbClient_EmbClient_H