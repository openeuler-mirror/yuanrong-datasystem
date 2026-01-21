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
 * Description: emb client implementation.
 */
#include <memory>         
#include <unordered_map> 
#include <vector>         
#include <string>         

#include "datasystem/utils/status.h"
#include "datasystem/emb_client.h"
#include "datasystem/kv_client.h"
#include "datasystem/client/emb_cache/emb_table_bucket.h"
#include "datasystem/client/emb_cache/emb_table_meta.h"
#include "datasystem/client/emb_cache/share_object_master/share_object_master.h"
#include "datasystem/common/util/thread_pool.h"

// namespace datasystem {
// namespace emb_cache {
//     // 前置声明 EmbTableClient
//     class EmbClient;
// }
// }
namespace datasystem {
namespace emb_cache {

HashFuncPtr GetHashExecutor(HashFunction algo);

class EmbClientImpl {
public:
    explicit EmbClientImpl(const ConnectOptions &connectOptions = {});
    static std::shared_ptr<EmbClientImpl> GetInstance() {
        // 这一行在整个进程生命周期内只执行一次构造
        static std::shared_ptr<EmbClientImpl> instance(new EmbClientImpl());
        return instance;
    }

    /**
     * @brief  Init KVClient object.
     * @param[in] params Table Structure Information.
     * @return K_OK on success; the error code otherwise.
     */
    Status Init(const InitParams &params,
                std::shared_ptr<EmbClient> embClient,
                const std::string &etcdServer, 
                std::shared_ptr<KVClient> kvClient,
                const std::string &localPath);

    /**
     * @brief  get client according to the tableIndex.
     * @param[in] params Table Structure Information.
     * @return K_OK on success; the error code otherwise.
     */
    // Status GetClient(std::string tableKey, std::shared_ptr<EmbClient> &embClient);

    /**
     * @brief  Insert pairs of keys and values into dataSystem.
     * @param[in] keys keys vector.
     * @param[in] values value vector.
     * @return K_OK on success; the error code otherwise.
     */
    Status Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values);

    /**
     * @brief Notify the ShareObject that all the kvs of the table has been inserted
              and every ShareObjectMap can start building index
     * @return K_OK on success; the error code otherwise.
     */
    Status BuildIndex();

    /**
     * @brief  Get the values for the keys and put them into the buffer.
     * @param[in] keys keys vector.
     * @param[in] values value vector.
     * @return K_OK on success; the error code otherwise.
     */

    Status Find(const std::vector<uint64_t> &keys, std::vector<StringView> &buffer);

    /**
     * @brief  load embedding files
     * @param[in] tableKey tableKey
     * @param[in] embFilesPath path of emb files
     * @return K_OK on success; the error code otherwise.
     */
    Status Load(const std::string &tableKey, const std::vector<std::string> embKeyFilesPath, const std::vector<std::string> embValueFilesPath, const std::string fileFormat);


    Status Parse(const std::string tableKey, std::string localKeyFilesPath, std::string localValueFilesPath);

    Status CreateTableMeta(InitParams params, std::shared_ptr<EmbTableMeta> &tableMeta);
    // Status CopyValueToBuffer(const StringView &value, Optional<Buffer> &buffer);

private:
    // 2. 禁用拷贝构造和赋值操作：防止 ClientImpl a = *instance;
    EmbClientImpl(const EmbClientImpl&) = delete;
    EmbClientImpl& operator=(const EmbClientImpl&) = delete;

    // 弱引用，避免循环引用导致键在值也无法释放的问题
    std::shared_ptr<emb_cache::EmbTableBucket> tableBucket_ = nullptr;
    std::shared_ptr<emb_cache::EmbTableMeta> tableMeta_ = nullptr;
    // std::unordered_map<std::string, std::weak_ptr<ShareObjectMaster>> ShareObjectMasters;
    std::shared_ptr<datasystem::master::ShareObjectMaster> master_ = nullptr;
    std::unique_ptr<ThreadPool> queryThreadPool_;
};
}  // namespace emb_cache
}  // namespace datasystem