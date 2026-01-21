/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Defines the worker service processing main class.
 */
#include "datasystem/client/emb_cache/emb_table_bucket.h"

#include <stdexcept>
#include <sstream>

#include "datasystem/utils/string_view.h"
#include "datasystem/kv_client.h"
#include "datasystem/client/emb_cache/share_object_master/share_object_master.h"

class EmbServiceClientImpl;
namespace datasystem {
namespace emb_cache {
HashFuncPtr GetHashExecutor(HashFunction hashFunction);
EmbTableBucket::BucketInfo::BucketInfo(const TableMetaInfo& tableMetaInfo)
    : tableKey(tableMetaInfo.tableKey),
      tableIndex(tableMetaInfo.tableIndex),
      tableName(tableMetaInfo.tableName),
      dimSize(tableMetaInfo.dimSize),
      bucketNum(tableMetaInfo.bucketNum),
      bucketCapacity(tableMetaInfo.bucketCapacity) {
    // 特殊处理 HashFunction (枚举) -> HashFuncPtr (函数指针)
      this->hashFunction = GetHashExecutor(tableMetaInfo.hashFunction);
}

// 构造函数：传入 ShareObjectMaster 引用
EmbTableBucket::EmbTableBucket(BucketInfo& bucketInfo, 
                               std::shared_ptr<KVClient> kvClient, 
                               std::shared_ptr<master::ShareObjectMaster> shareObjectMaster)
    : bucketInfo_(bucketInfo), 
      kvClient_(kvClient),
      shareObjectMaster_(shareObjectMaster)
{
    publishThreadPool_ = std::make_unique<ThreadPool>(4, 4);
    for (int i = 0; i < bucketInfo.bucketNum; ++i) {
        std::string bucketKey = bucketInfo.tableKey + "_" + std::to_string(i);
        
        auto rc = CreateAndSaveBucketMeta(
            bucketKey, 
            bucketInfo.tableKey, 
            bucketInfo.tableIndex, 
            bucketInfo.tableName, 
            bucketInfo.dimSize, 
            bucketInfo.bucketCapacity
        );

        if (!rc.IsOk()) {
            LOG(ERROR) << "Failed to create bucket meta: " << bucketKey;
            throw std::runtime_error("Bucket initialization failed");
        }
    }
}

// 计算分桶key：hash值取模转字符串
uint64_t EmbTableBucket::GetBucketIndex(uint64_t key) const {
    if (bucketInfo_.hashFunction == nullptr) {
        LOG(ERROR) << "Hash function is null for tableKey: " << bucketInfo_.tableKey;
    }
    uint64_t hashVal = bucketInfo_.hashFunction(key);
    return hashVal % bucketInfo_.bucketNum;
}

std::string EmbTableBucket::BucketIndex2Key(uint64_t bucketIndex) const {
    return bucketInfo_.tableKey + "_" + std::to_string(bucketIndex);
}

std::string EmbTableBucket::CalculateBucketKey(uint64_t key) const {
    return BucketIndex2Key(GetBucketIndex(key));
}

// 构造 CreateBucketPb 元数据（根据业务需求填充字段）
Status EmbTableBucket::CreateAndSaveBucketMeta(const std::string &bucketKey, const std::string &tableKey, const int32_t tableIndex, 
                                                const std::string &tableName, const int32_t dimSize, const int32_t bucketCapacity)
{
    BucketMetaPb meta;
    meta.set_bucket_key(bucketKey);
    meta.set_table_key(tableKey);
    meta.set_table_index(tableIndex);
    meta.set_table_name(tableName);
    meta.set_dim_size(dimSize);
    meta.set_bucket_capacity(bucketCapacity);
    std::string serializedStr;
    auto serialized = meta.SerializeToString(&serializedStr);
    if (!serialized) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("Failed to Serialize bucket[%s] meta", bucketKey));
    }
    RETURN_IF_NOT_OK(kvClient_->Set(bucketKey, serializedStr));
    return Status::OK();
}

Status EmbTableBucket::PublishBucketData(const std::string& bucketKey, 
                                     std::vector<std::pair<uint64_t, std::string>> publishData) {
    RETURN_OK_IF_TRUE(publishData.empty());
    const int maxRetries = 5;  // 最大重试次数
    const int retryIntervalMs = 1;  // 重试间隔（毫秒）
    int retryCount = 0;  // 重试计数器
    Status rc;

    std::vector<uint64_t> keys;
    std::vector<StringView> values;
    // 预留空间可以提高性能
    keys.reserve(publishData.size());
    values.reserve(publishData.size());
    for (const auto& [key, val] : publishData) { 
        keys.push_back(key);
        values.emplace_back(val.data(), val.size());
    }

    do {
        if (shareObjectMaster_ == nullptr) {
            LOG(ERROR) << "shareObjectMaster is null";
        }   
        rc = shareObjectMaster_->Publish(bucketKey, keys, values, bucketInfo_.dimSize);
        if (rc.IsOk()) {
            break;
        } else {
            ++retryCount;
            LOG(INFO) << "Attempt: " + std::to_string(retryCount) + ". Publish bucketData failed with rc: " + rc.ToString();
            std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
        }
    } while (rc.IsError() && retryCount < maxRetries);

    if (retryCount == maxRetries) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_IO_ERROR, "Maximum retries exceeded. Failed to perform publish bucketData.");
    }

    if (retryCount > 0) {
        LOG(INFO) << "Publish bucketData succeeded after " << retryCount << " retries.";
    }
    return Status::OK();
}

std::future<Status> EmbTableBucket::FlushBucket(const std::string& bucketKey, Bucket& bucket) {

    LOG(INFO) << "[Flush Bucket] BucketKey: " << bucketKey 
              << ", ElementCount: " << bucket.data_.size();

    // 1. 提取数据并清空
    auto publishData = bucket.ExtractAndClear();
    
    // 验证清空情况
    if (!bucket.data_.empty()) {
        LOG(ERROR) << "Critical: Bucket data not empty after clear! Size: " << bucket.data_.size();
    }

    // 2. 发布数据
    return publishThreadPool_->Submit([this, bucketKey, data = std::move(publishData)]() mutable {
        // 在后台线程执行真正的 IO
        return this->PublishBucketData(bucketKey, std::move(data)); 
    });

}

Status EmbTableBucket::Insert(const std::vector<uint64_t> &keys, const std::vector<StringView> &values) {
    /*
    Todo:
    1、对每个key进行处理，根据当前的hashfuc计算出指定的bucketkey，并插入到对应的bucket中，bucket是一个私有结构体
    2、检查当前的桶内元素数量是否已达到阈值：
        1）没达到阈值，继续插入
        2）达到阈值，调用CreateAndSaveBucketMeta函数创建出BucketMeta
        3) 调用PublishBucket函数发布桶数据，并且要清理桶数据
    3、当所有keys处理完毕，检查一下currentBucketKey_是否还有数据，如果有继续发布
    */
    // 1. 用于存储所有异步任务的句柄
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(keys.size() == values.size(), K_RUNTIME_ERROR,
                                        "keys size not equal values size");
    std::vector<std::future<Status>> futures;

    for (size_t i = 0; i < keys.size(); ++i) {
        std::string bucketKey = CalculateBucketKey(keys[i]);
        auto [bucketIt, _] = buckets_.try_emplace(bucketKey, bucketInfo_.bucketCapacity);
        Bucket &bucket = bucketIt->second;

        bucket.Push(keys[i], values[i]);

        if (bucket.IsFull()) {
            // 提交任务并将 future 存入 vector
            futures.emplace_back(FlushBucket(bucketKey, bucket));
        }
    }

    // 处理残留数据
    for (auto& [bucketKey, bucket] : buckets_) {
        if (!bucket.data_.empty()) {
            futures.emplace_back(FlushBucket(bucketKey, bucket));
        }
    }

    // 2. 状态同步阶段：确保所有任务成功
    for (auto& future: futures) {
        RETURN_IF_NOT_OK(future.get());
    }

    return Status::OK();
}

std::vector<std::string> EmbTableBucket::GetAllBucketKeys() const {
    std::lock_guard<std::mutex> lock(bucketsMtx_);
    std::vector<std::string> keys;
    keys.reserve(buckets_.size());  // 预分配内存

    for (const auto& pair : buckets_) {
        keys.push_back(pair.first);
    }
    return keys;
}

}  // namespace emb_cache
}  // namespace datasystem