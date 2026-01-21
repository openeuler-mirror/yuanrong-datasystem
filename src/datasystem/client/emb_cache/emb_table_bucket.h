
#ifndef EMB_TABLE_BUCKET_H
#define EMB_TABLE_BUCKET_H

#include <vector>
#include <string>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <thread>

#include "datasystem/utils/string_view.h"
// 引入 ShareObjectMaster 头文件（根据实际路径调整）
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/emb_service.pb.h"
#include "datasystem/emb_client.h"
#include "datasystem/client/emb_cache/emb_table_meta.h"

namespace datasystem {
    class KVClient; // 假设在 datasystem 下
    namespace master {
        class ShareObjectMaster; // 报错提示它在这里
    }
}
namespace datasystem {
namespace emb_cache {
class EmbTableBucket {

public:
    struct BucketInfo {
        std::string tableKey;
        int tableIndex;
        std::string tableName;
        uint64_t dimSize;
        uint64_t bucketNum;
        uint64_t bucketCapacity;
        HashFuncPtr hashFunction;

        BucketInfo() = default;
        explicit BucketInfo(const TableMetaInfo& tableMetaInfo);
    };
    // 构造函数：新增 ShareObjectMaster 引用（用于发布）
    EmbTableBucket(BucketInfo& bucketInfo, 
                    std::shared_ptr<KVClient> kvClient, 
                    std::shared_ptr<master::ShareObjectMaster> shareObjectMaster);

    std::string CalculateBucketKey(uint64_t key) const;
    
    // 构造发布所需的 CreateBucketPb 元数据
    Status CreateAndSaveBucketMeta(const std::string &bucketKey, const std::string &tableKey, const int32_t tableIndex, 
                                        const std::string &tableName, const int32_t dimSize, const int32_t bucketCapacity);
    Status PublishBucketData(const std::string& bucketKey, 
                                     std::vector<std::pair<uint64_t, std::string>> publishData);
    // 核心插入接口：直接在桶满时发布，无返回值
    Status Insert(const std::vector<uint64_t>& keys,
                const std::vector<StringView>& values);
    std::vector<std::string> GetAllBucketKeys() const;

    uint64_t GetBucketNum() const {
        return bucketInfo_.bucketNum;
    }

private:
    // 嵌套桶结构：保留 ExtractAndClear 用于提取发布数据
    struct Bucket {
        explicit Bucket(size_t capacity) : capacity_(capacity) {
            data_.reserve(capacity);
        }

        std::vector<std::pair<uint64_t, StringView>> data_;
        size_t capacity_;
        mutable std::mutex mtx_;  // 桶内线程安全锁

        void Push(const uint64_t& key, const StringView& value) {
            // std::lock_guard<std::mutex> lock(mtx_);
            data_.emplace_back(key, value);
        }

        bool IsFull() const {
            // std::lock_guard<std::mutex> lock(mtx_);
            return data_.size() >= capacity_;
        }

        // 提取数据并清空桶（发布专用）
        std::vector<std::pair<uint64_t, std::string>> ExtractAndClear() {
            std::vector<std::pair<uint64_t, std::string>> result;
            result.reserve(data_.size());
            for (const auto& item : data_) {
                // 这里执行了真正的深拷贝：StringView -> std::string
                result.emplace_back(item.first, std::string(item.second.data(), item.second.size()));
            }
            data_.clear();
            return result;
        }  
    };

    // 计算分桶key
    std::future<Status> FlushBucket(const std::string& bucketKey, Bucket& bucket);

    uint64_t GetBucketIndex(uint64_t key) const;
    std::string BucketIndex2Key(uint64_t bucketIndex) const;

    // 成员变量：新增 ShareObjectMaster 引用
    BucketInfo bucketInfo_;
    std::shared_ptr<KVClient> kvClient_; 
    std::shared_ptr<master::ShareObjectMaster> shareObjectMaster_;
    
    // HashFuncPtr hashFunction = nullptr;
    std::string currentBucketKey_;
    std::unordered_map<std::string, Bucket> buckets_;
    mutable std::mutex bucketsMtx_; 

    friend EmbClientImpl;
    std::unique_ptr<ThreadPool> publishThreadPool_;
};

} // namespace emb_cache
} // namespace datasystem

#endif // EMB_TABLE_BUCKET_H