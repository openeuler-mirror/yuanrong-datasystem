#pragma once

#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <atomic>

#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/share_object.h"
#include "datasystem/kv_client.h"

namespace datasystem {

class ShareMapObject;

class ShareVectorObject {
public:
    ShareVectorObject(const std::string &key, std::shared_ptr<KVClient> kvClient);

    ShareVectorObject(const std::string &key, uint64_t dimSize, std::shared_ptr<KVClient> kvClient,
                      uint64_t shareObjectSize = SHARE_OBJECT_SIZE, uint64_t vectorCapacity = 0);

    Status Reserve(uint64_t vectorCapacity, uint64_t oldCapacity);

    Status Set(uint64_t index, const void *value);

    Status Set(uint64_t index, uint64_t value);

    Status Lookup(uint64_t index, StringView &buffer) const;

    Status RestoreShareObject(uint64_t dimSize_, uint64_t dataNum,
                              uint64_t shareObjectSize_ = SHARE_OBJECT_SIZE);

    Status Publish();

    Status ExportToVector(std::vector<uint64_t> &buffer, size_t size);

    const std::string &GetKey() const;

private:
    std::string Index2ShareObjectName(size_t index) const;

    std::pair<size_t, size_t> IndexOffset(size_t index) const;

    std::string GetMetaKey() const
    {
        return key_ + "-meta";
    }

    std::string SerializeMeta() const;
    Status DeserializeMeta(const std::string &buffer);
    Status QueryMeta(std::string &buffer) const;
    Status UpdateMeta(const std::string &buffer) const;

    uint64_t ShareBufferNum(uint64_t dataNum) const
    {
        if (dataNum == 0)
            return 0;
        return (dataNum - 1) / objectCapacity_ + 1;
    }

    static constexpr uint64_t SHARE_OBJECT_SIZE = 64 * 1024 * 1024;  // 64 MB
    std::string key_;
    uint64_t dimSize_;  // 单个 embedding 或 单条数据 大小
    uint64_t shareObjectSize_;
    uint64_t objectCapacity_;                    // 单个 share object 能存多少条 embedding
    std::atomic<uint64_t> vectorCapacity_{ 0 };  // 当前 vector 能存多少条 embedding
    std::vector<ShareBuffer> shareBuffers_;
    bool isInit_{ false };  // 是否初始化了

    std::shared_ptr<KVClient> kvClient_;
    ShareObject shareObject_;

    mutable std::shared_mutex rw_mutex_;
    friend ShareMapObject;
};

}  // namespace datasystem
