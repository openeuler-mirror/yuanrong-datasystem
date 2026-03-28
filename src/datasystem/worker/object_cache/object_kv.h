
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Defines key-value pair for entries in the object table.
 */

#ifndef DATASYSTEM_WORKER_OBJECT_KV_H
#define DATASYSTEM_WORKER_OBJECT_KV_H

#include <cstddef>
#include <cstdint>
#include <string>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/util/request_table.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/object_cache/device/device_obj_cache.h"

namespace datasystem {
namespace object_cache {

using SafeObjType = SafeObject<ObjectInterface>;
using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;

struct ReadKey : public OffsetInfo {
    explicit ReadKey(const std::string &objectKey, uint64_t offset = 0, uint64_t size = 0)
        : OffsetInfo(offset, size), objectKey(objectKey)
    {
    }

    ReadKey(const std::string &objectKey, OffsetInfo offsetInfo) : OffsetInfo(offsetInfo), objectKey(objectKey)
    {
    }

    friend std::ostream &operator<<(std::ostream &out, const ReadKey &key)
    {
        out << key.objectKey;
        if (key.readOffset > 0 || key.readSize > 0) {
            out << "(" << key.readOffset << "," << key.readSize << ")";
        }
        return out;
    }

    bool operator<(const ReadKey &other) const
    {
        return objectKey < other.objectKey;
    }

    OffsetInfo GetOffsetInfo() const
    {
        return OffsetInfo(readOffset, readSize);
    }

    const std::string &objectKey;
};

class ReadObjectKV : public ObjectKV, protected OffsetInfo {
public:
    ReadObjectKV(const ReadKey &readKey, SafeObjType &entry,
                 std::shared_ptr<std::string> commId = nullptr)
        : ObjectKV(readKey.objectKey, entry),
          OffsetInfo(readKey.readOffset, readKey.readSize),
          commId_(std::move(commId))
    {
        // update when cache is valid.
        if (!GetObjEntry()->stateInfo.IsCacheInvalid()) {
            AdjustReadSize(GetObjEntry()->GetDataSize());
        }
    }

    ReadKey ConstructReadKey() const
    {
        return ReadKey(GetObjKey(), readOffset, readSize);
    }

    bool IsOffsetRead() const
    {
        return readOffset > 0 || (readSize > 0 && readSize < GetObjEntry()->GetDataSize());
    }

    uint64_t GetReadSize() const
    {
        return readSize;
    }

    uint64_t GetReadOffset() const
    {
        return readOffset;
    }

    Status CheckReadOffset()
    {
        uint64_t dataSize = GetObjEntry()->GetDataSize();
        if (readOffset < dataSize) {
            return Status::OK();
        }
        RETURN_STATUS(K_OUT_OF_RANGE, FormatString("Read offset %zu out of range [0, %zu)", readOffset, dataSize));
    }

    // Hold the client communicator root info so it can be passed to the remote worker.
    std::shared_ptr<std::string> commId_{ nullptr };
};

struct ObjEntryParams {
    const std::string &objectKey;
    const uint64_t dataSize;
    const uint64_t metaSize;
    const int64_t createTime;
    const ModeInfo &entryMode;
    const StateInfo &entryState;
    const ObjectLifeState lifeState;
};

/**
 * @brief Create a new object, but in this function not add reference count.
 * @param[in] objParams The obj parameters structure.
 * @return The objInterface that will be created.
 */
std::unique_ptr<ObjectInterface> CreateObjectEntry(const ObjEntryParams &objParams);

/**
 * @brief Update properties of object entry.
 * @param[in] consistencyType The consistency of safeObj.
 * @param[in] writeMode The through of back mode that refers to save entry in the L2 cache.
 * @param[in] cacheType The type of cache.
 * @param[in] dataSize The data size.
 * @param[in] metaDataSize The metadata size.
 * @param[out] safeObj Object entry.
 */
void UpdateObjectEntry(ConsistencyType type, WriteMode writeMode, CacheType cacheType, uint64_t metaDataSize,
                       SafeObjType &safeObj);

/**
 * @brief Set properties of object entry.
 * @param[in] namespaceUri Normalized object key.
 * @param[in] consistencyType The consistency of safeObj.
 * @param[in] writeMode The through of back mode that refers to save entry in the L2 cache.
 * @param[in] cacheType The type of cache.
 * @param[in] dataSize The data size.
 * @param[in] metaDataSize The metadata size.
 * @param[out] safeObj Object entry.
 */
void SetNewObjectEntry(const std::string &namespaceUri, ConsistencyType consistencyType, WriteMode writeMode,
                       CacheType cacheType, uint64_t dataSize, uint64_t metaDataSize, SafeObjType &safeObj);

/**
 * @brief Set meta info to device object entry.
 * @param[in] meta The meta of device object.
 * @param[in] metaDataSize The metadata size.
 * @param[out] entry The device object entry.
 */
void SetDeviceObjEntry(const ObjectMetaPb &meta, uint64_t metaDataSize, SafeObjType &entry);

/**
 * @brief Set properties of object entry.
 * @param[in] meta The meta of object.
 * @param[in] metaDataSize The metadata size.
 * @param[out] entry Object entry.
 */
void SetObjectEntryAccordingToMeta(const ObjectMetaPb &meta, uint64_t metaDataSize, SafeObjType &entry);

/**
 * @brief Set properties of object entry.
 * @param[in] objectKey The object key that need to be set.
 * @param[out] entry Object entry.
 */
void SetEmptyObjectEntry(const std::string &objectKey, SafeObjType &entry);

/**
 * @brief Retry to lock an object.
 * @param[in] objectKey The object key need to be lock.
 * @param[in] entry Safe object entry.
 * @param[in] nullable Enable nullable object or not.
 * @return Status of the call.
 */
Status TryLockWithRetry(const std::string &objectKey, const std::shared_ptr<SafeObjType> &entry, bool nullable = false);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_KV_H
