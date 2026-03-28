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
 * Description: Defines the object interface class including virtual functions.
 */
#ifndef DATASYSTEM_OBJECT_CACHE_OBJECT_INTERFACE_H
#define DATASYSTEM_OBJECT_CACHE_OBJECT_INTERFACE_H

#include <cstdint>
#include <deque>
#include <memory>
#include <vector>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/mmap/immap_table_entry.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

#ifndef DISABLE_RDMA
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#endif

namespace datasystem {

using RemoteH2DHostInfoMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<RemoteH2DHostInfoPb>>;
struct ObjectInterface {
    // The state and config of the object.
    datasystem::StateInfo stateInfo;
    datasystem::ModeInfo modeInfo;

    virtual Status FreeResources() = 0;
    virtual ~ObjectInterface() = default;

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return Base version just returns false. If implemented, returns true if invalid.
     */
    virtual bool IsInvalid() const
    {
        return false;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return Base version just returns false. If implemented, returns true if sealed.
     */
    virtual bool IsSealed() const
    {
        return false;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return Base version just returns false. If implemented, returns true if published.
     */
    virtual bool IsPublished() const
    {
        return false;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return Base version just returns false. If implemented, returns true if
     * FLAGS_enable_shared_memory == true and dataSize > SHM_THRESHOLD.
     */
    virtual bool IsShm() const
    {
        return false;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return Base version just returns the unknown state. If implemented, returns the life state.
     */
    virtual ObjectLifeState GetLifeState() const
    {
        return ObjectLifeState::OBJECT_INVALID;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return Base version just returns 0. If implemented, returns the create time.
     */
    virtual uint64_t GetCreateTime() const
    {
        return 0;
    }

    /**
     * @brief A setter function for the data size.
     * @param[in] size The data size.
     */
    virtual void SetDataSize(const uint64_t size)
    {
        (void)size;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return The dataSize (if implemented by derived child. returns 0 if not implemented.
     */
    virtual uint64_t GetDataSize() const
    {
        return 0;
    }

    /**
     * @brief A setter function for the meta data size.
     * @param[in] size The metadata size.
     */
    virtual void SetMetadataSize(const uint64_t size)
    {
        (void)size;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return The MetadataSize (if implemented by derived child. returns 0 if not implemented.
     */
    virtual uint64_t GetMetadataSize() const
    {
        return 0;
    }

    /**
     * @brief A setter function for the shared memory unit.
     * @param[in] shmUnit The shared memory unit.
     */
    virtual void SetShmUnit(const std::shared_ptr<ShmUnit> &shmUnit)
    {
        (void)shmUnit;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return The shared memory unit.
     */
    virtual std::shared_ptr<ShmUnit> GetShmUnit() const
    {
        return nullptr;
    }

    /**
     * @brief A getter function that a derived class can optionally implement.
     * @return The address (if implemented by derived child. returns empty string if not implemented.
     */
    virtual std::string GetAddress() const
    {
        return {};
    }

    /**
     * @brief A setter function that a derived class can optionally implement.
     * @param[in] newLifeState
     */
    virtual void SetLifeState(const ObjectLifeState newLifeState)
    {
        (void)newLifeState;
    }

    /**
     * @brief A setter function that a derived class can optionally implement.
     * @param[in] newCreateTime The create time.
     */
    virtual void SetCreateTime(const uint64_t newCreateTime)
    {
        (void)newCreateTime;
    }

    /**
     * @brief A setter function that a derived class can optionally implement.
     * @param[in] newAddress The new address.
     */
    virtual void SetAddress(const std::string &newAddress)
    {
        (void)newAddress;
    }

    /**
     * @brief An Erase function that a derived class can optionally implement.
     * @param[in] field The field to erase.
     * @return Status of the call.
     */
    virtual Status Erase(const std::string &field)
    {
        (void)field;
        return Status::OK();
    }

    /**
     * @brief Get function to test if its spilled or not.
     * @return True if its spilled.
     */
    virtual bool IsSpilled() const
    {
        return stateInfo.IsSpilled();
    }

    /**
     * @brief Check if the object is a hashmap.
     * @return True if the object is a hashmap, false otherwise.
     */
    bool IsHashmap() const
    {
        return stateInfo.GetDataFormat() == DataFormat::HASH_MAP;
    }

    /**
     * @brief Check if the object is binary.
     * @return True if the object is binary, false otherwise.
     */
    bool IsBinary() const
    {
        return stateInfo.GetDataFormat() == DataFormat::BINARY;
    }

    /**
     * @brief Check if the object is device object.
     * @return True if the object is device objecct, false otherwise.
     */
    bool IsHetero() const
    {
        return stateInfo.GetDataFormat() == DataFormat::HETERO;
    }

    /**
     * @brief Check if the object has L2 cache.
     * @return True if the object has L2 cache, false otherwise.
     */
    bool HasL2Cache() const
    {
        return modeInfo.GetWriteMode() == WriteMode::WRITE_THROUGH_L2_CACHE
               || modeInfo.GetWriteMode() == WriteMode::WRITE_BACK_L2_CACHE
               || modeInfo.GetWriteMode() == WriteMode::WRITE_BACK_L2_CACHE_EVICT;
    }

    /**
     * @brief Check if the object is write through mode.
     * @return True if the object is write through mode, false otherwise.
     */
    bool IsWriteThroughMode() const
    {
        return modeInfo.GetWriteMode() == WriteMode::WRITE_THROUGH_L2_CACHE;
    }

    /**
     * @brief Check if the object is write back mode.
     * @return True if the object is write back mode, false otherwise.
     */
    bool IsWriteBackMode() const
    {
        return modeInfo.GetWriteMode() == WriteMode::WRITE_BACK_L2_CACHE
               || modeInfo.GetWriteMode() == WriteMode::WRITE_BACK_L2_CACHE_EVICT;
    }

    /**
     * @brief Check if local node can get data from memory or spill or l2cache
     * @return True if the data can get from worker
     */
    bool IsGetDataEnablelFromLocal() const
    {
        return !stateInfo.IsIncomplete() || (stateInfo.IsIncomplete() && (IsSpilled() || HasL2Cache()));
    }

    /**
     * @brief Check if the object is none l2 cache evict mode.
     * @return True if the object is none l2 cache evict mode, false otherwise.
     */
    bool IsNoneL2CacheEvictMode() const
    {
        return modeInfo.GetWriteMode() == WriteMode::NONE_L2_CACHE_EVICT;
    }

    /**
     * @brief Check if the object is write back l2 cache evict mode.
     * @return True if the object is write back l2 cache evict mode, false otherwise.
     */
    bool IsWriteBackL2CacheEvictMode() const
    {
        return modeInfo.GetWriteMode() == WriteMode::WRITE_BACK_L2_CACHE_EVICT;
    }

    /**
     * @brief Check if the object is memory cache.
     * @return True if the object is memory cache, false otherwise.
     */
    bool IsMemoryCache() const
    {
        return modeInfo.GetCacheType() == CacheType::MEMORY;
    }

    /**
     * @brief Check if the object is disk cache.
     * @return True if the object is disk cache, false otherwise.
     */
    bool IsDiskCache() const
    {
        return modeInfo.GetCacheType() == CacheType::DISK;
    }

    /**
     * @brief Check if the object is empty.
     * @return True if the object is empty.
     */
    bool IsEmpty() const
    {
        return stateInfo.IsEmpty();
    }

    bool IsShmUnitExistsAndComplete() const
    {
        return GetShmUnit() != nullptr && !stateInfo.IsIncomplete();
    }

    #ifndef DISABLE_RDMA
    virtual void SetRemoteHostInfo(const std::string &clientCommId,
                                   const std::shared_ptr<RemoteH2DHostInfoPb> &remoteH2DHostInfo)
    {
        (void)clientCommId;
        (void)remoteH2DHostInfo;
    }

    virtual std::shared_ptr<RemoteH2DHostInfoMap> GetRemoteHostInfo() const
    {
        return nullptr;
    }
    #endif
};

struct ObjectBufferInfo {
    std::string objectKey;
    ShmKey shmId;
    uint8_t *pointer;
    uint64_t dataSize;
    uint64_t metadataSize;
    uint32_t ttlSecond = 0;
    ModeInfo objectMode;
    bool keep = false;
    bool isSeal = false;
    uint32_t version;
    std::shared_ptr<RpcMessage> payloadPointer;
    std::shared_ptr<client::IMmapTableEntry> mmapEntry;
    std::shared_ptr<RemoteH2DHostInfoPb> remoteHostInfo = nullptr;
    std::shared_ptr<UrmaRemoteAddrPb> ubUrmaDataInfo = nullptr;
    bool ubDataSentByMemoryCopy = false;
};

enum class TransferType : uint8_t { HOST = 0, P2P = 1 };

struct DeviceBufferInfo {
    DeviceBufferInfo(const std::string &devObjKey, int32_t deviceIdx, LifetimeType lifetimeType, bool cacheLocation,
                     TransferType transferType)
        : isPublished(false),
          cacheLocation(cacheLocation),
          deviceIdx(deviceIdx),
          version(0),
          transferType(transferType),
          lifetimeType(lifetimeType),
          devObjKey(devObjKey),
          srcOffset(0),
          autoRelease(true)
    {
    }

    bool isPublished;
    bool cacheLocation;
    int32_t deviceIdx;
    uint32_t version;
    TransferType transferType;
    LifetimeType lifetimeType;
    std::string devObjKey;
    std::string shmId;
    int32_t srcOffset;
    // HeteroClient is based on KV semantics,  when it is destroyed, DeviceBuffer does not delete the location.
    // ObjectClient, on the other hand, is based on object semantics and requires exposing DeviceBuffer and deletes the
    // location through DeviceBuffer destruction. Therefore, a boolean variable is needed to mark this difference.The
    // Default value is true"
    bool autoRelease;
};

struct OffsetInfo {
    OffsetInfo() = default;

    OffsetInfo(uint64_t offset, uint64_t size) : readOffset(offset), readSize(size)
    {
    }

    /**
     * @brief Adjust the real read size.
     * @param[in] dataSize The object data size.
     */
    void AdjustReadSize(uint64_t dataSize)
    {
        if (readOffset >= dataSize) {
            readSize = 0;
            return;
        }
        if (readSize == 0) {
            readSize = dataSize - readOffset;
        } else {
            readSize = std::min<uint64_t>(dataSize - readOffset, readSize);
        }
    }

    bool IsOffsetRead(uint64_t dataSize) const
    {
        return readOffset != 0 || dataSize > readSize;
    }

    bool operator==(const OffsetInfo &other) const
    {
        return (readOffset == other.readOffset && readSize == other.readSize);
    }

    uint64_t readOffset = 0;
    uint64_t readSize = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_OBJECT_INTERFACE_H
