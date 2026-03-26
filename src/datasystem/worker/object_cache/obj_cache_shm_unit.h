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
 * Description: ObjCacheShmUnit declaration.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_OBJ_CACHE_SHM_UNIT_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_OBJ_CACHE_SHM_UNIT_H

#include <sys/mman.h>
#include <memory>

#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/protos/object_posix.pb.h"

namespace datasystem {
namespace object_cache {

class ObjCacheShmUnit : public ObjectInterface {
public:
    /**
     * @brief ObjCacheShmUnit constructor.
     */
    ObjCacheShmUnit();

    /**
     * @brief Default destructor.
     */
    ~ObjCacheShmUnit() override = default;

    /**
     * @brief Free memory resources.
     * @return Status of the call.
     */
    Status FreeResources() override;

    /**
     * @brief Get object state: OBJECT_INVALID.
     * @return True if the state is OBJECT_INVALID
     */
    bool IsInvalid() const override;

    /**
     * @brief Get object state: OBJECT_SEALED.
     * @return True if the state is OBJECT_SEALED.
     */
    bool IsSealed() const override;
    /**
     * @brief Get object state: OBJECT_PUBLISHED.
     * @return True if the state is OBJECT_PUBLISHED.
     */
    bool IsPublished() const override;

    /**
     * @brief Get life state.
     * @return The life state.
     */
    ObjectLifeState GetLifeState() const override;

    /**
     * @brief Set the lifeState.
     * @param newLifeState The new lifeSate.
     */
    void SetLifeState(ObjectLifeState newLifeState) override;

    /**
     * @brief Get the create time.
     * @return The create time.
     */
    uint64_t GetCreateTime() const override;

    /**
     * @brief Set the createTime.
     * @param[in] newCreateTime The new createTime.
     */
    void SetCreateTime(uint64_t newCreateTime) override;

    /**
     * @brief Get dataSize.
     * @return The dataSize.
     */
    uint64_t GetDataSize() const override;

    /**
     * @brief Set dataSize.
     * @param[in] size new dataSize.
     */
    void SetDataSize(uint64_t size) override;

    /**
     * @brief Get the metadataSize.
     * @return The metadataSize.
     */
    uint64_t GetMetadataSize() const override;

    std::shared_ptr<ShmUnit> GetShmUnit() const override
    {
        return shmUnit_;
    }

    void SetShmUnit(const std::shared_ptr<ShmUnit> &shmUnit) override
    {
        shmUnit_ = shmUnit;
    }

    /**
     * @brief Set MetaDataSize.
     * @param[in] size new MetaDataSize.
     */
    void SetMetadataSize(uint64_t size) override;

    /**
     * @brief Get the address.
     * @return The address.
     */
    std::string GetAddress() const override;

    /**
     * @brief Set the address.
     * @param newAddress The new address.
     */
    void SetAddress(const std::string &newAddress) override;

#ifndef DISABLE_RDMA
    /**
     * @brief Record the remote host info for the client.
     * @param[in] clientCommId The client communicator identifier uuid.
     * @param[in] remoteH2DHostInfo The remote H2D host info, containing segment info, roor info and data info.
     */
    void SetRemoteHostInfo(const std::string &clientCommId,
                           const std::shared_ptr<RemoteH2DHostInfoPb> &remoteH2DHostInfo) override;

    /**
     * @brief Get the remote host info.
     * @return The entire map for remote h2d host info.
     */
    std::shared_ptr<RemoteH2DHostInfoMap> GetRemoteHostInfo() const override;
#endif
private:
    std::shared_ptr<ShmUnit> shmUnit_{ nullptr };
    // The metadata and data bytes, the struct of entry is |metadata frame|data frame|
    uint64_t dataSize_{ 0 };
    uint64_t metadataSize_{ 0 };
    // When this object was created.
    uint64_t createTime_{ 0 };
    // The source of updated data address.
    std::string address_;
    // The life state of object.
    ObjectLifeState lifeState_ = ObjectLifeState::OBJECT_INVALID;
    // This means the data is not local, and only metadata is here. So the shm unit shall be nullptr.
    std::shared_ptr<RemoteH2DHostInfoMap> remoteH2DHostInfoMap_{ nullptr };
};

/**
 * @brief Copy and split buffer into multiple rpc message which size small than 2G.
 * @param[in] tenantId The tenant of the data
 * @param[in] data Source of the buffer
 * @param[in] size Size of the buffer
 * @param[out] messages The rpc messages after split.
 * @return Status object
 */
Status CopyAndSplitBuffer(const std::string &tenantId, const void *data, size_t size,
                          std::vector<RpcMessage> &messages);

/**
 * @brief Allocate memory for object to share.
 * @param[in] objectKey The object key of entry that need to allocate memory.
 * @param[in] dataSize The data size of memory in bytes.
 * @param[in] metadataSize The metadata size of memory in bytes.
 * @param[in] populate Indicate need populate or not.
 * @param[in] evictionManager Eviction manager.
 * @param[out] shmUnit The share memory info of object.
 * @param[in] cacheType The type of cache.
 * @param[in] retryOnOOM Indicate need retry on OOM or not.
 * @return Status of the call.
 */
Status AllocateMemoryForObject(const std::string &objectKey, const uint64_t dataSize, uint64_t metadataSize,
                               bool populate, std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                               ShmUnit &shmUnit, CacheType cacheType = CacheType::MEMORY,
                               bool retryOnOOM = true);

/**
 * @brief Distribute memory from already allocated ShmOwner for object.
 * @param[in] objectKey The object key of entry that need to allocate memory.
 * @param[in] dataSize The data size of memory in bytes.
 * @param[in] metadataSize The metadata size of memory in bytes.
 * @param[in] populate Indicate need populate or not.
 * @param[in] shmOwner The share memory owner.
 * @param[out] shmUnit The share memory info of object.
 * @return Status of the call.
 */
Status DistributeMemoryForObject(const std::string &objectKey, const uint64_t dataSize, uint64_t metadataSize,
                                 bool populate, std::shared_ptr<ShmOwner> shmOwner, ShmUnit &shmUnit);

/**
 * @brief Allocate aggregated chunks of shared memory.
 * @param[in] firstObjectKey The first object key.
 * @param[in] traversalHelper Helper function that does the customized traversal work.
 * @param[in] evictionManager Eviction manager.
 * @param[out] shmOwners The allocated shared memory chunks.
 * @param[out] shmIndexMapping The object id to shmOwners index mapping.
 * @param[in] retryOnOOM Indicate need retry on OOM or not.
 * @param[in] includeLargeObjects Indicate whether include large objects in aggregate allocation.
 * @return Status of the call.
 */
Status AggregateAllocate(
    const std::string &firstObjectKey,
    std::function<void(std::function<void(uint64_t, uint64_t, uint32_t)>, bool &)> &traversalHelper,
    std::shared_ptr<WorkerOcEvictionManager> evictionManager, std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
    std::vector<uint32_t> &shmIndexMapping, bool retryOnOOM = true, bool includeLargeObjects = false);

/**
 * @brief Allocate Shm unit and init its id.
 * @param[in] objectKey The object key.
 * @param[in] dataSize The data size.
 * @param[in] metadataSize The metadata size.
 * @param[in] populate Indicate need populate or not.
 * @param[in] evictionManager Eviction manager.
 * @param[out] shmUnit The shared memory unit.
 * @param[in] cacheType The type of cache.
 * @return Status of the call.
 */
Status AllocateNewShmUnit(const std::string &objectKey, uint64_t dataSize, uint64_t metadataSize, bool populate,
                          std::shared_ptr<WorkerOcEvictionManager> evictionManager, std::shared_ptr<ShmUnit> &shmUnit,
                          CacheType cacheType = CacheType::MEMORY);

/**
 * @brief Read object from disk to memory, AllocateMemory will be occurs.
 * @param[in] objectKV The object entry and its corresponding objectKey.
 * @param[in] evictionManager Eviction manager.
 * @return Status of the call.
 */
Status LoadSpilledObjectToMemory(ReadObjectKV &objectKV, std::shared_ptr<WorkerOcEvictionManager> evictionManager);

/**
 * @brief Save the payload data to memory.
 * @param[in] objectKV The safe object shared unit and its corresponding objectKey.
 * @param[in] payloads The object data to be saved.
 * @param[in] evictionManager Eviction manager.
 * @param[in] threadPool the thread pool that might be used for larger memcopies.
 * @param[in] cacheType The type of cache.
 * @return Status of the call.
 */
Status SaveBinaryObjectToMemory(ObjectKV &objectKV, const std::vector<RpcMessage> &payloads,
                                std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                                const std::shared_ptr<ThreadPool> &threadPool);

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_OBJ_CACHE_SHM_UNIT_H
