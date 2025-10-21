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
 * Description: DeviceObjCache declaration.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_OBJECT_CACHE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_OBJECT_CACHE_H

#include <cstddef>
#include <cstdint>

#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/shared_memory/shm_unit.h"

namespace datasystem {
namespace object_cache {
class DeviceObjCache : public ObjectInterface {
public:
    /**
     * @brief Default constructor.
     */
    DeviceObjCache()
    {
        this->stateInfo.SetDataFormat(DataFormat::HETERO);
    }
    /**
     * @brief Default destructor.
     */
    ~DeviceObjCache() override = default;

    /**
     * @brief Get object state: OBJECT_PUBLISHED.
     * @return True if the state is OBJECT_PUBLISHED.
     */
    bool IsPublished() const override;

    /**
     * @brief Set the object is published.
     * @return True if the state is OBJECT_PUBLISHED.
     */
    void SetPublished();

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
     * @brief Get shmUnit
     * @return The shared_ptr of ShmUnit
     */
    std::shared_ptr<ShmUnit> GetShmUnit() const override;

    /**
     * @brief Set ShmUnit.
     * @param[in] shmUnit The shared_ptr of ShmUnit
     */
    void SetShmUnit(const std::shared_ptr<ShmUnit> &shmUnit) override;

    /**
     * @brief Set device index.
     * @param[in] deviceIdx The device index
     */
    void SetDeviceIdx(uint32_t deviceIdx);

    /**
     * @brief Get the device index.
     * @return The device index.
     */
    uint32_t GetDeviceIdx();

    /**
     * @brief Set offset
     * @param[in] offset The offset of device memory pointer.
     */
    void SetOffset(uint64_t offset);

    /**
     * @brief Get the offset of device memory pointer.
     * @return The offset of device memory pointer.
     */
    uint32_t GetOffset();

    /**
     * @brief Free the object resources.
     */
    Status FreeResources() override;

    /**
     * @brief Get the size of metadata.
     * @return The size of metadata.
     */
    uint64_t GetMetadataSize() const override;

    /**
     * @brief Get the size of metadata.
     * @param[in] size the size of metadata.
     */
    void SetMetadataSize(const uint64_t size) override;

private:
    std::shared_ptr<ShmUnit> shmUnit_{ nullptr };
    uint64_t dataSize_{ 0 };
    uint64_t metadataSize_{ 0 };
    uint32_t deviceIdx_;
    uint64_t offset_;
    bool isPublished_{ false };
};
}  // namespace object_cache
}  // namespace datasystem
#endif