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
 * Description: Base data unit.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_BASE_DATA_UNIT_H
#define DATASYSTEM_MIGRATE_DATA_BASE_DATA_UNIT_H

#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

class BaseDataUnit {
public:
    BaseDataUnit(const ImmutableString &objectKey, uint64_t version) : objectKey_(objectKey), version_(version)
    {
    }

    virtual ~BaseDataUnit() = default;

    /**
     * @brief Lock data before use.
     * @return K_OK if success, the error otherwise.
     */
    virtual Status LockData()
    {
        return Status::OK();
    }

    /**
     * @brief Get data memory views.
     * @return Memory views.
     */
    virtual std::vector<MemView> GetMemViews() const = 0;

    /**
     * @brief Get data size.
     * @return Data size.
     */
    virtual uint64_t Size() const = 0;

    /**
     * @brief Get object data version.
     * @return Version.
     */
    uint64_t Version() const
    {
        return version_;
    }

    /**
     * @brief Get object key.
     * @return Object key.
     */
    ImmutableString Id() const
    {
        return objectKey_;
    }

private:
    ImmutableString objectKey_;

    uint64_t version_;
};

class ShmData : public BaseDataUnit {
public:
    ShmData(const ImmutableString &objectKey, uint64_t version, std::shared_ptr<ShmUnit> unit, size_t dataSize,
            size_t metaSize)
        : BaseDataUnit(objectKey, version),
          data_(static_cast<uint8_t *>(unit->GetPointer()) + metaSize),
          size_(dataSize),
          shmGuard_(std::move(unit), dataSize, metaSize),
          needLock_(metaSize != 0)
    {
    }

    ~ShmData() = default;

    /**
     * @brief Lock data before use.
     * @return K_OK if success, the error otherwise.
     */
    Status LockData() override
    {
        return needLock_ ? shmGuard_.TryRLatch() : Status::OK();
    }

    /**
     * @brief Get data memory views.
     * @return Memory views.
     */
    std::vector<MemView> GetMemViews() const override
    {
        std::vector<MemView> result;
        result.emplace_back(data_, size_);
        return result;
    }

    /**
     * @brief Get data size.
     * @return Data size.
     */
    uint64_t Size() const override
    {
        return size_;
    }

private:
    void *data_;
    uint64_t size_;
    ShmGuard shmGuard_;
    bool needLock_;
};

class PayloadData : public BaseDataUnit {
public:
    PayloadData(const ImmutableString &objectKey, uint64_t version, std::vector<RpcMessage> payloads, size_t dataSize)
        : BaseDataUnit(objectKey, version), payloads_(std::move(payloads)), size_(dataSize)
    {
    }

    ~PayloadData() = default;

    /**
     * @brief Get data memory views.
     * @return Memory views.
     */
    std::vector<MemView> GetMemViews() const override
    {
        std::vector<MemView> result;
        for (const auto &payload : payloads_) {
            result.emplace_back(payload.Data(), payload.Size());
        }
        return result;
    }

    /**
     * @brief Get data size.
     * @return Data size.
     */
    uint64_t Size() const override
    {
        return size_;
    }

private:
    std::vector<RpcMessage> payloads_;
    uint64_t size_;
};


}  // namespace object_cache
}  // namespace datasystem

#endif
