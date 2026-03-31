/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Eviction manager common basic class.
 */

#ifndef TESTS_UT_WORKER_OBJECT_CACHE_EVICTION_MANAGER_COMMON_H
#define TESTS_UT_WORKER_OBJECT_CACHE_EVICTION_MANAGER_COMMON_H

#include <memory>

#include "securec.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {
class EvictionManagerCommon {
public:
    using SafeObjType = SafeObject<ObjectInterface>;
    using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;

    uint64_t GetMaxMemorySize()
    {
        return allocator->GetMaxMemorySize();
    }

    uint64_t GetAllocatedSize()
    {
        return allocator->GetMemoryUsage();
    }

    void GetAllObjsFromObjectTable(std::unordered_map<std::string, std::shared_ptr<SafeObjType>> &res)
    {
        for (auto &meta : *objectTable_) {
            res[meta.first] = meta.second;
        }
    }

    uint64_t GetMetaSize(uint64_t dataSize)
    {
        const uint64_t defaultMetaSize = 10;
        return WorkerOcServiceCrudCommonApi::CanTransferByShm(dataSize) ? defaultMetaSize : 0;
    }

    Status CreateObject(const std::string &objectKey, uint64_t dataSize, WriteMode writeMode = WriteMode::NONE_L2_CACHE,
                        bool primaryCopy = true, bool spillState = false, DataFormat dataFormat = DataFormat::BINARY,
                        bool retry = false, std::shared_ptr<WorkerOcEvictionManager> evictionManager = nullptr)
    {
        CHECK_FAIL_RETURN_STATUS(!objectTable_->Contains(objectKey), StatusCode::K_DUPLICATED, "object exist");
        const uint64_t metaSize = GetMetaSize(dataSize);
        uint64_t needSize = dataSize + metaSize;

        // add object to objectsInfo_
        auto ptr = std::make_unique<object_cache::ObjCacheShmUnit>();
        auto shmUnit = std::make_shared<ShmUnit>();
        if (!retry) {
            RETURN_IF_NOT_OK(shmUnit->AllocateMemory("", needSize, false));
        } else {
            CHECK_FAIL_RETURN_STATUS(evictionManager != nullptr, K_RUNTIME_ERROR, "evict manager is null");
            RETURN_IF_NOT_OK(AllocateMemoryForObject(objectKey, dataSize, metaSize, false, evictionManager, *shmUnit));
        }
        if (metaSize > 0) {
            auto ret = memset_s(shmUnit->GetPointer(), metaSize, 0, metaSize);
            if (ret != EOK) {
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                                        FormatString("[ObjectKey %s] Memset failed, errno: %d", objectKey, ret));
            }
        }
        ptr->SetShmUnit(shmUnit);
        ptr->SetDataSize(dataSize);
        ptr->SetMetadataSize(metaSize);
        ptr->SetCreateTime(1);
        ptr->SetLifeState(ObjectLifeState::OBJECT_SEALED);

        ptr->modeInfo.SetWriteMode(writeMode);
        ptr->modeInfo.SetCacheType(CacheType::MEMORY);
        ptr->stateInfo.SetDataFormat(dataFormat);
        ptr->stateInfo.SetPrimaryCopy(primaryCopy);
        ptr->stateInfo.SetSpillState(spillState);

        objectTable_->Insert(objectKey, std::move(ptr));
        return Status::OK();
    }

    Status DeleteObject(const std::string &objectKey)
    {
        CHECK_FAIL_RETURN_STATUS(objectTable_->Contains(objectKey), StatusCode::K_NOT_FOUND, "object not exist");
        objectTable_->Erase(objectKey);
        return Status::OK();
    }

    std::shared_ptr<ObjectTable> &GetObjectTable()
    {
        return objectTable_;
    }

    ~EvictionManagerCommon()
    {
        if (allocator) {
            allocator->Shutdown();
        }
    }

    datasystem::memory::Allocator *allocator = nullptr;
    std::shared_ptr<ObjectTable> objectTable_;
    const uint64_t maxMemorySize = 1 * 1024 * 1024 * 1024;
};
}  // namespace ut
}  // namespace datasystem

#endif
