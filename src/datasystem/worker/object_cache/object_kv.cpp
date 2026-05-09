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
 * Description: Definition of the operation to key-value pair for entries in the object table.
 */
#include "datasystem/worker/object_cache/object_kv.h"

#include "datasystem/object/object_enum.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"

namespace datasystem {
namespace object_cache {

std::unique_ptr<ObjectInterface> CreateObjectEntry(const ObjEntryParams &objParams)
{
    // Create derived class type.
    auto objShmUnit = std::make_unique<ObjCacheShmUnit>();
    objShmUnit->SetDataSize(objParams.dataSize);
    objShmUnit->SetMetadataSize(objParams.metaSize);
    objShmUnit->SetCreateTime(objParams.createTime);
    objShmUnit->SetTtlSecond(objParams.ttlSecond);
    objShmUnit->modeInfo = objParams.entryMode;
    objShmUnit->stateInfo = objParams.entryState;
    objShmUnit->SetLifeState(objParams.lifeState);
    return objShmUnit;
}

void UpdateObjectEntry(ConsistencyType type, WriteMode writeMode, CacheType cacheType, uint64_t metaDataSize,
                       SafeObjType &safeObj)
{
    safeObj->SetMetadataSize(metaDataSize);
    safeObj->modeInfo.SetConsistencyType(type);
    safeObj->stateInfo.SetDataFormat(DataFormat::BINARY);
    safeObj->modeInfo.SetWriteMode(writeMode);
    safeObj->modeInfo.SetCacheType(cacheType);
    safeObj->stateInfo.SetCacheInvalid(true);
    safeObj->stateInfo.SetWriteBackDone(false);
}

void SetNewObjectEntry(const std::string &namespaceUri, ConsistencyType consistencyType, WriteMode writeMode,
                       CacheType cacheType, uint64_t dataSize, uint64_t metaDataSize, SafeObjType &safeObj)
{
    safeObj.SetRealObject(CreateObjectEntry({ .objectKey = namespaceUri,
                                              .dataSize = dataSize,
                                              .metaSize = metaDataSize,
                                              .createTime = 0,
                                              .entryMode = ModeInfo(consistencyType, writeMode, cacheType),
                                              .entryState = StateInfo(DataFormat::BINARY),
                                              .lifeState = ObjectLifeState::OBJECT_INVALID }));

    safeObj->stateInfo.SetCacheInvalid(true);
}

void SetDeviceObjEntry(const ObjectMetaPb &meta, uint64_t metaDataSize, SafeObjType &entry)
{
    auto devObj = std::make_unique<DeviceObjCache>();
    devObj->SetMetadataSize(metaDataSize);
    devObj->SetDeviceIdx(meta.device_info().device_id());
    devObj->SetOffset(meta.device_info().offset());
    devObj->stateInfo.SetDataFormat(static_cast<DataFormat>(meta.config().data_format()));
    devObj->SetPublished();
    devObj->SetDataSize(meta.data_size());
    entry.SetRealObject(std::move(devObj));
}

void SetObjectEntryAccordingToMeta(const ObjectMetaPb &meta, uint64_t metaDataSize, SafeObjType &entry)
{
    const std::string &objectKey = meta.object_key();
    const ConfigPb &configPb = meta.config();
    auto dataFormat = static_cast<DataFormat>(configPb.data_format());
    if (dataFormat == DataFormat::HETERO) {
        SetDeviceObjEntry(meta, metaDataSize, entry);
    } else {
        entry.SetRealObject(CreateObjectEntry(
            { .objectKey = objectKey,
              .dataSize = meta.data_size(),
              .metaSize = metaDataSize,
              .createTime = (int64_t)meta.version(),
              .ttlSecond = meta.ttl_second(),
              .entryMode = ModeInfo(ConsistencyType(configPb.consistency_type()), WriteMode(configPb.write_mode()),
                                    CacheType(configPb.cache_type())),
              .entryState = StateInfo(DataFormat(configPb.data_format())),
              .lifeState = static_cast<ObjectLifeState>(meta.life_state()) }));
    }
}

void SetEmptyObjectEntry(const std::string &objectKey, SafeObjType &entry)
{
    auto realObj = CreateObjectEntry({ .objectKey = objectKey,
                                       .dataSize = 0,
                                       .metaSize = 0,
                                       .createTime = 0,
                                       .entryMode = ModeInfo(),
                                       .entryState = StateInfo(DataFormat::BINARY),
                                       .lifeState = ObjectLifeState::OBJECT_INVALID });
    realObj->stateInfo.SetEmpty(true);
    entry.SetRealObject(std::move(realObj));

    entry->stateInfo.SetCacheInvalid(true);
}

Status TryLockWithRetry(const std::string &objectKey, const std::shared_ptr<SafeObjType> &entry, bool nullable)
{
    Status rc = entry->TryWLock(nullable);
    if (rc.GetCode() != K_TRY_AGAIN) {
        return rc;
    }
    static const std::vector<int> delayMs = { 1, 10, 30, 50, 100 };
    int totalRetryMs = 0;
    int retryCount = 0;
    for (auto t : delayMs) {
        totalRetryMs += t;
        retryCount++;
        std::this_thread::sleep_for(std::chrono::milliseconds(t));
        rc = entry->TryWLock(nullable);
        if (rc.GetCode() != K_TRY_AGAIN) {
            LOG(INFO) << FormatString("TryWLock succeeded after %d retries for object key %s, cost %dms", retryCount,
                                      objectKey, totalRetryMs);
            return rc;
        }
    }
    LOG(INFO) << FormatString("TryWLock timeout after %d retries for object key %s, cost %dms", retryCount, objectKey,
                              totalRetryMs);
    return { K_WORKER_TIMEOUT, "Worker timeout" };
}
}  // namespace object_cache
}  // namespace datasystem
