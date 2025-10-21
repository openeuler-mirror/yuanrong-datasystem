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
 * Description: The hashmap object struct of worker
 */
#include "datasystem/worker/object_cache/obj_cache_hashmap.h"

namespace datasystem {
namespace object_cache {

ObjCacheHashmap::ObjCacheHashmap()
{
    this->stateInfo.SetDataFormat(DataFormat::HASH_MAP);
    this->modeInfo.SetWriteMode(WriteMode::WRITE_THROUGH_L2_CACHE);
}

Status ObjCacheHashmap::Set(const std::string &field, const std::shared_ptr<ShmUnit> &value)
{
    TbbHashMap::accessor accessor;
    if (map_.find(accessor, field)) {
        // Overwriting the old shared_ptr with a new shared_ptr will auto free the old one, causing it's destructor
        // to run and free the memory. Technically, calling FreeMemory manually here is not needed because the
        // destructor will do it, but we will leave this here so that it's clear that we're doing a free of the
        // old data.
        RETURN_IF_NOT_OK(accessor->second->FreeMemory());
        accessor->second = value;
    } else {
        map_.emplace(field, value);
    }
    return Status::OK();
}

Status ObjCacheHashmap::Get(const std::string &field, std::string &value)
{
    TbbHashMap::const_accessor accessor;
    if (map_.find(accessor, field)) {
        const auto &shmUnit = accessor->second;
        value = std::string(static_cast<const char *>(shmUnit->pointer), shmUnit->size);
    } else {
        RETURN_STATUS(K_NOT_FOUND, "field not found.");
    }
    return Status::OK();
}

bool ObjCacheHashmap::Exist(const std::string &field) const
{
    return map_.count(field) > 0;
}

Status ObjCacheHashmap::Erase(const std::string &field)
{
    TbbHashMap::accessor accessor;
    if (map_.find(accessor, field)) {
        // Erasing the element will cause it to go through its destructor (ShmUnit) and free the memory.
        map_.erase(accessor);
    } else {
        RETURN_STATUS(K_NOT_FOUND, "field not found.");
    }

    return Status::OK();
}

Status ObjCacheHashmap::FreeResources()
{
    // Clearing the map will go through destructor of each element (a ShmUnit) and call its destructor
    // to clean up the shm unit and free memory.
    map_.clear();
    return Status::OK();
}

ObjCacheHashmap::~ObjCacheHashmap()
{
    if (!map_.empty()) {
        LOG(INFO) << "field add after eviction, count " << map_.size();
        LOG_IF_ERROR(ObjCacheHashmap::FreeResources(), "free share memory failed:");
    }
}
}  // namespace object_cache
}  // namespace datasystem