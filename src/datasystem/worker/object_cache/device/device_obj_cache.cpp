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
 * Description: DeviceObjCache implementation.
 */

#include "datasystem/worker/object_cache/device/device_obj_cache.h"

namespace datasystem {
namespace object_cache {
bool DeviceObjCache::IsPublished() const
{
    return isPublished_;
}

void DeviceObjCache::SetPublished()
{
    isPublished_ = true;
}

uint64_t DeviceObjCache::GetDataSize() const
{
    return dataSize_;
}

void DeviceObjCache::SetDataSize(uint64_t size)
{
    dataSize_ = size;
}

std::shared_ptr<ShmUnit> DeviceObjCache::GetShmUnit() const
{
    return shmUnit_;
}

void DeviceObjCache::SetShmUnit(const std::shared_ptr<ShmUnit> &shmUnit)
{
    shmUnit_ = shmUnit;
}

uint64_t DeviceObjCache::GetMetadataSize() const
{
    return metadataSize_;
}

void DeviceObjCache::SetMetadataSize(const uint64_t size)
{
    metadataSize_ = size;
}

void DeviceObjCache::SetDeviceIdx(uint32_t deviceIdx)
{
    deviceIdx_ = deviceIdx;
}

uint32_t DeviceObjCache::GetDeviceIdx()
{
    return deviceIdx_;
}

void DeviceObjCache::SetOffset(uint64_t offset)
{
    offset_ = offset;
}

uint32_t DeviceObjCache::GetOffset()
{
    return offset_;
}

Status DeviceObjCache::FreeResources()
{
    shmUnit_.reset();
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem