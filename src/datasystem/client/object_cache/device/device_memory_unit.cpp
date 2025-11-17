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
 * Description: Implement the device memory unit.
 */
#include "datasystem/client/object_cache/device/device_memory_unit.h"

#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {

DeviceMemoryUnit::DeviceMemoryUnit(const std::string &devMemId, std::vector<Blob> blobStorage)
    : devMemId_(devMemId), blobStorage_(std::move(blobStorage)), dsAllocatedStorage_(blobStorage_.size(), false)
{
}

Status DeviceMemoryUnit::MallocDeviceMemoryIfUserNotSet()
{
    CHECK_FAIL_RETURN_STATUS(blobStorage_.size() == dsAllocatedStorage_.size(), K_RUNTIME_ERROR,
                             "The size of blobStorage and dsAllocatedStorage is not same.");
    for (auto i = 0ul; i < blobStorage_.size(); i++) {
        auto &blob = blobStorage_[i];
        if (blob.pointer == nullptr) {
            VLOG(1) << "Malloc device memory, size: " << blob.size;
            RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->MallocDeviceMemory(blob.size, blob.pointer));
            dsAllocatedStorage_[i] = true;
        }
    }
    return Status::OK();
}

const std::vector<Blob> &DeviceMemoryUnit::GetBlobsStorage() const
{
    return blobStorage_;
}

Status DeviceMemoryUnit::CheckAndGetSingleBlob(Blob &blob) const
{
    if (blobStorage_.empty()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "The list of data info in device buffer is empty.");
    }
    if (blobStorage_.size() > 1) {
        RETURN_STATUS(K_RUNTIME_ERROR, "The size of data info list in device buffer > 1");
    }
    blob = blobStorage_[0];
    return Status::OK();
}

DeviceMemoryUnit::~DeviceMemoryUnit()
{
    std::vector<size_t> freeIndexVec;
    freeIndexVec.reserve(blobStorage_.size());
    for (auto i = 0ul; i < blobStorage_.size(); i++) {
        const auto &blob = blobStorage_[i];
        if (dsAllocatedStorage_[i] && blob.pointer) {
            freeIndexVec.push_back(i);
            LOG_IF_ERROR(acl::AclDeviceManager::Instance()->FreeDeviceMemory(blob.pointer),
                         "Release device memory allocated by datasystem failed.");
        }
    }
    VLOG(1) << "Free device memory unit: " << devMemId_ << ", blob index: " << VectorToString(freeIndexVec);
}

Status DeviceMemoryUnit::CheckEmptyPointer() const
{
    for (auto i = 0ul; i < blobStorage_.size(); i++) {
        CHECK_FAIL_RETURN_STATUS(blobStorage_[i].pointer != nullptr, K_INVALID,
                                 FormatString("The device pointer [index: %zu] in device buffer is nullptr.", i));
    }
    return Status::OK();
}
}  // namespace datasystem