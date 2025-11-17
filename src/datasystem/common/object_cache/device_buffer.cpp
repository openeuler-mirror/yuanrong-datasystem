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
 * Description: Device buffer.
 */

#include "datasystem/client/hetero_cache/device_buffer.h"

#include <memory>

#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {
DeviceBuffer::DeviceBuffer(std::shared_ptr<DeviceBufferInfo> bufferInfo, std::shared_ptr<DeviceMemoryUnit> devMemUnit,
                           std::shared_ptr<object_cache::ObjectClientImpl> clientImpl)
    : bufferInfo_(std::move(bufferInfo)), deviceMemUnit_(std::move(devMemUnit)), clientImpl_(std::move(clientImpl))
{
}

std::shared_ptr<DeviceBuffer> DeviceBuffer::CreateDeviceBuffer(
    std::shared_ptr<DeviceBufferInfo> bufferInfo, std::shared_ptr<DeviceMemoryUnit> devMemUnit,
    const std::shared_ptr<object_cache::ObjectClientImpl> &clientImpl)
{
    struct ConcreteDeviceBuffer : public DeviceBuffer {
        ConcreteDeviceBuffer(std::shared_ptr<DeviceBufferInfo> bufferInfo, std::shared_ptr<DeviceMemoryUnit> devMemUnit,
                             std::shared_ptr<object_cache::ObjectClientImpl> clientImpl)
            : DeviceBuffer(std::move(bufferInfo), std::move(devMemUnit), std::move(clientImpl))
        {
        }
    };
    return std::make_shared<ConcreteDeviceBuffer>(bufferInfo, devMemUnit, clientImpl);
}

DeviceBuffer::DeviceBuffer(DeviceBuffer &&other) noexcept
    : bufferInfo_(std::move(other.bufferInfo_)),
      deviceMemUnit_(std::move(other.deviceMemUnit_)),
      clientImpl_(std::move(other.clientImpl_))
{
    other.Reset();
}

DeviceBuffer &DeviceBuffer::operator=(DeviceBuffer &&other) noexcept
{
    if (this != &other) {
        Release();
        bufferInfo_ = other.bufferInfo_;
        clientImpl_ = other.clientImpl_;
        other.Reset();
    }
    return *this;
}

void DeviceBuffer::Reset()
{
    bufferInfo_.reset();
    clientImpl_.reset();
}

void DeviceBuffer::Detach()
{
    Release();
}

std::string DeviceBuffer::GetObjectKey()
{
    return this->bufferInfo_->devObjKey;
}

void DeviceBuffer::Release()
{
    Raii raii([this]() {
        bufferInfo_.reset();
        clientImpl_.reset();
    });
    if (clientImpl_ == nullptr) {
        return;
    }
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG_IF_ERROR(clientImpl_->RemoveP2PLocation(bufferInfo_->devObjKey, bufferInfo_->deviceIdx),
                 "Remove P2P Location failed");
}

DeviceBuffer::~DeviceBuffer()
{
    if (this->bufferInfo_->autoRelease) {
        Release();
    }
}

uint64_t DeviceBuffer::Size() const
{
    Blob blob;
    Status rc = deviceMemUnit_->CheckAndGetSingleBlob(blob);
    if (rc.IsOk()) {
        return blob.size;
    }
    LOG(ERROR) << rc.ToString();
    return 0;
}

void *DeviceBuffer::Data() const
{
    Blob blob;
    Status rc = deviceMemUnit_->CheckAndGetSingleBlob(blob);
    if (rc.IsOk()) {
        return blob.pointer;
    }
    LOG(ERROR) << rc.ToString();
    return nullptr;
}

int32_t DeviceBuffer::GetDeviceIdx() const
{
    return bufferInfo_->deviceIdx;
}

Status DeviceBuffer::Publish()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    CHECK_FAIL_RETURN_STATUS(!bufferInfo_->isPublished, K_OC_ALREADY_SEALED, "Device object is already published");
    CHECK_FAIL_RETURN_STATUS(!deviceMemUnit_->GetBlobsStorage().empty(), K_INVALID,
                             "The blobs can't be empty in device buffer");
    Status rc = clientImpl_->PublishDeviceObject(shared_from_this());
    if (rc.IsOk()) {
        bufferInfo_->isPublished = true;
    }
    return rc;
}

std::vector<Blob> DeviceBuffer::GetDevBlobList() const
{
    return deviceMemUnit_->GetBlobsStorage();
}

Status DeviceBuffer::GetSendStatus(std::vector<Future> &futureVec)
{
    return clientImpl_->GetSendStatus(shared_from_this(), futureVec);
}

std::shared_ptr<DeviceMemoryUnit> DeviceBuffer::GetDeviceMemUnit()
{
    return deviceMemUnit_;
}

}  // namespace datasystem