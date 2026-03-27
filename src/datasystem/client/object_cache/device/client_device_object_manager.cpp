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
 * Description: The device object manager in client side.
 */
#include "datasystem/client/object_cache/device/client_device_object_manager.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/device/device_resource_manager_factory.h"
#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/client/object_cache/device/p2p_subscribe.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
ClientDeviceObjectManager::ClientDeviceObjectManager(ObjectClientImpl *impl)
    : devInterImpl_(DeviceManagerFactory::GetDeviceManager()),
      objClientImpl_(impl),
      clientDevOJTimeoutMs_(impl->requestTimeoutMs_)
{
    resourceMgr_ = DeviceResourceManagerFactory::Create();
}

Status ClientDeviceObjectManager::Init()
{
    devInterImpl_ = DeviceManagerFactory::GetDeviceManager();
    if (devInterImpl_ == nullptr || resourceMgr_ == nullptr) {
        LOG(INFO) << "Device runtime is unavailable during client initialization. "
                     "Regular KV/object operations remain available; device APIs will return an error when invoked.";
        return Status::OK();
    }
    std::shared_ptr<IClientWorkerApi> workerApi;
    RETURN_IF_NOT_OK(objClientImpl_->GetAvailableWorkerApi(workerApi));
    commFactory_ = std::make_shared<CommFactory>(workerApi, resourceMgr_.get());
    return Status::OK();
}

Status ClientDeviceObjectManager::CheckDeviceRuntimeAvailable(const char *operation) const
{
    CHECK_FAIL_RETURN_STATUS(devInterImpl_ != nullptr, K_RUNTIME_ERROR,
                             FormatString("%s requires a device runtime, but no device manager is available. "
                                          "Enable heterogeneous support or check the device runtime.",
                                          operation));
    CHECK_FAIL_RETURN_STATUS(resourceMgr_ != nullptr, K_RUNTIME_ERROR,
                             FormatString("%s requires a device runtime, but no device resource manager is available. "
                                          "Check runtime backend detection for GPU/NPU support.",
                                          operation));
    return Status::OK();
}

Status ClientDeviceObjectManager::CreateDevBuffer(const std::string &devObjKey, const DeviceBlobList &devBlobList,
                                                  const CreateDeviceParam &param,
                                                  std::shared_ptr<DeviceBuffer> &deviceBuffer)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("CreateDevBuffer"));
    auto bufferInfo = std::make_shared<DeviceBufferInfo>(devObjKey, devBlobList.deviceIdx, param.lifetime,
                                                         param.cacheLocation, TransferType::P2P);
    return CreateDevBufferImpl(bufferInfo, devBlobList, deviceBuffer);
}

Status ClientDeviceObjectManager::CreateDevBufferImpl(std::shared_ptr<DeviceBufferInfo> bufferInfo,
                                                      const DeviceBlobList &devBlobList,
                                                      std::shared_ptr<DeviceBuffer> &deviceBuffer)
{
    // check input parameter
    RETURN_IF_NOT_OK(objClientImpl_->IsClientReady());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo->devObjKey.empty(), K_INVALID, "The devObjKey is empty");
    CHECK_FAIL_RETURN_STATUS(
        !devBlobList.blobs.empty(), K_INVALID,
        "DeviceBlobList.blobs array cannot be empty. Please ensure at least one blob is provided for processing.");
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKey(bufferInfo->devObjKey));

    int32_t deviceIdxNow = -1;
    RETURN_IF_NOT_OK(devInterImpl_->GetDeviceIdx(deviceIdxNow));
    auto &deviceIdx = bufferInfo->deviceIdx;
    if (deviceIdx < 0) {
        deviceIdx = deviceIdxNow;
    } else if (deviceIdx != deviceIdxNow) {
        RETURN_STATUS(K_RUNTIME_ERROR,
                      FormatString("The input deviceIdx(%s) is not the same as the deviceIdx which client used(%s).",
                                   deviceIdx, deviceIdxNow));
    }
    // avoid check fail return while some device memory have allocated and not release.
    for (auto &blob : devBlobList.blobs) {
        CHECK_FAIL_RETURN_STATUS(blob.size > 0, K_INVALID, "The size value should be bigger than zero.");
    }

    auto memUnit = std::make_shared<DeviceMemoryUnit>(bufferInfo->devObjKey, devBlobList.blobs);
    RETURN_IF_NOT_OK(memUnit->MallocDeviceMemoryIfUserNotSet());

    deviceBuffer = DeviceBuffer::CreateDeviceBuffer(bufferInfo, memUnit, objClientImpl_->shared_from_this());
    return Status::OK();
}

Status ClientDeviceObjectManager::PublishDeviceObject(const std::shared_ptr<DeviceBuffer> &buffer)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("PublishDeviceObject"));
    if (buffer->bufferInfo_->transferType == TransferType::HOST) {
        return PublishDeviceObjectWithHost(buffer);
    } else if (buffer->bufferInfo_->transferType == TransferType::P2P) {
        return PublishDeviceObjectWithP2P(buffer);
    }
    RETURN_STATUS(K_RUNTIME_ERROR, "Invalid transfer type.");
}

Status ClientDeviceObjectManager::PublishDeviceObjectWithHost(const std::shared_ptr<DeviceBuffer> &buffer)
{
    auto &bufferInfo = buffer->bufferInfo_;
    Blob blob;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(buffer->GetDeviceMemUnit()->CheckAndGetSingleBlob(blob),
                                     "The device object with host buffer just support single blob for now");
    auto dataSize = blob.size;
    auto &devObjKey = bufferInfo->devObjKey;
    std::shared_ptr<Buffer> hostBuffer;
    RETURN_IF_NOT_OK(objClientImpl_->Create(devObjKey, dataSize, {}, hostBuffer));
    auto hostBufferInfo = ObjectClientImpl::GetBufferInfo(hostBuffer);
    bufferInfo->shmId = hostBufferInfo->shmId;
    bufferInfo->version = hostBufferInfo->version;
    RETURN_IF_NOT_OK(devInterImpl_->MemCopyD2H(hostBuffer->MutableData(), dataSize, blob.pointer, dataSize));
    std::shared_ptr<IClientWorkerApi> workerApi;
    RETURN_IF_NOT_OK(objClientImpl_->GetAvailableWorkerApi(workerApi));
    return workerApi->PublishDeviceObject(bufferInfo, dataSize, !bufferInfo->shmId.empty(), hostBuffer->MutableData());
}

Status ClientDeviceObjectManager::PublishDeviceObjectWithP2P(const std::shared_ptr<DeviceBuffer> &buffer)
{
    std::shared_ptr<P2PSubscribe> p2pSubscribe;
    auto &bufferInfo = buffer->bufferInfo_;
    RETURN_IF_NOT_OK(GetOrCreateP2PSubscribe(bufferInfo->deviceIdx, p2pSubscribe));
    return p2pSubscribe->PublishDeviceObject(buffer);
}

Status ClientDeviceObjectManager::GetDevBufferWithHost(const std::vector<std::string> &devObjKeys,
                                                       const ReShardingMap &map, int32_t timeoutMs,
                                                       DeviceBuffer &dstDevBuffer)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("GetDevBufferWithHost"));
    RETURN_IF_NOT_OK(objClientImpl_->IsClientReady());
    RETURN_IF_NOT_OK(ObjectClientImpl::CheckValidObjectKeyVector(devObjKeys));
    if (devObjKeys.size() > 1 || !map.empty()) {
        RETURN_STATUS(K_INVALID,
                      "The resharding get is not supported now, please keep the devObjKeys only have one objectKey and "
                      "map is empty.");
    }

    std::vector<RpcMessage> payloads;
    GetDeviceObjectRspPb rsp;
    std::shared_ptr<IClientWorkerApi> workerApi;
    RETURN_IF_NOT_OK(objClientImpl_->GetAvailableWorkerApi(workerApi));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        workerApi->GetDeviceObject(devObjKeys, dstDevBuffer.Size(), timeoutMs, rsp, payloads), "GetDeviceObject error");

    Status recvRc(static_cast<StatusCode>(rsp.last_rc().error_code()), rsp.last_rc().error_msg());

    if (!payloads.empty() && rsp.has_shm_info()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Receive object both from payload and ShmInfo");
    } else if (!payloads.empty()) {
        size_t offset = 0;
        for (const auto &part : payloads) {
            const auto destSize = std::min(dstDevBuffer.Size() - offset, part.Size());
            if (destSize < part.Size()) {
                RETURN_STATUS(K_RUNTIME_ERROR, FormatString("The destSize %d is smaller than part payload size: %d",
                                                            destSize, part.Size()));
            }
            RETURN_IF_NOT_OK(
                devInterImpl_->MemCopyH2D(static_cast<void *>(static_cast<uint8_t *>(dstDevBuffer.Data()) + offset),
                                          destSize, part.Data(), part.Size()));
            offset += part.Size();
        }
    } else if (rsp.has_shm_info()) {
        std::shared_ptr<client::IMmapTableEntry> mmapEntry;
        uint8_t *pointer;
        auto &shmInfo = rsp.shm_info();
        (void)objClientImpl_->MmapShmUnit(shmInfo.store_fd(), shmInfo.mmap_size(), shmInfo.offset(), mmapEntry,
                                          pointer);
        // copy to device buffer
        auto &bufferInfo = dstDevBuffer.bufferInfo_;
        bufferInfo->shmId = shmInfo.shm_id();
        bufferInfo->isPublished = true;
        bufferInfo->version = objClientImpl_->GetWorkerVersion();
        RETURN_IF_NOT_OK(devInterImpl_->MemCopyH2D(dstDevBuffer.Data(), dstDevBuffer.Size(),
                                                   pointer + shmInfo.metadata_size(), dstDevBuffer.Size()));
    } else {
        return recvRc.IsOk() ? Status(K_NOT_FOUND, "Cannot get objects from worker " + devObjKeys[0]) : recvRc;
    }
    return Status::OK();
}

Status ClientDeviceObjectManager::GetOrCreateP2PSubscribe(int32_t deviceId, std::shared_ptr<P2PSubscribe> &p2pSubscribe)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("GetOrCreateP2PSubscribe"));
    RETURN_IF_NOT_OK(resourceMgr_->EnsureInitialized());
    tbb::concurrent_hash_map<int, std::shared_ptr<P2PSubscribe>>::accessor acc;
    if (!subscribeTable_.find(acc, deviceId)) {
        std::shared_ptr<IClientWorkerApi> workerApi;
        RETURN_IF_NOT_OK(objClientImpl_->GetAvailableWorkerApi(workerApi));
        auto p2pSub = std::make_shared<P2PSubscribe>(deviceId, workerApi, commFactory_,
                                                     objClientImpl_->clientEnableP2Ptransfer_, clientDevOJTimeoutMs_);
        p2pSub->Init();
        (void)subscribeTable_.insert(acc, { deviceId, p2pSub });
    }
    RETURN_RUNTIME_ERROR_IF_NULL(acc->second);
    p2pSubscribe = acc->second;
    return Status::OK();
}

void ClientDeviceObjectManager::RemoveSubscribe(const std::string &key)
{
    for (auto &it : subscribeTable_) {
        it.second->RemoveSubscribe(key);
    }
}

Status ClientDeviceObjectManager::AsyncGetDevBuffer(const std::vector<std::string> &devObjKeys,
                                                    std::vector<std::shared_ptr<DeviceBuffer>> &deviceBuffers,
                                                    std::vector<Future> &futureVec, int64_t prefetchTimeoutMs,
                                                    int64_t subTimeoutMs)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("AsyncGetDevBuffer"));
    CHECK_FAIL_RETURN_STATUS(futureVec.empty(), K_INVALID,
                             "The input future vector is not empty, please clear it before async get.");
    std::shared_ptr<P2PSubscribe> p2pSubscribe;
    CHECK_FAIL_RETURN_STATUS(deviceBuffers.size() == devObjKeys.size(), K_INVALID,
                             "DevOjbIds size not equal to buffer size");

    for (size_t i = 0; i < deviceBuffers.size(); i++) {
        CHECK_FAIL_RETURN_STATUS(deviceBuffers[i] && deviceBuffers[i]->bufferInfo_, K_INVALID,
                                 "The DeviceBuffer pointer is empty, please create it before async get.");
        const auto &bufferInfo = deviceBuffers[i]->bufferInfo_;
        CHECK_FAIL_RETURN_STATUS(
            devObjKeys[i] == bufferInfo->devObjKey, K_INVALID,
            FormatString("The input device objectKey [%s] is not the same as objectKey in device buffer [%s].",
                         devObjKeys[i], bufferInfo->devObjKey));
        RETURN_IF_NOT_OK(deviceBuffers[i]->GetDeviceMemUnit()->CheckEmptyPointer());
        RETURN_IF_NOT_OK(GetOrCreateP2PSubscribe(bufferInfo->deviceIdx, p2pSubscribe));
    }
    return p2pSubscribe->AsyncGet(deviceBuffers, futureVec, prefetchTimeoutMs, subTimeoutMs);
}

Status ClientDeviceObjectManager::GetSendStatus(const std::shared_ptr<DeviceBuffer> &buffer,
                                                std::vector<Future> &futureVec)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("GetSendStatus"));
    const auto &bufferInfo = buffer->bufferInfo_;
    CHECK_FAIL_RETURN_STATUS(bufferInfo->lifetimeType == LifetimeType::MOVE, K_INVALID,
                             "The GetSendStatus interface is only useful when lifetime type is MOVE.");
    std::shared_ptr<P2PSubscribe> p2pSubscribe;
    RETURN_IF_NOT_OK(GetOrCreateP2PSubscribe(bufferInfo->deviceIdx, p2pSubscribe));
    Status rc = p2pSubscribe->GetSendStatus(bufferInfo->devObjKey, futureVec);
    /* After publishing, the master notifies the SDK to delete metadata if move semantics used. If user calls
     * GetSendStatus, the corresponding Future is created to check sending success. Due to parallelism, the peer may
     * receive the message before the sender reaches GetSendStatus, resulting in K_NOT_FOUND error. When K_NOT_FOUND and
     * isPublished, the message is deemed sent, returning true to the Future. */
    if (rc.GetCode() == K_NOT_FOUND && bufferInfo->isPublished) {
        auto prom = PromiseWithEvent(bufferInfo->devObjKey);
        prom.SetPromiseValue(Status::OK());
        prom.CreateEventAndFutureList(0, futureVec);
        return Status::OK();
    }
    return rc;
}

Status ClientDeviceObjectManager::MemCopyBetweenDevAndHost(const std::vector<DeviceBlobList> &devBlobList,
                                                           std::vector<Buffer *> &bufferList, MemcpyKind copyKind,
                                                           bool enableHugeTlb)
{
    RETURN_IF_NOT_OK(CheckDeviceRuntimeAvailable("MemCopyBetweenDevAndHost"));
    resourceMgr_->SetPolicyByHugeTlb(enableHugeTlb);

    INJECT_POINT("NO_USE_FFTS", [this]() {
        resourceMgr_->SetPolicyDirect();
        return Status::OK();
    });
    // caller makes sure that the copyKind is HOST_TO_DEVICE or DEVICE_TO_HOST
    if (copyKind == MemcpyKind::HOST_TO_DEVICE) {
        return resourceMgr_->MemcpyBatchH2D(devBlobList, bufferList);
    } else {
        return resourceMgr_->MemcpyBatchD2H(devBlobList, bufferList);
    }
}

void ClientDeviceObjectManager::SetThreadInterruptFlag2True()
{
    for (const auto &item : subscribeTable_) {
        item.second->SetThreadInterruptFlag2True();
    }
}

}  // namespace object_cache
}  // namespace datasystem
