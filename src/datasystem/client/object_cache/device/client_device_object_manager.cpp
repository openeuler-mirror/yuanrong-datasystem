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

#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/common/device/ascend/acl_resource_manager.h"
#include "datasystem/client/object_cache/device/device_memory_unit.h"
#include "datasystem/client/object_cache/device/p2p_subscribe.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/object_cache/buffer_composer.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/client/hetero_cache/device_buffer.h"
#include "datasystem/object_cache/object_enum.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
ClientDeviceObjectManager::ClientDeviceObjectManager(ObjectClientImpl *impl)
    : devInterImpl_(acl::AclDeviceManager::Instance()), objClientImpl_(impl), clientDevOJTimeoutMs_(impl->timeoutMs_)
{
}

Status ClientDeviceObjectManager::Init()
{
    devInterImpl_ = acl::AclDeviceManager::Instance();
    std::shared_ptr<ClientWorkerApi> workerApi;
    RETURN_IF_NOT_OK(objClientImpl_->GetAvailableWorkerApi(workerApi));
    commFactory_ = std::make_shared<HcclCommFactory>(workerApi, &aclResourceMgr_);
    swapOutPool_ = std::make_unique<AsyncAclMemCopyPool>(&aclResourceMgr_);
    swapInPool_ = std::make_unique<AsyncAclMemCopyPool>(&aclResourceMgr_);
    return Status::OK();
}

Status ClientDeviceObjectManager::CreateDevBuffer(const std::string &devObjKey, uint64_t size, void *devPtr,
                                                  int32_t deviceIdx, std::shared_ptr<DeviceBuffer> &deviceBuffer)
{
    std::vector<DataInfo> dataInfoList{ { devPtr, DataType::DATA_TYPE_INT8, size } };
    auto bufferInfo =
        std::make_shared<DeviceBufferInfo>(devObjKey, deviceIdx, LifetimeType::REFERENCE, true, TransferType::HOST);
    return CreateDevBufferImpl(bufferInfo, dataInfoList, deviceBuffer);
}

Status ClientDeviceObjectManager::CreateDevBuffer(const std::string &devObjKey,
                                                  const std::vector<DataInfo> &dataInfoList, int32_t deviceIdx,
                                                  const CreateDeviceParam &param,
                                                  std::shared_ptr<DeviceBuffer> &deviceBuffer)
{
    auto bufferInfo = std::make_shared<DeviceBufferInfo>(devObjKey, deviceIdx, param.lifetime, param.cacheLocation,
                                                         TransferType::P2P);
    return CreateDevBufferImpl(bufferInfo, dataInfoList, deviceBuffer);
}

Status ClientDeviceObjectManager::CreateDevBufferImpl(std::shared_ptr<DeviceBufferInfo> bufferInfo,
                                                      const std::vector<DataInfo> &dataInfoList,
                                                      std::shared_ptr<DeviceBuffer> &deviceBuffer)
{
    // check input parameter
    RETURN_IF_NOT_OK(objClientImpl_->IsClientReady());
    CHECK_FAIL_RETURN_STATUS(!bufferInfo->devObjKey.empty(), K_INVALID, "The devObjKey is empty");
    CHECK_FAIL_RETURN_STATUS(!dataInfoList.empty(), K_INVALID, "The dataInfoList is empty");
    CHECK_FAIL_RETURN_STATUS(Validator::IsIdFormat(bufferInfo->devObjKey), K_INVALID,
                             "The devObjKey maybe contains illegal char(s) or the length of id is > 255.");

    int32_t deviceIdxNow = -1;
    RETURN_IF_NOT_OK_APPEND_MSG(devInterImpl_->GetDeviceIdx(deviceIdxNow),
                                "May not create context or set device in this thread.");
    auto &deviceIdx = bufferInfo->deviceIdx;
    if (deviceIdx < 0) {
        deviceIdx = deviceIdxNow;
    } else if (deviceIdx != deviceIdxNow) {
        RETURN_STATUS(K_RUNTIME_ERROR,
                      FormatString("The input deviceIdx(%s) is not the same as the deviceIdx which client used(%s).",
                                   deviceIdx, deviceIdxNow));
    }
    // avoid check fail return while some device memory have allocated and not release.
    for (auto &dataInfo : dataInfoList) {
        CHECK_FAIL_RETURN_STATUS(dataInfo.count > 0, K_INVALID, "The size value should be bigger than zero.");
    }

    auto memUnit = std::make_shared<DeviceMemoryUnit>(bufferInfo->devObjKey, dataInfoList);
    RETURN_IF_NOT_OK(memUnit->MallocDeviceMemoryIfUserNotSet());

    deviceBuffer = DeviceBuffer::CreateDeviceBuffer(bufferInfo, memUnit, objClientImpl_->shared_from_this());
    return Status::OK();
}

Status ClientDeviceObjectManager::PublishDeviceObject(const std::shared_ptr<DeviceBuffer> &buffer)
{
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
    DataInfo dataInfo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(buffer->GetDeviceMemUnit()->CheckAndGetSingleDataInfo(dataInfo),
                                     "The device object with host buffer just support single dataInfo for now");
    auto dataSize = dataInfo.Size();
    auto &devObjKey = bufferInfo->devObjKey;
    std::shared_ptr<Buffer> hostBuffer;
    RETURN_IF_NOT_OK(objClientImpl_->Create(devObjKey, dataSize, {}, hostBuffer));
    auto hostBufferInfo = ObjectClientImpl::GetBufferInfo(hostBuffer);
    bufferInfo->shmId = hostBufferInfo->shmId;
    bufferInfo->version = hostBufferInfo->version;
    RETURN_IF_NOT_OK(devInterImpl_->MemCopyD2H(hostBuffer->MutableData(), dataSize, dataInfo.devPtr, dataSize));
    std::shared_ptr<ClientWorkerApi> workerApi;
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
    RETURN_IF_NOT_OK(objClientImpl_->IsClientReady());
    RETURN_IF_NOT_OK(objClientImpl_->CheckStringVector(devObjKeys));
    if (devObjKeys.size() > 1 || !map.empty()) {
        RETURN_STATUS(K_INVALID,
                      "The resharding get is not supported now, please keep the devObjKeys only have one objectKey and "
                      "map is empty.");
    }

    std::vector<RpcMessage> payloads;
    GetDeviceObjectRspPb rsp;
    std::shared_ptr<ClientWorkerApi> workerApi;
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
        std::shared_ptr<client::MmapTableEntry> mmapEntry;
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
    RETURN_IF_NOT_OK(aclResourceMgr_.Init());
    tbb::concurrent_hash_map<int, std::shared_ptr<P2PSubscribe>>::accessor acc;
    if (!subscribeTable_.find(acc, deviceId)) {
        std::shared_ptr<ClientWorkerApi> workerApi;
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

void ClientDeviceObjectManager::WaitForDeleteKeys(const std::vector<std::string> &keys, int64_t timeoutMs,
                                                  std::vector<std::string> &failKeys)
{
    auto start = std::chrono::system_clock::now();
    for (const auto &key : keys) {
        for (auto &it : subscribeTable_) {
            auto elapse = std::chrono::system_clock::now() - start;
            auto remainMs = timeoutMs - std::chrono::duration_cast<std::chrono::milliseconds>(elapse).count();
            std::shared_ptr<P2PPutRequest> putRequest;
            if (it.second->GetPutRequest(key, putRequest)) {
                if (it.second->WaitForKeyDelete(key, remainMs).IsError()) {
                    failKeys.emplace_back(key);
                    break;
                };
            }
        }
    }
}

Status ClientDeviceObjectManager::AsyncGetDevBuffer(const std::vector<std::string> &devObjKeys,
                                                    std::vector<std::shared_ptr<DeviceBuffer>> &deviceBuffers,
                                                    std::vector<Future> &futureVec, int64_t prefetchTimeoutMs,
                                                    int64_t subTimeoutMs)
{
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

Status ClientDeviceObjectManager::MemCopyBetweenDevAndHost(const std::vector<std::vector<DataInfo>> &dataInfoList,
                                                           std::vector<Buffer *> &bufferList, aclrtMemcpyKind copyKind,
                                                           bool enableHugeTlb)
{
    DeviceBatchCopyHelper helper;
    RETURN_IF_NOT_OK(helper.Prepare(dataInfoList, bufferList, copyKind));
    auto deviceId = dataInfoList[0][0].deviceIdx;
    aclResourceMgr_.SetD2HPolicyByHugeTlb(enableHugeTlb);
    INJECT_POINT("NO_USE_FFTS", [this]() {
        aclResourceMgr_.SetPolicyDirect();
        return Status::OK();
    });
    if (copyKind == aclrtMemcpyKind::ACL_MEMCPY_DEVICE_TO_HOST) {
        auto policy = aclResourceMgr_.GetD2HPolicy();
        if (policy == MemcopyPolicy::FFTS || policy == MemcopyPolicy::HUGE_FFTS) {
            PrintGetPerfInfo(helper);
            return swapOutPool_->AclMemcpyBatchD2H(deviceId, helper.dstBuffers, helper.srcBuffers, helper.bufferMetas);
        } else {
            return swapOutPool_->AclMemcpyBatch(deviceId, helper.dstList, helper.dataSizeList, helper.srcList,
                                                helper.dataSizeList, copyKind, helper.batchSize);
        }
    }
    auto policy = aclResourceMgr_.GetH2DPolicy();
    if (policy == MemcopyPolicy::FFTS || policy == MemcopyPolicy::HUGE_FFTS) {
        return swapInPool_->AclMemcpyBatchH2D(deviceId, helper.srcBuffers, helper.dstBuffers, helper.bufferMetas);
    } else {
        return swapInPool_->AclMemcpyBatch(deviceId, helper.dstList, helper.dataSizeList, helper.srcList,
                                           helper.dataSizeList, copyKind, helper.batchSize);
    }
}

void ClientDeviceObjectManager::PrintGetPerfInfo(DeviceBatchCopyHelper &helper)
{
    BlobListInfo infoList;
    infoList.keyNums = helper.bufferMetas.size();
    int64_t blobSum = std::accumulate(helper.bufferMetas.begin(), helper.bufferMetas.end(), 0,
                                      [](int64_t total, const BufferMetaInfo &view) { return total + view.blobCount; });
    infoList.minBlobNums = std::min_element(helper.bufferMetas.begin(), helper.bufferMetas.end(),
                                            [](const BufferMetaInfo &view1, const BufferMetaInfo &view2) {
        return view1.blobCount < view2.blobCount;
    })
        ->blobCount;
    infoList.maxBlobNums = std::max_element(helper.bufferMetas.begin(), helper.bufferMetas.end(),
                                            [](const BufferMetaInfo &view1, const BufferMetaInfo &view2) {
        return view1.blobCount < view2.blobCount;
    })
        ->blobCount;
    infoList.avgBlobNums = blobSum / infoList.keyNums;

    infoList.totalSize = std::accumulate(helper.srcBuffers.begin(), helper.srcBuffers.end(), 0,
                                         [](int64_t total, const BufferView &view) { return total + view.size; });
    infoList.minBlockSize =
        std::min_element(helper.srcBuffers.begin(), helper.srcBuffers.end(),
                         [](const BufferView &view1, const BufferView &view2) { return view1.size < view2.size; })
                         ->size;
    infoList.maxBlockSize =
        std::max_element(helper.srcBuffers.begin(), helper.srcBuffers.end(),
                         [](const BufferView &view1, const BufferView &view2) { return view1.size < view2.size; })
                         ->size;
    infoList.avgBlockSize = infoList.totalSize / infoList.keyNums;
    LOG(INFO) << infoList.ToString(false);
}

void ClientDeviceObjectManager::SetThreadInterruptFlag2True()
{
    for (const auto &item : subscribeTable_) {
        item.second->SetThreadInterruptFlag2True();
    }
}

AsyncAclMemCopyPool::AsyncAclMemCopyPool(AclResourceManager *aclResourceMgr) : aclResourceMgr_(aclResourceMgr)
{
    copyPool_ = std::make_unique<ThreadPool>(1);
    fftsCopyPool_ = std::make_unique<ThreadPool>(1);
    const int h2hThreadCount = 2;
    h2hCopyPool_ = std::make_unique<ThreadPool>(h2hThreadCount);
    devInterImpl_ = acl::AclDeviceManager::Instance();
    for (size_t i = 0; i < AclHostMemMgr::GetPiplineNums(); i++) {
        aclrtStream copyStream = nullptr;
        copyStreams_.emplace_back(copyStream);
    }
}

Status AsyncAclMemCopyPool::AclMemcpyBatchD2H(uint32_t deviceId, const std::vector<BufferView> &hostBuffers,
                                              const std::vector<BufferView> &deviceBuffers,
                                              const std::vector<BufferMetaInfo> &metaInfos)
{
    PerfPoint point(PerfKey::CLIENT_D2H_MEMCPY_INIT);
    if (deviceNow_ != static_cast<int32_t>(deviceId)) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(devInterImpl_->SetDeviceIdx(deviceId), "Failed to init.");
    }
    RETURN_IF_NOT_OK(aclResourceMgr_->Init());
    point.RecordAndReset(PerfKey::CLIENT_D2H_MEMCPY_RUN);
    FftsPipelineD2HCopier copier(deviceId, aclResourceMgr_, metaInfos, h2hCopyPool_.get(), fftsCopyPool_.get());
    return copier.ExecuteMemcpy(hostBuffers, deviceBuffers);
}

Status AsyncAclMemCopyPool::AclMemcpyBatchH2D(uint32_t deviceId, const std::vector<BufferView> &hostBuffers,
                                              const std::vector<BufferView> &deviceBuffers,
                                              const std::vector<BufferMetaInfo> &metaInfos)
{
    PerfPoint point(PerfKey::CLIENT_H2D_MEMCPY_INIT);
    if (deviceNow_ != static_cast<int32_t>(deviceId)) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(devInterImpl_->SetDeviceIdx(deviceId), "Failed to init.");
    }
    RETURN_IF_NOT_OK(aclResourceMgr_->Init());
    point.RecordAndReset(PerfKey::CLIENT_H2D_MEMCPY_RUN);
    FftsPipelineH2DCopier copier(deviceId, aclResourceMgr_, metaInfos, h2hCopyPool_.get(), fftsCopyPool_.get());
    return copier.ExecuteMemcpy(deviceBuffers, hostBuffers);
}

Status AsyncAclMemCopyPool::AclMemcpyBatch(uint32_t deviceIdx, std::vector<void *> &dstList,
                                           std::vector<size_t> &destMaxList, std::vector<void *> &srcList,
                                           std::vector<size_t> &countList, aclrtMemcpyKind kind, size_t batchSize)
{
    auto fut =
        copyPool_->Submit([this, deviceIdx, &dstList, &destMaxList, &srcList, &countList, kind, batchSize]() -> Status {
            if (deviceNow_ != static_cast<int32_t>(deviceIdx)) {
                RETURN_IF_NOT_OK(devInterImpl_->SetDeviceIdx(deviceIdx));
                deviceNow_ = deviceIdx;
            }
            if (copyStreams_[0] == nullptr) {
                RETURN_IF_NOT_OK(devInterImpl_->RtCreateStream(&copyStreams_[0]));
            }
            Status ret;
            for (auto i = 0ul; i < batchSize; i++) {
                RETURN_IF_NOT_OK(devInterImpl_->aclrtMemcpyAsync(dstList[i], destMaxList[i], srcList[i], countList[i],
                                                                 kind, copyStreams_[0]));
            }
            return devInterImpl_->RtSynchronizeStream(copyStreams_[0]);
        });
    return fut.get();
}

AsyncAclMemCopyPool::~AsyncAclMemCopyPool()
{
    for (auto &stream : copyStreams_) {
        if (stream != nullptr) {
            LOG_IF_ERROR(devInterImpl_->RtDestroyStream(stream), "Destory stream failed.");
        }
    }
}
}  // namespace object_cache
}  // namespace datasystem
