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

#include "datasystem/common/device/ascend/acl_resource_manager.h"

#include <algorithm>
#include <exception>
#include <future>
#include <limits>
#include <new>
#include <optional>
#include <sstream>
#include <string>

#include "datasystem/common/device/ascend/ffts_dispatcher.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"

#define CHECK_ACL_RESULT(aclRet, apiName)                                                             \
    do {                                                                                              \
        int _aclRet = (aclRet);                                                                       \
        if (_aclRet != 0) {                                                                           \
            std::string errMsg = FormatString("%s api failed with error code %d ", apiName, _aclRet); \
            return Status(StatusCode::K_ACL_ERROR, __LINE__, __FILE__, errMsg);                       \
        }                                                                                             \
    } while (false)

namespace datasystem {
const size_t MAX_FFTS_TASKS_COUNT = 8;

// Sums the bytes of a ShmUnit pool with overflow protection. Returns empty when the running total would exceed
// uint64_t, so CacheFftsDeviceStaging can reject oversized pools without a hand-rolled overflow check at each call.
std::optional<uint64_t> CalcShmUnitPoolBytes(const std::vector<ShmUnit> &pool)
{
    uint64_t total = 0;
    for (const auto &unit : pool) {
        const auto unitSize = unit.GetSize();
        if (total > std::numeric_limits<uint64_t>::max() - unitSize) {
            return std::nullopt;
        }
        total += unitSize;
    }
    return total;
}
AclResourceManager::AclResourceManager()
{
    parallelH2DConfigStatus_ = parallelH2DConfig_.LoadFromEnv();
    LOG_IF_ERROR(parallelH2DConfigStatus_, "Load parallel H2D config failed");
    parallelD2HConfigStatus_ = parallelD2HConfig_.LoadFromEnv();
    LOG_IF_ERROR(parallelD2HConfigStatus_, "Load parallel D2H config failed");
    parallelFftsH2DConfigStatus_ = parallelFftsH2DConfig_.LoadFromEnv();
    LOG_IF_ERROR(parallelFftsH2DConfigStatus_, "Load parallel FFTS H2D config failed");
    parallelFftsD2HConfigStatus_ = parallelFftsD2HConfig_.LoadFromEnv();
    LOG_IF_ERROR(parallelFftsD2HConfigStatus_, "Load parallel FFTS D2H config failed");
    if (policyH2D == MemcopyPolicy::DIRECT && parallelH2DConfigStatus_.IsOk() && parallelH2DConfig_.workerNum > 1) {
        LOG(INFO) << parallelH2DConfig_.ToString();
    }
    if (policyD2H == MemcopyPolicy::DIRECT && parallelD2HConfigStatus_.IsOk() && parallelD2HConfig_.workerNum > 1) {
        LOG(INFO) << parallelD2HConfig_.ToString();
    }
    if ((policyH2D == MemcopyPolicy::FFTS || policyH2D == MemcopyPolicy::HUGE_FFTS)
        && parallelFftsH2DConfigStatus_.IsOk() && parallelFftsH2DConfig_.workerNum > 1) {
        LOG(INFO) << parallelFftsH2DConfig_.ToString();
    }
    if ((policyD2H == MemcopyPolicy::FFTS || policyD2H == MemcopyPolicy::HUGE_FFTS)
        && parallelFftsD2HConfigStatus_.IsOk() && parallelFftsD2HConfig_.workerNum > 1) {
        LOG(INFO) << parallelFftsD2HConfig_.ToString();
    }
    deviceResources_.reserve(MAX_DEVICE_COUNT);
    for (size_t deviceId = 0; deviceId < MAX_DEVICE_COUNT; deviceId++) {
        deviceResources_.emplace_back(std::make_unique<DeviceResource>(deviceId));
    }
    swapOutPool_ = std::make_unique<AclMemCopyPool>(this);
    swapInPool_ = std::make_unique<AclMemCopyPool>(this);
}

Status AclResourceManager::MemcpyBatchD2H(const std::vector<DeviceBlobList> &devBlobList,
                                          std::vector<Buffer *> &bufferList)
{
    DeviceBatchCopyHelper helper;
    RETURN_IF_NOT_OK(helper.Prepare(devBlobList, bufferList, MemcpyKind::DEVICE_TO_HOST));
    // Prepare before get deviceId in case that deviBlobList maybe empty
    auto deviceId = devBlobList[0].deviceIdx;
    helper.PrintGetPerfInfo(helper);
    return swapOutPool_->MemcpyBatchD2H(deviceId, helper, policyD2H);
}
Status AclResourceManager::MemcpyBatchH2D(const std::vector<DeviceBlobList> &devBlobList,
                                          std::vector<Buffer *> &bufferList)
{
    DeviceBatchCopyHelper helper;
    RETURN_IF_NOT_OK(helper.Prepare(devBlobList, bufferList, MemcpyKind::HOST_TO_DEVICE));
    auto deviceId = devBlobList[0].deviceIdx;
    helper.PrintGetPerfInfo(helper);
    return swapInPool_->MemcpyBatchH2D(deviceId, helper, policyH2D);
}

Status AclResourceManager::MemcpyBatchH2D(const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                                          const std::vector<Buffer *> &bufferList)
{
    DeviceBatchCopyHelper helper;
    PerfPoint preparePoint(PerfKey::CLIENT_H2D_HELPER_PREPARE);
    RETURN_IF_NOT_OK(helper.PrepareRefs(deviceBlobRefs, bufferList, MemcpyKind::HOST_TO_DEVICE));
    preparePoint.Record();
    auto deviceId = deviceBlobRefs[0]->deviceIdx;
    helper.PrintGetPerfInfo(helper);
    return swapInPool_->MemcpyBatchH2D(deviceId, helper, policyH2D);
}

Status AclResourceManager::MemcpyBatchD2H(const std::vector<const DeviceBlobList *> &deviceBlobRefs,
                                          const std::vector<Buffer *> &bufferList)
{
    DeviceBatchCopyHelper helper;
    PerfPoint preparePoint(PerfKey::CLIENT_D2H_HELPER_PREPARE);
    RETURN_IF_NOT_OK(helper.PrepareRefs(deviceBlobRefs, bufferList, MemcpyKind::DEVICE_TO_HOST));
    preparePoint.Record();
    auto deviceId = deviceBlobRefs[0]->deviceIdx;
    helper.PrintGetPerfInfo(helper);
    return swapOutPool_->MemcpyBatchD2H(deviceId, helper, policyD2H);
}

Status AclResourceManager::CreateAclRtStream(uint32_t deviceId, aclrtStream &stream, bool subscribeReport)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %zu, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    auto &deviceResource = deviceResources_[deviceId];
    return deviceResource->CreateAclRtStream(subscribeReport, stream);
}

Status AclResourceManager::FreeAclRtStream(uint32_t deviceId, aclrtStream stream, bool subscribeReport)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %zu, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    auto &deviceResource = deviceResources_[deviceId];
    return deviceResource->FreeAclRtStream(subscribeReport, stream);
}

Status AclResourceManager::CreateRtNotify(uint32_t deviceId, rtNotify_t &notify)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %u, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    auto &deviceResource = deviceResources_[deviceId];
    return deviceResource->CreateRtNotify(notify);
}

Status AclResourceManager::FreeRtNotify(uint32_t deviceId, rtNotify_t notify)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %zu, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    auto &deviceResource = deviceResources_[deviceId];
    return deviceResource->FreeRtNotify(notify);
}

Status AclResourceManager::CreateFftsDispatcher(uint32_t deviceId,
                                                std::unique_ptr<ffts::FftsDispatcher> &dispatcher)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %u, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    return deviceResources_[deviceId]->CreateFftsDispatcher(dispatcher);
}

void AclResourceManager::FreeFftsDispatcher(uint32_t deviceId,
                                            std::unique_ptr<ffts::FftsDispatcher> dispatcher)
{
    if (deviceId < MAX_DEVICE_COUNT) {
        deviceResources_[deviceId]->FreeFftsDispatcher(std::move(dispatcher));
    }
}

Status AclResourceManager::CreateFftsResourceBundle(uint32_t deviceId, bool subscribeReport,
                                                    std::unique_ptr<FftsResourceBundle> &bundle)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %u, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    return deviceResources_[deviceId]->CreateFftsResourceBundle(subscribeReport, bundle);
}

void AclResourceManager::FreeFftsResourceBundle(uint32_t deviceId, std::unique_ptr<FftsResourceBundle> bundle)
{
    if (bundle == nullptr) {
        return;
    }
    if (deviceId >= MAX_DEVICE_COUNT) {
        LOG(ERROR) << FormatString("Cannot free FFTS resource bundle for invalid device id %u", deviceId);
        return;
    }
    const size_t cacheSize = std::max(parallelFftsH2DConfig_.workerNum, parallelFftsD2HConfig_.workerNum);
    deviceResources_[deviceId]->FreeFftsResourceBundle(std::move(bundle), cacheSize);
}

Status AclResourceManager::AcquireFftsDeviceStaging(uint32_t deviceId,
                                                    const std::vector<BufferMetaInfo> &bufferMetas,
                                                    std::unique_ptr<std::vector<ShmUnit>> &memoryPool)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %u, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    size_t minSize = 0;
    for (const auto &meta : bufferMetas) {
        minSize = std::max(minSize, meta.size);
    }
    std::unique_ptr<std::vector<ShmUnit>> undersizedPool;
    memoryPool = deviceResources_[deviceId]->TakeFftsDeviceStaging(minSize, undersizedPool);
    if (memoryPool == nullptr) {
        if (undersizedPool != nullptr) {
            RETURN_IF_NOT_OK(Device()->Free(*undersizedPool));
        }
        memoryPool = std::make_unique<std::vector<ShmUnit>>(FFTS_PIPELINE);
        RETURN_IF_NOT_OK(Device()->Allocate(bufferMetas, *memoryPool));
    }
    return Status::OK();
}

Status AclResourceManager::ReleaseFftsDeviceStaging(uint32_t deviceId,
                                                    std::unique_ptr<std::vector<ShmUnit>> memoryPool)
{
    RETURN_OK_IF_TRUE(memoryPool == nullptr);
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %u, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    constexpr size_t maxCachedStagingPools = 8;
    // H2D and D2H share this per-device staging cache. Retain enough pools for the more parallel direction so one
    // direction's smaller worker count does not cause allocation churn in the other direction.
    const size_t maxParallelWorkers =
        std::max(parallelFftsH2DConfig_.workerNum, parallelFftsD2HConfig_.workerNum);
    const size_t cacheSize = std::min(maxParallelWorkers, maxCachedStagingPools);
    // Bound retained staging memory to a fraction of the device capacity. A pool larger than the budget is
    // released immediately, while smaller pools can still be reused without allowing the count-based cache to pin
    // an unbounded amount of HBM.
    constexpr uint64_t maxCachedStagingFraction = 8;
    const auto maxCacheBytes = GetDeviceMemSize() / maxCachedStagingFraction;
    auto poolToFree =
        deviceResources_[deviceId]->CacheFftsDeviceStaging(std::move(memoryPool), cacheSize, maxCacheBytes);
    if (poolToFree != nullptr) {
        RETURN_IF_NOT_OK(Device()->Free(*poolToFree));
    }
    return Status::OK();
}

Status AclResourceManager::DeviceResource::InitCallbackThread()
{
    {
        std::shared_lock<std::shared_timed_mutex> rlocker(mutex_);
        RETURN_OK_IF_TRUE(callbackThread_ != nullptr);
    }

    std::unique_lock<std::shared_timed_mutex> wlocker(mutex_);
    RETURN_OK_IF_TRUE(callbackThread_ != nullptr);
    callbackThread_ = std::make_unique<acl::CallbackThread>();
    return Status::OK();
}

Status AclResourceManager::DeviceResource::CreateFftsDispatcher(
    std::unique_ptr<ffts::FftsDispatcher> &dispatcher)
{
    {
        std::lock_guard<std::shared_timed_mutex> locker(mutex_);
        if (!fftsDispatcherQueue_.empty()) {
            dispatcher = std::move(fftsDispatcherQueue_.front());
            fftsDispatcherQueue_.pop_front();
            return Status::OK();
        }
    }
    auto aclDeviceManager = acl::AclDeviceManager::Instance();
    dispatcher = std::make_unique<ffts::FftsDispatcher>(deviceId_, aclDeviceManager);
    CHECK_ACL_RESULT(dispatcher->Init(), "FftsDispatcher init");
    CHECK_ACL_RESULT(dispatcher->CreateFftsCtxs(1), "FftsDispatcher CreateFftsCtxs");
    CHECK_ACL_RESULT(dispatcher->SetFftsCtx(0), "FftsDispatcher SetFftsCtx");
    return Status::OK();
}

void AclResourceManager::DeviceResource::FreeFftsDispatcher(
    std::unique_ptr<ffts::FftsDispatcher> dispatcher)
{
    if (dispatcher == nullptr) {
        return;
    }
    if (dispatcher->ReuseCtx(0) != HCCL_SUCCESS) {
        LOG(WARNING) << "Drop FFTS dispatcher because resetting its context failed";
        return;
    }
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    if (fftsDispatcherQueue_.size() < CACHE_SIZE) {
        fftsDispatcherQueue_.emplace_back(std::move(dispatcher));
    }
}

Status AclResourceManager::DeviceResource::CreateFftsResourceBundle(bool subscribeReport,
                                                                    std::unique_ptr<FftsResourceBundle> &bundle)
{
    CHECK_FAIL_RETURN_STATUS(bundle == nullptr, K_INVALID, "Output FFTS resource bundle must be empty");
    auto &queue = subscribeReport ? subscribeReportFftsResourceBundleQueue_ : fftsResourceBundleQueue_;
    {
        std::lock_guard<std::mutex> locker(fftsResourceBundleMutex_);
        if (!queue.empty()) {
            bundle = std::move(queue.front());
            queue.pop_front();
            return Status::OK();
        }
    }

    // A cache miss is a cold-path operation. Reuse the individual resource factories so partial failures retain the
    // existing cleanup semantics; after the first successful request the complete bundle is acquired with one lock.
    bundle = std::make_unique<FftsResourceBundle>();
    bundle->resource.subscribeReport = subscribeReport;
    auto rollback = [&bundle, this]() { FreeFftsResourceBundle(std::move(bundle), 0); };
    auto rc = CreateFftsDispatcher(bundle->dispatcher);
    if (rc.IsError()) {
        rollback();
        return rc;
    }
    rc = CreateAclRtStream(subscribeReport, bundle->resource.primaryStream);
    if (rc.IsError()) {
        rollback();
        return rc;
    }
    rc = CreateAclRtStream(false, bundle->resource.secondaryStream);
    if (rc.IsError()) {
        rollback();
        return rc;
    }
    for (size_t i = 0; i < FFTS_PIPELINE; ++i) {
        rc = CreateRtNotify(bundle->resource.toDestDone[i]);
        if (rc.IsError()) {
            rollback();
            return rc;
        }
        rc = CreateRtNotify(bundle->resource.toPinDone[i]);
        if (rc.IsError()) {
            rollback();
            return rc;
        }
    }
    return Status::OK();
}

void AclResourceManager::DeviceResource::FreeFftsResourceBundle(std::unique_ptr<FftsResourceBundle> bundle,
                                                                size_t cacheSize)
{
    if (bundle == nullptr) {
        return;
    }
    auto &resource = bundle->resource;
    bool complete =
        bundle->dispatcher != nullptr && resource.primaryStream != nullptr && resource.secondaryStream != nullptr;
    for (size_t i = 0; i < FFTS_PIPELINE; ++i) {
        complete = complete && resource.toDestDone[i] != nullptr && resource.toPinDone[i] != nullptr;
    }
    if (complete && bundle->dispatcher->ReuseCtx(0) == HCCL_SUCCESS) {
        auto &queue = resource.subscribeReport ? subscribeReportFftsResourceBundleQueue_ : fftsResourceBundleQueue_;
        try {
            std::lock_guard<std::mutex> locker(fftsResourceBundleMutex_);
            if (queue.size() < cacheSize) {
                queue.emplace_back(std::move(bundle));
                return;
            }
        } catch (const std::bad_alloc &) {
            LOG(WARNING) << "Cache FFTS resource bundle failed due to out of memory";
        }
    }

    // Partial bundles and excess cache entries are rare. Return their valid members through the individual pools so
    // no successfully created runtime handle is lost after a cold-path failure.
    LOG_IF_ERROR(FreeAclRtStream(resource.subscribeReport, resource.primaryStream), "FreeAclRtStream failed");
    LOG_IF_ERROR(FreeAclRtStream(false, resource.secondaryStream), "FreeAclRtStream failed");
    for (size_t i = 0; i < FFTS_PIPELINE; ++i) {
        LOG_IF_ERROR(FreeRtNotify(resource.toDestDone[i]), "FreeRtNotify failed");
        LOG_IF_ERROR(FreeRtNotify(resource.toPinDone[i]), "FreeRtNotify failed");
    }
    FreeFftsDispatcher(std::move(bundle->dispatcher));
}

std::unique_ptr<std::vector<ShmUnit>> AclResourceManager::DeviceResource::TakeFftsDeviceStaging(
    size_t minSize, std::unique_ptr<std::vector<ShmUnit>> &undersizedPool)
{
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    auto iter =
        std::find_if(fftsDeviceStagingQueue_.begin(), fftsDeviceStagingQueue_.end(), [minSize](const auto &pool) {
            return pool != nullptr && pool->size() == FFTS_PIPELINE && !pool->empty()
                   && pool->front().GetSize() >= minSize;
        });
    if (iter == fftsDeviceStagingQueue_.end()) {
        if (!fftsDeviceStagingQueue_.empty()) {
            undersizedPool = std::move(fftsDeviceStagingQueue_.front());
            fftsDeviceStagingQueue_.pop_front();
        }
        return nullptr;
    }
    auto pool = std::move(*iter);
    fftsDeviceStagingQueue_.erase(iter);
    return pool;
}

std::unique_ptr<std::vector<ShmUnit>> AclResourceManager::DeviceResource::CacheFftsDeviceStaging(
    std::unique_ptr<std::vector<ShmUnit>> memoryPool, size_t cacheSize, uint64_t maxCacheBytes)
{
    if (memoryPool == nullptr || memoryPool->size() != FFTS_PIPELINE || memoryPool->empty() || cacheSize == 0) {
        return memoryPool;
    }
    auto poolBytesOpt = CalcShmUnitPoolBytes(*memoryPool);
    if (!poolBytesOpt.has_value() || maxCacheBytes == 0 || *poolBytesOpt > maxCacheBytes) {
        return memoryPool;
    }
    auto poolBytes = *poolBytesOpt;
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    uint64_t cachedBytes = 0;
    for (const auto &pool : fftsDeviceStagingQueue_) {
        if (pool == nullptr) {
            continue;
        }
        for (const auto &unit : *pool) {
            const auto unitSize = unit.GetSize();
            if (cachedBytes > std::numeric_limits<uint64_t>::max() - unitSize) {
                return memoryPool;
            }
            cachedBytes += unitSize;
        }
    }
    if (fftsDeviceStagingQueue_.size() < cacheSize) {
        if (cachedBytes > maxCacheBytes - poolBytes) {
            return memoryPool;
        }
        fftsDeviceStagingQueue_.emplace_back(std::move(memoryPool));
        return nullptr;
    }
    auto smallest = std::min_element(fftsDeviceStagingQueue_.begin(), fftsDeviceStagingQueue_.end(),
                                     [](const auto &lhs, const auto &rhs) {
                                         return lhs->front().GetSize() < rhs->front().GetSize();
                                     });
    auto smallestBytesOpt = CalcShmUnitPoolBytes(**smallest);
    if (!smallestBytesOpt.has_value()) {
        return memoryPool;
    }
    auto smallestBytes = *smallestBytesOpt;
    if (smallestBytes < poolBytes && cachedBytes >= smallestBytes
        && cachedBytes - smallestBytes <= maxCacheBytes - poolBytes) {
        std::swap(*smallest, memoryPool);
    }
    return memoryPool;
}

Status AclResourceManager::DeviceResource::CreateAclRtStream(bool subscribeReport, aclrtStream &stream)
{
    auto &queue = subscribeReport ? subscribeReportStreamQueue_ : streamQueue_;
    if (subscribeReport) {
        RETURN_IF_NOT_OK(InitCallbackThread());
    }
    auto aclDeviceManager = acl::AclDeviceManager::Instance();
    std::lock_guard<std::shared_timed_mutex> wlocker(mutex_);
    if (queue.empty()) {
        RETURN_IF_NOT_OK(aclDeviceManager->RtCreateStream(&stream));
        if (subscribeReport) {
            RETURN_IF_NOT_OK(callbackThread_->SubscribeStream(stream));
        }
    } else {
        stream = queue.front();
        queue.pop_front();
    }
    return Status::OK();
}

Status AclResourceManager::DeviceResource::FreeAclRtStream(bool subscribeReport, aclrtStream stream)
{
    RETURN_OK_IF_TRUE(stream == nullptr);
    auto &queue = subscribeReport ? subscribeReportStreamQueue_ : streamQueue_;
    if (subscribeReport) {
        RETURN_IF_NOT_OK(InitCallbackThread());
    }

    auto aclDeviceManager = acl::AclDeviceManager::Instance();
    std::lock_guard<std::shared_timed_mutex> wlocker(mutex_);
    const size_t streamCacheSize = 8;
    if (queue.size() < streamCacheSize) {
        queue.emplace_back(stream);
    } else {
        if (subscribeReport) {
            RETURN_IF_NOT_OK(callbackThread_->UnSubscribeStream(stream));
        }
        RETURN_IF_NOT_OK(aclDeviceManager->RtDestroyStream(stream));
    }
    return Status::OK();
}

Status AclResourceManager::DeviceResource::CreateRtNotify(rtNotify_t &notify)
{
    auto aclDeviceManager = acl::AclDeviceManager::Instance();
    std::lock_guard<std::shared_timed_mutex> wlocker(mutex_);
    if (notifyQueue_.empty()) {
        RETURN_IF_NOT_OK(aclDeviceManager->RtNotifyCreate(deviceId_, &notify));
    } else {
        notify = notifyQueue_.front();
        notifyQueue_.pop_front();
    }
    return Status::OK();
}

Status AclResourceManager::DeviceResource::FreeRtNotify(rtNotify_t notify)
{
    RETURN_OK_IF_TRUE(notify == nullptr);
    auto aclDeviceManager = acl::AclDeviceManager::Instance();
    std::lock_guard<std::shared_timed_mutex> wlocker(mutex_);
    if (notifyQueue_.size() < CACHE_SIZE) {
        notifyQueue_.emplace_back(notify);
    } else {
        RETURN_IF_NOT_OK(aclDeviceManager->RtNotifyDestroy(notify));
    }
    return Status::OK();
}

AclMemCopyPool::AclMemCopyPool(AclResourceManager *resourceMgr) : resourceMgr_(resourceMgr)
{
    const size_t pipeLineNums = 2;
    copyPool_ = std::make_unique<ThreadPool>(1);
    fftsCopyPool_ = std::make_unique<ThreadPool>(1);
    const int h2hThreadCount = 2;
    h2hCopyPool_ = std::make_unique<ThreadPool>(h2hThreadCount);
    devInterImpl_ = acl::AclDeviceManager::Instance();
    for (size_t i = 0; i < pipeLineNums; i++) {
        aclrtStream copyStream = nullptr;
        copyStreams_.emplace_back(copyStream);
    }
}

bool AclMemCopyPool::ShouldFallbackToDirectForH2D(const DeviceBatchCopyHelper &helper, MemcopyPolicy policy)
{
    if (policy != MemcopyPolicy::FFTS && policy != MemcopyPolicy::HUGE_FFTS) {
        return false;
    }

    const auto d2hPolicy = resourceMgr_->GetD2HPolicy();
    const auto h2dPolicy = resourceMgr_->GetH2DPolicy();
    const auto hostMemSize = resourceMgr_->GetHostMemSize();
    const auto deviceMemSize = resourceMgr_->GetDeviceMemSize();
    const bool skipHostPinMemcpy = d2hPolicy == h2dPolicy && d2hPolicy == MemcopyPolicy::HUGE_FFTS;

    uint64_t totalObjectSize = 0;
    for (const auto &meta : helper.bufferMetas) {
        totalObjectSize += meta.size;
        // Divide instead of multiplying to avoid unsigned wrap-around.
        if (meta.size > deviceMemSize / FFTS_PIPELINE) {
            LOG(WARNING) << FormatString("Fallback h2dPolicy to DIRECT, device pin memory pool size %zu, need size %zu",
                                         static_cast<size_t>(deviceMemSize),
                                         static_cast<size_t>(meta.size * FFTS_PIPELINE));
            return true;
        }
        if (!skipHostPinMemcpy && totalObjectSize > hostMemSize) {
            LOG(WARNING) << FormatString("Fallback h2dPolicy to DIRECT, host pin memory pool size %zu, need size %zu",
                                         static_cast<size_t>(hostMemSize), static_cast<size_t>(totalObjectSize));
            return true;
        }
    }

    return false;
}

template <typename Config>
bool ShouldUseParallelDirectImpl(const DeviceBatchCopyHelper &helper, const Config &config)
{
    const size_t descriptorCount = helper.dataSizeList.size();
    if (config.workerNum <= 1 || descriptorCount <= config.aggregateNum) {
        return false;
    }
    const size_t taskCount = (descriptorCount - 1) / config.aggregateNum + 1;
    if (taskCount < config.workerNum) {
        return false;
    }

    uint64_t totalBytes = 0;
    for (auto size : helper.dataSizeList) {
        if (size > std::numeric_limits<uint64_t>::max() - totalBytes) {
            return true;
        }
        totalBytes += size;
    }
    return totalBytes >= config.minBytes;
}

bool AclMemCopyPool::ShouldUseParallelDirect(const DeviceBatchCopyHelper &helper, const ParallelH2DConfig &config) const
{
    return ShouldUseParallelDirectImpl(helper, config);
}

bool AclMemCopyPool::ShouldUseParallelDirect(const DeviceBatchCopyHelper &helper, const ParallelD2HConfig &config) const
{
    return ShouldUseParallelDirectImpl(helper, config);
}

template <typename Config>
bool ShouldUseParallelFftsImpl(const DeviceBatchCopyHelper &helper, const Config &config)
{
    if (config.workerNum <= 1 || helper.bufferMetas.size() < config.workerNum) {
        return false;
    }
    uint64_t totalBytes = 0;
    for (const auto &meta : helper.bufferMetas) {
        if (meta.size > std::numeric_limits<uint64_t>::max() - totalBytes) {
            return true;
        }
        totalBytes += meta.size;
    }
    return totalBytes >= config.minBytes;
}

bool AclMemCopyPool::ShouldUseParallelFfts(const DeviceBatchCopyHelper &helper,
                                           const ParallelFftsH2DConfig &config) const
{
    return ShouldUseParallelFftsImpl(helper, config);
}

bool AclMemCopyPool::ShouldUseParallelFfts(const DeviceBatchCopyHelper &helper,
                                           const ParallelFftsD2HConfig &config) const
{
    return ShouldUseParallelFftsImpl(helper, config);
}

Status AclMemCopyPool::MemcpyFftsH2DSerial(uint32_t deviceId, DeviceBatchCopyHelper &helper)
{
    return ExecuteFftsH2D(deviceId, helper, false, nullptr);
}

Status AclMemCopyPool::MemcpyFftsD2HSerial(uint32_t deviceId, DeviceBatchCopyHelper &helper)
{
    return ExecuteFftsD2H(deviceId, helper, false, nullptr);
}

Status AclMemCopyPool::ExecuteFftsH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper, bool parallelShard,
                                      ThreadPool *deviceSubmitPool)
{
    (void)deviceSubmitPool;
    FftsPipelineH2DCopier copier(deviceId, resourceMgr_, helper.bufferMetas, h2hCopyPool_.get(), fftsCopyPool_.get());
    return parallelShard ? copier.ExecuteMemcpyInlineFfts(helper.dstBuffers, helper.srcBuffers)
                         : copier.ExecuteMemcpy(helper.dstBuffers, helper.srcBuffers);
}

Status AclMemCopyPool::ExecuteFftsD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper, bool parallelShard,
                                      ThreadPool *deviceSubmitPool)
{
    auto *submitPool = parallelShard ? deviceSubmitPool : fftsCopyPool_.get();
    CHECK_FAIL_RETURN_STATUS(submitPool != nullptr, K_INVALID, "D2H FFTS device submit pool is null");
    try {
        FftsPipelineD2HCopier copier(deviceId, resourceMgr_, helper.bufferMetas, h2hCopyPool_.get(), submitPool);
        return copier.ExecuteMemcpy(helper.dstBuffers, helper.srcBuffers);
    } catch (const std::bad_alloc &) {
        return Status(K_OUT_OF_MEMORY, "Allocate FFTS D2H callback state failed");
    } catch (const std::exception &e) {
        return Status(K_RUNTIME_ERROR, FormatString("Create FFTS D2H copier failed: %s", e.what()));
    } catch (...) {
        return Status(K_RUNTIME_ERROR, "Create FFTS D2H copier failed with unknown exception");
    }
}

Status AclMemCopyPool::GetOrCreateParallelDirectExecutor(uint32_t deviceId, MemcpyKind kind,
                                                         std::shared_ptr<AclParallelDirectExecutor> &executor)
{
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %u, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));

    std::lock_guard<std::mutex> lock(parallelExecutorMutex_);
    auto &executors = kind == MemcpyKind::DEVICE_TO_HOST ? parallelD2HExecutors_ : parallelH2DExecutors_;
    auto iter = executors.find(deviceId);
    if (iter != executors.end()) {
        executor = iter->second;
        return Status::OK();
    }

    try {
        std::shared_ptr<AclParallelDirectExecutor> newExecutor;
        if (kind == MemcpyKind::DEVICE_TO_HOST) {
            newExecutor = std::make_shared<AclParallelDirectExecutor>(deviceId, devInterImpl_,
                                                                      resourceMgr_->GetParallelD2HConfig());
        } else {
            newExecutor = std::make_shared<AclParallelDirectExecutor>(deviceId, devInterImpl_,
                                                                      resourceMgr_->GetParallelH2DConfig());
        }
        RETURN_IF_NOT_OK(newExecutor->Init());
        executors.emplace(deviceId, newExecutor);
        executor = std::move(newExecutor);
    } catch (const std::bad_alloc &) {
        RETURN_STATUS(K_OUT_OF_MEMORY, "Allocate parallel direct executor failed");
    }
    return Status::OK();
}

Status AclMemCopyPool::GetOrCreateParallelFftsExecutor(MemcpyKind kind, AclParallelFftsExecutor *&executor)
{
    auto &holder = kind == MemcpyKind::DEVICE_TO_HOST ? parallelFftsD2HExecutor_ : parallelFftsH2DExecutor_;
    if (holder == nullptr) {
        try {
            if (kind == MemcpyKind::DEVICE_TO_HOST) {
                auto copyFunction = [this](uint32_t deviceId, DeviceBatchCopyHelper &helper, bool parallelShard,
                                           ThreadPool *deviceSubmitPool) {
                    return ExecuteFftsD2H(deviceId, helper, parallelShard, deviceSubmitPool);
                };
                holder = std::make_unique<AclParallelFftsExecutor>(resourceMgr_, devInterImpl_, std::move(copyFunction),
                                                                   resourceMgr_->GetParallelFftsD2HConfig());
            } else {
                auto copyFunction = [this](uint32_t deviceId, DeviceBatchCopyHelper &helper, bool parallelShard,
                                           ThreadPool *deviceSubmitPool) {
                    return ExecuteFftsH2D(deviceId, helper, parallelShard, deviceSubmitPool);
                };
                holder = std::make_unique<AclParallelFftsExecutor>(resourceMgr_, devInterImpl_, std::move(copyFunction),
                                                                   resourceMgr_->GetParallelFftsH2DConfig());
            }
        } catch (const std::bad_alloc &) {
            RETURN_STATUS(K_OUT_OF_MEMORY, "Allocate parallel FFTS executor failed");
        }
    }
    executor = holder.get();
    return Status::OK();
}

Status AclMemCopyPool::MemcpyBatchD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcopyPolicy policy)
{
    PerfPoint point(PerfKey::CLIENT_D2H_MEMCPY_INIT);
    if (policy == MemcopyPolicy::DIRECT) {
        return MemcpyBatchDirect(deviceId, helper, MemcpyKind::DEVICE_TO_HOST, point);
    }
    if (policy == MemcopyPolicy::FFTS || policy == MemcopyPolicy::HUGE_FFTS) {
        return MemcpyBatchFftsD2H(deviceId, helper, point);
    }
    PerfPoint directPoint(PerfKey::TOTAL_D2H_BATCH_MEMCPY);
    return AclMemcpyBatch(deviceId, helper, MemcpyKind::DEVICE_TO_HOST);
}

Status AclMemCopyPool::MemcpyBatchH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcopyPolicy policy)
{
    PerfPoint point(PerfKey::CLIENT_H2D_MEMCPY_INIT);
    if (policy == MemcopyPolicy::DIRECT) {
        return MemcpyBatchDirect(deviceId, helper, MemcpyKind::HOST_TO_DEVICE, point);
    }
    if (policy == MemcopyPolicy::FFTS || policy == MemcopyPolicy::HUGE_FFTS) {
        return MemcpyBatchFftsH2D(deviceId, helper, policy, point);
    }
    PerfPoint directPoint(PerfKey::TOTAL_H2D_BATCH_MEMCPY);
    return AclMemcpyBatch(deviceId, helper, MemcpyKind::HOST_TO_DEVICE);
}

Status AclMemCopyPool::MemcpyBatchDirect(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind kind,
                                         PerfPoint &point)
{
    const auto &configStatus = kind == MemcpyKind::DEVICE_TO_HOST ? resourceMgr_->GetParallelD2HConfigStatus()
                                                                  : resourceMgr_->GetParallelH2DConfigStatus();
    RETURN_IF_NOT_OK(configStatus);
    const bool useParallel = kind == MemcpyKind::DEVICE_TO_HOST
                                 ? ShouldUseParallelDirect(helper, resourceMgr_->GetParallelD2HConfig())
                                 : ShouldUseParallelDirect(helper, resourceMgr_->GetParallelH2DConfig());
    if (useParallel) {
        point.RecordAndReset(kind == MemcpyKind::DEVICE_TO_HOST ? PerfKey::CLIENT_D2H_MEMCPY_RUN
                                                                : PerfKey::CLIENT_H2D_MEMCPY_RUN);
        std::shared_ptr<AclParallelDirectExecutor> executor;
        RETURN_IF_NOT_OK(GetOrCreateParallelDirectExecutor(deviceId, kind, executor));
        return executor->MemcpyBatch(helper);
    }
    PerfPoint directPoint(kind == MemcpyKind::DEVICE_TO_HOST ? PerfKey::TOTAL_D2H_BATCH_MEMCPY
                                                             : PerfKey::TOTAL_H2D_BATCH_MEMCPY);
    return AclMemcpyBatch(deviceId, helper, kind);
}

Status AclMemCopyPool::MemcpyBatchFftsD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper, PerfPoint &point)
{
    RETURN_IF_NOT_OK(resourceMgr_->GetParallelFftsD2HConfigStatus());
    if (deviceNow_ != static_cast<int32_t>(deviceId)) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(devInterImpl_->SetDevice(deviceId), "Failed to init.");
    }
    RETURN_IF_NOT_OK(resourceMgr_->EnsureInitialized());
    point.RecordAndReset(PerfKey::CLIENT_D2H_MEMCPY_RUN);
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %zu, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    return HandleFftsResult(deviceId, helper, MemcpyKind::DEVICE_TO_HOST, RunFftsD2H(deviceId, helper));
}

Status AclMemCopyPool::MemcpyBatchFftsH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcopyPolicy policy,
                                          PerfPoint &point)
{
    RETURN_IF_NOT_OK(resourceMgr_->GetParallelFftsH2DConfigStatus());
    if (ShouldFallbackToDirectForH2D(helper, policy)) {
        PerfPoint directPoint(PerfKey::TOTAL_H2D_BATCH_MEMCPY);
        return AclMemcpyBatch(deviceId, helper, MemcpyKind::HOST_TO_DEVICE);
    }
    if (deviceNow_ != static_cast<int32_t>(deviceId)) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(devInterImpl_->SetDevice(deviceId), "Failed to init.");
    }
    RETURN_IF_NOT_OK(resourceMgr_->EnsureInitialized());
    point.RecordAndReset(PerfKey::CLIENT_H2D_MEMCPY_RUN);
    CHECK_FAIL_RETURN_STATUS(
        deviceId < MAX_DEVICE_COUNT, K_INVALID,
        FormatString("Invalid device id %zu, exceed max device id %zu", deviceId, MAX_DEVICE_COUNT));
    return HandleFftsResult(deviceId, helper, MemcpyKind::HOST_TO_DEVICE, RunFftsH2D(deviceId, helper));
}

Status AclMemCopyPool::RunFftsD2H(uint32_t deviceId, DeviceBatchCopyHelper &helper)
{
    if (resourceMgr_->GetParallelFftsD2HConfig().workerNum <= 1) {
        return MemcpyFftsD2HSerial(deviceId, helper);
    }
    std::lock_guard<std::mutex> parallelCallLock(parallelFftsCallMutex_);
    if (!ShouldUseParallelFfts(helper, resourceMgr_->GetParallelFftsD2HConfig())) {
        return MemcpyFftsD2HSerial(deviceId, helper);
    }
    AclParallelFftsExecutor *executor = nullptr;
    RETURN_IF_NOT_OK(GetOrCreateParallelFftsExecutor(MemcpyKind::DEVICE_TO_HOST, executor));
    return executor->Memcpy(deviceId, helper);
}

Status AclMemCopyPool::RunFftsH2D(uint32_t deviceId, DeviceBatchCopyHelper &helper)
{
    if (resourceMgr_->GetParallelFftsH2DConfig().workerNum <= 1) {
        return MemcpyFftsH2DSerial(deviceId, helper);
    }
    std::lock_guard<std::mutex> parallelCallLock(parallelFftsCallMutex_);
    if (!ShouldUseParallelFfts(helper, resourceMgr_->GetParallelFftsH2DConfig())) {
        return MemcpyFftsH2DSerial(deviceId, helper);
    }
    AclParallelFftsExecutor *executor = nullptr;
    RETURN_IF_NOT_OK(GetOrCreateParallelFftsExecutor(MemcpyKind::HOST_TO_DEVICE, executor));
    return executor->Memcpy(deviceId, helper);
}

Status AclMemCopyPool::HandleFftsResult(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind kind,
                                        Status fftsRc)
{
    if (fftsRc.GetCode() != K_OUT_OF_MEMORY) {
        return fftsRc;
    }
    const char *direction = kind == MemcpyKind::DEVICE_TO_HOST ? "d2h" : "h2d";
    LOG(WARNING) << FormatString("Fallback %sPolicy to DIRECT after FFTS OOM, status: %s", direction,
                                 fftsRc.ToString());
    return AclMemcpyBatch(deviceId, helper, kind);
}

Status AclMemCopyPool::AclMemcpyBatch(uint32_t deviceId, DeviceBatchCopyHelper &helper, MemcpyKind copyKind)
{
    size_t leftNum = helper.dataSizeList.size();
    size_t startIndex = 0;
    while (leftNum > 0) {
        auto batchNum = std::min(leftNum, ACL_MEMCPY_BATCH_LIMIT);
        size_t failedIdx = 0;
        auto res =
            devInterImpl_->MemcpyBatch(helper.dstList.data() + startIndex, helper.dataSizeList.data() + startIndex,
                                       helper.srcList.data() + startIndex, helper.dataSizeList.data() + startIndex,
                                       batchNum, copyKind, deviceId, &failedIdx);
        if (res.IsError()) {
            LOG(ERROR) << FormatString("AclMemcpyBatch return error , failed index:%lu", failedIdx) << "," << res;
            return res;
        }
        leftNum -= batchNum;
        startIndex += batchNum;
    }
    return Status::OK();
}

AclMemCopyPool::~AclMemCopyPool()
{
    // Hold the call mutex so no RunFfts* is mid-Memcpy on an executor being destroyed.
    {
        std::lock_guard<std::mutex> parallelCallLock(parallelFftsCallMutex_);
        parallelFftsH2DExecutor_.reset();
        parallelFftsD2HExecutor_.reset();
    }

    std::unordered_map<uint32_t, std::shared_ptr<AclParallelDirectExecutor>> h2dExecutors;
    std::unordered_map<uint32_t, std::shared_ptr<AclParallelDirectExecutor>> d2hExecutors;
    {
        std::lock_guard<std::mutex> lock(parallelExecutorMutex_);
        h2dExecutors.swap(parallelH2DExecutors_);
        d2hExecutors.swap(parallelD2HExecutors_);
    }
    h2dExecutors.clear();
    d2hExecutors.clear();
    for (auto &stream : copyStreams_) {
        if (stream != nullptr) {
            LOG_IF_ERROR(devInterImpl_->DestroyStream(stream), "Destory stream failed.");
        }
    }
}

FftsPipelineCopierBase::FftsPipelineCopierBase(int32_t deviceId, AclResourceManager *aclResourceMgr,
                                               const std::vector<BufferMetaInfo> &bufferMetas, ThreadPool *h2hCopyPool,
                                               ThreadPool *fftsCopyPool)
    : aclResourceMgr_(aclResourceMgr),
      deviceId_(deviceId),
      bufferMetas_(bufferMetas),
      h2hCopyPool_(h2hCopyPool),
      fftsCopyPool_(fftsCopyPool),
      transferHostPool_(std::make_unique<std::vector<ShmUnit>>()),
      transferDevicePool_(std::make_unique<std::vector<ShmUnit>>()),
      finishCount_(0)
{
    aclDeviceManager_ = acl::AclDeviceManager::Instance();
    skipH2HMemcpy_ = aclResourceMgr_->GetD2HPolicy() == aclResourceMgr_->GetH2DPolicy()
                     && aclResourceMgr_->GetD2HPolicy() == MemcopyPolicy::HUGE_FFTS;
}

FftsPipelineCopierBase::~FftsPipelineCopierBase()
{
    PerfPoint point(PerfKey::CLIENT_FREE_STREAM_NOTIFY);
    if (resource_.primaryStream != nullptr && !primaryStreamSynchronized_) {
        auto rc = WaitFinish();
        if (rc.IsError()) {
            // A callback may still be queued on this stream. Do not recycle the stream, notifies, staging buffers,
            // or FFTS descriptors until the runtime can prove that the stream is quiescent. Callback userData is
            // independently allocated and owns only a weak state reference, so releasing callbackState_ here breaks
            // the old state <-> callback-data cycle without leaving a callback with a dangling state pointer.
            LOG(ERROR) << "Abandon FFTS resources because synchronizing the primary stream failed: " << rc;
            callbackState_.reset();
            (void)fftsDispatcher_.release();
            (void)transferHostPool_.release();
            (void)transferDevicePool_.release();
            return;
        }
    }
    if (resourceBundle_ != nullptr) {
        resourceBundle_->resource = resource_;
        resourceBundle_->dispatcher = std::move(fftsDispatcher_);
        try {
            aclResourceMgr_->FreeFftsResourceBundle(deviceId_, std::move(resourceBundle_));
        } catch (const std::exception &e) {
            LOG(ERROR) << "Free FFTS resource bundle failed: " << e.what();
        }
    }
    point.RecordAndReset(PerfKey::CLIENT_FREE_TRANS_BUFFERS);
    LOG_IF_ERROR(aclResourceMgr_->Host()->Free(*transferHostPool_), "Free transferHostMem failed");
    if (cacheDeviceStaging_) {
        LOG_IF_ERROR(aclResourceMgr_->ReleaseFftsDeviceStaging(deviceId_, std::move(transferDevicePool_)),
                     "Release FFTS device staging failed");
    } else {
        LOG_IF_ERROR(aclResourceMgr_->Device()->Free(*transferDevicePool_), "Free transferDeviceMem failed");
    }
    point.Record();
}

Status FftsPipelineCopierBase::GetBufferViews(size_t count, const std::vector<ShmUnit> &memoryPool,
                                              std::vector<BufferView> &buffers)
{
    buffers.reserve(count);
    for (uint64_t i = 0; i < memoryPool.size(); i++) {
        buffers.emplace_back(BufferView{ .ptr = memoryPool[i].pointer, .size = memoryPool[i].size });
    }
    CHECK_FAIL_RETURN_STATUS(
        buffers.size() == count, K_RUNTIME_ERROR,
        FormatString("key count mismatch: allocate memory count %zu, expect count %zu", buffers.size(), count));
    return Status::OK();
}

Status FftsPipelineCopierBase::AllocAndInitTransferBuffers(const std::vector<BufferView> &hostBuffer)
{
    size_t count = bufferMetas_.size();
    std::vector<ShmUnit> transferHostPool(count);
    std::vector<ShmUnit> transferDevicePool(FFTS_PIPELINE);
    *transferHostPool_ = std::move(transferHostPool);
    *transferDevicePool_ = std::move(transferDevicePool);

    if (skipH2HMemcpy_) {
        transferHostBuffers_.clear();
        transferHostBuffers_.assign(hostBuffer.begin(), hostBuffer.end());
    } else {
        RETURN_IF_NOT_OK(aclResourceMgr_->Host()->Allocate(bufferMetas_, *transferHostPool_));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetBufferViews(count, *transferHostPool_, transferHostBuffers_),
                                         "GetBufferViews for host buffer failed.");
    }

    if (skipH2HMemcpy_) {
        RETURN_IF_NOT_OK(aclResourceMgr_->AcquireFftsDeviceStaging(deviceId_, bufferMetas_, transferDevicePool_));
        cacheDeviceStaging_ = true;
    } else {
        RETURN_IF_NOT_OK(aclResourceMgr_->Device()->Allocate(bufferMetas_, *transferDevicePool_));
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetBufferViews(FFTS_PIPELINE, *transferDevicePool_, transferDeviceBuffers_),
                                     "GetBufferViews for device buffer failed.");

    return Status::OK();
}

Status FftsPipelineCopierBase::InitAclResource(bool subscribeReport)
{
    PerfPoint point(PerfKey::CLIENT_SET_DEVICE_IDX);
    // FFTS submit pools own their threads, so their device binding remains valid between requests.
    thread_local acl::AclDeviceManager *boundManager = nullptr;
    thread_local int32_t boundDeviceId = -1;
    if (boundManager != aclDeviceManager_ || boundDeviceId != deviceId_) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclDeviceManager_->SetDeviceIdx(deviceId_), "SetDeviceIdx failed");
        boundManager = aclDeviceManager_;
        boundDeviceId = deviceId_;
    }
    point.RecordAndReset(PerfKey::CLIENT_FFTS_INIT);
    auto rc = aclResourceMgr_->CreateFftsResourceBundle(deviceId_, subscribeReport, resourceBundle_);
    if (resourceBundle_ != nullptr) {
        resource_ = resourceBundle_->resource;
        fftsDispatcher_ = std::move(resourceBundle_->dispatcher);
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "CreateFftsResourceBundle failed");
    return Status::OK();
}

Status FftsPipelineCopierBase::NotifyStart()
{
    primaryStreamSynchronized_ = false;
    for (size_t i = 0; i < FFTS_PIPELINE; i++) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            aclDeviceManager_->RtNotifyRecord(resource_.toDestDone[i], resource_.secondaryStream),
            "RtNotifyRecord failed.");
    }
    return Status::OK();
}

Status FftsPipelineCopierBase::WaitFinish()
{
    auto rc = aclDeviceManager_->RtSynchronizeStream(resource_.primaryStream);
    if (rc.IsOk()) {
        primaryStreamSynchronized_ = true;
    }
    return rc;
}

FftsPipelineH2DCopier::FftsPipelineH2DCopier(int32_t deviceId, AclResourceManager *aclResourceMgr,
                                             const std::vector<BufferMetaInfo> &bufferMetas, ThreadPool *h2hCopyPool,
                                             ThreadPool *fftsCopyPool)
    : FftsPipelineCopierBase(deviceId, aclResourceMgr, bufferMetas, h2hCopyPool, fftsCopyPool),
      blobOffset_(0),
      submitCount_(0)
{
}

Status FftsPipelineH2DCopier::AddFftsNotifyTask(size_t index, const std::vector<BufferView> &deviceBuffers,
                                                bool addTask)
{
    PerfPoint point(PerfKey::CLIENT_H2D_H2H_NOTIFY);
    std::unique_lock<std::mutex> locker(mutex_);
    VLOG(1) << FormatString("AddTaskForH2D[%d] with index %ld and buffer size is %ld", addTask, index,
                            deviceBuffers.size());
    if (addTask) {
        AddTask(index, deviceBuffers);
    }
    finishCount_++;
    cv_.notify_one();
    point.Record();
    return Status::OK();
}

Status FftsPipelineH2DCopier::ExecuteMemcpy(const std::vector<BufferView> &deviceBuffers,
                                            const std::vector<BufferView> &hostBuffers)
{
    return ExecuteMemcpyImpl(deviceBuffers, hostBuffers, false);
}

Status FftsPipelineH2DCopier::ExecuteMemcpyInlineFfts(const std::vector<BufferView> &deviceBuffers,
                                                      const std::vector<BufferView> &hostBuffers)
{
    return ExecuteMemcpyImpl(deviceBuffers, hostBuffers, true);
}

Status FftsPipelineH2DCopier::ExecuteMemcpyImpl(const std::vector<BufferView> &deviceBuffers,
                                                const std::vector<BufferView> &hostBuffers, bool inlineFfts)
{
    PerfPoint point(PerfKey::CLIENT_H2D_ALLOC_TRANS_BUFFERS);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AllocAndInitTransferBuffers(hostBuffers), "AllocAndInitTransferBuffers failed");
    point.Record();
    std::vector<std::future<Status>> futs;
    futs.reserve(hostBuffers.size());
    point.RecordAndReset(PerfKey::CLIENT_H2D_H2H_ALL);
    for (size_t i = 0; i < hostBuffers.size(); i++) {
        if (skipH2HMemcpy_) {
            AddFftsNotifyTask(i, deviceBuffers);
            continue;
        }
        futs.emplace_back(h2hCopyPool_->Submit([this, i, &hostBuffers, &deviceBuffers] {
            PerfPoint::RecordElapsed(PerfKey::CLIENT_H2D_H2H_MEMLEN, hostBuffers[i].size);
            PerfPoint point(PerfKey::CLIENT_H2D_H2H_MEMCPY);
            Status rc = aclResourceMgr_->Host()->HostMemoryCopy(
                transferHostBuffers_[i].ptr, transferHostBuffers_[i].size, hostBuffers[i].ptr, hostBuffers[i].size);
            point.RecordAndReset(PerfKey::CLIENT_H2D_H2H_NOTIFY);
            AddFftsNotifyTask(i, deviceBuffers, rc.IsOk());
            return rc;
        }));
    }

    std::future<Status> h2dFut;
    Status h2dRc;
    if (inlineFfts) {
        h2dRc = RunFfts();
    } else {
        h2dFut = fftsCopyPool_->Submit([this] { return RunFfts(); });
    }

    Status lastRc;
    for (auto &fut : futs) {
        auto rc = fut.get();
        lastRc = rc.IsError() ? rc : lastRc;
    }
    point.RecordAndReset(PerfKey::CLIENT_H2D_FFTS_TAIL_WAIT);
    if (!inlineFfts) {
        h2dRc = h2dFut.get();
    }
    point.Record();
    if (h2dRc.IsError()) {
        lastRc = h2dRc;
    }
    return lastRc;
}

Status FftsPipelineH2DCopier::RunFfts()
{
    PerfPoint point(PerfKey::CLIENT_H2D_FFTS_INIT);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitAclResource(false), "InitDeviceResource failed");
    point.RecordAndReset(PerfKey::CLIENT_H2D_FFTS_NOTIFY_START);
    RETURN_IF_NOT_OK(NotifyStart());
    point.RecordAndReset(PerfKey::CLIENT_H2D_FFTS_WAIT_AND_RUN);
    bool finished = false;
    while (!finished) {
        PipelineH2DTasks tasks;
        {
            std::unique_lock<std::mutex> locker(mutex_);
            cv_.wait(locker, [this] { return !tasks_.IsEmpty() || IsFinish(); });
            finished = IsFinish() && tasks_.IsEmpty();
            if (!finished) {
                std::swap(tasks, tasks_);
                blobOffset_ = 0;
            }
        }
        if (finished || tasks.IsEmpty()) {
            continue;
        }
        PerfPoint::RecordElapsed(PerfKey::CLIENT_H2D_FFTS_TASK_COUNT, tasks.srcBuffers.size());
        PerfPoint submitPoint(PerfKey::CLIENT_H2D_FFTS_SUBMIT);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SubmitToStream(tasks.srcBuffers, tasks.destBuffers, tasks.bufferMetas),
                                         "SubmitToStreamForH2D failed");
    }
    PerfPoint waitPoint(PerfKey::CLIENT_H2D_FFTS_WAIT_FINISH);
    VLOG(1) << "Start WaitFinish";
    return WaitFinish();
}

void FftsPipelineH2DCopier::AddTask(size_t index, const std::vector<BufferView> &deviceBuffers)
{
    auto firstBlobOffset = bufferMetas_[index].firstBlobOffset;
    auto blobCount = bufferMetas_[index].blobCount;
    auto objectSize = bufferMetas_[index].size;
    tasks_.srcBuffers.emplace_back(
        BufferView{ .ptr = transferHostBuffers_[index].ptr, .size = transferHostBuffers_[index].size });
    for (size_t blobIndex = firstBlobOffset; blobIndex < blobCount + firstBlobOffset; blobIndex++) {
        tasks_.destBuffers.emplace_back(
            BufferView{ .ptr = deviceBuffers[blobIndex].ptr, .size = deviceBuffers[blobIndex].size });
    }
    tasks_.bufferMetas.emplace_back(
        BufferMetaInfo{ .blobCount = blobCount, .firstBlobOffset = blobOffset_, .size = objectSize });
    blobOffset_ += blobCount;
}

Status FftsPipelineH2DCopier::SubmitToStream(const std::vector<BufferView> &srcBuffers,
                                             const std::vector<BufferView> &destBuffers,
                                             const std::vector<BufferMetaInfo> &bufferMetas)
{
    auto srcCount = srcBuffers.size();
    auto destCount = destBuffers.size();
    auto toPinDone = resource_.toPinDone;
    auto toDestDone = resource_.toDestDone;
    auto primaryStream = resource_.primaryStream;
    auto secondaryStream = resource_.secondaryStream;
    CHECK_FAIL_RETURN_STATUS(
        srcBuffers.size() == bufferMetas.size(), K_RUNTIME_ERROR,
        FormatString("The source size %zu and meta size %zu mismatch.", srcBuffers.size(), bufferMetas.size()));
    for (size_t i = 0; i < srcCount; i++) {
        size_t pipelineIndex = submitCount_ % FFTS_PIPELINE;
        submitCount_++;
        auto hostPinBuffer = srcBuffers[i].ptr;
        auto hostPinBufferSize = srcBuffers[i].size;

        auto devicePinBuffer = transferDeviceBuffers_[pipelineIndex].ptr;
        auto devicePinBufferSize = transferDeviceBuffers_[pipelineIndex].size;
        auto objectSize = bufferMetas[i].size;
        CHECK_FAIL_RETURN_STATUS(
            objectSize <= devicePinBufferSize, K_RUNTIME_ERROR,
            FormatString("The devicePinBuffer size %zu too small, expect size %zu", devicePinBufferSize, objectSize));

        CHECK_FAIL_RETURN_STATUS(
            objectSize <= hostPinBufferSize, K_RUNTIME_ERROR,
            FormatString("The hostPinBuffer size %zu too small, expect size %zu", hostPinBufferSize, objectSize));

        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyWait(toDestDone[pipelineIndex], secondaryStream));

        RETURN_IF_NOT_OK(aclDeviceManager_->aclrtMemcpyAsync(devicePinBuffer, devicePinBufferSize, hostPinBuffer,
                                                             objectSize, ACL_MEMCPY_HOST_TO_DEVICE, secondaryStream));

        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyRecord(toPinDone[pipelineIndex], secondaryStream));

        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyWait(toPinDone[pipelineIndex], primaryStream));
        std::vector<int32_t> lastTaskId(MAX_FFTS_TASKS_COUNT, -1);

        auto deviceBlobCount = bufferMetas[i].blobCount;
        auto firstDeviceBlobIndex = bufferMetas[i].firstBlobOffset;
        size_t offsetInDevicePinBuffer = 0;
        for (size_t n = 0; n < deviceBlobCount; n++) {
            size_t deviceBlobIndex = firstDeviceBlobIndex + n;
            CHECK_FAIL_RETURN_STATUS(
                deviceBlobIndex < destCount, K_RUNTIME_ERROR,
                FormatString("The deviceBlobIndex %zu exceed the destCount %zu", deviceBlobIndex, destCount));
            auto deviceBlobPtr = destBuffers[deviceBlobIndex].ptr;
            size_t deviceBlobSize = destBuffers[deviceBlobIndex].size;
            void *devicePinBlobPtr =
                static_cast<void *>(static_cast<uint8_t *>(devicePinBuffer) + offsetInDevicePinBuffer);
            offsetInDevicePinBuffer += deviceBlobSize;
            CHECK_FAIL_RETURN_STATUS(offsetInDevicePinBuffer <= devicePinBufferSize, K_RUNTIME_ERROR,
                                     FormatString("The devicePinBuffer size %zu too small, expect size %zu",
                                                  devicePinBufferSize, offsetInDevicePinBuffer));
            uint32_t memcpyTaskId = 0;
            CHECK_ACL_RESULT(
                fftsDispatcher_->MemcpyAsync(deviceBlobPtr, devicePinBlobPtr, deviceBlobSize, &memcpyTaskId),
                "ffts MemcpyAsync");
            auto taskIdIndex = n % MAX_FFTS_TASKS_COUNT;
            if (lastTaskId[taskIdIndex] >= 0) {
                CHECK_ACL_RESULT(fftsDispatcher_->AddTaskDependency(lastTaskId[taskIdIndex], memcpyTaskId),
                                 "ffts AddTaskDependency");
            }
            lastTaskId[taskIdIndex] = static_cast<int32_t>(memcpyTaskId);
        }
        CHECK_ACL_RESULT(
            fftsDispatcher_->LaunchFftsTask(primaryStream, std::min(deviceBlobCount, MAX_FFTS_TASKS_COUNT), 0),
            "ffts LaunchFftsTask");
        CHECK_ACL_RESULT(fftsDispatcher_->ReuseCtx(0), "ffts ReuseCtx");
        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyRecord(toDestDone[pipelineIndex], primaryStream));
    }
    return Status::OK();
}

FftsPipelineD2HCopier::FftsPipelineD2HCopier(int32_t deviceId, AclResourceManager *aclResourceMgr,
                                             const std::vector<BufferMetaInfo> &bufferMetas, ThreadPool *h2hCopyPool,
                                             ThreadPool *fftsCopyPool)
    : FftsPipelineCopierBase(deviceId, aclResourceMgr, bufferMetas, h2hCopyPool, fftsCopyPool)
{
    if (!skipH2HMemcpy_) {
        callbackState_ = std::make_shared<D2HCallbackState>(bufferMetas.size());
        auto *storage = new D2HCallbackDataStorage(bufferMetas.size());
        callbackDataStorage_ = std::shared_ptr<D2HCallbackDataStorage>(storage, [](auto *value) {
            value->Release();
        });
    }
}

Status FftsPipelineD2HCopier::ExecuteMemcpy(const std::vector<BufferView> &hostBuffers,
                                            const std::vector<BufferView> &deviceBuffers)
{
    PerfPoint point(PerfKey::CLIENT_D2H_ALLOC_TRANS_BUFFERS);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AllocAndInitTransferBuffers(hostBuffers), "AllocAndInitTransferBuffers failed");
    point.Record();
    Timer afterFftsFinishTimer;
    auto d2hTask = [this, &deviceBuffers, &afterFftsFinishTimer] {
        PerfPoint point(PerfKey::CLIENT_D2H_FFTS_INIT);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitAclResource(!skipH2HMemcpy_), "InitDeviceResource failed");
        point.RecordAndReset(PerfKey::CLIENT_D2H_FFTS_NOTIFY_START);
        RETURN_IF_NOT_OK(NotifyStart());
        point.RecordAndReset(PerfKey::CLIENT_D2H_FFTS_SUBMIT);
        auto submitRc = SubmitToStream(deviceBuffers, transferDeviceBuffers_, transferHostBuffers_, bufferMetas_);
        point.RecordAndReset(PerfKey::CLIENT_D2H_FFTS_WAIT_FINISH);
        VLOG(1) << "Start WaitFinish";
        // aclrtLaunchCallback is asynchronous. Even after a partial submission failure, synchronize every callback
        // that was successfully queued before allowing callback state or stream resources to be reclaimed.
        auto syncRc = WaitFinish();
        point.Record();
        afterFftsFinishTimer.Reset();
        return submitRc.IsError() ? submitRc : syncRc;
    };

    std::future<Status> d2hFut;
    try {
        d2hFut = fftsCopyPool_->Submit([this, &d2hTask] {
            Status rc;
            try {
                rc = d2hTask();
            } catch (const std::bad_alloc &) {
                rc = Status(K_OUT_OF_MEMORY, "FFTS D2H device task failed due to out of memory");
            } catch (const std::exception &e) {
                rc = Status(K_RUNTIME_ERROR, FormatString("FFTS D2H device task threw exception: %s", e.what()));
            } catch (...) {
                rc = Status(K_RUNTIME_ERROR, "FFTS D2H device task threw unknown exception");
            }
            if (rc.IsError()) {
                LOG(ERROR) << "force finish with rc:" << rc.GetMsg();
                ForceFinish();
            }
            return rc;
        });
    } catch (const std::bad_alloc &) {
        RETURN_STATUS(K_OUT_OF_MEMORY, "Submit FFTS D2H device task failed");
    } catch (const std::exception &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Submit FFTS D2H device task failed: %s", e.what()));
    } catch (...) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Submit FFTS D2H device task failed with unknown exception");
    }

    auto waitD2H = [&d2hFut] {
        try {
            return d2hFut.get();
        } catch (const std::bad_alloc &) {
            return Status(K_OUT_OF_MEMORY, "FFTS D2H device task failed due to out of memory");
        } catch (const std::exception &e) {
            return Status(K_RUNTIME_ERROR, FormatString("FFTS D2H device task threw exception: %s", e.what()));
        } catch (...) {
            return Status(K_RUNTIME_ERROR, "FFTS D2H device task threw unknown exception");
        }
    };
    // HUGE_FFTS writes directly into the destination huge-page buffers. There is no H2H phase to wake or schedule,
    // so wait only for the device-submit task and avoid callback state, condition variables, and H2H futures.
    if (skipH2HMemcpy_) {
        return waitD2H();
    }

    std::vector<std::future<Status>> futs;
    try {
        futs.reserve(hostBuffers.size());
    } catch (const std::bad_alloc &) {
        ForceFinish();
        (void)waitD2H();
        RETURN_STATUS(K_OUT_OF_MEMORY, "Reserve D2H H2H futures failed");
    }

    Status submitRc;
    while (true) {
        PerfPoint point(PerfKey::CLIENT_D2H_H2H_WAIT_START);
        PipelineH2HTasks tasks;
        {
            std::unique_lock<std::mutex> locker(callbackState_->mutex);
            callbackState_->cv.wait(locker, [this] {
                return callbackState_->forced || !callbackState_->tasks.IsEmpty()
                       || callbackState_->finishCount >= callbackState_->expectedCount;
            });
            point.Record();
            if (callbackState_->forced) {
                break;
            }
            if (callbackState_->finishCount >= callbackState_->expectedCount && callbackState_->tasks.IsEmpty()) {
                VLOG(1) << "h2h finish";
                break;
            }
            std::swap(tasks, callbackState_->tasks);
        }
        if (tasks.IsEmpty() || skipH2HMemcpy_ || submitRc.IsError()) {
            continue;
        }
        PerfPoint::RecordElapsed(PerfKey::CLIENT_D2H_H2H_TASK_COUNT, tasks.indexes.size());
        for (auto index : tasks.indexes) {
            try {
                futs.emplace_back(h2hCopyPool_->Submit([this, index, hostBuffers] {
                    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                        index < bufferMetas_.size(), K_RUNTIME_ERROR,
                        FormatString("Invalid index %zu, out range of [0, %zu)", index, bufferMetas_.size()));
                    PerfPoint::RecordElapsed(PerfKey::CLIENT_D2H_H2H_MEMLEN, hostBuffers[index].size);
                    PerfPoint point(PerfKey::CLIENT_D2H_H2H_MEMCPY);
                    Status rc = aclResourceMgr_->Host()->HostMemoryCopy(hostBuffers[index].ptr, hostBuffers[index].size,
                                                                        transferHostBuffers_[index].ptr,
                                                                        hostBuffers[index].size);
                    return rc;
                }));
            } catch (const std::bad_alloc &) {
                submitRc = Status(K_OUT_OF_MEMORY, "Submit FFTS D2H H2H task failed");
                break;
            } catch (const std::exception &e) {
                submitRc = Status(K_RUNTIME_ERROR, FormatString("Submit FFTS D2H H2H task failed: %s", e.what()));
                break;
            } catch (...) {
                submitRc = Status(K_RUNTIME_ERROR, "Submit FFTS D2H H2H task failed with unknown exception");
                break;
            }
        }
    }

    Status lastRc = waitD2H();
    for (auto &fut : futs) {
        Status rc;
        try {
            rc = fut.get();
        } catch (const std::bad_alloc &) {
            rc = Status(K_OUT_OF_MEMORY, "FFTS D2H H2H task failed due to out of memory");
        } catch (const std::exception &e) {
            rc = Status(K_RUNTIME_ERROR, FormatString("FFTS D2H H2H task threw exception: %s", e.what()));
        } catch (...) {
            rc = Status(K_RUNTIME_ERROR, "FFTS D2H H2H task threw unknown exception");
        }
        lastRc = rc.IsError() ? rc : lastRc;
    }
    const int microToNano = 1000;
    PerfPoint::RecordElapsed(PerfKey::CLIENT_D2H_H2H_WAIT, afterFftsFinishTimer.ElapsedMicroSecond() * microToNano);
    return submitRc.IsError() ? submitRc : lastRc;
}

Status FftsPipelineD2HCopier::SubmitToStream(const std::vector<BufferView> &srcBuffers,
                                             const std::vector<BufferView> &transferBuffers,
                                             const std::vector<BufferView> &destBuffers,
                                             const std::vector<BufferMetaInfo> &bufferMetas)
{
    auto srcCount = srcBuffers.size();
    auto destCount = destBuffers.size();
    auto toPinDone = resource_.toPinDone;
    auto toDestDone = resource_.toDestDone;
    auto secondaryStream = resource_.secondaryStream;
    auto primaryStream = resource_.primaryStream;
    CHECK_FAIL_RETURN_STATUS(
        destCount == bufferMetas.size(), K_RUNTIME_ERROR,
        FormatString("The dest buffer size %zu and buffer meta size %zu mismatch.", destCount, bufferMetas.size()));
    CHECK_FAIL_RETURN_STATUS(transferBuffers.size() == FFTS_PIPELINE, K_RUNTIME_ERROR,
                             FormatString("The transfer buffer size %zu and buffer meta size %zu mismatch.",
                                          FFTS_PIPELINE, bufferMetas.size()));
    for (size_t index = 0; index < destCount; index++) {
        size_t pipelineIndex = index % FFTS_PIPELINE;

        auto devicePinBuffer = transferBuffers[pipelineIndex].ptr;
        auto devicePinBufferSize = transferBuffers[pipelineIndex].size;
        auto deviceBlobCount = bufferMetas[index].blobCount;
        auto firstDeviceBlobIndex = bufferMetas[index].firstBlobOffset;
        auto objectSize = bufferMetas[index].size;
        auto hostPinBufferSize = destBuffers[index].size;
        CHECK_FAIL_RETURN_STATUS(
            objectSize <= devicePinBufferSize, K_RUNTIME_ERROR,
            FormatString("The devicePinBuffer size %zu too small, expect size %zu", devicePinBufferSize, objectSize));

        CHECK_FAIL_RETURN_STATUS(
            objectSize <= hostPinBufferSize, K_RUNTIME_ERROR,
            FormatString("The hostPinBuffer size %zu too small, expect size %zu", hostPinBufferSize, objectSize));

        size_t offsetInDevicePinBuffer = 0;
        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyWait(toDestDone[pipelineIndex], secondaryStream));
        std::vector<int32_t> lastTaskId(MAX_FFTS_TASKS_COUNT, -1);
        for (size_t n = 0; n < deviceBlobCount; n++) {
            size_t deviceBlobIndex = firstDeviceBlobIndex + n;
            CHECK_FAIL_RETURN_STATUS(
                deviceBlobIndex < srcCount, K_RUNTIME_ERROR,
                FormatString("The deviceBlobIndex %zu exceed the srcCount %zu", deviceBlobIndex, srcCount));
            auto deviceBlobPtr = srcBuffers[deviceBlobIndex].ptr;
            size_t deviceBlobSize = srcBuffers[deviceBlobIndex].size;
            void *devicePinBlobPtr =
                static_cast<void *>(static_cast<uint8_t *>(devicePinBuffer) + offsetInDevicePinBuffer);
            offsetInDevicePinBuffer += deviceBlobSize;

            CHECK_FAIL_RETURN_STATUS(offsetInDevicePinBuffer <= devicePinBufferSize, K_RUNTIME_ERROR,
                                     FormatString("The devicePinBuffer size %zu too small, expect size %zu",
                                                  devicePinBufferSize, offsetInDevicePinBuffer));
            uint32_t memcpyTaskId = 0;
            CHECK_ACL_RESULT(
                fftsDispatcher_->MemcpyAsync(devicePinBlobPtr, deviceBlobPtr, deviceBlobSize, &memcpyTaskId),
                "ffts MemcpyAsync");
            auto taskIdIndex = n % MAX_FFTS_TASKS_COUNT;
            if (lastTaskId[taskIdIndex] >= 0) {
                CHECK_ACL_RESULT(fftsDispatcher_->AddTaskDependency(lastTaskId[taskIdIndex], memcpyTaskId),
                                 "ffts AddTaskDependency");
            }
            lastTaskId[taskIdIndex] = static_cast<int32_t>(memcpyTaskId);
        }
        CHECK_ACL_RESULT(
            fftsDispatcher_->LaunchFftsTask(secondaryStream, std::min(deviceBlobCount, MAX_FFTS_TASKS_COUNT), 0),
            "ffts LaunchFftsTask");
        CHECK_ACL_RESULT(fftsDispatcher_->ReuseCtx(0), "ffts ReuseCtx");

        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyRecord(toPinDone[pipelineIndex], secondaryStream));

        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyWait(toPinDone[pipelineIndex], primaryStream));
        RETURN_IF_NOT_OK(aclDeviceManager_->aclrtMemcpyAsync(destBuffers[index].ptr, objectSize, devicePinBuffer,
                                                             objectSize, ACL_MEMCPY_DEVICE_TO_HOST, primaryStream));

        RETURN_IF_NOT_OK(aclDeviceManager_->RtNotifyRecord(toDestDone[pipelineIndex], primaryStream));
        if (skipH2HMemcpy_) {
            continue;
        }
        // The runtime retains the preallocated record until the callback executes. Keep the callback storage alive
        // with one intrusive reference per queued callback, so an abandoned stream can release callbackState_
        // without creating a shared_ptr cycle or allocating on every object.
        auto *callbackData = callbackDataStorage_->At(index);
        callbackData->state = callbackState_;
        callbackData->storage->Acquire();
        auto rc = aclDeviceManager_->AclrtLaunchCallback(FftsPipelineD2HCopier::NotifyH2HCallback, callbackData,
                                                         ACL_CALLBACK_NO_BLOCK, primaryStream);
        if (rc.IsError()) {
            callbackData->state.reset();
            callbackData->storage->Release();
            return rc;
        }
    }
    return Status::OK();
}

void FftsPipelineD2HCopier::NotifyH2HCallback(void *userData)
{
    auto *callbackData = reinterpret_cast<NotifyH2HCallbackData *>(userData);
    if (callbackData == nullptr) {
        return;
    }
    auto state = callbackData->state.lock();
    if (state != nullptr) {
        state->Complete(callbackData->index);
    }
    callbackData->state.reset();
    if (callbackData->storage != nullptr) {
        callbackData->storage->Release();
    }
}

void FftsPipelineD2HCopier::ForceFinish()
{
    if (callbackState_ != nullptr) {
        callbackState_->ForceFinish();
    }
}
}  // namespace datasystem
