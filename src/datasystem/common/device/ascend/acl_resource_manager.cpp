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
#include <securec.h>
#include <cstring>
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

Status AclResourceManager::DeviceResource::InitFftsDispatcher()
{
    {
        std::shared_lock<std::shared_timed_mutex> rlocker(mutex_);
        RETURN_OK_IF_TRUE(fftsDispatcher_ != nullptr);
    }

    std::lock_guard<std::shared_timed_mutex> wlocker(mutex_);
    RETURN_OK_IF_TRUE(fftsDispatcher_ != nullptr);
    auto aclDeviceManager = acl::AclDeviceManager::Instance();
    auto dispatcher = std::make_unique<ffts::FftsDispatcher>(deviceId_, aclDeviceManager);
    CHECK_ACL_RESULT(dispatcher->Init(), "FftsDispatcher init");
    CHECK_ACL_RESULT(dispatcher->CreateFftsCtxs(1), "FftsDispatcher CreateFftsCtxs");
    CHECK_ACL_RESULT(dispatcher->SetFftsCtx(0), "FftsDispatcher SetFftsCtx");
    fftsDispatcher_ = std::move(dispatcher);
    return Status::OK();
}

Status AclResourceManager::DeviceResource::CreateAclRtStream(bool subscribeReport, aclrtStream &stream)
{
    auto &queue = subscribeReport ? subscribeReportStreamQueue_ : streamQueue_;
    RETURN_IF_NOT_OK(InitCallbackThread());
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
    RETURN_IF_NOT_OK(InitCallbackThread());

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
    FftsPipelineD2HCopier copier(deviceId, resourceMgr_, helper.bufferMetas, h2hCopyPool_.get(), submitPool);
    return copier.ExecuteMemcpy(helper.dstBuffers, helper.srcBuffers);
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
      finishCount_(0)
{
    auto ret = memset_s(&resource_, sizeof(AclResource), 0, sizeof(AclResource));
    if (ret != EOK) {
        LOG(WARNING) << FormatString("Memset DeviceResource failed, ret = %d", ret);
    }
    aclDeviceManager_ = acl::AclDeviceManager::Instance();
    fftsDispatcher_ = std::make_unique<ffts::FftsDispatcher>(deviceId, aclDeviceManager_);
    skipH2HMemcpy_ = aclResourceMgr_->GetD2HPolicy() == aclResourceMgr_->GetH2DPolicy()
                     && aclResourceMgr_->GetD2HPolicy() == MemcopyPolicy::HUGE_FFTS;
}

FftsPipelineCopierBase::~FftsPipelineCopierBase()
{
    PerfPoint point(PerfKey::CLIENT_FREE_STREAM_NOTIFY);
    try {
        LOG_IF_ERROR(aclResourceMgr_->FreeAclRtStream(deviceId_, resource_.primaryStream, resource_.subscribeReport),
                     "FreeAclRtStream failed");
        LOG_IF_ERROR(aclResourceMgr_->FreeAclRtStream(deviceId_, resource_.secondaryStream, false),
                     "FreeAclRtStream failed");
        for (size_t i = 0; i < FFTS_PIPELINE; i++) {
            LOG_IF_ERROR(aclResourceMgr_->FreeRtNotify(deviceId_, resource_.toDestDone[i]), "FreeRtNotify failed");
            LOG_IF_ERROR(aclResourceMgr_->FreeRtNotify(deviceId_, resource_.toPinDone[i]), "FreeRtNotify failed");
        }
    } catch (const std::exception &e) {
        LOG(ERROR) << e.what();
    }
    point.RecordAndReset(PerfKey::CLIENT_FREE_TRANS_BUFFERS);
    LOG_IF_ERROR(aclResourceMgr_->Host()->Free(transferHostPool_), "Free transferHostMem failed");
    LOG_IF_ERROR(aclResourceMgr_->Device()->Free(transferDevicePool_), "Free transferDeviceMem failed");
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
    transferHostPool_ = std::move(transferHostPool);
    transferDevicePool_ = std::move(transferDevicePool);

    if (skipH2HMemcpy_) {
        transferHostBuffers_.clear();
        transferHostBuffers_.assign(hostBuffer.begin(), hostBuffer.end());
    } else {
        RETURN_IF_NOT_OK(aclResourceMgr_->Host()->Allocate(bufferMetas_, transferHostPool_));
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetBufferViews(count, transferHostPool_, transferHostBuffers_),
                                         "GetBufferViews for host buffer failed.");
    }

    RETURN_IF_NOT_OK(aclResourceMgr_->Device()->Allocate(bufferMetas_, transferDevicePool_));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetBufferViews(FFTS_PIPELINE, transferDevicePool_, transferDeviceBuffers_),
                                     "GetBufferViews for device buffer failed.");

    return Status::OK();
}

Status FftsPipelineCopierBase::InitAclResource(bool subscribeReport)
{
    PerfPoint point(PerfKey::CLIENT_SET_DEVICE_IDX);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclDeviceManager_->SetDeviceIdx(deviceId_), "SetDeviceIdx failed");
    point.RecordAndReset(PerfKey::CLIENT_FFTS_INIT);
    CHECK_ACL_RESULT(fftsDispatcher_->Init(), "FftsDispatcher init");
    CHECK_ACL_RESULT(fftsDispatcher_->CreateFftsCtxs(1), "FftsDispatcher CreateFftsCtxs");
    CHECK_ACL_RESULT(fftsDispatcher_->SetFftsCtx(0), "FftsDispatcher SetFftsCtx");

    point.RecordAndReset(PerfKey::CLIENT_CREATE_STREAM_NOTIFY);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        aclResourceMgr_->CreateAclRtStream(deviceId_, resource_.primaryStream, subscribeReport),
        "CreateAclRtStream failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclResourceMgr_->CreateAclRtStream(deviceId_, resource_.secondaryStream, false),
                                     "CreateAclRtStream failed");

    for (size_t i = 0; i < FFTS_PIPELINE; i++) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclResourceMgr_->CreateRtNotify(deviceId_, resource_.toDestDone[i]),
                                         "CreateRtNotify failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(aclResourceMgr_->CreateRtNotify(deviceId_, resource_.toPinDone[i]),
                                         "CreateRtNotify failed");
    }
    resource_.subscribeReport = subscribeReport;
    return Status::OK();
}

Status FftsPipelineCopierBase::NotifyStart()
{
    for (size_t i = 0; i < FFTS_PIPELINE; i++) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            aclDeviceManager_->RtNotifyRecord(resource_.toDestDone[i], resource_.secondaryStream),
            "RtNotifyRecord failed.");
    }
    return Status::OK();
}

Status FftsPipelineCopierBase::WaitFinish()
{
    return aclDeviceManager_->RtSynchronizeStream(resource_.primaryStream);
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
}

Status FftsPipelineD2HCopier::ExecuteMemcpy(const std::vector<BufferView> &hostBuffers,
                                            const std::vector<BufferView> &deviceBuffers)
{
    PerfPoint point(PerfKey::CLIENT_D2H_ALLOC_TRANS_BUFFERS);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AllocAndInitTransferBuffers(hostBuffers), "AllocAndInitTransferBuffers failed");
    point.Record();
    std::vector<NotifyH2HCallbackData> callbackDatas;
    callbackDatas.reserve(bufferMetas_.size());
    for (size_t index = 0; index < bufferMetas_.size(); index++) {
        callbackDatas.emplace_back(NotifyH2HCallbackData{ .copier = this, .index = index });
    }
    Timer afterFftsFinishTimer;
    auto d2hTask = [this, &deviceBuffers, &callbackDatas, &afterFftsFinishTimer] {
        PerfPoint point(PerfKey::CLIENT_D2H_FFTS_INIT);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitAclResource(true), "InitDeviceResource failed");
        point.RecordAndReset(PerfKey::CLIENT_D2H_FFTS_NOTIFY_START);
        RETURN_IF_NOT_OK(NotifyStart());
        point.RecordAndReset(PerfKey::CLIENT_D2H_FFTS_SUBMIT);
        RETURN_IF_NOT_OK(
            SubmitToStream(deviceBuffers, transferDeviceBuffers_, transferHostBuffers_, bufferMetas_, callbackDatas));
        point.RecordAndReset(PerfKey::CLIENT_D2H_FFTS_WAIT_FINISH);
        VLOG(1) << "Start WaitFinish";
        auto rc = WaitFinish();
        point.Record();
        afterFftsFinishTimer.Reset();
        return rc;
    };

    std::vector<std::future<Status>> futs;
    try {
        futs.reserve(hostBuffers.size());
    } catch (const std::bad_alloc &) {
        RETURN_STATUS(K_OUT_OF_MEMORY, "Reserve D2H H2H futures failed");
    }

    std::future<Status> d2hFut;
    try {
        d2hFut = fftsCopyPool_->Submit([this, &d2hTask] {
            auto rc = d2hTask();
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

    Status submitRc;
    while (true) {
        PerfPoint point(PerfKey::CLIENT_D2H_H2H_WAIT_START);
        PipelineH2HTasks tasks;
        {
            std::unique_lock<std::mutex> locker(mutex_);
            cv_.wait(locker, [this] { return !tasks_.IsEmpty() || IsFinish(); });
            point.Record();
            if (IsFinish() && tasks_.IsEmpty()) {
                VLOG(1) << "h2h finish";
                break;
            }
            std::swap(tasks, tasks_);
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

    Status lastRc;
    try {
        lastRc = d2hFut.get();
    } catch (const std::bad_alloc &) {
        lastRc = Status(K_OUT_OF_MEMORY, "FFTS D2H device task failed due to out of memory");
    } catch (const std::exception &e) {
        lastRc = Status(K_RUNTIME_ERROR, FormatString("FFTS D2H device task threw exception: %s", e.what()));
    } catch (...) {
        lastRc = Status(K_RUNTIME_ERROR, "FFTS D2H device task threw unknown exception");
    }
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
                                             const std::vector<BufferMetaInfo> &bufferMetas,
                                             std::vector<NotifyH2HCallbackData> &callbackDatas)
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
    CHECK_FAIL_RETURN_STATUS(callbackDatas.size() == bufferMetas.size(), K_RUNTIME_ERROR,
                             FormatString("The callbackDatas size %zu and buffer meta size %zu mismatch.",
                                          callbackDatas.size(), bufferMetas.size()));
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
        // launch callback to notify H2H start.
        RETURN_IF_NOT_OK(aclDeviceManager_->AclrtLaunchCallback(
            FftsPipelineD2HCopier::NotifyH2HCallback, &callbackDatas[index], ACL_CALLBACK_NO_BLOCK, primaryStream));
    }
    return Status::OK();
}

void FftsPipelineD2HCopier::NotifyH2HCallback(void *userData)
{
    auto callbackData = reinterpret_cast<NotifyH2HCallbackData *>(userData);
    auto copier = callbackData->copier;
    std::unique_lock<std::mutex> locker(copier->mutex_);
    copier->tasks_.indexes.emplace_back(callbackData->index);
    copier->finishCount_++;
    copier->cv_.notify_one();
}

void FftsPipelineD2HCopier::ForceFinish()
{
    std::unique_lock<std::mutex> locker(mutex_);
    finishCount_ = bufferMetas_.size();
    tasks_.indexes.clear();
    cv_.notify_one();
}
}  // namespace datasystem
