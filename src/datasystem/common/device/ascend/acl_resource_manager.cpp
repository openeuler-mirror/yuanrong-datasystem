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

#include <securec.h>
#include <cstring>
#include <sstream>
#include <string>

#include "datasystem/common/device/ascend/ffts_dispatcher.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/status.h"

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

void MemcopyConfig::Init()
{
    LOG_IF_ERROR(GetNumberFromEnv("DS_DEVICE_ACL_SIZE", deviceMemSize), "GetNumberFromEnv failed");
    LOG_IF_ERROR(GetNumberFromEnv("DS_HOST_ACL_SIZE", hostMemSize), "GetNumberFromEnv failed");
    LOG_IF_ERROR(GetPolicyFromEnv("DS_D2H_MEMCPY_POLICY", policyD2H), "GetPolicyFromEnv failed");
    LOG_IF_ERROR(GetPolicyFromEnv("DS_H2D_MEMCPY_POLICY", policyH2D), "GetPolicyFromEnv failed");
}

std::string MemcopyConfig::ToString()
{
    std::stringstream ss;
    ss << "MemcopyConfig { policyD2H:" << static_cast<int>(policyD2H);
    ss << ", policyH2D:" << static_cast<int>(policyH2D);
    ss << ", deviceMemSize:" << deviceMemSize;
    ss << ", hostMemSize:" << hostMemSize;
    ss << "}";
    return ss.str();
}

Status MemcopyConfig::GetNumberFromEnv(const char *key, uint64_t &value)
{
    auto strValue = std::getenv(key);
    RETURN_OK_IF_TRUE(strValue == nullptr);
    try {
         uint64_t ret = StrToUnsignedLong(strValue);
        if (ret == 0) {
            throw std::out_of_range("Memory should not be set to zero.");
        }
        value = ret;
    } catch (std::invalid_argument &invalidArgument) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Env %s value %s parse to number failed, invalid argument", key, strValue));
    } catch (std::out_of_range &outOfRange) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR,
                      FormatString("Env %s value %s parse to number failed, out of range", key, strValue));
    }
    return Status::OK();
}

Status MemcopyConfig::GetPolicyFromEnv(const char *key, MemcopyPolicy &policy)
{
    auto strValue = std::getenv(key);
    RETURN_OK_IF_TRUE(strValue == nullptr);
    std::string str = strValue;
    if (str == "ffts") {
        policy = MemcopyPolicy::FFTS;
    } else if (str == "direct") {
        policy = MemcopyPolicy::DIRECT;
    } else if (str == "huge_ffts") {
        policy = MemcopyPolicy::HUGE_FFTS;
    } else {
        RETURN_STATUS(K_INVALID, FormatString("Unknown memcopy policy %s from env %s", str, key));
    }
    return Status::OK();
}

Status AclMemMgrBase::Init()
{
    int index;
    auto rc = acl::AclDeviceManager::Instance()->GetDeviceIdx(index);
    if (rc.IsError()) {
        LOG(WARNING) << "Not set device idx yet, return warning!";
        return Status::OK();
    }
    waitPost_ = std::make_unique<WaitPost>();
    return Status::OK();
}

Status AclMemMgrBase::Allocate(const std::vector<BufferMetaInfo> &bMeta, std::vector<ShmUnit> &memoryPool,
                               bool skipRetry)
{
    std::lock_guard<std::mutex> lock(memPoolLock_);
    uint64_t batchSize = bMeta.size();
    uint64_t maxAllocateSize = 0;
    if (type_ == AllocateType::DEV_DEVICE) {
        for (auto meta : bMeta) {
            maxAllocateSize = std::max(maxAllocateSize, meta.size);
        }
        batchSize = memoryPool.size();
    }
    uint32_t intervalMs = 100;
    for (uint64_t i = 0; i < batchSize; i++) {
        int retryNums = 10;
        if (skipRetry) {
            retryNums = 0;
        }
        Status rc = Status::OK();
        do {
            if (rc.IsError()) {
                waitPost_->WaitFor(intervalMs);
            }

            size_t allocSize = (type_ == AllocateType::DEV_DEVICE) ? maxAllocateSize : bMeta[i].size;
            rc = memoryPool[i].AllocateMemory(DEFAULT_TENANTID, allocSize, false, ServiceType::OBJECT, type_);
            if (retryNums <= 0) {
                break;
            }
            retryNums--;
        } while (rc.IsError() && rc.GetCode() == K_OUT_OF_MEMORY);
        if (skipRetry) {
            RETURN_IF_NOT_OK(rc);
        } else {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, FormatString("Failed to allocate memory with size %d", bMeta[i].size));
        }
    }
    return Status::OK();
}

Status AclMemMgrBase::Free(std::vector<ShmUnit> &memoryPool)
{
    std::lock_guard<std::mutex> lock(memPoolLock_);
    uint64_t size = memoryPool.size();
    for (uint64_t i = 0; i < size; i++) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(memoryPool[i].FreeMemory(), "Failed to free memory");
    }
    return Status::OK();
}

AclHostMemMgr::AclHostMemMgr(Allocator *allocator) : AclMemMgrBase(allocator)
{
    type_ = AllocateType::DEV_HOST;
    memoryCopyThreadPool_ = std::make_shared<ThreadPool>(0, GetRecommendedMemoryCopyThreadsNum());
}

Status AclHostMemMgr::HostMemoryCopy(void *dstData, uint64_t dstLength, void *srcData, uint64_t srcLength)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(dstData != nullptr, K_INVALID, "Can't put null dst ptr!");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(srcData != nullptr, K_INVALID, "Can't put null src ptr!");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        dstLength > 0 && srcLength > 0, K_INVALID,
        FormatString("length must greater than 0! dstLength : %lld, srcLength : %lld", dstLength, srcLength));
    Status status = ::datasystem::MemoryCopy(static_cast<uint8_t *>(dstData), dstLength,
                                             static_cast<const uint8_t *>(srcData), srcLength, memoryCopyThreadPool_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(status.IsOk(), K_RUNTIME_ERROR,
                                         FormatString("Copy data to buffer failed, err: %s", status.ToString()));
    return Status::OK();
}

AclResourceManager::AclResourceManager()
{
    deviceResources_.reserve(MAX_DEVICE_COUNT);
    for (size_t deviceId = 0; deviceId < MAX_DEVICE_COUNT; deviceId++) {
        deviceResources_.emplace_back(std::make_unique<DeviceResource>(deviceId));
    }
    config.Init();
}

Status AclResourceManager::Init()
{
    {
        std::shared_lock<std::shared_timed_mutex> rlocker(mutex_);
        if (aclHostMemMgr_ != nullptr && aclDeviceMemMgr_ != nullptr) {
            return Status::OK();
        }
    }

    auto devInterImpl = acl::AclDeviceManager::Instance();
    struct DevMemFuncRegister regFunc;

    auto hostAllocFunc = [devInterImpl](void **ptr, size_t maxSize) -> Status {
        return devInterImpl->aclrtMallocHost(&(*ptr), maxSize);
    };
    auto hostdestroyFunc = [devInterImpl](void *ptr, size_t destroySize) -> Status {
        (void)destroySize;
        if (ptr) {
            // do not free in instance yet (destroy func : devInterImpl->aclrtFreeHost(ptr))
            // destroy may cause interrupt with acl static data
            return Status::OK();
        }
        return Status::OK();
    };
    auto devAllocFunc = [devInterImpl](void **ptr, size_t maxSize) -> Status {
        return devInterImpl->aclrtMalloc(&(*ptr), maxSize, ACL_MEM_MALLOC_HUGE_FIRST);
    };
    auto devdestroyFunc = [devInterImpl](void *ptr, size_t destroySize) -> Status {
        (void)destroySize;
        if (ptr) {
            // do not free in instance yet (destroy func : devInterImpl->aclrtFree(ptr)
            // destroy may cause interrupt with acl static data
            return Status::OK();
        }
        return Status::OK();
    };
    regFunc.devDeviceCreateFunc = devAllocFunc;
    regFunc.devDeviceDestroyFunc = devdestroyFunc;
    regFunc.devHostCreateFunc = hostAllocFunc;
    regFunc.devHostDestroyFunc = hostdestroyFunc;

    std::lock_guard<std::shared_timed_mutex> wlocker(mutex_);
    auto *allocator = Allocator::Instance();
    LOG(INFO) << config.ToString();
    allocator->InitWithoutShm(config.deviceMemSize, config.hostMemSize, regFunc);

    if (!aclHostMemMgr_) {
        aclHostMemMgr_ = std::make_unique<AclHostMemMgr>(allocator);
        auto rc = aclHostMemMgr_->Init();
        if (rc.IsError()) {
            aclHostMemMgr_.reset();
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Failed to init dev host mem.");
    }

    if (!aclDeviceMemMgr_) {
        aclDeviceMemMgr_ = std::make_unique<AclDeviceMemMgr>(allocator);
        auto rc = aclDeviceMemMgr_->Init();
        if (rc.IsError()) {
            aclDeviceMemMgr_.reset();
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Failed to init dev device mem.");
    }
    return Status::OK();
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

    auto h2dFut = fftsCopyPool_->Submit([this] {
        PerfPoint point(PerfKey::CLIENT_H2D_FFTS_INIT);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitAclResource(false), "InitDeviceResource failed");
        point.RecordAndReset(PerfKey::CLIENT_H2D_FFTS_NOTIFY_START);
        RETURN_IF_NOT_OK(NotifyStart());
        point.RecordAndReset(PerfKey::CLIENT_H2D_FFTS_WAIT_AND_RUN);
        while (true) {
            PipelineH2DTasks tasks;
            {
                std::unique_lock<std::mutex> locker(mutex_);
                cv_.wait(locker, [this] { return !tasks_.IsEmpty() || IsFinish(); });
                if (IsFinish() && tasks_.IsEmpty()) {
                    PerfPoint point(PerfKey::CLIENT_H2D_FFTS_WAIT_FINISH);
                    VLOG(1) << "Start WaitFinish";
                    return WaitFinish();
                }
                std::swap(tasks, tasks_);
                blobOffset_ = 0;
            }
            if (tasks.IsEmpty()) {
                continue;
            }
            PerfPoint::RecordElapsed(PerfKey::CLIENT_H2D_FFTS_TASK_COUNT, tasks.srcBuffers.size());
            PerfPoint point(PerfKey::CLIENT_H2D_FFTS_SUBMIT);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SubmitToStream(tasks.srcBuffers, tasks.destBuffers, tasks.bufferMetas),
                                             "SubmitToStreamForH2D failed");
        }
        return Status::OK();
    });

    Status lastRc;
    for (auto &fut : futs) {
        auto rc = fut.get();
        lastRc = rc.IsError() ? rc : lastRc;
    }
    point.RecordAndReset(PerfKey::CLIENT_H2D_FFTS_TAIL_WAIT);
    auto h2dRc = h2dFut.get();
    point.Record();
    if (h2dRc.IsError()) {
        lastRc = h2dRc;
    }
    return lastRc;
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
        return Status::OK();
    };

    auto d2hFut = fftsCopyPool_->Submit([this, &d2hTask] {
        auto rc = d2hTask();
        if (rc.IsError()) {
            LOG(ERROR) << "force finish with rc:" << rc.GetMsg();
            ForceFinish();
        }
        return rc;
    });

    std::vector<std::future<Status>> futs;
    futs.reserve(hostBuffers.size());
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
        if (tasks.IsEmpty() || skipH2HMemcpy_) {
            continue;
        }
        PerfPoint::RecordElapsed(PerfKey::CLIENT_D2H_H2H_TASK_COUNT, tasks.indexes.size());
        for (auto index : tasks.indexes) {
            futs.emplace_back(h2hCopyPool_->Submit([this, index, hostBuffers] {
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                    index < bufferMetas_.size(), K_RUNTIME_ERROR,
                    FormatString("Invalid index %zu, out range of [0, %zu)", index, bufferMetas_.size()));
                PerfPoint::RecordElapsed(PerfKey::CLIENT_D2H_H2H_MEMLEN, hostBuffers[index].size);
                PerfPoint point(PerfKey::CLIENT_D2H_H2H_MEMCPY);
                Status rc =
                    aclResourceMgr_->Host()->HostMemoryCopy(hostBuffers[index].ptr, hostBuffers[index].size,
                                                            transferHostBuffers_[index].ptr, hostBuffers[index].size);
                return rc;
            }));
        }
    }

    auto lastRc = d2hFut.get();
    for (auto &fut : futs) {
        auto rc = fut.get();
        lastRc = rc.IsError() ? rc : lastRc;
    }
    const int microToNano = 1000;
    PerfPoint::RecordElapsed(PerfKey::CLIENT_D2H_H2H_WAIT, afterFftsFinishTimer.ElapsedMicroSecond() * microToNano);
    return lastRc;
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
