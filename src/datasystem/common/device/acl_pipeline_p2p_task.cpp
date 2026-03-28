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

/**
 * Description: Defines the ascend device manager.
 */
#include "datasystem/common/device/acl_pipeline_p2p_task.h"

#include <mutex>
#include <numeric>

#include "datasystem/common/device/comm_wrapper_base.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/device/device_manager_factory.h"

namespace datasystem {
namespace acl {
namespace {
bool EnableMerge(const std::vector<Blob> blobs)
{
    // Only NPU supports merge
    if (DeviceManagerFactory::ProbeBackend() != DeviceBackend::NPU) {
        return false;
    }

    if (blobs.size() <= 1) {
        return false;
    }

    uint64_t mergeLimit = 1024 * 1024;  // 1MB
    INJECT_POINT("NO_USE_FFTS", [&mergeLimit]() {
        mergeLimit = 0;
        return true;
    });
    auto totalSize = std::accumulate(blobs.cbegin(), blobs.cend(), 0ul,
                                     [](size_t total, const Blob &info) { return total + info.size; });
    return totalSize / blobs.size() <= mergeLimit;
}
}  // namespace

PipeLineP2PBase::~PipeLineP2PBase()
{
    std::lock_guard<std::mutex> locker(mutex_);
    if (!transferUnitPools_.empty()) {
        LOG(WARNING) << "transfer buffer not release, remaining count:" << transferUnitPools_.size();
        for (auto &kv : transferUnitPools_) {
            LOG_IF_ERROR(aclResourceMgr_->Device()->Free(kv.second), "Free send transfer pool failed");
        }
    }
}

Status PipeLineP2PBase::AllocTransferBuffer(size_t objectSize, Blob &transBuffer, uint64_t &seq)
{
    auto tryReuse = [this](size_t objectSize, uint64_t ackSeq, std::vector<ShmUnit> &transferVec) {
        const uint64_t cacheSize = 2;

        std::lock_guard<std::mutex> locker(mutex_);
        auto iter = transferUnitPools_.begin();
        while (iter != transferUnitPools_.end()) {
            if (iter->first > ackSeq) {
                break;
            }
            auto &currentPool = iter->second;
            if (currentPool.begin()->GetSize() >= objectSize && transferVec.empty()) {
                // reuse the first match.
                transferVec = std::move(currentPool);
                iter = transferUnitPools_.erase(iter);
            } else if (transferUnitPools_.size() > cacheSize) {
                LOG_IF_ERROR(aclResourceMgr_->Device()->Free(currentPool), "Free transfer pool failed");
                iter = transferUnitPools_.erase(iter);
            } else {
                ++iter;
            }
        }
    };
    std::vector<ShmUnit> transferVec;
    uint64_t ackSeq = ackSeq_;
    tryReuse(objectSize, ackSeq, transferVec);
    Timer timer;
    const int maxRetrySec = 60;
    while (transferVec.empty()) {
        transferVec = std::move(std::vector<ShmUnit>(1));
        auto rc = aclResourceMgr_->Device()->Allocate(
            { BufferMetaInfo{ .blobCount = 1, .firstBlobOffset = 0, .size = objectSize } }, transferVec, true);
        if (rc.IsOk()) {
            break;
        }
        transferVec.clear();
        if (timer.ElapsedSecond() > maxRetrySec) {
            LOG(ERROR) << "Retry timeout, ackSeq_:" << ackSeq_ << ", seq_:" << seq_;
            return rc;
        }
        if (ackSeq_ > ackSeq) {
            tryReuse(objectSize, ackSeq_, transferVec);
        } else if (seq_ > ackSeq_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            return rc;
        }
    }

    CHECK_FAIL_RETURN_STATUS(transferVec.begin()->GetSize() >= objectSize, K_RUNTIME_ERROR,
                             FormatString("The transfer memory chunk size %zu too small, expect size %zu.",
                                          transferVec.begin()->GetSize(), objectSize));
    transBuffer.pointer = transferVec.begin()->GetPointer();
    transBuffer.size = objectSize;
    std::lock_guard<std::mutex> locker(mutex_);
    seq = seq_.fetch_add(1);
    (void)transferUnitPools_.emplace(seq, std::move(transferVec));
    VLOG(1) << "transferPools size:" << transferUnitPools_.size() << ", ackSeq:" << ackSeq_ << ", seq:" << seq;
    return Status::OK();
}

void PipeLineP2PBase::NotifyCallback(void *userData)
{
    if (userData == nullptr) {
        LOG(WARNING) << "PipeLine callback data is null";
        return;
    }
    auto callbackData = reinterpret_cast<CallbackData *>(userData);
    callbackData->self->ackSeq_ = callbackData->ackSeq;
    delete callbackData;
}

Status PipeLineP2PSend::Submit(P2PSendTask &&task)
{
    if (!EnableMerge(task.srcBuffers)) {
        VLOG(1) << "Direct P2PSend.";
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(task.comm->P2PSend(task.srcBuffers, task.event, PrimaryStream()),
                                         "P2PSend failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(task.event->RecordEvent(PrimaryStream()), "Record send event failed");
        return Status::OK();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AllocTransferBuffer(task.totalSize, task.transBuffer, task.seq),
                                     "AllocTransferBuffer failed");
    return TwoPhaseAclPipeLineBase::Submit(std::move(task));
}

Status PipeLineP2PSend::WaitFinish()
{
    auto stream = PrimaryStream();
    RETURN_RUNTIME_ERROR_IF_NULL(stream);
    return aclApi_->RtSynchronizeStream(stream);
}

Status PipeLineP2PSend::RunTaskPhaseOneImpl(size_t pipelineIndex, const P2PSendTask &task, aclrtStream stream)
{
    (void)pipelineIndex;
    if (!EnableMerge(task.srcBuffers)) {
        return Status::OK();
    }
    size_t MAX_FFTS_TASKS_COUNT = 8;
    auto &fftsDispatcher = GetResource()->fftsDispatcher;
    std::vector<int32_t> lastTaskId(MAX_FFTS_TASKS_COUNT, -1);
    size_t offset = 0;
    auto transferPtr = task.transBuffer.pointer;
    auto transferSize = task.transBuffer.size;
    size_t srcCount = task.srcBuffers.size();
    for (size_t n = 0; n < srcCount; n++) {
        auto srcPtr = task.srcBuffers[n].pointer;
        size_t srcSize = task.srcBuffers[n].size;
        void *destPtr = static_cast<void *>(static_cast<uint8_t *>(transferPtr) + offset);
        offset += srcSize;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            transferSize >= offset, K_RUNTIME_ERROR,
            FormatString("Invalid buffer size, offset %zu, transfer buffer size %zu", offset, transferSize));
        uint32_t memcpyTaskId = 0;
        CHECK_ACL_RESULT(fftsDispatcher->MemcpyAsync(destPtr, srcPtr, srcSize, &memcpyTaskId), "ffts MemcpyAsync");
        auto taskIdIndex = n % MAX_FFTS_TASKS_COUNT;
        if (lastTaskId[taskIdIndex] >= 0) {
            CHECK_ACL_RESULT(fftsDispatcher->AddTaskDependency(lastTaskId[taskIdIndex], memcpyTaskId),
                             "ffts AddTaskDependency");
        }
        lastTaskId[taskIdIndex] = static_cast<int32_t>(memcpyTaskId);
    }
    CHECK_ACL_RESULT(fftsDispatcher->LaunchFftsTask(stream, std::min(srcCount, MAX_FFTS_TASKS_COUNT), 0),
                     "ffts LaunchFftsTask");
    CHECK_ACL_RESULT(fftsDispatcher->ReuseCtx(0), "ffts ReuseCtx");
    return Status::OK();
}

Status PipeLineP2PSend::RunTaskPhaseTwoImpl(size_t pipelineIndex, const P2PSendTask &task, aclrtStream stream)
{
    (void)pipelineIndex;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(task.transBuffer.size >= task.totalSize, K_RUNTIME_ERROR,
                                         FormatString("Invalid transfer buffer size %zu, must greate or equal than %zu",
                                                      task.transBuffer.size, task.totalSize));
    return task.comm->P2PSend({ task.transBuffer }, task.event, stream);
}

Status PipeLineP2PSend::PostTaskProcessImpl(const P2PSendTask &task)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(task.event->RecordEvent(PrimaryStream()), "Record send event failed.");
    auto callbackData = std::make_unique<CallbackData>();
    callbackData->self = this;
    callbackData->ackSeq = task.seq;
    RETURN_IF_NOT_OK(aclApi_->AclrtLaunchCallback(PipeLineP2PBase::NotifyCallback, callbackData.release(),
                                                  ACL_CALLBACK_NO_BLOCK, PrimaryStream()));
    return Status::OK();
}

Status PipeLineP2PRecv::Submit(P2PRecvTask &&task)
{
    if (!EnableMerge(task.destBuffers)) {
        VLOG(1) << "Direct P2PRecv.";
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(task.comm->P2PRecv(task.destBuffers, task.event, PrimaryStream()),
                                         "P2PRecv failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(task.event->RecordEvent(PrimaryStream()), "Record send event failed");
        return Status::OK();
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AllocTransferBuffer(task.totalSize, task.transBuffer, task.seq),
                                     "AllocTransferBuffer failed");
    return TwoPhaseAclPipeLineBase::Submit(std::move(task));
}

Status PipeLineP2PRecv::WaitFinish()
{
    auto stream = PrimaryStream();
    RETURN_RUNTIME_ERROR_IF_NULL(stream);
    return aclApi_->RtSynchronizeStream(stream);
}

Status PipeLineP2PRecv::RunTaskPhaseOneImpl(size_t pipelineIndex, const P2PRecvTask &task, aclrtStream stream)
{
    (void)pipelineIndex;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(task.transBuffer.size >= task.totalSize, K_RUNTIME_ERROR,
                                         FormatString("Invalid transfer buffer size %zu, must greate or equal than %zu",
                                                      task.transBuffer.size, task.totalSize));

    return task.comm->P2PRecv({ task.transBuffer }, task.event, stream);
}

Status PipeLineP2PRecv::RunTaskPhaseTwoImpl(size_t pipelineIndex, const P2PRecvTask &task, aclrtStream stream)
{
    (void)pipelineIndex;
    if (!EnableMerge(task.destBuffers)) {
        return Status::OK();
    }
    size_t MAX_FFTS_TASKS_COUNT = 8;
    auto &fftsDispatcher = GetResource()->fftsDispatcher;
    std::vector<int32_t> lastTaskId(MAX_FFTS_TASKS_COUNT, -1);
    size_t offset = 0;
    auto transferPtr = task.transBuffer.pointer;
    auto transferSize = task.transBuffer.size;
    size_t srcCount = task.destBuffers.size();
    for (size_t n = 0; n < srcCount; n++) {
        auto destPtr = task.destBuffers[n].pointer;
        size_t destSize = task.destBuffers[n].size;
        void *srcPtr = static_cast<void *>(static_cast<uint8_t *>(transferPtr) + offset);
        offset += destSize;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            transferSize >= offset, K_RUNTIME_ERROR,
            FormatString("Invalid buffer size, offset %zu, transfer buffer size %zu", offset, transferSize));
        uint32_t memcpyTaskId = 0;
        CHECK_ACL_RESULT(fftsDispatcher->MemcpyAsync(destPtr, srcPtr, destSize, &memcpyTaskId), "ffts MemcpyAsync");
        auto taskIdIndex = n % MAX_FFTS_TASKS_COUNT;
        if (lastTaskId[taskIdIndex] >= 0) {
            CHECK_ACL_RESULT(fftsDispatcher->AddTaskDependency(lastTaskId[taskIdIndex], memcpyTaskId),
                             "ffts AddTaskDependency");
        }
        lastTaskId[taskIdIndex] = static_cast<int32_t>(memcpyTaskId);
    }
    CHECK_ACL_RESULT(fftsDispatcher->LaunchFftsTask(stream, std::min(srcCount, MAX_FFTS_TASKS_COUNT), 0),
                     "ffts LaunchFftsTask");
    CHECK_ACL_RESULT(fftsDispatcher->ReuseCtx(0), "ffts ReuseCtx");
    return Status::OK();
}

Status PipeLineP2PRecv::PostTaskProcessImpl(const P2PRecvTask &task)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(task.event->RecordEvent(PrimaryStream()), "Record recv event failed.");
    auto callbackData = std::make_unique<CallbackData>();
    callbackData->self = this;
    callbackData->ackSeq = task.seq;
    RETURN_IF_NOT_OK(aclApi_->AclrtLaunchCallback(PipeLineP2PBase::NotifyCallback, callbackData.release(),
                                                  ACL_CALLBACK_NO_BLOCK, PrimaryStream()));
    return Status::OK();
}
}  // namespace acl
}  // namespace datasystem
