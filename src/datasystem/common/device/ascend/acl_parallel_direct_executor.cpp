/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/device/ascend/acl_parallel_direct_executor.h"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <exception>
#include <limits>
#include <mutex>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/device/ascend/acl_parallel_config_util.h"

namespace datasystem {
namespace {
constexpr size_t DEFAULT_PARALLEL_DIRECT_WORKER_NUM = 1;
constexpr size_t MAX_PARALLEL_DIRECT_WORKER_NUM = 16;
constexpr size_t DEFAULT_PARALLEL_DIRECT_AGGREGATE_NUM = 512;
constexpr size_t DEFAULT_PARALLEL_DIRECT_PENDING_FACTOR = 2;
constexpr size_t MAX_PARALLEL_DIRECT_PENDING_TASK_NUM = 64;
constexpr size_t DEFAULT_PARALLEL_DIRECT_INLINE_ALLOWANCE = 1;
constexpr uint64_t DEFAULT_PARALLEL_DIRECT_MIN_BYTES = 24ULL * 1024ULL * 1024ULL;
constexpr size_t MAX_PARALLEL_DIRECT_INFLIGHT_BATCH_NUM = MAX_PARALLEL_DIRECT_WORKER_NUM + 1;
constexpr char PARALLEL_H2D_THREAD_NAME[] = "H2DParallel";
constexpr char PARALLEL_D2H_THREAD_NAME[] = "D2HParallel";

constexpr char H2D_PARALLEL_WORKER_NUM_ENV[] = "DS_H2D_PARALLEL_WORKER_NUM";
constexpr char H2D_PARALLEL_AGGREGATE_NUM_ENV[] = "DS_H2D_PARALLEL_AGGREGATE_NUM";
constexpr char H2D_PARALLEL_MAX_PENDING_TASK_NUM_ENV[] = "DS_H2D_PARALLEL_MAX_PENDING_TASK_NUM";
constexpr char H2D_PARALLEL_MIN_BYTES_ENV[] = "DS_H2D_PARALLEL_MIN_BYTES";
constexpr char H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM_ENV[] = "DS_H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM";
constexpr char D2H_PARALLEL_WORKER_NUM_ENV[] = "DS_D2H_PARALLEL_WORKER_NUM";
constexpr char D2H_PARALLEL_AGGREGATE_NUM_ENV[] = "DS_D2H_PARALLEL_AGGREGATE_NUM";
constexpr char D2H_PARALLEL_MAX_PENDING_TASK_NUM_ENV[] = "DS_D2H_PARALLEL_MAX_PENDING_TASK_NUM";
constexpr char D2H_PARALLEL_MIN_BYTES_ENV[] = "DS_D2H_PARALLEL_MIN_BYTES";
constexpr char D2H_PARALLEL_MAX_INFLIGHT_BATCH_NUM_ENV[] = "DS_D2H_PARALLEL_MAX_INFLIGHT_BATCH_NUM";

struct ParallelDirectExecutorConfig {
    size_t workerNum;
    size_t aggregateNum;
    size_t maxPendingTaskNum;
    uint64_t minBytes;
    size_t maxInflightBatchNum;
};

struct ParallelDirectPerfKeys {
    PerfKey total;
    PerfKey partition;
    PerfKey submit;
    PerfKey wait;
    PerfKey taskCount;
    PerfKey inlineTaskCount;
    PerfKey queueFullCount;
    PerfKey memLen;
};

ParallelDirectPerfKeys GetParallelDirectPerfKeys(MemcpyKind kind)
{
    if (kind == MemcpyKind::DEVICE_TO_HOST) {
        return { PerfKey::TOTAL_D2H_PARALLEL_MEMCPY,
                 PerfKey::CLIENT_D2H_PARALLEL_PARTITION,
                 PerfKey::CLIENT_D2H_PARALLEL_SUBMIT,
                 PerfKey::CLIENT_D2H_PARALLEL_WAIT,
                 PerfKey::CLIENT_D2H_PARALLEL_TASK_COUNT,
                 PerfKey::CLIENT_D2H_PARALLEL_INLINE_TASK_COUNT,
                 PerfKey::CLIENT_D2H_PARALLEL_QUEUE_FULL_COUNT,
                 PerfKey::CLIENT_D2H_PARALLEL_MEMLEN };
    }
    return { PerfKey::TOTAL_H2D_PARALLEL_MEMCPY,
             PerfKey::CLIENT_H2D_PARALLEL_PARTITION,
             PerfKey::CLIENT_H2D_PARALLEL_SUBMIT,
             PerfKey::CLIENT_H2D_PARALLEL_WAIT,
             PerfKey::CLIENT_H2D_PARALLEL_TASK_COUNT,
             PerfKey::CLIENT_H2D_PARALLEL_INLINE_TASK_COUNT,
             PerfKey::CLIENT_H2D_PARALLEL_QUEUE_FULL_COUNT,
             PerfKey::CLIENT_H2D_PARALLEL_MEMLEN };
}

template <typename Config>
ParallelDirectExecutorConfig ToExecutorConfig(const Config &config)
{
    return ParallelDirectExecutorConfig{ config.workerNum, config.aggregateNum, config.maxPendingTaskNum,
                                         config.minBytes, config.maxInflightBatchNum };
}

Status ValidateParallelDirectConfig(const ParallelDirectExecutorConfig &config, const char *direction)
{
    CHECK_FAIL_RETURN_STATUS(config.workerNum >= 1 && config.workerNum <= MAX_PARALLEL_DIRECT_WORKER_NUM, K_INVALID,
                             FormatString("Parallel %s worker number is out of range", direction));
    CHECK_FAIL_RETURN_STATUS(config.aggregateNum >= 1 && config.aggregateNum <= ACL_MEMCPY_BATCH_LIMIT, K_INVALID,
                             FormatString("Parallel %s aggregate number is out of range", direction));
    CHECK_FAIL_RETURN_STATUS(config.maxPendingTaskNum >= config.workerNum
                                 && config.maxPendingTaskNum <= MAX_PARALLEL_DIRECT_PENDING_TASK_NUM,
                             K_INVALID, FormatString("Parallel %s pending task number is out of range", direction));
    CHECK_FAIL_RETURN_STATUS(config.maxInflightBatchNum >= config.workerNum
                                 && config.maxInflightBatchNum <= MAX_PARALLEL_DIRECT_INFLIGHT_BATCH_NUM,
                             K_INVALID, FormatString("Parallel %s inflight batch number is out of range", direction));
    return Status::OK();
}

// Env var names for one direction of the parallel direct config.
struct ParallelDirectConfigEnvNames {
    const char *workerNum;
    const char *aggregateNum;
    const char *maxPendingTaskNum;
    const char *minBytes;
    const char *maxInflightBatchNum;
};

Status LoadParallelDirectConfig(const ParallelDirectConfigEnvNames &envNames, ParallelDirectExecutorConfig &config)
{
    uint64_t value = config.workerNum;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(envNames.workerNum, 1, MAX_PARALLEL_DIRECT_WORKER_NUM, value));
    config.workerNum = static_cast<size_t>(value);

    value = config.aggregateNum;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(envNames.aggregateNum, 1, ACL_MEMCPY_BATCH_LIMIT, value));
    config.aggregateNum = static_cast<size_t>(value);

    config.maxPendingTaskNum = config.workerNum * DEFAULT_PARALLEL_DIRECT_PENDING_FACTOR;
    value = config.maxPendingTaskNum;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(envNames.maxPendingTaskNum, config.workerNum,
                                        MAX_PARALLEL_DIRECT_PENDING_TASK_NUM, value));
    config.maxPendingTaskNum = static_cast<size_t>(value);

    value = config.minBytes;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(envNames.minBytes, 0, std::numeric_limits<uint64_t>::max(), value));
    config.minBytes = value;

    config.maxInflightBatchNum = config.workerNum + DEFAULT_PARALLEL_DIRECT_INLINE_ALLOWANCE;
    value = config.maxInflightBatchNum;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(envNames.maxInflightBatchNum, config.workerNum,
                                        MAX_PARALLEL_DIRECT_INFLIGHT_BATCH_NUM, value));
    config.maxInflightBatchNum = static_cast<size_t>(value);
    return Status::OK();
}

}  // namespace

ParallelH2DConfig::ParallelH2DConfig()
    : workerNum(DEFAULT_PARALLEL_DIRECT_WORKER_NUM),
      aggregateNum(DEFAULT_PARALLEL_DIRECT_AGGREGATE_NUM),
      maxPendingTaskNum(DEFAULT_PARALLEL_DIRECT_WORKER_NUM * DEFAULT_PARALLEL_DIRECT_PENDING_FACTOR),
      minBytes(DEFAULT_PARALLEL_DIRECT_MIN_BYTES),
      maxInflightBatchNum(DEFAULT_PARALLEL_DIRECT_WORKER_NUM + DEFAULT_PARALLEL_DIRECT_INLINE_ALLOWANCE)
{
}

Status ParallelH2DConfig::LoadFromEnv()
{
    *this = ParallelH2DConfig();
    const ParallelDirectConfigEnvNames envNames{
        H2D_PARALLEL_WORKER_NUM_ENV,   H2D_PARALLEL_AGGREGATE_NUM_ENV, H2D_PARALLEL_MAX_PENDING_TASK_NUM_ENV,
        H2D_PARALLEL_MIN_BYTES_ENV,    H2D_PARALLEL_MAX_INFLIGHT_BATCH_NUM_ENV,
    };
    ParallelDirectExecutorConfig config = ToExecutorConfig(*this);
    RETURN_IF_NOT_OK(LoadParallelDirectConfig(envNames, config));
    workerNum = config.workerNum;
    aggregateNum = config.aggregateNum;
    maxPendingTaskNum = config.maxPendingTaskNum;
    minBytes = config.minBytes;
    maxInflightBatchNum = config.maxInflightBatchNum;
    return Validate();
}

Status ParallelH2DConfig::Validate() const
{
    return ValidateParallelDirectConfig(ToExecutorConfig(*this), "H2D");
}

ParallelD2HConfig::ParallelD2HConfig()
    : workerNum(DEFAULT_PARALLEL_DIRECT_WORKER_NUM),
      aggregateNum(DEFAULT_PARALLEL_DIRECT_AGGREGATE_NUM),
      maxPendingTaskNum(DEFAULT_PARALLEL_DIRECT_WORKER_NUM * DEFAULT_PARALLEL_DIRECT_PENDING_FACTOR),
      minBytes(DEFAULT_PARALLEL_DIRECT_MIN_BYTES),
      maxInflightBatchNum(DEFAULT_PARALLEL_DIRECT_WORKER_NUM + DEFAULT_PARALLEL_DIRECT_INLINE_ALLOWANCE)
{
}

Status ParallelD2HConfig::LoadFromEnv()
{
    *this = ParallelD2HConfig();
    const ParallelDirectConfigEnvNames envNames{
        D2H_PARALLEL_WORKER_NUM_ENV,   D2H_PARALLEL_AGGREGATE_NUM_ENV, D2H_PARALLEL_MAX_PENDING_TASK_NUM_ENV,
        D2H_PARALLEL_MIN_BYTES_ENV,    D2H_PARALLEL_MAX_INFLIGHT_BATCH_NUM_ENV,
    };
    ParallelDirectExecutorConfig config = ToExecutorConfig(*this);
    RETURN_IF_NOT_OK(LoadParallelDirectConfig(envNames, config));
    workerNum = config.workerNum;
    aggregateNum = config.aggregateNum;
    maxPendingTaskNum = config.maxPendingTaskNum;
    minBytes = config.minBytes;
    maxInflightBatchNum = config.maxInflightBatchNum;
    return Validate();
}

Status ParallelD2HConfig::Validate() const
{
    return ValidateParallelDirectConfig(ToExecutorConfig(*this), "D2H");
}

std::string ParallelD2HConfig::ToString() const
{
    return FormatString(
        "ParallelD2HConfig { workerNum:%zu, aggregateNum:%zu, maxPendingTaskNum:%zu, minBytes:%llu, "
        "maxInflightBatchNum:%zu }",
        workerNum, aggregateNum, maxPendingTaskNum, static_cast<unsigned long long>(minBytes), maxInflightBatchNum);
}

std::string ParallelH2DConfig::ToString() const
{
    return FormatString(
        "ParallelH2DConfig { workerNum:%zu, aggregateNum:%zu, maxPendingTaskNum:%zu, minBytes:%llu, "
        "maxInflightBatchNum:%zu }",
        workerNum, aggregateNum, maxPendingTaskNum, static_cast<unsigned long long>(minBytes), maxInflightBatchNum);
}

struct AclParallelDirectExecutor::Impl {
    enum class SubmitResult : int { ACCEPTED, QUEUE_FULL, CLOSED, ERROR };

    class CallState {
    public:
        CallState() = default;
        ~CallState() = default;

        void ReserveTask()
        {
            std::lock_guard<std::mutex> lock(mutex_);
            remainingTaskNum_++;
        }

        void CancelReservedTask()
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (remainingTaskNum_ > 0) {
                remainingTaskNum_--;
            }
            if (remainingTaskNum_ == 0) {
                cv_.notify_all();
            }
        }

        void CompleteTask(size_t taskIndex, const Status &status)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ObserveErrorLocked(taskIndex, status);
            if (remainingTaskNum_ > 0) {
                remainingTaskNum_--;
            }
            if (remainingTaskNum_ == 0) {
                cv_.notify_all();
            }
        }

        void ObserveError(size_t taskIndex, const Status &status)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ObserveErrorLocked(taskIndex, status);
        }

        bool HasError() const
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return firstError_.IsError();
        }

        Status Wait()
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return remainingTaskNum_ == 0; });
            return firstError_;
        }

    private:
        void ObserveErrorLocked(size_t taskIndex, const Status &status)
        {
            if (status.IsError() && taskIndex < firstErrorTaskIndex_) {
                firstErrorTaskIndex_ = taskIndex;
                firstError_ = status;
            }
        }

        mutable std::mutex mutex_;
        std::condition_variable cv_;
        size_t remainingTaskNum_ = 0;
        size_t firstErrorTaskIndex_ = std::numeric_limits<size_t>::max();
        Status firstError_;
    };

    struct Task {
        size_t taskIndex;
        size_t startIndex;
        size_t batchNum;
        void **dstList;
        size_t *dstSizeList;
        void **srcList;
        size_t *srcSizeList;
        std::shared_ptr<CallState> callState;
    };

    struct SubmitOutcome {
        SubmitResult result;
        Status status;
    };

    struct PartitionState {
        size_t taskIndex;
        size_t startIndex;
        size_t remainingTaskNum;
        size_t remainingDescriptorNum;
        uint64_t remainingBytes;
    };

    struct SubmitStats {
        size_t inlineTaskCount = 0;
        size_t queueFullCount = 0;
    };

    Impl(uint32_t deviceId, DeviceManagerBase *deviceManager, ParallelDirectExecutorConfig config, MemcpyKind kind)
        : deviceId_(deviceId),
          deviceManager_(deviceManager),
          config_(std::move(config)),
          kind_(kind),
          direction_(kind == MemcpyKind::DEVICE_TO_HOST ? "D2H" : "H2D"),
          perfKeys_(GetParallelDirectPerfKeys(kind))
    {
    }

    ~Impl()
    {
        Shutdown();
    }

    Status Init()
    {
        std::lock_guard<std::mutex> lifecycleLock(shutdownMutex_);
        CHECK_FAIL_RETURN_STATUS(deviceManager_ != nullptr, K_INVALID,
                                 FormatString("Parallel %s device manager is null", direction_));
        RETURN_IF_NOT_OK(ValidateParallelDirectConfig(config_, direction_));
        {
            std::lock_guard<std::mutex> lock(mutex_);
            CHECK_FAIL_RETURN_STATUS(!closing_, K_SHUTTING_DOWN,
                                     FormatString("Parallel %s executor is shutting down", direction_));
            RETURN_OK_IF_TRUE(initialized_);
            initializedWorkerNum_ = 0;
            workerInitStatus_ = Status::OK();
        }

        try {
            workers_.reserve(config_.workerNum);
            for (size_t index = 0; index < config_.workerNum; index++) {
                workers_.emplace_back([this] { WorkerLoop(); });
                workers_.back().set_name(kind_ == MemcpyKind::DEVICE_TO_HOST ? PARALLEL_D2H_THREAD_NAME
                                                                             : PARALLEL_H2D_THREAD_NAME);
            }
        } catch (const std::exception &error) {
            ShutdownLocked();
            RETURN_STATUS(K_RUNTIME_ERROR,
                          FormatString("Create parallel %s worker failed: %s", direction_, error.what()));
        }

        Status initStatus;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            workerInitCv_.wait(lock, [this] { return initializedWorkerNum_ == config_.workerNum; });
            initStatus = workerInitStatus_;
            initialized_ = initStatus.IsOk();
        }
        if (initStatus.IsError()) {
            ShutdownLocked();
            return initStatus;
        }
        return Status::OK();
    }

    void Shutdown()
    {
        std::lock_guard<std::mutex> lifecycleLock(shutdownMutex_);
        ShutdownLocked();
    }

    void ShutdownLocked()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closing_ = true;
        }
        taskCv_.notify_all();
        progressCv_.notify_all();
        for (auto &worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        {
            std::unique_lock<std::mutex> lock(mutex_);
            progressCv_.wait(lock, [this] { return activeBatchNum_ == 0; });
        }
        workers_.clear();
    }

    Status MemcpyBatch(DeviceBatchCopyHelper &helper)
    {
        PerfPoint totalPoint(perfKeys_.total);
        uint64_t totalBytes = 0;
        RETURN_IF_NOT_OK(ValidateMemcpyBatch(helper, totalBytes));
        RETURN_OK_IF_TRUE(helper.dataSizeList.empty());
        PerfPoint stagePoint(perfKeys_.partition);
        const size_t descriptorCount = helper.dataSizeList.size();
        const size_t taskCount = (descriptorCount - 1) / config_.aggregateNum + 1;
        PerfPoint::RecordElapsed(perfKeys_.taskCount, taskCount);
        PerfPoint::RecordElapsed(perfKeys_.memLen, totalBytes);
        std::shared_ptr<CallState> callState;
        try {
            callState = std::make_shared<CallState>();
        } catch (const std::bad_alloc &) {
            RETURN_STATUS(K_OUT_OF_MEMORY, FormatString("Allocate parallel %s call state failed", direction_));
        }

        stagePoint.RecordAndReset(perfKeys_.submit);
        PartitionState partition{ 0, 0, taskCount, descriptorCount, totalBytes };
        SubmitStats stats;
        SubmitTasks(helper, callState, partition, stats);
        PerfPoint::RecordElapsed(perfKeys_.inlineTaskCount, stats.inlineTaskCount);
        PerfPoint::RecordElapsed(perfKeys_.queueFullCount, stats.queueFullCount);
        stagePoint.RecordAndReset(perfKeys_.wait);
        auto status = callState->Wait();
        stagePoint.Record();
        return status;
    }

private:
    Status ValidateMemcpyBatch(const DeviceBatchCopyHelper &helper, uint64_t &totalBytes)
    {
        RETURN_IF_NOT_OK(ValidateParallelDirectConfig(config_, direction_));
        {
            std::lock_guard<std::mutex> lock(mutex_);
            CHECK_FAIL_RETURN_STATUS(initialized_, K_NOT_READY,
                                     FormatString("Parallel %s executor is not initialized", direction_));
            CHECK_FAIL_RETURN_STATUS(!closing_, K_SHUTTING_DOWN,
                                     FormatString("Parallel %s executor is shutting down", direction_));
        }
        CHECK_FAIL_RETURN_STATUS(
            helper.dstList.size() == helper.srcList.size() && helper.dstList.size() == helper.dataSizeList.size(),
            K_INVALID, FormatString("Parallel %s descriptor list sizes do not match", direction_));
        totalBytes = 0;
        for (auto size : helper.dataSizeList) {
            CHECK_FAIL_RETURN_STATUS(size <= std::numeric_limits<uint64_t>::max() - totalBytes, K_OUT_OF_RANGE,
                                     FormatString("Parallel %s total byte size overflow", direction_));
            totalBytes += size;
        }
        return Status::OK();
    }

    Task BuildNextTask(DeviceBatchCopyHelper &helper, const std::shared_ptr<CallState> &callState,
                       const PartitionState &partition, size_t &batchNum, uint64_t &batchBytes) const
    {
        // Cap the product at SIZE_MAX to avoid unsigned wrap-around; on overflow minBatchNum degrades to 1.
        const size_t remainingTasks = partition.remainingTaskNum - 1;
        const size_t remainingTaskCapacity =
            remainingTasks > std::numeric_limits<size_t>::max() / config_.aggregateNum
                ? std::numeric_limits<size_t>::max()
                : remainingTasks * config_.aggregateNum;
        const size_t minBatchNum = partition.remainingDescriptorNum > remainingTaskCapacity
                                       ? partition.remainingDescriptorNum - remainingTaskCapacity
                                       : 1;
        const size_t maxBatchNum =
            std::min(config_.aggregateNum, partition.remainingDescriptorNum - partition.remainingTaskNum + 1);
        const uint64_t targetBytes =
            partition.remainingBytes / partition.remainingTaskNum
            + static_cast<uint64_t>(partition.remainingBytes % partition.remainingTaskNum != 0);
        batchNum = 0;
        batchBytes = 0;
        while (batchNum < maxBatchNum) {
            batchBytes += helper.dataSizeList[partition.startIndex + batchNum];
            batchNum++;
            if (batchNum >= minBatchNum && batchBytes >= targetBytes) {
                break;
            }
        }
        return Task{ .taskIndex = partition.taskIndex,
                     .startIndex = partition.startIndex,
                     .batchNum = batchNum,
                     .dstList = helper.dstList.data() + partition.startIndex,
                     .dstSizeList = helper.dataSizeList.data() + partition.startIndex,
                     .srcList = helper.srcList.data() + partition.startIndex,
                     .srcSizeList = helper.dataSizeList.data() + partition.startIndex,
                     .callState = callState };
    }

    bool SubmitTaskWithBackpressure(Task &&task, const std::shared_ptr<CallState> &callState, SubmitStats &stats)
    {
        const size_t taskIndex = task.taskIndex;
        bool capacityAvailable = true;
        do {
            callState->ReserveTask();
            auto outcome = TrySubmit(std::move(task));
            if (outcome.result == SubmitResult::ACCEPTED) {
                return true;
            }
            if (outcome.result != SubmitResult::QUEUE_FULL) {
                callState->CompleteTask(taskIndex, outcome.status);
                return false;
            }
            stats.queueFullCount++;
            if (TryRunInline(std::move(task))) {
                stats.inlineTaskCount++;
                return true;
            }
            callState->CancelReservedTask();
            capacityAvailable = WaitUntilAvailable();
        } while (capacityAvailable);
        callState->ObserveError(
            taskIndex, Status(K_SHUTTING_DOWN,
                              FormatString("Parallel %s executor stopped while waiting for capacity", direction_)));
        return false;
    }

    void SubmitTasks(DeviceBatchCopyHelper &helper, const std::shared_ptr<CallState> &callState,
                     PartitionState &partition, SubmitStats &stats)
    {
        while (partition.startIndex < helper.dataSizeList.size() && !callState->HasError()) {
            size_t batchNum = 0;
            uint64_t batchBytes = 0;
            auto task = BuildNextTask(helper, callState, partition, batchNum, batchBytes);
            if (!SubmitTaskWithBackpressure(std::move(task), callState, stats)) {
                return;
            }
            partition.startIndex += batchNum;
            partition.remainingDescriptorNum -= batchNum;
            partition.remainingBytes -= batchBytes;
            partition.remainingTaskNum--;
            partition.taskIndex++;
        }
    }

    SubmitOutcome TrySubmit(Task &&task)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closing_) {
            return SubmitOutcome{ SubmitResult::CLOSED,
                                  Status(K_SHUTTING_DOWN,
                                         FormatString("Parallel %s executor is shutting down", direction_)) };
        }
        if (taskQueue_.size() >= config_.maxPendingTaskNum) {
            return SubmitOutcome{ SubmitResult::QUEUE_FULL, Status::OK() };
        }
        try {
            taskQueue_.emplace_back(std::move(task));
        } catch (const std::bad_alloc &) {
            return SubmitOutcome{ SubmitResult::ERROR,
                                  Status(K_OUT_OF_MEMORY,
                                         FormatString("Enqueue parallel %s task failed", direction_)) };
        }
        taskCv_.notify_one();
        return SubmitOutcome{ SubmitResult::ACCEPTED, Status::OK() };
    }

    bool TryRunInline(Task &&task)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closing_ || activeBatchNum_ >= config_.maxInflightBatchNum) {
                return false;
            }
            activeBatchNum_++;
        }
        ExecuteWithPermit(std::move(task), true);
        return true;
    }

    bool WaitUntilAvailable()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        progressCv_.wait(lock, [this] {
            return closing_ || taskQueue_.size() < config_.maxPendingTaskNum
                   || activeBatchNum_ < config_.maxInflightBatchNum;
        });
        return !closing_;
    }

    bool WaitForTask(Task &task)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        taskCv_.wait(lock, [this] { return closing_ || !taskQueue_.empty(); });
        if (closing_ && taskQueue_.empty()) {
            return false;
        }
        task = std::move(taskQueue_.front());
        taskQueue_.pop_front();
        progressCv_.notify_all();
        progressCv_.wait(lock, [this] { return activeBatchNum_ < config_.maxInflightBatchNum; });
        activeBatchNum_++;
        return true;
    }

    void WorkerLoop()
    {
        if (!Thread::SetCurrentThreadNice(0)) {
            LOG(WARNING) << FormatString("Failed to reset parallel %s worker nice value", direction_);
        }
        Status initStatus;
        try {
            initStatus = deviceManager_->SetDevice(static_cast<int32_t>(deviceId_));
        } catch (const std::exception &error) {
            initStatus = Status(K_RUNTIME_ERROR, FormatString("Set device for parallel %s worker threw exception: %s",
                                                              direction_, error.what()));
        } catch (...) {
            initStatus = Status(K_RUNTIME_ERROR,
                                FormatString("Set device for parallel %s worker threw unknown exception", direction_));
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (initStatus.IsError() && workerInitStatus_.IsOk()) {
                workerInitStatus_ = initStatus;
            }
            initializedWorkerNum_++;
        }
        workerInitCv_.notify_one();
        if (initStatus.IsError()) {
            LOG(ERROR) << FormatString("Set device %u for parallel %s worker failed: %s", deviceId_, direction_,
                                       initStatus.ToString());
            return;
        }
        Task task;
        while (WaitForTask(task)) {
            ExecuteWithPermit(std::move(task), false);
        }
    }

    void ExecuteWithPermit(Task &&task, bool setDevice)
    {
        Status status = Status::OK();
        try {
            if (setDevice) {
                status = deviceManager_->SetDevice(static_cast<int32_t>(deviceId_));
                if (status.IsError()) {
                    LOG(ERROR) << FormatString("Set device %u for inline parallel %s task %zu failed: %s", deviceId_,
                                               direction_, task.taskIndex, status.ToString());
                }
            }
            if (status.IsOk()) {
                status = ExecuteTask(task);
            }
        } catch (const std::exception &error) {
            status =
                Status(K_RUNTIME_ERROR, FormatString("Parallel %s task threw exception: %s", direction_, error.what()));
        } catch (...) {
            status = Status(K_RUNTIME_ERROR, FormatString("Parallel %s task threw unknown exception", direction_));
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (activeBatchNum_ > 0) {
                activeBatchNum_--;
            } else {
                // Bookkeeping bug: surface instead of wrapping to SIZE_MAX.
                LOG(ERROR) << FormatString(
                    "Parallel %s activeBatchNum_ underflow on task %zu: already zero before decrement", direction_,
                    task.taskIndex);
            }
        }
        progressCv_.notify_all();
        task.callState->CompleteTask(task.taskIndex, status);
    }

    Status ExecuteTask(const Task &task)
    {
        size_t failedIndex = 0;
        auto status = deviceManager_->MemcpyBatch(task.dstList, task.dstSizeList, task.srcList, task.srcSizeList,
                                                  task.batchNum, kind_, deviceId_, &failedIndex);
        if (status.IsError()) {
            LOG(ERROR) << FormatString("Parallel %s task %zu failed at descriptor %zu on device %u: %s", direction_,
                                       task.taskIndex, task.startIndex + failedIndex, deviceId_, status.ToString());
        }
        return status;
    }

    uint32_t deviceId_;
    DeviceManagerBase *deviceManager_;
    ParallelDirectExecutorConfig config_;
    MemcpyKind kind_;
    const char *direction_;
    ParallelDirectPerfKeys perfKeys_;
    std::mutex shutdownMutex_;
    std::mutex mutex_;
    std::condition_variable taskCv_;
    std::condition_variable progressCv_;
    std::condition_variable workerInitCv_;
    std::deque<Task> taskQueue_;
    std::vector<Thread> workers_;
    size_t activeBatchNum_ = 0;
    size_t initializedWorkerNum_ = 0;
    Status workerInitStatus_;
    bool initialized_ = false;
    bool closing_ = false;
};

AclParallelDirectExecutor::AclParallelDirectExecutor(uint32_t deviceId, DeviceManagerBase *deviceManager,
                                                     ParallelH2DConfig config)
    : impl_(std::make_unique<Impl>(deviceId, deviceManager, ToExecutorConfig(config), MemcpyKind::HOST_TO_DEVICE))
{
}

AclParallelDirectExecutor::AclParallelDirectExecutor(uint32_t deviceId, DeviceManagerBase *deviceManager,
                                                     ParallelD2HConfig config)
    : impl_(std::make_unique<Impl>(deviceId, deviceManager, ToExecutorConfig(config), MemcpyKind::DEVICE_TO_HOST))
{
}

AclParallelDirectExecutor::~AclParallelDirectExecutor() = default;

Status AclParallelDirectExecutor::Init()
{
    return impl_->Init();
}

Status AclParallelDirectExecutor::MemcpyBatch(DeviceBatchCopyHelper &helper)
{
    return impl_->MemcpyBatch(helper);
}

void AclParallelDirectExecutor::Shutdown()
{
    impl_->Shutdown();
}

}  // namespace datasystem
