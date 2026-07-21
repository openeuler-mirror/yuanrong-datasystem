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

#include "datasystem/common/device/ascend/acl_parallel_ffts_executor.h"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <future>
#include <limits>
#include <new>
#include <numeric>
#include <stdexcept>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/device/ascend/acl_parallel_config_util.h"

namespace datasystem {
namespace {
constexpr size_t DEFAULT_PARALLEL_FFTS_WORKER_NUM = 1;
constexpr size_t MAX_PARALLEL_FFTS_WORKER_NUM = 16;
constexpr size_t FFTS_PIPELINE_DEPTH = 2;
constexpr uint64_t DEFAULT_PARALLEL_FFTS_MIN_BYTES = 24ULL * 1024ULL * 1024ULL;
constexpr char H2D_FFTS_PARALLEL_WORKER_NUM_ENV[] = "DS_H2D_FFTS_PARALLEL_WORKER_NUM";
constexpr char H2D_FFTS_PARALLEL_MIN_BYTES_ENV[] = "DS_H2D_FFTS_PARALLEL_MIN_BYTES";
constexpr char D2H_FFTS_PARALLEL_WORKER_NUM_ENV[] = "DS_D2H_FFTS_PARALLEL_WORKER_NUM";
constexpr char D2H_FFTS_PARALLEL_MIN_BYTES_ENV[] = "DS_D2H_FFTS_PARALLEL_MIN_BYTES";

struct ParallelFftsShardRange {
    size_t begin;
    size_t end;
    uint64_t bytes;
    size_t maxObjectSize;
};

struct ParallelFftsPerfKeys {
    PerfKey partition;
    PerfKey taskCount;
    PerfKey memLen;
    PerfKey total;
    PerfKey submit;
    PerfKey wait;
};

Status LoadParallelFftsConfig(const char *workerEnv, const char *minBytesEnv, size_t &workerNum, uint64_t &minBytes)
{
    uint64_t value = workerNum;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(workerEnv, 1, MAX_PARALLEL_FFTS_WORKER_NUM, value));
    workerNum = static_cast<size_t>(value);
    value = minBytes;
    RETURN_IF_NOT_OK(ParseUint64FromEnv(minBytesEnv, 0, std::numeric_limits<uint64_t>::max(), value));
    minBytes = value;
    return Status::OK();
}

ParallelFftsPerfKeys GetParallelFftsPerfKeys(MemcpyKind kind)
{
    if (kind == MemcpyKind::DEVICE_TO_HOST) {
        return { PerfKey::CLIENT_D2H_PARALLEL_FFTS_PARTITION, PerfKey::CLIENT_D2H_PARALLEL_FFTS_TASK_COUNT,
                 PerfKey::CLIENT_D2H_PARALLEL_FFTS_MEMLEN,    PerfKey::TOTAL_D2H_PARALLEL_FFTS_MEMCPY,
                 PerfKey::CLIENT_D2H_PARALLEL_FFTS_SUBMIT,    PerfKey::CLIENT_D2H_PARALLEL_FFTS_WAIT };
    }
    return { PerfKey::CLIENT_H2D_PARALLEL_FFTS_PARTITION, PerfKey::CLIENT_H2D_PARALLEL_FFTS_TASK_COUNT,
             PerfKey::CLIENT_H2D_PARALLEL_FFTS_MEMLEN,    PerfKey::TOTAL_H2D_PARALLEL_FFTS_MEMCPY,
             PerfKey::CLIENT_H2D_PARALLEL_FFTS_SUBMIT,    PerfKey::CLIENT_H2D_PARALLEL_FFTS_WAIT };
}

Status ValidateParallelFftsInput(const DeviceBatchCopyHelper &helper, size_t workerNum, MemcpyKind kind,
                                 uint64_t &totalBytes)
{
    CHECK_FAIL_RETURN_STATUS(workerNum > 0, K_INVALID, "Parallel FFTS worker number must be positive");
    const auto &objectBuffers = kind == MemcpyKind::HOST_TO_DEVICE ? helper.srcBuffers : helper.dstBuffers;
    const auto &blobBuffers = kind == MemcpyKind::HOST_TO_DEVICE ? helper.dstBuffers : helper.srcBuffers;
    CHECK_FAIL_RETURN_STATUS(helper.bufferMetas.size() == objectBuffers.size(), K_INVALID,
                             "FFTS object metadata and object buffer count mismatch");
    totalBytes = 0;
    for (const auto &meta : helper.bufferMetas) {
        CHECK_FAIL_RETURN_STATUS(
            meta.firstBlobOffset <= blobBuffers.size() && meta.blobCount <= blobBuffers.size() - meta.firstBlobOffset,
            K_INVALID, "FFTS object blob range exceeds blob buffers");
        CHECK_FAIL_RETURN_STATUS(meta.size <= std::numeric_limits<uint64_t>::max() - totalBytes, K_INVALID,
                                 "FFTS object bytes overflow uint64");
        totalBytes += meta.size;
    }
    return Status::OK();
}

ParallelFftsShardRange GetParallelFftsShardRange(const std::vector<BufferMetaInfo> &bufferMetas, size_t begin,
                                                 size_t maxEnd, uint64_t targetBytes)
{
    ParallelFftsShardRange range{ begin, begin, 0, 0 };
    while (range.end < maxEnd) {
        const auto &meta = bufferMetas[range.end];
        const uint64_t afterBytes = range.bytes + meta.size;
        if (range.end > begin && range.bytes < targetBytes) {
            const uint64_t beforeDistance = targetBytes - range.bytes;
            const uint64_t afterDistance =
                afterBytes > targetBytes ? afterBytes - targetBytes : targetBytes - afterBytes;
            if (beforeDistance <= afterDistance) {
                break;
            }
        }
        range.bytes = afterBytes;
        range.maxObjectSize = std::max(range.maxObjectSize, meta.size);
        range.end++;
        if (range.bytes >= targetBytes) {
            break;
        }
    }
    return range;
}

void AppendParallelFftsObject(const BufferMetaInfo &meta, const BufferView &objectBuffer,
                              const std::vector<BufferView> &blobBuffers, MemcpyKind kind, DeviceBatchCopyHelper &shard)
{
    const size_t firstBlobOffset =
        kind == MemcpyKind::HOST_TO_DEVICE ? shard.dstBuffers.size() : shard.srcBuffers.size();
    shard.bufferMetas.emplace_back(
        BufferMetaInfo{ .blobCount = meta.blobCount, .firstBlobOffset = firstBlobOffset, .size = meta.size });
    const auto blobBegin = blobBuffers.begin() + static_cast<std::ptrdiff_t>(meta.firstBlobOffset);
    const auto blobEnd = blobBegin + static_cast<std::ptrdiff_t>(meta.blobCount);
    if (kind == MemcpyKind::HOST_TO_DEVICE) {
        shard.dstBuffers.insert(shard.dstBuffers.end(), blobBegin, blobEnd);
        shard.srcBuffers.emplace_back(objectBuffer);
    } else {
        shard.srcBuffers.insert(shard.srcBuffers.end(), blobBegin, blobEnd);
        shard.dstBuffers.emplace_back(objectBuffer);
    }
    shard.batchSize += meta.blobCount;
}

Status BuildParallelFftsShard(const DeviceBatchCopyHelper &helper, MemcpyKind kind, const ParallelFftsShardRange &range,
                              DeviceBatchCopyHelper &shard, uint64_t &deviceStagingBytes)
{
    const auto &objectBuffers = kind == MemcpyKind::HOST_TO_DEVICE ? helper.srcBuffers : helper.dstBuffers;
    const auto &blobBuffers = kind == MemcpyKind::HOST_TO_DEVICE ? helper.dstBuffers : helper.srcBuffers;
    shard.bufferMetas.reserve(range.end - range.begin);
    shard.srcBuffers.reserve(range.end - range.begin);
    for (size_t objectIndex = range.begin; objectIndex < range.end; ++objectIndex) {
        AppendParallelFftsObject(helper.bufferMetas[objectIndex], objectBuffers[objectIndex], blobBuffers, kind, shard);
    }
    CHECK_FAIL_RETURN_STATUS(range.maxObjectSize <= std::numeric_limits<uint64_t>::max() / FFTS_PIPELINE_DEPTH,
                             K_INVALID, "Parallel FFTS staging bytes overflow uint64");
    const uint64_t shardStagingBytes = static_cast<uint64_t>(range.maxObjectSize) * FFTS_PIPELINE_DEPTH;
    CHECK_FAIL_RETURN_STATUS(shardStagingBytes <= std::numeric_limits<uint64_t>::max() - deviceStagingBytes, K_INVALID,
                             "Parallel FFTS staging bytes overflow uint64");
    deviceStagingBytes += shardStagingBytes;
    return Status::OK();
}
}  // namespace

ParallelFftsH2DConfig::ParallelFftsH2DConfig()
    : workerNum(DEFAULT_PARALLEL_FFTS_WORKER_NUM), minBytes(DEFAULT_PARALLEL_FFTS_MIN_BYTES)
{
}

Status ParallelFftsH2DConfig::LoadFromEnv()
{
    *this = ParallelFftsH2DConfig();
    RETURN_IF_NOT_OK(
        LoadParallelFftsConfig(H2D_FFTS_PARALLEL_WORKER_NUM_ENV, H2D_FFTS_PARALLEL_MIN_BYTES_ENV, workerNum, minBytes));
    return Validate();
}

Status ParallelFftsH2DConfig::Validate() const
{
    CHECK_FAIL_RETURN_STATUS(workerNum >= 1 && workerNum <= MAX_PARALLEL_FFTS_WORKER_NUM, K_INVALID,
                             "Parallel FFTS H2D worker number is out of range");
    return Status::OK();
}

std::string ParallelFftsH2DConfig::ToString() const
{
    return FormatString("ParallelFftsH2DConfig { workerNum:%zu, minBytes:%llu }", workerNum,
                        static_cast<unsigned long long>(minBytes));
}

ParallelFftsD2HConfig::ParallelFftsD2HConfig()
    : workerNum(DEFAULT_PARALLEL_FFTS_WORKER_NUM), minBytes(DEFAULT_PARALLEL_FFTS_MIN_BYTES)
{
}

Status ParallelFftsD2HConfig::LoadFromEnv()
{
    *this = ParallelFftsD2HConfig();
    RETURN_IF_NOT_OK(
        LoadParallelFftsConfig(D2H_FFTS_PARALLEL_WORKER_NUM_ENV, D2H_FFTS_PARALLEL_MIN_BYTES_ENV, workerNum, minBytes));
    return Validate();
}

Status ParallelFftsD2HConfig::Validate() const
{
    CHECK_FAIL_RETURN_STATUS(workerNum >= 1 && workerNum <= MAX_PARALLEL_FFTS_WORKER_NUM, K_INVALID,
                             "Parallel FFTS D2H worker number is out of range");
    return Status::OK();
}

std::string ParallelFftsD2HConfig::ToString() const
{
    return FormatString("ParallelFftsD2HConfig { workerNum:%zu, minBytes:%llu }", workerNum,
                        static_cast<unsigned long long>(minBytes));
}

Status BuildParallelFftsShards(const DeviceBatchCopyHelper &helper, size_t workerNum, MemcpyKind kind,
                               std::vector<DeviceBatchCopyHelper> &shards, uint64_t &deviceStagingBytes)
{
    shards.clear();
    deviceStagingBytes = 0;
    uint64_t totalBytes = 0;
    RETURN_IF_NOT_OK(ValidateParallelFftsInput(helper, workerNum, kind, totalBytes));
    RETURN_OK_IF_TRUE(helper.bufferMetas.empty());

    const size_t shardCount = std::min(workerNum, helper.bufferMetas.size());
    shards.reserve(shardCount);
    size_t begin = 0;
    uint64_t remainingBytes = totalBytes;
    for (size_t shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
        const size_t remainingShards = shardCount - shardIndex;
        const size_t maxEnd = helper.bufferMetas.size() - (remainingShards - 1);
        const uint64_t targetBytes = remainingBytes / remainingShards + (remainingBytes % remainingShards != 0);
        const auto range = GetParallelFftsShardRange(helper.bufferMetas, begin, maxEnd, targetBytes);
        DeviceBatchCopyHelper shard;
        RETURN_IF_NOT_OK(BuildParallelFftsShard(helper, kind, range, shard, deviceStagingBytes));
        shards.emplace_back(std::move(shard));
        remainingBytes -= range.bytes;
        begin = range.end;
    }
    return Status::OK();
}

struct AclParallelFftsExecutor::Impl {
    Impl(DeviceResourceManager *resourceManager, DeviceManagerBase *deviceManager, FftsCopyFunction copyFunction,
         size_t workerNum, MemcpyKind kind)
        : resourceManager_(resourceManager),
          deviceManager_(deviceManager),
          copyFunction_(std::move(copyFunction)),
          workerNum_(workerNum),
          kind_(kind),
          direction_(kind == MemcpyKind::DEVICE_TO_HOST ? "D2H" : "H2D"),
          perfKeys_(GetParallelFftsPerfKeys(kind))
    {
    }

    ~Impl()
    {
        // Drain outer shard tasks before destroying the D2H device-submit pool they use.
        hostPool_.reset();
        devicePool_.reset();
    }

    Status Memcpy(uint32_t deviceId, DeviceBatchCopyHelper &helper)
    {
        CHECK_FAIL_RETURN_STATUS(resourceManager_ != nullptr && deviceManager_ != nullptr && copyFunction_ != nullptr,
                                 K_INVALID, "Parallel FFTS executor dependency is null");
        CHECK_FAIL_RETURN_STATUS(workerNum_ > 0, K_INVALID, "Parallel FFTS worker number must be positive");
        RETURN_IF_NOT_OK(InitPools());
        std::vector<DeviceBatchCopyHelper> shards;
        uint64_t deviceStagingBytes = 0;
        PerfPoint point(perfKeys_.partition);
        RETURN_IF_NOT_OK(PrepareShards(helper, point, shards, deviceStagingBytes));
        if (deviceStagingBytes > resourceManager_->GetDeviceMemSize()) {
            VLOG(1) << FormatString("Use serial FFTS %s because parallel staging needs %llu bytes, pool has %llu bytes",
                                    direction_, static_cast<unsigned long long>(deviceStagingBytes),
                                    static_cast<unsigned long long>(resourceManager_->GetDeviceMemSize()));
            return ExecuteSerial(deviceId, helper);
        }

        RecordWorkload(helper, shards.size());
        PerfPoint totalPoint(perfKeys_.total);
        point.Reset(perfKeys_.submit);
        std::vector<std::future<Status>> futures;
        RETURN_IF_NOT_OK(SubmitShards(deviceId, shards, futures));
        point.RecordAndReset(perfKeys_.wait);
        auto result = DrainFutures(futures);
        point.Record();
        return result;
    }

private:
    Status InitPools()
    {
        std::lock_guard<std::mutex> lock(poolInitMutex_);
        const bool initialized =
            kind_ == MemcpyKind::HOST_TO_DEVICE ? hostPool_ != nullptr : hostPool_ != nullptr && devicePool_ != nullptr;
        RETURN_OK_IF_TRUE(initialized);
        try {
            if (kind_ == MemcpyKind::HOST_TO_DEVICE) {
                hostPool_ = std::make_unique<ThreadPool>(workerNum_, workerNum_, "H2DParallelFfts");
            } else {
                hostPool_ = std::make_unique<ThreadPool>(workerNum_, workerNum_, "D2HFftsHost");
                devicePool_ = std::make_unique<ThreadPool>(workerNum_, workerNum_, "D2HFftsDevice");
            }
        } catch (const std::bad_alloc &) {
            hostPool_.reset();
            devicePool_.reset();
            RETURN_STATUS(K_OUT_OF_MEMORY, FormatString("Allocate parallel FFTS %s thread pool failed", direction_));
        } catch (const std::exception &e) {
            hostPool_.reset();
            devicePool_.reset();
            RETURN_STATUS(K_RUNTIME_ERROR,
                          FormatString("Create parallel FFTS %s thread pool failed: %s", direction_, e.what()));
        }
        return Status::OK();
    }

    Status PrepareShards(const DeviceBatchCopyHelper &helper, PerfPoint &point,
                         std::vector<DeviceBatchCopyHelper> &shards, uint64_t &deviceStagingBytes) const
    {
        try {
            RETURN_IF_NOT_OK(BuildParallelFftsShards(helper, workerNum_, kind_, shards, deviceStagingBytes));
        } catch (const std::bad_alloc &) {
            RETURN_STATUS(K_OUT_OF_MEMORY, FormatString("Allocate parallel FFTS %s shard metadata failed", direction_));
        }
        point.Record();
        return Status::OK();
    }

    void RecordWorkload(const DeviceBatchCopyHelper &helper, size_t taskCount) const
    {
        PerfPoint::RecordElapsed(perfKeys_.taskCount, taskCount);
        PerfPoint::RecordElapsed(perfKeys_.memLen,
                                 std::accumulate(helper.bufferMetas.begin(), helper.bufferMetas.end(), uint64_t{ 0 },
                                                 [](uint64_t total, const BufferMetaInfo &meta) {
                                                     return total + static_cast<uint64_t>(meta.size);
                                                 }));
    }

    Status ExecuteSerial(uint32_t deviceId, DeviceBatchCopyHelper &helper) const
    {
        return copyFunction_(deviceId, helper, false, nullptr);
    }

    Status ExecuteShard(uint32_t deviceId, DeviceBatchCopyHelper &shard) const
    {
        try {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                deviceManager_->SetDevice(static_cast<int32_t>(deviceId)),
                FormatString("Set device %u for parallel FFTS %s task failed", deviceId, direction_));
            return copyFunction_(deviceId, shard, true, devicePool_.get());
        } catch (const std::bad_alloc &) {
            RETURN_STATUS(K_OUT_OF_MEMORY, FormatString("Parallel FFTS %s task allocation failed", direction_));
        } catch (const std::exception &e) {
            RETURN_STATUS(K_RUNTIME_ERROR,
                          FormatString("Parallel FFTS %s task threw exception: %s", direction_, e.what()));
        }
    }

    Status SubmitShards(uint32_t deviceId, std::vector<DeviceBatchCopyHelper> &shards,
                        std::vector<std::future<Status>> &futures)
    {
        try {
            futures.reserve(shards.size());
            for (auto &shard : shards) {
                auto *shardPtr = &shard;
                futures.emplace_back(
                    hostPool_->Submit([this, deviceId, shardPtr] { return ExecuteShard(deviceId, *shardPtr); }));
            }
        } catch (const std::bad_alloc &) {
            (void)DrainFutures(futures);
            RETURN_STATUS(K_OUT_OF_MEMORY,
                          FormatString("Submit parallel FFTS %s task failed due to out of memory", direction_));
        } catch (const std::exception &e) {
            (void)DrainFutures(futures);
            RETURN_STATUS(K_RUNTIME_ERROR,
                          FormatString("Submit parallel FFTS %s task failed: %s", direction_, e.what()));
        }
        return Status::OK();
    }

    Status DrainFutures(std::vector<std::future<Status>> &futures) const
    {
        Status result;
        for (auto &future : futures) {
            auto status = future.get();
            if (result.IsOk() && status.IsError()) {
                result = status;
            }
        }
        return result;
    }

    DeviceResourceManager *resourceManager_;
    DeviceManagerBase *deviceManager_;
    FftsCopyFunction copyFunction_;
    size_t workerNum_;
    MemcpyKind kind_;
    const char *direction_;
    ParallelFftsPerfKeys perfKeys_;
    std::unique_ptr<ThreadPool> hostPool_;
    std::unique_ptr<ThreadPool> devicePool_;
    // Makes InitPools self-contained; failure is not cached so a later Memcpy can retry.
    std::mutex poolInitMutex_;
};

AclParallelFftsExecutor::AclParallelFftsExecutor(DeviceResourceManager *resourceManager,
                                                 DeviceManagerBase *deviceManager, FftsCopyFunction copyFunction,
                                                 ParallelFftsH2DConfig config)
    : impl_(std::make_unique<Impl>(resourceManager, deviceManager, std::move(copyFunction), config.workerNum,
                                   MemcpyKind::HOST_TO_DEVICE))
{
}

AclParallelFftsExecutor::AclParallelFftsExecutor(DeviceResourceManager *resourceManager,
                                                 DeviceManagerBase *deviceManager, FftsCopyFunction copyFunction,
                                                 ParallelFftsD2HConfig config)
    : impl_(std::make_unique<Impl>(resourceManager, deviceManager, std::move(copyFunction), config.workerNum,
                                   MemcpyKind::DEVICE_TO_HOST))
{
}

AclParallelFftsExecutor::~AclParallelFftsExecutor() = default;

Status AclParallelFftsExecutor::Memcpy(uint32_t deviceId, DeviceBatchCopyHelper &helper)
{
    return impl_->Memcpy(deviceId, helper);
}

}  // namespace datasystem
