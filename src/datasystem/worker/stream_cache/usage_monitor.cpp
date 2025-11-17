/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
#include "datasystem/worker/stream_cache/usage_monitor.h"
#include <mutex>

#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
UsageItem::UsageItem() : usage(0), usageBlocked(false)
{
}

UsageItem::UsageItem(std::string streamName, std::string remoteWorkerAddr, std::uint64_t usage)
    : streamName(std::move(streamName)),
      remoteWorkerAddr(std::move(remoteWorkerAddr)),
      usage(usage),
      usageBlocked(false)
{
}

MemReserveEntry::MemReserveEntry(uint64_t reserve) : reserveSize(reserve), usedSize(0)
{
}

UsageMonitor::UsageMonitor(ClientWorkerSCServiceImpl *clientWorkerScService, const uint64_t maxBufferPoolMem)
    : clientWorkerScService_(clientWorkerScService),
      totalUsedSize_(0),
      maxBufferPoolMem_(maxBufferPoolMem),
      interrupt_(false)
{
}

Status UsageMonitor::Init()
{
    producerBlockerThreadPool_ = std::make_unique<ThreadPool>(1, 0, "ScUsageMonitor");
    try {
        producerBlockerThreadPool_->Execute([this]() { BlockProducersIfNeeded(); });
        return Status::OK();
    } catch (const std::exception &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, e.what());
    }
}

void UsageMonitor::Stop()
{
    interrupt_ = true;
    cv_.notify_all();
    producerBlockerThreadPool_.reset();
}

Status UsageMonitor::IncUsage(const std::string &streamName, const std::string &workerAddr, const std::uint64_t size)
{
    std::shared_lock<std::shared_timed_mutex> l(usageMutex_);

    // Update per stream usage
    TbbStreamReserveTable::Accessor accessorStTbl;
    if (streamMemoryMap_.Find(accessorStTbl, streamName)) {
        accessorStTbl.entry->data->usedSize += size;
    } else {
        LOG(WARNING) << "Stream name not found for the reservation";
    }

    return IncTotalUsageUnlocked(streamName, workerAddr, size);
}

Status UsageMonitor::IncTotalUsageUnlocked(const std::string &streamName, const std::string &workerAddr,
                                           const std::uint64_t size)
{
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Increase Usage for stream " << streamName << " Remote worker "
                              << workerAddr << " by size " << size << " max size available " << maxBufferPoolMem_;
    std::string id = streamName + workerAddr;
    // Update per stream per remote worker usage
    TbbUsageTable::accessor accessor;
    if (usage_.insert(accessor, id)) {
        // Insert new item
        accessor->second = std::make_shared<UsageItem>(streamName, workerAddr, size);
    } else {
        // Update usage.
        accessor->second->usage += size;
    }

    // Update total usage count
    totalUsedSize_.fetch_add(size, std::memory_order_relaxed);
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Total memory used " << totalUsedSize_;
    return Status::OK();
}

void UsageMonitor::DecUsage(const std::uint64_t size)
{
    do {
        uint64_t val = totalUsedSize_.load();
        uint64_t updatedVal = val;
        // Update total usage count
        if (updatedVal > size) {
            updatedVal -= size;
        } else {
            updatedVal = 0;
        }
        if (totalUsedSize_.compare_exchange_strong(val, updatedVal)) {
            return;
        }
    } while (true);
}

Status UsageMonitor::DecUsage(const std::string &streamName, const std::string &workerAddr, const std::uint64_t size)
{
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Decrease Usage for stream " << streamName << " Remote worker "
                              << workerAddr << " by size " << size << " max size available " << maxBufferPoolMem_;
    std::string id = streamName + workerAddr;
    std::shared_lock<std::shared_timed_mutex> l(usageMutex_);
    // Update per stream usage
    TbbStreamReserveTable::Accessor accessorStTbl;
    if (streamMemoryMap_.Find(accessorStTbl, streamName)) {
        accessorStTbl.entry->data->usedSize -= size;
    }

    // Update per stream per remote worker usage
    TbbUsageTable::accessor accessor;
    if (usage_.find(accessor, id)) {
        // If usage is 0 or less delete it
        if (accessor->second->usage > size) {
            accessor->second->usage -= size;
        } else {
            (void)usage_.erase(accessor);
        }
    } else {
        // If key not found its a error. Usage is never recorded
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "usage key not found");
    }

    DecUsage(size);

    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Total memory used " << totalUsedSize_;
    return Status::OK();
}

Status UsageMonitor::RemoveUsageStats(const std::string &streamName, const std::string &workerAddr)
{
    std::string id = streamName + workerAddr;
    std::shared_lock<std::shared_timed_mutex> l(usageMutex_);
    TbbUsageTable::accessor accessor;
    if (usage_.find(accessor, id)) {
        (void)usage_.erase(accessor);
    } else {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "usage key not found");
    }
    return Status::OK();
}

Status UsageMonitor::CheckOverUsed(const double threshold, const uint64_t size)
{
    if (totalUsedSize_.load() + size > maxBufferPoolMem_ * threshold) {
        VLOG(SC_NORMAL_LOG_LEVEL) << "BufferPool is out of Memory, Total used: " << totalUsedSize_.load()
                                  << " Total allocated " << maxBufferPoolMem_ << " Limited by "
                                  << maxBufferPoolMem_ * threshold;
        RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "BufferPool is out of memory");
    }
    return Status::OK();
}

Status UsageMonitor::CheckNIncOverUsedForStream(const std::string &streamName, const std::string &workerAddr,
                                                const uint64_t lowerBound, const double threshold, const uint64_t size)
{
    INJECT_POINT("worker.UsageMonitor.CheckOverUsedForStream.MockError");
    // Update per stream per remote worker usage
    auto limit = std::max<double>(lowerBound, (maxBufferPoolMem_ * threshold));
    std::shared_lock<std::shared_timed_mutex> l(usageMutex_);
    TbbStreamReserveTable::Accessor accessor;
    if (streamMemoryMap_.Find(accessor, streamName)) {
        auto &entry = accessor.entry->data;
        if (entry->usedSize > limit) {
            RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY,
                          FormatString("BufferPool is out of memory per stream %s, TotalUsed=%llu, LimitedBy=%f",
                                       streamName, entry->usedSize, limit));
        }
        uint64_t remainingReservedSize = entry->reserveSize - std::min(entry->reserveSize, entry->usedSize);
        // Best effort check remaining fair share reserved memory + available mutual memory is enough for the request
        uint64_t totalUsedSize = totalUsedSize_.load();
        uint64_t totalReservedSize = totalReservedSize_.load();
        if (std::max(totalUsedSize, totalReservedSize) + size
            > remainingReservedSize + maxBufferPoolMem_ * DEFAULT_THRESHOLD) {
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
                "BufferPool is out of Memory, Total used: %llu, Total reserved: %llu, Total allocated %llu Limited by "
                "%llu",
                totalUsedSize, totalReservedSize, maxBufferPoolMem_, maxBufferPoolMem_ * threshold);
            RETURN_STATUS(StatusCode::K_OUT_OF_MEMORY, "BufferPool is out of memory");
        }
        INJECT_POINT("CheckNIncOverUsedForStream.TbbStreamReserveTable.CPU");
        // Increase the memory usage so that the reserved memory will not be counted for multiple times
        entry->usedSize += size;
        IncTotalUsageUnlocked(streamName, workerAddr, size);
        return Status::OK();
    }
    // The memory has to be reserved upfront
    RETURN_STATUS(StatusCode::K_INVALID, "Local cache memory is not reserved.");
}

Status UsageMonitor::GetMostUsed(std::shared_ptr<UsageItem> &usageItem)
{
    std::lock_guard<std::shared_timed_mutex> l(usageMutex_);
    if (usage_.empty()) {
        RETURN_STATUS(StatusCode::K_INVALID, "usage vector is empty");
    }
    auto iter = std::max_element(usage_.begin(), usage_.end(), [](auto a, auto b) {
        return ((a.second->usage < b.second->usage) && !b.second->usageBlocked);
    });
    RETURN_RUNTIME_ERROR_IF_NULL(iter->second);
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Most used memory stream " << iter->second->streamName << " producer "
                              << iter->second->remoteWorkerAddr << " by size " << iter->second->usage;
    usageItem = iter->second;
    return Status::OK();
}

Status UsageMonitor::BlockUsage(std::shared_ptr<UsageItem> &usageItem)
{
    Status rc = clientWorkerScService_->SendBlockProducerReq(usageItem->streamName, usageItem->remoteWorkerAddr);
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Usage Blocked for stream: " << usageItem->streamName
                              << " remote producer " << usageItem->remoteWorkerAddr;
    // If ok or if stream or producer is already deleted then make blocked true
    // If producer is already gone no need to block it
    if (rc.IsOk() || rc.GetCode() == StatusCode::K_SC_STREAM_NOT_FOUND
        || rc.GetCode() == StatusCode::K_SC_PRODUCER_NOT_FOUND) {
        usageItem->usageBlocked = true;
        LOG_IF_ERROR(rc, "Error while blocking");
        return Status::OK();
    }
    return rc;
}

Status UsageMonitor::UnBlockUsage(std::shared_ptr<UsageItem> &usageItem)
{
    Status rc = clientWorkerScService_->SendUnBlockProducerReq(usageItem->streamName, usageItem->remoteWorkerAddr);
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Usage UnBlocked for stream: " << usageItem->streamName
                              << " remote producer " << usageItem->remoteWorkerAddr;
    // If ok or if stream or producer is already deleted then make blocked false
    // If producer is already gone no need to unblock it
    if (rc.IsOk() || rc.GetCode() == StatusCode::K_SC_STREAM_NOT_FOUND
        || rc.GetCode() == StatusCode::K_SC_PRODUCER_NOT_FOUND) {
        usageItem->usageBlocked = false;
        LOG_IF_ERROR(rc, "Error while unblocking");
        return Status::OK();
    }
    return rc;
}

bool UsageMonitor::WaitForOverUseCondition(const uint64_t timeoutMs, const double threshold)
{
    std::unique_lock<std::mutex> lock(mux_);
    // CheckOverused returns error when no memory is available
    return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs),
                        [this, threshold]() { return CheckOverUsed(threshold).IsError(); });
}

Status UsageMonitor::BlockMostUsed()
{
    // Get producer that produces most
    std::shared_ptr<UsageItem> usageItem;
    RETURN_IF_NOT_OK(GetMostUsed(usageItem));
    // Block the producer
    RETURN_IF_NOT_OK(BlockUsage(usageItem));
    // Add it to the list so that we can unblock them later
    blockedStreamProducers_.push_back(usageItem);
    return Status::OK();
}

Status UsageMonitor::UnBlockAllProducers(const double unBlockThreshold)
{
    // If more than unBlockThreshold of Memory is available
    // CheckOverUsed OK means memory is available
    if (!blockedStreamProducers_.empty() && CheckOverUsed(unBlockThreshold).IsOk()) {
        for (auto Iter = blockedStreamProducers_.begin(); Iter != blockedStreamProducers_.end();) {
            // we only try once if does not work it will timeout at sender side
            LOG_IF_ERROR(UnBlockUsage(*Iter), "Error in unblocking producer");
            Iter = blockedStreamProducers_.erase(Iter);
        }
    }
    return Status::OK();
}

void UsageMonitor::BlockProducersIfNeeded()
{
    const uint64_t timeoutMs = 100;
    // We block when 90% of memory is used
    // Unblock when more than 30% of memory is available (i.e. < 70% in use)
    const auto blockThreshold = 0.9;
    const auto unBlockThreshold = 0.7;
    const auto waitTimeSecs = 1;
    while (true) {
        // Wait on the cv for 0.1s for work or interrupt
        auto overUsed = WaitForOverUseCondition(timeoutMs, blockThreshold);
        if (interrupt_) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << "BlockProducers thread exits";
            break;
        }
        // Memory is available
        if (!overUsed) {
            // unblock all producers that were blocked
            UnBlockAllProducers(unBlockThreshold);
            continue;  // No need to block continue to check
        }
        // Memory is not available
        // Block the producer that uses most memory
        auto rc = BlockMostUsed();
        if (rc.IsError()) {
            LOG_IF_ERROR(rc, "Error while blocking most used producer");
            continue;  // try again
        }
        std::this_thread::sleep_for(std::chrono::seconds(waitTimeSecs));  // Wait for memory usage to reduce
    }
}

Status UsageMonitor::ReserveMemory(const std::string &streamName, size_t reserveSize)
{
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Reserve memory for: " << streamName << " with size = " << reserveSize;
    std::shared_lock<std::shared_timed_mutex> l(usageMutex_);
    TbbStreamReserveTable::Accessor accessor;
    if (streamMemoryMap_.Insert(accessor, streamName)) {
        // As long as the total reserved memory is still in bound, it is allowed to reserve.
        auto func = [this, reserveSize]() {
            RETURN_IF_NOT_OK(CheckNIncTotalReservedMemory(reserveSize));
            return Status::OK();
        };
        Status rc = func();
        if (rc.IsError()) {
            streamMemoryMap_.BlockingErase(accessor);
            return rc;
        }
        accessor.entry->data = std::make_shared<MemReserveEntry>(reserveSize);
    } else {
        // If the reservation already exists, update the entry if applicable.
        // The reserve size should be max between chunk size and page size, so it should not be less.
        auto &entry = accessor.entry->data;
        if (reserveSize > entry->reserveSize) {
            uint64_t difference = reserveSize - entry->reserveSize;
            RETURN_IF_NOT_OK(CheckNIncTotalReservedMemory(difference));
            entry->reserveSize = reserveSize;
        }
    }
    return Status::OK();
}

void UsageMonitor::UndoReserveMemory(const std::string &streamName)
{
    VLOG(SC_NORMAL_LOG_LEVEL) << "[UsageMonitor] Undo the memory reservation for: " << streamName;
    std::shared_lock<std::shared_timed_mutex> l(usageMutex_);
    TbbStreamReserveTable::Accessor accessor;
    if (streamMemoryMap_.Find(accessor, streamName)) {
        const auto &entry = accessor.entry->data;
        totalReservedSize_ -= entry->reserveSize;
        streamMemoryMap_.BlockingErase(accessor);
    }
}

uint64_t UsageMonitor::GetLocalMemoryUsed(const std::string &streamName)
{
    TbbStreamReserveTable::ConstAccessor accessor;
    uint64_t val = 0;
    if (streamMemoryMap_.Find(accessor, streamName)) {
        val = accessor.entry->data->usedSize;
    }
    return val;
}

std::string UsageMonitor::GetLocalMemoryUsed()
{
    return FormatString("%lu/%lu/%lu/%.3f", totalUsedSize_.load(), totalReservedSize_.load(), maxBufferPoolMem_,
                        totalUsedSize_ / static_cast<float>(maxBufferPoolMem_));
}

Status UsageMonitor::CheckNIncTotalReservedMemory(uint64_t reserveSize)
{
    bool success = false;
    do {
        uint64_t totalReservedSize = totalReservedSize_.load();
        uint64_t remainingForReserve = maxBufferPoolMem_ - totalReservedSize;
        // If not enough memory left for reservation, fail the CreateProducer/Subscribe
        CHECK_FAIL_RETURN_STATUS(
            remainingForReserve >= reserveSize, K_OUT_OF_MEMORY,
            FormatString("Reserve local cache memory failed, need %d, remaining %d", reserveSize, remainingForReserve));
        success = totalReservedSize_.compare_exchange_weak(totalReservedSize, totalReservedSize + reserveSize,
                                                           std::memory_order_release, std::memory_order_acquire);
    } while (!success);
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
