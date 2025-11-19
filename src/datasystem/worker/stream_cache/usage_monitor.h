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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_USAGE_MANAGER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_USAGE_MANAGER_H

#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/util/lock_map.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class ClientWorkerSCServiceImpl;
static constexpr double DEFAULT_THRESHOLD = 1.0;
/**
 * UsageManager creates a thread that continuously monitor
 * The size of BufferPool if it goes above a threshold
 * Sends blocking RPC call if this happens
 */
struct UsageItem {
    std::string streamName;
    std::string remoteWorkerAddr;
    std::uint64_t usage;
    bool usageBlocked;
    UsageItem();
    UsageItem(std::string streamName, std::string remoteWorkerAddr, std::uint64_t usage);
};

struct MemReserveEntry {
    // The reserved memory size.
    uint64_t reserveSize;
    // The memory used by the stream.
    uint64_t usedSize;
    MemReserveEntry(uint64_t reserve);
};

using TbbUsageTable = tbb::concurrent_hash_map<std::string, std::shared_ptr<UsageItem>>;
using TbbStreamReserveTable = LockMap<std::string, std::shared_ptr<MemReserveEntry>>;
class UsageMonitor {
public:
    /**
     * @brief Construct the UsageHeap.
     * @param[in] maxMemThresholdBytes BufferPool memory Limit set by user
     */
    UsageMonitor(ClientWorkerSCServiceImpl *clientWorkerScService, const uint64_t maxMemThresholdBytes);

    ~UsageMonitor() = default;

    /**
     * @brief Init function
     * @return
     */
    Status Init();

    /**
     * @brief Shutdown usage monitor
     */
    void Stop();

    /**
     * @brief Adds usage of a stream and remote worker to BufferPool
     * @param[in] streamName stream name
     * @param[in] workerAddr remote worker address
     * @param[in] size size of new PageView added
     * @return Status of the call.
     */
    Status IncUsage(const std::string &streamName, const std::string &workerAddr, const std::uint64_t size);

    /**
     * @brief Decreases usage of a stream and remote worker from BufferPool
     * @param[in] streamName stream name
     * @param[in] workerAddr remote worker address
     * @param[in] size size of new PageView added
     * @return Status of the call.
     */
    Status DecUsage(const std::string &streamName, const std::string &workerAddr, const std::uint64_t size);

    /**
     * @brief Removes a Stream and Remote Worker from usage stats
     * @param[in] streamName stream name
     * @param[in] workerAddr remote worker address
     * @return Status of the call.
     */
    Status RemoveUsageStats(const std::string &streamName, const std::string &workerAddr);

    /**
     * @brief Does Current BufferPool Usage Exceeds the User defined limit?
     * @param[in] threshold e.g. if set to 0.8 will check 80% of max
     * @param[in] size The size for the check
     * @return Status of the call.
     */
    Status CheckOverUsed(const double threshold = DEFAULT_THRESHOLD, const uint64_t size = 0);

    /**
     * @brief Check if the stream Exceeds the User defined ratio? And also increase the memory usage accordingly.
     * @param[in] streamName stream name
     * @param[in] workerAddr remote worker address
     * @param[in] lowerBound stream lower bound limit
     * @param[in] threshold % of total memory allowed for the stream (ratio)
     * @param[in] size The size for the check
     * @return Status of the call.
     */
    Status CheckNIncOverUsedForStream(const std::string &streamName, const std::string &workerAddr,
                                      const uint64_t lowerBound, const double threshold, const uint64_t size);

    /**
     * @brief Gets the StreamName and Remote Worker combination that uses most space
     * @param[out] usageItem streamName and Remote Worker
     * @return Status of the call.
     */
    Status GetMostUsed(std::shared_ptr<UsageItem> &usageItem);

    /**
     * @brief Reserve local cache memory for stream
     * @param[in] streamName stream name
     * @param[in] reserveSize The size to reserve
     * @return Status of the call.
     */
    Status ReserveMemory(const std::string &streamName, size_t reserveSize);

    /**
     * @brief Undo the reserved local cache memory for stream
     * @param[in] streamName stream name
     */
    void UndoReserveMemory(const std::string &streamName);

    /**
     * @brief Gets amount of local memory used for a stream
     * @param[in] streamName stream name
     * @return The amount of memory
     */
    uint64_t GetLocalMemoryUsed(const std::string &streamName);

    /**
     * @brief Get the usage of scLocalCache. The format is totalUsedSize/totalReservedSize/totalLimit/usage
     * @return The amount of memory
     */
    std::string GetLocalMemoryUsed();

private:
    /**
     * @brief If Total memory exceeds user set limit
     *        Block Most offending producers
     */
    void BlockProducersIfNeeded();

    /**
     * @brief Blocks usage for the stream and remote worker
     * @param[in] usageItem gives stream name and remote worker
     * @return Status of the call.
     */
    Status BlockUsage(std::shared_ptr<UsageItem> &usageItem);

    /**
     * @brief UnBlocks usage for the stream and remote worker
     * @param[in] usageItem gives stream name and remote worker
     * @return Status of the call.
     */
    Status UnBlockUsage(std::shared_ptr<UsageItem> &usageItem);

    /**
     * @brief waits for timeoutMs, checks for the memory usage
     * @param[in] timeoutMs gives stream name and remote worker
     * @param[in] threshold e.g. if set to 0.8 will check 80% of max
     * @return Status of the call.
     */
    bool WaitForOverUseCondition(const uint64_t timeoutMs, const double threshold);

    /**
     * @brief Find producer that used local cache most and send a blocking call
     * @return Status of the call.
     */
    Status BlockMostUsed();

    /**
     * @brief Unblock all previously blocked producer if memory availability exceeds threshold
     * @param[in] unBlockThreshold e.g. if set to 0.8, usage should be less than 80% of max
     * @return Status of the call.
     */
    Status UnBlockAllProducers(const double unBlockThreshold);

    /**
     * @brief Helper function to decrease the total memory usage.
     * @return Status of the call.
     */
    void DecUsage(const std::uint64_t size);

    /**
     * @brief Helper function to increase the total usage of a stream and usage regarding remote worker to BufferPool
     * @param[in] streamName stream name
     * @param[in] workerAddr remote worker address
     * @param[in] size size of new PageView added
     * @return Status of the call.
     */
    Status IncTotalUsageUnlocked(const std::string &streamName, const std::string &workerAddr,
                                 const std::uint64_t size);

    /**
     * @brief Helper function to increment total reserved memory while make sure the limit is not exceeded.
     * @param[in] reserveSize The size to increment with.
     * @return Status of the call.
     */
    Status CheckNIncTotalReservedMemory(uint64_t reserveSize);

    ClientWorkerSCServiceImpl *clientWorkerScService_;
    // Backend thread to check memory usage and invoke blocking callbacks
    std::unique_ptr<ThreadPool> producerBlockerThreadPool_;
    // protect for tbbUsageTable usage_;
    mutable std::shared_timed_mutex usageMutex_;
    // Stores usage per stream per remote worker
    TbbUsageTable usage_;
    // Stores total size of all elements in the BufferPool
    std::atomic_uint64_t totalUsedSize_;
    // Max memory that can be used by Buffer pool
    const uint64_t maxBufferPoolMem_;
    // The memory reservation helpers, includes the usage per stream
    TbbStreamReserveTable streamMemoryMap_;
    std::atomic_uint64_t totalReservedSize_{ 0 };
    // Used to Interrupt Background Thread
    std::atomic<bool> interrupt_;
    mutable std::mutex mux_;  // protect for cv_;
    std::condition_variable cv_;
    std::vector<std::shared_ptr<UsageItem>> blockedStreamProducers_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif