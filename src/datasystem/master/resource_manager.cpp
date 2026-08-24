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

/**
 * Description: The resource manager implement.
 */

#include "datasystem/master/resource_manager.h"

#include <chrono>
#include <mutex>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/timer.h"

DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
namespace {
void FillWorkerStat(const NodeInfo &nodeInfo, master::WorkerStat &stat)
{
    stat.set_address(nodeInfo.nodeId);
    stat.set_available_memory(nodeInfo.availableMemory);
    stat.set_is_ready(nodeInfo.isReady);
    stat.set_used_memory(nodeInfo.usedMemory);
    stat.set_memory_capacity(nodeInfo.memoryCapacity);
    stat.set_memory_limit(nodeInfo.memoryLimit);
}
}  // namespace

ResourceManager::ResourceManager()
{
    workerThread_ = Thread(&ResourceManager::WorkerThread, this);
    workerThread_.set_name("ResourceManager");
    LOG(INFO) << "ResourceManager initialized with double-buffered snapshot";
}

ResourceManager::~ResourceManager()
{
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        running_.store(false);
    }
    taskCv_.notify_all();
    if (workerThread_.joinable()) {
        workerThread_.join();
    }
}

void ResourceManager::SetTopologyMembership(const cluster::MembershipEndpointView *topologyMembership)
{
    rebalanceScheduler_.SetTopologyMembership(topologyMembership);
}

Status ResourceManager::ReportResource(const master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp)
{
    auto currentTimestamp = GetSteadyClockTimeStampMs();
    std::string address = req.stat().address();
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_INVALID, "The address can not be empty");
    NodeInfo newInfo(address, req.stat().available_memory(), req.stat().is_ready(), currentTimestamp,
                     req.stat().used_memory(), req.stat().memory_capacity(), req.stat().memory_limit());
    {
        // Update the write snapshot
        std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
        auto result = writeSnapshot_.emplace(address, newInfo);
        if (!result.second) {
            result.first->second = newInfo;
        }
    }

    bool needScheduleSnapshot = rebalanceScheduler_.NeedSnapshotForSchedule(req, newInfo, rsp);
    if (needScheduleSnapshot) {
        std::unordered_map<std::string, NodeInfo> snapshot;
        BuildLatestSnapshot(snapshot);
        auto reportingWorker = snapshot.find(address);
        if (reportingWorker == snapshot.end() || reportingWorker->second.timestamp < newInfo.timestamp) {
            snapshot.insert_or_assign(address, newInfo);
        }
        auto *stats = rsp.mutable_stats();
        for (const auto &[worker, nodeInfo] : snapshot) {
            (void)worker;
            FillWorkerStat(nodeInfo, *stats->Add());
        }
        return rebalanceScheduler_.Schedule(req, snapshot, rsp);
    }
    auto *stats = rsp.mutable_stats();
    {
        std::shared_lock<SharedMutex> lock(readSnapshotMutex_);
        bool hasReportingWorker = false;
        for (const auto &[worker, nodeInfoInSnapshot] : readSnapshot_) {
            const NodeInfo &nodeInfo = worker == address ? newInfo : nodeInfoInSnapshot;
            hasReportingWorker = hasReportingWorker || worker == address;
            auto *stat = stats->Add();
            FillWorkerStat(nodeInfo, *stat);
        }
        if (!hasReportingWorker) {
            auto *stat = stats->Add();
            FillWorkerStat(newInfo, *stat);
        }
    }
    return Status::OK();
}

Status ResourceManager::ReportRebalanceResult(const master::ReportRebalanceResultReqPb &req,
                                              master::ReportRebalanceResultRspPb &rsp)
{
    return rebalanceScheduler_.ReportResult(req, rsp);
}

void ResourceManager::WorkerThread()
{
    int switchNumber = 0;
    int switchClearRatio = 3;
    int64_t intervalMs = WORKER_THREAD_INTERVAL_MS;
    INJECT_POINT_NO_RETURN("ResourceManager.setInterval", [&intervalMs](int64_t interval) { intervalMs = interval; });
    while (running_) {
        // Testability hook: skip background mutation so a UT can place reports in each buffer deterministically.
        bool skipBackgroundSwap = false;
        INJECT_POINT_NO_RETURN("ResourceManager.skipBackgroundSwap",
                               [&skipBackgroundSwap]() { skipBackgroundSwap = true; });
        if (!skipBackgroundSwap) {
            // Clear once every switchClearRatio switches
            if (++switchNumber % switchClearRatio == 0) {
                ClearWriteSnapshot();
            }
            SwitchSnapshots();
        }
        std::unique_lock<std::mutex> lock(taskMutex_);
        if (!running_.load()) {
            break;
        }
        (void)taskCv_.wait_for(lock, std::chrono::milliseconds(intervalMs), [this]() { return !running_.load(); });
    }
}

void ResourceManager::ClearWriteSnapshot()
{
    auto deadTimestamp = GetSteadyClockTimeStampMs() - static_cast<uint64_t>(FLAGS_node_dead_timeout_s) * SECS_TO_MS;
    std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
    for (auto it = writeSnapshot_.begin(); it != writeSnapshot_.end();) {
        if (it->second.timestamp < deadTimestamp) {
            it = writeSnapshot_.erase(it);
        } else {
            ++it;
        }
    }
}

void ResourceManager::SwitchSnapshots()
{
    std::unique_lock<SharedMutex> swapLock(snapshotSwapMutex_);
    std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
    std::unique_lock<SharedMutex> lockRead(readSnapshotMutex_);
    std::swap(readSnapshot_, writeSnapshot_);
}

void ResourceManager::BuildLatestSnapshot(std::unordered_map<std::string, NodeInfo> &snapshot)
{
    std::shared_lock<SharedMutex> swapLock(snapshotSwapMutex_);
    size_t readSnapshotSize;
    {
        std::shared_lock<SharedMutex> readLock(readSnapshotMutex_);
        readSnapshotSize = readSnapshot_.size();
    }
    size_t writeSnapshotSize;
    {
        std::lock_guard<std::mutex> writeLock(writeSnapshotMutex_);
        writeSnapshotSize = writeSnapshot_.size();
    }
    snapshot.reserve(readSnapshotSize + writeSnapshotSize);
    auto merge = [&snapshot](const std::unordered_map<std::string, NodeInfo> &source) {
        for (const auto &[worker, nodeInfo] : source) {
            auto iter = snapshot.find(worker);
            if (iter == snapshot.end() || iter->second.timestamp <= nodeInfo.timestamp) {
                snapshot.insert_or_assign(worker, nodeInfo);
            }
        }
    };
    {
        std::shared_lock<SharedMutex> readLock(readSnapshotMutex_);
        INJECT_POINT_NO_RETURN("ResourceManager.BuildLatestSnapshot.beforeReadMerge");
        merge(readSnapshot_);
    }
    {
        std::lock_guard<std::mutex> writeLock(writeSnapshotMutex_);
        merge(writeSnapshot_);
    }
}
} // namespace master
} // namespace datasystem
