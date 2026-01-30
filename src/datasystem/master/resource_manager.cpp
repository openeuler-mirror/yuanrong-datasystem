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

#include "datasystem/common/util/timer.h"

DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
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

Status ResourceManager::ReportResource(const master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp)
{
    auto currentTimestamp = GetSteadyClockTimeStampMs();
    std::string address = req.stat().address();
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_INVALID, "The address can not be empty");
    NodeInfo newInfo(address, req.stat().available_memory(), req.stat().is_ready(), currentTimestamp);
    {
        // Update the write snapshot
        std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
        auto result = writeSnapshot_.emplace(address, newInfo);
        if (!result.second) {
            result.first->second = newInfo;
        }
    }
    // Get the read snapshot and fill response
    std::shared_lock<std::shared_timed_mutex> lock(readSnapshotMutex_);
    auto *stats = rsp.mutable_stats();
    for (const auto &info : readSnapshot_) {
        auto &nodeInfo = info.second;
        auto *stat = stats->Add();
        stat->set_address(nodeInfo.nodeId);
        stat->set_available_memory(nodeInfo.availableMemory);
        stat->set_is_ready(nodeInfo.isReady);
    }
    return Status::OK();
}

void ResourceManager::WorkerThread()
{
    int switchNumber = 0;
    int switchClearRatio = 3;
    while (running_) {
        // Clear once every switchClearRatio switches
        if (++switchNumber % switchClearRatio == 0) {
            ClearWriteSnapshot();
        }
        SwitchSnapshots();
        std::unique_lock<std::mutex> lock(taskMutex_);
        if (!running_.load()) {
            break;
        }
        (void)taskCv_.wait_for(lock, std::chrono::milliseconds(WORKER_THREAD_INTERVAL_MS),
                               [this]() { return !running_.load(); });
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
    std::lock_guard<std::mutex> lock(writeSnapshotMutex_);
    std::unique_lock<std::shared_timed_mutex> lockRead(readSnapshotMutex_);
    std::swap(readSnapshot_, writeSnapshot_);
}
} // namespace master
} // namespace datasystem
