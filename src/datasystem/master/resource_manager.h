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
 * Description: The resource manager define.
 */
#ifndef DATASYSTEM_MASTER_RESOURCE_MANAGER_H
#define DATASYSTEM_MASTER_RESOURCE_MANAGER_H

#include <array>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/object_cache/node_info.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/protos/master_object.pb.h"

namespace datasystem {
namespace master {
class ResourceManager {
public:
    /**
     * @brief Construct the resource manager.
     */
    ResourceManager();

    /**
     * @brief Deconstruct the reousrce manager.
     */
    ~ResourceManager();

    /**
     * @brief Report the memory info to master.
     * @param[in] req The req info.
     * @param[out] rsp The response of the call.
     * @return Status of this call.
     */
    Status ReportResource(const master::ResourceReportReqPb &req, master::ResourceReportRspPb &rsp);
protected:
    /**
     * @brief Clear the expired resource in write snapshot.
     */
    void ClearWriteSnapshot();

    /**
     * @brief Switch the read/write snapshot.
     */
    void SwitchSnapshots();
private:
    /**
     * The worker thread.
     */
    void WorkerThread();

    /**
     * @brief Get current read snapshot.
     */
    const std::unordered_map<std::string, NodeInfo> &GetReadSnapshot() const
    {
        return readSnapshot_;
    };

    static constexpr int64_t WORKER_THREAD_INTERVAL_MS = 10 * 1000;
    Thread workerThread_;
    std::mutex taskMutex_;
    std::condition_variable taskCv_;
    std::atomic<bool> running_ = true;

    std::mutex writeSnapshotMutex_;
    std::shared_timed_mutex readSnapshotMutex_;
    std::unordered_map<std::string, NodeInfo> readSnapshot_{};
    std::unordered_map<std::string, NodeInfo> writeSnapshot_{};
};
} // namespace master
} // namespace datasystem
#endif
