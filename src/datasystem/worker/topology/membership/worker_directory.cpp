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
 * Description: R0 worker directory snapshot.
 */
#include "datasystem/worker/topology/membership/worker_directory.h"

#include <mutex>
#include <utility>

namespace datasystem {
namespace topology {

Status WorkerDirectory::ResolveWorker(const std::string &workerId, WorkerEndpoint &endpoint) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(snapshot_ != nullptr, K_NOT_READY, "Worker directory snapshot is not published.");
    auto iter = snapshot_->workers.find(workerId);
    CHECK_FAIL_RETURN_STATUS(iter != snapshot_->workers.end(), K_NOT_FOUND, "Worker endpoint is not found.");
    endpoint = iter->second;
    return Status::OK();
}

Status WorkerDirectory::GetLocalWorker(WorkerEndpoint &endpoint) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(snapshot_ != nullptr, K_NOT_READY, "Worker directory snapshot is not published.");
    auto iter = snapshot_->workers.find(snapshot_->localWorkerId);
    if (iter != snapshot_->workers.end()) {
        endpoint = iter->second;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!snapshot_->localWorkerId.empty() || !snapshot_->localAddress.Empty(), K_NOT_FOUND,
                             "Local worker is not found.");
    endpoint.workerId = snapshot_->localWorkerId;
    endpoint.address = snapshot_->localAddress;
    endpoint.availability = WorkerAvailability::NOT_READY;
    return Status::OK();
}

void WorkerDirectory::Publish(std::shared_ptr<const WorkerDirectorySnapshot> snapshot)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    snapshot_ = std::move(snapshot);
}

}  // namespace topology
}  // namespace datasystem
