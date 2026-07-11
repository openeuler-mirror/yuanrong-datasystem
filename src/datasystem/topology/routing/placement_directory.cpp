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
 * Description: R0 placement directory snapshot (atomic snapshot version).
 */
#include "datasystem/topology/routing/placement_directory.h"

#include <memory>
#include <utility>

namespace datasystem {
namespace topology {

Status PlacementDirectory::ResolveWorker(const std::string &workerId, PlacementEndpoint &endpoint) const
{
    auto snap = std::atomic_load(&snapshot_);
    CHECK_FAIL_RETURN_STATUS(snap != nullptr, K_NOT_READY, "Placement directory snapshot is not published.");
    auto iter = snap->workers.find(workerId);
    CHECK_FAIL_RETURN_STATUS(iter != snap->workers.end(), K_NOT_FOUND, "Worker endpoint is not found.");
    endpoint = iter->second;
    return Status::OK();
}

Status PlacementDirectory::ResolveWorkerByAddress(const std::string &workerAddress, PlacementEndpoint &endpoint) const
{
    auto snap = std::atomic_load(&snapshot_);
    CHECK_FAIL_RETURN_STATUS(snap != nullptr, K_NOT_READY, "Placement directory snapshot is not published.");
    auto idIter = snap->workerIdsByAddress.find(workerAddress);
    CHECK_FAIL_RETURN_STATUS(idIter != snap->workerIdsByAddress.end(), K_NOT_FOUND,
                             "Worker id is not found by address.");
    auto endpointIter = snap->workers.find(idIter->second);
    CHECK_FAIL_RETURN_STATUS(endpointIter != snap->workers.end(), K_NOT_FOUND, "Worker endpoint is not found.");
    endpoint = endpointIter->second;
    return Status::OK();
}

Status PlacementDirectory::GetLocalWorker(PlacementEndpoint &endpoint) const
{
    auto snap = std::atomic_load(&snapshot_);
    CHECK_FAIL_RETURN_STATUS(snap != nullptr, K_NOT_READY, "Placement directory snapshot is not published.");
    auto iter = snap->workers.find(snap->localWorkerId);
    if (iter != snap->workers.end()) {
        endpoint = iter->second;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!snap->localWorkerId.empty() || !snap->localAddress.Empty(), K_NOT_FOUND,
                             "Local worker is not found.");
    endpoint.workerId = snap->localWorkerId;
    endpoint.address = snap->localAddress;
    endpoint.availability = WorkerAvailability::NOT_READY;
    return Status::OK();
}

Status PlacementDirectory::ResolveEndpoint(const std::string &nodeId, MemberEndpoint &endpoint) const
{
    PlacementEndpoint workerEndpoint;
    RETURN_IF_NOT_OK(ResolveWorker(nodeId, workerEndpoint));
    endpoint = workerEndpoint;
    return Status::OK();
}

Status PlacementDirectory::ResolveEndpointByAddress(const std::string &nodeAddress, MemberEndpoint &endpoint) const
{
    PlacementEndpoint workerEndpoint;
    RETURN_IF_NOT_OK(ResolveWorkerByAddress(nodeAddress, workerEndpoint));
    endpoint = workerEndpoint;
    return Status::OK();
}

Status PlacementDirectory::GetLocalEndpoint(MemberEndpoint &endpoint) const
{
    PlacementEndpoint workerEndpoint;
    RETURN_IF_NOT_OK(GetLocalWorker(workerEndpoint));
    endpoint = workerEndpoint;
    return Status::OK();
}

void PlacementDirectory::Publish(std::shared_ptr<const PlacementDirectorySnapshot> snapshot)
{
    std::atomic_store(&snapshot_, std::move(snapshot));
}

}  // namespace topology
}  // namespace datasystem
