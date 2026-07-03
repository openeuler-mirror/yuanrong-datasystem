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
 * Description: R0 membership endpoint view snapshot.
 */
#include "datasystem/topology/routing/membership_endpoint_view.h"

#include <mutex>
#include <utility>

namespace datasystem {
namespace topology {

Status MembershipEndpointView::ResolveEndpoint(const std::string &nodeId, MemberEndpoint &endpoint) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(snapshot_ != nullptr, K_NOT_READY, "Membership endpoint view snapshot is not published.");
    auto iter = snapshot_->members.find(nodeId);
    CHECK_FAIL_RETURN_STATUS(iter != snapshot_->members.end(), K_NOT_FOUND, "Member endpoint is not found.");
    endpoint = iter->second;
    return Status::OK();
}

Status MembershipEndpointView::ResolveEndpointByAddress(const std::string &nodeAddress, MemberEndpoint &endpoint) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(snapshot_ != nullptr, K_NOT_READY, "Membership endpoint view snapshot is not published.");
    auto idIter = snapshot_->nodeIdsByAddress.find(nodeAddress);
    CHECK_FAIL_RETURN_STATUS(idIter != snapshot_->nodeIdsByAddress.end(), K_NOT_FOUND,
                             "Node id is not found by address.");
    auto endpointIter = snapshot_->members.find(idIter->second);
    CHECK_FAIL_RETURN_STATUS(endpointIter != snapshot_->members.end(), K_NOT_FOUND, "Member endpoint is not found.");
    endpoint = endpointIter->second;
    return Status::OK();
}

Status MembershipEndpointView::GetLocalEndpoint(MemberEndpoint &endpoint) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(snapshot_ != nullptr, K_NOT_READY, "Membership endpoint view snapshot is not published.");
    auto iter = snapshot_->members.find(snapshot_->localNodeId);
    if (iter != snapshot_->members.end()) {
        endpoint = iter->second;
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!snapshot_->localNodeId.empty() || !snapshot_->localAddress.Empty(), K_NOT_FOUND,
                             "Local member is not found.");
    endpoint.nodeId = snapshot_->localNodeId;
    endpoint.address = snapshot_->localAddress;
    endpoint.availability = MemberAvailability::NOT_READY;
    return Status::OK();
}

void MembershipEndpointView::Publish(std::shared_ptr<const MembershipEndpointSnapshot> snapshot)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    snapshot_ = std::move(snapshot);
}

}  // namespace topology
}  // namespace datasystem
