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
 * Description: Local membership snapshot manager for common topology.
 */
#include "datasystem/topology/membership/cluster_membership.h"

#include <memory>
#include <mutex>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

ClusterMembership::ClusterMembership(IClusterRegistry &registry, TopologyNodeId localNodeId)
    : registry_(registry), localNodeId_(std::move(localNodeId))
{
}

ClusterMembership::~ClusterMembership()
{
    Stop();
}

Status ClusterMembership::Rebuild()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == MembershipRuntimeState::STOPPED) {
            RETURN_STATUS(K_NOT_READY, "membership is stopped");
        }
        state_ = MembershipRuntimeState::REBUILDING;
    }

    MembershipSnapshot listed;
    auto rc = registry_.ListMembers(listed);
    if (!rc.IsOk()) {
        PublishSnapshot(nullptr, MembershipRuntimeState::DEGRADED);
        return rc;
    }
    PublishSnapshot(std::make_shared<MembershipSnapshot>(std::move(listed)), MembershipRuntimeState::READY);
    return Status::OK();
}

void ClusterMembership::Stop()
{
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_.reset();
    state_ = MembershipRuntimeState::STOPPED;
}

Status ClusterMembership::HandleStoreEvent(const CoordinationEvent &event)
{
    MembershipWatchEvent typed;
    RETURN_IF_NOT_OK(registry_.HandleMembershipEvent(event, typed));
    return ApplyMembershipEvent(typed);
}

Status ClusterMembership::GetSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot = snapshot_;
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr && state_ == MembershipRuntimeState::READY, K_NOT_READY,
                             "membership snapshot is not ready");
    return Status::OK();
}

Status ClusterMembership::GetLastSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot = snapshot_;
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr && state_ != MembershipRuntimeState::STOPPED, K_NOT_READY,
                             "membership last-good snapshot is not ready");
    return Status::OK();
}

Status ClusterMembership::GetRecord(const TopologyNodeId &nodeId, MembershipRecord &record) const
{
    record = {};
    std::shared_ptr<const MembershipSnapshot> snapshot;
    RETURN_IF_NOT_OK(GetSnapshot(snapshot));
    auto iter = snapshot->members.find(nodeId);
    CHECK_FAIL_RETURN_STATUS(iter != snapshot->members.end(), K_NOT_FOUND, "member is absent from membership snapshot");
    record = iter->second;
    return Status::OK();
}

Status ClusterMembership::GetReadyEndpoint(const TopologyNodeId &nodeId, TopologyEndpoint &endpoint) const
{
    endpoint = {};
    MembershipRecord record;
    RETURN_IF_NOT_OK(GetRecord(nodeId, record));
    CHECK_FAIL_RETURN_STATUS(record.lifecycleState == MemberLifecycleState::READY, K_NOT_FOUND, "member is not ready");
    endpoint = record.endpoint;
    return Status::OK();
}

Status ClusterMembership::ListReadyMembers(std::vector<MembershipRecord> &members) const
{
    members.clear();
    std::shared_ptr<const MembershipSnapshot> snapshot;
    RETURN_IF_NOT_OK(GetSnapshot(snapshot));
    for (const auto &entry : snapshot->members) {
        if (entry.second.lifecycleState == MemberLifecycleState::READY) {
            members.push_back(entry.second);
        }
    }
    return Status::OK();
}

Status ClusterMembership::BuildLocalScaleInRequest(ScaleInReason reason, ScaleInRequest &request,
                                                   Revision &observedRevision) const
{
    request = {};
    observedRevision = 0;
    RETURN_IF_NOT_OK(ValidateScaleInReason(reason));
    std::shared_ptr<const MembershipSnapshot> snapshot;
    RETURN_IF_NOT_OK(GetSnapshot(snapshot));
    auto iter = snapshot->members.find(localNodeId_);
    CHECK_FAIL_RETURN_STATUS(iter != snapshot->members.end(), K_NOT_FOUND,
                             "local member is not in membership snapshot");
    CHECK_FAIL_RETURN_STATUS(iter->second.lifecycleState == MemberLifecycleState::READY, K_NOT_READY,
                             "local member is not ready for scale-in");
    request.nodeId = localNodeId_;
    request.reason = reason;
    observedRevision = snapshot->revision;
    return Status::OK();
}

MembershipRuntimeState ClusterMembership::RuntimeState() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

Status ClusterMembership::ApplyMembershipEvent(const MembershipWatchEvent &event)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(snapshot_ != nullptr && state_ == MembershipRuntimeState::READY, K_NOT_READY,
                             "membership snapshot is not ready");
    auto next = std::make_shared<MembershipSnapshot>(*snapshot_);
    next->revision = event.revision;
    if (event.type == MembershipWatchEventType::DELETED) {
        next->members.erase(event.nodeId);
    } else {
        next->members[event.record.nodeId] = event.record;
    }
    snapshot_ = std::move(next);
    return Status::OK();
}

void ClusterMembership::PublishSnapshot(std::shared_ptr<const MembershipSnapshot> snapshot,
                                        MembershipRuntimeState state)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == MembershipRuntimeState::STOPPED) {
        return;
    }
    if (snapshot != nullptr) {
        snapshot_ = std::move(snapshot);
    }
    state_ = state;
}

Status ClusterMembership::ValidateScaleInReason(ScaleInReason reason)
{
    CHECK_FAIL_RETURN_STATUS(reason == ScaleInReason::ORDERLY_SHUTDOWN || reason == ScaleInReason::MANUAL_DRAIN,
                             K_INVALID, "invalid scale-in reason");
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
