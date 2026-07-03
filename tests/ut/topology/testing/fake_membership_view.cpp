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
 * Description: Membership view fake for module tests.
 */
#include "tests/ut/topology/testing/fake_membership_view.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

Status FakeMembershipView::SeedSnapshot(const MembershipSnapshot &snapshot)
{
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_ = std::make_shared<MembershipSnapshot>(snapshot);
    return Status::OK();
}

Status FakeMembershipView::GetSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot = snapshot_;
    CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_NOT_READY, "fake membership snapshot is not ready");
    return Status::OK();
}

Status FakeMembershipView::GetRecord(const TopologyNodeId &nodeId, MembershipRecord &record) const
{
    record = {};
    std::shared_ptr<const MembershipSnapshot> snapshot;
    RETURN_IF_NOT_OK(GetSnapshot(snapshot));
    auto iter = snapshot->members.find(nodeId);
    CHECK_FAIL_RETURN_STATUS(iter != snapshot->members.end(), K_NOT_FOUND, "fake member is absent");
    record = iter->second;
    return Status::OK();
}

Status FakeMembershipView::GetReadyEndpoint(const TopologyNodeId &nodeId, TopologyEndpoint &endpoint) const
{
    endpoint = {};
    MembershipRecord record;
    RETURN_IF_NOT_OK(GetRecord(nodeId, record));
    CHECK_FAIL_RETURN_STATUS(record.lifecycleState == MemberLifecycleState::READY, K_NOT_FOUND,
                             "fake member is not ready");
    endpoint = record.endpoint;
    return Status::OK();
}

Status FakeMembershipView::ListReadyMembers(std::vector<MembershipRecord> &members) const
{
    members.clear();
    std::shared_ptr<const MembershipSnapshot> snapshot;
    RETURN_IF_NOT_OK(GetSnapshot(snapshot));
    members.reserve(snapshot->members.size());
    for (const auto &entry : snapshot->members) {
        if (entry.second.lifecycleState == MemberLifecycleState::READY) {
            members.push_back(entry.second);
        }
    }
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
