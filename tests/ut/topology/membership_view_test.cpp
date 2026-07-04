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
 * Description: Cluster membership view tests.
 */
#include "datasystem/topology/membership/cluster_membership.h"

#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

class MembershipRegistry final : public IClusterRegistry {
public:
    Status ListMembers(MembershipSnapshot &snapshot) override
    {
        ++listCount;
        snapshot = snapshot_;
        return Status::OK();
    }

    Status HandleMembershipEvent(const CoordinationEvent &event, MembershipWatchEvent &typed) override
    {
        (void)event;
        (void)typed;
        return Status(K_RUNTIME_ERROR, "watch is unused");
    }

    void SetSnapshot(MembershipSnapshot snapshot)
    {
        snapshot_ = std::move(snapshot);
    }

    uint64_t listCount{ 0 };

private:
    MembershipSnapshot snapshot_;
};

MembershipRecord MakeRecord(const std::string &nodeId, MemberLifecycleState state)
{
    MembershipRecord record;
    record.nodeId = nodeId;
    record.endpoint = { "127.0.0.1", nodeId == "127.0.0.1:7001" ? 7001 : 7002 };
    record.lifecycleState = state;
    record.capability.hostId = "host-a";
    record.capability.compatibilityVersion = "v1";
    record.modRevision = 1;
    return record;
}

TEST(MembershipViewTest, SnapshotUnavailableReturnsNotReady)
{
    MembershipRegistry registry;
    ClusterMembership membership(registry, "127.0.0.1:7001");
    ClusterMembership &endpointView = membership;

    std::shared_ptr<const MembershipSnapshot> snapshot;
    EXPECT_EQ(endpointView.GetSnapshot(snapshot).GetCode(), K_NOT_READY);
    MembershipRecord record;
    EXPECT_EQ(endpointView.GetRecord("127.0.0.1:7001", record).GetCode(), K_NOT_READY);
    TopologyEndpoint endpoint;
    EXPECT_EQ(endpointView.GetReadyEndpoint("127.0.0.1:7001", endpoint).GetCode(), K_NOT_READY);
    std::vector<MembershipRecord> members;
    EXPECT_EQ(endpointView.ListReadyMembers(members).GetCode(), K_NOT_READY);
    EXPECT_EQ(registry.listCount, 0ul);
}

TEST(MembershipViewTest, ReadsOnlyImmutableSnapshot)
{
    MembershipSnapshot snapshot;
    snapshot.revision = 10;
    snapshot.members.emplace("127.0.0.1:7001", MakeRecord("127.0.0.1:7001", MemberLifecycleState::READY));
    snapshot.members.emplace("127.0.0.1:7002", MakeRecord("127.0.0.1:7002", MemberLifecycleState::STARTING));

    MembershipRegistry registry;
    registry.SetSnapshot(std::move(snapshot));
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());
    ClusterMembership &endpointView = membership;

    MembershipRecord record;
    DS_ASSERT_OK(endpointView.GetRecord("127.0.0.1:7001", record));
    EXPECT_EQ(record.endpoint.ToString(), "127.0.0.1:7001");
    EXPECT_EQ(record.lifecycleState, MemberLifecycleState::READY);
    EXPECT_EQ(endpointView.GetRecord("127.0.0.1:7003", record).GetCode(), K_NOT_FOUND);

    TopologyEndpoint endpoint;
    DS_ASSERT_OK(endpointView.GetReadyEndpoint("127.0.0.1:7001", endpoint));
    EXPECT_EQ(endpoint.ToString(), "127.0.0.1:7001");
    EXPECT_EQ(endpointView.GetReadyEndpoint("127.0.0.1:7002", endpoint).GetCode(), K_NOT_FOUND);

    std::vector<MembershipRecord> members;
    DS_ASSERT_OK(endpointView.ListReadyMembers(members));
    ASSERT_EQ(members.size(), 1ul);
    EXPECT_EQ(members[0].nodeId, "127.0.0.1:7001");
    EXPECT_EQ(registry.listCount, 1ul);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
