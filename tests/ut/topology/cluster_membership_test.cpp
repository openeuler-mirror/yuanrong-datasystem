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
 * Description: Cluster membership tests.
 */
#include "datasystem/topology/membership/cluster_membership.h"

#include <functional>
#include <memory>

#include "gtest/gtest.h"

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/topology/membership/membership_value_codec.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"

namespace datasystem {
namespace topology {
namespace {

class FailingRegistry final : public IClusterRegistry {
public:
    Status ListMembers(MembershipSnapshot &snapshot) override
    {
        snapshot = {};
        return Status(K_RUNTIME_ERROR, "list failed");
    }

    Status HandleMembershipEvent(const CoordinationEvent &event, MembershipWatchEvent &typed) override
    {
        (void)event;
        (void)typed;
        return Status(K_RUNTIME_ERROR, "watch failed");
    }
};

class CallbackRegistry final : public IClusterRegistry {
public:
    std::function<void()> beforeReturn;

    Status ListMembers(MembershipSnapshot &snapshot) override
    {
        snapshot = {};
        MembershipRecord record;
        record.nodeId = "127.0.0.1:7001";
        record.lifecycleState = MemberLifecycleState::READY;
        snapshot.members[record.nodeId] = record;
        if (beforeReturn != nullptr) {
            beforeReturn();
        }
        return Status::OK();
    }

    Status HandleMembershipEvent(const CoordinationEvent &event, MembershipWatchEvent &typed) override
    {
        (void)event;
        (void)typed;
        return Status(K_RUNTIME_ERROR, "watch is unused");
    }
};

std::string MakeMembershipValue(MemberLifecycleState state)
{
    MembershipValue value;
    value.timestamp = 100;
    value.hostId = "host-a";
    value.lifecycleState = state;
    std::string encoded;
    auto rc = MembershipValueCodec::Encode(value, encoded);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    return encoded;
}

CoordinationEvent MakeEvent(CoordinationEventType type, const std::string &nodeId, const std::string &value,
                            int64_t revision)
{
    return { type, "/datasystem/cluster/" + nodeId, value, revision, revision };
}

TEST(ClusterMembershipTest, RebuildPublishesSnapshotAndValidatesScaleIn)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001",
                                  MakeMembershipValue(MemberLifecycleState::READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");

    DS_ASSERT_OK(membership.Rebuild());
    EXPECT_EQ(membership.RuntimeState(), MembershipRuntimeState::READY);

    std::shared_ptr<const MembershipSnapshot> snapshot;
    DS_ASSERT_OK(membership.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->members.size(), 1ul);

    ScaleInRequest request;
    Revision revision = 0;
    DS_ASSERT_OK(membership.BuildLocalScaleInRequest(ScaleInReason::ORDERLY_SHUTDOWN, request, revision));
    EXPECT_EQ(request.nodeId, "127.0.0.1:7001");
}

TEST(ClusterMembershipTest, AppliesExternalStorePutAndDeleteEvents)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001",
                                  MakeMembershipValue(MemberLifecycleState::READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());

    DS_ASSERT_OK(membership.HandleStoreEvent(
        MakeEvent(CoordinationEventType::PUT, "127.0.0.1:7002",
                  MakeMembershipValue(MemberLifecycleState::STARTING), 3)));
    std::shared_ptr<const MembershipSnapshot> snapshot;
    DS_ASSERT_OK(membership.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->members.size(), 2ul);
    EXPECT_EQ(snapshot->members.at("127.0.0.1:7002").lifecycleState, MemberLifecycleState::STARTING);

    DS_ASSERT_OK(membership.HandleStoreEvent(MakeEvent(CoordinationEventType::DELETE, "127.0.0.1:7002", "", 4)));
    DS_ASSERT_OK(membership.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->members.count("127.0.0.1:7002"), 0ul);
    EXPECT_EQ(snapshot->revision, 4);
}

TEST(ClusterMembershipTest, NotReadyBeforeRebuildAndStoppedAfterStop)
{
    FakeCoordinationBackend store;
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");

    std::shared_ptr<const MembershipSnapshot> snapshot;
    EXPECT_EQ(membership.GetSnapshot(snapshot).GetCode(), K_NOT_READY);
    EXPECT_EQ(membership
                  .HandleStoreEvent(MakeEvent(CoordinationEventType::PUT, "127.0.0.1:7001",
                                              MakeMembershipValue(MemberLifecycleState::READY), 1))
                  .GetCode(),
              K_NOT_READY);

    membership.Stop();
    EXPECT_EQ(membership.Rebuild().GetCode(), K_NOT_READY);
}

TEST(ClusterMembershipTest, RebuildFailurePublishesDegradedStateWithoutSnapshot)
{
    FailingRegistry registry;
    ClusterMembership membership(registry, "127.0.0.1:7001");

    EXPECT_EQ(membership.Rebuild().GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(membership.RuntimeState(), MembershipRuntimeState::DEGRADED);

    std::shared_ptr<const MembershipSnapshot> snapshot;
    EXPECT_EQ(membership.GetSnapshot(snapshot).GetCode(), K_NOT_READY);
}

TEST(ClusterMembershipTest, StopDuringRebuildKeepsStoppedState)
{
    CallbackRegistry registry;
    ClusterMembership membership(registry, "127.0.0.1:7001");
    registry.beforeReturn = [&membership]() { membership.Stop(); };

    DS_ASSERT_OK(membership.Rebuild());
    EXPECT_EQ(membership.RuntimeState(), MembershipRuntimeState::STOPPED);

    std::shared_ptr<const MembershipSnapshot> snapshot;
    EXPECT_EQ(membership.GetSnapshot(snapshot).GetCode(), K_NOT_READY);
}

TEST(ClusterMembershipTest, DestructorStopsMembership)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001",
                                  MakeMembershipValue(MemberLifecycleState::READY)));
    ClusterRegistry registry(store);
    auto membership = std::make_unique<ClusterMembership>(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership->Rebuild());

    membership.reset();
}

TEST(ClusterMembershipTest, ScaleInRequiresReadyLocalWorker)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001",
                                  MakeMembershipValue(MemberLifecycleState::STARTING)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());

    ScaleInRequest request;
    Revision revision = 0;
    EXPECT_EQ(membership.BuildLocalScaleInRequest(ScaleInReason::MANUAL_DRAIN, request, revision).GetCode(),
              K_NOT_READY);
    EXPECT_EQ(membership.BuildLocalScaleInRequest(static_cast<ScaleInReason>(99), request, revision).GetCode(),
              K_INVALID);
}

TEST(ClusterMembershipTest, ScaleInRequiresLocalMemberInSnapshot)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002",
                                  MakeMembershipValue(MemberLifecycleState::READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());

    ScaleInRequest request;
    Revision revision = 0;
    EXPECT_EQ(membership.BuildLocalScaleInRequest(ScaleInReason::ORDERLY_SHUTDOWN, request, revision).GetCode(),
              K_NOT_FOUND);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
