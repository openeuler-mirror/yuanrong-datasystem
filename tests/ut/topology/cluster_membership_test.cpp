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
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"

namespace datasystem {
namespace topology {
namespace {

class FailingRegistry final : public IClusterRegistry {
public:
    Status ListWorkers(MembershipSnapshot &snapshot) override
    {
        snapshot = {};
        return Status(K_RUNTIME_ERROR, "list failed");
    }

    Status HandleWorkerEvent(const CoordinationEvent &event, WorkerWatchEvent &typed) override
    {
        (void)event;
        (void)typed;
        return Status(K_RUNTIME_ERROR, "watch failed");
    }
};

class CallbackRegistry final : public IClusterRegistry {
public:
    std::function<void()> beforeReturn;

    Status ListWorkers(MembershipSnapshot &snapshot) override
    {
        snapshot = {};
        WorkerRecord record;
        record.workerId = "127.0.0.1:7001";
        record.serviceState = WorkerServiceState::READY;
        snapshot.workers[record.workerId] = record;
        if (beforeReturn != nullptr) {
            beforeReturn();
        }
        return Status::OK();
    }

    Status HandleWorkerEvent(const CoordinationEvent &event, WorkerWatchEvent &typed) override
    {
        (void)event;
        (void)typed;
        return Status(K_RUNTIME_ERROR, "watch is unused");
    }
};

std::string MakeWorkerServiceInfoValue(const std::string &state)
{
    WorkerServiceInfo value;
    value.timestamp = 100;
    value.hostId = "host-a";
    auto rc = StringToWorkerServiceState(state, value.state);
    return rc.IsOk() ? value.ToString() : "100;" + state + ";host-a";
}

CoordinationEvent MakeEvent(CoordinationEventType type, const std::string &workerId, const std::string &value,
                            int64_t revision)
{
    return { type, "/datasystem/cluster/" + workerId, value, revision, revision };
}

TEST(ClusterMembershipTest, RebuildPublishesSnapshotAndValidatesScaleIn)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeWorkerServiceInfoValue(ETCD_NODE_READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");

    DS_ASSERT_OK(membership.Rebuild());
    EXPECT_EQ(membership.RuntimeState(), MembershipRuntimeState::READY);

    std::shared_ptr<const MembershipSnapshot> snapshot;
    DS_ASSERT_OK(membership.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->workers.size(), 1ul);

    ScaleInRequest request;
    Revision revision = 0;
    DS_ASSERT_OK(membership.BuildLocalScaleInRequest(ScaleInReason::ORDERLY_SHUTDOWN, request, revision));
    EXPECT_EQ(request.workerId, "127.0.0.1:7001");
}

TEST(ClusterMembershipTest, AppliesExternalStorePutAndDeleteEvents)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeWorkerServiceInfoValue(ETCD_NODE_READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());

    DS_ASSERT_OK(membership.HandleStoreEvent(
        MakeEvent(CoordinationEventType::PUT, "127.0.0.1:7002", MakeWorkerServiceInfoValue("start"), 3)));
    std::shared_ptr<const MembershipSnapshot> snapshot;
    DS_ASSERT_OK(membership.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->workers.size(), 2ul);
    EXPECT_EQ(snapshot->workers.at("127.0.0.1:7002").serviceState, WorkerServiceState::START);

    DS_ASSERT_OK(membership.HandleStoreEvent(MakeEvent(CoordinationEventType::DELETE, "127.0.0.1:7002", "", 4)));
    DS_ASSERT_OK(membership.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->workers.count("127.0.0.1:7002"), 0ul);
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
                                              MakeWorkerServiceInfoValue(ETCD_NODE_READY), 1))
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
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeWorkerServiceInfoValue(ETCD_NODE_READY)));
    ClusterRegistry registry(store);
    auto membership = std::make_unique<ClusterMembership>(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership->Rebuild());

    membership.reset();
}

TEST(ClusterMembershipTest, ScaleInRequiresReadyLocalWorker)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeWorkerServiceInfoValue("start")));
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

TEST(ClusterMembershipTest, ScaleInRequiresLocalWorkerInSnapshot)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", MakeWorkerServiceInfoValue(ETCD_NODE_READY)));
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
