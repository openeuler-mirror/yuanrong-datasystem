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
 * Description: Topology change handler tests.
 */
#include "datasystem/topology/runtime/topology_change_handler.h"

#include "gtest/gtest.h"

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"

namespace datasystem {
namespace topology {
namespace {

class FakeRequester final : public ITopologyChangeRequester {
public:
    Status SubmitScaleInRequest(const ScaleInRequest &request, Revision observedMembershipRevision) override
    {
        ++callCount;
        lastRequest = request;
        lastRevision = observedMembershipRevision;
        return nextStatus;
    }

    uint64_t callCount{ 0 };
    ScaleInRequest lastRequest;
    Revision lastRevision{ 0 };
    Status nextStatus;
};

std::string MakeKeepAliveValue(const std::string &state)
{
    KeepAliveValue value;
    value.timestamp = "100";
    value.state = state;
    value.hostId = "host-a";
    return value.ToString();
}

TEST(TopologyChangeHandlerTest, SubmitsScaleInForReadyLocalWorker)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeKeepAliveValue(ETCD_NODE_READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());
    FakeRequester requester;
    TopologyChangeHandler handler(membership, requester);

    DS_ASSERT_OK(handler.RequestScaleIn(ScaleInReason::ORDERLY_SHUTDOWN));

    EXPECT_EQ(requester.callCount, 1ul);
    EXPECT_EQ(requester.lastRequest.workerId, "127.0.0.1:7001");
    EXPECT_EQ(requester.lastRequest.reason, ScaleInReason::ORDERLY_SHUTDOWN);
}

TEST(TopologyChangeHandlerTest, DoesNotSubmitWhenMembershipRejects)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeKeepAliveValue("start")));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());
    FakeRequester requester;
    TopologyChangeHandler handler(membership, requester);

    EXPECT_EQ(handler.RequestScaleIn(ScaleInReason::MANUAL_DRAIN).GetCode(), K_NOT_READY);
    EXPECT_EQ(requester.callCount, 0ul);
}

TEST(TopologyChangeHandlerTest, PropagatesRequesterBackpressure)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeKeepAliveValue(ETCD_NODE_READY)));
    ClusterRegistry registry(store);
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());
    FakeRequester requester;
    requester.nextStatus = Status(K_WRITE_BACK_QUEUE_FULL, "queue full");
    TopologyChangeHandler handler(membership, requester);

    EXPECT_EQ(handler.RequestScaleIn(ScaleInReason::ORDERLY_SHUTDOWN).GetCode(), K_WRITE_BACK_QUEUE_FULL);
    EXPECT_EQ(requester.callCount, 1ul);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
