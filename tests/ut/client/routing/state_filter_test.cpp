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

#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "datasystem/client/routing/state_filter.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
namespace {
constexpr char WORKER_ADDR[] = "127.0.0.1:1000";

std::shared_ptr<ClusterTopologyPb> BuildTopology(MembershipPb::StatePb state)
{
    auto topology = std::make_shared<ClusterTopologyPb>();
    topology->set_tokens_per_member(1);
    auto &member = (*topology->mutable_members())[WORKER_ADDR];
    member.set_state(state);
    return topology;
}

std::shared_ptr<std::unordered_map<std::string, std::string>> BuildHostIdMap()
{
    auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
    (*hostIdMap)[WORKER_ADDR] = "host-a";
    return hostIdMap;
}

Status UpdateHashRing(client::WorkerRouter &router, std::shared_ptr<ClusterTopologyPb> topology,
                      const std::shared_ptr<std::unordered_map<std::string, std::string>> &hostIdMap)
{
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    RETURN_IF_NOT_OK(client::PreparedClusterTopology::Create(std::move(*topology), prepared));
    router.UpdateHashRing(*prepared, *hostIdMap);
    return Status::OK();
}
}  // namespace

class StateFilterTest : public CommonTest {
protected:
    client::WorkerRouter router_{ "host-a" };
    client::StateFilter filter_{ &router_ };
    HostPort worker_{ "127.0.0.1", 1000 };
};

TEST_F(StateFilterTest, ActiveWorkerIsAvailable)
{
    DS_ASSERT_OK(UpdateHashRing(router_, BuildTopology(MembershipPb::ACTIVE), BuildHostIdMap()));

    EXPECT_TRUE(filter_.IsAvailable(worker_));
}

TEST_F(StateFilterTest, NonActiveWorkersAreUnavailable)
{
    const MembershipPb::StatePb unavailableStates[] = {
        MembershipPb::INITIAL,
        MembershipPb::JOINING,
        MembershipPb::PRE_LEAVING,
        MembershipPb::LEAVING,
        MembershipPb::FAILED,
    };

    for (auto state : unavailableStates) {
        DS_ASSERT_OK(UpdateHashRing(router_, BuildTopology(state), BuildHostIdMap()));
        EXPECT_FALSE(filter_.IsAvailable(worker_)) << "state=" << state;
    }
}

TEST_F(StateFilterTest, MissingRouterOrWorkerIsUnavailable)
{
    client::StateFilter missingRouter(nullptr);

    EXPECT_FALSE(missingRouter.IsAvailable(worker_));
    EXPECT_FALSE(filter_.IsAvailable(worker_));
}

TEST_F(StateFilterTest, ReflectsUpdatedHashRingWithoutCachedState)
{
    DS_ASSERT_OK(UpdateHashRing(router_, BuildTopology(MembershipPb::ACTIVE), BuildHostIdMap()));
    EXPECT_TRUE(filter_.IsAvailable(worker_));

    auto leavingTopology = BuildTopology(MembershipPb::LEAVING);
    filter_.OnHashRingUpdated(*leavingTopology);
    EXPECT_TRUE(filter_.IsAvailable(worker_));

    DS_ASSERT_OK(UpdateHashRing(router_, leavingTopology, BuildHostIdMap()));
    EXPECT_FALSE(filter_.IsAvailable(worker_));
}

}  // namespace ut
}  // namespace datasystem
