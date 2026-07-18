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
#include "datasystem/client/cluster_query/cluster_query_projector.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "datasystem/common/util/uuid_generator.h"
#include "gtest/gtest.h"
#include "ut/common.h"

namespace datasystem::ut {
namespace {

using client::cluster_query::ClusterNodeHealth;
using client::cluster_query::ClusterQueryProjector;
using client::cluster_query::ClusterSourceSnapshot;
using client::cluster_query::MembershipObservation;

cluster::Member MakeMember(char idByte, std::string address, cluster::MemberState state,
                           std::vector<uint32_t> tokens)
{
    return { { std::string(UUID_SIZE, idByte), std::move(address) }, state, std::move(tokens) };
}

std::shared_ptr<const cluster::TopologySnapshot> MakeSnapshot(std::vector<cluster::Member> members,
                                                               uint64_t version = 7)
{
    std::sort(members.begin(), members.end(), [](const auto &left, const auto &right) {
        return left.identity.address < right.identity.address;
    });
    cluster::TopologyState state;
    state.clusterHasInit = true;
    state.version = version;
    state.members = std::move(members);
    const bool hasFailedMember = std::any_of(state.members.begin(), state.members.end(), [](const auto &member) {
        return member.state == cluster::MemberState::FAILED;
    });
    if (hasFailedMember) {
        state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::FAILURE, version };
    }
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    EXPECT_TRUE(cluster::TopologySnapshot::Create(std::move(state), 11, std::string(64, 'a'), snapshot).IsOk());
    return snapshot;
}

}  // namespace

TEST(ClusterQueryProjectorTest, BuildsDeterministicHealthAndHashRanges)
{
    ClusterSourceSnapshot source;
    source.topology = MakeSnapshot({ MakeMember('a', "127.0.0.1:31501", cluster::MemberState::ACTIVE, { 10 }),
                                     MakeMember('b', "127.0.0.1:31502", cluster::MemberState::PRE_LEAVING, { 30 }),
                                     MakeMember('c', "127.0.0.1:31503", cluster::MemberState::FAILED, {}) });
    source.memberships = {
        MembershipObservation{ "127.0.0.1:31501", cluster::MemberLifecycleState::READY },
        MembershipObservation{ "127.0.0.1:31502", cluster::MemberLifecycleState::EXITING },
        MembershipObservation{ "127.0.0.1:31504", cluster::MemberLifecycleState::STARTING },
    };
    ClusterQueryProjector projector;
    client::cluster_query::ClusterQueryResult result;
    DS_ASSERT_OK(projector.BuildCluster(source, result));
    ASSERT_EQ(result.topologyVersion, 7);
    ASSERT_EQ(result.nodes.size(), 3U);
    EXPECT_EQ(result.nodes[0].health, ClusterNodeHealth::HEALTHY);
    ASSERT_EQ(result.nodes[0].hashRanges.size(), 1U);
    EXPECT_EQ(result.nodes[0].hashRanges[0].start, 30U);
    EXPECT_EQ(result.nodes[0].hashRanges[0].end, 10U);
    EXPECT_EQ(result.nodes[1].health, ClusterNodeHealth::DRAINING);
    EXPECT_EQ(result.nodes[2].health, ClusterNodeHealth::UNHEALTHY);
}

TEST(ClusterQueryProjectorTest, RoutesExactTokensAndPreservesDuplicateInputOrder)
{
    cluster::HashAlgorithm algorithm;
    const uint32_t firstToken = algorithm.Hash("key-1");
    const uint32_t secondToken = algorithm.Hash("key-2");
    ASSERT_NE(firstToken, secondToken);
    const bool firstIsLower = firstToken < secondToken;
    const std::string firstOwner = firstIsLower ? "127.0.0.1:31501" : "127.0.0.1:31502";
    const std::string secondOwner = firstIsLower ? "127.0.0.1:31502" : "127.0.0.1:31501";
    ClusterSourceSnapshot source;
    source.topology = MakeSnapshot({ MakeMember('a', firstOwner, cluster::MemberState::ACTIVE, { firstToken }),
                                     MakeMember('b', secondOwner, cluster::MemberState::ACTIVE, { secondToken }) });
    source.memberships = {
        MembershipObservation{ firstOwner, cluster::MemberLifecycleState::READY },
        MembershipObservation{ secondOwner, cluster::MemberLifecycleState::READY },
    };
    const std::vector<std::string_view> keys{ "key-1", "key-2", "key-1" };
    client::cluster_query::RouteQueryResult result;
    ClusterQueryProjector projector;
    DS_ASSERT_OK(projector.Route(source, keys, result));
    ASSERT_EQ(result.routes.size(), 2U);
    const auto firstRoute = std::find_if(result.routes.begin(), result.routes.end(), [&](const auto &route) {
        return route.workerAddress == firstOwner;
    });
    ASSERT_NE(firstRoute, result.routes.end());
    EXPECT_EQ(firstRoute->keys, (std::vector<std::string>{ "key-1", "key-1" }));
}

TEST(ClusterQueryProjectorTest, EnforcesBatchAndByteLimitsBeforeRouting)
{
    ClusterSourceSnapshot source;
    ClusterQueryProjector projector;
    client::cluster_query::RouteQueryResult result;
    std::vector<std::string_view> noKeys;
    EXPECT_EQ(projector.Route(source, noKeys, result).GetCode(), K_INVALID);
    const std::vector<std::string_view> emptyKey{ "" };
    EXPECT_EQ(projector.Route(source, emptyKey, result).GetCode(), K_INVALID);
    const std::string oversized(client::cluster_query::MAX_QUERY_KEY_BYTES + 1, 'x');
    const std::vector<std::string_view> keys{ oversized };
    EXPECT_EQ(projector.Route(source, keys, result).GetCode(), K_INVALID);
}

}  // namespace datasystem::ut
