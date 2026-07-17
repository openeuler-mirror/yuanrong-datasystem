/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Object-cache endpoint and route failure policy tests.
 */
#include "datasystem/worker/object_cache/object_endpoint_policy.h"

#include "gtest/gtest.h"

#include "ut/common.h"

namespace datasystem::object_cache {
namespace {
constexpr char MEMBER_ADDRESS[] = "127.0.0.1:19001";

std::shared_ptr<const cluster::TopologySnapshot> MakeSnapshot()
{
    cluster::TopologyState state;
    state.version = 1;
    state.members = { cluster::Member{ { std::string(16, 'a'), MEMBER_ADDRESS }, cluster::MemberState::ACTIVE,
                                       { 1 } } };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    EXPECT_TRUE(cluster::TopologySnapshot::Create(state, 1, std::string(64, 'a'), snapshot).IsOk());
    return snapshot;
}

TEST(ObjectEndpointPolicyTest, AppliesDirectoryLagOnlyAtObjectBoundary)
{
    cluster::TopologySnapshotState snapshots;
    cluster::MembershipEndpointView membership(snapshots);
    worker::MetadataRouteOptions options;
    options.centralizedMode = true;
    options.masterAddress = HostPort("127.0.0.1", 19002);
    worker::MetadataRouteResolver resolver(nullptr, options);
    ObjectEndpointPolicy policy(resolver, membership);

    EXPECT_EQ(policy.CheckEndpoint(options.masterAddress, false).GetCode(), K_NOT_READY);
    EXPECT_TRUE(policy.CheckEndpoint(options.masterAddress, true).IsOk());
    EXPECT_EQ(policy.CheckMetaOwner("key", false).GetCode(), K_NOT_READY);
    EXPECT_TRUE(policy.CheckMetaOwner("key", true).IsOk());
}

TEST(ObjectEndpointPolicyTest, RejectsObservedUnreachableEndpoint)
{
    cluster::TopologySnapshotState snapshots;
    auto snapshot = MakeSnapshot();
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    cluster::MembershipEndpointView membership(snapshots);
    worker::MetadataRouteOptions options;
    options.centralizedMode = true;
    options.masterAddress = HostPort("127.0.0.1", 19001);
    worker::MetadataRouteResolver resolver(nullptr, options);
    ObjectEndpointPolicy policy(resolver, membership);
    cluster::EndpointObservation observation{ snapshot->Members().front().identity, 1,
                                              cluster::EndpointAvailability::UNREACHABLE,
                                              std::chrono::steady_clock::now() };
    DS_ASSERT_OK(membership.UpdateObservation(observation));

    EXPECT_EQ(policy.CheckMetaOwner("key", false).GetCode(), K_MASTER_TIMEOUT);
}

TEST(ObjectEndpointPolicyTest, AllowsUnknownEndpointWhenDirectoryLagFallbackIsEnabled)
{
    cluster::TopologySnapshotState snapshots;
    auto snapshot = MakeSnapshot();
    cluster::SnapshotUpdateOutcome outcome;
    DS_ASSERT_OK(snapshots.Publish(snapshot, outcome));
    cluster::MembershipEndpointView membership(snapshots);
    worker::MetadataRouteOptions options;
    options.centralizedMode = true;
    options.masterAddress = HostPort("127.0.0.1", 19001);
    worker::MetadataRouteResolver resolver(nullptr, options);
    ObjectEndpointPolicy policy(resolver, membership);
    cluster::MemberEndpoint endpoint;
    DS_ASSERT_OK(membership.ResolveByAddress(MEMBER_ADDRESS, endpoint));
    ASSERT_EQ(endpoint.localAvailability, cluster::EndpointAvailability::UNKNOWN);

    EXPECT_TRUE(policy.CheckEndpoint(options.masterAddress, true).IsOk());
}

TEST(ObjectEndpointPolicyTest, AppendsRouteFailuresWithoutCreatingAnEmptyGroup)
{
    worker::MetaOwnerRouteGroups grouped;
    AppendRouteFailures(grouped);
    EXPECT_TRUE(grouped.groups.empty());

    grouped.failures.emplace("key-a", Status(K_NOT_READY, "not ready"));
    grouped.failures.emplace("key-b", Status(K_NOT_FOUND, "not found"));
    AppendRouteFailures(grouped);
    ASSERT_EQ(grouped.groups.size(), 1);
    EXPECT_EQ(grouped.groups.begin()->second.size(), 2);
    EXPECT_EQ(grouped.failures.at("key-a").GetCode(), K_NOT_READY);
    EXPECT_EQ(grouped.failures.at("key-a").GetMsg(), "not ready");
    EXPECT_EQ(grouped.failures.at("key-b").GetCode(), K_NOT_FOUND);
    EXPECT_EQ(grouped.failures.at("key-b").GetMsg(), "not found");
}
}  // namespace
}  // namespace datasystem::object_cache
