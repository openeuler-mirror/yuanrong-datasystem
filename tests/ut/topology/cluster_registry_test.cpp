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
 * Description: Cluster registry tests.
 */
#include "datasystem/topology/membership/cluster_registry.h"

#include "gtest/gtest.h"

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"

namespace datasystem {
namespace topology {
namespace {

std::string MakeMembershipValue(int64_t timestamp, MemberLifecycleState state, const std::string &hostId = "host-a")
{
    MembershipValue value;
    value.timestamp = timestamp;
    value.lifecycleState = state;
    value.hostId = hostId;
    std::string encoded;
    auto rc = MembershipValueCodec::Encode(value, encoded);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    return encoded;
}

TEST(ClusterRegistryTest, ListsMembersAndIsolatesBadRecords)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001",
                                  MakeMembershipValue(100, MemberLifecycleState::READY, "host-a")));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", "bad-value"));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListMembers(snapshot));

    EXPECT_EQ(snapshot.members.size(), 1ul);
    EXPECT_EQ(snapshot.malformedRecordCount, 1ul);
    auto iter = snapshot.members.find("127.0.0.1:7001");
    ASSERT_NE(iter, snapshot.members.end());
    EXPECT_EQ(iter->second.lifecycleState, MemberLifecycleState::READY);
    EXPECT_EQ(iter->second.capability.hostId, "host-a");
    EXPECT_EQ(iter->second.endpoint.ToString(), "127.0.0.1:7001");
}

TEST(ClusterRegistryTest, ParsesIpv6AndLifecycleStates)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[::1]:7001",
                                  MakeMembershipValue(100, MemberLifecycleState::DOWNGRADE_RESTARTING)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", MakeMembershipValue(101, MemberLifecycleState::EXITING)));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListMembers(snapshot));

    EXPECT_EQ(snapshot.members.at("[::1]:7001").endpoint.ToString(), "[::1]:7001");
    EXPECT_EQ(snapshot.members.at("[::1]:7001").lifecycleState, MemberLifecycleState::DOWNGRADE_RESTARTING);
    EXPECT_EQ(snapshot.members.at("127.0.0.1:7002").lifecycleState, MemberLifecycleState::EXITING);
}

TEST(ClusterRegistryTest, ParsesStartupLifecycleStatesAndBuildsMemberKey)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeMembershipValue(100, MemberLifecycleState::STARTING)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002",
                                  MakeMembershipValue(101, MemberLifecycleState::RESTARTING)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7003",
                                  MakeMembershipValue(102, MemberLifecycleState::RECOVERING)));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListMembers(snapshot));

    EXPECT_EQ(snapshot.members.at("127.0.0.1:7001").lifecycleState, MemberLifecycleState::STARTING);
    EXPECT_EQ(snapshot.members.at("127.0.0.1:7002").lifecycleState, MemberLifecycleState::RESTARTING);
    EXPECT_EQ(snapshot.members.at("127.0.0.1:7003").lifecycleState, MemberLifecycleState::RECOVERING);

    std::string key;
    DS_ASSERT_OK(ClusterRegistryKeyHelper::BuildMemberKey("127.0.0.1:7001", key));
    EXPECT_EQ(key, "127.0.0.1:7001");
    EXPECT_EQ(ClusterRegistryKeyHelper::BuildMemberKey("bad/id", key).GetCode(), K_INVALID);
}

TEST(ClusterRegistryTest, RejectsLegacyStringMembershipValue)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", "100;ready;host-a"));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListMembers(snapshot));

    EXPECT_TRUE(snapshot.members.empty());
    EXPECT_EQ(snapshot.malformedRecordCount, 1ul);
}

TEST(ClusterRegistryTest, IsolatesInvalidWorkerKeysAndValues)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "", MakeMembershipValue(100, MemberLifecycleState::READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1", MakeMembershipValue(101, MemberLifecycleState::READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:abc", MakeMembershipValue(102, MemberLifecycleState::READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:65536", MakeMembershipValue(103, MemberLifecycleState::READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:1:2", MakeMembershipValue(104, MemberLifecycleState::READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[::1", MakeMembershipValue(105, MemberLifecycleState::READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001|bad", MakeMembershipValue(106, MemberLifecycleState::READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[]:7001", MakeMembershipValue(107, MemberLifecycleState::READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[::1]:", MakeMembershipValue(108, MemberLifecycleState::READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", MakeMembershipValue(0, MemberLifecycleState::READY)));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListMembers(snapshot));

    EXPECT_TRUE(snapshot.members.empty());
    EXPECT_EQ(snapshot.malformedRecordCount, 10ul);

    std::string key;
    EXPECT_EQ(ClusterRegistryKeyHelper::BuildMemberKey("", key).GetCode(), K_INVALID);
}

TEST(ClusterRegistryTest, DecodesPutDeleteEvents)
{
    FakeCoordinationBackend store;
    ClusterRegistry registry(store);

    CoordinationEvent put;
    put.type = CoordinationEventType::PUT;
    put.key = "/datasystem/cluster/127.0.0.1:7001";
    put.value = MakeMembershipValue(200, MemberLifecycleState::READY);
    put.revision = 8;
    MembershipWatchEvent typed;
    DS_ASSERT_OK(registry.HandleMembershipEvent(put, typed));
    EXPECT_EQ(typed.type, MembershipWatchEventType::UPDATED);
    EXPECT_EQ(typed.nodeId, "127.0.0.1:7001");
    EXPECT_EQ(typed.record.modRevision, 8);

    CoordinationEvent del;
    del.type = CoordinationEventType::DELETE;
    del.key = "/datasystem/cluster/127.0.0.1:7001";
    del.revision = 9;
    DS_ASSERT_OK(registry.HandleMembershipEvent(del, typed));
    EXPECT_EQ(typed.type, MembershipWatchEventType::DELETED);
    EXPECT_EQ(typed.nodeId, "127.0.0.1:7001");
    EXPECT_EQ(
        registry.HandleMembershipEvent({ CoordinationEventType::PUT, "/datasystem/ring/", "", 0, 0 }, typed).GetCode(),
        K_NOT_FOUND);
    EXPECT_EQ(
        registry.HandleMembershipEvent({ CoordinationEventType::PUT, "127.0.0.1:7001", "", 0, 0 }, typed).GetCode(),
        K_NOT_FOUND);
}

TEST(ClusterRegistryTest, RejectsMalformedMembershipEventPayload)
{
    FakeCoordinationBackend store;
    ClusterRegistry registry(store);

    CoordinationEvent put;
    put.type = CoordinationEventType::PUT;
    put.key = "/datasystem/cluster/127.0.0.1:7001";
    put.value = "bad-value";
    put.revision = 10;
    MembershipWatchEvent typed;
    EXPECT_EQ(registry.HandleMembershipEvent(put, typed).GetCode(), K_INVALID);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
