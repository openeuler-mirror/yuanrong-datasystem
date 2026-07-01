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

std::string MakeWorkerServiceInfoValue(const std::string &timestamp, const std::string &state,
                                       const std::string &hostId = "host-a")
{
    WorkerServiceInfo value;
    auto rc = StringToWorkerServiceState(state, value.state);
    if (rc.IsError()) {
        return timestamp + ";" + state + ";" + hostId;
    }
    value.timestamp = timestamp.empty() ? 0 : std::stoll(timestamp);
    value.hostId = hostId;
    return value.ToString();
}

TEST(ClusterRegistryTest, ListsWorkersAndIsolatesBadRecords)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001",
                                  MakeWorkerServiceInfoValue("100", ETCD_NODE_READY, "host-a")));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", "bad-value"));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListWorkers(snapshot));

    EXPECT_EQ(snapshot.workers.size(), 1ul);
    EXPECT_EQ(snapshot.badRecordCount, 1ul);
    auto iter = snapshot.workers.find("127.0.0.1:7001");
    ASSERT_NE(iter, snapshot.workers.end());
    EXPECT_EQ(iter->second.serviceState, WorkerServiceState::READY);
    EXPECT_EQ(iter->second.capability.hostId, "host-a");
    EXPECT_EQ(iter->second.endpoint.ToString(), "127.0.0.1:7001");
}

TEST(ClusterRegistryTest, ParsesIpv6AndLifecycleStates)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[::1]:7001",
                                  MakeWorkerServiceInfoValue("100", ETCD_NODE_DOWNGRADE_RESTART)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", MakeWorkerServiceInfoValue("101", ETCD_NODE_EXITING)));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListWorkers(snapshot));

    EXPECT_EQ(snapshot.workers.at("[::1]:7001").endpoint.ToString(), "[::1]:7001");
    EXPECT_EQ(snapshot.workers.at("[::1]:7001").serviceState, WorkerServiceState::DOWNGRADE_RESTART);
    EXPECT_EQ(snapshot.workers.at("127.0.0.1:7002").serviceState, WorkerServiceState::EXITING);
}

TEST(ClusterRegistryTest, ParsesStartupServiceStatesAndBuildsWorkerKey)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeWorkerServiceInfoValue("100", "start")));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", MakeWorkerServiceInfoValue("101", "restart")));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7003", MakeWorkerServiceInfoValue("102", "recover")));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListWorkers(snapshot));

    EXPECT_EQ(snapshot.workers.at("127.0.0.1:7001").serviceState, WorkerServiceState::START);
    EXPECT_EQ(snapshot.workers.at("127.0.0.1:7002").serviceState, WorkerServiceState::RESTART);
    EXPECT_EQ(snapshot.workers.at("127.0.0.1:7003").serviceState, WorkerServiceState::RECOVER);

    std::string key;
    DS_ASSERT_OK(ClusterRegistryKeyHelper::BuildWorkerKey("127.0.0.1:7001", key));
    EXPECT_EQ(key, "127.0.0.1:7001");
    EXPECT_EQ(ClusterRegistryKeyHelper::BuildWorkerKey("bad/id", key).GetCode(), K_INVALID);
}

TEST(ClusterRegistryTest, IsolatesUnknownServiceState)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001", MakeWorkerServiceInfoValue("100", "unknown")));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListWorkers(snapshot));

    EXPECT_TRUE(snapshot.workers.empty());
    EXPECT_EQ(snapshot.badRecordCount, 1ul);
}

TEST(ClusterRegistryTest, IsolatesInvalidWorkerKeysAndValues)
{
    FakeCoordinationBackend store;
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "", MakeWorkerServiceInfoValue("100", ETCD_NODE_READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1", MakeWorkerServiceInfoValue("101", ETCD_NODE_READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:abc", MakeWorkerServiceInfoValue("102", ETCD_NODE_READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:65536", MakeWorkerServiceInfoValue("103", ETCD_NODE_READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:1:2", MakeWorkerServiceInfoValue("104", ETCD_NODE_READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[::1", MakeWorkerServiceInfoValue("105", ETCD_NODE_READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7001|bad", MakeWorkerServiceInfoValue("106", ETCD_NODE_READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[]:7001", MakeWorkerServiceInfoValue("107", ETCD_NODE_READY)));
    DS_ASSERT_OK(store.PutForTest(ETCD_CLUSTER_TABLE, "[::1]:", MakeWorkerServiceInfoValue("108", ETCD_NODE_READY)));
    DS_ASSERT_OK(
        store.PutForTest(ETCD_CLUSTER_TABLE, "127.0.0.1:7002", MakeWorkerServiceInfoValue("", ETCD_NODE_READY)));
    ClusterRegistry registry(store);

    MembershipSnapshot snapshot;
    DS_ASSERT_OK(registry.ListWorkers(snapshot));

    EXPECT_TRUE(snapshot.workers.empty());
    EXPECT_EQ(snapshot.badRecordCount, 10ul);

    std::string key;
    EXPECT_EQ(ClusterRegistryKeyHelper::BuildWorkerKey("", key).GetCode(), K_INVALID);
}

TEST(ClusterRegistryTest, DecodesPutDeleteEvents)
{
    FakeCoordinationBackend store;
    ClusterRegistry registry(store);

    CoordinationEvent put;
    put.type = CoordinationEventType::PUT;
    put.key = "/datasystem/cluster/127.0.0.1:7001";
    put.value = MakeWorkerServiceInfoValue("200", ETCD_NODE_READY);
    put.revision = 8;
    WorkerWatchEvent typed;
    DS_ASSERT_OK(registry.HandleWorkerEvent(put, typed));
    EXPECT_EQ(typed.type, WorkerWatchEventType::UPDATED);
    EXPECT_EQ(typed.workerId, "127.0.0.1:7001");
    EXPECT_EQ(typed.record.modRevision, 8);

    CoordinationEvent del;
    del.type = CoordinationEventType::DELETE;
    del.key = "/datasystem/cluster/127.0.0.1:7001";
    del.revision = 9;
    DS_ASSERT_OK(registry.HandleWorkerEvent(del, typed));
    EXPECT_EQ(typed.type, WorkerWatchEventType::DELETED);
    EXPECT_EQ(typed.workerId, "127.0.0.1:7001");
    EXPECT_EQ(
        registry.HandleWorkerEvent({ CoordinationEventType::PUT, "/datasystem/ring/", "", 0, 0 }, typed).GetCode(),
        K_NOT_FOUND);
    EXPECT_EQ(registry.HandleWorkerEvent({ CoordinationEventType::PUT, "127.0.0.1:7001", "", 0, 0 }, typed).GetCode(),
              K_NOT_FOUND);
}

TEST(ClusterRegistryTest, RejectsMalformedWorkerEventPayload)
{
    FakeCoordinationBackend store;
    ClusterRegistry registry(store);

    CoordinationEvent put;
    put.type = CoordinationEventType::PUT;
    put.key = "/datasystem/cluster/127.0.0.1:7001";
    put.value = "bad-value";
    put.revision = 10;
    WorkerWatchEvent typed;
    EXPECT_EQ(registry.HandleWorkerEvent(put, typed).GetCode(), K_INVALID);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
