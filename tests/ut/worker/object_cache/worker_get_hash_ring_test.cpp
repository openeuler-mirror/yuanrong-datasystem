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
#include <utility>

#include <gtest/gtest.h>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/worker/object_cache/get_hash_ring_response.h"
#include "ut/common.h"

namespace datasystem::object_cache {
namespace {
constexpr uint64_t TOPOLOGY_VERSION = 7;
constexpr char WORKER_A[] = "127.0.0.1:1000";
constexpr char WORKER_B[] = "127.0.0.1:2000";

Status MakeSnapshot(std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    state.version = TOPOLOGY_VERSION;
    state.clusterHasInit = true;
    state.tokensPerMember = 1;
    state.activeBatch = cluster::ActiveBatch{ cluster::TopologyChangeType::SCALE_OUT, TOPOLOGY_VERSION };
    state.members = {
        cluster::Member{ { std::string(16, 'a'), WORKER_A }, cluster::MemberState::ACTIVE, { 100u } },
        cluster::Member{ { std::string(16, 'b'), WORKER_B }, cluster::MemberState::JOINING, { 200u } },
    };
    return cluster::TopologySnapshot::Create(std::move(state), TOPOLOGY_VERSION, std::string(64, 'a'), snapshot,
                                             { { WORKER_A, "host-a" }, { WORKER_B, "host-b" } });
}
}  // namespace

class WorkerGetHashRingTest : public ::datasystem::ut::CommonTest {};

TEST_F(WorkerGetHashRingTest, MatchingVersionClearsPayload)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(MakeSnapshot(snapshot));
    GetHashRingRspPb rsp;
    (*rsp.mutable_host_id_map())["stale"] = "stale";
    (*rsp.mutable_hash_ring()->mutable_members())["stale"];

    DS_ASSERT_OK(BuildGetHashRingResponse(*snapshot, TOPOLOGY_VERSION, "127.0.0.1:9000", rsp));

    EXPECT_FALSE(rsp.hash_ring_changed());
    EXPECT_EQ(rsp.version(), TOPOLOGY_VERSION);
    EXPECT_EQ(rsp.master_address(), "127.0.0.1:9000");
    EXPECT_TRUE(rsp.hash_ring().members().empty());
    EXPECT_TRUE(rsp.host_id_map().empty());
}

TEST_F(WorkerGetHashRingTest, DifferentVersionReturnsTopologyAndHostIdMap)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(MakeSnapshot(snapshot));
    GetHashRingRspPb rsp;

    DS_ASSERT_OK(BuildGetHashRingResponse(*snapshot, 0, "127.0.0.1:9000", rsp));

    EXPECT_TRUE(rsp.hash_ring_changed());
    EXPECT_EQ(rsp.version(), TOPOLOGY_VERSION);
    EXPECT_EQ(rsp.hash_ring().version(), TOPOLOGY_VERSION);
    EXPECT_EQ(rsp.hash_ring().schema_version(), "2");
    EXPECT_TRUE(rsp.hash_ring().cluster_has_init());
    EXPECT_EQ(rsp.hash_ring().tokens_per_member(), 1);
    EXPECT_EQ(rsp.hash_ring().members_size(), 2);
    EXPECT_EQ(rsp.hash_ring().members().at(WORKER_A).state(), MembershipPb::ACTIVE);
    EXPECT_EQ(rsp.hash_ring().members().at(WORKER_B).state(), MembershipPb::JOINING);
    EXPECT_EQ(rsp.hash_ring().active_batch().type(), SCALE_OUT);
    EXPECT_EQ(rsp.hash_ring().active_batch().epoch(), TOPOLOGY_VERSION);
    EXPECT_EQ(rsp.host_id_map().at(WORKER_A), "host-a");
    EXPECT_EQ(rsp.host_id_map().at(WORKER_B), "host-b");
}

TEST_F(WorkerGetHashRingTest, ResponseRetainsHeldSnapshotGeneration)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(MakeSnapshot(snapshot));
    const auto held = snapshot;
    auto updated = snapshot->CopyState();
    ++updated.version;
    DS_ASSERT_OK(cluster::TopologySnapshot::Create(std::move(updated), TOPOLOGY_VERSION + 1,
                                                   std::string(64, 'b'), snapshot,
                                                   { { WORKER_A, "new-host" } }));
    GetHashRingRspPb rsp;

    DS_ASSERT_OK(BuildGetHashRingResponse(*held, 1, "127.0.0.1:9000", rsp));

    EXPECT_EQ(rsp.version(), TOPOLOGY_VERSION);
    EXPECT_EQ(rsp.hash_ring().version(), TOPOLOGY_VERSION);
    EXPECT_EQ(rsp.host_id_map().at(WORKER_A), "host-a");
}

TEST_F(WorkerGetHashRingTest, SameVersionHostIdChangesReturnCompleteSnapshot)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    DS_ASSERT_OK(MakeSnapshot(snapshot));
    const auto original = snapshot;
    GetHashRingRspPb rsp;
    DS_ASSERT_OK(BuildGetHashRingResponse(*snapshot, TOPOLOGY_VERSION, "", rsp, snapshot->HostIdsDigest()));
    EXPECT_FALSE(rsp.hash_ring_changed());
    EXPECT_FALSE(rsp.has_hash_ring());
    EXPECT_EQ(rsp.host_ids_digest(), snapshot->HostIdsDigest());

    DS_ASSERT_OK(cluster::TopologySnapshot::Create(original->CopyState(), TOPOLOGY_VERSION,
                                                  original->CanonicalDigest(), snapshot, {}, true));
    DS_ASSERT_OK(BuildGetHashRingResponse(*snapshot, TOPOLOGY_VERSION, "", rsp, original->HostIdsDigest()));
    EXPECT_TRUE(rsp.hash_ring_changed());
    EXPECT_TRUE(rsp.has_hash_ring());
    EXPECT_TRUE(rsp.host_id_map().empty());
    EXPECT_EQ(rsp.version(), TOPOLOGY_VERSION);
    const auto emptyDigest = rsp.host_ids_digest();
    EXPECT_NE(emptyDigest, original->HostIdsDigest());

    DS_ASSERT_OK(BuildGetHashRingResponse(*original, TOPOLOGY_VERSION, "", rsp, emptyDigest));
    EXPECT_TRUE(rsp.hash_ring_changed());
    EXPECT_EQ(rsp.host_id_map().at(WORKER_A), "host-a");
    DS_ASSERT_OK(BuildGetHashRingResponse(*original, TOPOLOGY_VERSION, "", rsp, original->HostIdsDigest()));
    EXPECT_FALSE(rsp.hash_ring_changed());
    EXPECT_TRUE(rsp.host_id_map().empty());
}

}  // namespace datasystem::object_cache
