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
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/routing/broken_filter.h"
#include "datasystem/client/routing/i_worker_filter.h"
#include "datasystem/client/routing/routing.h"
#include "datasystem/client/routing/state_filter.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

class RoutingTest : public CommonTest {
protected:
    std::shared_ptr<HashRingPb> BuildRing()
    {
        auto ring = std::make_shared<HashRingPb>();
        auto &wA = (*ring->mutable_workers())["127.0.0.1:1000"];
        wA.set_state(WorkerPb_StatePb_ACTIVE);
        wA.add_hash_tokens(100u);
        wA.add_hash_tokens(200u);
        auto &wB = (*ring->mutable_workers())["127.0.0.1:2000"];
        wB.set_state(WorkerPb_StatePb_ACTIVE);
        wB.add_hash_tokens(300u);
        wB.add_hash_tokens(400u);
        return ring;
    }

    std::shared_ptr<std::unordered_map<std::string, std::string>> BuildHostIdMap()
    {
        auto map = std::make_shared<std::unordered_map<std::string, std::string>>();
        (*map)["127.0.0.1:1000"] = "host-a";
        (*map)["127.0.0.1:2000"] = "host-b";
        return map;
    }

    std::shared_ptr<client::WorkerRouter> CreateRouter(const std::string &hostId = "host-a")
    {
        auto router = std::make_shared<client::WorkerRouter>(hostId, std::vector<std::shared_ptr<client::IWorkerFilter>>{});
        return router;
    }

    std::shared_ptr<client::WorkerRouter> CreateRouterWithFilters(const std::string &hostId = "host-a")
    {
        auto router = std::make_shared<client::WorkerRouter>(hostId, std::vector<std::shared_ptr<client::IWorkerFilter>>{});
        // Filters created externally and registered via UpdateState broadcast
        return router;
    }
};

// === WorkerRouter Tests ===

TEST_F(RoutingTest, TestSelectWorkerEmptyRing)
{
    auto router = CreateRouter();
    HostPort worker;
    auto st = router->SelectWorker("key", client::SelectStrategy::HASH_RING_AFFINITY, worker);
    EXPECT_FALSE(st.IsOk());
}

TEST_F(RoutingTest, TestSelectWorkerReturnsActive)
{
    auto router = CreateRouter();
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort worker;
    DS_ASSERT_OK(router->SelectWorker("key", client::SelectStrategy::HASH_RING_AFFINITY, worker));
    std::string addr = worker.ToString();
    EXPECT_TRUE(addr == "127.0.0.1:1000" || addr == "127.0.0.1:2000");
}

TEST_F(RoutingTest, TestSelectWorkerConsistency)
{
    auto router = CreateRouter();
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort w1, w2;
    DS_ASSERT_OK(router->SelectWorker("consistency_key", client::SelectStrategy::HASH_RING_AFFINITY, w1));
    DS_ASSERT_OK(router->SelectWorker("consistency_key", client::SelectStrategy::HASH_RING_AFFINITY, w2));
    EXPECT_EQ(w1.ToString(), w2.ToString());
}

TEST_F(RoutingTest, TestSelectWorkerExclude)
{
    auto router = CreateRouter();
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort first;
    DS_ASSERT_OK(router->SelectWorker("exclude_key", client::SelectStrategy::HASH_RING_AFFINITY, first));

    // Exclude the first result, should get a different one
    HostPort second;
    DS_ASSERT_OK(router->SelectWorker("exclude_key", client::SelectStrategy::HASH_RING_AFFINITY, second, {first}));
    EXPECT_NE(first.ToString(), second.ToString());
}

TEST_F(RoutingTest, TestSelectWorkersBatch)
{
    auto router = CreateRouter();
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    std::vector<std::string> keys = { "k1", "k2", "k3", "k4", "k5" };
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(router->SelectWorkers(keys, client::SelectStrategy::HASH_RING_AFFINITY, groups));

    // All keys should be grouped
    size_t totalKeys = 0;
    for (const auto &g : groups) {
        totalKeys += g.second.size();
    }
    EXPECT_EQ(totalKeys, keys.size());
}

TEST_F(RoutingTest, TestSameNodePreferred)
{
    auto router = CreateRouter("host-a");
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort worker;
    DS_ASSERT_OK(router->SelectWorker("samenode_key", client::SelectStrategy::SAME_NODE_PREFERRED, worker));
    EXPECT_EQ(worker.ToString(), "127.0.0.1:1000");  // host-a's worker
}

TEST_F(RoutingTest, TestGetRingState)
{
    auto router = CreateRouter();
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort w("127.0.0.1", 1000);
    EXPECT_EQ(router->GetRingState(w), client::WorkerRingState::ACTIVE);

    HostPort unknown("1.2.3.4", 9999);
    EXPECT_EQ(router->GetRingState(unknown), client::WorkerRingState::UNKNOWN);
}

// === BrokenFilter Tests ===

TEST_F(RoutingTest, TestBrokenFilter)
{
    client::BrokenFilter filter;

    HostPort addr("127.0.0.1", 1000);
    EXPECT_TRUE(filter.IsAvailable(addr));

    filter.OnWorkerStateChange(addr, K_CLIENT_WORKER_DISCONNECT);
    EXPECT_FALSE(filter.IsAvailable(addr));

    // Other status should be ignored
    filter.OnWorkerStateChange(addr, K_RUNTIME_ERROR);
    EXPECT_FALSE(filter.IsAvailable(addr));  // Still broken from K_DISCONNECT
}

TEST_F(RoutingTest, TestBrokenFilterIgnoresOtherWorkers)
{
    client::BrokenFilter filter;

    HostPort a("127.0.0.1", 1000);
    HostPort b("127.0.0.1", 2000);

    filter.OnWorkerStateChange(a, K_CLIENT_WORKER_DISCONNECT);
    EXPECT_FALSE(filter.IsAvailable(a));
    EXPECT_TRUE(filter.IsAvailable(b));  // b unaffected
}

TEST_F(RoutingTest, TestBrokenFilterIntegrationWithRouter)
{
    auto brokenFilter = std::make_shared<client::BrokenFilter>();
    auto router = std::make_shared<client::WorkerRouter>(
        "host-a", std::vector<std::shared_ptr<client::IWorkerFilter>>{brokenFilter});
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort first;
    DS_ASSERT_OK(router->SelectWorker("broken_key", client::SelectStrategy::HASH_RING_AFFINITY, first));

    // Mark first worker as broken
    router->UpdateState(first, K_CLIENT_WORKER_DISCONNECT);

    // Subsequent SelectWorker should skip broken worker
    HostPort second;
    DS_ASSERT_OK(router->SelectWorker("broken_key", client::SelectStrategy::HASH_RING_AFFINITY, second));
    EXPECT_NE(first.ToString(), second.ToString());
}

// === Status.WithExtra Tests ===

TEST_F(RoutingTest, TestStatusWithExtra)
{
    Status st(K_NOT_OWNER, "not owner");
    EXPECT_FALSE(st.HasExtra());

    st.WithExtra("127.0.0.1:2000");
    EXPECT_TRUE(st.HasExtra());
    EXPECT_EQ(st.GetExtra(), "127.0.0.1:2000");
}

TEST_F(RoutingTest, TestStatusWithExtraOverwrite)
{
    Status st(K_NOT_OWNER, "not owner");
    st.WithExtra("addr1");
    st.WithExtra("addr2");
    EXPECT_EQ(st.GetExtra(), "addr2");
}

TEST_F(RoutingTest, TestStatusWithExtraEmpty)
{
    Status st(K_NOT_OWNER, "not owner");
    st.WithExtra("");
    EXPECT_FALSE(st.HasExtra());
}

}  // namespace ut
}  // namespace datasystem
