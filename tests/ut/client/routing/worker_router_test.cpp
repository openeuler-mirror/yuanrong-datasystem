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

#include <atomic>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/object_cache/routing/broken_filter.h"
#include "datasystem/client/object_cache/routing/client_read_bandwidth_scheduler.h"
#include "datasystem/client/object_cache/routing/i_worker_filter.h"
#include "datasystem/client/object_cache/routing/routing.h"
#include "datasystem/client/object_cache/routing/state_filter.h"
#include "datasystem/client/object_cache/routing/worker_router.h"
#include "datasystem/client/object_cache/routing/worker_ub_health_registry.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/hash_ring_token.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

class RejectAllFilter : public client::IWorkerFilter {
public:
    ~RejectAllFilter() override = default;

    bool IsAvailable(const HostPort &, client::WorkerAccessAction action) const override
    {
        (void)action;
        return false;
    }
};

class RoutingTest : public CommonTest {
protected:
    std::shared_ptr<::datasystem::ClusterTopologyPb> BuildRing()
    {
        auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
        ring->set_tokens_per_member(2);
        auto &wA = (*ring->mutable_members())["127.0.0.1:1000"];
        wA.set_state(::datasystem::MembershipPb::ACTIVE);
        auto &wB = (*ring->mutable_members())["127.0.0.1:2000"];
        wB.set_state(::datasystem::MembershipPb::ACTIVE);
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
        auto router = std::make_shared<client::WorkerRouter>(
            hostId, std::vector<std::shared_ptr<client::IWorkerFilter>>{});
        return router;
    }

    Status UpdateHashRing(const std::shared_ptr<client::WorkerRouter> &router,
                          std::shared_ptr<::datasystem::ClusterTopologyPb> ring,
                          const std::shared_ptr<std::unordered_map<std::string, std::string>> &hostIdMap)
    {
        std::unique_ptr<client::PreparedClusterTopology> prepared;
        RETURN_IF_NOT_OK(client::PreparedClusterTopology::Create(std::move(*ring), prepared));
        router->UpdateHashRing(*prepared, *hostIdMap);
        return Status::OK();
    }

};

// === WorkerRouter Tests ===

TEST_F(RoutingTest, TestSelectWorkerEmptyRing)
{
    auto router = CreateRouter();
    HostPort worker;
    auto st = router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, worker);
    EXPECT_FALSE(st.IsOk());
}

TEST_F(RoutingTest, TestSelectWorkerReturnsActive)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort worker;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, worker));
    std::string addr = worker.ToString();
    EXPECT_TRUE(addr == "127.0.0.1:1000" || addr == "127.0.0.1:2000");
}

TEST_F(RoutingTest, TestSelectWorkerConsistency)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort w1, w2;
    DS_ASSERT_OK(router->SelectWorker("consistency_key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, w1));
    DS_ASSERT_OK(router->SelectWorker("consistency_key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, w2));
    EXPECT_EQ(w1.ToString(), w2.ToString());
}

TEST_F(RoutingTest, RedirectCandidatesPreservePlacementExclusionsAndHealth)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));
    const HostPort sameNode("127.0.0.1", 1000);
    const HostPort remote("127.0.0.1", 2000);
    const std::vector<HostPort> candidates{ remote, sameNode };
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorkerFromCandidates(
        candidates, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected, remote);
    DS_ASSERT_OK(router->SelectWorkerFromCandidates(
        candidates, client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected, sameNode);
    EXPECT_EQ(router
                  ->SelectWorkerFromCandidates(candidates, client::DataPlacementPolicy::REQUIRED_SAME_NODE, client::WorkerAccessAction::CONTROL,
                                               selected, { sameNode })
                  .GetCode(),
              K_NO_AVAILABLE_WORKER);
    router->UpdateState(remote, K_SCALE_DOWN);
    DS_ASSERT_OK(router->SelectWorkerFromCandidates(
        candidates, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected, sameNode);
}

TEST_F(RoutingTest, InvalidSeedOverrideDoesNotReplaceLastGoodRing)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));
    HostPort before;
    DS_ASSERT_OK(router->SelectWorker("stable-key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, before));

    auto invalidRing = BuildRing();
    auto *seedOverride = invalidRing->mutable_members()->at("127.0.0.1:1000").add_token_seed_overrides();
    seedOverride->set_token_index(invalidRing->tokens_per_member());
    seedOverride->set_token_seed(1);
    EXPECT_EQ(UpdateHashRing(router, invalidRing, BuildHostIdMap()).GetCode(), K_INVALID);

    HostPort after;
    DS_ASSERT_OK(router->SelectWorker("stable-key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, after));
    EXPECT_EQ(after, before);
}

TEST_F(RoutingTest, FailedPreparationDoesNotMutatePreparedTopology)
{
    std::unique_ptr<client::PreparedClusterTopology> prepared;
    auto validRing = BuildRing();
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*validRing), prepared));
    const auto *topology = &prepared->GetTopology();
    const auto tokenIndex = prepared->GetTokenIndex();

    auto invalidRing = BuildRing();
    invalidRing->set_tokens_per_member(0);
    EXPECT_EQ(client::PreparedClusterTopology::Create(std::move(*invalidRing), prepared).GetCode(), K_INVALID);
    EXPECT_EQ(&prepared->GetTopology(), topology);
    EXPECT_EQ(prepared->GetTokenIndex(), tokenIndex);
}

TEST_F(RoutingTest, RejectsTopologyAboveTokenLimit)
{
    auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    ring->set_tokens_per_member(MAX_HASH_RING_TOKENS_PER_MEMBER);
    constexpr size_t memberCount = 640'000 / MAX_HASH_RING_TOKENS_PER_MEMBER + 1;
    for (size_t index = 0; index < memberCount; ++index) {
        auto &member = (*ring->mutable_members())["127.0.0.1:" + std::to_string(index + 1)];
        member.set_state(::datasystem::MembershipPb::ACTIVE);
    }

    std::unique_ptr<client::PreparedClusterTopology> prepared;
    EXPECT_EQ(client::PreparedClusterTopology::Create(std::move(*ring), prepared).GetCode(), K_INVALID);
    EXPECT_EQ(prepared, nullptr);
}

TEST_F(RoutingTest, TestSelectWorkerExclude)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort first;
    DS_ASSERT_OK(router->SelectWorker("exclude_key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, first));

    // Exclude the first result, should get a different one
    HostPort second;
    DS_ASSERT_OK(router->SelectWorker("exclude_key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, second,
                                      { first }));
    EXPECT_NE(first.ToString(), second.ToString());
}

TEST_F(RoutingTest, TestSelectWorkersBatch)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    std::vector<std::string> keys = { "k1", "k2", "k3", "k4", "k5" };
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(router->SelectWorkers(keys, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, groups));

    // All keys should be grouped
    size_t totalKeys = 0;
    for (const auto &g : groups) {
        totalKeys += g.second.size();
    }
    EXPECT_EQ(totalKeys, keys.size());
}

TEST_F(RoutingTest, TestSelectWorkersFailureDoesNotMutateOutput)
{
    auto router = std::make_shared<client::WorkerRouter>(
        "host-a", std::vector<std::shared_ptr<client::IWorkerFilter>>{ std::make_shared<RejectAllFilter>() });
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort existing("127.0.0.1", 3000);
    std::unordered_map<HostPort, std::vector<std::string>> groups{ { existing, { "existing" } } };
    auto rc = router->SelectWorkers({ "k1", "k2" }, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, groups);

    EXPECT_TRUE(rc.IsError());
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups.at(existing), std::vector<std::string>{ "existing" });
}

TEST_F(RoutingTest, TestSelectWorkersEmptyInputClearsOutput)
{
    auto router = CreateRouter();
    HostPort existing("127.0.0.1", 3000);
    std::unordered_map<HostPort, std::vector<std::string>> groups{ { existing, { "existing" } } };

    DS_ASSERT_OK(router->SelectWorkers({}, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, groups));
    EXPECT_TRUE(groups.empty());
}

TEST_F(RoutingTest, TestSelectWorkersUsesSameNodeWorkersWithoutHashTokens)
{
    auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    ring->set_tokens_per_member(1);
    auto &worker = (*ring->mutable_members())["127.0.0.1:1000"];
    worker.set_state(::datasystem::MembershipPb::ACTIVE);
    auto router = CreateRouter("host-a");
    DS_ASSERT_OK(UpdateHashRing(router, ring, BuildHostIdMap()));

    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(router->SelectWorkers({ "k1", "k2" }, client::DataPlacementPolicy::REQUIRED_SAME_NODE, client::WorkerAccessAction::CONTROL, groups));
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups.begin()->first.ToString(), "127.0.0.1:1000");
    EXPECT_EQ(groups.begin()->second.size(), 2u);
}

TEST_F(RoutingTest, TestSameNodePreferred)
{
    auto router = CreateRouter("host-a");
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort worker;
    DS_ASSERT_OK(router->SelectWorker("samenode_key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, worker));
    EXPECT_EQ(worker.ToString(), "127.0.0.1:1000");  // host-a's worker
}

TEST_F(RoutingTest, TestRequiredSameNodeDoesNotFallback)
{
    auto router = CreateRouter("host-without-worker");
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort worker;
    auto rc = router->SelectWorker("key", client::DataPlacementPolicy::REQUIRED_SAME_NODE, client::WorkerAccessAction::CONTROL, worker);
    EXPECT_EQ(rc.GetCode(), K_NO_AVAILABLE_WORKER);
}

TEST_F(RoutingTest, TestPreferredSameNodeFallsBackToMetaOwner)
{
    auto router = CreateRouter("host-without-worker");
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort expected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, expected));
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected, expected);
}

TEST_F(RoutingTest, TestPreferredSameNodeHonorsExclude)
{
    auto hostIdMap = BuildHostIdMap();
    (*hostIdMap)["127.0.0.1:2000"] = "host-a";
    auto router = CreateRouter("host-a");
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), hostIdMap));

    HostPort first;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, first));
    HostPort second;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, second, { first }));
    EXPECT_NE(first, second);
}

TEST_F(RoutingTest, TestHashEqualTokenSelectsTokenOwner)
{
    const std::string address = "127.0.0.1:1000";
    const std::string key = address + "#0";
    auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    ring->set_tokens_per_member(1);
    auto &exactOwner = (*ring->mutable_members())[address];
    exactOwner.set_state(::datasystem::MembershipPb::ACTIVE);

    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, ring, BuildHostIdMap()));
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_EQ(selected.ToString(), address);
}

TEST_F(RoutingTest, TestSameNodePreferredDistributesByKey)
{
    auto hostIdMap = BuildHostIdMap();
    (*hostIdMap)["127.0.0.1:2000"] = "host-a";
    auto router = CreateRouter("host-a");
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), hostIdMap));

    std::set<std::string> selected;
    for (int i = 0; i < 64; ++i) {
        HostPort worker;
        DS_ASSERT_OK(router->SelectWorker("same-node-" + std::to_string(i),
                                         client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, worker));
        selected.emplace(worker.ToString());
    }
    EXPECT_EQ(selected.size(), 2u);
}

TEST_F(RoutingTest, TestEmptyHostIdDoesNotTreatMissingWorkerHostIdAsSameNode)
{
    auto hostIdMap = BuildHostIdMap();
    (*hostIdMap)["127.0.0.1:1000"] = "";
    auto router = CreateRouter("");
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), hostIdMap));

    // When client hostId is empty, PREFERRED_SAME_NODE should not treat
    // workers with empty hostId as same-node. It should behave identically
    // to PREFERRED_META_OWNER (no same-node bias).
    // Verify with multiple keys: every key should return the same worker
    // regardless of strategy.
    for (int i = 0; i < 100; ++i) {
        std::string key = "empty-hostid-key-" + std::to_string(i);
        HostPort hashOwner;
        DS_ASSERT_OK(router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, hashOwner));

        HostPort selected;
        DS_ASSERT_OK(router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_SAME_NODE, client::WorkerAccessAction::CONTROL, selected));
        EXPECT_EQ(selected, hashOwner)
            << "Key " << key << ": PREFERRED_SAME_NODE diverged from PREFERRED_META_OWNER";
    }
}

TEST_F(RoutingTest, TestStateFilterRejectsLeavingWorker)
{
    auto router = CreateRouter();
    auto ring = BuildRing();
    DS_ASSERT_OK(UpdateHashRing(router, ring, BuildHostIdMap()));

    const std::string key = "leaving-owner";
    HostPort original;
    DS_ASSERT_OK(router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, original));

    auto updatedRing = BuildRing();
    (*updatedRing->mutable_members())[original.ToString()].set_state(::datasystem::MembershipPb::LEAVING);
    DS_ASSERT_OK(UpdateHashRing(router, updatedRing, BuildHostIdMap()));

    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected));
    EXPECT_NE(selected, original);
}

TEST_F(RoutingTest, TestStateFilterRejectsWhenRouterIsMissing)
{
    client::StateFilter filter(nullptr);
    EXPECT_FALSE(filter.IsAvailable(HostPort("127.0.0.1", 1000), client::WorkerAccessAction::CONTROL));
}

TEST_F(RoutingTest, TestConcurrentSelectAndHashRingUpdate)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));
    std::atomic<bool> failed{ false };
    std::thread updater([&] {
        for (int i = 0; i < 200; ++i) {
            if (UpdateHashRing(router, BuildRing(), BuildHostIdMap()).IsError()) {
                failed.store(true);
            }
        }
    });
    std::vector<std::thread> readers;
    for (int threadIndex = 0; threadIndex < 4; ++threadIndex) {
        readers.emplace_back([&, threadIndex] {
            for (int i = 0; i < 500; ++i) {
                HostPort selected;
                auto rc = router->SelectWorker("key-" + std::to_string(threadIndex) + "-" + std::to_string(i),
                                               client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected);
                if (rc.IsError() || selected.Empty()) {
                    failed.store(true);
                }
            }
        });
    }
    updater.join();
    for (auto &reader : readers) {
        reader.join();
    }
    EXPECT_FALSE(failed.load());
}

TEST_F(RoutingTest, TestGetRingState)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort w("127.0.0.1", 1000);
    EXPECT_EQ(router->GetRingState(w), client::WorkerRingState::ACTIVE);

    HostPort unknown("1.2.3.4", 9999);
    EXPECT_EQ(router->GetRingState(unknown), client::WorkerRingState::UNKNOWN);
}

// === BrokenFilter Tests ===

// Reaches the debounce threshold (BrokenFilter::EVICT_CONSECUTIVE_FAILURES) so the worker is
// actually marked broken. A single disconnect must NOT evict -- that was the code=37 cascade.
constexpr int EVICTION_THRESHOLD = 100;
void MarkWorkerBroken(client::BrokenFilter &filter, const HostPort &addr)
{
    for (int i = 0; i < EVICTION_THRESHOLD; ++i) {
        filter.OnWorkerStateChange(addr, K_CLIENT_WORKER_DISCONNECT);
    }
}
void MarkWorkerBroken(client::WorkerRouter &router, const HostPort &addr)
{
    for (int i = 0; i < EVICTION_THRESHOLD; ++i) {
        router.UpdateState(addr, K_CLIENT_WORKER_DISCONNECT);
    }
}

TEST_F(RoutingTest, TestBrokenFilter)
{
    client::BrokenFilter filter;

    HostPort addr("127.0.0.1", 1000);
    EXPECT_TRUE(filter.IsAvailable(addr, client::WorkerAccessAction::CONTROL));

    MarkWorkerBroken(filter, addr);
    EXPECT_FALSE(filter.IsAvailable(addr, client::WorkerAccessAction::CONTROL));

    // Other status should be ignored
    filter.OnWorkerStateChange(addr, K_RUNTIME_ERROR);
    EXPECT_FALSE(filter.IsAvailable(addr, client::WorkerAccessAction::CONTROL));  // Still broken from the disconnect burst
}

TEST_F(RoutingTest, TestBrokenFilterIgnoresOtherWorkers)
{
    client::BrokenFilter filter;

    HostPort a("127.0.0.1", 1000);
    HostPort b("127.0.0.1", 2000);

    MarkWorkerBroken(filter, a);
    EXPECT_FALSE(filter.IsAvailable(a, client::WorkerAccessAction::CONTROL));
    EXPECT_TRUE(filter.IsAvailable(b, client::WorkerAccessAction::CONTROL));  // b unaffected
}

TEST_F(RoutingTest, TestBrokenFilterClearsOnHashRingUpdate)
{
    client::BrokenFilter filter;
    HostPort addr("127.0.0.1", 1000);

    MarkWorkerBroken(filter, addr);
    EXPECT_FALSE(filter.IsAvailable(addr, client::WorkerAccessAction::CONTROL));

    filter.OnHashRingUpdated(*BuildRing());
    EXPECT_TRUE(filter.IsAvailable(addr, client::WorkerAccessAction::CONTROL));
}

TEST_F(RoutingTest, TestBrokenFilterConcurrentUpdatesAreNotLost)
{
    client::BrokenFilter filter;
    constexpr int workerCount = 16;
    std::vector<std::thread> threads;
    threads.reserve(workerCount);
    for (int i = 0; i < workerCount; ++i) {
        threads.emplace_back([&filter, i] {
            MarkWorkerBroken(filter, HostPort("127.0.0.1", 1000 + i));
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    for (int i = 0; i < workerCount; ++i) {
        EXPECT_FALSE(filter.IsAvailable(HostPort("127.0.0.1", 1000 + i), client::WorkerAccessAction::CONTROL));
    }
}

TEST_F(RoutingTest, TestBrokenFilterIntegrationWithRouter)
{
    auto brokenFilter = std::make_shared<client::BrokenFilter>();
    auto router = std::make_shared<client::WorkerRouter>(
        "host-a", std::vector<std::shared_ptr<client::IWorkerFilter>>{brokenFilter});
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    HostPort first;
    DS_ASSERT_OK(router->SelectWorker("broken_key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, first));

    // Mark first worker as broken (reach the debounce threshold)
    MarkWorkerBroken(*router, first);

    // Subsequent SelectWorker should skip broken worker
    HostPort second;
    DS_ASSERT_OK(router->SelectWorker("broken_key", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, second));
    EXPECT_NE(first.ToString(), second.ToString());
}

TEST_F(RoutingTest, U7RoutesWithFiveThousandWorkerSnapshot)
{
    constexpr size_t workerCount = 5'000;
    constexpr size_t keyCount = 2'048;
    constexpr int portBase = 10'000;
    auto ring = std::make_shared<ClusterTopologyPb>();
    ring->set_tokens_per_member(1);
    auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
    for (size_t i = 0; i < workerCount; ++i) {
        const std::string address = "127.0.0.1:" + std::to_string(portBase + i);
        auto &worker = (*ring->mutable_members())[address];
        worker.set_state(MembershipPb::ACTIVE);
        (*hostIdMap)[address] = "scale-host-" + std::to_string(i);
    }

    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, ring, hostIdMap));
    ASSERT_EQ(router->GetAvailableWorkers().size(), workerCount);
    const std::vector<HostPort> redirectCandidates{
        HostPort("127.0.0.1", portBase + static_cast<int>(workerCount) - 3),
        HostPort("127.0.0.1", portBase + static_cast<int>(workerCount) - 2),
        HostPort("127.0.0.1", portBase + static_cast<int>(workerCount) - 1)
    };
    HostPort redirectWorker;
    DS_ASSERT_OK(router->SelectWorkerFromCandidates(
        redirectCandidates, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, redirectWorker));
    EXPECT_NE(std::find(redirectCandidates.begin(), redirectCandidates.end(), redirectWorker),
              redirectCandidates.end());

    std::vector<std::string> keys;
    keys.reserve(keyCount);
    for (size_t i = 0; i < keyCount; ++i) {
        keys.emplace_back("u7-scale-key-" + std::to_string(i));
    }
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(router->SelectWorkers(keys, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, groups));

    size_t selectedKeyCount = 0;
    for (const auto &group : groups) {
        selectedKeyCount += group.second.size();
        EXPECT_GE(group.first.Port(), portBase);
        EXPECT_LT(group.first.Port(), portBase + static_cast<int>(workerCount));
    }
    EXPECT_EQ(selectedKeyCount, keyCount);
}

TEST_F(RoutingTest, U7BatchSelectionNeverMixesConcurrentSnapshots)
{
    constexpr int generationAPortBase = 11'000;
    constexpr int generationBPortBase = 21'000;
    auto buildGeneration = [=](uint32_t tokenSeed) {
        auto ring = std::make_shared<ClusterTopologyPb>();
        ring->set_tokens_per_member(1);
        for (int portBase : { generationAPortBase, generationBPortBase }) {
            for (int i = 0; i < 2; ++i) {
                auto &worker = (*ring->mutable_members())["127.0.0.1:" + std::to_string(portBase + i)];
                worker.set_state(MembershipPb::ACTIVE);
                if (tokenSeed != 0) {
                    auto *seedOverride = worker.add_token_seed_overrides();
                    seedOverride->set_token_index(0);
                    seedOverride->set_token_seed(tokenSeed);
                }
            }
        }
        return ring;
    };
    auto buildHostIdMap = [=] {
        auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
        for (int portBase : { generationAPortBase, generationBPortBase }) {
            for (int i = 0; i < 2; ++i) {
                (*hostIdMap)["127.0.0.1:" + std::to_string(portBase + i)] = "snapshot-host";
            }
        }
        return hostIdMap;
    };

    auto ringA = buildGeneration(0);
    auto ringB = buildGeneration(1);
    auto hostIdMap = buildHostIdMap();
    auto router = CreateRouter();
    std::unique_ptr<client::PreparedClusterTopology> preparedA;
    std::unique_ptr<client::PreparedClusterTopology> preparedB;
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*ringA), preparedA));
    DS_ASSERT_OK(client::PreparedClusterTopology::Create(std::move(*ringB), preparedB));

    std::vector<std::string> keys;
    for (size_t i = 0; i < 512; ++i) {
        keys.emplace_back("u7-snapshot-key-" + std::to_string(i));
    }
    std::unordered_map<HostPort, std::vector<std::string>> expectedA;
    router->UpdateHashRing(*preparedA, *hostIdMap);
    DS_ASSERT_OK(router->SelectWorkers(keys, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, expectedA));
    std::unordered_map<HostPort, std::vector<std::string>> expectedB;
    router->UpdateHashRing(*preparedB, *hostIdMap);
    DS_ASSERT_OK(router->SelectWorkers(keys, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, expectedB));
    ASSERT_NE(expectedA, expectedB);

    std::atomic<bool> stop{ false };
    std::thread updater([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            router->UpdateHashRing(*preparedB, *hostIdMap);
            router->UpdateHashRing(*preparedA, *hostIdMap);
        }
    });

    Status selectionStatus = Status::OK();
    bool snapshotsConsistent = true;
    for (size_t iteration = 0; iteration < 200; ++iteration) {
        std::unordered_map<HostPort, std::vector<std::string>> groups;
        selectionStatus =
            router->SelectWorkers(keys, client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, groups);
        if (selectionStatus.IsError() || (groups != expectedA && groups != expectedB)) {
            snapshotsConsistent = false;
            break;
        }
    }
    stop.store(true, std::memory_order_relaxed);
    updater.join();
    DS_ASSERT_OK(selectionStatus);
    EXPECT_TRUE(snapshotsConsistent);
}

// Characterizes the code=37 ("All workers filtered or excluded") mechanism: when every ring
// candidate is marked broken, WorkerRouter exhausts the ring and returns K_NO_AVAILABLE_WORKER.
// This is the user-observable failure behind the field reports (8 MB / high-GET-pressure).
//
// Historically (083cc75bd4) a transient retryable RPC error reached `routing->UpdateState(...,
// K_CLIENT_WORKER_DISCONNECT)` and pushed workers into BrokenFilter for BROKEN_TTL, so under load
// the whole ring could be marked broken within seconds. The fix narrows global eviction to
// genuine peer failures (see IsRoutingEvictionFailure + its classification test below); this
// test now exercises the remaining legitimate eviction path (K_CLIENT_WORKER_DISCONNECT) to lock
// the exhaustion contract in place.
TEST_F(RoutingTest, AllWorkersBrokenExhaustsRingAndReturnsCode37)
{
    auto router = CreateRouter();
    DS_ASSERT_OK(UpdateHashRing(router, BuildRing(), BuildHostIdMap()));

    // Sanity: routing succeeds before any worker is marked broken.
    HostPort healthy;
    DS_ASSERT_OK(router->SelectWorker("k", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, healthy));

    const std::vector<HostPort> allWorkers{ HostPort("127.0.0.1", 1000), HostPort("127.0.0.1", 2000) };
    // Mark every candidate broken via the genuine-disconnect path that HandleSetRouteFailure
    // still feeds into BrokenFilter (object_client_impl.cpp, gated by IsRoutingEvictionFailure).
    for (const auto &w : allWorkers) {
        MarkWorkerBroken(*router, w);
    }

    // Every candidate filtered by BrokenFilter -> K_NO_AVAILABLE_WORKER (code 37).
    HostPort selected;
    auto rc = router->SelectWorker("k", client::DataPlacementPolicy::PREFERRED_META_OWNER, client::WorkerAccessAction::CONTROL, selected);
    EXPECT_EQ(rc.GetCode(), K_NO_AVAILABLE_WORKER);
}

// Guards the code=37 fix at the classification layer: only genuine peer/connection failures
// (K_CLIENT_WORKER_DISCONNECT, K_RPC_PEER_DEAD) may evict a worker from the global routing
// table. Every transient retryable error must classify as non-evicting -- evicting on them is
// the 083cc75bd4 regression that collapsed routing capacity under load.
TEST_F(RoutingTest, IsRoutingEvictionFailureExcludesTransientRetryableErrors)
{
    EXPECT_TRUE(IsRoutingEvictionFailure(Status(K_CLIENT_WORKER_DISCONNECT, "disconnect")));
    EXPECT_TRUE(IsRoutingEvictionFailure(Status(K_RPC_PEER_DEAD, "peer dead")));
    // Transient retryable errors: a slow/busy peer, must NOT evict globally.
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_RPC_DEADLINE_EXCEEDED, "deadline")));
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_RPC_NETWORK_BLIP, "blip")));
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_RPC_UNAVAILABLE, "unavailable")));
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_URMA_WAIT_TIMEOUT, "urma wait")));
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_URMA_NEED_CONNECT, "urma need connect")));
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_RPC_CANCELLED, "cancelled")));
    EXPECT_FALSE(IsRoutingEvictionFailure(Status(K_RUNTIME_ERROR, "runtime")));
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

// A passive health summary (plain business response) only feeds the registry routing snapshot. SET selection must
// ignore that unverified hint and keep the affinity worker; GET keeps consuming the snapshot, which is what proves
// the hint really reaches the scheduler.
TEST_F(RoutingTest, WriteSelectionIgnoresPassivePortHealthHint)
{
    // WorkerRouter::UpdateHashRing reconciles the UB health registry from its own ring, so the ring fed to the router
    // must carry the same incarnation ids, otherwise the registry drops every summary as an unknown incarnation.
    auto registryRing = BuildRing();
    (*registryRing->mutable_members())["127.0.0.1:1000"].set_id("inc-1000");
    (*registryRing->mutable_members())["127.0.0.1:2000"].set_id("inc-2000");
    auto registry = std::make_shared<client::WorkerUbHealthRegistry>();
    registry->ReconcileTopology(*registryRing);

    client::ClientReadBandwidthScheduler::Config config;
    config.enabled = true;
    config.clientSalt = "router-purpose-test";
    config.latencyCandidateRefreshMs = 60000;
    config.latencyClientStaleMs = 60000;
    config.latencyWorkerRecycleCheckMs = 60000;
    config.latencyWorkerInactiveRecycleMs = 60000;
    config.latencyStarvationProtectMs = 60000;
    auto scheduler = std::make_shared<client::ClientReadBandwidthScheduler>(config);
    auto router = std::make_shared<client::WorkerRouter>(
        "host-a", registry, std::vector<std::shared_ptr<client::IWorkerFilter>>{}, scheduler);
    ASSERT_TRUE(
        UpdateHashRing(router, std::make_shared<::datasystem::ClusterTopologyPb>(*registryRing), BuildHostIdMap())
            .IsOk());

    const HostPort workerA("127.0.0.1", 1000);
    const HostPort workerB("127.0.0.1", 2000);
    scheduler->Observe(workerA, 1'000'000, 1'200'000, 1, "test");
    scheduler->Observe(workerB, 1'000'000, 1'200'000, 1, "test");

    const std::string key = "passive-hint-key";
    HostPort affinity;
    ASSERT_TRUE(
        router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                             client::WorkerAccessAction::SET, affinity)
            .IsOk());

    UbHealthSummary passive;
    passive.worker = affinity;
    passive.incarnation = affinity.Port() == 1000 ? "inc-1000" : "inc-2000";
    passive.epoch = 1;
    passive.portHealth = UbPortHealthSummary{ true, 4, 4, 1, false };
    ASSERT_TRUE(registry->ApplySummary(passive, passive.incarnation));

    auto snapshot = registry->GetRoutingSnapshot();
    ASSERT_NE(snapshot, nullptr);
    ASSERT_EQ(snapshot->workers.count(affinity), 1U);
    uint64_t taskId = 0;
    EXPECT_FALSE(scheduler->ShouldKeepAffinity(affinity, key, snapshot, taskId));

    HostPort writePick;
    ASSERT_TRUE(
        router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                             client::WorkerAccessAction::SET, writePick)
            .IsOk());
    EXPECT_EQ(writePick, affinity);

    HostPort readPick;
    ASSERT_TRUE(
        router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                             client::WorkerAccessAction::GET, readPick)
            .IsOk());
    EXPECT_NE(readPick, affinity);
}

}  // namespace ut
}  // namespace datasystem
