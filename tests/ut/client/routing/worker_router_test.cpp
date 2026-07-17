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

#include "datasystem/client/routing/broken_filter.h"
#include "datasystem/client/routing/i_worker_filter.h"
#include "datasystem/client/routing/routing.h"
#include "datasystem/client/routing/state_filter.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {

class RejectAllFilter : public client::IWorkerFilter {
public:
    ~RejectAllFilter() override = default;

    bool IsAvailable(const HostPort &) const override
    {
        return false;
    }
};

class RoutingTest : public CommonTest {
protected:
    std::shared_ptr<::datasystem::ClusterTopologyPb> BuildRing()
    {
        auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
        auto &wA = (*ring->mutable_members())["127.0.0.1:1000"];
        wA.set_state(::datasystem::MembershipPb::ACTIVE);
        wA.add_tokens(100u);
        wA.add_tokens(200u);
        auto &wB = (*ring->mutable_members())["127.0.0.1:2000"];
        wB.set_state(::datasystem::MembershipPb::ACTIVE);
        wB.add_tokens(300u);
        wB.add_tokens(400u);
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

TEST_F(RoutingTest, TestSelectWorkersFailureDoesNotMutateOutput)
{
    auto router = std::make_shared<client::WorkerRouter>(
        "host-a", std::vector<std::shared_ptr<client::IWorkerFilter>>{ std::make_shared<RejectAllFilter>() });
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort existing("127.0.0.1", 3000);
    std::unordered_map<HostPort, std::vector<std::string>> groups{ { existing, { "existing" } } };
    auto rc = router->SelectWorkers({ "k1", "k2" }, client::SelectStrategy::HASH_RING_AFFINITY, groups);

    EXPECT_TRUE(rc.IsError());
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups.at(existing), std::vector<std::string>{ "existing" });
}

TEST_F(RoutingTest, TestSelectWorkersEmptyInputClearsOutput)
{
    auto router = CreateRouter();
    HostPort existing("127.0.0.1", 3000);
    std::unordered_map<HostPort, std::vector<std::string>> groups{ { existing, { "existing" } } };

    DS_ASSERT_OK(router->SelectWorkers({}, client::SelectStrategy::HASH_RING_AFFINITY, groups));
    EXPECT_TRUE(groups.empty());
}

TEST_F(RoutingTest, TestSelectWorkersUsesSameNodeWorkersWithoutHashTokens)
{
    auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    auto &worker = (*ring->mutable_members())["127.0.0.1:1000"];
    worker.set_state(::datasystem::MembershipPb::ACTIVE);
    auto router = CreateRouter("host-a");
    router->UpdateHashRing(ring, BuildHostIdMap());

    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(router->SelectWorkers({ "k1", "k2" }, client::DataPlacementPolicy::REQUIRED_SAME_NODE, groups));
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups.begin()->first.ToString(), "127.0.0.1:1000");
    EXPECT_EQ(groups.begin()->second.size(), 2u);
}

TEST_F(RoutingTest, TestSameNodePreferred)
{
    auto router = CreateRouter("host-a");
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort worker;
    DS_ASSERT_OK(router->SelectWorker("samenode_key", client::SelectStrategy::SAME_NODE_PREFERRED, worker));
    EXPECT_EQ(worker.ToString(), "127.0.0.1:1000");  // host-a's worker
}

TEST_F(RoutingTest, TestRequiredSameNodeDoesNotFallback)
{
    auto router = CreateRouter("host-without-worker");
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort worker;
    auto rc = router->SelectWorker("key", client::DataPlacementPolicy::REQUIRED_SAME_NODE, worker);
    EXPECT_EQ(rc.GetCode(), K_NO_AVAILABLE_WORKER);
}

TEST_F(RoutingTest, TestPreferredSameNodeFallsBackToMetaOwner)
{
    auto router = CreateRouter("host-without-worker");
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());

    HostPort expected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, expected));
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, selected));
    EXPECT_EQ(selected, expected);
}

TEST_F(RoutingTest, TestPreferredSameNodeHonorsExclude)
{
    auto hostIdMap = BuildHostIdMap();
    (*hostIdMap)["127.0.0.1:2000"] = "host-a";
    auto router = CreateRouter("host-a");
    router->UpdateHashRing(BuildRing(), hostIdMap);

    HostPort first;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, first));
    HostPort second;
    DS_ASSERT_OK(router->SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, second, { first }));
    EXPECT_NE(first, second);
}

TEST_F(RoutingTest, TestHashEqualTokenSelectsTokenOwner)
{
    constexpr int MAX_KEY_SEARCH_ATTEMPTS = 100;
    std::string key = "exact-token";
    uint32_t keyHash = MurmurHash3_32(key);
    for (int i = 0; keyHash == std::numeric_limits<uint32_t>::max() && i < MAX_KEY_SEARCH_ATTEMPTS; ++i) {
        key = "exact-token-" + std::to_string(i);
        keyHash = MurmurHash3_32(key);
    }
    ASSERT_NE(keyHash, std::numeric_limits<uint32_t>::max());
    auto ring = std::make_shared<::datasystem::ClusterTopologyPb>();
    auto &exactOwner = (*ring->mutable_members())["127.0.0.1:1000"];
    exactOwner.set_state(::datasystem::MembershipPb::ACTIVE);
    exactOwner.add_tokens(keyHash);
    auto &nextOwner = (*ring->mutable_members())["127.0.0.1:2000"];
    nextOwner.set_state(::datasystem::MembershipPb::ACTIVE);
    nextOwner.add_tokens(keyHash + 1);

    auto router = CreateRouter();
    router->UpdateHashRing(ring, BuildHostIdMap());
    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker(key, client::DataPlacementPolicy::PREFERRED_META_OWNER, selected));
    EXPECT_EQ(selected.ToString(), "127.0.0.1:1000");
}

TEST_F(RoutingTest, TestSameNodePreferredDistributesByKey)
{
    auto hostIdMap = BuildHostIdMap();
    (*hostIdMap)["127.0.0.1:2000"] = "host-a";
    auto router = CreateRouter("host-a");
    router->UpdateHashRing(BuildRing(), hostIdMap);

    std::set<std::string> selected;
    for (int i = 0; i < 64; ++i) {
        HostPort worker;
        DS_ASSERT_OK(router->SelectWorker("same-node-" + std::to_string(i),
                                         client::SelectStrategy::SAME_NODE_PREFERRED, worker));
        selected.emplace(worker.ToString());
    }
    EXPECT_EQ(selected.size(), 2u);
}

TEST_F(RoutingTest, TestEmptyHostIdDoesNotTreatMissingWorkerHostIdAsSameNode)
{
    auto hostIdMap = BuildHostIdMap();
    (*hostIdMap)["127.0.0.1:1000"] = "";
    auto router = CreateRouter("");
    router->UpdateHashRing(BuildRing(), hostIdMap);

    // When client hostId is empty, SAME_NODE_PREFERRED should not treat
    // workers with empty hostId as same-node. It should behave identically
    // to HASH_RING_AFFINITY (no same-node bias).
    // Verify with multiple keys: every key should return the same worker
    // regardless of strategy.
    for (int i = 0; i < 100; ++i) {
        std::string key = "empty-hostid-key-" + std::to_string(i);
        HostPort hashOwner;
        DS_ASSERT_OK(router->SelectWorker(key, client::SelectStrategy::HASH_RING_AFFINITY, hashOwner));

        HostPort selected;
        DS_ASSERT_OK(router->SelectWorker(key, client::SelectStrategy::SAME_NODE_PREFERRED, selected));
        EXPECT_EQ(selected, hashOwner)
            << "Key " << key << ": SAME_NODE_PREFERRED diverged from HASH_RING_AFFINITY";
    }
}

TEST_F(RoutingTest, TestStateFilterRejectsLeavingWorker)
{
    auto router = CreateRouter();
    auto ring = BuildRing();
    router->UpdateHashRing(ring, BuildHostIdMap());

    const std::string key = "leaving-owner";
    HostPort original;
    DS_ASSERT_OK(router->SelectWorker(key, client::SelectStrategy::HASH_RING_AFFINITY, original));

    auto updatedRing = BuildRing();
    (*updatedRing->mutable_members())[original.ToString()].set_state(::datasystem::MembershipPb::LEAVING);
    router->UpdateHashRing(updatedRing, BuildHostIdMap());

    HostPort selected;
    DS_ASSERT_OK(router->SelectWorker(key, client::SelectStrategy::HASH_RING_AFFINITY, selected));
    EXPECT_NE(selected, original);
}

TEST_F(RoutingTest, TestStateFilterRejectsWhenRouterIsMissing)
{
    client::StateFilter filter(nullptr);
    EXPECT_FALSE(filter.IsAvailable(HostPort("127.0.0.1", 1000)));
}

TEST_F(RoutingTest, TestConcurrentSelectAndHashRingUpdate)
{
    auto router = CreateRouter();
    router->UpdateHashRing(BuildRing(), BuildHostIdMap());
    std::atomic<bool> failed{ false };
    std::thread updater([&] {
        for (int i = 0; i < 200; ++i) {
            router->UpdateHashRing(BuildRing(), BuildHostIdMap());
        }
    });
    std::vector<std::thread> readers;
    for (int threadIndex = 0; threadIndex < 4; ++threadIndex) {
        readers.emplace_back([&, threadIndex] {
            for (int i = 0; i < 500; ++i) {
                HostPort selected;
                auto rc = router->SelectWorker("key-" + std::to_string(threadIndex) + "-" + std::to_string(i),
                                               client::DataPlacementPolicy::PREFERRED_META_OWNER, selected);
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

TEST_F(RoutingTest, TestBrokenFilterConcurrentUpdatesAreNotLost)
{
    client::BrokenFilter filter;
    constexpr int workerCount = 64;
    std::vector<std::thread> threads;
    threads.reserve(workerCount);
    for (int i = 0; i < workerCount; ++i) {
        threads.emplace_back([&filter, i] {
            filter.OnWorkerStateChange(HostPort("127.0.0.1", 1000 + i), K_CLIENT_WORKER_DISCONNECT);
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    for (int i = 0; i < workerCount; ++i) {
        EXPECT_FALSE(filter.IsAvailable(HostPort("127.0.0.1", 1000 + i)));
    }
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

TEST_F(RoutingTest, U7RoutesWithFiveThousandWorkerSnapshot)
{
    constexpr size_t workerCount = 5'000;
    constexpr size_t keyCount = 2'048;
    constexpr int portBase = 10'000;
    auto ring = std::make_shared<ClusterTopologyPb>();
    auto hostIdMap = std::make_shared<std::unordered_map<std::string, std::string>>();
    for (size_t i = 0; i < workerCount; ++i) {
        const std::string address = "127.0.0.1:" + std::to_string(portBase + i);
        auto &worker = (*ring->mutable_members())[address];
        worker.set_state(MembershipPb::ACTIVE);
        worker.add_tokens(static_cast<uint32_t>(
            (static_cast<uint64_t>(i) * std::numeric_limits<uint32_t>::max()) / workerCount));
        (*hostIdMap)[address] = "scale-host-" + std::to_string(i);
    }

    auto router = CreateRouter();
    router->UpdateHashRing(ring, hostIdMap);
    ASSERT_EQ(router->GetAvailableWorkers().size(), workerCount);

    std::vector<std::string> keys;
    keys.reserve(keyCount);
    for (size_t i = 0; i < keyCount; ++i) {
        keys.emplace_back("u7-scale-key-" + std::to_string(i));
    }
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(router->SelectWorkers(keys, client::SelectStrategy::HASH_RING_AFFINITY, groups));

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
    auto buildGeneration = [=](int tokenOwnerPortBase) {
        auto ring = std::make_shared<ClusterTopologyPb>();
        for (int portBase : { generationAPortBase, generationBPortBase }) {
            for (int i = 0; i < 2; ++i) {
                auto &worker = (*ring->mutable_members())["127.0.0.1:" + std::to_string(portBase + i)];
                worker.set_state(MembershipPb::ACTIVE);
                if (portBase == tokenOwnerPortBase) {
                    worker.add_tokens(i == 0 ? 0u : std::numeric_limits<uint32_t>::max() / 2);
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

    auto ringA = buildGeneration(generationAPortBase);
    auto ringB = buildGeneration(generationBPortBase);
    auto hostIdMap = buildHostIdMap();
    auto router = CreateRouter();
    router->UpdateHashRing(ringA, hostIdMap);

    std::vector<std::string> keys;
    for (size_t i = 0; i < 512; ++i) {
        keys.emplace_back("u7-snapshot-key-" + std::to_string(i));
    }

    std::atomic<bool> stop{ false };
    std::thread updater([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            router->UpdateHashRing(ringB, hostIdMap);
            router->UpdateHashRing(ringA, hostIdMap);
        }
    });

    Status selectionStatus = Status::OK();
    bool snapshotsConsistent = true;
    for (size_t iteration = 0; iteration < 200; ++iteration) {
        std::unordered_map<HostPort, std::vector<std::string>> groups;
        selectionStatus =
            router->SelectWorkers(keys, client::SelectStrategy::HASH_RING_AFFINITY, groups);
        if (selectionStatus.IsError() || groups.size() != 2u) {
            snapshotsConsistent = false;
            break;
        }
        bool allFromA = true;
        bool allFromB = true;
        for (const auto &group : groups) {
            allFromA = allFromA && group.first.Port() >= generationAPortBase
                       && group.first.Port() < generationAPortBase + 2;
            allFromB = allFromB && group.first.Port() >= generationBPortBase
                       && group.first.Port() < generationBPortBase + 2;
        }
        if (!allFromA && !allFromB) {
            snapshotsConsistent = false;
            break;
        }
    }
    stop.store(true, std::memory_order_relaxed);
    updater.join();
    DS_ASSERT_OK(selectionStatus);
    EXPECT_TRUE(snapshotsConsistent);
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
