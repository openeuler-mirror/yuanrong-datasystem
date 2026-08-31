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
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/client/routing/hash_ring_refresher.h"
#include "datasystem/client/routing/routing.h"
#include "datasystem/client/routing/routing_rpc_client.h"
#include "datasystem/client/routing/worker_router.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "ut/common.h"

namespace datasystem {
namespace ut {
// Matches BrokenFilter::EVICT_CONSECUTIVE_FAILURES (a worker is evicted only after this many
// consecutive K_CLIENT_WORKER_DISCONNECT signals).
constexpr int EVICTION_THRESHOLD = 100;
namespace {
bool WaitForCondition(const std::function<bool()> &predicate, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}
}  // namespace

class RoutingFacadeTest : public CommonTest {
protected:
    static void FillRing(::datasystem::ClusterTopologyPb &ring, std::unordered_map<std::string, std::string> &hostIdMap)
    {
        ring.set_tokens_per_member(1);
        auto &worker = (*ring.mutable_members())["127.0.0.1:1000"];
        worker.set_state(::datasystem::MembershipPb::ACTIVE);
        hostIdMap["127.0.0.1:1000"] = "host-a";
    }
};

class RoutingRpcClientTest : public CommonTest {
};

TEST_F(RoutingFacadeTest, TestInitFetchesRingAndStartsFacade)
{
    auto router = std::make_shared<client::WorkerRouter>("");
    std::atomic<int> fetchCount{ 0 };
    auto fetch = [&fetchCount](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                               uint64_t &newVersion, bool &changed,
                               std::unordered_map<std::string, std::string> &hostIdMap) {
        fetchCount.fetch_add(1);
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = true;
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher, 60'000);

    DS_ASSERT_OK(routing.Init("host-a", HostPort("127.0.0.1", 1000)));
    EXPECT_GE(fetchCount.load(), 1);

    HostPort worker;
    DS_ASSERT_OK(routing.SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, worker));
    EXPECT_EQ(worker.ToString(), "127.0.0.1:1000");
    routing.Shutdown();
}

TEST_F(RoutingFacadeTest, TestExplicitHostIdPreservesLocalityWithRemoteInitialWorker)
{
    const HostPort localWorker("127.0.0.1", 1000);
    const HostPort remoteWorker("127.0.0.1", 2000);
    auto router = std::make_shared<client::WorkerRouter>("");
    auto fetch = [&localWorker, &remoteWorker](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring,
                                               std::string &, uint64_t &newVersion, bool &changed,
                                               std::unordered_map<std::string, std::string> &hostIdMap) {
        ring.set_tokens_per_member(1);
        auto &local = (*ring.mutable_members())[localWorker.ToString()];
        local.set_state(::datasystem::MembershipPb::ACTIVE);
        auto &remote = (*ring.mutable_members())[remoteWorker.ToString()];
        remote.set_state(::datasystem::MembershipPb::ACTIVE);
        hostIdMap[localWorker.ToString()] = "host-local";
        hostIdMap[remoteWorker.ToString()] = "host-remote";
        newVersion = 1;
        changed = true;
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher, 60'000);

    DS_ASSERT_OK(routing.Init("host-local", remoteWorker, false));
    HostPort selected;
    DS_ASSERT_OK(routing.SelectWorker("key", client::DataPlacementPolicy::PREFERRED_SAME_NODE, selected));
    EXPECT_EQ(selected, localWorker);

    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(routing.SelectWorkers({ "key-a", "key-b" }, client::DataPlacementPolicy::PREFERRED_SAME_NODE, groups));
    ASSERT_EQ(groups.size(), 1);
    EXPECT_EQ(groups.begin()->first, localWorker);
    routing.Shutdown();
}

TEST_F(RoutingFacadeTest, TestCallsBeforeInitFail)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher);

    HostPort worker;
    EXPECT_EQ(routing.SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, worker).GetCode(),
              K_NOT_READY);

    std::unordered_map<HostPort, std::vector<std::string>> groups;
    EXPECT_EQ(routing.SelectWorkers({ "key" }, client::DataPlacementPolicy::PREFERRED_META_OWNER, groups).GetCode(),
              K_NOT_READY);
}

// Defect B (fix-brpc-latency plan): the SDK's own host_id must be resolvable from a LATER ring
// change when the first ring fetch lands before the bound worker's keepalive has populated
// host_id_map. Previously hostIdResolutionAttempted_ was consumed (exchange(true)) on the first
// changed ring fetch regardless of whether the bound worker was in host_id_map, so a missing
// host_id on first fetch permanently disabled same-node affinity for the SDK's lifetime. Now the
// flag is set only on a successful resolution, so a later ring change that finally carries the
// bound worker's host_id must adopt it and restore same-node routing.
TEST_F(RoutingFacadeTest, TestHostIdResolvesOnLaterRingChangeWhenFirstFetchMissedIt)
{
    const HostPort localWorker("127.0.0.1", 1000);
    const HostPort remoteWorker("127.0.0.1", 2000);
    auto router = std::make_shared<client::WorkerRouter>("");
    std::atomic<int> fetchCount{ 0 };
    auto fetch = [&localWorker, &remoteWorker, &fetchCount](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring,
                                                            std::string &, uint64_t &newVersion, bool &changed,
                                                            std::unordered_map<std::string, std::string> &hostIdMap) {
        const int n = fetchCount.fetch_add(1) + 1;
        ring.set_tokens_per_member(1);
        auto &local = (*ring.mutable_members())[localWorker.ToString()];
        local.set_state(::datasystem::MembershipPb::ACTIVE);
        auto &remote = (*ring.mutable_members())[remoteWorker.ToString()];
        remote.set_state(::datasystem::MembershipPb::ACTIVE);
        if (n == 1) {
            // First fetch: bound (local) worker is ACTIVE in the ring but its host_id is absent from
            // host_id_map (keepalive propagation lag). Same-node affinity cannot resolve yet.
            hostIdMap.clear();
        } else {
            // Later fetch: host_id_map now carries the bound worker's host_id.
            hostIdMap[localWorker.ToString()] = "host-local";
            hostIdMap[remoteWorker.ToString()] = "host-remote";
        }
        newVersion = static_cast<uint64_t>(n);
        changed = true;
        return Status::OK();
    };
    auto resolveHostId = [&localWorker, &router](uint64_t, const ::datasystem::ClusterTopologyPb &,
                                                 const std::unordered_map<std::string, std::string> &hostIdMap) {
        const auto iter = hostIdMap.find(localWorker.ToString());
        if (iter != hostIdMap.end()) {
            router->SetHostId(iter->second);
        }
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch, resolveHostId);
    // Short refresh interval so a second fetch happens quickly after Init's InitialFetch.
    client::Routing routing(router, refresher, 50);

    // Empty host_id + is_local=true enters the resolve-from-hostIdMap path; Init's InitialFetch
    // is the first fetch (host_id absent).
    DS_ASSERT_OK(routing.Init("", localWorker, true));

    // Wait for a second ring change to carry the bound worker's host_id and rebuild sameNodeWorkers.
    const std::vector<std::string> keys = { "k0", "k1", "k2", "k3", "k4" };
    EXPECT_TRUE(WaitForCondition([&] {
        std::unordered_map<HostPort, std::vector<std::string>> groups;
        if (routing.SelectWorkers(keys, client::DataPlacementPolicy::PREFERRED_SAME_NODE, groups).IsError()) {
            return false;
        }
        // When same-node affinity resolves, all keys route to the local worker in a single group.
        return groups.size() == 1 && groups.begin()->first == localWorker;
    }, std::chrono::seconds(5)));

    routing.Shutdown();
}

TEST_F(RoutingFacadeTest, TestInitRejectsInvalidDependenciesAndArguments)
{
    auto router = std::make_shared<client::WorkerRouter>("host-a");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &, std::string &, uint64_t &, bool &,
                    std::unordered_map<std::string, std::string> &) { return Status::OK(); };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);

    client::Routing noRouter(nullptr, refresher);
    EXPECT_TRUE(noRouter.Init("host-a", HostPort("127.0.0.1", 1000)).IsError());

    client::Routing noRefresher(router, nullptr);
    EXPECT_TRUE(noRefresher.Init("host-a", HostPort("127.0.0.1", 1000)).IsError());

    client::Routing routing(router, refresher);
    EXPECT_EQ(routing.Init("host-a", HostPort()).GetCode(), K_INVALID);
    DS_ASSERT_OK(routing.Init("", HostPort("127.0.0.1", 1000)));
    routing.Shutdown();
}

TEST_F(RoutingFacadeTest, TestOwnedChannelInitFailsFastOnInvalidConfig)
{
    BrpcChannelConfig channelConfig;
    client::Routing missingSignature(channelConfig, nullptr);
    EXPECT_TRUE(missingSignature.Init("", HostPort("127.0.0.1", 1000)).IsError());

    channelConfig.timeout_ms = 0;
    client::Routing invalidTimeout(channelConfig, std::make_shared<Signature>());
    EXPECT_TRUE(invalidTimeout.Init("", HostPort("127.0.0.1", 1000)).IsError());
}

TEST_F(RoutingRpcClientTest, TestConcurrentConnectionCreationUsesOneCachedChannel)
{
    BrpcChannelConfig channelConfig;
    channelConfig.timeout_ms = 500;
    std::atomic<size_t> createCount{ 0 };
    client::RoutingRpcClient rpcClient(
        channelConfig, std::make_shared<Signature>(), [&createCount](const BrpcChannelConfig &config) {
            createCount.fetch_add(1, std::memory_order_relaxed);
            return std::shared_ptr<brpc::Channel>(BrpcChannelFactory::Create(config));
        });
    DS_ASSERT_OK(rpcClient.Init());

    constexpr size_t threadCount = 16;
    HostPort worker("127.0.0.1", 19'001);
    std::vector<Status> statuses(threadCount);
    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        threads.emplace_back([&, i] {
            GetHashRingRspPb response;
            statuses[i] = rpcClient.GetHashRing(worker, 0, response);
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }

    for (size_t i = 0; i < threadCount; ++i) {
        EXPECT_TRUE(statuses[i].IsError());
    }
    EXPECT_EQ(createCount.load(std::memory_order_relaxed), 1u);
}

TEST_F(RoutingRpcClientTest, TestPruneAndShutdownReleaseCachedConnections)
{
    BrpcChannelConfig channelConfig;
    std::atomic<size_t> createCount{ 0 };
    client::RoutingRpcClient rpcClient(
        channelConfig, std::make_shared<Signature>(), [&createCount](const BrpcChannelConfig &config) {
            createCount.fetch_add(1, std::memory_order_relaxed);
            return std::shared_ptr<brpc::Channel>(BrpcChannelFactory::Create(config));
        });
    DS_ASSERT_OK(rpcClient.Init());
    HostPort retainedWorker("127.0.0.1", 19'002);
    HostPort staleWorker("127.0.0.1", 19'003);
    GetHashRingRspPb response;
    EXPECT_TRUE(rpcClient.GetHashRing(retainedWorker, 0, response).IsError());
    EXPECT_TRUE(rpcClient.GetHashRing(staleWorker, 0, response).IsError());
    ASSERT_EQ(createCount.load(std::memory_order_relaxed), 2u);

    ClusterTopologyPb ring;
    (*ring.mutable_members())[retainedWorker.ToString()].set_state(MembershipPb::ACTIVE);
    rpcClient.PruneConnections(ring);
    EXPECT_TRUE(rpcClient.GetHashRing(retainedWorker, 0, response).IsError());
    EXPECT_EQ(createCount.load(std::memory_order_relaxed), 2u);
    EXPECT_TRUE(rpcClient.GetHashRing(staleWorker, 0, response).IsError());
    EXPECT_EQ(createCount.load(std::memory_order_relaxed), 3u);

    rpcClient.Shutdown();
    EXPECT_EQ(rpcClient.GetHashRing(retainedWorker, 0, response).GetCode(), K_NOT_READY);
}

TEST_F(RoutingRpcClientTest, TestChannelCreationFailureIsReturned)
{
    BrpcChannelConfig channelConfig;
    client::RoutingRpcClient rpcClient(
        channelConfig, std::make_shared<Signature>(),
        [](const BrpcChannelConfig &) { return std::shared_ptr<brpc::Channel>{}; });
    DS_ASSERT_OK(rpcClient.Init());

    GetHashRingRspPb response;
    EXPECT_EQ(rpcClient.GetHashRing(HostPort("127.0.0.1", 19'004), 0, response).GetCode(),
              K_RPC_UNAVAILABLE);
}

TEST_F(RoutingFacadeTest, TestForwardsPolicyBatchAndStateUpdates)
{
    auto router = std::make_shared<client::WorkerRouter>("");
    auto fetch = [](const HostPort &, uint64_t currentVersion, ::datasystem::ClusterTopologyPb &ring, std::string &,
                    uint64_t &newVersion, bool &changed,
                    std::unordered_map<std::string, std::string> &hostIdMap) {
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = currentVersion == 0;
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher, 60'000);
    DS_ASSERT_OK(routing.Init("host-a", HostPort("127.0.0.1", 1000)));

    std::unordered_map<HostPort, std::vector<std::string>> groups;
    DS_ASSERT_OK(routing.SelectWorkers({ "key-a", "key-b" }, client::DataPlacementPolicy::PREFERRED_META_OWNER,
                                       groups));
    ASSERT_EQ(groups.size(), 1u);
    EXPECT_EQ(groups.begin()->first.ToString(), "127.0.0.1:1000");
    EXPECT_EQ(groups.begin()->second.size(), 2u);

    HostPort worker("127.0.0.1", 1000);
    // BrokenFilter debounces: reach EVICT_CONSECUTIVE_FAILURES (100) before the worker is evicted.
    for (int i = 0; i < EVICTION_THRESHOLD; ++i) {
        routing.UpdateState(worker, K_CLIENT_WORKER_DISCONNECT);
    }
    EXPECT_EQ(routing.SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, worker).GetCode(),
              K_NO_AVAILABLE_WORKER);
}

TEST_F(RoutingFacadeTest, TestWorkerDisconnectForcesHashRingRefresh)
{
    auto router = std::make_shared<client::WorkerRouter>("");
    std::atomic<int> fetchCount{ 0 };
    auto fetch = [&fetchCount](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                               uint64_t &newVersion, bool &changed,
                               std::unordered_map<std::string, std::string> &hostIdMap) {
        const int current = fetchCount.fetch_add(1, std::memory_order_acq_rel) + 1;
        FillRing(ring, hostIdMap);
        newVersion = static_cast<uint64_t>(current);
        changed = true;
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher, 60'000);
    DS_ASSERT_OK(routing.Init("host-a", HostPort("127.0.0.1", 1000)));
    ASSERT_TRUE(WaitForCondition([&fetchCount] {
        return fetchCount.load(std::memory_order_acquire) >= 2;
    }, std::chrono::seconds(2)));

    const int beforeDisconnect = fetchCount.load(std::memory_order_acquire);
    routing.UpdateState(HostPort("127.0.0.1", 1000), K_CLIENT_WORKER_DISCONNECT);

    EXPECT_TRUE(WaitForCondition([&fetchCount, beforeDisconnect] {
        return fetchCount.load(std::memory_order_acquire) > beforeDisconnect;
    }, std::chrono::seconds(2)));
    routing.Shutdown();
}

TEST_F(RoutingFacadeTest, TestShutdownRejectsSubsequentCalls)
{
    auto router = std::make_shared<client::WorkerRouter>("");
    auto fetch = [](const HostPort &, uint64_t, ::datasystem::ClusterTopologyPb &ring, std::string &,
                    uint64_t &newVersion, bool &changed,
                    std::unordered_map<std::string, std::string> &hostIdMap) {
        FillRing(ring, hostIdMap);
        newVersion = 1;
        changed = true;
        return Status::OK();
    };
    auto refresher = std::make_shared<client::HashRingRefresher>(router, fetch);
    client::Routing routing(router, refresher, 60'000);
    DS_ASSERT_OK(routing.Init("host-a", HostPort("127.0.0.1", 1000)));
    routing.Shutdown();

    HostPort worker;
    EXPECT_EQ(routing.SelectWorker("key", client::DataPlacementPolicy::PREFERRED_META_OWNER, worker).GetCode(),
              K_NOT_READY);
}

TEST_F(RoutingFacadeTest, TestParseDataPlacementPolicy)
{
    EXPECT_EQ(FLAGS_sdk_data_placement_policy, "PREFERRED_SAME_NODE");
    client::DataPlacementPolicy policy = client::DataPlacementPolicy::PREFERRED_SAME_NODE;
    DS_ASSERT_OK(client::ParseDataPlacementPolicy("PREFERRED_SAME_NODE", policy));
    EXPECT_EQ(policy, client::DataPlacementPolicy::PREFERRED_SAME_NODE);
    DS_ASSERT_OK(client::ParseDataPlacementPolicy("REQUIRED_SAME_NODE", policy));
    EXPECT_EQ(policy, client::DataPlacementPolicy::REQUIRED_SAME_NODE);
    DS_ASSERT_OK(client::ParseDataPlacementPolicy("PREFERRED_META_OWNER", policy));
    EXPECT_EQ(policy, client::DataPlacementPolicy::PREFERRED_META_OWNER);
    DS_ASSERT_OK(client::ParseDataPlacementPolicy("  preferred_meta_owner \t", policy));
    EXPECT_EQ(policy, client::DataPlacementPolicy::PREFERRED_META_OWNER);
    Status invalid = client::ParseDataPlacementPolicy("invalid", policy);
    EXPECT_EQ(invalid.GetCode(), K_INVALID);
    EXPECT_NE(invalid.GetMsg().find("PREFERRED_SAME_NODE"), std::string::npos);
}

}  // namespace ut
}  // namespace datasystem
