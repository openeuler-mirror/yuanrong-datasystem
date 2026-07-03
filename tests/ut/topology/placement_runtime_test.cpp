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
 * Description: Placement runtime read path unit tests.
 */
#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "datasystem/topology/algorithm/topology_algorithm.h"
#include "datasystem/topology/repository/topology_key_helper.h"
#include "datasystem/topology/repository/topology_repository.h"
#include "datasystem/topology/routing/membership_endpoint_view.h"
#include "datasystem/topology/routing/placement_facade.h"
#include "datasystem/topology/routing/redirect_policy.h"
#include "datasystem/topology/routing/routing_cache.h"
#include "datasystem/topology/routing/routing_view.h"
#include "datasystem/topology/routing/owner_endpoint_resolver.h"
#include "tests/ut/common.h"
#include "tests/ut/topology/fake_coordination_backend.h"
#include "tests/ut/topology/testing/fake_topology_repository.h"
#include "tests/ut/topology/testing/topology_test_utils.h"

namespace datasystem {
namespace topology {
namespace {

constexpr int64_t VERSION_1 = 1;
constexpr int64_t VERSION_2 = 2;
constexpr int64_t VERSION_3 = 3;
constexpr int KEY_SEARCH_LIMIT = 1000;
const std::string WORKER_A = "worker-a";
const std::string WORKER_B = "worker-b";
constexpr char TEST_ALGORITHM_ID[] = "test-route";
constexpr char TEST_UNIT_TYPE[] = "key";

class TestRoutingState final : public AlgorithmRoutingState {
public:
    std::unordered_map<std::string, TopologyNodeId> ownerByKey;
    TopologyNodeId defaultOwner;
};

class TestRoutingAlgorithm final : public IRoutingAlgorithm {
public:
    AlgorithmId GetAlgorithmId() const override
    {
        return TEST_ALGORITHM_ID;
    }

    Status BuildRoutingState(const TopologyDescriptor &snapshot,
                             std::unique_ptr<AlgorithmRoutingState> &routing) const override
    {
        auto state = std::make_unique<TestRoutingState>();
        state->algorithmId = TEST_ALGORITHM_ID;
        state->topologyVersion = snapshot.version;
        for (const auto &worker : snapshot.members) {
            if (state->defaultOwner.empty() && worker.state == TopologyNodeState::ACTIVE) {
                state->defaultOwner = worker.nodeId;
            }
        }
        routing = std::move(state);
        return Status::OK();
    }

    Status BuildPlacementUnit(const RouteContext &context, const PlacementPolicyRule &policy,
                              PlacementUnit &unit) const override
    {
        CHECK_FAIL_RETURN_STATUS(!context.key.empty(), K_INVALID, "test route key is empty");
        CHECK_FAIL_RETURN_STATUS(policy.algorithmId == TEST_ALGORITHM_ID, K_INVALID,
                                 "test route policy algorithm mismatch");
        unit.algorithmId = TEST_ALGORITHM_ID;
        unit.unitType = TEST_UNIT_TYPE;
        unit.opaqueUnit = context.key;
        return Status::OK();
    }

    Status Route(const AlgorithmRoutingState &routing, const PlacementUnit &unit, LogicalOwner &owner) const override
    {
        owner = {};
        CHECK_FAIL_RETURN_STATUS(routing.algorithmId == TEST_ALGORITHM_ID, K_INVALID,
                                 "test route state algorithm mismatch");
        CHECK_FAIL_RETURN_STATUS(unit.algorithmId == TEST_ALGORITHM_ID && unit.unitType == TEST_UNIT_TYPE, K_INVALID,
                                 "test route unit mismatch");
        const auto *state = dynamic_cast<const TestRoutingState *>(&routing);
        CHECK_FAIL_RETURN_STATUS(state != nullptr, K_INVALID, "test route state type mismatch");
        auto iter = state->ownerByKey.find(unit.opaqueUnit);
        if (iter != state->ownerByKey.end()) {
            owner.nodeId = iter->second;
        } else {
            owner.nodeId = state->defaultOwner;
        }
        CHECK_FAIL_RETURN_STATUS(!owner.nodeId.empty(), K_NOT_FOUND, "test route owner is missing");
        owner.topologyVersion = routing.topologyVersion;
        return Status::OK();
    }
};

HostPort MakeAddr(int port)
{
    return HostPort("127.0.0.1", port);
}

std::shared_ptr<RoutingSnapshot> MakeSnapshot()
{
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ 0, 100 }, WORKER_A },
        { RoutingRange{ 100, 200 }, WORKER_B },
    };
    return std::make_shared<RoutingSnapshot>(VERSION_1, std::move(owners));
}

PlacementUnit MakeTestPlacementUnit(const std::string &key)
{
    PlacementUnit unit;
    unit.algorithmId = TEST_ALGORITHM_ID;
    unit.unitType = TEST_UNIT_TYPE;
    unit.opaqueUnit = key;
    return unit;
}

std::unique_ptr<AlgorithmRoutingState> MakeTestRoutingState(
    std::initializer_list<std::pair<std::string, TopologyNodeId>> ownersByKey)
{
    auto state = std::make_unique<TestRoutingState>();
    state->algorithmId = TEST_ALGORITHM_ID;
    state->topologyVersion = VERSION_1;
    for (const auto &owner : ownersByKey) {
        state->ownerByKey.emplace(owner.first, owner.second);
        if (state->defaultOwner.empty()) {
            state->defaultOwner = owner.second;
        }
    }
    return state;
}

std::shared_ptr<RoutingSnapshot> MakeSingleOwnerSnapshotForKey(const std::string &key, const std::string &nodeId)
{
    const uint32_t hash = MurmurHash3_32(key);
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ hash - 1, hash }, nodeId },
    };
    RoutingSnapshotFacts facts;
    return std::make_shared<RoutingSnapshot>(VERSION_1, MakeTestRoutingState({ { key, nodeId } }),
                                             std::move(owners), std::move(facts));
}

std::shared_ptr<RoutingSnapshot> MakeRoutedSnapshot(
    std::initializer_list<std::pair<std::string, TopologyNodeId>> ownersByKey, std::vector<RoutingOwnerEntry> owners,
    std::vector<RoutingRedirectHint> hints = {})
{
    RoutingSnapshotFacts facts;
    facts.redirectHints = std::move(hints);
    return std::make_shared<RoutingSnapshot>(VERSION_1, MakeTestRoutingState(ownersByKey), std::move(owners),
                                             std::move(facts));
}

TopologyDescriptor MakeTopologyDescriptor(int64_t version)
{
    TopologyDescriptor topology;
    topology.version = version;
    topology.clusterHasInit = true;
    topology.members = {
        TopologyNode{ WORKER_A, TopologyNodeState::ACTIVE, { 100 } },
        TopologyNode{ WORKER_B, TopologyNodeState::ACTIVE, { 200 } },
    };
    return topology;
}

TransferTaskRecord MakeScaleOutTransferTask(bool finished)
{
    TransferTaskRecord task;
    task.taskId = "migrate-v1-a-to-b";
    task.executorNodeId = WORKER_A;
    task.sourceNodeId = WORKER_A;
    task.targetNodeId = WORKER_B;
    task.createdTopologyVersion = VERSION_1;
    task.targetTopologyVersion = VERSION_2;
    task.ranges = {
        TokenRange{ 0, std::numeric_limits<uint32_t>::max(), WORKER_A, finished },
    };
    return task;
}

Status MakeTopologyPutEvent(const TopologyDescriptor &topology, CoordinationEvent &event)
{
    TopologyRepositoryCodec codec;
    event = {};
    event.type = CoordinationEventType::PUT;
    event.key = TopologyKeyHelper::CommittedTopologyKey();
    event.revision = topology.version;
    return codec.EncodeTopology(topology, event.value);
}

CoordinationEvent MakeRawEvent(CoordinationEventType type, std::string key, std::string value, Revision revision)
{
    CoordinationEvent event;
    event.type = type;
    event.key = std::move(key);
    event.value = std::move(value);
    event.revision = revision;
    return event;
}

std::shared_ptr<MembershipEndpointSnapshot> MakeEndpointSnapshot(
    MemberAvailability workerBAvailability = MemberAvailability::READY)
{
    auto snapshot = std::make_shared<MembershipEndpointSnapshot>();
    snapshot->version = VERSION_1;
    snapshot->localNodeId = WORKER_A;
    snapshot->localAddress = MakeAddr(1111);
    snapshot->members.emplace(WORKER_A, MemberEndpoint{ WORKER_A, MakeAddr(1111), MemberAvailability::READY });
    snapshot->members.emplace(WORKER_B, MemberEndpoint{ WORKER_B, MakeAddr(2222), workerBAvailability });
    snapshot->nodeIdsByAddress.emplace(MakeAddr(1111).ToString(), WORKER_A);
    snapshot->nodeIdsByAddress.emplace(MakeAddr(2222).ToString(), WORKER_B);
    return snapshot;
}

PlacementFacade MakeScopeOnlyFacade()
{
    return PlacementFacade(nullptr, nullptr, nullptr);
}

TEST(PlacementRuntimeTest, RoutingSnapshotLocateNormalRange)
{
    auto snapshot = MakeSnapshot();
    RoutingOwnerEntry entry;
    auto status = snapshot->Locate(50, entry);
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(entry.ownerNodeId, WORKER_A);

    RoutingOwnerEntry boundaryEntry;
    status = snapshot->Locate(100, boundaryEntry);
    EXPECT_TRUE(status.IsOk());
    EXPECT_EQ(boundaryEntry.ownerNodeId, WORKER_A);
}

TEST(PlacementRuntimeTest, RoutingSnapshotLocateWrappedRange)
{
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ 300, 100 }, WORKER_A },
        { RoutingRange{ 100, 300 }, WORKER_B },
    };
    RoutingSnapshot snapshot(VERSION_1, std::move(owners));

    RoutingOwnerEntry lowEntry;
    EXPECT_TRUE(snapshot.Locate(50, lowEntry).IsOk());
    EXPECT_EQ(lowEntry.ownerNodeId, WORKER_A);

    RoutingOwnerEntry highEntry;
    EXPECT_TRUE(snapshot.Locate(350, highEntry).IsOk());
    EXPECT_EQ(highEntry.ownerNodeId, WORKER_A);
}

TEST(PlacementRuntimeTest, RoutingSnapshotLocateEmpty)
{
    RoutingSnapshot snapshot(VERSION_1, {});
    EXPECT_TRUE(snapshot.Empty());

    RoutingOwnerEntry entry;
    auto status = snapshot.Locate(50, entry);
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
}

TEST(PlacementRuntimeTest, RoutingViewNotReady)
{
    RoutingView view;
    std::shared_ptr<const RoutingSnapshot> snapshot;
    auto status = view.GetSnapshot(snapshot);
    EXPECT_EQ(status.GetCode(), K_NOT_READY);
}

TEST(PlacementRuntimeTest, RoutingViewAppliesCommittedTopologyFromRepository)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    RoutingView view;
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(store, MakeTopologyDescriptor(VERSION_1)));

    std::shared_ptr<const RoutingSnapshot> snapshot;
    EXPECT_EQ(view.GetSnapshot(snapshot).GetCode(), K_NOT_READY);

    HashAlgorithm algorithm;
    DS_ASSERT_OK(view.ApplyCommittedTopology(repo, algorithm));

    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), VERSION_1);
    EXPECT_FALSE(snapshot->Empty());
}

TEST(PlacementRuntimeTest, RoutingViewPublishesEmptySnapshotForEmptyCommittedTopology)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    RoutingView view;
    auto topology = MakeTopologyDescriptor(VERSION_1);
    topology.members.clear();
    DS_ASSERT_OK(testing::SeedCommittedTopologyForTest(store, topology));

    HashAlgorithm algorithm;
    DS_ASSERT_OK(view.ApplyCommittedTopology(repo, algorithm));

    std::shared_ptr<const RoutingSnapshot> snapshot;
    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    ASSERT_NE(snapshot, nullptr);
    EXPECT_EQ(snapshot->Version(), VERSION_1);
    EXPECT_TRUE(snapshot->Empty());
}

TEST(PlacementRuntimeTest, RoutingViewPublishesTransferTaskRedirectHintsBeforeActivation)
{
    FakeTopologyRepository repo;
    RoutingView view;
    DS_ASSERT_OK(repo.SeedCommittedTopology(MakeTopologyDescriptor(VERSION_1)));
    DS_ASSERT_OK(repo.SeedTransferTask(MakeScaleOutTransferTask(true)));

    auto algorithm = std::make_shared<HashAlgorithm>();
    DS_ASSERT_OK(view.ApplyCommittedTopology(repo, *algorithm));

    std::shared_ptr<const RoutingSnapshot> snapshot;
    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    ASSERT_NE(snapshot, nullptr);
    ASSERT_EQ(snapshot->RedirectHints().size(), 1ul);
    EXPECT_EQ(snapshot->RedirectHints().front().targetNodeId, WORKER_B);

    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(snapshot);
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, endpointView, locator);
    PlacementFacade facade(locator, redirectPolicy, routingView);

    std::string localOwnedKey;
    for (int i = 0; i < KEY_SEARCH_LIMIT; ++i) {
        auto candidate = "transfer-hint-key-" + std::to_string(i);
        RoutingOwnerEntry entry;
        DS_ASSERT_OK(snapshot->Locate(MurmurHash3_32(candidate), entry));
        if (entry.ownerNodeId == WORKER_A) {
            localOwnedKey = std::move(candidate);
            break;
        }
    }
    ASSERT_FALSE(localOwnedKey.empty());

    RedirectDecision decision;
    RouteOptions options;
    DS_ASSERT_OK(facade.EvaluateRedirect(localOwnedKey, options, decision));
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.nodeId, WORKER_B);
}

TEST(PlacementRuntimeTest, RoutingViewDropsStaleTransferTaskRedirectHintsAfterTargetVersionPassed)
{
    FakeTopologyRepository repo;
    RoutingView view;
    DS_ASSERT_OK(repo.SeedCommittedTopology(MakeTopologyDescriptor(VERSION_3)));
    DS_ASSERT_OK(repo.SeedTransferTask(MakeScaleOutTransferTask(false)));

    HashAlgorithm algorithm;
    DS_ASSERT_OK(view.ApplyCommittedTopology(repo, algorithm));

    std::shared_ptr<const RoutingSnapshot> snapshot;
    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    ASSERT_NE(snapshot, nullptr);
    EXPECT_TRUE(snapshot->RedirectHints().empty());
}

TEST(PlacementRuntimeTest, RouterIgnoresRingSubKeysAndKeepsLastSnapshot)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    RoutingView view;
    CoordinationEvent topologyEvent;
    DS_ASSERT_OK(MakeTopologyPutEvent(MakeTopologyDescriptor(VERSION_1), topologyEvent));
    HashAlgorithm algorithm;

    DS_ASSERT_OK(view.ApplyCommittedTopologyEvent(repo, topologyEvent, algorithm));
    std::shared_ptr<const RoutingSnapshot> snapshot;
    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), VERSION_1);
    EXPECT_FALSE(snapshot->Empty());

    auto subkey = TopologyKeyHelper::CommittedTopologyKey() + "/migrate_tasks/task-1";
    DS_ASSERT_OK(view.ApplyCommittedTopologyEvent(
        repo, MakeRawEvent(CoordinationEventType::PUT, subkey, "not-a-topology-payload", VERSION_2), algorithm));

    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), VERSION_1);
    EXPECT_FALSE(snapshot->Empty());
}

TEST(PlacementRuntimeTest, ExactRingDeleteAndMalformedUpdateKeepLastSnapshot)
{
    FakeCoordinationBackend store;
    TopologyRepository repo(store);
    RoutingView view;
    CoordinationEvent topologyEvent;
    DS_ASSERT_OK(MakeTopologyPutEvent(MakeTopologyDescriptor(VERSION_1), topologyEvent));
    HashAlgorithm algorithm;

    DS_ASSERT_OK(view.ApplyCommittedTopologyEvent(repo, topologyEvent, algorithm));
    DS_ASSERT_OK(view.ApplyCommittedTopologyEvent(
        repo, MakeRawEvent(CoordinationEventType::DELETE, TopologyKeyHelper::CommittedTopologyKey(), "", VERSION_2),
        algorithm));

    std::shared_ptr<const RoutingSnapshot> snapshot;
    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), VERSION_1);

    EXPECT_EQ(view.ApplyCommittedTopologyEvent(
                      repo,
                      MakeRawEvent(CoordinationEventType::PUT, TopologyKeyHelper::CommittedTopologyKey(), "bad", 3),
                      algorithm)
                  .GetCode(),
              K_INVALID);
    DS_ASSERT_OK(view.GetSnapshot(snapshot));
    EXPECT_EQ(snapshot->Version(), VERSION_1);
}

TEST(PlacementRuntimeTest, MembershipEndpointViewResolveMissing)
{
    MembershipEndpointView endpointView;
    endpointView.Publish(MakeEndpointSnapshot());

    MemberEndpoint endpoint;
    auto status = endpointView.ResolveEndpoint("missing-worker", endpoint);
    EXPECT_EQ(status.GetCode(), K_NOT_FOUND);
}

TEST(PlacementRuntimeTest, MembershipEndpointViewNotReadyThenLocal)
{
    MembershipEndpointView endpointView;

    MemberEndpoint resolved;
    EXPECT_EQ(endpointView.ResolveEndpoint(WORKER_A, resolved).GetCode(), K_NOT_READY);

    MemberEndpoint local;
    EXPECT_EQ(endpointView.GetLocalEndpoint(local).GetCode(), K_NOT_READY);

    endpointView.Publish(MakeEndpointSnapshot());
    EXPECT_TRUE(endpointView.GetLocalEndpoint(local).IsOk());
    EXPECT_EQ(local.nodeId, WORKER_A);
    EXPECT_EQ(local.availability, MemberAvailability::READY);
}

TEST(PlacementRuntimeTest, MembershipEndpointViewResolveByAddress)
{
    MembershipEndpointView endpointView;
    endpointView.Publish(MakeEndpointSnapshot(MemberAvailability::NOT_READY));

    MemberEndpoint endpoint;
    EXPECT_TRUE(endpointView.ResolveEndpointByAddress(MakeAddr(2222).ToString(), endpoint).IsOk());
    EXPECT_EQ(endpoint.nodeId, WORKER_B);
    EXPECT_EQ(endpoint.availability, MemberAvailability::NOT_READY);

    EXPECT_EQ(endpointView.ResolveEndpointByAddress(MakeAddr(3333).ToString(), endpoint).GetCode(), K_NOT_FOUND);
}

TEST(PlacementRuntimeTest, RoutingSnapshotTopologyFacts)
{
    RoutingSnapshotFacts facts;
    facts.localOwnedRanges = { RoutingRange{ 10, 20 } };
    facts.nodeOrder = { WORKER_A, WORKER_B };
    facts.validTopologyNodeIds = { WORKER_A, WORKER_B };
    facts.activeTopologyNodeIds = { WORKER_A };
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ 10, 20 }, WORKER_A },
        { RoutingRange{ 20, 30 }, WORKER_B },
    };

    RoutingSnapshot snapshot(VERSION_1, std::move(owners), std::move(facts));
    EXPECT_EQ(snapshot.LocalOwnedRanges().size(), 1ul);
    EXPECT_EQ(snapshot.ValidTopologyNodeIds().size(), 2ul);
    EXPECT_EQ(snapshot.ActiveTopologyNodeIds().size(), 1ul);

    std::string standbyTopologyNodeId;
    EXPECT_TRUE(snapshot.GetStandbyTopologyNodeId(WORKER_A, standbyTopologyNodeId).IsOk());
    EXPECT_EQ(standbyTopologyNodeId, WORKER_B);
}

TEST(PlacementRuntimeTest, RoutingCacheVersionMismatchMiss)
{
    RoutingCache cache;
    RouteDecision decision;
    decision.routingVersion = VERSION_1;
    decision.ownerNodeId = WORKER_A;
    cache.Store(RouteCacheKey{ VERSION_1, MakeTestPlacementUnit("cache-key") }, decision);

    RouteDecision cached;
    EXPECT_FALSE(cache.Lookup(RouteCacheKey{ VERSION_2, MakeTestPlacementUnit("cache-key") }, cached));
    EXPECT_TRUE(cache.Lookup(RouteCacheKey{ VERSION_1, MakeTestPlacementUnit("cache-key") }, cached));
    EXPECT_EQ(cached.ownerNodeId, WORKER_A);
}

TEST(PlacementRuntimeTest, PlacementFacadeRoutesFromPublishedSnapshotsWithoutBackend)
{
    const std::string key = "foreground-route-from-snapshot";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_B));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    PlacementFacade facade(locator, nullptr, routingView);

    RouteDecision route;
    RouteOptions options;
    DS_ASSERT_OK(facade.LocateMetaOwner(key, options, route));
    EXPECT_EQ(route.routingVersion, VERSION_1);
    EXPECT_EQ(route.ownerNodeId, WORKER_B);
    EXPECT_EQ(route.ownerEndpoint.nodeId, WORKER_B);
    EXPECT_EQ(route.ownerEndpoint.address, MakeAddr(2222));

    LocalPlacementQuery query;
    query.key = key;
    LocalPlacementDecision local;
    DS_ASSERT_OK(facade.QueryLocalPlacement(query, local));
    EXPECT_EQ(local.routingVersion, VERSION_1);
    EXPECT_EQ(local.keyHash, hash);
}

TEST(PlacementRuntimeTest, OwnerEndpointResolverRoutesThroughAlgorithmStateWithoutHashRanges)
{
    const std::string key = "algorithm-owned-non-hash-route";
    auto routingView = std::make_shared<RoutingView>();
    RoutingSnapshotFacts facts;
    routingView->Publish(std::make_shared<RoutingSnapshot>(VERSION_1, MakeTestRoutingState({ { key, WORKER_B } }),
                                                           std::vector<RoutingOwnerEntry>(), std::move(facts)));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    OwnerEndpointResolver locator(routingView, endpointView, algorithm);

    RouteDecision decision;
    RouteOptions options;
    DS_ASSERT_OK(locator.LocateMetaOwner(key, options, nullptr, decision));
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_EQ(decision.ownerNodeId, WORKER_B);
    EXPECT_EQ(decision.ownerEndpoint.nodeId, WORKER_B);
    EXPECT_EQ(decision.ownerEndpoint.address, MakeAddr(2222));
}

TEST(PlacementRuntimeTest, LocateMetaOwnerRequireAvailableTarget)
{
    const std::string key = "key-for-unavailable-worker";
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_B));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot(MemberAvailability::NOT_READY));
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    OwnerEndpointResolver locator(routingView, endpointView, algorithm);

    RouteDecision decision;
    RouteOptions options;
    options.requireAvailableTarget = true;
    auto status = locator.LocateMetaOwner(key, options, nullptr, decision);
    EXPECT_EQ(status.GetCode(), K_RPC_UNAVAILABLE);
}

TEST(PlacementRuntimeTest, LocateMetaOwnersBatchPartialFailure)
{
    const std::string key = "key-for-missing-worker";
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_B));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    auto endpointSnapshot = MakeEndpointSnapshot();
    endpointSnapshot->members.erase(WORKER_B);
    endpointView->Publish(endpointSnapshot);
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    OwnerEndpointResolver locator(routingView, endpointView, algorithm);

    BatchRouteDecision decision;
    RouteOptions options;
    std::vector<std::string> keys{ key };
    EXPECT_TRUE(locator.LocateMetaOwnersBatch(keys, options, decision).IsOk());
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_TRUE(decision.hasPartialFailure);
    EXPECT_EQ(decision.perKeyFailure.size(), 1ul);
}

TEST(PlacementRuntimeTest, LocateMetaOwnersBatchGroupsByEndpoint)
{
    const std::string keyA = "batch-group-key-a";
    const std::string keyB = "batch-group-key-b";
    const uint32_t hashA = MurmurHash3_32(keyA);
    const uint32_t hashB = MurmurHash3_32(keyB);
    // Build a two-owner ring that deterministically covers both hashes: the lower hash goes to
    // WORKER_A and the higher hash to WORKER_B. Using each key's real hash avoids hard-coded ranges.
    const uint32_t lowerHash = std::min(hashA, hashB);
    const uint32_t higherHash = std::max(hashA, hashB);
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ lowerHash - 1, lowerHash }, WORKER_A },
        { RoutingRange{ lowerHash, higherHash }, WORKER_B },
    };
    const std::string &expectedOwnerA = hashA < hashB ? WORKER_A : WORKER_B;
    const std::string &expectedOwnerB = hashA < hashB ? WORKER_B : WORKER_A;
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeRoutedSnapshot({ { keyA, expectedOwnerA }, { keyB, expectedOwnerB } },
                                            std::move(owners)));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    OwnerEndpointResolver locator(routingView, endpointView, algorithm);

    BatchRouteDecision decision;
    RouteOptions options;
    std::vector<std::string> keys{ keyA, keyB };
    EXPECT_TRUE(locator.LocateMetaOwnersBatch(keys, options, decision).IsOk());
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_FALSE(decision.hasPartialFailure);
    // Two distinct owners form two endpoint groups, one key each.
    EXPECT_EQ(decision.groupsByEndpoint.size(), 2ul);
    EXPECT_EQ(decision.perKeyDecision.size(), keys.size());
    EXPECT_EQ(decision.perKeyDecision.at(keyA).ownerNodeId, expectedOwnerA);
    EXPECT_EQ(decision.perKeyDecision.at(keyB).ownerNodeId, expectedOwnerB);
}

TEST(PlacementRuntimeTest, IsInRangeMatchesAndMisses)
{
    auto facade = MakeScopeOnlyFacade();

    // Normal non-wrapped range: a key hashing into (begin, end] matches.
    const std::string key = "is-in-range-key";
    const uint32_t hash = MurmurHash3_32(key);
    EXPECT_TRUE(facade.IsInRange(key, { { hash - 1, hash } }));
    // A gap before the key's hash does not match.
    EXPECT_FALSE(facade.IsInRange(key, { { hash + 1, hash + 2 } }));
    // Wrapped range across the 0 boundary still matches a low hash.
    EXPECT_TRUE(facade.IsInRange(key, { { UINT32_MAX - 1, hash } }));
    // Empty range set never matches.
    EXPECT_FALSE(facade.IsInRange(key, {}));
}

TEST(PlacementRuntimeTest, RedirectServeLocal)
{
    const std::string key = "redirect-local-key";
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_A));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    RedirectPolicy policy(routingView, endpointView, locator);

    RedirectDecision decision;
    RouteOptions options;
    EXPECT_TRUE(policy.Evaluate(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::SERVE_LOCAL);
}

TEST(PlacementRuntimeTest, RedirectTargetUnavailableStillRedirects)
{
    const std::string key = "redirect-unavailable-key";
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_B));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot(MemberAvailability::NOT_READY));
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    RedirectPolicy policy(routingView, endpointView, locator);

    RedirectDecision decision;
    RouteOptions options;
    EXPECT_TRUE(policy.Evaluate(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.nodeId, WORKER_B);
}

TEST(PlacementRuntimeTest, RedirectScaleUpInFlightRange)
{
    const std::string key = "redirect-in-flight-key";
    const uint32_t hash = MurmurHash3_32(key);
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ hash - 1, hash }, WORKER_A },
    };
    std::vector<RoutingRedirectHint> hints{
        { RoutingRange{ hash - 1, hash }, WORKER_B, MakeAddr(2222) },
    };
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeRoutedSnapshot({ { key, WORKER_A } }, std::move(owners), std::move(hints)));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, endpointView, locator);
    PlacementFacade facade(locator, redirectPolicy, routingView);

    RouteDecision route;
    RouteOptions options;
    EXPECT_TRUE(facade.LocateMetaOwner(key, options, route).IsOk());
    EXPECT_EQ(route.ownerNodeId, WORKER_A);

    RedirectDecision decision;
    EXPECT_TRUE(facade.EvaluateRedirect(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.nodeId, WORKER_B);
}

TEST(PlacementRuntimeTest, LocateMetaOwnerDoesNotUseRedirectHint)
{
    const std::string key = "locate-with-redirect-hint-key";
    const uint32_t hash = MurmurHash3_32(key);
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ hash - 1, hash }, WORKER_A },
    };
    std::vector<RoutingRedirectHint> hints{
        { RoutingRange{ hash - 1, hash }, WORKER_B, MakeAddr(2222) },
    };
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeRoutedSnapshot({ { key, WORKER_A } }, std::move(owners), std::move(hints)));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    OwnerEndpointResolver locator(routingView, endpointView, algorithm);

    RouteDecision route;
    RouteOptions options;
    EXPECT_TRUE(locator.LocateMetaOwner(key, options, nullptr, route).IsOk());
    EXPECT_EQ(route.ownerNodeId, WORKER_A);
    EXPECT_EQ(route.ownerEndpoint.nodeId, WORKER_A);
}

TEST(PlacementRuntimeTest, RedirectScaleUpFinished)
{
    const std::string key = "redirect-finished-key";
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_B));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, endpointView, locator);
    PlacementFacade facade(locator, redirectPolicy, routingView);

    RouteOptions options;

    RedirectDecision decision;
    EXPECT_TRUE(facade.EvaluateRedirect(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.nodeId, WORKER_B);
}

TEST(PlacementRuntimeTest, PlacementFacadeQueryLocalPlacement)
{
    const std::string key = "facade-local-placement-key";
    const uint32_t hash = MurmurHash3_32(key);
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeSingleOwnerSnapshotForKey(key, WORKER_A));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    auto redirectPolicy = std::make_shared<RedirectPolicy>(routingView, endpointView, locator);
    PlacementFacade facade(locator, redirectPolicy, routingView);

    LocalPlacementQuery query;
    query.key = key;
    LocalPlacementDecision decision;
    EXPECT_TRUE(facade.QueryLocalPlacement(query, decision).IsOk());
    EXPECT_EQ(decision.keyHash, hash);
    EXPECT_EQ(decision.routingVersion, VERSION_1);
    EXPECT_EQ(decision.placementUnit.rangeBegin, hash - 1);
    EXPECT_EQ(decision.placementUnit.rangeEnd, hash);
}

// Hint nodeId is absent from the endpointView: the policy must fall back to the hint's raw address
// with NOT_READY availability instead of failing.
TEST(PlacementRuntimeTest, RedirectHintWorkerMissingFallsBackToRawAddress)
{
    const std::string key = "redirect-hint-unknown-worker-key";
    const uint32_t hash = MurmurHash3_32(key);
    const std::string unknownWorker = "worker-unknown";
    const HostPort hintAddr = MakeAddr(3333);
    std::vector<RoutingOwnerEntry> owners{ { RoutingRange{ hash - 1, hash }, WORKER_A } };
    std::vector<RoutingRedirectHint> hints{ { RoutingRange{ hash - 1, hash }, unknownWorker, hintAddr } };
    auto routingView = std::make_shared<RoutingView>();
    routingView->Publish(MakeRoutedSnapshot({ { key, WORKER_A } }, std::move(owners), std::move(hints)));
    auto endpointView = std::make_shared<MembershipEndpointView>();
    endpointView->Publish(MakeEndpointSnapshot());
    auto algorithm = std::make_shared<TestRoutingAlgorithm>();
    auto locator = std::make_shared<OwnerEndpointResolver>(routingView, endpointView, algorithm);
    RedirectPolicy policy(routingView, endpointView, locator);

    RedirectDecision decision;
    RouteOptions options;
    EXPECT_TRUE(policy.Evaluate(key, options, decision).IsOk());
    EXPECT_EQ(decision.action, RedirectAction::REDIRECT);
    EXPECT_EQ(decision.targetEndpoint.nodeId, unknownWorker);
    EXPECT_EQ(decision.targetEndpoint.address, hintAddr);
    EXPECT_EQ(decision.targetEndpoint.availability, MemberAvailability::NOT_READY);
}

// MembershipView has a local identity/address but no entry for it in the members map: GetLocalEndpoint must
// synthesize a NOT_READY endpoint from the local address rather than fail.
TEST(PlacementRuntimeTest, MembershipEndpointViewLocalWorkerFallsBackToAddress)
{
    const std::string localNode = "worker-local-only";
    const HostPort localAddr = MakeAddr(4321);
    auto snapshot = std::make_shared<MembershipEndpointSnapshot>();
    snapshot->version = VERSION_1;
    snapshot->localNodeId = localNode;
    snapshot->localAddress = localAddr;
    // members map intentionally omits localNode so the fallback branch is exercised.
    snapshot->members.emplace(WORKER_B, MemberEndpoint{ WORKER_B, MakeAddr(2222), MemberAvailability::READY });

    MembershipEndpointView endpointView;
    endpointView.Publish(snapshot);

    MemberEndpoint endpoint;
    EXPECT_TRUE(endpointView.GetLocalEndpoint(endpoint).IsOk());
    EXPECT_EQ(endpoint.nodeId, localNode);
    EXPECT_EQ(endpoint.address, localAddr);
    EXPECT_EQ(endpoint.availability, MemberAvailability::NOT_READY);
}

// Locate returns K_NOT_FOUND when no owner range covers the hash (a genuine gap in the ring).
TEST(PlacementRuntimeTest, RoutingSnapshotLocateGapReturnsNotFound)
{
    std::vector<RoutingOwnerEntry> owners{
        { RoutingRange{ 10, 100 }, WORKER_A },
        { RoutingRange{ 200, 300 }, WORKER_B },
    };
    RoutingSnapshot snapshot(VERSION_1, std::move(owners));

    RoutingOwnerEntry entry;
    auto status = snapshot.Locate(150, entry);  // 150 sits between 100 and 200.
    EXPECT_EQ(status.GetCode(), K_NOT_FOUND);
}

// A wrapped redirect hint range must match hashes that span the 0 boundary.
TEST(PlacementRuntimeTest, FindRedirectHintWrappedRangeMatches)
{
    std::vector<RoutingOwnerEntry> owners{ { RoutingRange{ 0, 100 }, WORKER_A } };
    std::vector<RoutingRedirectHint> hints{ { RoutingRange{ 300, 100 }, WORKER_B, MakeAddr(2222) } };
    RoutingSnapshot snapshot(VERSION_1, std::move(owners), std::move(hints));

    RoutingRedirectHint hint;
    EXPECT_TRUE(snapshot.FindRedirectHint(50, hint));  // 50 is in (300, 100] across the wrap.
    EXPECT_EQ(hint.targetNodeId, WORKER_B);
    EXPECT_FALSE(snapshot.FindRedirectHint(200, hint));  // 200 is outside the wrapped hint.
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
