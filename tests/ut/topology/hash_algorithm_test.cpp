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
 * Description: Hash routing algorithm tests.
 */
#include "datasystem/topology/algorithm/hash_algorithm.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "datasystem/common/util/hash_algorithm.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char HASH_ALGORITHM_ID[] = "hash";
constexpr char HASH_UNIT_TYPE[] = "hash-token";
constexpr char WORKER_A[] = "worker-a";
constexpr char WORKER_B[] = "worker-b";
constexpr char WORKER_C[] = "worker-c";
constexpr int SCALE_WORKER_NUM = 1000;
constexpr int ROUTE_SAMPLE_NUM = 1000;
constexpr int ROUTE_BUDGET_MS = 200;
constexpr uint32_t PLANNING_VIRTUAL_TOKEN_NUM = 4;
constexpr int PLANNING_SCALE_BASE_WORKER_NUM = 2000;
constexpr int PLANNING_SCALE_CHANGE_WORKER_NUM = 500;
constexpr int PLANNING_BUDGET_MS = 2000;
constexpr int PLANNING_THREAD_NUM = 8;

TopologyDescriptor MakeTopology(std::vector<TopologyWorker> workers, Revision version = 1)
{
    TopologyDescriptor topology;
    topology.version = version;
    topology.clusterHasInit = true;
    topology.workers = std::move(workers);
    return topology;
}

PlacementPolicyRule MakeHashPolicy()
{
    PlacementPolicyRule policy;
    policy.policyId = "default";
    policy.matchType = PlacementPolicyMatchType::CATCH_ALL;
    policy.algorithmId = HASH_ALGORITHM_ID;
    return policy;
}

PlacementUnit MakeHashUnit(uint32_t token)
{
    PlacementUnit unit;
    unit.algorithmId = HASH_ALGORITHM_ID;
    unit.unitType = HASH_UNIT_TYPE;
    unit.opaqueUnit = std::to_string(token);
    return unit;
}

LogicalOwner RouteToken(const HashAlgorithm &algorithm, const AlgorithmRoutingState &state, uint32_t token)
{
    LogicalOwner owner;
    auto rc = algorithm.Route(state, MakeHashUnit(token), owner);
    EXPECT_TRUE(rc.IsOk()) << rc.ToString();
    return owner;
}

std::vector<WorkerId> MakeWorkerIds(std::initializer_list<const char *> workers)
{
    std::vector<WorkerId> ids;
    ids.reserve(workers.size());
    for (auto *worker : workers) {
        ids.emplace_back(worker);
    }
    return ids;
}

std::vector<WorkerId> MakeWorkerIds(int begin, int count)
{
    std::vector<WorkerId> ids;
    ids.reserve(count);
    for (int i = 0; i < count; ++i) {
        ids.emplace_back("worker-" + std::to_string(begin + i));
    }
    return ids;
}

const TopologyWorker *FindWorker(const TopologyDescriptor &topology, const WorkerId &workerId)
{
    auto iter = std::find_if(topology.workers.begin(), topology.workers.end(),
                             [&workerId](const auto &worker) { return worker.workerId == workerId; });
    return iter == topology.workers.end() ? nullptr : &(*iter);
}

std::vector<uint32_t> GetTokens(const TopologyDescriptor &topology, const WorkerId &workerId)
{
    auto *worker = FindWorker(topology, workerId);
    return worker == nullptr ? std::vector<uint32_t>{} : worker->tokens;
}

bool HasWorker(const TopologyDescriptor &topology, const WorkerId &workerId)
{
    return FindWorker(topology, workerId) != nullptr;
}

size_t CountRanges(const std::vector<OwnerChange> &changes)
{
    size_t rangeCount = 0;
    for (const auto &change : changes) {
        rangeCount += change.ranges.size();
    }
    return rangeCount;
}

void ExpectNoDuplicatedTokens(const TopologyDescriptor &topology)
{
    std::unordered_set<uint32_t> tokens;
    for (const auto &worker : topology.workers) {
        for (auto token : worker.tokens) {
            EXPECT_TRUE(tokens.insert(token).second) << "duplicated token: " << token;
        }
    }
}

TEST(TopologyHashAlgorithmTest, HashRoutingStateBuildCanonical)
{
    HashAlgorithm algorithm;
    auto topology = MakeTopology({
        { WORKER_B, WorkerTopologyState::ACTIVE, { 200, 100 } },
        { WORKER_A, WorkerTopologyState::ACTIVE, { 50 } },
    });
    auto sameTopologyDifferentOrder = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 50 } },
        { WORKER_B, WorkerTopologyState::ACTIVE, { 100, 200 } },
    });

    std::unique_ptr<AlgorithmRoutingState> state;
    std::unique_ptr<AlgorithmRoutingState> sameState;
    DS_ASSERT_OK(algorithm.BuildRoutingState(topology, state));
    DS_ASSERT_OK(algorithm.BuildRoutingState(sameTopologyDifferentOrder, sameState));
    EXPECT_EQ(RouteToken(algorithm, *state, 55).workerId, RouteToken(algorithm, *sameState, 55).workerId);
    EXPECT_EQ(RouteToken(algorithm, *state, 201).workerId, WORKER_A);

    auto duplicateToken = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 50 } },
        { WORKER_B, WorkerTopologyState::ACTIVE, { 50 } },
    });
    EXPECT_EQ(algorithm.BuildRoutingState(duplicateToken, state).GetCode(), K_INVALID);

    auto emptyActiveTokens = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 50 } },
        { WORKER_B, WorkerTopologyState::ACTIVE, {} },
    });
    EXPECT_EQ(algorithm.BuildRoutingState(emptyActiveTokens, state).GetCode(), K_INVALID);

    auto duplicateWorkerId = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 50 } },
        { WORKER_A, WorkerTopologyState::JOINING, { 60 } },
    });
    EXPECT_EQ(algorithm.BuildRoutingState(duplicateWorkerId, state).GetCode(), K_INVALID);

    auto invalidState = MakeTopology({ { WORKER_A, static_cast<WorkerTopologyState>(99), { 50 } } });
    EXPECT_EQ(algorithm.BuildRoutingState(invalidState, state).GetCode(), K_INVALID);

    auto notInitialized = MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 50 } } });
    notInitialized.clusterHasInit = false;
    EXPECT_EQ(algorithm.BuildRoutingState(notInitialized, state).GetCode(), K_NOT_READY);
}

TEST(TopologyHashAlgorithmTest, HashAlgorithmIdIsStable)
{
    HashAlgorithm algorithm("custom-hash");
    EXPECT_EQ(algorithm.GetAlgorithmId(), "custom-hash");
}

TEST(TopologyHashAlgorithmTest, HashBuildPlacementUnitDeterministic)
{
    HashAlgorithm algorithm;
    RouteContext context;
    context.objectKey = "object-key";
    auto policy = MakeHashPolicy();

    PlacementUnit first;
    PlacementUnit second;
    DS_ASSERT_OK(algorithm.BuildPlacementUnit(context, policy, first));
    DS_ASSERT_OK(algorithm.BuildPlacementUnit(context, policy, second));
    EXPECT_EQ(first.algorithmId, HASH_ALGORITHM_ID);
    EXPECT_EQ(first.unitType, HASH_UNIT_TYPE);
    EXPECT_EQ(first.opaqueUnit, second.opaqueUnit);
    EXPECT_EQ(first.opaqueUnit, std::to_string(MurmurHash3_32(context.objectKey)));

    context.objectKey.clear();
    EXPECT_EQ(algorithm.BuildPlacementUnit(context, policy, first).GetCode(), K_INVALID);

    context.objectKey = "object-key";
    policy.algorithmId = "other";
    EXPECT_EQ(algorithm.BuildPlacementUnit(context, policy, first).GetCode(), K_INVALID);
}

TEST(TopologyHashAlgorithmTest, HashRouteDeterministicOwner)
{
    HashAlgorithm algorithm;
    auto topology = MakeTopology(
        {
            { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } },
            { WORKER_B, WorkerTopologyState::ACTIVE, { 200 } },
        },
        3);
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(topology, state));

    auto owner = RouteToken(algorithm, *state, 150);
    EXPECT_EQ(owner.workerId, WORKER_B);
    EXPECT_EQ(owner.topologyVersion, topology.version);

    EXPECT_EQ(RouteToken(algorithm, *state, 250).workerId, WORKER_A);
    EXPECT_EQ(RouteToken(algorithm, *state, 150).workerId, WORKER_B);

    PlacementUnit mismatch = MakeHashUnit(150);
    mismatch.algorithmId = "other";
    EXPECT_EQ(algorithm.Route(*state, mismatch, owner).GetCode(), K_INVALID);

    AlgorithmRoutingState genericState;
    genericState.algorithmId = HASH_ALGORITHM_ID;
    genericState.topologyVersion = topology.version;
    EXPECT_EQ(algorithm.Route(genericState, MakeHashUnit(150), owner).GetCode(), K_INVALID);

    auto noOwnerTopology = MakeTopology({ { WORKER_C, WorkerTopologyState::JOINING, { 1 } } });
    EXPECT_EQ(algorithm.BuildRoutingState(noOwnerTopology, state).GetCode(), K_NOT_READY);
}

TEST(TopologyHashAlgorithmTest, HashRoutingRejectsMalformedStateAndUnit)
{
    HashAlgorithm algorithm;
    auto topology = MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } } });
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(topology, state));

    LogicalOwner owner;
    auto unit = MakeHashUnit(100);
    unit.unitType = "range";
    EXPECT_EQ(algorithm.Route(*state, unit, owner).GetCode(), K_INVALID);

    unit = MakeHashUnit(100);
    unit.opaqueUnit.clear();
    EXPECT_EQ(algorithm.Route(*state, unit, owner).GetCode(), K_INVALID);

    unit.opaqueUnit = "12a";
    EXPECT_EQ(algorithm.Route(*state, unit, owner).GetCode(), K_INVALID);

    unit.opaqueUnit = "4294967296";
    EXPECT_EQ(algorithm.Route(*state, unit, owner).GetCode(), K_INVALID);

    state->algorithmId = "other";
    EXPECT_EQ(algorithm.Route(*state, MakeHashUnit(100), owner).GetCode(), K_INVALID);

    HashAlgorithm emptyAlgorithmId("");
    EXPECT_EQ(emptyAlgorithmId.BuildRoutingState(topology, state).GetCode(), K_INVALID);

    topology.version = -1;
    EXPECT_EQ(algorithm.BuildRoutingState(topology, state).GetCode(), K_INVALID);
}

TEST(TopologyHashAlgorithmTest, C2RoutingPureComputationNoRepositoryIo)
{
    HashAlgorithm algorithm;
    auto topology = MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } } });
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(topology, state));

    RouteContext context;
    context.objectKey = "object-key";
    PlacementUnit unit;
    DS_ASSERT_OK(algorithm.BuildPlacementUnit(context, MakeHashPolicy(), unit));
    LogicalOwner owner;
    DS_ASSERT_OK(algorithm.Route(*state, unit, owner));
    EXPECT_EQ(owner.workerId, WORKER_A);
}

TEST(TopologyHashAlgorithmTest, CommittedTopologyOnlyRouting)
{
    HashAlgorithm algorithm;
    auto committedTopology = MakeTopology({ { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } } }, 10);
    auto newerCommittedTopology = MakeTopology(
        {
            { WORKER_A, WorkerTopologyState::ACTIVE, { 100 } },
            { WORKER_B, WorkerTopologyState::ACTIVE, { 60 } },
        },
        11);
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(committedTopology, state));

    EXPECT_EQ(RouteToken(algorithm, *state, 50).workerId, WORKER_A);

    std::unique_ptr<AlgorithmRoutingState> updatedState;
    DS_ASSERT_OK(algorithm.BuildRoutingState(newerCommittedTopology, updatedState));
    EXPECT_EQ(RouteToken(algorithm, *updatedState, 50).workerId, WORKER_B);
}

TEST(TopologyHashAlgorithmTest, HashAlgorithmRouteScaleBudget)
{
    std::vector<TopologyWorker> workers;
    workers.reserve(SCALE_WORKER_NUM);
    for (int i = 0; i < SCALE_WORKER_NUM; ++i) {
        workers.push_back({ "worker-" + std::to_string(i), WorkerTopologyState::ACTIVE, { static_cast<uint32_t>(i) } });
    }

    HashAlgorithm algorithm;
    auto begin = std::chrono::steady_clock::now();
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm.BuildRoutingState(MakeTopology(std::move(workers)), state));
    for (int i = 0; i < ROUTE_SAMPLE_NUM; ++i) {
        LogicalOwner owner;
        DS_ASSERT_OK(algorithm.Route(*state, MakeHashUnit(static_cast<uint32_t>(i)), owner));
        EXPECT_FALSE(owner.workerId.empty());
    }
    auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);
    EXPECT_LT(costMs.count(), ROUTE_BUDGET_MS);
}

TEST(TopologyHashAlgorithmTest, HashInitPlacementAllocatesTokensDeterministically)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput input;
    input.targetWorkerIds = MakeWorkerIds({ WORKER_B, WORKER_A });

    PlanResult first;
    PlanResult second;
    DS_ASSERT_OK(algorithm.InitPlacement(input, first));
    DS_ASSERT_OK(algorithm.InitPlacement(input, second));
    EXPECT_EQ(first.algorithmId, HASH_ALGORITHM_ID);
    EXPECT_EQ(first.next.workers.size(), input.targetWorkerIds.size());
    EXPECT_EQ(first.next.workers.size(), second.next.workers.size());
    EXPECT_TRUE(first.ownerChanges.empty());
    ExpectNoDuplicatedTokens(first.next);

    auto workerATokens = GetTokens(first.next, WORKER_A);
    auto workerBTokens = GetTokens(first.next, WORKER_B);
    ASSERT_EQ(workerATokens.size(), PLANNING_VIRTUAL_TOKEN_NUM);
    ASSERT_EQ(workerBTokens.size(), PLANNING_VIRTUAL_TOKEN_NUM);
    EXPECT_EQ(workerATokens, GetTokens(second.next, WORKER_A));
    EXPECT_EQ(workerBTokens, GetTokens(second.next, WORKER_B));
    EXPECT_EQ(workerATokens.front(), MurmurHash3_32(std::string(WORKER_A) + "#0"));

    input.current.version = -1;
    EXPECT_EQ(algorithm.InitPlacement(input, first).GetCode(), K_INVALID);
}

TEST(TopologyHashAlgorithmTest, HashPlanAddsOneOrManyMembers)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    PlanInput addInput;
    addInput.current = current.next;
    addInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B, WORKER_C, "worker-d" });
    PlanResult result;
    DS_ASSERT_OK(algorithm.PlanPlacement(addInput, result));

    EXPECT_EQ(GetTokens(result.next, WORKER_A), GetTokens(current.next, WORKER_A));
    EXPECT_EQ(GetTokens(result.next, WORKER_B), GetTokens(current.next, WORKER_B));
    ASSERT_EQ(GetTokens(result.next, WORKER_C).size(), PLANNING_VIRTUAL_TOKEN_NUM);
    ASSERT_EQ(GetTokens(result.next, "worker-d").size(), PLANNING_VIRTUAL_TOKEN_NUM);
    EXPECT_FALSE(result.ownerChanges.empty());

    std::set<WorkerId> changedTargets;
    for (const auto &change : result.ownerChanges) {
        changedTargets.insert(change.toWorkerId);
    }
    EXPECT_TRUE(changedTargets.count(WORKER_C) > 0);
    EXPECT_TRUE(changedTargets.count("worker-d") > 0);
}

TEST(TopologyHashAlgorithmTest, HashPlanRemovesOneOrManyMembers)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B, WORKER_C, "worker-d" });
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    PlanInput removeInput;
    removeInput.current = current.next;
    removeInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_C });
    PlanResult result;
    DS_ASSERT_OK(algorithm.PlanPlacement(removeInput, result));

    EXPECT_TRUE(HasWorker(result.next, WORKER_A));
    EXPECT_FALSE(HasWorker(result.next, WORKER_B));
    EXPECT_TRUE(HasWorker(result.next, WORKER_C));
    EXPECT_FALSE(HasWorker(result.next, "worker-d"));
    EXPECT_FALSE(result.ownerChanges.empty());

    std::set<WorkerId> removedSources;
    for (const auto &change : result.ownerChanges) {
        EXPECT_TRUE(change.toWorkerId == WORKER_A || change.toWorkerId == WORKER_C);
        removedSources.insert(change.fromWorkerId);
    }
    EXPECT_TRUE(removedSources.count(WORKER_B) > 0);
    EXPECT_TRUE(removedSources.count("worker-d") > 0);
}

TEST(TopologyHashAlgorithmTest, HashPlanHandlesUnavailableMemberAsTargetExclusion)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B, WORKER_C });
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    PlanInput failureInput;
    failureInput.current = current.next;
    failureInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_C });
    PlanResult result;
    DS_ASSERT_OK(algorithm.PlanPlacement(failureInput, result));

    EXPECT_FALSE(HasWorker(result.next, WORKER_B));
    ASSERT_FALSE(result.ownerChanges.empty());
    EXPECT_TRUE(std::any_of(result.ownerChanges.begin(), result.ownerChanges.end(),
                            [](const auto &change) { return change.fromWorkerId == WORKER_B; }));
}

TEST(TopologyHashAlgorithmTest, HashDiffPlacementProducesMinimalOwnerChanges)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    std::vector<OwnerChange> changes;
    DS_ASSERT_OK(algorithm.DiffPlacement(current.next, current.next, changes));
    EXPECT_TRUE(changes.empty());

    PlanInput addInput;
    addInput.current = current.next;
    addInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B, WORKER_C });
    PlanResult next;
    DS_ASSERT_OK(algorithm.PlanPlacement(addInput, next));
    DS_ASSERT_OK(algorithm.DiffPlacement(current.next, next.next, changes));
    EXPECT_EQ(changes.size(), next.ownerChanges.size());
    EXPECT_EQ(CountRanges(changes), CountRanges(next.ownerChanges));
    for (const auto &change : changes) {
        EXPECT_NE(change.fromWorkerId, change.toWorkerId);
        EXPECT_FALSE(change.ranges.empty());
    }

    auto from = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 10 } },
        { WORKER_B, WorkerTopologyState::ACTIVE, { 20 } },
    });
    auto to = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 10 } },
        { WORKER_C, WorkerTopologyState::ACTIVE, { 20 } },
    });
    DS_ASSERT_OK(algorithm.DiffPlacement(from, to, changes));
    ASSERT_EQ(changes.size(), 1ul);
    EXPECT_EQ(changes[0].fromWorkerId, WORKER_B);
    EXPECT_EQ(changes[0].toWorkerId, WORKER_C);
    ASSERT_EQ(changes[0].ranges.size(), 1ul);
    EXPECT_EQ(changes[0].ranges[0].begin, 11u);
    EXPECT_EQ(changes[0].ranges[0].end, 20u);
}

TEST(TopologyHashAlgorithmTest, HashValidatePlacementRejectsBadTopology)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    ValidateResult validation;
    DS_ASSERT_OK(algorithm.ValidatePlacement(current.next, validation));
    EXPECT_TRUE(validation.valid);
    EXPECT_TRUE(validation.diagnostics.empty());

    auto duplicateToken = current.next;
    duplicateToken.workers[1].tokens[0] = duplicateToken.workers[0].tokens[0];
    DS_ASSERT_OK(algorithm.ValidatePlacement(duplicateToken, validation));
    EXPECT_FALSE(validation.valid);
    EXPECT_FALSE(validation.diagnostics.empty());

    auto emptyOwner = current.next;
    emptyOwner.workers[0].tokens.clear();
    DS_ASSERT_OK(algorithm.ValidatePlacement(emptyOwner, validation));
    EXPECT_FALSE(validation.valid);

    HashAlgorithm emptyAlgorithmId("", PLANNING_VIRTUAL_TOKEN_NUM);
    DS_ASSERT_OK(emptyAlgorithmId.ValidatePlacement(current.next, validation));
    EXPECT_FALSE(validation.valid);
}

TEST(TopologyHashAlgorithmTest, HashValidatePlacementReportsAllMalformedFields)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    ValidateResult validation;

    TopologyDescriptor topology;
    topology.version = -1;
    topology.clusterHasInit = false;
    DS_ASSERT_OK(algorithm.ValidatePlacement(topology, validation));
    EXPECT_FALSE(validation.valid);
    EXPECT_GE(validation.diagnostics.size(), 3ul);

    topology.version = 1;
    topology.clusterHasInit = true;
    topology.workers = {
        { "", WorkerTopologyState::ACTIVE, { 1 } },
        { WORKER_A, WorkerTopologyState::ACTIVE, {} },
        { WORKER_A, WorkerTopologyState::ACTIVE, { 1 } },
        { WORKER_B, static_cast<WorkerTopologyState>(99), { 2 } },
    };
    DS_ASSERT_OK(algorithm.ValidatePlacement(topology, validation));
    EXPECT_FALSE(validation.valid);
    EXPECT_GE(validation.diagnostics.size(), 4ul);

    topology.workers = {
        { WORKER_A, WorkerTopologyState::JOINING, { 1 } },
        { WORKER_B, WorkerTopologyState::LEAVING, { 2 } },
    };
    DS_ASSERT_OK(algorithm.ValidatePlacement(topology, validation));
    EXPECT_FALSE(validation.valid);
    EXPECT_TRUE(std::any_of(validation.diagnostics.begin(), validation.diagnostics.end(),
                            [](const auto &message) { return message == "topology has no active worker"; }));
}

TEST(TopologyHashAlgorithmTest, HashPlanningDoesNotTouchPbOrRepository)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput input;
    input.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    PlanResult result;
    DS_ASSERT_OK(algorithm.InitPlacement(input, result));
    EXPECT_EQ(result.algorithmId, HASH_ALGORITHM_ID);
    EXPECT_TRUE(result.diagnostics.empty());
}

TEST(TopologyHashAlgorithmTest, HashPlanInitializesWhenCommittedTopologyIsMissing)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput input;
    input.current.version = 9;
    input.current.clusterHasInit = true;
    input.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });

    PlanResult result;
    DS_ASSERT_OK(algorithm.PlanPlacement(input, result));
    EXPECT_EQ(result.next.version, 10);
    EXPECT_TRUE(result.next.clusterHasInit);
    ASSERT_EQ(result.next.workers.size(), 2ul);
    ExpectNoDuplicatedTokens(result.next);
}

TEST(TopologyHashAlgorithmTest, HashPlanReprobesWhenGeneratedTokenAlreadyExists)
{
    constexpr char WORKER_D[] = "worker-d";
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, 1);
    auto collidingToken = MurmurHash3_32(std::string(WORKER_D) + "#0");
    PlanInput input;
    input.current = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { collidingToken } },
    });
    input.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_D });

    PlanResult result;
    DS_ASSERT_OK(algorithm.PlanPlacement(input, result));
    auto workerDTokens = GetTokens(result.next, WORKER_D);
    ASSERT_EQ(workerDTokens.size(), 1ul);
    EXPECT_NE(workerDTokens[0], collidingToken);
    EXPECT_EQ(workerDTokens[0], MurmurHash3_32(std::string(WORKER_D) + "#0#1"));
    ExpectNoDuplicatedTokens(result.next);
}

TEST(TopologyHashAlgorithmTest, HashPlanningScaleBudget)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds(0, PLANNING_SCALE_BASE_WORKER_NUM);
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    PlanInput addInput;
    addInput.current = current.next;
    addInput.targetWorkerIds = MakeWorkerIds(0, PLANNING_SCALE_BASE_WORKER_NUM + PLANNING_SCALE_CHANGE_WORKER_NUM);
    auto begin = std::chrono::steady_clock::now();
    PlanResult scaleOut;
    DS_ASSERT_OK(algorithm.PlanPlacement(addInput, scaleOut));
    auto scaleOutMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);
    EXPECT_LT(scaleOutMs.count(), PLANNING_BUDGET_MS);
    EXPECT_EQ(scaleOut.next.workers.size(), addInput.targetWorkerIds.size());
    EXPECT_FALSE(scaleOut.ownerChanges.empty());

    PlanInput removeInput;
    removeInput.current = scaleOut.next;
    removeInput.targetWorkerIds = MakeWorkerIds(0, PLANNING_SCALE_BASE_WORKER_NUM);
    begin = std::chrono::steady_clock::now();
    PlanResult scaleIn;
    DS_ASSERT_OK(algorithm.PlanPlacement(removeInput, scaleIn));
    auto scaleInMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin);
    EXPECT_LT(scaleInMs.count(), PLANNING_BUDGET_MS);
    EXPECT_EQ(scaleIn.next.workers.size(), removeInput.targetWorkerIds.size());
    EXPECT_FALSE(scaleIn.ownerChanges.empty());
}

TEST(TopologyHashAlgorithmTest, HashPlanningConcurrentReentrant)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    PlanInput initInput;
    initInput.targetWorkerIds = MakeWorkerIds(0, 64);
    PlanResult current;
    DS_ASSERT_OK(algorithm.InitPlacement(initInput, current));

    PlanInput input;
    input.current = current.next;
    input.targetWorkerIds = MakeWorkerIds(0, 80);

    std::vector<PlanResult> results(PLANNING_THREAD_NUM);
    std::vector<Status> statuses(PLANNING_THREAD_NUM);
    std::vector<std::thread> threads;
    threads.reserve(PLANNING_THREAD_NUM);
    for (int i = 0; i < PLANNING_THREAD_NUM; ++i) {
        threads.emplace_back(
            [&algorithm, &input, &results, &statuses, i] { statuses[i] = algorithm.PlanPlacement(input, results[i]); });
    }
    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &status : statuses) {
        DS_ASSERT_OK(status);
    }
    for (int i = 1; i < PLANNING_THREAD_NUM; ++i) {
        EXPECT_EQ(results[i].next.workers.size(), results[0].next.workers.size());
        EXPECT_EQ(results[i].ownerChanges.size(), results[0].ownerChanges.size());
        EXPECT_EQ(GetTokens(results[i].next, "worker-79"), GetTokens(results[0].next, "worker-79"));
    }
}

TEST(TopologyHashAlgorithmTest, HashPlanningRejectsMalformedInputs)
{
    PlanInput input;
    input.targetWorkerIds = MakeWorkerIds({ WORKER_A });
    PlanResult result;

    HashAlgorithm emptyAlgorithmId("", PLANNING_VIRTUAL_TOKEN_NUM);
    EXPECT_EQ(emptyAlgorithmId.InitPlacement(input, result).GetCode(), K_INVALID);

    HashAlgorithm zeroTokenNum(HASH_ALGORITHM_ID, 0);
    EXPECT_EQ(zeroTokenNum.InitPlacement(input, result).GetCode(), K_INVALID);

    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    input.targetWorkerIds.clear();
    EXPECT_EQ(algorithm.InitPlacement(input, result).GetCode(), K_INVALID);

    input.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_A });
    EXPECT_EQ(algorithm.InitPlacement(input, result).GetCode(), K_INVALID);

    input.targetWorkerIds = MakeWorkerIds({ WORKER_A, "" });
    EXPECT_EQ(algorithm.InitPlacement(input, result).GetCode(), K_INVALID);

    input.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    DS_ASSERT_OK(algorithm.InitPlacement(input, result));
    PlanInput zeroTokenPlanInput;
    zeroTokenPlanInput.current = result.next;
    zeroTokenPlanInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    EXPECT_EQ(zeroTokenNum.PlanPlacement(zeroTokenPlanInput, result).GetCode(), K_INVALID);

    PlanInput planInput;
    planInput.current = zeroTokenPlanInput.current;
    planInput.current.workers[1].tokens[0] = planInput.current.workers[0].tokens[0];
    planInput.targetWorkerIds = MakeWorkerIds({ WORKER_A, WORKER_B });
    EXPECT_EQ(algorithm.PlanPlacement(planInput, result).GetCode(), K_INVALID);
}

TEST(TopologyHashAlgorithmTest, HashDiffHandlesMaxTokenBoundary)
{
    HashAlgorithm algorithm(HASH_ALGORITHM_ID, PLANNING_VIRTUAL_TOKEN_NUM);
    auto from = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 10 } },
        { WORKER_B, WorkerTopologyState::ACTIVE, { std::numeric_limits<uint32_t>::max() } },
    });
    auto to = MakeTopology({
        { WORKER_A, WorkerTopologyState::ACTIVE, { 10 } },
        { WORKER_C, WorkerTopologyState::ACTIVE, { std::numeric_limits<uint32_t>::max() } },
    });

    std::vector<OwnerChange> changes;
    DS_ASSERT_OK(algorithm.DiffPlacement(from, to, changes));
    ASSERT_EQ(changes.size(), 1ul);
    EXPECT_EQ(changes[0].fromWorkerId, WORKER_B);
    EXPECT_EQ(changes[0].toWorkerId, WORKER_C);
    ASSERT_EQ(changes[0].ranges.size(), 1ul);
    EXPECT_EQ(changes[0].ranges[0].begin, 11u);
    EXPECT_EQ(changes[0].ranges[0].end, std::numeric_limits<uint32_t>::max());
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
