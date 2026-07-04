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
 * Description: Topology algorithm catalog tests.
 */
#include "datasystem/topology/algorithm/algorithm_catalog.h"

#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/algorithm/hash_algorithm.h"
#include "datasystem/topology/algorithm/route_policy_engine.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char FAKE_ALGORITHM_ID[] = "fake-routing";
constexpr char HASH_ALGORITHM_ID[] = "hash";
constexpr char PLUGIN_ALGORITHM_ID[] = "prefix-plugin";
constexpr char PLUGIN_OWNER[] = "plugin-owner";

class FakeRoutingAlgorithm final : public IRoutingAlgorithm {
public:
    explicit FakeRoutingAlgorithm(AlgorithmId id) : id_(std::move(id))
    {
    }

    ~FakeRoutingAlgorithm() override = default;

    AlgorithmId GetAlgorithmId() const override
    {
        return id_;
    }

    Status BuildRoutingState(const TopologyDescriptor &snapshot,
                             std::unique_ptr<AlgorithmRoutingState> &routing) const override
    {
        routing = std::make_unique<AlgorithmRoutingState>();
        routing->algorithmId = id_;
        routing->topologyVersion = snapshot.version;
        return Status::OK();
    }

    Status BuildPlacementUnit(const RouteContext &context, const PlacementPolicyRule &policy,
                              PlacementUnit &unit) const override
    {
        CHECK_FAIL_RETURN_STATUS(!context.key.empty(), K_INVALID, "business key is empty");
        CHECK_FAIL_RETURN_STATUS(policy.algorithmId == id_, K_INVALID, "policy algorithm mismatch");
        unit.algorithmId = id_;
        unit.unitType = "fake-key";
        unit.opaqueUnit = context.key;
        return Status::OK();
    }

    Status Route(const AlgorithmRoutingState &routing, const PlacementUnit &unit, LogicalOwner &owner) const override
    {
        CHECK_FAIL_RETURN_STATUS(routing.algorithmId == id_, K_INVALID, "routing algorithm mismatch");
        CHECK_FAIL_RETURN_STATUS(unit.algorithmId == id_, K_INVALID, "unit algorithm mismatch");
        owner.nodeId = PLUGIN_OWNER;
        owner.topologyVersion = routing.topologyVersion;
        return Status::OK();
    }

private:
    AlgorithmId id_;
};

class IdOnlyAlgorithm final : public ITopologyAlgorithm {
public:
    explicit IdOnlyAlgorithm(AlgorithmId id) : id_(std::move(id))
    {
    }

    ~IdOnlyAlgorithm() override = default;

    AlgorithmId GetAlgorithmId() const override
    {
        return id_;
    }

private:
    AlgorithmId id_;
};

class FakePlanningAlgorithm final : public IPlanningAlgorithm {
public:
    explicit FakePlanningAlgorithm(AlgorithmId id) : id_(std::move(id))
    {
    }

    ~FakePlanningAlgorithm() override = default;

    AlgorithmId GetAlgorithmId() const override
    {
        return id_;
    }

    Status InitPlacement(const PlanInput &, PlanResult &result) const override
    {
        result.algorithmId = id_;
        return Status::OK();
    }

    Status PlanPlacement(const PlanInput &, PlanResult &result) const override
    {
        result.algorithmId = id_;
        return Status::OK();
    }

    Status DiffPlacement(const TopologyDescriptor &, const TopologyDescriptor &,
                         std::vector<OwnerChange> &changes) const override
    {
        changes.clear();
        return Status::OK();
    }

    Status ValidatePlacement(const TopologyDescriptor &, ValidateResult &result) const override
    {
        result.valid = true;
        return Status::OK();
    }

private:
    AlgorithmId id_;
};

TEST(AlgorithmCatalogTest, RoutingAlgorithmCatalogRegistration)
{
    AlgorithmCatalog registry;
    const IRoutingAlgorithm *algorithm = nullptr;

    EXPECT_EQ(registry.RegisterAlgorithm(std::unique_ptr<const ITopologyAlgorithm>{}).GetCode(), K_INVALID);
    EXPECT_EQ(registry.RegisterAlgorithm(std::make_unique<FakeRoutingAlgorithm>("")).GetCode(), K_INVALID);
    EXPECT_EQ(registry.ResolveRouting(FAKE_ALGORITHM_ID, algorithm).GetCode(), K_NOT_FOUND);

    auto fake = std::make_unique<FakeRoutingAlgorithm>(FAKE_ALGORITHM_ID);
    auto *raw = fake.get();
    DS_ASSERT_OK(registry.RegisterAlgorithm(std::move(fake)));
    DS_ASSERT_OK(registry.ResolveRouting(FAKE_ALGORITHM_ID, algorithm));
    EXPECT_EQ(algorithm, raw);
    EXPECT_EQ(algorithm->GetAlgorithmId(), FAKE_ALGORITHM_ID);

    EXPECT_EQ(registry.RegisterAlgorithm(std::make_unique<FakeRoutingAlgorithm>(FAKE_ALGORITHM_ID)).GetCode(),
              K_INVALID);
}

TEST(AlgorithmCatalogTest, RejectsAlgorithmWithoutRoutingOrPlanningFacet)
{
    AlgorithmCatalog registry;
    EXPECT_EQ(registry.RegisterAlgorithm(std::make_unique<IdOnlyAlgorithm>("id-only")).GetCode(), K_INVALID);
}

TEST(AlgorithmCatalogTest, PlanningOnlyAlgorithmResolvesOnlyPlanningFacet)
{
    AlgorithmCatalog registry;
    const IRoutingAlgorithm *routing = nullptr;
    const IPlanningAlgorithm *planning = nullptr;

    auto planningOnly = std::make_unique<FakePlanningAlgorithm>("planning-only");
    auto *raw = planningOnly.get();
    DS_ASSERT_OK(registry.RegisterAlgorithm(std::move(planningOnly)));
    EXPECT_EQ(registry.ResolveRouting("planning-only", routing).GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(registry.ResolvePlanning("planning-only", planning));
    EXPECT_EQ(planning, static_cast<const IPlanningAlgorithm *>(raw));
}

TEST(AlgorithmCatalogTest, PlanningAlgorithmCatalogFacet)
{
    AlgorithmCatalog registry;
    const IRoutingAlgorithm *routing = nullptr;
    const IPlanningAlgorithm *planning = nullptr;

    EXPECT_EQ(registry.ResolvePlanning(HASH_ALGORITHM_ID, planning).GetCode(), K_NOT_FOUND);
    EXPECT_EQ(registry.RegisterAlgorithm(std::make_unique<HashAlgorithm>("")).GetCode(), K_INVALID);

    auto hash = std::make_unique<HashAlgorithm>();
    auto *raw = hash.get();
    DS_ASSERT_OK(registry.RegisterAlgorithm(std::move(hash)));
    DS_ASSERT_OK(registry.ResolveRouting(HASH_ALGORITHM_ID, routing));
    DS_ASSERT_OK(registry.ResolvePlanning(HASH_ALGORITHM_ID, planning));
    EXPECT_EQ(routing, static_cast<const IRoutingAlgorithm *>(raw));
    EXPECT_EQ(planning, static_cast<const IPlanningAlgorithm *>(raw));

    EXPECT_EQ(registry.RegisterAlgorithm(std::make_unique<HashAlgorithm>()).GetCode(), K_INVALID);
}

TEST(AlgorithmCatalogTest, RoutingAlgorithmPluginNoFacadeChange)
{
    AlgorithmCatalog registry;
    DS_ASSERT_OK(registry.RegisterAlgorithm(std::make_unique<FakeRoutingAlgorithm>(PLUGIN_ALGORITHM_ID)));

    RoutePolicyEngine policyEngine;
    RouteContext context;
    context.key = "prefix-object";
    std::vector<PlacementPolicyRule> rules = {
        { "fallback", PlacementPolicyMatchType::CATCH_ALL, "", 0, FAKE_ALGORITHM_ID, "" },
        { "plugin", PlacementPolicyMatchType::PREFIX, "prefix-", 10, PLUGIN_ALGORITHM_ID, "" },
    };

    PlacementPolicyRule selected;
    DS_ASSERT_OK(policyEngine.SelectPolicy(context, rules, selected));
    EXPECT_EQ(selected.algorithmId, PLUGIN_ALGORITHM_ID);

    const IRoutingAlgorithm *algorithm = nullptr;
    DS_ASSERT_OK(registry.ResolveRouting(selected.algorithmId, algorithm));

    TopologyDescriptor topology;
    topology.version = 7;
    topology.members = { { PLUGIN_OWNER, TopologyNodeState::ACTIVE, { 1 } } };
    std::unique_ptr<AlgorithmRoutingState> state;
    DS_ASSERT_OK(algorithm->BuildRoutingState(topology, state));

    PlacementUnit unit;
    DS_ASSERT_OK(algorithm->BuildPlacementUnit(context, selected, unit));

    LogicalOwner owner;
    DS_ASSERT_OK(algorithm->Route(*state, unit, owner));
    EXPECT_EQ(owner.nodeId, PLUGIN_OWNER);
    EXPECT_EQ(owner.topologyVersion, topology.version);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
