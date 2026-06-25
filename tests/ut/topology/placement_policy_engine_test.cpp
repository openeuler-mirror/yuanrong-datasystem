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
 * Description: Placement policy engine tests.
 */
#include "datasystem/topology/algorithm/placement_policy_engine.h"

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char HASH_ALGORITHM_ID[] = "hash";

PlacementPolicyRule MakeRule(PlacementPolicyId policyId, PlacementPolicyMatchType matchType, std::string pattern,
                             uint32_t priority)
{
    PlacementPolicyRule rule;
    rule.policyId = std::move(policyId);
    rule.matchType = matchType;
    rule.matchPattern = std::move(pattern);
    rule.priority = priority;
    rule.algorithmId = HASH_ALGORITHM_ID;
    return rule;
}

TEST(PlacementPolicyEngineTest, PlacementPolicyDeterministicSelection)
{
    PlacementPolicyEngine engine;
    RouteContext context;
    context.objectKey = "tenant-a/orders/42";
    context.namespaceId = "tenant-a";

    std::vector<PlacementPolicyRule> rules = {
        MakeRule("catch-all", PlacementPolicyMatchType::CATCH_ALL, "", 0),
        MakeRule("suffix", PlacementPolicyMatchType::SUFFIX, "/42", 10),
        MakeRule("prefix", PlacementPolicyMatchType::PREFIX, "tenant-a/orders", 10),
        MakeRule("namespace", PlacementPolicyMatchType::NAMESPACE, "tenant-a", 10),
    };
    PlacementPolicyRule selected;
    DS_ASSERT_OK(engine.SelectPolicy(context, rules, selected));
    EXPECT_EQ(selected.policyId, "prefix");

    rules.push_back(MakeRule("exact-b", PlacementPolicyMatchType::EXACT_KEY, context.objectKey, 10));
    rules.push_back(MakeRule("exact-a", PlacementPolicyMatchType::EXACT_KEY, context.objectKey, 10));
    DS_ASSERT_OK(engine.SelectPolicy(context, rules, selected));
    EXPECT_EQ(selected.policyId, "exact-a");

    rules.push_back(MakeRule("priority-wins", PlacementPolicyMatchType::PREFIX, "tenant-a", 20));
    DS_ASSERT_OK(engine.SelectPolicy(context, rules, selected));
    EXPECT_EQ(selected.policyId, "priority-wins");
}

TEST(PlacementPolicyEngineTest, PlacementPolicyRejectsInvalidRules)
{
    PlacementPolicyEngine engine;
    RouteContext context;
    context.objectKey = "object";
    PlacementPolicyRule selected;

    EXPECT_EQ(engine.SelectPolicy(context, {}, selected).GetCode(), K_NOT_FOUND);

    auto rule = MakeRule("dup", PlacementPolicyMatchType::CATCH_ALL, "", 0);
    std::vector<PlacementPolicyRule> rules = { rule, rule };
    EXPECT_EQ(engine.SelectPolicy(context, rules, selected).GetCode(), K_INVALID);

    rules = { MakeRule("empty-algorithm", PlacementPolicyMatchType::CATCH_ALL, "", 0) };
    rules[0].algorithmId.clear();
    EXPECT_EQ(engine.SelectPolicy(context, rules, selected).GetCode(), K_INVALID);

    rules = { MakeRule("bad-catch-all", PlacementPolicyMatchType::CATCH_ALL, "not-empty", 0) };
    EXPECT_EQ(engine.SelectPolicy(context, rules, selected).GetCode(), K_INVALID);

    rules = { MakeRule("bad-prefix", PlacementPolicyMatchType::PREFIX, "", 0) };
    EXPECT_EQ(engine.SelectPolicy(context, rules, selected).GetCode(), K_INVALID);

    rules = { MakeRule("unsupported", PlacementPolicyMatchType::CUSTOM, "custom", 0) };
    EXPECT_EQ(engine.SelectPolicy(context, rules, selected).GetCode(), K_INVALID);
}

TEST(PlacementPolicyEngineTest, PlacementPolicyReturnsNotFoundWhenNoRuleMatches)
{
    PlacementPolicyEngine engine;
    RouteContext context;
    context.objectKey = "object";
    context.namespaceId = "tenant-a";
    std::vector<PlacementPolicyRule> rules = {
        MakeRule("prefix", PlacementPolicyMatchType::PREFIX, "other", 0),
        MakeRule("suffix", PlacementPolicyMatchType::SUFFIX, ".bin", 0),
        MakeRule("namespace", PlacementPolicyMatchType::NAMESPACE, "tenant-b", 0),
    };

    PlacementPolicyRule selected;
    EXPECT_EQ(engine.SelectPolicy(context, rules, selected).GetCode(), K_NOT_FOUND);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem
