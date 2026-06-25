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
 * Description: Placement policy selection engine.
 */
#include "datasystem/topology/algorithm/placement_policy_engine.h"

#include <unordered_set>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

constexpr uint32_t EXACT_KEY_SPECIFICITY = 50;
constexpr uint32_t PREFIX_SPECIFICITY = 40;
constexpr uint32_t SUFFIX_SPECIFICITY = 30;
constexpr uint32_t NAMESPACE_SPECIFICITY = 20;
constexpr uint32_t CATCH_ALL_SPECIFICITY = 0;

Status MatchRule(const RouteContext &context, const PlacementPolicyRule &rule, bool &matched, uint32_t &specificity)
{
    matched = false;
    specificity = CATCH_ALL_SPECIFICITY;
    CHECK_FAIL_RETURN_STATUS(!rule.policyId.empty(), K_INVALID, "policy id is empty");
    CHECK_FAIL_RETURN_STATUS(!rule.algorithmId.empty(), K_INVALID, "policy algorithm id is empty");

    switch (rule.matchType) {
        case PlacementPolicyMatchType::CATCH_ALL:
            CHECK_FAIL_RETURN_STATUS(rule.matchPattern.empty(), K_INVALID, "catch-all pattern must be empty");
            matched = true;
            specificity = CATCH_ALL_SPECIFICITY;
            return Status::OK();
        case PlacementPolicyMatchType::EXACT_KEY:
            CHECK_FAIL_RETURN_STATUS(!rule.matchPattern.empty(), K_INVALID, "exact pattern is empty");
            matched = context.objectKey == rule.matchPattern;
            specificity = EXACT_KEY_SPECIFICITY;
            return Status::OK();
        case PlacementPolicyMatchType::PREFIX:
            CHECK_FAIL_RETURN_STATUS(!rule.matchPattern.empty(), K_INVALID, "prefix pattern is empty");
            matched = context.objectKey.rfind(rule.matchPattern, 0) == 0;
            specificity = PREFIX_SPECIFICITY;
            return Status::OK();
        case PlacementPolicyMatchType::SUFFIX:
            CHECK_FAIL_RETURN_STATUS(!rule.matchPattern.empty(), K_INVALID, "suffix pattern is empty");
            matched = context.objectKey.size() >= rule.matchPattern.size()
                      && context.objectKey.compare(context.objectKey.size() - rule.matchPattern.size(),
                                                   rule.matchPattern.size(), rule.matchPattern)
                             == 0;
            specificity = SUFFIX_SPECIFICITY;
            return Status::OK();
        case PlacementPolicyMatchType::NAMESPACE:
            CHECK_FAIL_RETURN_STATUS(!rule.matchPattern.empty(), K_INVALID, "namespace pattern is empty");
            matched = context.namespaceId == rule.matchPattern;
            specificity = NAMESPACE_SPECIFICITY;
            return Status::OK();
        default:
            RETURN_STATUS(K_INVALID, "unsupported placement policy match type");
    }
}

bool IsBetterRule(const PlacementPolicyRule &candidate, uint32_t candidateSpecificity,
                  const PlacementPolicyRule &current, uint32_t currentSpecificity)
{
    if (candidate.priority != current.priority) {
        return candidate.priority > current.priority;
    }
    if (candidateSpecificity != currentSpecificity) {
        return candidateSpecificity > currentSpecificity;
    }
    return candidate.policyId < current.policyId;
}

}  // namespace

Status PlacementPolicyEngine::SelectPolicy(const RouteContext &context, const std::vector<PlacementPolicyRule> &rules,
                                           PlacementPolicyRule &rule) const
{
    rule = {};
    CHECK_FAIL_RETURN_STATUS(!rules.empty(), K_NOT_FOUND, "placement policy rule list is empty");

    std::unordered_set<PlacementPolicyId> policyIds;
    bool hasMatch = false;
    uint32_t selectedSpecificity = CATCH_ALL_SPECIFICITY;
    for (const auto &candidate : rules) {
        CHECK_FAIL_RETURN_STATUS(policyIds.insert(candidate.policyId).second, K_INVALID,
                                 "duplicated placement policy id");
        bool matched = false;
        uint32_t specificity = CATCH_ALL_SPECIFICITY;
        RETURN_IF_NOT_OK(MatchRule(context, candidate, matched, specificity));
        if (!matched) {
            continue;
        }
        if (!hasMatch || IsBetterRule(candidate, specificity, rule, selectedSpecificity)) {
            rule = candidate;
            selectedSpecificity = specificity;
            hasMatch = true;
        }
    }
    CHECK_FAIL_RETURN_STATUS(hasMatch, K_NOT_FOUND, "no placement policy rule matched");
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
