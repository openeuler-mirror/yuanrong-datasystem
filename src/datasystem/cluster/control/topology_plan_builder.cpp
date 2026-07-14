/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Pure cluster topology state-transition construction.
 */
#include "datasystem/cluster/control/topology_plan_builder.h"

#include <algorithm>
#include <unordered_set>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr uint32_t DEFAULT_TOKENS_PER_MEMBER = 4;

bool SameIdentity(const Member &member, const MemberIdentity &identity)
{
    return member.identity == identity;
}

bool IsCommittedState(MemberState state)
{
    return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

Status ValidateSelected(const TopologyState &latest, const std::vector<MemberIdentity> &selected, MemberState state)
{
    CHECK_FAIL_RETURN_STATUS(!selected.empty(), K_INVALID, "selected topology member set is empty");
    std::unordered_set<std::string> addresses;
    for (const auto &identity : selected) {
        auto iter = std::find_if(latest.members.begin(), latest.members.end(),
                                 [&](const auto &member) { return SameIdentity(member, identity); });
        CHECK_FAIL_RETURN_STATUS(iter != latest.members.end() && iter->state == state, K_INVALID,
                                 "selected topology identity or state is stale");
        CHECK_FAIL_RETURN_STATUS(addresses.insert(identity.address).second, K_INVALID,
                                 "selected topology identity is duplicated");
    }
    return Status::OK();
}

void AdvanceEpoch(const TopologyState &latest, TopologyChangeType type, TopologyPlan &plan)
{
    plan.next.version = latest.version + 1;
    plan.next.activeBatch = ActiveBatch{ type, plan.next.version };
}

std::vector<MemberIdentity> MembersInState(const TopologyState &state, MemberState selected)
{
    std::vector<MemberIdentity> identities;
    for (const auto &member : state.members) {
        if (member.state == selected) {
            identities.push_back(member.identity);
        }
    }
    return identities;
}

void RemoveMembers(TopologyState &state, const std::unordered_set<std::string> &addresses)
{
    state.members.erase(
        std::remove_if(state.members.begin(), state.members.end(),
                       [&](const auto &member) { return addresses.count(member.identity.address) > 0; }),
        state.members.end());
}

Status ValidateBatch(const TopologyState &latest, TopologyChangeType type)
{
    CHECK_FAIL_RETURN_STATUS(latest.activeBatch.has_value() && latest.activeBatch->type == type, K_INVALID,
                             "topology active batch type does not match finalization");
    return Status::OK();
}
}  // namespace

TopologyPlanBuilder::TopologyPlanBuilder(const IPlanningAlgorithm &algorithm) : algorithm_(algorithm)
{
}

Status TopologyPlanBuilder::BuildBootstrap(const TopologyState &latest, const std::vector<MemberIdentity> &ready,
                                           TopologyState &next) const
{
    const bool hasCommitted = std::any_of(latest.members.begin(), latest.members.end(),
                                          [](const auto &member) { return IsCommittedState(member.state); });
    CHECK_FAIL_RETURN_STATUS(!hasCommitted && !latest.activeBatch.has_value(), K_INVALID,
                             "bootstrap requires a stable topology without a committed owner");
    RETURN_IF_NOT_OK(ValidateSelected(latest, ready, MemberState::INITIAL));
    TopologyPlan built;
    RETURN_IF_NOT_OK(algorithm_.BuildInitialPlacement({ TopologyState{}, ready, DEFAULT_TOKENS_PER_MEMBER }, built));
    std::unordered_set<std::string> readyAddresses;
    for (const auto &identity : ready) {
        readyAddresses.insert(identity.address);
    }
    for (const auto &member : latest.members) {
        if (readyAddresses.count(member.identity.address) == 0) {
            built.next.members.push_back(member);
        }
    }
    built.next.version = latest.version + 1;
    built.next.clusterHasInit = latest.clusterHasInit || built.next.clusterHasInit;
    next = std::move(built.next);
    return algorithm_.Validate(next);
}

Status TopologyPlanBuilder::BuildScaleOutStart(const TopologyState &latest, const std::vector<MemberIdentity> &selected,
                                               TopologyPlan &plan) const
{
    CHECK_FAIL_RETURN_STATUS(latest.clusterHasInit && !latest.activeBatch.has_value(), K_INVALID,
                             "ScaleOut start requires a stable initialized topology");
    RETURN_IF_NOT_OK(ValidateSelected(latest, selected, MemberState::INITIAL));
    TopologyState current = latest;
    std::unordered_set<std::string> selectedAddresses;
    for (const auto &identity : selected) {
        selectedAddresses.insert(identity.address);
    }
    RemoveMembers(current, selectedAddresses);
    TopologyPlan built;
    RETURN_IF_NOT_OK(algorithm_.PlanScaleOut({ current, selected, DEFAULT_TOKENS_PER_MEMBER }, built));
    AdvanceEpoch(latest, TopologyChangeType::SCALE_OUT, built);
    RETURN_IF_NOT_OK(algorithm_.Validate(built.next));
    plan = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildScaleOutReplan(const TopologyState &latest,
                                                const std::vector<MemberIdentity> &failedJoining,
                                                TopologyPlan &plan) const
{
    RETURN_IF_NOT_OK(ValidateBatch(latest, TopologyChangeType::SCALE_OUT));
    RETURN_IF_NOT_OK(ValidateSelected(latest, failedJoining, MemberState::JOINING));
    TopologyState retained = latest;
    std::unordered_set<std::string> failedAddresses;
    for (const auto &identity : failedJoining) {
        failedAddresses.insert(identity.address);
    }
    RemoveMembers(retained, failedAddresses);
    auto joining = MembersInState(retained, MemberState::JOINING);
    TopologyPlan built;
    if (!joining.empty()) {
        RETURN_IF_NOT_OK(algorithm_.PlanScaleOut({ retained, joining, DEFAULT_TOKENS_PER_MEMBER }, built));
        AdvanceEpoch(latest, TopologyChangeType::SCALE_OUT, built);
    } else {
        built.next = std::move(retained);
        built.next.version = latest.version + 1;
        built.next.activeBatch.reset();
    }
    plan = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildScaleInStart(const TopologyState &latest, const std::vector<MemberIdentity> &selected,
                                              TopologyPlan &plan) const
{
    CHECK_FAIL_RETURN_STATUS(latest.clusterHasInit && !latest.activeBatch.has_value(), K_INVALID,
                             "ScaleIn start requires a stable initialized topology");
    RETURN_IF_NOT_OK(ValidateSelected(latest, selected, MemberState::PRE_LEAVING));
    TopologyPlan built;
    RETURN_IF_NOT_OK(algorithm_.PlanScaleIn({ latest, selected }, built));
    for (auto &member : built.next.members) {
        if (member.state == MemberState::PRE_LEAVING
            && std::find(selected.begin(), selected.end(), member.identity) != selected.end()) {
            member.state = MemberState::LEAVING;
        }
    }
    AdvanceEpoch(latest, TopologyChangeType::SCALE_IN, built);
    plan = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildFailureStartOrReplan(const TopologyState &latest,
                                                      const std::vector<MemberIdentity> &confirmed,
                                                      TopologyPlan &plan) const
{
    CHECK_FAIL_RETURN_STATUS(latest.clusterHasInit, K_INVALID, "Failure requires an initialized topology");
    TopologyState planning = latest;
    for (auto &member : planning.members) {
        if (member.state == MemberState::FAILED) {
            member.state = MemberState::ACTIVE;
        }
    }
    for (const auto &member : latest.members) {
        if (member.state != MemberState::FAILED) {
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(std::find(confirmed.begin(), confirmed.end(), member.identity) != confirmed.end(),
                                 K_INVALID, "Failure replan cannot omit an existing failed member");
    }
    std::unordered_set<std::string> failedAddresses;
    for (const auto &identity : confirmed) {
        failedAddresses.insert(identity.address);
    }
    const bool hasHealthyOwner = std::any_of(latest.members.begin(), latest.members.end(), [&](const auto &member) {
        return member.state == MemberState::ACTIVE && failedAddresses.count(member.identity.address) == 0;
    });
    if (!hasHealthyOwner) {
        TopologyPlan built;
        built.next = latest;
        for (auto &member : built.next.members) {
            if (failedAddresses.count(member.identity.address) > 0) {
                member.state = MemberState::FAILED;
            }
        }
        AdvanceEpoch(latest, TopologyChangeType::FAILURE, built);
        plan = std::move(built);
        return Status::OK();
    }
    TopologyPlan built;
    auto status = algorithm_.PlanFailure({ planning, confirmed }, built);
    if (status.IsOk()) {
        for (const auto &member : latest.members) {
            const bool preserveOrdinaryFact =
                (member.state == MemberState::JOINING || member.state == MemberState::LEAVING)
                && failedAddresses.count(member.identity.address) == 0;
            if (preserveOrdinaryFact) {
                auto iter = std::find_if(built.next.members.begin(), built.next.members.end(),
                                         [&](const auto &next) { return next.identity == member.identity; });
                if (iter != built.next.members.end()) {
                    iter->state = member.state;
                }
            }
        }
    } else {
        return status;
    }
    AdvanceEpoch(latest, TopologyChangeType::FAILURE, built);
    plan = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildScaleOutFinal(const TopologyState &latest, TopologyState &next) const
{
    RETURN_IF_NOT_OK(ValidateBatch(latest, TopologyChangeType::SCALE_OUT));
    CHECK_FAIL_RETURN_STATUS(!MembersInState(latest, MemberState::JOINING).empty(), K_INVALID,
                             "ScaleOut final requires joining members");
    TopologyState built = latest;
    for (auto &member : built.members) {
        if (member.state == MemberState::JOINING) {
            member.state = MemberState::ACTIVE;
        }
    }
    built.version = latest.version + 1;
    built.activeBatch.reset();
    next = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildScaleInFinal(const TopologyState &latest, TopologyState &next) const
{
    RETURN_IF_NOT_OK(ValidateBatch(latest, TopologyChangeType::SCALE_IN));
    auto leaving = MembersInState(latest, MemberState::LEAVING);
    CHECK_FAIL_RETURN_STATUS(!leaving.empty(), K_INVALID, "ScaleIn final requires leaving members");
    std::unordered_set<std::string> addresses;
    for (const auto &identity : leaving) {
        addresses.insert(identity.address);
    }
    TopologyState built = latest;
    RemoveMembers(built, addresses);
    built.version = latest.version + 1;
    built.activeBatch.reset();
    next = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildFailureFinal(const TopologyState &latest, TopologyState &next) const
{
    RETURN_IF_NOT_OK(ValidateBatch(latest, TopologyChangeType::FAILURE));
    auto failed = MembersInState(latest, MemberState::FAILED);
    CHECK_FAIL_RETURN_STATUS(!failed.empty(), K_INVALID, "Failure final requires failed members");
    std::unordered_set<std::string> addresses;
    for (const auto &identity : failed) {
        addresses.insert(identity.address);
    }
    TopologyState built = latest;
    RemoveMembers(built, addresses);
    auto committed = std::count_if(built.members.begin(), built.members.end(), [](const auto &member) {
        return member.state == MemberState::ACTIVE || member.state == MemberState::LEAVING
               || member.state == MemberState::PRE_LEAVING;
    });
    if (committed == 0) {
        built.members.clear();
        built.activeBatch.reset();
    } else if (!MembersInState(built, MemberState::JOINING).empty()) {
        built.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_OUT, latest.version + 1 };
    } else if (!MembersInState(built, MemberState::LEAVING).empty()) {
        built.activeBatch = ActiveBatch{ TopologyChangeType::SCALE_IN, latest.version + 1 };
    } else {
        built.activeBatch.reset();
    }
    built.version = latest.version + 1;
    next = std::move(built);
    return Status::OK();
}

Status TopologyPlanBuilder::BuildClusterShutdownFinal(const TopologyState &latest, TopologyState &next) const
{
    CHECK_FAIL_RETURN_STATUS(!latest.members.empty(), K_INVALID, "cluster shutdown requires topology members");
    const bool allExiting = std::all_of(latest.members.begin(), latest.members.end(), [](const auto &member) {
        return member.state == MemberState::PRE_LEAVING || member.state == MemberState::LEAVING;
    });
    CHECK_FAIL_RETURN_STATUS(allExiting, K_INVALID, "cluster shutdown requires every member to be exiting");
    next = latest;
    next.version = latest.version + 1;
    next.members.clear();
    next.activeBatch.reset();
    return Status::OK();
}

}  // namespace datasystem::cluster
