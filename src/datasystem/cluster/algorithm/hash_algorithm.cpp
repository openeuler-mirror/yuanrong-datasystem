/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Built-in MurmurHash3 cluster topology algorithm.
 */
#include "datasystem/cluster/algorithm/hash_algorithm.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <unordered_set>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem::cluster {
namespace {
constexpr char ALGORITHM_ID[] = "hash";
constexpr char TOKEN_SEPARATOR[] = "#";
constexpr uint32_t MAX_TOKEN_PROBES = 1'024;
constexpr size_t MAX_MEMBER_ADDRESS_BYTES = 1'024;
constexpr uint32_t MAX_TOKENS_PER_MEMBER = 4'096;

struct TokenOwner {
    uint32_t token;
    MemberIdentity identity;
};

bool IsCommitted(MemberState state)
{
    return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

Status NormalizeIdentities(std::vector<MemberIdentity> identities)
{
    std::sort(identities.begin(), identities.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    std::unordered_set<std::string> ids;
    std::unordered_set<std::string> addresses;
    for (const auto &identity : identities) {
        CHECK_FAIL_RETURN_STATUS(identity.id.size() == UUID_SIZE && !identity.address.empty()
                                     && identity.address.size() <= MAX_MEMBER_ADDRESS_BYTES,
                                 K_INVALID, "invalid planning member identity");
        HostPort endpoint;
        CHECK_FAIL_RETURN_STATUS(
            endpoint.ParseString(identity.address).IsOk() && endpoint.ToString() == identity.address, K_INVALID,
            "non-canonical planning member address");
        CHECK_FAIL_RETURN_STATUS(ids.insert(identity.id).second && addresses.insert(identity.address).second, K_INVALID,
                                 "duplicate planning member identity");
    }
    return Status::OK();
}

Status ValidateSelectedMembers(const TopologyState &current, const std::vector<MemberIdentity> &selected)
{
    for (const auto &identity : selected) {
        auto iter = std::find_if(current.members.begin(), current.members.end(),
                                 [&](const auto &member) { return member.identity.address == identity.address; });
        CHECK_FAIL_RETURN_STATUS(
            iter != current.members.end() && iter->identity == identity && IsCommitted(iter->state), K_INVALID,
            "selected member identity/state is stale");
    }
    return Status::OK();
}

uint32_t MakeToken(const std::string &address, uint32_t index, uint32_t probe)
{
    std::string seed = address + TOKEN_SEPARATOR + std::to_string(index);
    if (probe > 0) {
        seed += TOKEN_SEPARATOR + std::to_string(probe);
    }
    return MurmurHash3_32(seed);
}

Status GenerateTokens(const MemberIdentity &identity, uint32_t count, std::unordered_set<uint32_t> &occupied,
                      std::vector<uint32_t> &tokens)
{
    tokens.clear();
    tokens.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
        bool allocated = false;
        for (uint32_t probe = 0; probe < MAX_TOKEN_PROBES; ++probe) {
            auto token = MakeToken(identity.address, index, probe);
            if (occupied.insert(token).second) {
                tokens.emplace_back(token);
                allocated = true;
                break;
            }
        }
        CHECK_FAIL_RETURN_STATUS(allocated, K_INVALID, "unique cluster token probe budget exhausted");
    }
    std::sort(tokens.begin(), tokens.end());
    return Status::OK();
}

Status BuildOwners(const std::vector<Member> &members, bool includeJoining, const std::set<std::string> &excluded,
                   std::vector<TokenOwner> &owners, bool activeOnly = false)
{
    owners.clear();
    for (const auto &member : members) {
        const bool eligible =
            activeOnly ? member.state == MemberState::ACTIVE
                       : IsCommitted(member.state) || (includeJoining && member.state == MemberState::JOINING);
        if (!eligible || excluded.count(member.identity.address) > 0) {
            continue;
        }
        for (uint32_t token : member.tokens) {
            owners.push_back({ token, member.identity });
        }
    }
    CHECK_FAIL_RETURN_STATUS(!owners.empty(), K_NOT_READY, "cluster topology has no committed token owner");
    std::sort(owners.begin(), owners.end(),
              [](const auto &left, const auto &right) { return left.token < right.token; });
    return Status::OK();
}

const TokenOwner &FindOwner(const std::vector<TokenOwner> &owners, uint32_t token)
{
    auto iter = std::lower_bound(owners.begin(), owners.end(), token,
                                 [](const auto &owner, uint32_t value) { return owner.token < value; });
    return iter == owners.end() ? owners.front() : *iter;
}

Status LocateIndexedOwner(const std::vector<std::pair<uint32_t, const Member *>> &owners, uint32_t token,
                          const Member *&owner)
{
    CHECK_FAIL_RETURN_STATUS(!owners.empty(), K_NOT_READY, "cluster topology has no token owner");
    auto iter = std::lower_bound(owners.begin(), owners.end(), token,
                                 [](const auto &entry, uint32_t value) { return entry.first < value; });
    if (iter == owners.end()) {
        iter = owners.begin();
    }
    owner = iter->second;
    return Status::OK();
}

void AppendRange(std::vector<TokenRange> &ranges, uint32_t from, uint32_t end)
{
    if (!ranges.empty() && ranges.back().end != std::numeric_limits<uint32_t>::max() && ranges.back().end + 1 == from) {
        ranges.back().end = end;
    } else {
        ranges.emplace_back(TokenRange{ from, end });
    }
}

Status DiffOwners(const std::vector<TokenOwner> &fromOwners, const std::vector<TokenOwner> &toOwners,
                  std::vector<TopologyOwnerChange> &changes)
{
    std::vector<uint32_t> boundaries{ 0 };
    for (const auto &owner : fromOwners) {
        if (owner.token != std::numeric_limits<uint32_t>::max()) {
            boundaries.push_back(owner.token + 1);
        }
    }
    for (const auto &owner : toOwners) {
        if (owner.token != std::numeric_limits<uint32_t>::max()) {
            boundaries.push_back(owner.token + 1);
        }
    }
    std::sort(boundaries.begin(), boundaries.end());
    boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
    std::map<std::pair<std::string, std::string>, TopologyOwnerChange> grouped;
    for (size_t index = 0; index < boundaries.size(); ++index) {
        const uint32_t from = boundaries[index];
        const uint32_t end =
            index + 1 < boundaries.size() ? boundaries[index + 1] - 1 : std::numeric_limits<uint32_t>::max();
        const auto &source = FindOwner(fromOwners, from).identity;
        const auto &target = FindOwner(toOwners, from).identity;
        if (source == target) {
            continue;
        }
        auto &change = grouped[{ source.address, target.address }];
        change.source = source;
        change.target = target;
        AppendRange(change.ranges, from, end);
    }
    changes.clear();
    for (auto &[key, change] : grouped) {
        changes.emplace_back(std::move(change));
    }
    return Status::OK();
}
}  // namespace

TopologyAlgorithmId HashAlgorithm::GetId() const
{
    return ALGORITHM_ID;
}

uint32_t HashAlgorithm::Hash(std::string_view placementKey) const noexcept
{
    return MurmurHash3_32(reinterpret_cast<const uint8_t *>(placementKey.data()), placementKey.size());
}

Status HashAlgorithm::LocateOwner(const TopologySnapshot &snapshot, uint32_t token, const Member *&owner) const
{
    return LocateIndexedOwner(snapshot.committedTokenOwners_, token, owner);
}

Status HashAlgorithm::LocateProspectiveOwner(const TopologySnapshot &snapshot, uint32_t token,
                                             const Member *&owner) const
{
    const auto &owners =
        snapshot.prospectiveTokenOwners_.empty() ? snapshot.committedTokenOwners_ : snapshot.prospectiveTokenOwners_;
    return LocateIndexedOwner(owners, token, owner);
}

Status HashAlgorithm::AllocateTokens(const std::vector<MemberIdentity> &members, uint32_t tokensPerMember,
                                     std::unordered_map<std::string, std::vector<uint32_t>> &tokens) const
{
    CHECK_FAIL_RETURN_STATUS(tokensPerMember > 0 && tokensPerMember <= MAX_TOKENS_PER_MEMBER, K_INVALID,
                             "tokens per member is outside the supported range");
    RETURN_IF_NOT_OK(NormalizeIdentities(members));
    auto ordered = members;
    std::sort(ordered.begin(), ordered.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    std::unordered_set<uint32_t> occupied;
    std::unordered_map<std::string, std::vector<uint32_t>> allocated;
    for (const auto &identity : ordered) {
        RETURN_IF_NOT_OK(GenerateTokens(identity, tokensPerMember, occupied, allocated[identity.address]));
    }
    tokens = std::move(allocated);
    return Status::OK();
}

Status HashAlgorithm::BuildInitialPlacement(const ScaleOutPlanInput &input, TopologyPlan &plan) const
{
    CHECK_FAIL_RETURN_STATUS(input.current.members.empty() && !input.joining.empty(), K_INVALID,
                             "bootstrap requires an empty topology and members");
    std::unordered_map<std::string, std::vector<uint32_t>> tokens;
    RETURN_IF_NOT_OK(AllocateTokens(input.joining, input.tokensPerMember, tokens));
    TopologyPlan built;
    built.next = input.current;
    built.next.clusterHasInit = true;
    auto ordered = input.joining;
    std::sort(ordered.begin(), ordered.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    for (const auto &identity : ordered) {
        built.next.members.push_back({ identity, MemberState::ACTIVE, tokens[identity.address] });
    }
    plan = std::move(built);
    return Status::OK();
}

Status HashAlgorithm::Validate(const TopologyState &state) const
{
    std::unordered_set<std::string> ids;
    std::unordered_set<std::string> addresses;
    std::unordered_set<uint32_t> tokens;
    bool hasCommitted = false;
    for (const auto &member : state.members) {
        CHECK_FAIL_RETURN_STATUS(member.identity.id.size() == UUID_SIZE && !member.identity.address.empty(), K_INVALID,
                                 "invalid topology member identity");
        CHECK_FAIL_RETURN_STATUS(
            ids.insert(member.identity.id).second && addresses.insert(member.identity.address).second, K_INVALID,
            "duplicate topology member identity");
        hasCommitted = hasCommitted || IsCommitted(member.state);
        for (uint32_t token : member.tokens) {
            CHECK_FAIL_RETURN_STATUS(tokens.insert(token).second, K_INVALID, "duplicate topology token");
        }
    }
    CHECK_FAIL_RETURN_STATUS(state.members.empty() || hasCommitted, K_INVALID,
                             "non-empty topology has no committed member");
    return Status::OK();
}

Status HashAlgorithm::PlanScaleOut(const ScaleOutPlanInput &input, TopologyPlan &plan) const
{
    RETURN_IF_NOT_OK(Validate(input.current));
    RETURN_IF_NOT_OK(NormalizeIdentities(input.joining));
    CHECK_FAIL_RETURN_STATUS(
        !input.joining.empty() && input.tokensPerMember > 0 && input.tokensPerMember <= MAX_TOKENS_PER_MEMBER,
        K_INVALID, "ScaleOut requires joining members and tokens");
    TopologyPlan built;
    built.next = input.current;
    std::unordered_set<uint32_t> occupied;
    for (const auto &member : built.next.members) {
        occupied.insert(member.tokens.begin(), member.tokens.end());
    }
    auto ordered = input.joining;
    std::sort(ordered.begin(), ordered.end(),
              [](const auto &left, const auto &right) { return left.address < right.address; });
    for (const auto &identity : ordered) {
        auto iter = std::find_if(built.next.members.begin(), built.next.members.end(),
                                 [&](const auto &member) { return member.identity.address == identity.address; });
        if (iter != built.next.members.end()) {
            CHECK_FAIL_RETURN_STATUS(iter->identity == identity && iter->state == MemberState::JOINING, K_INVALID,
                                     "retained ScaleOut identity/state mismatch");
            continue;
        }
        const auto idCollision = [&](const auto &member) { return member.identity.id == identity.id; };
        CHECK_FAIL_RETURN_STATUS(std::none_of(built.next.members.begin(), built.next.members.end(), idCollision),
                                 K_INVALID, "new ScaleOut member id collides with current topology");
        std::vector<uint32_t> memberTokens;
        RETURN_IF_NOT_OK(GenerateTokens(identity, input.tokensPerMember, occupied, memberTokens));
        built.next.members.push_back({ identity, MemberState::JOINING, std::move(memberTokens) });
    }
    std::vector<TokenOwner> fromOwners;
    std::vector<TokenOwner> toOwners;
    RETURN_IF_NOT_OK(BuildOwners(input.current.members, false, {}, fromOwners));
    RETURN_IF_NOT_OK(BuildOwners(built.next.members, true, {}, toOwners));
    RETURN_IF_NOT_OK(DiffOwners(fromOwners, toOwners, built.ownerChanges));
    plan = std::move(built);
    return Status::OK();
}

Status HashAlgorithm::PlanScaleIn(const ScaleInPlanInput &input, TopologyPlan &plan) const
{
    RETURN_IF_NOT_OK(Validate(input.current));
    RETURN_IF_NOT_OK(NormalizeIdentities(input.leaving));
    RETURN_IF_NOT_OK(ValidateSelectedMembers(input.current, input.leaving));
    CHECK_FAIL_RETURN_STATUS(!input.leaving.empty(), K_INVALID, "ScaleIn requires leaving members");
    TopologyPlan built;
    built.next = input.current;
    std::set<std::string> excluded;
    for (const auto &identity : input.leaving) {
        excluded.insert(identity.address);
    }
    for (auto &member : built.next.members) {
        if (excluded.count(member.identity.address) > 0) {
            member.state = MemberState::PRE_LEAVING;
        }
    }
    std::vector<TokenOwner> fromOwners;
    std::vector<TokenOwner> toOwners;
    RETURN_IF_NOT_OK(BuildOwners(input.current.members, false, {}, fromOwners));
    RETURN_IF_NOT_OK(BuildOwners(input.current.members, false, excluded, toOwners));
    RETURN_IF_NOT_OK(DiffOwners(fromOwners, toOwners, built.ownerChanges));
    plan = std::move(built);
    return Status::OK();
}

Status HashAlgorithm::PlanFailure(const FailurePlanInput &input, TopologyPlan &plan) const
{
    RETURN_IF_NOT_OK(Validate(input.current));
    RETURN_IF_NOT_OK(NormalizeIdentities(input.failed));
    RETURN_IF_NOT_OK(ValidateSelectedMembers(input.current, input.failed));
    CHECK_FAIL_RETURN_STATUS(!input.failed.empty(), K_INVALID, "Failure planning requires failed members");
    TopologyPlan built;
    built.next = input.current;
    std::set<std::string> excluded;
    for (const auto &identity : input.failed) {
        excluded.insert(identity.address);
    }
    for (auto &member : built.next.members) {
        if (excluded.count(member.identity.address) > 0) {
            member.state = MemberState::FAILED;
        }
    }
    std::vector<TokenOwner> fromOwners;
    std::vector<TokenOwner> toOwners;
    RETURN_IF_NOT_OK(BuildOwners(input.current.members, false, {}, fromOwners));
    RETURN_IF_NOT_OK(BuildOwners(input.current.members, false, excluded, toOwners, true));
    RETURN_IF_NOT_OK(DiffOwners(fromOwners, toOwners, built.ownerChanges));
    plan = std::move(built);
    return Status::OK();
}

}  // namespace datasystem::cluster
