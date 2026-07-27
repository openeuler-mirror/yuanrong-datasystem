/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Immutable validated cluster topology snapshot.
 */
#include "datasystem/cluster/model/topology_snapshot.h"

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <utility>

#include "datasystem/cluster/model/topology_diagnostics.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr size_t MEMBER_ID_SIZE = 16;
constexpr size_t SHA256_HEX_SIZE = 64;
constexpr size_t MAX_MEMBER_ADDRESS_BYTES = 1'024;

bool IsCommitted(MemberState state)
{
    return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
}

bool IsProspective(MemberState state, const std::optional<ActiveBatch> &batch)
{
    if (!batch.has_value()) {
        return false;
    }
    if (batch->type == TopologyChangeType::SCALE_OUT) {
        return IsCommitted(state) || state == MemberState::JOINING;
    }
    if (batch->type == TopologyChangeType::SCALE_IN) {
        return IsCommitted(state) && state != MemberState::LEAVING;
    }
    return false;
}

bool MatchesBatch(MemberState state, TopologyChangeType type)
{
    if (type == TopologyChangeType::SCALE_OUT) {
        return state != MemberState::LEAVING && state != MemberState::FAILED;
    }
    if (type == TopologyChangeType::SCALE_IN) {
        return state != MemberState::JOINING && state != MemberState::FAILED;
    }
    return true;
}

Status ValidateBatchStates(const TopologyState &state)
{
    if (!state.activeBatch.has_value()) {
        const auto transitional = [](const Member &member) {
            return member.state == MemberState::JOINING || member.state == MemberState::LEAVING
                   || member.state == MemberState::FAILED;
        };
        CHECK_FAIL_RETURN_STATUS(std::none_of(state.members.begin(), state.members.end(), transitional), K_INVALID,
                                 "stable topology contains transitional member");
        return Status::OK();
    }
    bool hasParticipant = false;
    for (const auto &member : state.members) {
        RETURN_IF_NOT_OK(MatchesBatch(member.state, state.activeBatch->type)
                             ? Status::OK()
                             : Status(K_INVALID, "member state does not match active batch"));
        hasParticipant =
            hasParticipant
            || (state.activeBatch->type == TopologyChangeType::SCALE_OUT && member.state == MemberState::JOINING)
            || (state.activeBatch->type == TopologyChangeType::SCALE_IN && member.state == MemberState::LEAVING)
            || (state.activeBatch->type == TopologyChangeType::FAILURE && member.state == MemberState::FAILED);
    }
    CHECK_FAIL_RETURN_STATUS(hasParticipant, K_INVALID, "active batch has no participant");
    return Status::OK();
}

Status ValidateAddress(const std::string &address)
{
    HostPort hostPort;
    CHECK_FAIL_RETURN_STATUS(!address.empty() && address.size() <= MAX_MEMBER_ADDRESS_BYTES
                                 && hostPort.ParseString(address).IsOk() && hostPort.ToString() == address,
                             K_INVALID, "invalid canonical member address");
    return Status::OK();
}

Status FindExactMember(const TopologySnapshot &snapshot, const MemberIdentity &identity, const Member *&member)
{
    CHECK_FAIL_RETURN_STATUS(!identity.id.empty() && !identity.address.empty(), K_INVALID,
                             "incomplete migration member identity");
    const Member *candidate = nullptr;
    auto rc = snapshot.FindMemberByAddress(identity.address, candidate);
    CHECK_FAIL_RETURN_STATUS(rc.IsOk() && candidate != nullptr && candidate->identity == identity, K_INVALID,
                             "migration member identity does not match topology");
    member = candidate;
    return Status::OK();
}

bool IsSha256Hex(const std::string &digest)
{
    return digest.size() == SHA256_HEX_SIZE &&
           std::all_of(digest.begin(), digest.end(), [](char value) {
               return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
           });
}
}  // namespace

Status TopologySnapshot::Create(TopologyState state, int64_t authorityRevision, std::string canonicalDigest,
                                std::shared_ptr<const TopologySnapshot> &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(state.version > 0 && state.version <= std::numeric_limits<int64_t>::max(), K_INVALID,
                             "invalid topology version");
    CHECK_FAIL_RETURN_STATUS(authorityRevision >= 0 && IsSha256Hex(canonicalDigest), K_INVALID,
                             "invalid topology evidence");
    if (state.activeBatch.has_value()) {
        CHECK_FAIL_RETURN_STATUS(state.activeBatch->epoch > 0 && state.activeBatch->epoch <= state.version, K_INVALID,
                                 "invalid active batch epoch");
    }
    RETURN_IF_NOT_OK(ValidateBatchStates(state));
    std::sort(state.members.begin(), state.members.end(),
              [](const Member &left, const Member &right) { return left.identity.address < right.identity.address; });
    for (auto &member : state.members) {
        std::sort(member.tokens.begin(), member.tokens.end());
    }
    auto candidate = std::shared_ptr<TopologySnapshot>(
        new TopologySnapshot(std::move(state), authorityRevision, std::move(canonicalDigest)));
    RETURN_IF_NOT_OK(candidate->BuildIndexes());
    snapshot = std::move(candidate);
    return Status::OK();
}

TopologySnapshot::TopologySnapshot(TopologyState state, int64_t authorityRevision, std::string canonicalDigest)
    : state_(std::move(state)), authorityRevision_(authorityRevision), canonicalDigest_(std::move(canonicalDigest))
{
}

Status TopologySnapshot::BuildIndexes()
{
    ReserveIndexes();
    RETURN_IF_NOT_OK(BuildIndexEntries());
    CHECK_FAIL_RETURN_STATUS(committedMembers_.empty() || !committedTokenOwners_.empty(), K_INVALID,
                             "committed topology members have no token owner");
    std::sort(committedTokenOwners_.begin(), committedTokenOwners_.end(),
              [](const auto &left, const auto &right) { return left.first < right.first; });
    std::sort(prospectiveTokenOwners_.begin(), prospectiveTokenOwners_.end(),
              [](const auto &left, const auto &right) { return left.first < right.first; });
    return Status::OK();
}

void TopologySnapshot::ReserveIndexes()
{
    size_t committedMemberCount = 0;
    size_t activeMemberCount = 0;
    size_t failedMemberCount = 0;
    size_t committedTokenCount = 0;
    size_t prospectiveTokenCount = 0;
    for (const auto &member : state_.members) {
        if (IsCommitted(member.state)) {
            ++committedMemberCount;
            committedTokenCount += member.tokens.size();
        }
        activeMemberCount += member.state == MemberState::ACTIVE ? 1 : 0;
        failedMemberCount += member.state == MemberState::FAILED ? 1 : 0;
        if (IsProspective(member.state, state_.activeBatch)) {
            prospectiveTokenCount += member.tokens.size();
        }
    }
    addressIndex_.reserve(state_.members.size());
    idIndex_.reserve(state_.members.size());
    committedMembers_.reserve(committedMemberCount);
    activeMembers_.reserve(activeMemberCount);
    failedMembers_.reserve(failedMemberCount);
    committedTokenOwners_.reserve(committedTokenCount);
    prospectiveTokenOwners_.reserve(prospectiveTokenCount);
}

Status TopologySnapshot::BuildIndexEntries()
{
    std::unordered_set<uint32_t> tokens;
    size_t tokenCount = 0;
    for (const auto &member : state_.members) {
        tokenCount += member.tokens.size();
    }
    tokens.reserve(tokenCount);
    for (size_t index = 0; index < state_.members.size(); ++index) {
        const auto &member = state_.members[index];
        RETURN_IF_NOT_OK(ValidateAddress(member.identity.address));
        CHECK_FAIL_RETURN_STATUS(member.identity.id.size() == MEMBER_ID_SIZE, K_INVALID, "invalid member id");
        CHECK_FAIL_RETURN_STATUS(addressIndex_.emplace(member.identity.address, index).second, K_INVALID,
                                 "duplicate member address");
        CHECK_FAIL_RETURN_STATUS(idIndex_.emplace(member.identity.id, index).second, K_INVALID, "duplicate member id");
        for (uint32_t token : member.tokens) {
            CHECK_FAIL_RETURN_STATUS(tokens.emplace(token).second, K_INVALID, "duplicate topology token");
        }
        if (IsCommitted(member.state)) {
            committedMembers_.emplace_back(&member);
            for (uint32_t token : member.tokens) {
                committedTokenOwners_.emplace_back(token, &member);
            }
        }
        if (member.state == MemberState::ACTIVE) {
            activeMembers_.emplace_back(&member);
        } else if (member.state == MemberState::FAILED) {
            failedMembers_.emplace_back(&member);
        }
        if (IsProspective(member.state, state_.activeBatch)) {
            for (uint32_t token : member.tokens) {
                prospectiveTokenOwners_.emplace_back(token, &member);
            }
        }
    }
    return Status::OK();
}

uint64_t TopologySnapshot::Version() const noexcept
{
    return state_.version;
}

int64_t TopologySnapshot::AuthorityRevision() const noexcept
{
    return authorityRevision_;
}

const std::string &TopologySnapshot::CanonicalDigest() const noexcept
{
    return canonicalDigest_;
}

bool TopologySnapshot::ClusterHasInit() const noexcept
{
    return state_.clusterHasInit;
}

const std::optional<ActiveBatch> &TopologySnapshot::GetActiveBatch() const noexcept
{
    return state_.activeBatch;
}

const std::vector<Member> &TopologySnapshot::Members() const noexcept
{
    return state_.members;
}

Status TopologySnapshot::FindMemberByAddress(const std::string &address, const Member *&member) const
{
    auto iter = addressIndex_.find(address);
    CHECK_FAIL_RETURN_STATUS(iter != addressIndex_.end(), K_NOT_FOUND, "member address not found");
    member = &state_.members[iter->second];
    return Status::OK();
}

Status TopologySnapshot::FindMemberById(const std::string &id, const Member *&member) const
{
    CHECK_FAIL_RETURN_STATUS(id.size() == MEMBER_ID_SIZE, K_INVALID, "invalid member id");
    auto iter = idIndex_.find(id);
    CHECK_FAIL_RETURN_STATUS(iter != idIndex_.end(), K_NOT_FOUND, "member id not found");
    member = &state_.members[iter->second];
    return Status::OK();
}

const std::vector<const Member *> &TopologySnapshot::CommittedMembers() const noexcept
{
    return committedMembers_;
}

const std::vector<const Member *> &TopologySnapshot::ActiveMembers() const noexcept
{
    return activeMembers_;
}

const std::vector<const Member *> &TopologySnapshot::FailedMembers() const noexcept
{
    return failedMembers_;
}

Status TopologySnapshot::FindNextCommittedMember(const std::string &address, const Member *&member) const
{
    const Member *current = nullptr;
    auto rc = FindMemberByAddress(address, current);
    CHECK_FAIL_RETURN_STATUS(rc.IsOk() && current != nullptr && IsCommitted(current->state), K_NOT_FOUND,
                             "committed member address not found");
    CHECK_FAIL_RETURN_STATUS(committedMembers_.size() > 1, K_NOT_FOUND, "no distinct committed member");
    auto iter = std::upper_bound(
        committedMembers_.begin(), committedMembers_.end(), address,
        [](const std::string &value, const Member *candidate) { return value < candidate->identity.address; });
    const Member *candidate = iter == committedMembers_.end() ? committedMembers_.front() : *iter;
    CHECK_FAIL_RETURN_STATUS(candidate->identity.address != address, K_NOT_FOUND, "no distinct committed member");
    member = candidate;
    return Status::OK();
}

Status TopologySnapshot::FindNextActiveMember(const std::string &address, const Member *&member) const
{
    CHECK_FAIL_RETURN_STATUS(!activeMembers_.empty(), K_NOT_FOUND, "no active member");
    auto iter = std::upper_bound(
        activeMembers_.begin(), activeMembers_.end(), address,
        [](const std::string &value, const Member *candidate) { return value < candidate->identity.address; });
    const Member *candidate = iter == activeMembers_.end() ? activeMembers_.front() : *iter;
    CHECK_FAIL_RETURN_STATUS(candidate->identity.address != address, K_NOT_FOUND, "no distinct active member");
    member = candidate;
    return Status::OK();
}

Status TopologySnapshot::ValidateMigrationFence(const TopologyMigrationFence &fence) const
{
    if (fence.topologyVersion > Version()) {
        RETURN_STATUS(K_TRY_AGAIN, "migration fence topology is newer than local Snapshot");
    }
    CHECK_FAIL_RETURN_STATUS(fence.topologyVersion != 0 && fence.topologyVersion == Version(), K_INVALID,
                             "migration fence topology version is stale or incomplete");
    CHECK_FAIL_RETURN_STATUS(state_.activeBatch.has_value() && fence.batchEpoch != 0
                                 && state_.activeBatch->epoch == fence.batchEpoch,
                             K_INVALID, "migration fence batch epoch does not match topology");
    const Member *source = nullptr;
    const Member *target = nullptr;
    RETURN_IF_NOT_OK(FindExactMember(*this, fence.source, source));
    RETURN_IF_NOT_OK(FindExactMember(*this, fence.target, target));
    const auto type = state_.activeBatch->type;
    const bool scaleOut = type == TopologyChangeType::SCALE_OUT && IsCommitted(source->state)
                          && target->state == MemberState::JOINING;
    const bool scaleIn = type == TopologyChangeType::SCALE_IN && source->state == MemberState::LEAVING
                         && (target->state == MemberState::ACTIVE || target->state == MemberState::PRE_LEAVING)
                         && !(source->identity == target->identity);
    CHECK_FAIL_RETURN_STATUS(
        scaleOut || scaleIn, K_INVALID,
        FormatString("migration fence participants do not match batch, type: %s, source state: %s, target state: %s",
                     TopologyChangeTypeName(type), MemberStateName(source->state), MemberStateName(target->state)));
    return Status::OK();
}

}  // namespace datasystem::cluster
