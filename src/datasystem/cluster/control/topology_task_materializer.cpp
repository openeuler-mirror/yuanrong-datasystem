/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Deterministic cluster topology task and notify materialization.
 */
#include "datasystem/cluster/control/topology_task_materializer.h"

#include <algorithm>
#include <type_traits>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {
constexpr size_t ID_HASH_CHARS = 32;
constexpr size_t MAX_RANGES_PER_TASK = 4'096;
constexpr uint32_t DEFAULT_TOKENS_PER_MEMBER = 4;
constexpr int BITS_PER_BYTE = 8;
constexpr int U64_MOST_SIGNIFICANT_SHIFT = 56;
constexpr int U32_MOST_SIGNIFICANT_SHIFT = 24;
constexpr uint64_t BYTE_MASK = 0xff;

void AppendU64(std::string &seed, uint64_t value)
{
    for (int shift = U64_MOST_SIGNIFICANT_SHIFT; shift >= 0; shift -= BITS_PER_BYTE) {
        seed.push_back(static_cast<char>((value >> shift) & BYTE_MASK));
    }
}

void AppendU32(std::string &seed, uint32_t value)
{
    for (int shift = U32_MOST_SIGNIFICANT_SHIFT; shift >= 0; shift -= BITS_PER_BYTE) {
        seed.push_back(static_cast<char>((value >> shift) & BYTE_MASK));
    }
}

void AppendString(std::string &seed, const std::string &value)
{
    AppendU64(seed, value.size());
    seed.append(value);
}

void AppendIdentity(std::string &seed, const std::optional<MemberIdentity> &identity)
{
    seed.push_back(identity.has_value() ? 1 : 0);
    if (identity.has_value()) {
        AppendString(seed, identity->id);
        AppendString(seed, identity->address);
    }
}

void AppendRanges(std::string &seed, std::vector<TokenRange> ranges)
{
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) {
        return std::tie(left.from, left.end) < std::tie(right.from, right.end);
    });
    AppendU64(seed, ranges.size());
    for (const auto &range : ranges) {
        AppendU32(seed, range.from);
        AppendU32(seed, range.end);
    }
}

std::string DigestPrefix(const std::string &seed)
{
    Hasher hasher;
    std::string digest;
    if (hasher.GetSha256Hex(seed, digest).IsError()) {
        return {};
    }
    return digest.substr(0, ID_HASH_CHARS);
}

std::vector<TokenRange> ToRanges(const std::vector<TopologyTaskRange> &taskRanges)
{
    std::vector<TokenRange> ranges;
    ranges.reserve(taskRanges.size());
    for (const auto &range : taskRanges) {
        ranges.push_back(range.range);
    }
    return ranges;
}

TopologyState CopyState(const TopologySnapshot &snapshot)
{
    return { snapshot.ClusterHasInit(), snapshot.Version(), snapshot.Members(), snapshot.GetActiveBatch() };
}

void CanonicalizeRanges(std::vector<TopologyTaskRange> &ranges)
{
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) {
        return std::tie(left.range.from, left.range.end) < std::tie(right.range.from, right.range.end);
    });
}

bool MatchesSnapshot(const TopologySnapshot &latest, const TopologyState &next)
{
    if (latest.Version() != next.version || latest.ClusterHasInit() != next.clusterHasInit
        || latest.GetActiveBatch().has_value() != next.activeBatch.has_value()) {
        return false;
    }
    if (next.activeBatch.has_value()
        && (latest.GetActiveBatch()->type != next.activeBatch->type
            || latest.GetActiveBatch()->epoch != next.activeBatch->epoch)) {
        return false;
    }
    if (latest.Members().size() != next.members.size()) {
        return false;
    }
    for (size_t index = 0; index < next.members.size(); ++index) {
        const auto &left = latest.Members()[index];
        const auto &right = next.members[index];
        if (!(left.identity == right.identity) || left.state != right.state || left.tokens != right.tokens) {
            return false;
        }
    }
    return true;
}

Status ValidateOwnerChange(const TopologySnapshot &latest, TopologyChangeType type, const TopologyOwnerChange &change)
{
    CHECK_FAIL_RETURN_STATUS(change.source.has_value() && !change.ranges.empty(), K_INVALID,
                             "owner change lacks a source or ranges");
    const Member *source = nullptr;
    const Member *target = nullptr;
    RETURN_IF_NOT_OK(latest.FindMemberByAddress(change.source->address, source));
    RETURN_IF_NOT_OK(latest.FindMemberByAddress(change.target.address, target));
    CHECK_FAIL_RETURN_STATUS(source->identity == *change.source && target->identity == change.target, K_INVALID,
                             "owner change member generation is stale");
    const auto committed = [](MemberState state) {
        return state == MemberState::ACTIVE || state == MemberState::PRE_LEAVING || state == MemberState::LEAVING;
    };
    const auto expectedSource = type == TopologyChangeType::FAILURE ? MemberState::FAILED : MemberState::LEAVING;
    if (type == TopologyChangeType::SCALE_OUT) {
        CHECK_FAIL_RETURN_STATUS(committed(source->state) && target->state == MemberState::JOINING, K_INVALID,
                                 "ScaleOut target is not joining");
    } else if (type == TopologyChangeType::FAILURE) {
        CHECK_FAIL_RETURN_STATUS(source->state == expectedSource && target->state == MemberState::ACTIVE, K_INVALID,
                                 "Failure owner change target is not active");
    } else {
        CHECK_FAIL_RETURN_STATUS(source->state == expectedSource && committed(target->state), K_INVALID,
                                 "owner change source state does not match batch");
    }
    auto ranges = change.ranges;
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) {
        return std::tie(left.from, left.end) < std::tie(right.from, right.end);
    });
    for (size_t index = 0; index < ranges.size(); ++index) {
        CHECK_FAIL_RETURN_STATUS(ranges[index].from <= ranges[index].end, K_INVALID, "invalid owner-change range");
        CHECK_FAIL_RETURN_STATUS(index == 0 || ranges[index - 1].end < ranges[index].from, K_INVALID,
                                 "overlapping owner-change ranges");
    }
    return Status::OK();
}

void AppendTaskChunks(TopologyChangeType type, uint64_t epoch, const TopologyOwnerChange &change,
                      std::vector<TopologyTask> &tasks)
{
    auto ranges = change.ranges;
    std::sort(ranges.begin(), ranges.end(), [](const auto &left, const auto &right) {
        return std::tie(left.from, left.end) < std::tie(right.from, right.end);
    });
    for (size_t offset = 0; offset < ranges.size(); offset += MAX_RANGES_PER_TASK) {
        const size_t end = std::min(offset + MAX_RANGES_PER_TASK, ranges.size());
        if (type == TopologyChangeType::FAILURE) {
            TopologyDeleteTask task{ "", epoch, change.target.address, change.source->address, {} };
            for (size_t index = offset; index < end; ++index) {
                task.recoveryRanges.push_back({ task.executorAddress, ranges[index], false });
            }
            CanonicalizeRanges(task.recoveryRanges);
            tasks.emplace_back(std::move(task));
        } else {
            TopologyMigrateTask task{ "", type, epoch, change.source->address, change.target.address, {} };
            for (size_t index = offset; index < end; ++index) {
                task.sourceRanges.push_back({ task.executorAddress, ranges[index], false });
            }
            CanonicalizeRanges(task.sourceRanges);
            tasks.emplace_back(std::move(task));
        }
    }
}

Status RebuildPlan(const TopologySnapshot &latest, const IPlanningAlgorithm &algorithm, TopologyPlan &plan)
{
    const auto &batch = latest.GetActiveBatch();
    CHECK_FAIL_RETURN_STATUS(batch.has_value(), K_INVALID, "derived state requires an active batch");
    TopologyState current = CopyState(latest);
    std::vector<MemberIdentity> selected;
    MemberState selectedState = MemberState::JOINING;
    if (batch->type == TopologyChangeType::SCALE_IN) {
        selectedState = MemberState::LEAVING;
    }
    if (batch->type == TopologyChangeType::FAILURE) {
        selectedState = MemberState::FAILED;
    }
    for (auto &member : current.members) {
        if (member.state != selectedState) {
            continue;
        }
        selected.push_back(member.identity);
        if (selectedState == MemberState::FAILED) {
            member.state = MemberState::ACTIVE;
        }
    }
    CHECK_FAIL_RETURN_STATUS(!selected.empty(), K_INVALID, "active batch has no selected members");
    if (batch->type == TopologyChangeType::FAILURE) {
        const bool hasHealthyOwner =
            std::any_of(latest.Members().begin(), latest.Members().end(),
                        [](const auto &member) { return member.state == MemberState::ACTIVE; });
        if (!hasHealthyOwner) {
            plan = { CopyState(latest), {} };
            return Status::OK();
        }
    }
    if (batch->type == TopologyChangeType::SCALE_OUT) {
        RETURN_IF_NOT_OK(algorithm.PlanScaleOut({ current, selected, DEFAULT_TOKENS_PER_MEMBER }, plan));
    } else if (batch->type == TopologyChangeType::SCALE_IN) {
        RETURN_IF_NOT_OK(algorithm.PlanScaleIn({ current, selected }, plan));
    } else {
        RETURN_IF_NOT_OK(algorithm.PlanFailure({ current, selected }, plan));
    }
    plan.next = CopyState(latest);
    return Status::OK();
}
}  // namespace

std::string TopologyTaskMaterializer::BuildTaskId(const TopologyTask &task)
{
    std::string seed;
    std::string prefix;
    std::visit(
        [&](const auto &value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TopologyMigrateTask>) {
                seed.push_back(static_cast<char>(TopologyTaskKind::MIGRATE));
                seed.push_back(static_cast<char>(value.type));
                AppendU64(seed, value.epoch);
                AppendString(seed, value.executorAddress);
                AppendString(seed, value.targetAddress);
                AppendRanges(seed, ToRanges(value.sourceRanges));
                prefix = "m-e" + std::to_string(value.epoch) + "-";
            } else {
                seed.push_back(static_cast<char>(TopologyTaskKind::DELETE_MEMBER));
                seed.push_back(static_cast<char>(TopologyChangeType::FAILURE));
                AppendU64(seed, value.epoch);
                AppendString(seed, value.executorAddress);
                AppendString(seed, value.failedAddress);
                AppendRanges(seed, ToRanges(value.recoveryRanges));
                prefix = "d-e" + std::to_string(value.epoch) + "-";
            }
        },
        task);
    return prefix + DigestPrefix(seed);
}

Status TopologyTaskMaterializer::BuildExpected(const TopologySnapshot &latest, const TopologyPlan &plan,
                                               ExpectedDerivedState &expected) const
{
    CHECK_FAIL_RETURN_STATUS(latest.GetActiveBatch().has_value() && MatchesSnapshot(latest, plan.next), K_INVALID,
                             "placement plan does not match latest active topology");
    ExpectedDerivedState built;
    const auto type = latest.GetActiveBatch()->type;
    const auto epoch = latest.GetActiveBatch()->epoch;
    for (const auto &change : plan.ownerChanges) {
        RETURN_IF_NOT_OK(ValidateOwnerChange(latest, type, change));
        AppendTaskChunks(type, epoch, change, built.tasks);
    }
    for (auto &task : built.tasks) {
        const auto taskId = BuildTaskId(task);
        CHECK_FAIL_RETURN_STATUS(taskId.size() > ID_HASH_CHARS, K_RUNTIME_ERROR, "build task digest failed");
        std::visit([&](auto &value) { value.taskId = taskId; }, task);
    }
    std::sort(built.tasks.begin(), built.tasks.end(), [](const auto &left, const auto &right) {
        return std::visit([](const auto &value) { return value.taskId; }, left)
               < std::visit([](const auto &value) { return value.taskId; }, right);
    });
    for (const auto &task : built.tasks) {
        std::visit(
            [&](const auto &value) {
                auto &notify = built.notifiesByAddress[value.executorAddress];
                notify.type = type;
                notify.taskIds.push_back(value.taskId);
            },
            task);
    }
    VLOG(1) << "CLUSTER_TASK action=materialize type=" << static_cast<uint32_t>(type) << " epoch=" << epoch
            << " task_count=" << built.tasks.size() << " notify_count=" << built.notifiesByAddress.size();
    expected = std::move(built);
    return Status::OK();
}

Status TopologyTaskMaterializer::RebuildExpected(const TopologySnapshot &latest, const IPlanningAlgorithm &algorithm,
                                                 ExpectedDerivedState &expected) const
{
    if (!latest.GetActiveBatch().has_value()) {
        expected = {};
        return Status::OK();
    }
    TopologyPlan plan;
    RETURN_IF_NOT_OK(RebuildPlan(latest, algorithm, plan));
    return BuildExpected(latest, plan, expected);
}

std::string TopologyTaskMaterializer::BuildBusinessOperationId(TopologyCallbackPhase phase,
                                                               const TopologyExecutionFence &fence)
{
    std::string seed;
    seed.push_back(static_cast<char>(phase));
    auto source = fence.source;
    auto target = fence.target;
    auto failed = fence.failed;
    if (phase == TopologyCallbackPhase::FAILURE) {
        source.reset();
    }
    AppendIdentity(seed, source);
    AppendIdentity(seed, target);
    AppendIdentity(seed, failed);
    AppendRanges(seed, fence.ranges);
    return "op-" + DigestPrefix(seed);
}

}  // namespace datasystem::cluster
