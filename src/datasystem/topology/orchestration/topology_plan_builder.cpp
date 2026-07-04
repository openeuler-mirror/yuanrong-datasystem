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
 * Description: Backend-neutral topology transition plan builder.
 */
#include "datasystem/topology/orchestration/topology_plan_builder.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char TASK_MIGRATE_PREFIX[] = "migrate-v";
constexpr char TASK_RECOVERY_PREFIX[] = "delete-v";
constexpr char TASK_VERSION_SEPARATOR[] = "-to-v";
constexpr char TASK_SEGMENT_SEPARATOR[] = "-";

struct TaskIdSpec {
    const char *prefix{ nullptr };
    int64_t fromVersion{ 0 };
    int64_t toVersion{ 0 };
    TopologyNodeId fromNodeId;
    TopologyNodeId toNodeId;
    size_t changeIndex{ 0 };
};

Status NormalizeIds(const std::vector<TopologyNodeId> &input, std::vector<TopologyNodeId> &ids)
{
    ids = input;
    std::sort(ids.begin(), ids.end());
    CHECK_FAIL_RETURN_STATUS(std::adjacent_find(ids.begin(), ids.end()) == ids.end(), K_INVALID,
                             "duplicated topology node id");
    CHECK_FAIL_RETURN_STATUS(std::none_of(ids.begin(), ids.end(), [](const auto &id) { return id.empty(); }),
                             K_INVALID, "topology node id is empty");
    return Status::OK();
}

std::set<TopologyNodeId> ToSet(const std::vector<TopologyNodeId> &ids)
{
    return { ids.begin(), ids.end() };
}

std::string SafeTaskSegment(const TopologyNodeId &nodeId)
{
    std::string segment = nodeId;
    for (auto &ch : segment) {
        if (ch == '|' || ch == ',' || ch == '=' || ch == ';' || ch == '/') {
            ch = '_';
        }
    }
    return segment;
}

TaskId BuildTaskId(const TaskIdSpec &spec)
{
    return std::string(spec.prefix) + std::to_string(spec.fromVersion) + TASK_VERSION_SEPARATOR
           + std::to_string(spec.toVersion) + TASK_SEGMENT_SEPARATOR + SafeTaskSegment(spec.fromNodeId)
           + TASK_SEGMENT_SEPARATOR + SafeTaskSegment(spec.toNodeId) + TASK_SEGMENT_SEPARATOR
           + std::to_string(spec.changeIndex);
}

std::vector<TokenRange> ToTaskRanges(const std::vector<PlacementRange> &ranges, const TopologyNodeId &nodeId)
{
    std::vector<TokenRange> result;
    result.reserve(ranges.size());
    for (const auto &range : ranges) {
        result.emplace_back(TokenRange{ range.begin, range.end, nodeId, false });
    }
    return result;
}

bool IsActivationPendingState(TopologyNodeState state)
{
    return state == TopologyNodeState::JOINING || state == TopologyNodeState::PRE_LEAVING
           || state == TopologyNodeState::LEAVING;
}

bool HasPendingActivation(const TopologyDescriptor &topology)
{
    return std::any_of(topology.members.begin(), topology.members.end(), [](const auto &member) {
        return IsActivationPendingState(member.state);
    });
}

const TopologyNode *FindMember(const TopologyDescriptor &topology, const TopologyNodeId &nodeId)
{
    auto iter = std::find_if(topology.members.begin(), topology.members.end(), [&nodeId](const auto &member) {
        return member.nodeId == nodeId;
    });
    return iter == topology.members.end() ? nullptr : &(*iter);
}

std::vector<TopologyNodeId> BuildActivationTargetIds(const TopologyDescriptor &current)
{
    std::vector<TopologyNodeId> targetIds;
    targetIds.reserve(current.members.size());
    for (const auto &member : current.members) {
        if (member.state == TopologyNodeState::ACTIVE || member.state == TopologyNodeState::JOINING) {
            targetIds.emplace_back(member.nodeId);
        }
    }
    return targetIds;
}

Status BuildFinalTopologyFromCurrent(const TopologyDescriptor &current, TopologyDescriptor &finalTopology)
{
    finalTopology = {};
    finalTopology.version = current.version + 1;
    finalTopology.clusterHasInit = true;
    const auto targetIds = BuildActivationTargetIds(current);
    CHECK_FAIL_RETURN_STATUS(!targetIds.empty(), K_INVALID, "activation target topology would be empty");
    finalTopology.members.reserve(targetIds.size());
    for (const auto &nodeId : targetIds) {
        const auto *member = FindMember(current, nodeId);
        CHECK_FAIL_RETURN_STATUS(member != nullptr, K_INVALID, "activation target node is missing");
        CHECK_FAIL_RETURN_STATUS(!member->tokens.empty(), K_INVALID, "activation target tokens are empty");
        auto nextMember = *member;
        nextMember.state = TopologyNodeState::ACTIVE;
        finalTopology.members.emplace_back(std::move(nextMember));
    }
    return Status::OK();
}

TopologyNodeState TransitionStateForCurrentMember(TopologyChangeKind kind, const TopologyNodeId &nodeId,
                                                  const std::set<TopologyNodeId> &leavingIds,
                                                  const std::set<TopologyNodeId> &failedIds)
{
    if (kind == TopologyChangeKind::PASSIVE_FAILURE && failedIds.count(nodeId) != 0) {
        return TopologyNodeState::LEAVING;
    }
    if (kind == TopologyChangeKind::ACTIVE_SCALE_IN && leavingIds.count(nodeId) != 0) {
        return TopologyNodeState::PRE_LEAVING;
    }
    return TopologyNodeState::ACTIVE;
}

TopologyDescriptor BuildTransitionTopology(const TopologyDescriptor &current, const TopologyDescriptor &target,
                                           TopologyChangeKind kind, const std::set<TopologyNodeId> &leavingIds,
                                           const std::set<TopologyNodeId> &failedIds)
{
    TopologyDescriptor transition;
    transition.version = target.version;
    transition.clusterHasInit = true;
    std::set<TopologyNodeId> emitted;
    for (const auto &member : current.members) {
        auto nextMember = member;
        nextMember.state = TransitionStateForCurrentMember(kind, member.nodeId, leavingIds, failedIds);
        transition.members.emplace_back(std::move(nextMember));
        emitted.emplace(member.nodeId);
    }
    for (const auto &member : target.members) {
        if (emitted.count(member.nodeId) != 0) {
            continue;
        }
        auto joiningMember = member;
        joiningMember.state = TopologyNodeState::JOINING;
        transition.members.emplace_back(std::move(joiningMember));
    }
    return transition;
}

TopologyChangeKind ClassifyChange(const TopologyDescriptor &current, const std::set<TopologyNodeId> &targetIds,
                                  const std::set<TopologyNodeId> &leavingIds,
                                  const std::set<TopologyNodeId> &failedIds)
{
    if (!failedIds.empty()) {
        return TopologyChangeKind::PASSIVE_FAILURE;
    }
    if (!leavingIds.empty() && targetIds.empty() && leavingIds.size() == current.members.size()) {
        return TopologyChangeKind::ORDERLY_CLUSTER_SHUTDOWN;
    }
    if (!leavingIds.empty()) {
        return TopologyChangeKind::ACTIVE_SCALE_IN;
    }
    for (const auto &nodeId : targetIds) {
        auto iter = std::find_if(current.members.begin(), current.members.end(), [&nodeId](const auto &member) {
            return member.nodeId == nodeId;
        });
        if (iter == current.members.end()) {
            return TopologyChangeKind::SCALE_OUT;
        }
    }
    return TopologyChangeKind::NONE;
}

void BuildTargetIds(const TopologyDescriptor &current, const std::set<TopologyNodeId> &readyIds,
                    const std::set<TopologyNodeId> &leavingIds, const std::set<TopologyNodeId> &failedIds,
                    std::vector<TopologyNodeId> &targetIds)
{
    std::set<TopologyNodeId> targets;
    for (const auto &member : current.members) {
        if (member.state == TopologyNodeState::ACTIVE || member.state == TopologyNodeState::PRE_LEAVING) {
            targets.emplace(member.nodeId);
        }
    }
    targets.insert(readyIds.begin(), readyIds.end());
    for (const auto &nodeId : leavingIds) {
        targets.erase(nodeId);
    }
    for (const auto &nodeId : failedIds) {
        targets.erase(nodeId);
    }
    targetIds.assign(targets.begin(), targets.end());
}

TaskNotifyType NotifyTypeForKind(TopologyChangeKind kind)
{
    if (kind == TopologyChangeKind::PASSIVE_FAILURE) {
        return TaskNotifyType::PASSIVE_SCALE_IN;
    }
    if (kind == TopologyChangeKind::ACTIVE_SCALE_IN) {
        return TaskNotifyType::ACTIVE_SCALE_IN;
    }
    return TaskNotifyType::SCALE_OUT;
}

void AddNotify(const TopologyAddress &nodeAddress, TaskNotifyType type, const TaskId &taskId,
               std::map<TopologyAddress, TaskNotify> &notifies)
{
    auto &notify = notifies[nodeAddress];
    notify.nodeAddress = nodeAddress;
    notify.type = type;
    notify.taskIds.emplace_back(taskId);
}

void BuildTasks(const PlanResult &result, const std::set<TopologyNodeId> &failedIds,
                TopologyChangePlan &plan)
{
    std::map<TopologyAddress, TaskNotify> notifies;
    const auto notifyType = NotifyTypeForKind(plan.kind);
    for (size_t index = 0; index < result.ownerChanges.size(); ++index) {
        const auto &change = result.ownerChanges[index];
        if (failedIds.count(change.fromNodeId) != 0) {
            RecoveryTaskRecord task;
            task.taskId = BuildTaskId({ TASK_RECOVERY_PREFIX, plan.expectedTopology.version, plan.nextTopology.version,
                                        change.fromNodeId, change.toNodeId, index });
            task.executorNodeId = change.toNodeId;
            task.failedNodeId = change.fromNodeId;
            task.recoveryNodeId = change.toNodeId;
            task.createdTopologyVersion = plan.expectedTopology.version;
            task.targetTopologyVersion = plan.nextTopology.version;
            task.ranges = ToTaskRanges(change.ranges, change.toNodeId);
            plan.recoveryTasks.emplace_back(std::move(task));
            AddNotify(change.toNodeId, notifyType, plan.recoveryTasks.back().taskId, notifies);
            continue;
        }
        TransferTaskRecord task;
        task.taskId = BuildTaskId({ TASK_MIGRATE_PREFIX, plan.expectedTopology.version, plan.nextTopology.version,
                                    change.fromNodeId, change.toNodeId, index });
        task.executorNodeId = change.fromNodeId;
        task.sourceNodeId = change.fromNodeId;
        task.targetNodeId = change.toNodeId;
        task.createdTopologyVersion = plan.expectedTopology.version;
        task.targetTopologyVersion = plan.nextTopology.version;
        task.ranges = ToTaskRanges(change.ranges, change.fromNodeId);
        plan.transferTasks.emplace_back(std::move(task));
        AddNotify(change.fromNodeId, notifyType, plan.transferTasks.back().taskId, notifies);
    }
    for (auto &entry : notifies) {
        plan.notifies.emplace_back(std::move(entry.second));
    }
}

}  // namespace

TopologyPlanBuilder::TopologyPlanBuilder(const IPlanningAlgorithm &algorithm) : algorithm_(algorithm)
{
}

Status TopologyPlanBuilder::Build(const TopologyReconcileRequest &request, const TopologyDescriptor &current,
                                  Revision currentRevision, bool hasCommittedTopology,
                                  TopologyChangePlan &plan) const
{
    plan = {};
    std::vector<TopologyNodeId> readyIds;
    std::vector<TopologyNodeId> leavingIds;
    std::vector<TopologyNodeId> failedIds;
    RETURN_IF_NOT_OK(NormalizeIds(request.readyNodeIds, readyIds));
    RETURN_IF_NOT_OK(NormalizeIds(request.orderlyLeavingNodeIds, leavingIds));
    RETURN_IF_NOT_OK(NormalizeIds(request.failedNodeIds, failedIds));
    const auto readySet = ToSet(readyIds);
    const auto leavingSet = ToSet(leavingIds);
    const auto failedSet = ToSet(failedIds);
    if (!hasCommittedTopology) {
        PlanInput input{ current, readyIds, "" };
        PlanResult result;
        RETURN_IF_NOT_OK(algorithm_.InitPlacement(input, result));
        plan.kind = TopologyChangeKind::FIRST_INIT;
        plan.nextTopology = std::move(result.next);
        return Status::OK();
    }

    plan.expectedTopology = current;
    plan.expectedRevision = currentRevision;
    std::vector<TopologyNodeId> targetIds;
    BuildTargetIds(current, readySet, leavingSet, failedSet, targetIds);
    const auto targetSet = ToSet(targetIds);
    if (request.hasUnfinishedTasks) {
        plan.diagnostics.emplace_back("topology activation is blocked by unfinished tasks");
        RETURN_STATUS(K_TRY_AGAIN, "topology activation is blocked by unfinished tasks");
    }
    if (HasPendingActivation(current)) {
        plan.kind = TopologyChangeKind::ACTIVATE_PENDING;
        RETURN_IF_NOT_OK(BuildFinalTopologyFromCurrent(current, plan.nextTopology));
        return Status::OK();
    }
    plan.kind = ClassifyChange(current, targetSet, leavingSet, failedSet);
    if (plan.kind == TopologyChangeKind::NONE) {
        return Status::OK();
    }
    if (plan.kind == TopologyChangeKind::ORDERLY_CLUSTER_SHUTDOWN) {
        plan.nextTopology = current;
        ++plan.nextTopology.version;
        plan.nextTopology.members.clear();
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!targetIds.empty(), K_INVALID, "target topology would be empty");
    PlanInput input{ current, targetIds, "" };
    PlanResult result;
    RETURN_IF_NOT_OK(algorithm_.PlanPlacement(input, result));
    plan.nextTopology = BuildTransitionTopology(current, result.next, plan.kind, leavingSet, failedSet);
    BuildTasks(result, failedSet, plan);
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
