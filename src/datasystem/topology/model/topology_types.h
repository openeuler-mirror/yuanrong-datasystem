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
 * Description: Topology module model types.
 */
#ifndef DATASYSTEM_TOPOLOGY_MODEL_TOPOLOGY_TYPES_H
#define DATASYSTEM_TOPOLOGY_MODEL_TOPOLOGY_TYPES_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace topology {

using Revision = int64_t;
using AlgorithmId = std::string;
using PlacementPolicyId = std::string;
using TaskId = std::string;
using TopologyAddress = std::string;
using TopologyNodeId = std::string;

static constexpr uint32_t TOPOLOGY_SCHEMA_VERSION = 1;

enum class TopologyNodeState {
    INITIAL,
    JOINING,
    ACTIVE,
    LEAVING,
    PRE_LEAVING,
};

struct TopologyNode {
    TopologyNodeId nodeId;
    TopologyNodeState state{ TopologyNodeState::INITIAL };
    std::vector<uint32_t> tokens;
};

struct TopologyDescriptor {
    int64_t version{ 0 };
    bool clusterHasInit{ true };
    std::vector<TopologyNode> members;
};

enum class PlacementPolicyMatchType {
    CATCH_ALL,
    EXACT_KEY,
    PREFIX,
    SUFFIX,
    NAMESPACE,
    CUSTOM,
};

struct PlacementPolicyRule {
    PlacementPolicyId policyId;
    PlacementPolicyMatchType matchType{ PlacementPolicyMatchType::CATCH_ALL };
    std::string matchPattern;
    uint32_t priority{ 0 };
    AlgorithmId algorithmId;
    std::string algorithmOptions;
};

struct PlacementUnit {
    AlgorithmId algorithmId;
    std::string unitType;
    std::string opaqueUnit;
};

struct LogicalOwner {
    TopologyNodeId nodeId;
    int64_t topologyVersion{ 0 };
};

struct AlgorithmOwnerRange {
    uint32_t begin{ 0 };
    uint32_t end{ 0 };
    TopologyNodeId nodeId;
};

struct AlgorithmRoutingState {
    virtual ~AlgorithmRoutingState() = default;

    AlgorithmId algorithmId;
    int64_t topologyVersion{ 0 };
    std::vector<AlgorithmOwnerRange> ownerRanges;
};

struct RouteContext {
    std::string key;
    std::string namespaceId;
};

struct TokenRange {
    uint32_t begin{ 0 };
    uint32_t end{ 0 };
    TopologyNodeId nodeId;
    bool finished{ false };
};

struct TaskFilter {
    std::optional<TopologyNodeId> nodeId;
    std::optional<TopologyNodeId> executorNodeId;
    bool unfinishedOnly{ false };
};

enum class TaskTerminalStatus {
    RUNNING,
    SUCCEEDED,
    FAILED,
    BLOCKED,
};

enum class TaskNotifyType {
    SCALE_OUT,
    ACTIVE_SCALE_IN,
    PASSIVE_SCALE_IN,
};

struct TaskNotify {
    TopologyAddress nodeAddress;
    TaskNotifyType type{ TaskNotifyType::SCALE_OUT };
    std::vector<TaskId> taskIds;
};

enum class TopologyChangeKind {
    NONE,
    FIRST_INIT,
    SCALE_OUT,
    ACTIVE_SCALE_IN,
    PASSIVE_FAILURE,
    ORDERLY_CLUSTER_SHUTDOWN,
    ACTIVATE_PENDING,
};

struct TopologyReconcileRequest {
    std::vector<TopologyNodeId> readyNodeIds;
    std::vector<TopologyNodeId> orderlyLeavingNodeIds;
    std::vector<TopologyNodeId> failedNodeIds;
    bool hasUnfinishedTasks{ false };
};

struct TopologyReconcileResult {
    TopologyChangeKind kind{ TopologyChangeKind::NONE };
    Revision committedRevision{ 0 };
    int64_t committedVersion{ 0 };
    uint64_t createdTransferTasks{ 0 };
    uint64_t createdRecoveryTasks{ 0 };
    uint64_t updatedNotifies{ 0 };
    Status status;
    std::vector<std::string> diagnostics;
};

struct TopologyTaskSummary {
    uint64_t unfinishedTransferTasks{ 0 };
    uint64_t unfinishedRecoveryTasks{ 0 };
};

struct LocalTopologySnapshot {
    TopologyDescriptor topology;
    Revision topologyRevision{ 0 };
    TopologyTaskSummary taskSummary;
    std::string digest;
};

struct PeerStateSnapshot {
    TopologyNodeId nodeId;
    int64_t topologyVersion{ 0 };
    Revision topologyRevision{ 0 };
    TopologyTaskSummary taskSummary;
    bool backendAvailable{ false };
    bool ready{ false };
    std::string digest;
};

struct DegradedStartupDecision {
    bool accepted{ false };
    std::string reason;
};

struct TransferTaskRecord {
    TaskId taskId;
    Revision taskRevision{ 0 };
    Revision ringRevision{ 0 };
    TopologyNodeId executorNodeId;
    TopologyNodeId sourceNodeId;
    TopologyNodeId targetNodeId;
    int64_t createdTopologyVersion{ 0 };
    int64_t targetTopologyVersion{ 0 };
    TaskTerminalStatus status{ TaskTerminalStatus::RUNNING };
    std::vector<TokenRange> ranges;
};

struct RecoveryTaskRecord {
    TaskId taskId;
    Revision taskRevision{ 0 };
    Revision ringRevision{ 0 };
    TopologyNodeId executorNodeId;
    TopologyNodeId failedNodeId;
    TopologyNodeId recoveryNodeId;
    int64_t createdTopologyVersion{ 0 };
    int64_t targetTopologyVersion{ 0 };
    TaskTerminalStatus status{ TaskTerminalStatus::RUNNING };
    std::vector<TokenRange> ranges;
};

struct TopologyChangePlan {
    TopologyChangeKind kind{ TopologyChangeKind::NONE };
    TopologyDescriptor expectedTopology;
    Revision expectedRevision{ 0 };
    TopologyDescriptor nextTopology;
    std::vector<TransferTaskRecord> transferTasks;
    std::vector<RecoveryTaskRecord> recoveryTasks;
    std::vector<TaskNotify> notifies;
    std::vector<std::string> diagnostics;
};

struct TaskProgressUpdate {
    TaskId taskId;
    TopologyNodeId nodeId;
    TokenRange range;
};

enum class TopologyWatchEventType {
    UPDATED,
    DELETED,
};

struct TopologyWatchEvent {
    TopologyWatchEventType type{ TopologyWatchEventType::UPDATED };
    TopologyDescriptor topology;
    Revision revision{ 0 };
    Status status;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_MODEL_TOPOLOGY_TYPES_H
