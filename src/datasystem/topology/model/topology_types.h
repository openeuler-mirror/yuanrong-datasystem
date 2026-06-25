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
using WorkerAddress = std::string;
using WorkerId = std::string;

enum class WorkerTopologyState {
    INITIAL,
    JOINING,
    ACTIVE,
    LEAVING,
};

struct TopologyWorker {
    WorkerId workerId;
    WorkerTopologyState state{ WorkerTopologyState::INITIAL };
    std::vector<uint32_t> tokens;
};

struct TopologyDescriptor {
    int64_t version{ 0 };
    bool clusterHasInit{ true };
    std::vector<TopologyWorker> workers;
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
    WorkerId workerId;
    int64_t topologyVersion{ 0 };
};

struct AlgorithmRoutingState {
    virtual ~AlgorithmRoutingState() = default;

    AlgorithmId algorithmId;
    int64_t topologyVersion{ 0 };
};

struct RouteContext {
    std::string objectKey;
    std::string namespaceId;
};

struct TokenRange {
    uint32_t begin{ 0 };
    uint32_t end{ 0 };
    WorkerId workerId;
    bool finished{ false };
};

struct TaskFilter {
    std::optional<WorkerId> workerId;
    bool unfinishedOnly{ false };
};

struct TransferTaskRecord {
    TaskId taskId;
    Revision ringRevision{ 0 };
    WorkerId sourceWorkerId;
    WorkerId targetWorkerId;
    std::vector<TokenRange> ranges;
};

struct RecoveryTaskRecord {
    TaskId taskId;
    Revision ringRevision{ 0 };
    WorkerId failedWorkerId;
    WorkerId recoveryWorkerId;
    std::vector<TokenRange> ranges;
};

struct TaskProgressUpdate {
    TaskId taskId;
    WorkerId workerId;
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
