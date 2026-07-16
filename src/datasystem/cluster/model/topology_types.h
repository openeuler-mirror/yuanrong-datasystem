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
 * Description: PB-free cluster topology domain values.
 */
#ifndef DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_TYPES_H
#define DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_TYPES_H

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace datasystem::cluster {

enum class MemberState : uint8_t { INITIAL = 0, JOINING = 1, ACTIVE = 2, PRE_LEAVING = 3, LEAVING = 4, FAILED = 5 };
enum class TopologyChangeType : uint8_t { SCALE_OUT = 0, SCALE_IN = 1, FAILURE = 2 };
enum class TopologyTaskKind : uint8_t { MIGRATE = 0, DELETE_MEMBER = 1 };
enum class TopologyCallbackPhase : uint8_t { SCALE_OUT = 0, SCALE_IN = 1, SCALE_IN_CLEANUP = 2, FAILURE = 3 };

struct MemberIdentity {
    std::string id;
    std::string address;
    bool operator==(const MemberIdentity &other) const noexcept
    {
        return id == other.id && address == other.address;
    }
};

struct TokenRange {
    uint32_t from{ 0 };
    uint32_t end{ 0 };
    bool operator==(const TokenRange &other) const noexcept
    {
        return from == other.from && end == other.end;
    }
    bool operator!=(const TokenRange &other) const noexcept
    {
        return !(*this == other);
    }
};

struct Member {
    MemberIdentity identity;
    MemberState state{ MemberState::INITIAL };
    std::vector<uint32_t> tokens;
};

struct ActiveBatch {
    TopologyChangeType type{ TopologyChangeType::SCALE_OUT };
    uint64_t epoch{ 0 };
};

/**
 * @brief Identity- and epoch-bound metadata migration RPC fence.
 */
struct TopologyMigrationFence {
    uint64_t topologyVersion{ 0 };
    uint64_t batchEpoch{ 0 };
    MemberIdentity source;
    MemberIdentity target;
};

struct TopologyState {
    bool clusterHasInit{ false };
    uint64_t version{ 0 };
    std::vector<Member> members;
    std::optional<ActiveBatch> activeBatch;
};

struct TopologyTaskRange {
    std::string ownerAddress;
    TokenRange range;
    bool finished{ false };
};

struct TopologyMigrateTask {
    std::string taskId;
    TopologyChangeType type{ TopologyChangeType::SCALE_OUT };
    uint64_t epoch{ 0 };
    std::string executorAddress;
    std::string targetAddress;
    std::vector<TopologyTaskRange> sourceRanges;
};

struct TopologyDeleteTask {
    std::string taskId;
    uint64_t epoch{ 0 };
    std::string executorAddress;
    std::string failedAddress;
    std::vector<TopologyTaskRange> recoveryRanges;
};

using TopologyTask = std::variant<TopologyMigrateTask, TopologyDeleteTask>;

struct TopologyTaskNotify {
    TopologyChangeType type{ TopologyChangeType::SCALE_OUT };
    std::vector<std::string> taskIds;
};

struct TopologyExecutionFence {
    std::string taskId;
    TopologyTaskKind taskKind{ TopologyTaskKind::MIGRATE };
    TopologyChangeType batchType{ TopologyChangeType::SCALE_OUT };
    uint64_t batchEpoch{ 0 };
    TopologyCallbackPhase phase{ TopologyCallbackPhase::SCALE_OUT };
    MemberIdentity executor;
    std::optional<MemberIdentity> source;
    std::optional<MemberIdentity> target;
    std::optional<MemberIdentity> failed;
    std::vector<TokenRange> ranges;
};

struct TopologyOwnerChange {
    std::optional<MemberIdentity> source;
    MemberIdentity target;
    std::vector<TokenRange> ranges;
};

struct TopologyPlan {
    TopologyState next;
    std::vector<TopologyOwnerChange> ownerChanges;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MODEL_TOPOLOGY_TYPES_H
