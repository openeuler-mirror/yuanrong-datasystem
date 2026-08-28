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
 * Description: Coordinator raft startup plans and option validation.
 */
#ifndef DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_TYPES_H
#define DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_TYPES_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem::coordinator {

inline constexpr char kCoordinatorRaftGroupId[] = "datasystem-coordinator";
inline constexpr int kCoordinatorRaftMinHeartbeatIntervalMs = 10;
inline constexpr int kCoordinatorRaftMaxHeartbeatIntervalMs = 10'000;
inline constexpr int kCoordinatorRaftMinElectionHeartbeatRatio = 5;
inline constexpr int kCoordinatorRaftMaxElectionHeartbeatRatio = 10;
inline constexpr int kCoordinatorRaftMinElectionTimeoutMs =
    kCoordinatorRaftMinHeartbeatIntervalMs * kCoordinatorRaftMinElectionHeartbeatRatio;
inline constexpr int kCoordinatorRaftMaxElectionTimeoutMs =
    kCoordinatorRaftMaxHeartbeatIntervalMs * kCoordinatorRaftMaxElectionHeartbeatRatio;
inline constexpr uint32_t kDefaultCoordinatorElectionHealthCheckIntervalMs = 3'000;
inline constexpr uint32_t kDefaultCoordinatorElectionBootstrapWarningIntervalMs = 3'000;
inline constexpr int64_t kBraftDefaultMaxClockDriftMs = 1'000;
inline constexpr int64_t kBraftIntTimerMaxMs = std::numeric_limits<int>::max();
inline constexpr int64_t kCoordinatorRaftMaxVoteTimerBaseMs =
    static_cast<int64_t>(kCoordinatorRaftMaxElectionTimeoutMs) + kBraftDefaultMaxClockDriftMs;
static_assert(kCoordinatorRaftMaxVoteTimerBaseMs <= kBraftIntTimerMaxMs, "braft vote timer base must fit in int");
static_assert(kCoordinatorRaftMaxVoteTimerBaseMs + kCoordinatorRaftMaxVoteTimerBaseMs <= kBraftIntTimerMaxMs,
              "braft random_timeout must fit after adding at most one vote timer base");

enum class RaftMetadataState { ABSENT, VALID, CORRUPT, UNKNOWN };

enum class RaftBootstrapMode : uint8_t { STATIC_INITIAL_PEERS = 0, DISCOVERY_OBSERVATION = 1 };

enum class RaftBootstrapPhase : uint8_t { OBSERVING = 0, PROPOSED = 1, STARTED = 2, TERMINAL = 3 };

struct BootstrapPlan {
    std::vector<std::string> initialPeers;
};

struct RaftBootstrapState {
    struct ReceivedObservation {
        std::vector<std::string> peers;
        std::vector<std::string> committedPeers;
        RaftBootstrapPhase phase{ RaftBootstrapPhase::OBSERVING };
        std::chrono::steady_clock::time_point lastSeen;
    };

    struct ConsistentView {
        std::vector<std::string> peers;
        std::chrono::steady_clock::time_point since;
    };

    std::map<std::string, ReceivedObservation> knownPeers;
    std::optional<ConsistentView> consistentView;
    std::optional<BootstrapPlan> frozenPlan;
    std::vector<std::string> committedPeers;
    RaftBootstrapPhase phase{ RaftBootstrapPhase::OBSERVING };
};

struct CoordinatorRaftFlags {
    std::string localAddress;
    std::string dataDir;
    int32_t heartbeatIntervalMs{ 0 };
    int32_t electionTimeoutMs{ 0 };
    uint32_t discoveryRetryIntervalMs{ 0 };
    uint32_t memberFailureGraceMs{ 0 };
    uint32_t healthCheckIntervalMs{ kDefaultCoordinatorElectionHealthCheckIntervalMs };
    uint32_t bootstrapWarningIntervalMs{ kDefaultCoordinatorElectionBootstrapWarningIntervalMs };
};

struct RecoverPlan {
    // Recover only from valid local raft state; corrupt state must not be repaired by this startup plan.
};

struct WaitingToJoinPlan {};

using RaftStartPlan = std::variant<BootstrapPlan, RecoverPlan, WaitingToJoinPlan>;

struct CoordinatorRaftOptions {
    std::string localPeer;
    std::string dataDir;
    int heartbeatIntervalMs{ 0 };
    int electionTimeoutMs{ 0 };
    RaftStartPlan startPlan;
};

Status ValidateCoordinatorRaftOptions(const CoordinatorRaftOptions &options, RaftMetadataState metadataState);

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_TYPES_H
