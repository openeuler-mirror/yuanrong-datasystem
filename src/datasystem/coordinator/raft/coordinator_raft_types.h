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

#include <cstdint>
#include <limits>
#include <string>
#include <variant>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem::coordinator {

inline constexpr char kCoordinatorRaftGroupId[] = "datasystem-coordinator";
inline constexpr int kCoordinatorRaftMinElectionTimeoutMs = 100;
inline constexpr int64_t kBraftDefaultMaxClockDriftMs = 1'000;
inline constexpr int64_t kBraftIntTimerMaxMs = std::numeric_limits<int>::max();
inline constexpr int64_t kBraftMaxSafeVoteTimerBaseMs = kBraftIntTimerMaxMs / 2;
inline constexpr int kCoordinatorRaftMaxElectionTimeoutMs =
    static_cast<int>(kBraftMaxSafeVoteTimerBaseMs - kBraftDefaultMaxClockDriftMs);
inline constexpr int64_t kCoordinatorRaftMaxVoteTimerBaseMs =
    static_cast<int64_t>(kCoordinatorRaftMaxElectionTimeoutMs) + kBraftDefaultMaxClockDriftMs;
static_assert(kCoordinatorRaftMaxVoteTimerBaseMs <= kBraftIntTimerMaxMs, "braft vote timer base must fit in int");
static_assert(kCoordinatorRaftMaxVoteTimerBaseMs + kCoordinatorRaftMaxVoteTimerBaseMs <= kBraftIntTimerMaxMs,
              "braft random_timeout must fit after adding at most one vote timer base");

enum class RaftMetadataState { ABSENT, VALID, CORRUPT, UNKNOWN };

struct BootstrapPlan {
    std::vector<std::string> initialPeers;
};

struct RecoverPlan {
    // Recover only from valid local raft state; corrupt state must not be repaired by this startup plan.
};

struct WaitingToJoinPlan {};

using RaftStartPlan = std::variant<BootstrapPlan, RecoverPlan, WaitingToJoinPlan>;

struct CoordinatorRaftOptions {
    std::string localPeer;
    std::string dataDir;
    int electionTimeoutMs{ 0 };
    RaftStartPlan startPlan;
};

Status ValidateCoordinatorRaftOptions(const CoordinatorRaftOptions &options, RaftMetadataState metadataState);

}  // namespace datasystem::coordinator

#endif  // DATASYSTEM_COORDINATOR_RAFT_COORDINATOR_RAFT_TYPES_H
