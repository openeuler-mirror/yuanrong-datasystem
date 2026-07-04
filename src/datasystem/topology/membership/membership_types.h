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
 * Description: Membership DTOs for common topology.
 */
#ifndef DATASYSTEM_TOPOLOGY_MEMBERSHIP_MEMBERSHIP_TYPES_H
#define DATASYSTEM_TOPOLOGY_MEMBERSHIP_MEMBERSHIP_TYPES_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

enum class MemberLifecycleState {
    UNKNOWN = 0,
    STARTING,
    RESTARTING,
    RECOVERING,
    READY,
    EXITING,
    DOWNGRADE_RESTARTING,
    FAILED,
};

enum class ScaleInReason {
    ORDERLY_SHUTDOWN,
    MANUAL_DRAIN,
};

struct TopologyEndpoint {
    std::string host;
    int port{ -1 };

    std::string ToString() const
    {
        if (host.empty() || port < 0) {
            return "";
        }
        if (host.find(':') != std::string::npos) {
            return "[" + host + "]:" + std::to_string(port);
        }
        return host + ":" + std::to_string(port);
    }
};

struct MemberCapability {
    std::string hostId;
    std::string compatibilityVersion;
};

struct MembershipRecord {
    TopologyNodeId nodeId;
    TopologyEndpoint endpoint;
    MemberLifecycleState lifecycleState{ MemberLifecycleState::STARTING };
    MemberCapability capability;
    std::string registerTimestamp;
    Revision modRevision{ 0 };
};

struct MembershipSnapshot {
    Revision revision{ 0 };
    std::unordered_map<TopologyNodeId, MembershipRecord> members;
    uint64_t malformedRecordCount{ 0 };
};

enum class MembershipWatchEventType {
    UPDATED,
    DELETED,
};

struct MembershipWatchEvent {
    MembershipWatchEventType type{ MembershipWatchEventType::UPDATED };
    TopologyNodeId nodeId;
    MembershipRecord record;
    Revision revision{ 0 };
    Status status;
};

struct ScaleInRequest {
    TopologyNodeId nodeId;
    ScaleInReason reason{ ScaleInReason::ORDERLY_SHUTDOWN };
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_TOPOLOGY_MEMBERSHIP_MEMBERSHIP_TYPES_H
