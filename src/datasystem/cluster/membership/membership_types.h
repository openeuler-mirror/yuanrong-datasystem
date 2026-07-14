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
 * Description: Cluster membership lease and endpoint observation types.
 */
#ifndef DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_TYPES_H
#define DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_TYPES_H

#include <cstdint>
#include <string>

namespace datasystem::cluster {

/**
 * @brief Master-compatible runtime state stored in the membership lease value.
 */
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

/**
 * @brief Master-compatible backend membership payload before address-key projection.
 */
struct MembershipValue {
    int64_t timestamp = 0;
    MemberLifecycleState lifecycleState = MemberLifecycleState::UNKNOWN;
    std::string hostId;
    std::string compatibilityVersion;
};

/**
 * @brief Authoritative value read from `/cluster/{address}`.
 */
struct MembershipRecord {
    std::string address;
    MemberLifecycleState state{ MemberLifecycleState::STARTING };
    int64_t timestamp{ 0 };
    std::string hostId;
};

/**
 * @brief Process-local endpoint observation; never persisted or globally propagated.
 */
enum class EndpointAvailability : uint8_t {
    UNKNOWN,
    REACHABLE,
    UNREACHABLE,
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_MEMBERSHIP_MEMBERSHIP_TYPES_H
