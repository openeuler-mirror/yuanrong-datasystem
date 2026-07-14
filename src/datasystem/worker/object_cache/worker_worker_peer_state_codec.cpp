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
 * Description: Convert worker topology peer state to and from worker RPC protobufs.
 */
#include "datasystem/worker/object_cache/worker_worker_peer_state_codec.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr size_t MEMBER_ID_SIZE = 16;
constexpr size_t SHA256_HEX_SIZE = 64;
constexpr auto CONTROL_OBSERVATION_MAX_AGE = std::chrono::seconds(5);

bool IsLowerHex(char value)
{
    return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
}

Status ValidateEvidenceFields(const std::string &id, int64_t version, int64_t revision,
                              const std::string &digest)
{
    CHECK_FAIL_RETURN_STATUS(id.size() == MEMBER_ID_SIZE, K_INVALID, "invalid cluster member id evidence");
    CHECK_FAIL_RETURN_STATUS(version >= 0 && revision >= 0, K_INVALID, "negative cluster topology evidence stamp");
    CHECK_FAIL_RETURN_STATUS(digest.size() == SHA256_HEX_SIZE
                                 && std::all_of(digest.begin(), digest.end(), IsLowerHex),
                             K_INVALID, "invalid cluster topology digest evidence");
    return Status::OK();
}

Status ValidateCanonicalAddress(const std::string &address)
{
    HostPort endpoint;
    CHECK_FAIL_RETURN_STATUS(!address.empty() && endpoint.ParseString(address).IsOk()
                                 && endpoint.ToString() == address,
                             K_INVALID, "invalid cluster peer evidence address");
    return Status::OK();
}
}  // namespace
Status FillGetClusterStateRspPbFromControlBackendObservation(const cluster::ControlBackendObservation &observation,
                                                             GetClusterStateRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(observation.state != cluster::ControlBackendState::UNKNOWN, K_NOT_READY,
                             "cluster control-backend evidence is unknown");
    CHECK_FAIL_RETURN_STATUS(observation.topologyVersion <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
                             K_INVALID, "cluster topology evidence version exceeds peer RPC range");
    RETURN_IF_NOT_OK(ValidateCanonicalAddress(observation.reporter.address));
    RETURN_IF_NOT_OK(ValidateEvidenceFields(observation.reporter.id, static_cast<int64_t>(observation.topologyVersion),
                                            observation.topologyRevision, observation.topologyDigest));
    const auto now = std::chrono::steady_clock::now();
    CHECK_FAIL_RETURN_STATUS(observation.observedAt != std::chrono::steady_clock::time_point{}
                                 && observation.observedAt <= now
                                 && now - observation.observedAt <= CONTROL_OBSERVATION_MAX_AGE,
                             K_NOT_READY, "cluster control-backend evidence is stale");
    GetClusterStateRspPb candidate;
    candidate.set_coordinator_available(observation.state == cluster::ControlBackendState::AVAILABLE);
    candidate.set_node_id(observation.reporter.id);
    candidate.set_topology_version(static_cast<int64_t>(observation.topologyVersion));
    candidate.set_topology_revision(observation.topologyRevision);
    candidate.set_topology_digest(observation.topologyDigest);
    candidate.set_ready(true);
    candidate.set_degraded(observation.state == cluster::ControlBackendState::UNAVAILABLE);
    rsp = std::move(candidate);
    return Status::OK();
}

Status FillControlBackendObservationFromGetClusterStateRspPb(const std::string &peerAddress,
                                                             const GetClusterStateRspPb &rsp,
                                                             cluster::ControlBackendObservation &observation)
{
    CHECK_FAIL_RETURN_STATUS(rsp.ready(), K_NOT_READY, "cluster peer is not ready to report backend evidence");
    RETURN_IF_NOT_OK(ValidateCanonicalAddress(peerAddress));
    RETURN_IF_NOT_OK(ValidateEvidenceFields(rsp.node_id(), rsp.topology_version(), rsp.topology_revision(),
                                            rsp.topology_digest()));
    cluster::ControlBackendObservation candidate;
    candidate.reporter = { rsp.node_id(), peerAddress };
    candidate.state = rsp.coordinator_available() ? cluster::ControlBackendState::AVAILABLE
                                                  : cluster::ControlBackendState::UNAVAILABLE;
    candidate.topologyVersion = static_cast<uint64_t>(rsp.topology_version());
    candidate.topologyRevision = rsp.topology_revision();
    candidate.topologyDigest = rsp.topology_digest();
    candidate.observedAt = std::chrono::steady_clock::now();
    observation = std::move(candidate);
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
