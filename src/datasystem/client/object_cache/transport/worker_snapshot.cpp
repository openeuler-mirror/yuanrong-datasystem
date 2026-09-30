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

/** Description: Builds validated worker snapshots for transport reconciliation. */

#include "datasystem/client/object_cache/transport/worker_snapshot.h"

#include <sstream>
#include <utility>

#include "datasystem/common/log/log.h"

namespace datasystem {
namespace client {
namespace {

bool IsDraining(::datasystem::MembershipPb::StatePb state)
{
    return state == ::datasystem::MembershipPb::PRE_LEAVING || state == ::datasystem::MembershipPb::LEAVING;
}

}  // namespace

void LogRoutingHostIdMap(
    uint64_t ringVersion, const ::datasystem::ClusterTopologyPb &ring,
    const std::unordered_map<std::string, std::string> &hostIdMap, const std::string &sdkHostId)
{
    constexpr size_t fullLogLimit = 16;
    std::ostringstream workers;
    for (const auto &[worker, hostId] : hostIdMap) {
        if (hostIdMap.size() > fullLogLimit && hostId != sdkHostId) {
            continue;
        }
        auto member = ring.members().find(worker);
        workers << " {worker=" << worker
                << ", hostId=" << hostId << ", status="
                << (member == ring.members().end() ? "UNKNOWN" : MembershipPb::StatePb_Name(member->second.state()))
                << "}";
    }
    // Called only when the hash ring or hostIdMap changes, so no additional log deduplication is needed.
    LOG(INFO) << "[Routing] worker snapshot hostIdMap, ringVersion=" << ringVersion << ", sdkHostId=" << sdkHostId
              << ", totalNodes=" << ring.members_size() << ", hostIdMapSize=" << hostIdMap.size()
              << ", sameHostOnly=" << (hostIdMap.size() > fullLogLimit) << ", workers=[" << workers.str() << "]";
}

Status BuildWorkerSnapshot(uint64_t ringVersion, const ::datasystem::ClusterTopologyPb &ring,
                           const std::unordered_map<std::string, std::string> &hostIdMap,
                           const std::string &sdkHostId, WorkerSnapshot &snapshot)
{
    WorkerSnapshot updated;
    updated.ringVersion = ringVersion;
    updated.remoteTransportAddrs.reserve(ring.members_size());
    updated.workerIncarnations.reserve(ring.members_size());
    const bool canPartition = !sdkHostId.empty() && !hostIdMap.empty();
    for (const auto &member : ring.members()) {
        HostPort worker;
        Status rc = worker.ParseString(member.first);
        if (rc.IsError()) {
            return Status(K_INVALID, "Invalid worker endpoint in cluster topology: " + member.first);
        }
        if (!member.second.id().empty()) {
            updated.workerIncarnations.emplace(worker, member.second.id());
        }
        bool sameHost = false;
        if (canPartition) {
            auto it = hostIdMap.find(member.first);
            sameHost = it != hostIdMap.end() && it->second == sdkHostId;
        }
        if (member.second.state() == ::datasystem::MembershipPb::ACTIVE) {
            updated.writeProbeAddrs.emplace_back(worker);
        }
        if (sameHost && !IsDraining(member.second.state())) {
            updated.shmCandidateAddrs.emplace_back(std::move(worker));
        } else {
            updated.remoteTransportAddrs.emplace_back(std::move(worker));
        }
    }
    snapshot = std::move(updated);
    return Status::OK();
}

std::string ResolveSdkHostId(bool boundWorkerIsLocal, const HostPort &boundWorker,
                             const std::unordered_map<std::string, std::string> &hostIdMap)
{
    // The sdk cannot confirm any worker is same-host when its own hostId was not resolved. Adopt the
    // bound worker's hostId only when the caller has positively confirmed the bound worker is local;
    // a cross-node bound worker's hostId must NOT be adopted or the whole remote host is misclassified
    // as same-host and cross-node Gets time out on the SHM/UDS path.
    if (!boundWorkerIsLocal) {
        return std::string{};
    }
    auto iter = hostIdMap.find(boundWorker.ToString());
    if (iter != hostIdMap.end() && !iter->second.empty()) {
        return iter->second;
    }
    return std::string{};
}

}  // namespace client
}  // namespace datasystem
