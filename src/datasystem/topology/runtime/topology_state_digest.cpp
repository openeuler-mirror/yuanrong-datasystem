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
 * Description: Stable topology state digest helpers.
 */
#include "datasystem/topology/runtime/topology_state_digest.h"

#include <algorithm>
#include <string>
#include <vector>

#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/repository/topology_repository_codec.h"

namespace datasystem {
namespace topology {
namespace {

void AppendField(const char *name, uint64_t value, std::string &payload)
{
    payload += name;
    payload += '=';
    payload += std::to_string(value);
    payload += ';';
}

void AppendStringField(const char *name, const std::string &value, std::string &payload)
{
    payload += name;
    payload += '#';
    payload += std::to_string(value.size());
    payload += '=';
    payload += value;
    payload += ';';
}

void AppendTopology(const TopologyDescriptor &topology, std::string &payload)
{
    AppendField("topology_version", static_cast<uint64_t>(topology.version), payload);
    AppendField("cluster_has_init", topology.clusterHasInit ? 1 : 0, payload);
    auto members = topology.members;
    std::sort(members.begin(), members.end(),
              [](const TopologyNode &left, const TopologyNode &right) { return left.nodeId < right.nodeId; });
    AppendField("member_count", members.size(), payload);
    for (auto member : members) {
        std::sort(member.tokens.begin(), member.tokens.end());
        AppendStringField("member", member.nodeId, payload);
        AppendField("state", static_cast<uint64_t>(member.state), payload);
        AppendField("token_count", member.tokens.size(), payload);
        for (auto token : member.tokens) {
            AppendField("token", token, payload);
        }
    }
}

}  // namespace

Status TopologyStateDigest::Build(const LocalTopologySnapshot &snapshot, std::string &digest)
{
    digest.clear();
    CHECK_FAIL_RETURN_STATUS(snapshot.topology.version >= 0, K_INVALID, "topology version is invalid");
    CHECK_FAIL_RETURN_STATUS(snapshot.topologyRevision >= 0, K_INVALID, "topology revision is invalid");
    TopologyRepositoryCodec codec;
    std::string ignoredBytes;
    RETURN_IF_NOT_OK(codec.EncodeTopology(snapshot.topology, ignoredBytes));

    std::string payload;
    AppendField("revision", static_cast<uint64_t>(snapshot.topologyRevision), payload);
    AppendField("transfer", snapshot.taskSummary.unfinishedTransferTasks, payload);
    AppendField("recovery", snapshot.taskSummary.unfinishedRecoveryTasks, payload);
    AppendTopology(snapshot.topology, payload);
    digest = std::to_string(MurmurHash3_32(payload));
    return Status::OK();
}

Status TopologyStateDigest::BuildSnapshot(const TopologyDescriptor &topology, Revision revision,
                                          const TopologyTaskSummary &taskSummary,
                                          LocalTopologySnapshot &snapshot)
{
    snapshot = {};
    snapshot.topology = topology;
    snapshot.topologyRevision = revision;
    snapshot.taskSummary = taskSummary;
    return Build(snapshot, snapshot.digest);
}

}  // namespace topology
}  // namespace datasystem
