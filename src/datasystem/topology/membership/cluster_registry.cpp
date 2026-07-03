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
 * Description: Cluster table registry for topology membership.
 */
#include "datasystem/topology/membership/cluster_registry.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

constexpr int MAX_ENDPOINT_PORT = 65535;
constexpr size_t BRACKET_PORT_OFFSET = 2;
constexpr int DECIMAL_BASE = 10;

Status ParseTopologyEndpoint(const TopologyNodeId &nodeId, TopologyEndpoint &endpoint)
{
    endpoint = {};
    CHECK_FAIL_RETURN_STATUS(!nodeId.empty(), K_INVALID, "member id is empty");
    CHECK_FAIL_RETURN_STATUS(nodeId.find('/') == std::string::npos, K_INVALID, "member id contains reserved char");
    CHECK_FAIL_RETURN_STATUS(nodeId.find_first_of("|,=;") == std::string::npos, K_INVALID,
                             "member id contains reserved char");

    std::string host;
    std::string portText;
    if (nodeId.front() == '[') {
        auto close = nodeId.find(']');
        CHECK_FAIL_RETURN_STATUS(
            close != std::string::npos && close + 1 < nodeId.size() && nodeId[close + 1] == ':', K_INVALID,
            "invalid member address");
        host = nodeId.substr(1, close - 1);
        portText = nodeId.substr(close + BRACKET_PORT_OFFSET);
    } else {
        auto sep = nodeId.rfind(':');
        CHECK_FAIL_RETURN_STATUS(sep != std::string::npos && sep > 0 && sep + 1 < nodeId.size(), K_INVALID,
                                 "invalid member address");
        CHECK_FAIL_RETURN_STATUS(nodeId.find(':') == sep, K_INVALID, "ipv6 member address must be bracketed");
        host = nodeId.substr(0, sep);
        portText = nodeId.substr(sep + 1);
    }
    CHECK_FAIL_RETURN_STATUS(!host.empty() && !portText.empty(), K_INVALID, "invalid member address");
    int port = 0;
    for (auto c : portText) {
        CHECK_FAIL_RETURN_STATUS(c >= '0' && c <= '9', K_INVALID, "invalid member port");
        port = port * DECIMAL_BASE + (c - '0');
        CHECK_FAIL_RETURN_STATUS(port <= MAX_ENDPOINT_PORT, K_INVALID, "member port is out of range");
    }
    endpoint.host = std::move(host);
    endpoint.port = port;
    return Status::OK();
}

Status DecodeMemberValue(const TopologyNodeId &nodeId, const std::string &value, Revision revision,
                         MembershipRecord &record)
{
    record = {};
    record.nodeId = nodeId;
    RETURN_IF_NOT_OK(ParseTopologyEndpoint(nodeId, record.endpoint));
    MembershipValue memberValue;
    RETURN_IF_NOT_OK(MembershipValueCodec::Decode(value, memberValue));
    record.lifecycleState = memberValue.lifecycleState;
    CHECK_FAIL_RETURN_STATUS(memberValue.timestamp > 0, K_INVALID, "member timestamp is empty");
    record.capability.hostId = memberValue.hostId;
    record.capability.compatibilityVersion = memberValue.compatibilityVersion;
    record.registerTimestamp = std::to_string(memberValue.timestamp);
    record.modRevision = revision;
    return Status::OK();
}

void LogBadMembershipRecord(const std::string &tableName, const std::string &key, Revision revision,
                            uint64_t malformedRecordCount, const Status &rc)
{
    LOG(WARNING) << "Ignore malformed topology membership record, table=" << tableName << ", key=" << key
                 << ", revision=" << revision << ", malformedRecordCount=" << malformedRecordCount
                 << ", status=" << rc.ToString();
}

}  // namespace

Status ClusterRegistryKeyHelper::BuildMemberKey(const TopologyNodeId &nodeId, std::string &key)
{
    key.clear();
    TopologyEndpoint endpoint;
    RETURN_IF_NOT_OK(ParseTopologyEndpoint(nodeId, endpoint));
    key = nodeId;
    return Status::OK();
}

Status ClusterRegistryKeyHelper::ParseMemberKey(const std::string &key, TopologyNodeId &nodeId)
{
    nodeId.clear();
    CHECK_FAIL_RETURN_STATUS(!key.empty(), K_INVALID, "cluster member key is empty");
    auto tablePos = key.find(ETCD_CLUSTER_TABLE);
    if (tablePos != std::string::npos) {
        auto begin = tablePos + std::string(ETCD_CLUSTER_TABLE).size();
        if (begin < key.size() && key[begin] == '/') {
            ++begin;
        }
        nodeId = key.substr(begin);
    } else {
        nodeId = key;
    }
    TopologyEndpoint endpoint;
    return ParseTopologyEndpoint(nodeId, endpoint);
}

ClusterRegistry::ClusterRegistry(ICoordinationBackend &store) : store_(store)
{
}

Status ClusterRegistry::ListMembers(MembershipSnapshot &snapshot)
{
    snapshot = {};
    std::vector<std::pair<std::string, std::string>> kvs;
    RETURN_IF_NOT_OK(store_.GetAll(ETCD_CLUSTER_TABLE, kvs));
    for (const auto &kv : kvs) {
        TopologyNodeId nodeId;
        auto rc = ClusterRegistryKeyHelper::ParseMemberKey(kv.first, nodeId);
        if (rc.IsOk()) {
            MembershipRecord record;
            rc = DecodeMemberValue(nodeId, kv.second, 0, record);
            if (rc.IsOk()) {
                snapshot.members[record.nodeId] = std::move(record);
                continue;
            }
        }
        ++snapshot.malformedRecordCount;
        LogBadMembershipRecord(ETCD_CLUSTER_TABLE, kv.first, 0, snapshot.malformedRecordCount, rc);
    }
    return Status::OK();
}

Status ClusterRegistry::HandleMembershipEvent(const CoordinationEvent &event, MembershipWatchEvent &typed)
{
    typed = {};
    CHECK_FAIL_RETURN_STATUS(event.key.find(ETCD_CLUSTER_TABLE) != std::string::npos, K_NOT_FOUND,
                             "unrelated membership event");
    TopologyNodeId nodeId;
    RETURN_IF_NOT_OK(ClusterRegistryKeyHelper::ParseMemberKey(event.key, nodeId));
    typed.nodeId = nodeId;
    typed.revision = event.revision;
    if (event.type == CoordinationEventType::DELETE) {
        typed.type = MembershipWatchEventType::DELETED;
        return Status::OK();
    }
    typed.type = MembershipWatchEventType::UPDATED;
    auto rc = DecodeMemberValue(nodeId, event.value, event.revision, typed.record);
    if (rc.IsError()) {
        LogBadMembershipRecord(ETCD_CLUSTER_TABLE, event.key, event.revision, 1, rc);
        return rc;
    }
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
