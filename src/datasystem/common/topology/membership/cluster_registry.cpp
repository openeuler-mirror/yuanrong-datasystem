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
#include "datasystem/common/topology/membership/cluster_registry.h"

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

Status ParseWorkerEndpoint(const WorkerId &workerId, WorkerEndpoint &endpoint)
{
    endpoint = {};
    CHECK_FAIL_RETURN_STATUS(!workerId.empty(), K_INVALID, "worker id is empty");
    CHECK_FAIL_RETURN_STATUS(workerId.find('/') == std::string::npos, K_INVALID, "worker id contains reserved char");
    CHECK_FAIL_RETURN_STATUS(workerId.find_first_of("|,=;") == std::string::npos, K_INVALID,
                             "worker id contains reserved char");

    std::string host;
    std::string portText;
    if (workerId.front() == '[') {
        auto close = workerId.find(']');
        CHECK_FAIL_RETURN_STATUS(
            close != std::string::npos && close + 1 < workerId.size() && workerId[close + 1] == ':', K_INVALID,
            "invalid worker address");
        host = workerId.substr(1, close - 1);
        portText = workerId.substr(close + BRACKET_PORT_OFFSET);
    } else {
        auto sep = workerId.rfind(':');
        CHECK_FAIL_RETURN_STATUS(sep != std::string::npos && sep > 0 && sep + 1 < workerId.size(), K_INVALID,
                                 "invalid worker address");
        CHECK_FAIL_RETURN_STATUS(workerId.find(':') == sep, K_INVALID, "ipv6 worker address must be bracketed");
        host = workerId.substr(0, sep);
        portText = workerId.substr(sep + 1);
    }
    CHECK_FAIL_RETURN_STATUS(!host.empty() && !portText.empty(), K_INVALID, "invalid worker address");
    int port = 0;
    for (auto c : portText) {
        CHECK_FAIL_RETURN_STATUS(c >= '0' && c <= '9', K_INVALID, "invalid worker port");
        port = port * DECIMAL_BASE + (c - '0');
        CHECK_FAIL_RETURN_STATUS(port <= MAX_ENDPOINT_PORT, K_INVALID, "worker port is out of range");
    }
    endpoint.host = std::move(host);
    endpoint.port = port;
    return Status::OK();
}

Status StringToServiceState(const std::string &value, WorkerServiceState &state)
{
    if (value == "start") {
        state = WorkerServiceState::START;
        return Status::OK();
    }
    if (value == "restart") {
        state = WorkerServiceState::RESTART;
        return Status::OK();
    }
    if (value == "recover") {
        state = WorkerServiceState::RECOVER;
        return Status::OK();
    }
    if (value == ETCD_NODE_READY) {
        state = WorkerServiceState::READY;
        return Status::OK();
    }
    if (value == ETCD_NODE_EXITING) {
        state = WorkerServiceState::EXITING;
        return Status::OK();
    }
    if (value == ETCD_NODE_DOWNGRADE_RESTART) {
        state = WorkerServiceState::DOWNGRADE_RESTART;
        return Status::OK();
    }
    RETURN_STATUS(K_INVALID, "invalid worker service state");
}

Status DecodeWorkerValue(const WorkerId &workerId, const std::string &value, Revision revision, WorkerRecord &record)
{
    record = {};
    KeepAliveValue keepAliveValue;
    RETURN_IF_NOT_OK(KeepAliveValue::FromString(value, keepAliveValue));
    WorkerServiceState state{ WorkerServiceState::START };
    RETURN_IF_NOT_OK(StringToServiceState(keepAliveValue.state, state));
    CHECK_FAIL_RETURN_STATUS(!keepAliveValue.timestamp.empty(), K_INVALID, "worker timestamp is empty");

    record.workerId = workerId;
    RETURN_IF_NOT_OK(ParseWorkerEndpoint(workerId, record.endpoint));
    record.serviceState = state;
    record.capability.hostId = keepAliveValue.hostId;
    record.registerTimestamp = keepAliveValue.timestamp;
    record.modRevision = revision;
    return Status::OK();
}

void LogBadWorkerRecord(const std::string &tableName, const std::string &key, Revision revision,
                        uint64_t badRecordCount, const Status &rc)
{
    LOG(WARNING) << "Ignore malformed topology membership record, table=" << tableName << ", key=" << key
                 << ", revision=" << revision << ", badRecordCount=" << badRecordCount << ", status=" << rc.ToString();
}

}  // namespace

Status ClusterRegistryKeyHelper::BuildWorkerKey(const WorkerId &workerId, std::string &key)
{
    key.clear();
    WorkerEndpoint endpoint;
    RETURN_IF_NOT_OK(ParseWorkerEndpoint(workerId, endpoint));
    key = workerId;
    return Status::OK();
}

Status ClusterRegistryKeyHelper::ParseWorkerKey(const std::string &key, WorkerId &workerId)
{
    workerId.clear();
    CHECK_FAIL_RETURN_STATUS(!key.empty(), K_INVALID, "cluster worker key is empty");
    auto tablePos = key.find(ETCD_CLUSTER_TABLE);
    if (tablePos != std::string::npos) {
        auto begin = tablePos + std::string(ETCD_CLUSTER_TABLE).size();
        if (begin < key.size() && key[begin] == '/') {
            ++begin;
        }
        workerId = key.substr(begin);
    } else {
        workerId = key;
    }
    WorkerEndpoint endpoint;
    return ParseWorkerEndpoint(workerId, endpoint);
}

ClusterRegistry::ClusterRegistry(IClusterStore &store) : store_(store)
{
}

Status ClusterRegistry::ListWorkers(MembershipSnapshot &snapshot)
{
    snapshot = {};
    std::vector<std::pair<std::string, std::string>> kvs;
    RETURN_IF_NOT_OK(store_.GetAll(ETCD_CLUSTER_TABLE, kvs));
    for (const auto &kv : kvs) {
        WorkerId workerId;
        auto rc = ClusterRegistryKeyHelper::ParseWorkerKey(kv.first, workerId);
        if (rc.IsOk()) {
            WorkerRecord record;
            rc = DecodeWorkerValue(workerId, kv.second, 0, record);
            if (rc.IsOk()) {
                snapshot.workers[record.workerId] = std::move(record);
                continue;
            }
        }
        ++snapshot.badRecordCount;
        LogBadWorkerRecord(ETCD_CLUSTER_TABLE, kv.first, 0, snapshot.badRecordCount, rc);
    }
    return Status::OK();
}

Status ClusterRegistry::HandleWorkerEvent(const ClusterStoreEvent &event, WorkerWatchEvent &typed)
{
    typed = {};
    CHECK_FAIL_RETURN_STATUS(event.key.find(ETCD_CLUSTER_TABLE) != std::string::npos, K_NOT_FOUND,
                             "unrelated membership event");
    WorkerId workerId;
    RETURN_IF_NOT_OK(ClusterRegistryKeyHelper::ParseWorkerKey(event.key, workerId));
    typed.workerId = workerId;
    typed.revision = event.revision;
    if (event.type == ClusterStoreEventType::DELETE) {
        typed.type = WorkerWatchEventType::DELETED;
        return Status::OK();
    }
    typed.type = WorkerWatchEventType::UPDATED;
    auto rc = DecodeWorkerValue(workerId, event.value, event.revision, typed.record);
    if (rc.IsError()) {
        LogBadWorkerRecord(ETCD_CLUSTER_TABLE, event.key, event.revision, 1, rc);
        return rc;
    }
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
