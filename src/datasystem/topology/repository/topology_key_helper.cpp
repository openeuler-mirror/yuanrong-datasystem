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
 * Description: Topology repository key helper.
 */
#include "datasystem/topology/repository/topology_key_helper.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {
namespace {

constexpr char RING_KEY[] = "/datasystem/ring";
constexpr char MIGRATE_TASK_DIR[] = "migrate_tasks";
constexpr char DELETE_NODE_TASK_DIR[] = "delete_node_tasks";
constexpr char NOTIFY_DIR[] = "notify";

bool IsSafeKeyPart(const std::string &value)
{
    if (value.empty() || value == "..") {
        return false;
    }
    for (unsigned char ch : value) {
        if (ch <= 0x1F || ch == '/' || ch == 0x7F) {
            return false;
        }
    }
    return true;
}

Status BuildChildKey(const char *dir, const std::string &id, std::string &key)
{
    key.clear();
    CHECK_FAIL_RETURN_STATUS(IsSafeKeyPart(id), K_INVALID, "invalid topology key part");
    key = std::string(dir) + "/" + id;
    return Status::OK();
}

std::string ToRelativeRingKey(const std::string &key)
{
    if (key.empty()) {
        return "";
    }

    constexpr size_t ringKeyLen = sizeof(RING_KEY) - 1;
    auto ringPos = key.find(RING_KEY);
    if (ringPos != std::string::npos) {
        auto afterRing = ringPos + ringKeyLen;
        if (afterRing == key.size() || (afterRing + 1 == key.size() && key[afterRing] == '/')) {
            return "";
        }
        if (key[afterRing] == '/') {
            return key.substr(afterRing + 1);
        }
    }
    return key;
}

Status ParseChild(const std::string &relative, const char *dir, TopologyKeyType type, TopologyKeyParts &parts)
{
    const std::string prefix = std::string(dir) + "/";
    if (relative.rfind(prefix, 0) != 0) {
        RETURN_STATUS(K_NOT_FOUND, "not this topology key type");
    }
    auto id = relative.substr(prefix.size());
    CHECK_FAIL_RETURN_STATUS(IsSafeKeyPart(id), K_INVALID, "invalid topology child key");
    parts.type = type;
    if (type == TopologyKeyType::NOTIFY) {
        parts.nodeAddress = id;
    } else {
        parts.taskId = id;
    }
    return Status::OK();
}

}  // namespace

std::string TopologyKeyHelper::CommittedTopologyKey()
{
    return RING_KEY;
}

Status TopologyKeyHelper::BuildMigrateTaskKey(const TaskId &taskId, std::string &key)
{
    return BuildChildKey(MIGRATE_TASK_DIR, taskId, key);
}

Status TopologyKeyHelper::BuildDeleteNodeTaskKey(const TaskId &taskId, std::string &key)
{
    return BuildChildKey(DELETE_NODE_TASK_DIR, taskId, key);
}

Status TopologyKeyHelper::BuildNotifyKey(const TopologyAddress &nodeAddress, std::string &key)
{
    return BuildChildKey(NOTIFY_DIR, nodeAddress, key);
}

Status TopologyKeyHelper::Parse(const std::string &key, TopologyKeyParts &parts)
{
    parts = {};
    auto relative = ToRelativeRingKey(key);
    if (relative.empty()) {
        parts.type = TopologyKeyType::COMMITTED_TOPOLOGY;
        return Status::OK();
    }

    auto status = ParseChild(relative, MIGRATE_TASK_DIR, TopologyKeyType::MIGRATE_TASK, parts);
    if (status.IsOk() || status.GetCode() == K_INVALID) {
        return status;
    }
    status = ParseChild(relative, DELETE_NODE_TASK_DIR, TopologyKeyType::DELETE_NODE_TASK, parts);
    if (status.IsOk() || status.GetCode() == K_INVALID) {
        return status;
    }
    status = ParseChild(relative, NOTIFY_DIR, TopologyKeyType::NOTIFY, parts);
    if (status.IsOk() || status.GetCode() == K_INVALID) {
        return status;
    }

    parts.type = TopologyKeyType::UNRELATED;
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem
