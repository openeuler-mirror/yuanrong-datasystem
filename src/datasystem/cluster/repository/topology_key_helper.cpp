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
 * Description: Validated cluster topology keyspace builder.
 */
#include "datasystem/cluster/repository/topology_key_helper.h"

#include <algorithm>
#include <cctype>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {

constexpr size_t MAX_CLUSTER_NAME_SIZE = 128;
constexpr size_t TASK_DIGEST_SIZE = 32;
constexpr size_t TASK_KIND_PREFIX_SIZE = 3;
constexpr size_t MIN_TASK_ID_SIZE = TASK_KIND_PREFIX_SIZE + 1 + 1 + TASK_DIGEST_SIZE;
constexpr char ROOT_PREFIX[] = "/datasystem/";
const std::string EMPTY_KEY;

bool IsAlphaNumeric(char value)
{
    return (value >= '0' && value <= '9') || (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z');
}

bool IsValidClusterName(const std::string &name)
{
    if (name.empty() || name.size() > MAX_CLUSTER_NAME_SIZE || !IsAlphaNumeric(name.front())) {
        return false;
    }
    for (char value : name) {
        if (!IsAlphaNumeric(value) && value != '.' && value != '_' && value != '-') {
            return false;
        }
    }
    return true;
}

bool IsLowerHex(char value)
{
    return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
}

bool IsValidTaskId(const std::string &taskId)
{
    if (taskId.size() < MIN_TASK_ID_SIZE || (taskId.rfind("m-e", 0) != 0 && taskId.rfind("d-e", 0) != 0)) {
        return false;
    }
    const auto separator = taskId.find('-', TASK_KIND_PREFIX_SIZE);
    if (separator == std::string::npos || separator == TASK_KIND_PREFIX_SIZE || taskId[TASK_KIND_PREFIX_SIZE] < '1'
        || taskId[TASK_KIND_PREFIX_SIZE] > '9' || taskId.size() - separator - 1 != TASK_DIGEST_SIZE) {
        return false;
    }
    for (size_t index = TASK_KIND_PREFIX_SIZE; index < separator; ++index) {
        if (!std::isdigit(static_cast<unsigned char>(taskId[index]))) {
            return false;
        }
    }
    return std::all_of(taskId.begin() + separator + 1, taskId.end(), IsLowerHex);
}

Status ValidateAddress(const std::string &address)
{
    if (address.empty()) {
        LOG(ERROR) << "Empty topology member address";
        return Status(K_INVALID, "empty topology member address");
    }
    HostPort hostPort;
    auto status = hostPort.ParseString(address);
    if (status.IsError() || hostPort.ToString() != address) {
        LOG(ERROR) << "Invalid canonical topology member address";
        return Status(K_INVALID, "invalid canonical topology member address");
    }
    return Status::OK();
}

}  // namespace

Status TopologyKeyHelper::Create(std::string clusterName, std::unique_ptr<TopologyKeyHelper> &helper)
{
    if (!IsValidClusterName(clusterName)) {
        LOG(ERROR) << "Invalid cluster name for topology keyspace";
        return Status(K_INVALID, "invalid cluster name for topology keyspace");
    }
    auto candidate = std::unique_ptr<TopologyKeyHelper>(new TopologyKeyHelper(std::move(clusterName)));
    helper = std::move(candidate);
    return Status::OK();
}

TopologyKeyHelper::TopologyKeyHelper(std::string clusterName) : clusterName_(std::move(clusterName))
{
    const std::string root = ROOT_PREFIX + clusterName_;
    topologyTable_ = root + "/topology";
    migrateTaskTable_ = root + "/tasks/migrate";
    deleteTaskTable_ = root + "/tasks/delete";
    notifyTable_ = root + "/notify";
    membershipTable_ = root + "/cluster";
}

const std::string &TopologyKeyHelper::ClusterName() const noexcept
{
    return clusterName_;
}

const std::string &TopologyKeyHelper::TopologyTable() const noexcept
{
    return topologyTable_;
}

const std::string &TopologyKeyHelper::MigrateTaskTable() const noexcept
{
    return migrateTaskTable_;
}

const std::string &TopologyKeyHelper::DeleteTaskTable() const noexcept
{
    return deleteTaskTable_;
}

const std::string &TopologyKeyHelper::NotifyTable() const noexcept
{
    return notifyTable_;
}

const std::string &TopologyKeyHelper::MembershipTable() const noexcept
{
    return membershipTable_;
}

const std::string &TopologyKeyHelper::TopologyKey() noexcept
{
    return EMPTY_KEY;
}

Status TopologyKeyHelper::TaskKey(const std::string &taskId, std::string &key)
{
    if (!IsValidTaskId(taskId)) {
        LOG(ERROR) << "Invalid deterministic topology task ID";
        return Status(K_INVALID, "invalid deterministic topology task ID");
    }
    key = taskId;
    return Status::OK();
}

Status TopologyKeyHelper::NotifyKey(const std::string &address, std::string &key)
{
    RETURN_IF_NOT_OK(ValidateAddress(address));
    key = address;
    return Status::OK();
}

Status TopologyKeyHelper::MembershipKey(const std::string &address, std::string &key)
{
    RETURN_IF_NOT_OK(ValidateAddress(address));
    key = address;
    return Status::OK();
}

}  // namespace datasystem::cluster
