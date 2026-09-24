/*
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
 * Description: Cluster admin client implementation. Removes stale per-address topology records and
 *              updates the topology table via CAS to evict stale members from the hash ring.
 */
#include "datasystem/client/cluster_admin/cluster_admin_client.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/membership/membership_types.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/coordinator/key_value_entry.h"
#include "datasystem/common/coordinator/static_coordinator_discovery.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/protos/coordinator.pb.h"

namespace datasystem::client::cluster_admin {
namespace {

constexpr int32_t ADMIN_RPC_TIMEOUT_MS = 10'000;

Status ValidateOptions(const ClusterAdminOptions &options)
{
    const bool hasEtcd = !options.etcdAddress.empty();
    const bool hasCoordinator = !options.coordinatorAddress.empty();
    CHECK_FAIL_RETURN_STATUS(hasEtcd != hasCoordinator, K_INVALID,
        "exactly one coordination backend address is required");
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    return cluster::TopologyKeyHelper::Create(options.clusterName, keys);
}

Status ValidateAddresses(const std::vector<std::string> &addresses)
{
    CHECK_FAIL_RETURN_STATUS(!addresses.empty(), K_INVALID, "worker address list must not be empty");
    std::unordered_set<std::string> seen;
    for (const auto &address : addresses) {
        CHECK_FAIL_RETURN_STATUS(seen.insert(address).second, K_INVALID,
            "duplicate worker address: " + address);
        std::string dummy;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, dummy));
    }
    return Status::OK();
}

bool IsAddressNotFound(const Status &status)
{
    return status.GetCode() == K_NOT_FOUND;
}

Status DecodeTopologyValue(const std::string &value, cluster::TopologyState &state)
{
    if (value.empty()) {
        return Status(K_NOT_FOUND, "topology table is empty");
    }
    return cluster::TopologyRepositoryCodec::DecodeTopology(value, state);
}

Status RemoveMembersFromTopology(const cluster::TopologyState &current,
    const std::vector<std::string> &addresses, cluster::TopologyState &next, bool &changed)
{
    next = current;
    changed = false;
    std::unordered_set<std::string> toRemove(addresses.begin(), addresses.end());
    auto &members = next.members;
    members.erase(std::remove_if(members.begin(), members.end(),
        [&](const cluster::Member &m) {
            if (toRemove.count(m.identity.address) > 0) {
                changed = true;
                return true;
            }
            return false;
        }), members.end());
    if (changed) {
        next.version = current.version + 1;
    }
    return Status::OK();
}

void FillResults(std::vector<DeleteClusterMemberResult> &results,
    bool removed, uint64_t version, const std::string &error = "")
{
    for (auto &result : results) {
        if (result.error.empty()) {
            result.topologyMemberRemoved = removed;
            result.topologyVersion = version;
            if (!error.empty()) {
                result.error = error;
            }
        }
    }
}

}  // namespace

class ClusterAdminClient::Impl final {
public:
    explicit Impl(ClusterAdminOptions options) : options_(std::move(options)) {}
    ~Impl() = default;

    Status Init();
    Status DeleteClusterMembers(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);

private:
    Status InitEtcd();
    Status InitCoordinator();
    Status CheckMemberAbsentEtcd(const std::string &address, bool &absent);
    Status CheckMemberAbsentCoordinator(const std::string &address, bool &absent);
    Status DeletePerAddressKeysEtcd(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status DeletePerAddressKeysCoordinator(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status DeleteClusterMembersEtcd(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status DeleteClusterMembersCoordinator(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status ReadTopologyEtcd(std::string &value);
    Status ReadTopologyCoordinator(std::string &value);
    void PreCheckAddresses(const std::vector<std::string> &addresses,
        const std::function<Status(const std::string &, bool &)> &absentChecker,
        std::vector<DeleteClusterMemberResult> &results, std::vector<std::string> &toDelete);
    Status CommitTopologyUpdate(const std::vector<std::string> &toDelete,
        const std::function<Status(std::string &)> &topologyReader,
        const std::function<Status()> &topologyDeleter,
        const std::function<Status(std::string &)> &casWriter,
        std::vector<DeleteClusterMemberResult> &results);

    ClusterAdminOptions options_;
    std::unique_ptr<cluster::TopologyKeyHelper> keys_;
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<ICoordinatorServiceProxy> coordinatorProxy_;
    bool initialized_{ false };
};

Status ClusterAdminClient::Impl::InitEtcd()
{
    CHECK_FAIL_RETURN_STATUS(Validator::ValidateEtcdAddresses("etcd_address", options_.etcdAddress), K_INVALID,
        "invalid etcd address");
    etcdStore_ = std::make_unique<EtcdStore>(options_.etcdAddress);
    RETURN_IF_NOT_OK(etcdStore_->Init());
    RETURN_IF_NOT_OK(etcdStore_->CreateTableWithExactPrefix(keys_->MembershipTable(),
        keys_->EtcdMembershipTablePrefix()));
    RETURN_IF_NOT_OK(etcdStore_->CreateTableWithExactPrefix(keys_->TopologyTable(), keys_->TopologyTable()));
    RETURN_IF_NOT_OK(etcdStore_->CreateTableWithExactPrefix(keys_->NotifyTable(), keys_->NotifyTable()));
    RETURN_IF_NOT_OK(etcdStore_->CreateTableWithExactPrefix(keys_->ProbeTable(), keys_->ProbeTable()));
    RETURN_IF_NOT_OK(etcdStore_->CreateTableWithExactPrefix(keys_->UbHealthTable(), keys_->UbHealthTable()));
    return Status::OK();
}

Status ClusterAdminClient::Impl::InitCoordinator()
{
    CHECK_FAIL_RETURN_STATUS(
        Validator::ValidateCoordinatorAddresses("coordinator_address", options_.coordinatorAddress), K_INVALID,
        "invalid coordinator address");
    auto coordinatorDiscovery = std::make_shared<StaticCoordinatorDiscovery>(options_.coordinatorAddress);
    coordinatorProxy_ = std::make_unique<CoordinatorServiceProxyBrpcImpl>(std::move(coordinatorDiscovery));
    return coordinatorProxy_->Init();
}

Status ClusterAdminClient::Impl::Init()
{
    if (initialized_) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(ValidateOptions(options_));
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::Create(options_.clusterName, keys_));
    const Status rc = options_.etcdAddress.empty() ? InitCoordinator() : InitEtcd();
    initialized_ = rc.IsOk();
    return rc;
}

Status ClusterAdminClient::Impl::CheckMemberAbsentEtcd(const std::string &address, bool &absent)
{
    std::string key;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, key));
    std::string value;
    auto rc = etcdStore_->Get(keys_->MembershipTable(), key, value);
    if (rc.IsOk()) {
        absent = false;
        return Status::OK();
    }
    if (IsAddressNotFound(rc)) {
        absent = true;
        return Status::OK();
    }
    return rc;
}

Status ClusterAdminClient::Impl::CheckMemberAbsentCoordinator(const std::string &address, bool &absent)
{
    std::string key;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, key));
    std::string physicalKey = keys_->MembershipTable() + "/" + key;
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    auto rc = coordinatorProxy_->Range(physicalKey, "", kvs, revision, ADMIN_RPC_TIMEOUT_MS);
    if (rc.IsError()) {
        return rc;
    }
    absent = kvs.empty();
    return Status::OK();
}

Status ClusterAdminClient::Impl::DeletePerAddressKeysEtcd(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    auto deleteKey = [this](const std::string &tableName, const std::string &address, bool &deleted) {
        std::string key;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, key));
        auto rc = etcdStore_->Delete(tableName, key);
        if (rc.IsOk()) {
            deleted = true;
            return Status::OK();
        }
        if (IsAddressNotFound(rc)) {
            deleted = false;
            return Status::OK();
        }
        return rc;
    };
    for (const auto &address : addresses) {
        DeleteClusterMemberResult result;
        result.address = address;
        auto rc = deleteKey(keys_->MembershipTable(), address, result.membershipDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        rc = deleteKey(keys_->NotifyTable(), address, result.notifyDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        rc = deleteKey(keys_->ProbeTable(), address, result.probeDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        rc = deleteKey(keys_->UbHealthTable(), address, result.ubHealthDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        results.push_back(std::move(result));
    }
    return Status::OK();
}

Status ClusterAdminClient::Impl::ReadTopologyEtcd(std::string &value)
{
    return etcdStore_->Get(keys_->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(), value);
}

void ClusterAdminClient::Impl::PreCheckAddresses(const std::vector<std::string> &addresses,
    const std::function<Status(const std::string &, bool &)> &absentChecker,
    std::vector<DeleteClusterMemberResult> &results, std::vector<std::string> &toDelete)
{
    for (const auto &address : addresses) {
        bool absent = false;
        auto rc = absentChecker(address, absent);
        if (rc.IsError()) {
            DeleteClusterMemberResult result;
            result.address = address;
            result.error = "membership check failed: " + rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        if (!absent && !options_.force) {
            DeleteClusterMemberResult result;
            result.address = address;
            result.error = "worker is still online; use --force to override";
            results.push_back(std::move(result));
            continue;
        }
        if (options_.dryRun) {
            DeleteClusterMemberResult result;
            result.address = address;
            result.error = "dry-run: no changes applied";
            results.push_back(std::move(result));
            continue;
        }
        toDelete.push_back(address);
    }
}

Status ClusterAdminClient::Impl::CommitTopologyUpdate(const std::vector<std::string> &toDelete,
    const std::function<Status(std::string &)> &topologyReader,
    const std::function<Status()> &topologyDeleter,
    const std::function<Status(std::string &)> &casWriter,
    std::vector<DeleteClusterMemberResult> &results)
{
    std::string topologyValue;
    auto getRc = topologyReader(topologyValue);
    if (getRc.IsError() && !IsAddressNotFound(getRc)) {
        FillResults(results, false, 0, "topology read failed: " + getRc.ToString());
        return getRc;
    }
    cluster::TopologyState current;
    if (!topologyValue.empty()) {
        RETURN_IF_NOT_OK(DecodeTopologyValue(topologyValue, current));
    }
    cluster::TopologyState next;
    bool changed = false;
    RETURN_IF_NOT_OK(RemoveMembersFromTopology(current, toDelete, next, changed));
    if (!changed) {
        FillResults(results, false, current.version);
        return Status::OK();
    }
    if (next.members.empty()) {
        auto delRc = topologyDeleter();
        if (delRc.IsError()) {
            return delRc;
        }
        FillResults(results, true, 0);
        return Status::OK();
    }
    std::string nextValue;
    RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(next, nextValue));
    std::string committedValue;
    auto casRc = casWriter(committedValue);
    if (casRc.IsError()) {
        FillResults(results, false, 0, "topology CAS failed: " + casRc.ToString());
        return casRc;
    }
    cluster::TopologyState committed;
    if (!committedValue.empty()) {
        RETURN_IF_NOT_OK(DecodeTopologyValue(committedValue, committed));
    } else {
        committed = next;
    }
    FillResults(results, true, committed.version);
    return Status::OK();
}

Status ClusterAdminClient::Impl::DeleteClusterMembersEtcd(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    std::vector<std::string> toDelete;
    PreCheckAddresses(addresses, [this](const std::string &addr, bool &absent) {
        return CheckMemberAbsentEtcd(addr, absent);
    }, results, toDelete);
    if (toDelete.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(DeletePerAddressKeysEtcd(toDelete, results));
    auto deleter = [this]() {
        auto rc = etcdStore_->Delete(keys_->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey());
        return (rc.IsError() && !IsAddressNotFound(rc)) ? rc : Status::OK();
    };
    std::string nextValue;
    auto casWriter = [this, &nextValue](std::string &) {
        return etcdStore_->CAS(keys_->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(),
            [&nextValue](const std::string &,
                std::unique_ptr<std::string> &newValue, bool &retry) {
                retry = false;
                newValue = std::make_unique<std::string>(nextValue);
                return Status::OK();
            });
    };
    return CommitTopologyUpdate(toDelete,
        [this](std::string &v) { return ReadTopologyEtcd(v); }, deleter, casWriter, results);
}

Status ClusterAdminClient::Impl::DeleteClusterMembersCoordinator(
    const std::vector<std::string> &addresses, std::vector<DeleteClusterMemberResult> &results)
{
    std::vector<std::string> toDelete;
    PreCheckAddresses(addresses, [this](const std::string &addr, bool &absent) {
        return CheckMemberAbsentCoordinator(addr, absent);
    }, results, toDelete);
    if (toDelete.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(DeletePerAddressKeysCoordinator(toDelete, results));
    auto deleter = [this]() {
        std::string pk = keys_->TopologyTable() + "/";
        int64_t delCount = 0;
        int64_t revision = 0;
        return coordinatorProxy_->DeleteRange(pk, "", delCount, revision, ADMIN_RPC_TIMEOUT_MS);
    };
    std::string nextValue;
    auto casWriter = [this, &nextValue](std::string &) {
        std::string pk = keys_->TopologyTable() + "/";
        int64_t version = 0;
        int64_t revision = 0;
        return coordinatorProxy_->CAS(pk,
            [&nextValue](const std::string &, std::unique_ptr<std::string> &newValue, bool &retry) {
                retry = true;
                newValue = std::make_unique<std::string>(nextValue);
                return Status::OK();
            }, version, revision);
    };
    return CommitTopologyUpdate(toDelete,
        [this](std::string &v) { return ReadTopologyCoordinator(v); }, deleter, casWriter, results);
}

Status ClusterAdminClient::Impl::DeletePerAddressKeysCoordinator(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    auto deleteKey = [this](const std::string &tableName, const std::string &address, bool &deleted) {
        std::string key;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, key));
        std::string physicalKey = tableName + "/" + key;
        int64_t delCount = 0;
        int64_t revision = 0;
        auto rc = coordinatorProxy_->DeleteRange(physicalKey, "", delCount, revision, ADMIN_RPC_TIMEOUT_MS);
        if (rc.IsError()) {
            return rc;
        }
        deleted = delCount > 0;
        return Status::OK();
    };
    for (const auto &address : addresses) {
        DeleteClusterMemberResult result;
        result.address = address;
        auto rc = deleteKey(keys_->MembershipTable(), address, result.membershipDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        rc = deleteKey(keys_->NotifyTable(), address, result.notifyDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        rc = deleteKey(keys_->ProbeTable(), address, result.probeDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        rc = deleteKey(keys_->UbHealthTable(), address, result.ubHealthDeleted);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        results.push_back(std::move(result));
    }
    return Status::OK();
}

Status ClusterAdminClient::Impl::ReadTopologyCoordinator(std::string &value)
{
    std::string physicalKey = keys_->TopologyTable() + "/";
    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    auto rc = coordinatorProxy_->Range(physicalKey, "", kvs, revision, ADMIN_RPC_TIMEOUT_MS);
    if (rc.IsError()) {
        return rc;
    }
    if (kvs.empty()) {
        return Status(K_NOT_FOUND, "topology table is empty");
    }
    value = kvs.front().value;
    return Status::OK();
}

Status ClusterAdminClient::Impl::DeleteClusterMembers(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    CHECK_FAIL_RETURN_STATUS(initialized_, K_NOT_READY, "cluster admin client is not initialized");
    RETURN_IF_NOT_OK(ValidateAddresses(addresses));
    results.clear();
    return options_.etcdAddress.empty() ? DeleteClusterMembersCoordinator(addresses, results)
                                        : DeleteClusterMembersEtcd(addresses, results);
}

ClusterAdminClient::ClusterAdminClient(ClusterAdminOptions options)
    : impl_(std::make_unique<Impl>(std::move(options)))
{
}

ClusterAdminClient::~ClusterAdminClient() = default;

Status ClusterAdminClient::Init()
{
    return impl_->Init();
}

Status ClusterAdminClient::DeleteClusterMembers(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    return impl_->DeleteClusterMembers(addresses, results);
}

}  // namespace datasystem::client::cluster_admin
