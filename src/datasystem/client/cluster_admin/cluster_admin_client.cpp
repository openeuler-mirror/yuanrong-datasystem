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
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
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

ClusterTopologyPb ParseOrEmpty(const std::string &bytes)
{
    ClusterTopologyPb pb;
    if (!bytes.empty()) {
        pb.ParseFromString(bytes);
    }
    return pb;
}

Status RemoveMembersFromTopology(const ClusterTopologyPb &current,
    const std::vector<std::string> &addresses, ClusterTopologyPb &next, bool &changed)
{
    next = current;
    changed = false;
    for (const auto &address : addresses) {
        if (next.mutable_members()->erase(address) > 0) {
            changed = true;
        }
    }
    if (changed) {
        next.set_version(current.version() + 1);
    }
    return Status::OK();
}

Status BuildTopologyCasMutation(const std::string &current,
    const std::vector<std::string> &addresses, std::unique_ptr<std::string> &next)
{
    ClusterTopologyPb pb = ParseOrEmpty(current);
    if (pb.members().empty()) {
        return Status::OK();
    }
    ClusterTopologyPb updated;
    bool changed = false;
    RETURN_IF_NOT_OK(RemoveMembersFromTopology(pb, addresses, updated, changed));
    if (!changed || updated.members().empty()) {
        return Status::OK();
    }
    next = std::make_unique<std::string>();
    updated.SerializeToString(next.get());
    return Status::OK();
}

void FillTopologyResult(std::vector<DeleteClusterMemberResult> &results,
    bool removed, uint64_t version)
{
    for (auto &result : results) {
        if (result.error.empty()) {
            result.topologyMemberRemoved = removed;
            result.topologyVersion = version;
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
    Status DeleteClusterMembersEtcd(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status DeleteClusterMembersCoordinator(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status DeletePerAddressKeysEtcd(const std::vector<std::string> &addresses,
        std::vector<DeleteClusterMemberResult> &results);
    Status DeletePerAddressKeysCoordinator(const std::vector<std::string> &addresses,
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

Status ClusterAdminClient::Impl::DeletePerAddressKeysEtcd(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    auto deleteKey = [this](const std::string &tableName, const std::string &address) {
        std::string key;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, key));
        auto rc = etcdStore_->Delete(tableName, key);
        if (rc.IsError() && !IsAddressNotFound(rc)) {
            return rc;
        }
        return Status::OK();
    };
    for (const auto &address : addresses) {
        DeleteClusterMemberResult result;
        result.address = address;
        auto rc = deleteKey(keys_->MembershipTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.membershipDeleted = true;
        rc = deleteKey(keys_->NotifyTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.notifyDeleted = true;
        rc = deleteKey(keys_->ProbeTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.probeDeleted = true;
        rc = deleteKey(keys_->UbHealthTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.ubHealthDeleted = true;
        results.push_back(std::move(result));
    }
    return Status::OK();
}

Status ClusterAdminClient::Impl::DeleteClusterMembersEtcd(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    RETURN_IF_NOT_OK(DeletePerAddressKeysEtcd(addresses, results));
    std::vector<std::string> remaining(addresses.begin(), addresses.end());
    EtcdStore::EtcdProcessFunction process =
        [&remaining](const std::string &current, std::unique_ptr<std::string> &next, bool &retry) {
            retry = false;
            return BuildTopologyCasMutation(current, remaining, next);
        };
    auto casRc = etcdStore_->CAS(keys_->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(), process);
    if (casRc.IsError()) {
        for (auto &result : results) {
            if (result.error.empty()) {
                result.error = "topology CAS failed: " + casRc.ToString();
            }
        }
        return casRc;
    }
    std::string topologyValue;
    auto getRc = etcdStore_->Get(keys_->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey(), topologyValue);
    bool topologyEmpty = getRc.IsOk() && ParseOrEmpty(topologyValue).members().empty();
    if (topologyEmpty) {
        (void)etcdStore_->Delete(keys_->TopologyTable(), cluster::TopologyKeyHelper::TopologyKey());
    }
    if (getRc.IsOk()) {
        const auto version = ParseOrEmpty(topologyValue).version();
        FillTopologyResult(results, true, topologyEmpty ? 0 : static_cast<uint64_t>(version));
    }
    return Status::OK();
}

Status ClusterAdminClient::Impl::DeletePerAddressKeysCoordinator(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    auto deleteKey = [this](const std::string &tableName, const std::string &address) {
        std::string key;
        RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, key));
        std::string physicalKey = tableName + "/" + key;
        int64_t deleted = 0;
        int64_t revision = 0;
        auto rc = coordinatorProxy_->DeleteRange(physicalKey, "", deleted, revision, ADMIN_RPC_TIMEOUT_MS);
        if (rc.IsError() && !IsAddressNotFound(rc)) {
            return rc;
        }
        return Status::OK();
    };
    for (const auto &address : addresses) {
        DeleteClusterMemberResult result;
        result.address = address;
        auto rc = deleteKey(keys_->MembershipTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.membershipDeleted = true;
        rc = deleteKey(keys_->NotifyTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.notifyDeleted = true;
        rc = deleteKey(keys_->ProbeTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.probeDeleted = true;
        rc = deleteKey(keys_->UbHealthTable(), address);
        if (rc.IsError()) {
            result.error = rc.ToString();
            results.push_back(std::move(result));
            continue;
        }
        result.ubHealthDeleted = true;
        results.push_back(std::move(result));
    }
    return Status::OK();
}

Status ClusterAdminClient::Impl::DeleteClusterMembersCoordinator(const std::vector<std::string> &addresses,
    std::vector<DeleteClusterMemberResult> &results)
{
    RETURN_IF_NOT_OK(DeletePerAddressKeysCoordinator(addresses, results));
    std::vector<std::string> remaining(addresses.begin(), addresses.end());
    ICoordinatorServiceProxy::CasProcessFunc process =
        [&remaining](const std::string &current, std::unique_ptr<std::string> &next, bool &retry) {
            retry = true;
            return BuildTopologyCasMutation(current, remaining, next);
        };
    std::string topologyPhysicalKey = keys_->TopologyTable() + "/";
    int64_t version = 0;
    int64_t revision = 0;
    auto casRc = coordinatorProxy_->CAS(topologyPhysicalKey, process, version, revision);
    if (casRc.IsError()) {
        for (auto &result : results) {
            if (result.error.empty()) {
                result.error = "topology CAS failed: " + casRc.ToString();
            }
        }
        return casRc;
    }
    coordinator::GetClusterRawSnapshotReqPb req;
    req.set_cluster_name(options_.clusterName);
    coordinator::GetClusterRawSnapshotRspPb rsp;
    auto getRc = coordinatorProxy_->GetClusterRawSnapshot(req, rsp, ADMIN_RPC_TIMEOUT_MS);
    if (getRc.IsOk() && !rsp.topology_kvs().empty()) {
        const auto &topoValue = rsp.topology_kvs(0).value();
        const auto committedVersion = ParseOrEmpty(topoValue).version();
        bool topologyEmpty = ParseOrEmpty(topoValue).members().empty();
        if (topologyEmpty) {
            int64_t deleted = 0;
            int64_t dummyRevision = 0;
            (void)coordinatorProxy_->DeleteRange(topologyPhysicalKey, "", deleted, dummyRevision,
                ADMIN_RPC_TIMEOUT_MS);
        }
        FillTopologyResult(results, true, topologyEmpty ? 0 : static_cast<uint64_t>(committedVersion));
    }
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
