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
#include "datasystem/client/cluster_query/cluster_query_client.h"

#include <chrono>
#include <map>
#include <memory>
#include <string_view>
#include <utility>

#include "datasystem/client/cluster_query/cluster_query_projector.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/repository/topology_repository_codec.h"
#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/kvstore/coordination_keys.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

namespace datasystem::client::cluster_query {
namespace {

constexpr size_t MAX_RAW_SNAPSHOT_BYTES = 16 * 1'024 * 1'024;
constexpr size_t MAX_RAW_MEMBERSHIPS = 10'000;
constexpr uint64_t QUERY_RPC_STUB_CACHE_SIZE = 1;
using QueryDeadline = std::chrono::time_point<std::chrono::steady_clock>;

Status RemainingTimeoutMs(const QueryDeadline &deadline, int32_t &timeoutMs)
{
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline
                                                                                - std::chrono::steady_clock::now());
    CHECK_FAIL_RETURN_STATUS(remaining.count() > 0, K_RPC_DEADLINE_EXCEEDED, "cluster query deadline exceeded");
    timeoutMs = static_cast<int32_t>(remaining.count());
    return Status::OK();
}

Status BuildTopology(const std::string &value, int64_t revision,
                     std::shared_ptr<const cluster::TopologySnapshot> &snapshot)
{
    cluster::TopologyState state;
    RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::DecodeTopology(value, state));
    std::string canonical;
    RETURN_IF_NOT_OK(cluster::TopologyRepositoryCodec::EncodeTopology(state, canonical));
    std::string digest;
    RETURN_IF_NOT_OK(Hasher().GetSha256Hex(canonical, digest));
    return cluster::TopologySnapshot::Create(std::move(state), revision, std::move(digest), snapshot);
}

Status AppendMembership(const std::string &address, const std::string &value,
                        std::map<std::string, MembershipObservation> &memberships)
{
    std::string canonicalAddress;
    RETURN_IF_NOT_OK(cluster::TopologyKeyHelper::MembershipKey(address, canonicalAddress));
    cluster::MembershipValue decoded;
    RETURN_IF_NOT_OK(cluster::MembershipValueCodec::Decode(value, decoded));
    const bool inserted = memberships
                              .emplace(canonicalAddress,
                                       MembershipObservation{ canonicalAddress, decoded.lifecycleState })
                              .second;
    CHECK_FAIL_RETURN_STATUS(inserted, K_INVALID, "duplicate cluster membership address");
    return Status::OK();
}

Status AppendPayloadSize(size_t keyBytes, size_t valueBytes, size_t &totalBytes)
{
    CHECK_FAIL_RETURN_STATUS(keyBytes <= MAX_RAW_SNAPSHOT_BYTES
                                 && valueBytes <= MAX_RAW_SNAPSHOT_BYTES - keyBytes,
                             K_OUT_OF_RANGE, "raw cluster entry exceeds response limit");
    const size_t entryBytes = keyBytes + valueBytes;
    CHECK_FAIL_RETURN_STATUS(totalBytes <= MAX_RAW_SNAPSHOT_BYTES - entryBytes, K_OUT_OF_RANGE,
                             "raw cluster snapshot exceeds response limit");
    totalBytes += entryBytes;
    return Status::OK();
}

Status ValidateEtcdPayload(const std::vector<std::pair<std::string, std::string>> &memberships,
                           const std::vector<std::pair<std::string, std::string>> &topology)
{
    CHECK_FAIL_RETURN_STATUS(memberships.size() <= MAX_RAW_MEMBERSHIPS && topology.size() <= 1, K_INVALID,
                             "invalid ETCD cluster snapshot cardinality");
    size_t totalBytes = 0;
    for (const auto &[key, value] : memberships) {
        RETURN_IF_NOT_OK(AppendPayloadSize(key.size(), value.size(), totalBytes));
    }
    for (const auto &[key, value] : topology) {
        RETURN_IF_NOT_OK(AppendPayloadSize(key.size(), value.size(), totalBytes));
    }
    return Status::OK();
}

Status DecodeEtcdSnapshot(const std::vector<std::pair<std::string, std::string>> &rawMemberships,
                          const std::vector<std::pair<std::string, std::string>> &rawTopology,
                          int64_t revision, ClusterSourceSnapshot &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(!rawTopology.empty() && rawTopology.front().first.empty(), K_NOT_READY,
                             "cluster topology is unavailable");
    ClusterSourceSnapshot built;
    RETURN_IF_NOT_OK(BuildTopology(rawTopology.front().second, revision, built.topology));
    std::map<std::string, MembershipObservation> memberships;
    for (const auto &[address, value] : rawMemberships) {
        RETURN_IF_NOT_OK(AppendMembership(address, value, memberships));
    }
    for (auto &entry : memberships) {
        built.memberships.emplace_back(std::move(entry.second));
    }
    snapshot = std::move(built);
    return Status::OK();
}

Status ValidateCoordinatorRaw(const coordinator::GetClusterRawSnapshotRspPb &rsp,
                              const cluster::TopologyKeyHelper &keys)
{
    CHECK_FAIL_RETURN_STATUS(rsp.ByteSizeLong() <= MAX_RAW_SNAPSHOT_BYTES, K_INVALID,
                             "invalid raw cluster snapshot boundary");
    CHECK_FAIL_RETURN_STATUS(rsp.topology_kvs_size() <= 1
                                 && static_cast<size_t>(rsp.membership_kvs_size()) <= MAX_RAW_MEMBERSHIPS,
                             K_INVALID, "invalid raw cluster snapshot cardinality");
    const std::string topologyKey = keys.TopologyTable() + "/";
    const std::string membershipPrefix = keys.MembershipTable() + "/";
    for (const auto &kv : rsp.topology_kvs()) {
        CHECK_FAIL_RETURN_STATUS(kv.key() == topologyKey && kv.mod_revision() >= 0, K_INVALID,
                                 "raw topology key or revision is invalid");
    }
    for (const auto &kv : rsp.membership_kvs()) {
        CHECK_FAIL_RETURN_STATUS(kv.key().rfind(membershipPrefix, 0) == 0 && kv.mod_revision() >= 0,
                                 K_INVALID, "raw membership key or revision is invalid");
    }
    return Status::OK();
}

Status DecodeCoordinatorSnapshot(const coordinator::GetClusterRawSnapshotRspPb &rsp,
                                 const cluster::TopologyKeyHelper &keys, ClusterSourceSnapshot &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(!rsp.topology_kvs().empty(), K_NOT_READY, "cluster topology is unavailable");
    ClusterSourceSnapshot built;
    RETURN_IF_NOT_OK(
        BuildTopology(rsp.topology_kvs(0).value(), rsp.topology_kvs(0).mod_revision(), built.topology));
    const std::string membershipPrefix = keys.MembershipTable() + "/";
    std::map<std::string, MembershipObservation> memberships;
    for (const auto &kv : rsp.membership_kvs()) {
        RETURN_IF_NOT_OK(AppendMembership(kv.key().substr(membershipPrefix.size()), kv.value(), memberships));
    }
    for (auto &entry : memberships) {
        built.memberships.emplace_back(std::move(entry.second));
    }
    snapshot = std::move(built);
    return Status::OK();
}

Status ValidateOptions(const ClusterQueryOptions &options)
{
    const bool hasEtcd = !options.etcdAddress.empty();
    const bool hasCoordinator = !options.coordinatorAddress.empty();
    CHECK_FAIL_RETURN_STATUS(hasEtcd != hasCoordinator, K_INVALID,
                             "exactly one coordination backend address is required");
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    return cluster::TopologyKeyHelper::Create(options.clusterName, keys);
}

}  // namespace

class ClusterQueryClient::Impl final {
public:
    explicit Impl(ClusterQueryOptions options) : options_(std::move(options))
    {
    }

    Status Init();
    Status QueryCluster(ClusterQueryResult &result);
    Status QueryRoutes(const std::vector<std::string> &keys, RouteQueryResult &result);

private:
    Status InitEtcd();
    Status InitCoordinator();
    Status ReadSnapshot(ClusterSourceSnapshot &snapshot);
    Status ReadEtcd(ClusterSourceSnapshot &snapshot);
    Status ReadCoordinator(ClusterSourceSnapshot &snapshot);

    ClusterQueryOptions options_;
    std::unique_ptr<cluster::TopologyKeyHelper> keys_;
    std::unique_ptr<EtcdStore> etcdStore_;
    std::unique_ptr<CoordinatorServiceProxyZmqImpl> coordinatorProxy_;
    ClusterQueryProjector projector_;
    bool initialized_{ false };
};

Status ClusterQueryClient::Impl::InitEtcd()
{
    CHECK_FAIL_RETURN_STATUS(Validator::ValidateEtcdAddresses("etcd_address", options_.etcdAddress), K_INVALID,
                             "invalid etcd address");
    etcdStore_ = std::make_unique<EtcdStore>(options_.etcdAddress);
    RETURN_IF_NOT_OK(etcdStore_->Init());
    RETURN_IF_NOT_OK(etcdStore_->CreateTableWithExactPrefix(keys_->MembershipTable(),
                                                            keys_->EtcdMembershipTablePrefix()));
    return etcdStore_->CreateTableWithExactPrefix(keys_->TopologyTable(), keys_->TopologyTable());
}

Status ClusterQueryClient::Impl::InitCoordinator()
{
    HostPort address;
    RETURN_IF_NOT_OK(address.ParseString(options_.coordinatorAddress));
    CHECK_FAIL_RETURN_STATUS(address.ToString() == options_.coordinatorAddress, K_INVALID,
                             "coordinator address is not canonical");
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(QUERY_RPC_STUB_CACHE_SIZE));
    coordinatorProxy_ = std::make_unique<CoordinatorServiceProxyZmqImpl>(address);
    return Status::OK();
}

Status ClusterQueryClient::Impl::Init()
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

Status ClusterQueryClient::Impl::ReadEtcd(ClusterSourceSnapshot &snapshot)
{
    const QueryDeadline deadline = std::chrono::steady_clock::now()
                                   + std::chrono::milliseconds(CLUSTER_QUERY_TIMEOUT_MS);
    std::vector<std::pair<std::string, std::string>> rawMemberships;
    int64_t revision = 0;
    int32_t timeoutMs = 0;
    RETURN_IF_NOT_OK(RemainingTimeoutMs(deadline, timeoutMs));
    RETURN_IF_NOT_OK(etcdStore_->GetAll(keys_->MembershipTable(), 0, rawMemberships, revision, timeoutMs));
    std::vector<std::pair<std::string, std::string>> rawTopology;
    int64_t topologyRevision = 0;
    RETURN_IF_NOT_OK(RemainingTimeoutMs(deadline, timeoutMs));
    RETURN_IF_NOT_OK(
        etcdStore_->GetAll(keys_->TopologyTable(), revision, rawTopology, topologyRevision, timeoutMs));
    RETURN_IF_NOT_OK(ValidateEtcdPayload(rawMemberships, rawTopology));
    return DecodeEtcdSnapshot(rawMemberships, rawTopology, revision, snapshot);
}

Status ClusterQueryClient::Impl::ReadCoordinator(ClusterSourceSnapshot &snapshot)
{
    coordinator::GetClusterRawSnapshotReqPb req;
    req.set_cluster_name(options_.clusterName);
    coordinator::GetClusterRawSnapshotRspPb rsp;
    RETURN_IF_NOT_OK(coordinatorProxy_->GetClusterRawSnapshot(req, rsp, CLUSTER_QUERY_TIMEOUT_MS));
    RETURN_IF_NOT_OK(ValidateCoordinatorRaw(rsp, *keys_));
    return DecodeCoordinatorSnapshot(rsp, *keys_, snapshot);
}

Status ClusterQueryClient::Impl::ReadSnapshot(ClusterSourceSnapshot &snapshot)
{
    return options_.etcdAddress.empty() ? ReadCoordinator(snapshot) : ReadEtcd(snapshot);
}

Status ClusterQueryClient::Impl::QueryCluster(ClusterQueryResult &result)
{
    CHECK_FAIL_RETURN_STATUS(initialized_, K_NOT_READY, "cluster query client is not initialized");
    ClusterSourceSnapshot snapshot;
    RETURN_IF_NOT_OK(ReadSnapshot(snapshot));
    return projector_.BuildCluster(snapshot, result);
}

Status ClusterQueryClient::Impl::QueryRoutes(const std::vector<std::string> &keys, RouteQueryResult &result)
{
    CHECK_FAIL_RETURN_STATUS(initialized_, K_NOT_READY, "cluster query client is not initialized");
    std::vector<std::string_view> views;
    views.reserve(keys.size());
    for (const auto &key : keys) {
        views.emplace_back(key);
    }
    RETURN_IF_NOT_OK(ClusterQueryProjector::ValidateRouteKeys(views));
    ClusterSourceSnapshot snapshot;
    RETURN_IF_NOT_OK(ReadSnapshot(snapshot));
    return projector_.Route(snapshot, views, result);
}

ClusterQueryClient::ClusterQueryClient(ClusterQueryOptions options)
    : impl_(std::make_unique<Impl>(std::move(options)))
{
}

ClusterQueryClient::~ClusterQueryClient() = default;

Status ClusterQueryClient::Init()
{
    return impl_->Init();
}

Status ClusterQueryClient::QueryCluster(ClusterQueryResult &result)
{
    return impl_->QueryCluster(result);
}

Status ClusterQueryClient::QueryRoutes(const std::vector<std::string> &keys, RouteQueryResult &result)
{
    return impl_->QueryRoutes(keys, result);
}

}  // namespace datasystem::client::cluster_query
