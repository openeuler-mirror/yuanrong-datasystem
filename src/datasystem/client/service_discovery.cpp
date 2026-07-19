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
#include "datasystem/utils/service_discovery.h"

#include <iterator>
#include <memory>
#include <unordered_map>
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/coordinator/coordinator_service_proxy.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/cluster/repository/topology_key_helper.h"
#include "datasystem/cluster/membership/membership_value_codec.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace {
constexpr int COORDINATOR_SERVICE_DISCOVERY_RPC_STUB_CACHE_SIZE = 100;

std::string PickRandomAddr(const std::vector<std::string> &workers, RandomData *randomData)
{
    if (workers.empty()) {
        return "";
    }
    return workers[randomData->GetRandomIndex(workers.size())];
}

Status ParseWorkerAddr(const std::string &pickedAddr, std::string &workerIp, int &workerPort)
{
    HostPort hostPort;
    RETURN_IF_NOT_OK(hostPort.ParseString(pickedAddr));
    workerIp = hostPort.Host();
    workerPort = hostPort.Port();
    return Status::OK();
}

void ResolveHostId(const std::string &hostIdEnvName, std::string &hostId)
{
    hostId.clear();
    if (hostIdEnvName.empty()) {
        return;
    }
    auto envFilePath = GetWorkerEnvFilePath(FLAGS_log_dir);
    auto envHostId = GetStringFromEnv(hostIdEnvName.c_str(), "");
    // Use the actual env variable name as the persisted file key, same as worker FLAGS_host_id_env_name.
    hostId = GetStringFromEnvOrFile(hostIdEnvName.c_str(), envFilePath, hostIdEnvName, "");
    if (!hostId.empty()) {
        if (envHostId.empty()) {
            LOG(INFO) << "Host ID is " << hostId << " from persisted SDK env file " << envFilePath;
        } else {
            LOG(INFO) << "Host ID is " << hostId << " from env " << hostIdEnvName;
        }
    } else {
        VLOG(1) << FormatString(
            "hostId not found in env [%s] or file [%s], affinity policy may not work as "
            "expected",
            hostIdEnvName, envFilePath);
    }
}

std::string ExtractWorkerAddrFromClusterKey(const std::string &key)
{
    const std::string membershipSegment = ETCD_MEMBERSHIP_KEY_SEGMENT;
    auto tablePos = key.rfind(membershipSegment);
    if (tablePos == std::string::npos) {
        return key;
    }
    return key.substr(tablePos + membershipSegment.size());
}

std::string PrefixRangeEnd(const std::string &prefix)
{
    constexpr unsigned char maxKeyByte = 0xff;
    std::string rangeEnd = prefix;
    for (auto iter = rangeEnd.rbegin(); iter != rangeEnd.rend(); ++iter) {
        auto byte = static_cast<unsigned char>(*iter);
        if (byte != maxKeyByte) {
            *iter = static_cast<char>(byte + 1);
            rangeEnd.erase(iter.base(), rangeEnd.end());
            return rangeEnd;
        }
    }
    return "";
}

std::string MemberLifecycleStateName(cluster::MemberLifecycleState state)
{
    switch (state) {
        case cluster::MemberLifecycleState::STARTING:
            return "start";
        case cluster::MemberLifecycleState::RESTARTING:
            return "restart";
        case cluster::MemberLifecycleState::RECOVERING:
            return "recover";
        case cluster::MemberLifecycleState::READY:
            return "ready";
        case cluster::MemberLifecycleState::EXITING:
            return "exiting";
        case cluster::MemberLifecycleState::DOWNGRADE_RESTARTING:
            return "d_rst";
        case cluster::MemberLifecycleState::UNKNOWN:
        default:
            return "unknown";
    }
}

void AppendReadyWorker(const std::string &key, const std::string &valueStr, const std::string &hostId,
                       std::vector<std::string> &sameHost, std::vector<std::string> &other,
                       std::unordered_map<std::string, uint32_t> &workersStateCount)
{
    cluster::MembershipValue value;
    auto rc = cluster::MembershipValueCodec::Decode(valueStr, value);
    if (rc.IsError()) {
        LOG(WARNING) << "Failed to parse keep alive value for worker " << key << ": " << rc.ToString();
        return;
    }
    workersStateCount[MemberLifecycleStateName(value.lifecycleState)]++;
    if (value.lifecycleState != cluster::MemberLifecycleState::READY) {
        return;
    }
    auto workerAddr = ExtractWorkerAddrFromClusterKey(key);
    VLOG(1) << "Worker " << workerAddr << " is ready with hostId: " << value.hostId;
    if (!hostId.empty() && value.hostId == hostId) {
        sameHost.emplace_back(std::move(workerAddr));
    } else {
        other.emplace_back(std::move(workerAddr));
    }
}

void AppendReadyWorkerFromProto(const std::string &key, const std::string &valueStr, const std::string &hostId,
                                std::vector<std::string> &sameHost, std::vector<std::string> &other,
                                std::unordered_map<std::string, uint32_t> &workersStateCount)
{
    AppendReadyWorker(key, valueStr, hostId, sameHost, other, workersStateCount);
}

Status SelectWorkerFromPartitions(const std::vector<std::string> &sameHost, const std::vector<std::string> &other,
                                  ServiceAffinityPolicy affinityPolicy, RandomData *randomData, bool hostAffinityActive,
                                  std::string &workerIp, int &workerPort, bool *isSameNode, bool *isNoAvailableWorker)
{
    if (isNoAvailableWorker != nullptr) {
        *isNoAvailableWorker = false;
    }

    std::string pickedAddr;
    bool pickedSameNode = false;
    if (affinityPolicy == ServiceAffinityPolicy::REQUIRED_SAME_NODE) {
        CHECK_FAIL_RETURN_STATUS(hostAffinityActive, K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
        CHECK_FAIL_RETURN_STATUS(!sameHost.empty(), K_TRY_AGAIN, "No available same-node worker is detected.");
        pickedAddr = PickRandomAddr(sameHost, randomData);
        pickedSameNode = true;
    } else if (affinityPolicy == ServiceAffinityPolicy::PREFERRED_SAME_NODE && !sameHost.empty()) {
        pickedAddr = PickRandomAddr(sameHost, randomData);
        pickedSameNode = true;
    } else {
        // RANDOM, or PREFERRED_SAME_NODE with no same-host worker: pick uniformly across both partitions.
        size_t total = sameHost.size() + other.size();
        if (total == 0) {
            if (isNoAvailableWorker != nullptr) {
                *isNoAvailableWorker = true;
            }
            RETURN_STATUS(K_TRY_AGAIN, "No available worker is detected.");
        }
        size_t idx = randomData->GetRandomIndex(total);
        if (idx < sameHost.size()) {
            pickedAddr = sameHost[idx];
            pickedSameNode = true;
        } else {
            pickedAddr = other[idx - sameHost.size()];
        }
    }

    if (isSameNode != nullptr) {
        *isSameNode = pickedSameNode;
    }
    return ParseWorkerAddr(pickedAddr, workerIp, workerPort);
}

Status ApplyGetAllWorkersPolicy(ServiceAffinityPolicy affinityPolicy, bool hostAffinityActive,
                                std::vector<std::string> &sameHostAddrs, std::vector<std::string> &otherAddrs)
{
    if (affinityPolicy == ServiceAffinityPolicy::RANDOM) {
        // No host-affinity preference; expose every worker via `otherAddrs`.
        otherAddrs.insert(otherAddrs.end(), std::make_move_iterator(sameHostAddrs.begin()),
                          std::make_move_iterator(sameHostAddrs.end()));
        sameHostAddrs.clear();
    } else if (affinityPolicy == ServiceAffinityPolicy::REQUIRED_SAME_NODE) {
        CHECK_FAIL_RETURN_STATUS(hostAffinityActive, K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
        otherAddrs.clear();
    }
    // PREFERRED_SAME_NODE: keep the snapshot partitioned so callers can prefer same-node entries.
    return Status::OK();
}

std::unique_ptr<ICoordinatorServiceProxy> CreateCoordinatorProxy(const HostPort &coordinatorAddr)
{
    if (FLAGS_use_brpc) {
        return std::make_unique<CoordinatorServiceProxyBrpcImpl>(coordinatorAddr);
    }
    return std::make_unique<CoordinatorServiceProxyZmqImpl>(coordinatorAddr);
}
}  // namespace

ServiceDiscovery::ServiceDiscovery(const ServiceDiscoveryOptions &opts)
    : etcdAddress_(opts.etcdAddress),
      clusterName_(opts.clusterName),
      etcdCa_(opts.etcdCa),
      etcdCert_(opts.etcdCert),
      etcdKey_(opts.etcdKey),
      etcdDNSName_(opts.etcdDNSName),
      username_(opts.username),
      password_(opts.password),
      tokenRefreshInterval_(opts.tokenRefreshIntervalSec),
      hostIdEnvName_(opts.hostIdEnvName),
      affinityPolicy_(opts.affinityPolicy)
{
}

Status ServiceDiscovery::Init()
{
    Logging::GetInstance()->Start(CLIENT_LOG_FILENAME, true);
    randomData_ = std::make_shared<RandomData>();
    CHECK_FAIL_RETURN_STATUS(!etcdAddress_.empty(), K_INVALID,
                             FormatString("The value of EtcdAddress should not be empty."));
    CHECK_FAIL_RETURN_STATUS(Validator::ValidateEtcdAddresses("EtcdAddress", etcdAddress_), K_INVALID,
                             FormatString("Invalid etcd address: %s", etcdAddress_));
    etcdStore_ = std::make_shared<EtcdStore>(etcdAddress_, etcdCa_.GetData(), etcdCert_, etcdKey_, etcdDNSName_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Init(), "Failed to connect to etcd.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Authenticate(username_, password_, tokenRefreshInterval_),
                                     "Failed to connect to etcd.");
    ResolveHostId(hostIdEnvName_, hostId_);

    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(cluster::TopologyKeyHelper::Create(clusterName_, keys),
                                     "Invalid cluster name for service discovery.");
    membershipTable_ = keys->MembershipTable();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        etcdStore_->CreateTableWithExactPrefix(membershipTable_, keys->EtcdMembershipTablePrefix()),
        "The membership table already exists. tableName: " + membershipTable_);
    return Status::OK();
}

Status ServiceDiscovery::ObtainWorkers(std::vector<std::string> &sameHost, std::vector<std::string> &other)
{
    if (etcdStore_ == nullptr) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Init(), "Failed to connect to etcd.");
    }

    int64_t nodeRevision;
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->GetAll(membershipTable_, outKeyValues, nodeRevision),
                                     "Failed to fetch cluster info from etcd, ensure etcd service is healthy.");

    sameHost.clear();
    other.clear();
    std::unordered_map<std::string, uint32_t> workersStateCount;
    for (const auto &kv : outKeyValues) {
        AppendReadyWorker(kv.first, kv.second, hostId_, sameHost, other, workersStateCount);
    }
    LOG(INFO) << "The workers state count is " << MapToString(workersStateCount);
    return Status::OK();
}

Status ServiceDiscovery::SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode,
                                      bool *isNoAvailableWorker)
{
    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    RETURN_IF_NOT_OK(ObtainWorkers(sameHost, other));
    return SelectWorkerFromPartitions(sameHost, other, affinityPolicy_, randomData_.get(), HasHostAffinity(), workerIp,
                                      workerPort, isSameNode, isNoAvailableWorker);
}

Status ServiceDiscovery::SelectSameNodeWorker(std::string &workerIp, int &workerPort)
{
    CHECK_FAIL_RETURN_STATUS(!hostId_.empty(), K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    RETURN_IF_NOT_OK(ObtainWorkers(sameHost, other));
    std::string pickedAddr = PickRandomAddr(sameHost, randomData_.get());
    CHECK_FAIL_RETURN_STATUS(!pickedAddr.empty(), K_RUNTIME_ERROR, "No available same-node worker is detected.");
    return ParseWorkerAddr(pickedAddr, workerIp, workerPort);
}

Status ServiceDiscovery::GetAllWorkers(std::vector<std::string> &sameHostAddrs, std::vector<std::string> &otherAddrs)
{
    RETURN_IF_NOT_OK(ObtainWorkers(sameHostAddrs, otherAddrs));
    return ApplyGetAllWorkersPolicy(affinityPolicy_, HasHostAffinity(), sameHostAddrs, otherAddrs);
}

DefaultCoordinatorDiscovery::DefaultCoordinatorDiscovery(std::string serviceAddress)
    : serviceAddress_(std::move(serviceAddress))
{
}

Status DefaultCoordinatorDiscovery::GetCoordinators(std::vector<std::string> &serviceList)
{
    serviceList = { serviceAddress_ };
    return Status::OK();
}

CoordinatorServiceDiscovery::CoordinatorServiceDiscovery(const CoordinatorServiceDiscoveryOptions &opts)
    : serviceAddress_(opts.serviceAddress),
      clusterName_(opts.clusterName),
      hostIdEnvName_(opts.hostIdEnvName),
      affinityPolicy_(opts.affinityPolicy),
      coordinatorDiscovery_(opts.coordinatorDiscovery)
{
    if (coordinatorDiscovery_ == nullptr && !serviceAddress_.empty()) {
        coordinatorDiscovery_ = std::make_shared<DefaultCoordinatorDiscovery>(serviceAddress_);
    }
}

Status CoordinatorServiceDiscovery::Init()
{
    Logging::GetInstance()->Start(CLIENT_LOG_FILENAME, true);
    CHECK_FAIL_RETURN_STATUS(coordinatorDiscovery_ != nullptr, K_INVALID,
                             "coordinatorDiscovery should not be null when serviceAddress is empty.");
    std::unique_ptr<cluster::TopologyKeyHelper> keys;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(cluster::TopologyKeyHelper::Create(clusterName_, keys),
                                     "Invalid cluster name for coordinator service discovery.");
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(COORDINATOR_SERVICE_DISCOVERY_RPC_STUB_CACHE_SIZE));
    ResolveHostId(hostIdEnvName_, hostId_);
    membershipTable_ = keys->MembershipTable();
    randomData_ = std::make_shared<RandomData>();
    return Status::OK();
}

Status CoordinatorServiceDiscovery::ObtainWorkers(std::vector<std::string> &sameHost, std::vector<std::string> &other)
{
    if (randomData_ == nullptr) {
        RETURN_IF_NOT_OK(Init());
    }

    std::vector<std::string> coordinatorList;
    RETURN_IF_NOT_OK(coordinatorDiscovery_->GetCoordinators(coordinatorList));
    CHECK_FAIL_RETURN_STATUS(!coordinatorList.empty(), K_INVALID, "No coordinator address is detected.");
    CHECK_FAIL_RETURN_STATUS(coordinatorList.size() == 1, K_INVALID,
                             "Only one coordinator address is supported by coordinator service discovery.");

    sameHost.clear();
    other.clear();
    std::unordered_map<std::string, uint32_t> workersStateCount;
    const std::string clusterTablePrefix = membershipTable_ + "/";
    auto rangeEnd = PrefixRangeEnd(clusterTablePrefix);
    CHECK_FAIL_RETURN_STATUS(!rangeEnd.empty(), K_INVALID, "Failed to build coordinator cluster table range.");

    const auto &coordinatorAddrStr = coordinatorList.front();
    HostPort coordinatorAddr;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(coordinatorAddr.ParseString(coordinatorAddrStr),
                                     "Invalid coordinator address " + coordinatorAddrStr);

    std::vector<KeyValueEntry> kvs;
    int64_t revision = 0;
    auto proxy = CreateCoordinatorProxy(coordinatorAddr);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(proxy->Range(clusterTablePrefix, rangeEnd, kvs, revision),
                                     "Failed to fetch cluster info from coordinator " + coordinatorAddrStr);

    for (const auto &kv : kvs) {
        AppendReadyWorkerFromProto(kv.key, kv.value, hostId_, sameHost, other, workersStateCount);
    }
    LOG(INFO) << "The workers state count from coordinator " << coordinatorAddrStr << " is "
              << MapToString(workersStateCount);
    return Status::OK();
}

Status CoordinatorServiceDiscovery::SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode,
                                                 bool *isNoAvailableWorker)
{
    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    RETURN_IF_NOT_OK(ObtainWorkers(sameHost, other));
    return SelectWorkerFromPartitions(sameHost, other, affinityPolicy_, randomData_.get(), HasHostAffinity(), workerIp,
                                      workerPort, isSameNode, isNoAvailableWorker);
}

Status CoordinatorServiceDiscovery::SelectSameNodeWorker(std::string &workerIp, int &workerPort)
{
    CHECK_FAIL_RETURN_STATUS(!hostId_.empty(), K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    RETURN_IF_NOT_OK(ObtainWorkers(sameHost, other));
    std::string pickedAddr = PickRandomAddr(sameHost, randomData_.get());
    CHECK_FAIL_RETURN_STATUS(!pickedAddr.empty(), K_RUNTIME_ERROR, "No available same-node worker is detected.");
    return ParseWorkerAddr(pickedAddr, workerIp, workerPort);
}

Status CoordinatorServiceDiscovery::GetAllWorkers(std::vector<std::string> &sameHostAddrs,
                                                  std::vector<std::string> &otherAddrs)
{
    RETURN_IF_NOT_OK(ObtainWorkers(sameHostAddrs, otherAddrs));
    return ApplyGetAllWorkersPolicy(affinityPolicy_, HasHostAffinity(), sameHostAddrs, otherAddrs);
}
}  // namespace datasystem
