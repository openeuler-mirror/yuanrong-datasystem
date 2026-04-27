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
#include "datasystem/utils/service_discovery.h"

#include <cstdlib>

#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"

namespace datasystem {
namespace {
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
    auto traceId = Trace::Instance().GetTraceID();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Init(), "Failed to connect to etcd.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Authenticate(username_, password_, tokenRefreshInterval_),
                                     "Failed to connect to etcd.");
    if (!hostIdEnvName_.empty()) {
        const char *hostId = std::getenv(hostIdEnvName_.c_str());
        if (hostId != nullptr) {
            hostId_ = hostId;
            LOG(INFO) << "Host ID from environment: " << hostId_;
        }
    }

    std::string etcdTablePrefix = clusterName_.empty() ? "" : "/" + clusterName_;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        etcdStore_->CreateTable(ETCD_CLUSTER_TABLE, etcdTablePrefix + "/" + std::string(ETCD_CLUSTER_TABLE)),
        "The table already exists. tableName: " + std::string(ETCD_CLUSTER_TABLE));
    return Status::OK();
}

Status ServiceDiscovery::ObtainWorkers(std::vector<std::string> &sameHost, std::vector<std::string> &other)
{
    if (etcdStore_ == nullptr) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Init(), "Failed to connect to etcd.");
    }

    int64_t nodeRevision;
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->GetAll(ETCD_CLUSTER_TABLE, outKeyValues, nodeRevision),
                                     "Failed to fetch cluster info from etcd, ensure etcd service is healthy.");

    sameHost.clear();
    other.clear();
    std::unordered_map<std::string, uint32_t> workersStateCount;
    for (const auto &kv : outKeyValues) {
        KeepAliveValue value;
        auto rc = KeepAliveValue::FromString(kv.second, value);
        if (rc.IsError()) {
            LOG(WARNING) << "Failed to parse keep alive value for worker " << kv.first << ": " << rc.ToString();
            continue;
        }
        workersStateCount[value.state]++;
        if (value.state != ETCD_NODE_READY) {
            continue;
        }
        VLOG(1) << "Worker " << kv.first << " is ready with hostId: " << value.hostId;
        if (!hostId_.empty() && value.hostId == hostId_) {
            sameHost.emplace_back(kv.first);
        } else {
            other.emplace_back(kv.first);
        }
    }
    LOG(INFO) << "The workers state count is " << MapToString(workersStateCount);
    return Status::OK();
}

Status ServiceDiscovery::SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode)
{
    std::vector<std::string> sameHost;
    std::vector<std::string> other;
    RETURN_IF_NOT_OK(ObtainWorkers(sameHost, other));

    std::string pickedAddr;
    bool pickedSameNode = false;
    if (affinityPolicy_ == ServiceAffinityPolicy::REQUIRED_SAME_NODE) {
        CHECK_FAIL_RETURN_STATUS(!hostId_.empty(), K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
        CHECK_FAIL_RETURN_STATUS(!sameHost.empty(), K_RUNTIME_ERROR, "No available same-node worker is detected.");
        pickedAddr = PickRandomAddr(sameHost, randomData_.get());
        pickedSameNode = true;
    } else if (affinityPolicy_ == ServiceAffinityPolicy::PREFERRED_SAME_NODE && !sameHost.empty()) {
        pickedAddr = PickRandomAddr(sameHost, randomData_.get());
        pickedSameNode = true;
    } else {
        // RANDOM, or PREFERRED_SAME_NODE with no same-host worker: pick uniformly across both partitions.
        size_t total = sameHost.size() + other.size();
        CHECK_FAIL_RETURN_STATUS(total > 0, K_RUNTIME_ERROR, "No available worker is detected.");
        size_t idx = randomData_->GetRandomIndex(total);
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
    if (affinityPolicy_ == ServiceAffinityPolicy::RANDOM) {
        // No host-affinity preference; expose every worker via `otherAddrs`.
        otherAddrs.insert(otherAddrs.end(), std::make_move_iterator(sameHostAddrs.begin()),
                          std::make_move_iterator(sameHostAddrs.end()));
        sameHostAddrs.clear();
    } else if (affinityPolicy_ == ServiceAffinityPolicy::REQUIRED_SAME_NODE) {
        CHECK_FAIL_RETURN_STATUS(!hostId_.empty(), K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
        otherAddrs.clear();
    }
    // PREFERRED_SAME_NODE: keep the snapshot partitioned so callers can prefer same-node entries.
    return Status::OK();
}
}  // namespace datasystem
