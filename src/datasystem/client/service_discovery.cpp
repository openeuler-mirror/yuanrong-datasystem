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

#include <arpa/inet.h>
#include <cstdlib>

#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
namespace {
std::string SelectWorkerAddr(const std::unordered_map<std::string, std::string> &workerAddrs, RandomData *randomData)
{
    size_t rdIndex = randomData->GetRandomIndex(workerAddrs.size());
    size_t index = 0;
    for (const auto &worker : workerAddrs) {
        if (index++ == rdIndex) {
            return worker.first;
        }
    }
    return "";
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

Status ServiceDiscovery::ObtainWorkers()
{
    int64_t nodeRevision;
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->GetAll(ETCD_CLUSTER_TABLE, outKeyValues, nodeRevision),
                                     "Failed to fetch cluster info from etcd, ensure etcd service is healthy.");
    {
        std::lock_guard<std::shared_timed_mutex> lock(workerHostPortMutext_);
        activeWorkerInfo_.clear();
        for (const auto &kv : outKeyValues) {
            KeepAliveValue value;
            auto rc = KeepAliveValue::FromString(kv.second, value);
            if (rc.IsError()) {
                LOG(WARNING) << "Failed to parse keep alive value for worker " << kv.first << ": " << rc.ToString();
                continue;
            }
            if (value.state == "ready") {
                VLOG(1) << "Worker " << kv.first << " is ready with hostId: " << value.hostId;
                activeWorkerInfo_[kv.first] = value.hostId;
            }
        }
    }
    return Status::OK();
}

Status ServiceDiscovery::SelectWorker(std::string &workerIp, int &workerPort)
{
    if (etcdStore_ == nullptr) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Init(), "Failed to connect to etcd.");
    }

    RETURN_IF_NOT_OK(this->ObtainWorkers());
    if (activeWorkerInfo_.empty()) {
        return Status(K_RUNTIME_ERROR, "No available worker available is detected.");
    }

    std::string pickedAddr;
    if (affinityPolicy_ == ServiceAffinityPolicy::RANDOM) {
        pickedAddr = SelectWorkerAddr(activeWorkerInfo_, randomData_.get());
    } else {
        std::unordered_map<std::string, std::string> sameHostWorkers;
        if (!hostId_.empty()) {
            for (const auto &workerInfo : activeWorkerInfo_) {
                if (workerInfo.second == hostId_) {
                    sameHostWorkers.emplace(workerInfo);
                }
            }
        }
        if (affinityPolicy_ == ServiceAffinityPolicy::REQUIRED_SAME_NODE) {
            CHECK_FAIL_RETURN_STATUS(!hostId_.empty(), K_INVALID, "Failed to obtain sdk host_id from hostIdEnvName.");
            CHECK_FAIL_RETURN_STATUS(!sameHostWorkers.empty(), K_RUNTIME_ERROR,
                                     "No available same-node worker is detected.");
            pickedAddr = SelectWorkerAddr(sameHostWorkers, randomData_.get());
        } else {
            pickedAddr = sameHostWorkers.empty() ? SelectWorkerAddr(activeWorkerInfo_, randomData_.get())
                                                 : SelectWorkerAddr(sameHostWorkers, randomData_.get());
        }
    }

    // parse worker ip and port from string.
    HostPort hostPort;
    auto rc = hostPort.ParseString(pickedAddr);
    workerIp = hostPort.Host();
    workerPort = hostPort.Port();
    return Status::OK();
}
}  // namespace datasystem
