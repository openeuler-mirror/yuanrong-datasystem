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

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
ServiceDiscovery::ServiceDiscovery(const std::string &etcdAddress, const std::string &clusterName,
                                   const SensitiveValue &etcdCa, const SensitiveValue &etcdCert,
                                   const SensitiveValue &etcdKey, const std::string &etcdDNSName,
                                   const std::string &username, const SensitiveValue &password)
    : etcdAddress_(etcdAddress),
      clusterName_(clusterName),
      etcdCa_(etcdCa),
      etcdCert_(etcdCert),
      etcdKey_(etcdKey),
      etcdDNSName_(etcdDNSName),
      username_(username),
      password_(password)
{
}

Status ServiceDiscovery::Init()
{
    randomData_ = std::make_shared<RandomData>();
    etcdStore_ = std::make_shared<EtcdStore>(etcdAddress_, etcdCa_.GetData(), etcdCert_, etcdKey_, etcdDNSName_);
    auto traceId = Trace::Instance().GetTraceID();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Init(), "Failed to connect to etcd.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Authenticate(username_, password_), "Failed to connect to etcd.");

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
        activeWorkerAddrs_.clear();
        for (const auto &kv : outKeyValues) {
            // value format : timestamp;event_type.
            auto pos = kv.second.find(';');
            if (pos == std::string::npos)
                continue;
            std::string state = kv.second.substr(pos + 1);
            // select active state and filter out exiting/timeout/failed.
            if (state == "ready") {
                activeWorkerAddrs_.emplace(kv.first);
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
    if (activeWorkerAddrs_.empty()) {
        return Status(K_RUNTIME_ERROR, "No available worker available is detected.");
    }

    // randomly pick an available worker.
    size_t rdIndex = randomData_->GetRandomIndex(activeWorkerAddrs_.size());
    auto it = activeWorkerAddrs_.begin();
    std::advance(it, rdIndex);
    std::string pickedAddr = *it;

    // parse worker ip and port from string.
    HostPort hostPort;
    auto rc = hostPort.ParseString(pickedAddr);
    workerIp = hostPort.Host();
    workerPort = hostPort.Port();
    return Status::OK();
}
}  // namespace datasystem
