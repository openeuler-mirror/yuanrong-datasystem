/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Router client for selecting worker.
 */

#include <arpa/inet.h>

#include "datasystem/router_client.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/hash_ring.pb.h"
 
namespace datasystem {
static constexpr uint32_t BATCH_WORKER_SIZE_LIMIT = 100;
RouterClient::RouterClient(const std::string &azName, const std::string &etcdAddress, const SensitiveValue &etcdCa,
                           const SensitiveValue &etcdCert, const SensitiveValue &etcdKey,
                           const std::string &etcdDNSName)
    : azName_(azName),
      etcdAddress_(etcdAddress),
      etcdCa_(etcdCa),
      etcdCert_(etcdCert),
      etcdKey_(etcdKey),
      etcdDNSName_(etcdDNSName)
{
}
 
Status RouterClient::Init()
{
    randomData_ = std::make_shared<RandomData>();
    etcdStore_ = std::make_shared<EtcdStore>(etcdAddress_, etcdCa_.GetData(), etcdCert_, etcdKey_, etcdDNSName_);
    auto traceId = Trace::Instance().GetTraceID();
    etcdStore_->SetEventHandler([this, traceId](mvccpb::Event &&event) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        HandleEvent(std::move(event));
    });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Init(), "Failed to connect to etcd.");
 
    std::string etcdTablePrefix = azName_.empty() ? "" : "/" + azName_;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        etcdStore_->CreateTable(ETCD_CLUSTER_TABLE, etcdTablePrefix + "/" + std::string(ETCD_CLUSTER_TABLE)),
        "The table already exists. tableName: " + std::string(ETCD_CLUSTER_TABLE));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        etcdStore_->CreateTable(ETCD_RING_PREFIX, etcdTablePrefix + std::string(ETCD_RING_PREFIX)),
        "The table already exists. tableName: " + std::string(ETCD_RING_PREFIX));
 
    int64_t nodeRevision{};
    int64_t ringRevision{};
    RETURN_IF_NOT_OK(SetupInitialWorkers(nodeRevision, ringRevision));
    RETURN_IF_NOT_OK(etcdStore_->WatchEvents(
        { { ETCD_CLUSTER_TABLE, "", false, nodeRevision }, { ETCD_RING_PREFIX, "", false, ringRevision } }));
    return Status::OK();
}
 
Status RouterClient::SetupInitialWorkers(int64_t &nodeRevision, int64_t &ringRevision)
{
    // init active workers
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    RETURN_IF_NOT_OK(etcdStore_->GetAll(ETCD_CLUSTER_TABLE, outKeyValues, nodeRevision));
    {
        std::lock_guard<std::shared_timed_mutex> lock(eventMutex_);
        for (const auto &kv : outKeyValues) {
            activeWorkerAddrs_.emplace(kv.first);
        }
    }
    RangeSearchResult res;
    // Ring cannot be found if the router client inits before the worker cluster setup, ignore the not found error.
    RETURN_IF_NOT_OK_EXCEPT(etcdStore_->Get(ETCD_RING_PREFIX, "", res), K_NOT_FOUND);
    ringRevision = res.modRevision;
    mvccpb::Event fakeEvent;
    auto fakeKv = fakeEvent.mutable_kv();
    fakeEvent.set_type(mvccpb::Event_EventType::Event_EventType_PUT);
    fakeKv->set_key("");
    fakeKv->set_value(res.value);
    HandleRingEvent(std::move(fakeEvent));
    return Status::OK();
}
 
Status RouterClient::SelectWorker(const std::string &targetWorkerHost, std::string &outIpAddr)
{
    if (etcdStore_ == nullptr) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Init(), "Failed to connect to etcd.");
    }
    outIpAddr.clear();
 
    std::shared_lock<std::shared_timed_mutex> lock(eventMutex_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        !activeWorkerAddrs_.empty(), K_NOT_FOUND,
        FormatString("Failed to find any worker address about the az: %s in etcd: %s.", azName_, etcdAddress_));
 
    // Obtain the target worker address
    in_addr addr4;
    in6_addr addr6;
    bool isValidIpv4 = (inet_pton(AF_INET, targetWorkerHost.c_str(), &addr4) == 1);
    bool isValidIpv6 = (inet_pton(AF_INET6, targetWorkerHost.c_str(), &addr6) == 1);
    if (isValidIpv4 || isValidIpv6) {
        std::string targetWorkerPrefix;
        if (isValidIpv4) {
            targetWorkerPrefix = targetWorkerHost + ":";
        } else if (isValidIpv6) {
            targetWorkerPrefix = "[" + targetWorkerHost + "]:" ;
        }
        auto targetIt = activeWorkerAddrs_.lower_bound(targetWorkerPrefix);
        std::vector<std::string> targetWorkerAddress;
        while (targetIt != activeWorkerAddrs_.end()
               && targetIt->substr(0, targetWorkerPrefix.length()) == targetWorkerPrefix) {
            targetWorkerAddress.emplace_back(*targetIt);
            std::advance(targetIt, 1);
        }
        if (!targetWorkerAddress.empty()) {
            size_t rdIndex = randomData_->GetRandomIndex(targetWorkerAddress.size());
            outIpAddr = targetWorkerAddress[rdIndex];
            return Status::OK();
        }
    } else {
        LOG(ERROR) << FormatString("Invalid IP address: %s", targetWorkerHost.c_str());
    }
 
    LOG(INFO) << "Failed to find the worker of the target node, so randomly select the worker.";
    size_t rdIndex = randomData_->GetRandomIndex(activeWorkerAddrs_.size());
    auto it = activeWorkerAddrs_.begin();
    std::advance(it, rdIndex);
    outIpAddr = *it;
    return Status::OK();
}

 
Status RouterClient::GetWorkerAddrByWorkerId(const std::vector<std::string> &workerIds,
                                             std::vector<std::string> &workerAddrs) const
{
    CHECK_FAIL_RETURN_STATUS(!workerIds.empty(), K_INVALID, "The input IDs is empty.");
    CHECK_FAIL_RETURN_STATUS(workerIds.size() <= BATCH_WORKER_SIZE_LIMIT, K_INVALID,
                             FormatString("The size of IDs is out of limit. It is over %d.", BATCH_WORKER_SIZE_LIMIT));
    CHECK_FAIL_RETURN_STATUS(!std::any_of(workerIds.begin(), workerIds.end(),
                                          [this](const auto &id) { return id.empty() || !Validator::IsIdFormat(id); }),
                             K_INVALID, "There is an ID you provided is empty string or contains invalid characters.");
 
    bool hasSuccessElement = false;
    workerAddrs.clear();
    workerAddrs.reserve(workerIds.size());
    std::shared_lock<std::shared_timed_mutex> lock(eventMutex_);
    for (const auto &workerId : workerIds) {
        auto it = workerId2Addrs_.find(workerId);
        if (it != workerId2Addrs_.end()) {
            hasSuccessElement = true;
            workerAddrs.emplace_back(it->second);
        } else {
            workerAddrs.emplace_back("");
        }
    }
 
    RETURN_OK_IF_TRUE(hasSuccessElement);
    workerAddrs.clear();
    return Status(K_NOT_FOUND, "Cannot found the worker addresses of the worekrIds input.");
}
 
void RouterClient::HandleEvent(mvccpb::Event &&event)
{
    const auto &key = event.kv().key();
 
    if (key.find(ETCD_CLUSTER_TABLE) != std::string::npos) {
        HandleClusterEvent(std::move(event));
    } else if (key.find(ETCD_RING_PREFIX) != std::string::npos) {
        HandleRingEvent(std::move(event));
    } else {
        LOG(ERROR) << "Unrecognized event: " << key;
    }
}
 
void RouterClient::HandleClusterEvent(const mvccpb::Event &event)
{
    std::string nodeHostPortStr = event.kv().key();
    auto pos = nodeHostPortStr.find(ETCD_CLUSTER_TABLE);
    nodeHostPortStr.erase(0, pos + strlen(ETCD_CLUSTER_TABLE) + 1);
 
    if (event.type() == mvccpb::Event_EventType::Event_EventType_PUT) {
        std::lock_guard<std::shared_timed_mutex> lock(eventMutex_);
        LOG(INFO) << "Event Type: Add Node: " << nodeHostPortStr << ", verison: " << event.kv().version();
        activeWorkerAddrs_.emplace(nodeHostPortStr);
    } else if (event.type() == mvccpb::Event_EventType::Event_EventType_DELETE) {
        std::lock_guard<std::shared_timed_mutex> lock(eventMutex_);
        LOG(INFO) << "Event Type: Remove Node: " << nodeHostPortStr << ", verison: " << event.kv().version();
        activeWorkerAddrs_.erase(nodeHostPortStr);
    }
}
 
void RouterClient::HandleRingEvent(const mvccpb::Event &event)
{
    if (event.type() == mvccpb::Event_EventType::Event_EventType_DELETE) {
        LOG(INFO) << "Ignore delete event of ring in version " << event.kv().version();
        return;
    }
 
    HashRingPb newRing;
    if (!newRing.ParseFromString(event.kv().value())) {
        LOG(ERROR) << "Parse ring failed of version " << event.kv().version();
        return;
    }
    std::lock_guard<std::shared_timed_mutex> lock(eventMutex_);
    workerId2Addrs_.clear();
    for (auto &worker : newRing.workers()) {
        workerId2Addrs_.emplace(worker.second.worker_uuid(), worker.first);
    }
 
    LOG(INFO) << "Update ring of version " << event.kv().version() << ", worker size is " << workerId2Addrs_.size()
              << "\n Content: " << MapToString(workerId2Addrs_);
}
}  // namespace datasystem