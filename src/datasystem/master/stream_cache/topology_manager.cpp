/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: The interface of file cache worker descriptor.
 */
#include <mutex>

#include "datasystem/master/stream_cache/topology_manager.h"

#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {
TopologyManager::TopologyManager(std::string streamName) : streamName_(std::move(streamName)), isDeleting_(false)
{
}

Status TopologyManager::PubIncreaseNode(const ProducerMetaPb &producerMeta, bool &isFirstProducer)
{
    // Add pub worker node into pubTopo set.
    HostPort workerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    std::string workerAddressStr = workerAddress.ToString();
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto iter = producerCount_.find(workerAddressStr);
    if (iter == producerCount_.end()) {
        iter = producerCount_.emplace(workerAddressStr, ProducerCount()).first;
    }
    CHECK_FAIL_RETURN_STATUS(iter->second.firstProducerProcessing_ == false, K_TRY_AGAIN,
                             FormatString("First create producer request or the last close producer request from the "
                                          "worker [%s] for stream <%s> is not done yet, try again later.",
                                          workerAddressStr, streamName_));
    // Check if this is first request for the worker
    CHECK_FAIL_RETURN_STATUS(!ExistsProducerUnlocked(workerAddress), K_DUPLICATED,
                             "producer for worker already exists");

    auto preCount = iter->second.count_++;
    currentProducerCount_++;
    isFirstProducer = (preCount == 0);
    if (isFirstProducer) {
        iter->second.firstProducerProcessing_ = true;
    }
    return Status::OK();
}

Status TopologyManager::PubNodeFirstOrLastDone(const ProducerMetaPb &producerMeta)
{
    // Add pub worker node into pubTopo set.
    HostPort workerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto iter = producerCount_.find(workerAddress.ToString());
    CHECK_FAIL_RETURN_STATUS(
        iter != producerCount_.end() && iter->second.count_ == 1 && iter->second.firstProducerProcessing_,
        StatusCode::K_RUNTIME_ERROR,
        FormatString("Invalid producer source node <%s>, should be the first or last producer",
                     workerAddress.ToString()));
    iter->second.firstProducerProcessing_ = false;
    return Status::OK();
}

Status TopologyManager::PubDecreaseNodeStart(const ProducerMetaPb &producerMeta, bool &isLastProducer)
{
    HostPort workerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    std::string workerAddressStr = workerAddress.ToString();
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto iter = producerCount_.find(workerAddressStr);
    CHECK_FAIL_RETURN_STATUS(iter->second.firstProducerProcessing_ == false, K_TRY_AGAIN,
                             FormatString("First create producer request or the last close producer request from the "
                                          "worker [%s] for stream <%s> is not done yet, try again later.",
                                          workerAddressStr, streamName_));
    CHECK_FAIL_RETURN_STATUS(iter != producerCount_.end() && iter->second.count_ > 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Invalid producer source node <%s>", workerAddressStr));
    isLastProducer = (iter->second.count_ == 1);
    if (isLastProducer) {
        iter->second.firstProducerProcessing_ = true;
    }
    return Status::OK();
}

Status TopologyManager::PubDecreaseNode(const ProducerMetaPb &producerMeta, const bool delWorker)
{
    HostPort workerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto iter = producerCount_.find(workerAddress.ToString());

    CHECK_FAIL_RETURN_STATUS(iter != producerCount_.end() && iter->second.count_ > 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Invalid producer source node <%s>", workerAddress.ToString()));

    // Keep the producer count when deduct to 0 for delete stream.
    iter->second.count_--;
    currentProducerCount_--;
    // If last producer then delete the related node if worker is lost
    if (delWorker && !iter->second.count_) {
        LOG(INFO) << "Removing worker " << workerAddress.ToString() << " from producer related nodes";
        producerCount_.erase(workerAddress.ToString());
    }
    return Status::OK();
}

Status TopologyManager::SubIncreaseNode(const ConsumerMetaPb &consumerMeta, bool &isFirstConsumer)
{
    HostPort workerAddress(consumerMeta.worker_address().host(), consumerMeta.worker_address().port());
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    auto ret = consumerTopo_.emplace(consumerMeta.consumer_id(), consumerMeta);
    CHECK_FAIL_RETURN_STATUS(ret.second, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Fail to add consumer [%s] into topo structure for stream <%s>",
                                          consumerMeta.consumer_id(), streamName_));

    const auto &config = consumerMeta.sub_config();
    if (config.subscription_type() == SubscriptionTypePb::STREAM_PB) {
        const std::string &subName = config.subscription_name();
        CHECK_FAIL_RETURN_STATUS(streamModeSubDict_.find(subName) == streamModeSubDict_.end(), StatusCode::K_DUPLICATED,
                                 "In STREAM mode, one subscription can only contain one consumer");
        streamModeSubDict_.emplace(subName);
    }

    auto preCount = consumerCount_[workerAddress.ToString()]++;
    // we also maintain consumer count over stream life
    consumerLifeCount_++;
    LOG(INFO) << "[RetainData] Number of consumers for stream increased to " << consumerLifeCount_;
    isFirstConsumer = (preCount == 0);
    return Status::OK();
}

uint32_t TopologyManager::GetConsumerCountForLife()
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return consumerLifeCount_;
}

Status TopologyManager::SubDecreaseNode(const ConsumerMetaPb &consumerMeta, bool rollback, const bool delWorker)
{
    HostPort workerAddress(consumerMeta.worker_address().host(), consumerMeta.worker_address().port());
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    const auto &config = consumerMeta.sub_config();
    if (config.subscription_type() == SubscriptionTypePb::STREAM_PB) {
        const std::string &subName = config.subscription_name();
        CHECK_FAIL_RETURN_STATUS(streamModeSubDict_.erase(subName) == 1, StatusCode::K_NOT_FOUND,
                                 FormatString("Consumer:<%s>, Subscription:<%s>, Mode:<Stream>, Status:<%s>",
                                              consumerMeta.consumer_id(), subName, "Not found on master"));
    }

    auto iter = consumerCount_.find(workerAddress.ToString());

    CHECK_FAIL_RETURN_STATUS(iter != consumerCount_.end() && iter->second > 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Invalid consumer source node <%s>", workerAddress.ToString()));

    // Keep the consumer count when deduct to 0 for delete stream.
    iter->second--;

    CHECK_FAIL_RETURN_STATUS(consumerTopo_.erase(consumerMeta.consumer_id()) == 1, StatusCode::K_RUNTIME_ERROR,
                             "Fail to delete sub node");
    if (rollback) {
        consumerLifeCount_--;
        LOG(INFO) << "[RetainData] Number of consumers for stream decreased to " << consumerLifeCount_;
    }
    // If last consumer then delete the related node if worker is lost
    if (delWorker && !iter->second) {
        LOG(INFO) << "Removing worker " << workerAddress.ToString() << " from consumer related nodes";
        consumerCount_.erase(workerAddress.ToString());
    }
    return Status::OK();
}

Status TopologyManager::GetAllPubNode(std::set<HostPort> &pubNodeSet, bool informAll) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : producerCount_) {
        if (kv.second.count_ > 0 || informAll) {
            HostPort nodeAddr;
            RETURN_IF_NOT_OK(nodeAddr.ParseString(kv.first));
            pubNodeSet.emplace(std::move(nodeAddr));
        }
    }
    return Status::OK();
}

Status TopologyManager::GetAllSubNode(std::set<HostPort> &subNodeSet) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : consumerCount_) {
        if (kv.second > 0) {
            HostPort nodeAddr;
            RETURN_IF_NOT_OK(nodeAddr.ParseString(kv.first));
            subNodeSet.emplace(std::move(nodeAddr));
        }
    }
    return Status::OK();
}

Status TopologyManager::GetAllRelatedNode(std::set<HostPort> &nodeSet) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : producerCount_) {
        HostPort nodeAddr;
        RETURN_IF_NOT_OK(nodeAddr.ParseString(kv.first));
        (void)nodeSet.emplace(std::move(nodeAddr));
    }

    for (const auto &kv : consumerCount_) {
        HostPort nodeAddr;
        RETURN_IF_NOT_OK(nodeAddr.ParseString(kv.first));
        (void)nodeSet.emplace(std::move(nodeAddr));
    }
    return Status::OK();
}

void TopologyManager::GetAllRelatedNode(std::vector<std::string> &producerRelatedNodes,
                                        std::vector<std::string> &consumerRelatedNodes) const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    producerRelatedNodes.reserve(producerCount_.size());
    for (const auto &kv : producerCount_) {
        producerRelatedNodes.emplace_back(kv.first);
    }

    consumerRelatedNodes.reserve(consumerCount_.size());
    for (const auto &kv : consumerCount_) {
        consumerRelatedNodes.emplace_back(kv.first);
    }
}

bool TopologyManager::RecoverEmptyMetaIfNeeded(const HostPort &nodeAddr)
{
    std::set<HostPort> nodeSet;
    auto rc = GetAllRelatedNode(nodeSet);
    if (rc.IsError()) {
        LOG(WARNING) << "GetAllRelatedNode failed: " << rc.ToString();
        return false;
    }
    if (nodeSet.count(nodeAddr) > 0) {
        return false;
    }
    LOG(INFO) << FormatString("[S: %s] Recover empty meta for worker: %s", streamName_, nodeAddr.ToString());
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    if (producerCount_.find(nodeAddr.ToString()) == producerCount_.end()) {
        (void)producerCount_.emplace(nodeAddr.ToString(), ProducerCount());
    }
    if (consumerCount_.find(nodeAddr.ToString()) == consumerCount_.end()) {
        (void)consumerCount_.emplace(nodeAddr.ToString(), 0);
    }
    return true;
}

void TopologyManager::PreparePubSubRelNodes(const std::vector<std::string> &producerRelatedNodes,
                                            const std::vector<std::string> &consumerRelatedNodes)
{
    for (const auto &producerAddr : producerRelatedNodes) {
        producerCount_.emplace(producerAddr, ProducerCount());
    }
    for (const auto &consumerAddr : consumerRelatedNodes) {
        consumerCount_.emplace(consumerAddr, 0);
    }
}

std::vector<ConsumerMetaPb> TopologyManager::GetAllConsumerNotFromSrc(const std::string &srcNode) const
{
    std::vector<ConsumerMetaPb> consumerList;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : consumerTopo_) {
        const auto hostPortPb = kv.second.worker_address();
        HostPort addr(hostPortPb.host(), hostPortPb.port());
        if (addr.ToString() != srcNode) {
            consumerList.emplace_back(kv.second);
        }
    }
    return consumerList;
}

std::unordered_map<std::string, ConsumerMetaPb> TopologyManager::GetAllConsumerFromWorker(
    const HostPort &workerAddress) const
{
    std::unordered_map<std::string, ConsumerMetaPb> consumerMap;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (auto &kv : consumerTopo_) {
        const auto hostPortPb = kv.second.worker_address();
        HostPort addr(hostPortPb.host(), hostPortPb.port());
        if (addr == workerAddress) {
            consumerMap.emplace(kv.first, kv.second);
        }
    }
    return consumerMap;
}

Status TopologyManager::GetAllProducerFromWorker(const HostPort &workerAddress,
                                                 std::unordered_map<std::string, ProducerMetaPb> &producerMap)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &kv : producerCount_) {
        if (kv.first == workerAddress.ToString()) {
            // Create a temporary ProducerMetaPb
            ProducerMetaPb producerMeta;

            // Add stream name, Address, and producer count to it (which will be 0 or 1)
            producerMeta.set_stream_name(streamName_);
            HostPort workerHostPort;
            RETURN_IF_NOT_OK(workerHostPort.ParseString(kv.first));
            producerMeta.mutable_worker_address()->set_host(workerHostPort.Host());
            producerMeta.mutable_worker_address()->set_port(workerHostPort.Port());
            producerMeta.set_producer_count(kv.second.count_);

            // Key: WorkerAddress Value: MetaPb
            producerMap.emplace(kv.first, producerMeta);
        }
    }
    return Status::OK();
}

Status TopologyManager::GetAllProducer(std::vector<ProducerMetaPb> &producerList)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    producerList.reserve(producerCount_.size());
    for (const auto &kv : producerCount_) {
        // Skip the closed producer, we are getting that in the related node info.
        if (kv.second.count_ == 0) {
            continue;
        }
        // Create a temporary ProducerMetaPb
        ProducerMetaPb producerMeta;

        // Add stream name, Address, and producer count to it (which will be 0 or 1)
        producerMeta.set_stream_name(streamName_);
        HostPort workerHostPort;
        RETURN_IF_NOT_OK(workerHostPort.ParseString(kv.first));
        producerMeta.mutable_worker_address()->set_host(workerHostPort.Host());
        producerMeta.mutable_worker_address()->set_port(workerHostPort.Port());
        producerMeta.set_producer_count(kv.second.count_);

        producerList.emplace_back(producerMeta);
    }
    return Status::OK();
}

std::vector<ConsumerMetaPb> TopologyManager::GetAllConsumer() const
{
    std::vector<ConsumerMetaPb> consumerList;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    consumerList.reserve(consumerTopo_.size());
    for (const auto &kv : consumerTopo_) {
        consumerList.emplace_back(kv.second);
    }
    return consumerList;
}

bool TopologyManager::GetStreamStatus() const
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    return isDeleting_;
}

Status TopologyManager::SetDeletingStatus()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!isDeleting_, StatusCode::K_IO_ERROR,
                             FormatString("Stream:<%s>, State:<Repeat deleting>", streamName_));
    isDeleting_ = true;
    return Status::OK();
}

void TopologyManager::UnsetDeletingStatus()
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    isDeleting_ = false;
}

Status TopologyManager::GlobalUniqueCheck(const ConsumerMetaPb &consumerMeta)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    bool isUnique = true;
    const auto &config = consumerMeta.sub_config();
    RETURN_OK_IF_TRUE(config.subscription_type() != SubscriptionTypePb::STREAM_PB);
    RETURN_OK_IF_TRUE(consumerTopo_.empty());

    const std::string &subName = config.subscription_name();
    for (const auto &kv : consumerTopo_) {
        if (subName == kv.second.sub_config().subscription_name()) {
            isUnique = false;
            break;
        }
    }
    CHECK_FAIL_RETURN_STATUS(isUnique, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Stream:<%s>, SubscriptionName:<%s> is not unique in global scope",
                                          streamName_, consumerMeta.sub_config().subscription_name()));
    return Status::OK();
}

Status TopologyManager::CheckNewConsumer(const ConsumerMetaPb &consumerMeta)
{
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    auto iter = consumerTopo_.find(consumerMeta.consumer_id());
    if (iter != consumerTopo_.end()) {
        auto retCode = iter->second.client_id() == consumerMeta.client_id() ? StatusCode::K_DUPLICATED
                                                                            : StatusCode::K_RUNTIME_ERROR;
        RETURN_STATUS(retCode, FormatString("The consumer [%s] already exists in stream <%s>",
                                            consumerMeta.consumer_id(), streamName_));
    }
    return GlobalUniqueCheck(consumerMeta);
}

bool TopologyManager::CheckIfAllPubSubHaveClosed()
{
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    return consumerTopo_.empty() && (currentProducerCount_ == 0);
}

namespace {
template <class T>
std::string IntoString(const T &val);

template <>
std::string IntoString(const ConsumerMetaPb &val)
{
    ConsumerMetaPb meta = val;
    meta.clear_consumer_id();
    meta.clear_stream_name();
    return "<" + meta.ShortDebugString() + ">";
}

template <>
std::string IntoString(const uint32_t &val)
{
    return std::to_string(val);
}

template <>
std::string IntoString(const TopologyManager::ProducerCount &val)
{
    return std::to_string(val.count_);
}

template <typename T>
void GetTopoInformation(std::ostream &os, const std::unordered_map<std::string, T> &map)
{
    const size_t maxCount = 64;
    size_t count = 0;
    os << "{";
    for (const auto &kv : map) {
        if (count >= maxCount) {
            break;
        }
        os << kv.first << ":" << IntoString(kv.second);
        count++;
    }
    if (count > maxCount) {
        os << "...(" << (count - maxCount) << ")";
    }
    os << "}";
}
}  // namespace

std::ostream &operator<<(std::ostream &os, const TopologyManager &obj)
{
    std::shared_lock<std::shared_timed_mutex> lock(obj.mutex_);
    os << "{producerCount:";
    GetTopoInformation(os, obj.producerCount_);
    os << ", consumers:";
    GetTopoInformation(os, obj.consumerTopo_);
    os << ", consumerCount:";
    GetTopoInformation(os, obj.consumerCount_);
    os << "}";
    return os;
}
}  // namespace master
}  // namespace datasystem
