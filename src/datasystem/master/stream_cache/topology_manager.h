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
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_TOPOLOGY_MANAGER_H
#define DATASYSTEM_MASTER_STREAM_CACHE_TOPOLOGY_MANAGER_H

#include <algorithm>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <numeric>

#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/protos/worker_stream.pb.h"

namespace datasystem {
namespace master {
class TopologyManager {
public:
    explicit TopologyManager(std::string streamName);
    ~TopologyManager() = default;

    /**
     * @brief Increase a pub node for a stream.
     * @param[in] producerMeta The producer metadata.
     * @param[out] isFirstProducer Whether the target producer is the first one on current local worker node.
     * @return Status of the call.
     */
    Status PubIncreaseNode(const ProducerMetaPb &producerMeta, bool &isFirstProducer);

    /**
     * @brief The first producer processing is done for the worker.
     * @param[in] producerMeta The producer metadata.
     * @return Status of the call.
     */
    Status PubNodeFirstOrLastDone(const ProducerMetaPb &producerMeta);

    /**
     * @brief Prepare for decrease of a pub node for a stream.
     * @param[in] producerMeta The producer metadata.
     * @param[out] isLastProducer Indicate whether the producer to close is the last producer.
     * @return Status of the call.
     */
    Status PubDecreaseNodeStart(const ProducerMetaPb &producerMeta, bool &isLastProducer);

    /**
     * @brief Decrease a pub node for a stream.
     * @param[in] producerMeta The producer metadata.
     * @param[in] delWorker Delete the related worker as part of reconciliation.
     * @return Status of the call.
     */
    Status PubDecreaseNode(const ProducerMetaPb &producerMeta, const bool delWorker);

    /**
     * @brief Increase a sub node for a stream.
     * @param[in] consumerMeta The consumer meta info which will be transformed into a sub node.
     * @param[out] isFirstConsumer Whether the target consumer is the first one on current local worker node.
     * @return Status of the call.
     */
    Status SubIncreaseNode(const ConsumerMetaPb &consumerMeta, bool &isFirstConsumer);

    /**
     * @brief Decrease a sub node for a stream.
     * @param[in] consumerMeta The consumer meta info which will be transformed into a sub node.
     * @param[in] rollback Wether this is a rollback or actual close.
     * @param[in] delWorker Delete the related worker as part of reconciliation
     * @return Status of the call.
     */
    Status SubDecreaseNode(const ConsumerMetaPb &consumerMeta, bool rollback, const bool delWorker);

    /**
     * @brief Get all pub node for a stream.
     * @param[out] pubNodeSet The set of pub node address.
     * @param[in] informAll Whether to inform nodes with no producers.
     * @return Status of the call.
     */
    Status GetAllPubNode(std::set<HostPort> &pubNodeSet, bool informAll = false) const;

    /**
     * @brief Get all sub node for a stream, only in worker node layer definition.
     * @param[out] subNodeSet The set of sub node address.
     * @return Status of the call.
     */
    Status GetAllSubNode(std::set<HostPort> &subNodeSet) const;

    /**
     * @brief Get the all worker address if metadata exist.
     * @param[out] nodeSet The set of all pub sub node address.
     * @return Status of the call.
     */
    Status GetAllRelatedNode(std::set<HostPort> &nodeSet) const;

    /**
     * @brief Recover Empty Meta for a node If Needed. (Failure recovery scenarios require)
     * @param[in] nodeAddr The node where both consumer and producer are closed.
     * @return True if empty metadata was recovered, false otherwise.
     */
    bool RecoverEmptyMetaIfNeeded(const HostPort &nodeAddr);

    /**
     * @brief Get the all worker address if metadata exist.
     * @param[out] producerRelatedNodes All of the producer related nodes.
     * @param[out] consumerRelatedNodes All of the consumer related nodes.
     * @return Status of the call.
     */
    void GetAllRelatedNode(std::vector<std::string> &producerRelatedNodes,
                           std::vector<std::string> &consumerRelatedNodes) const;

    /**
     * @brief Initialize producer count and consumer count with zero to keep the related node info.
     * @param[in] producerRelatedNodes The producer related nodes.
     * @param[in] consumerRelatedNodes The consumer related nodes.
     */
    void PreparePubSubRelNodes(const std::vector<std::string> &producerRelatedNodes,
                               const std::vector<std::string> &consumerRelatedNodes);

    /**
     * @brief Get all sub node for a stream except source node.
     * @param[in] srcNode Source node that will be excluded.
     * @return The consumer metadatas for the stream.
     */
    std::vector<ConsumerMetaPb> GetAllConsumerNotFromSrc(const std::string &srcNode) const;

    /**
     * @brief Get the all consumer metadata from worker.
     * @param[in] workerAddress The worker address.
     * @return The The producer metadatas.
     */
    std::unordered_map<std::string, ConsumerMetaPb> GetAllConsumerFromWorker(const HostPort &workerAddress) const;

    /**
     * @brief Get all the producer metadata.
     * @param[out] producerList list of producers in the worker
     * @return Status of the call.
     */
    Status GetAllProducer(std::vector<ProducerMetaPb> &producerList);

    /**
     * @brief Get all the consumer metadata.
     * @return The consumer metadatas.
     */
    std::vector<ConsumerMetaPb> GetAllConsumer() const;

    /**
     * @brief Get the all producer metadata from worker.
     * @param[in] workerAddress The worker address.
     * @param[out] producerMap list of producers in the worker
     * @return Status of the call.
     */
    Status GetAllProducerFromWorker(const HostPort &workerAddress,
                                    std::unordered_map<std::string, ProducerMetaPb> &producerMap);
    /**
     * @brief Get the status of a stream.
     * @return True if target stream is deleting.
     */
    bool GetStreamStatus() const;

    /**
     * @brief Set the status of current stream as isDeleting.
     * @return Status of the call.
     */
    Status SetDeletingStatus();

    /**
     * @brief Unset the isDeleting status.
     */
    void UnsetDeletingStatus();

    /**
     * @brief Check whether the subscription name of target consumer is unique in global scope.
     * @param[in] consumerMeta Target consumer meta info.
     * @return Status of the call.
     */
    Status GlobalUniqueCheck(const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Get the producer count in worker.
     * @param[in] workerAddr The worker address.
     */
    void ClearEmptyMeta(const std::string &workerAddr)
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto producerIter = producerCount_.find(workerAddr);
        if (producerIter != producerCount_.end() && !producerIter->second.count_) {
            producerCount_.erase(workerAddr);
        }
        auto consumerIter = consumerCount_.find(workerAddr);
        if (consumerIter != consumerCount_.end() && !consumerIter->second) {
            consumerCount_.erase(workerAddr);
        }
    }

    /**
     * @brief Get the producer count in worker.
     * @param[in] workerAddr The worker address.
     * @return The producer count in worker.
     */
    size_t GetProducerCountInWorker(const std::string &workerAddr)
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = producerCount_.find(workerAddr);
        if (iter != producerCount_.end()) {
            return iter->second.count_;
        }
        return 0;
    }

    /**
     * @brief Get the consumer count in worker.
     * @param[in] workerAddr The worker address.
     * @return The producer count in worker..
     */
    size_t GetConsumerCountInWorker(const std::string &workerAddr) const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = consumerCount_.find(workerAddr);
        if (iter != consumerCount_.end()) {
            return iter->second;
        }
        return 0;
    }

    /**
     * @brief Get the producer count in this stream.
     * @return uint64_t The producer count.
     */
    uint64_t GetProducerCount() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return std::accumulate(
            producerCount_.begin(), producerCount_.end(), 0ul,
            [](uint64_t init, const std::pair<std::string, ProducerCount> &kv) { return init + kv.second.count_; });
    }

    /**
     * @brief Get the consumer count in this stream.
     * @return uint64_t The consumer count.
     */
    uint64_t GetConsumerCount() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return std::accumulate(
            consumerCount_.begin(), consumerCount_.end(), 0ul,
            [](uint64_t init, const std::pair<std::string, uint32_t> &kv) { return init + kv.second; });
    }

    /**
     * @brief Check if the worker has a producer or not.
     * @param[in] producerMeta The producer meta.
     * @return The producer exists or not.
     */
    bool ExistsProducer(const ProducerMetaPb &producerMeta) const
    {
        HostPort workerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return ((producerCount_.count(workerAddress.ToString()) > 0)
                && (producerCount_.at(workerAddress.ToString()).count_ > 0));
    }

    /**
     * @brief Check if the worker has a producer or not.
     * @param[in] workerAddress workerAddress of producer
     * @return The producer exists or not.
     */
    bool ExistsProducerUnlocked(const HostPort &workerAddress) const
    {
        return ((producerCount_.count(workerAddress.ToString()) > 0)
                && (producerCount_.at(workerAddress.ToString()).count_ > 0));
    }

    /**
     * @brief Check the consumer exists or not.
     * @param[in] consumerId The consumer id.
     * @return The consumer exists or not.
     */
    bool ExistsConsumer(const std::string &consumerId) const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return consumerTopo_.count(consumerId) > 0;
    }

    Status RestoreConsumerLifeCount(const uint32_t consumerCount)
    {
        std::lock_guard<std::shared_timed_mutex> lock(mutex_);
        consumerLifeCount_ = consumerCount;
        return Status::OK();
    }

    /**
     * @brief Check for add new consumer.
     * @param[in] consumerMeta The consumer metadata.
     * @return Status of the call.
     */
    Status CheckNewConsumer(const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Check if all the producers/consumers have closed
     * @return T/F
     */
    bool CheckIfAllPubSubHaveClosed();

    /**
     * @brief Returns consumer count over stream life time
     * @return Consumer count
     */
    uint32_t GetConsumerCountForLife();

    struct ProducerCount {
        uint32_t count_ = 0;
        bool firstProducerProcessing_ = false;
    };

private:
    friend std::ostream &operator<<(std::ostream &os, const TopologyManager &obj);

    mutable std::shared_timed_mutex mutex_;

    std::string streamName_;

    // Key: workerAddress. Value: consumer number on that worker.
    std::unordered_map<std::string, uint32_t> consumerCount_;

    // Num of consumers over life of the stream.
    uint32_t consumerLifeCount_{ 0 };

    // Key: workerAddress. Value: producer number on that worker,
    // and whether the first producer request is still processing.
    std::unordered_map<std::string, ProducerCount> producerCount_;

    // Topology for consumer. Key: consumerId. Value: ConsumerMetaPb.
    std::unordered_map<std::string, ConsumerMetaPb> consumerTopo_;
    // Measure total number of workers with atleast one producer
    // Used as a check before DeleteStreams
    uint64_t currentProducerCount_{ 0 };

    std::set<std::string> streamModeSubDict_;  // Used for record all stream mode subscription in global.

    bool isDeleting_;
};
}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_STREAM_CACHE_TOPOLOGY_MANAGER_H
