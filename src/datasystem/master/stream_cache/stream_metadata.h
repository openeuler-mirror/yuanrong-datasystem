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
 * Description: The stream metadata object.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_STREAM_METADATA_H
#define DATASYSTEM_MASTER_STREAM_CACHE_STREAM_METADATA_H

#include <algorithm>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

#include "datasystem/common/log/log.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/stream_cache/consumer_meta.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/master/stream_cache/master_worker_sc_api.h"
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/master/stream_cache/sc_notify_worker_manager.h"
#include "datasystem/master/stream_cache/store/rocks_stream_meta_store.h"
#include "datasystem/master/stream_cache/topology_manager.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics.h"

namespace datasystem {
namespace master {
using RocksStreamMetaStore = stream_cache::RocksStreamMetaStore;
class StreamMetadata : public std::enable_shared_from_this<StreamMetadata> {
public:
    StreamMetadata(std::string streamName, const StreamFields &streamFields, RocksStreamMetaStore *streamMetaStore,
                   std::shared_ptr<AkSkManager> akSkManager, std::shared_ptr<RpcSessionManager> rpcSessionManager,
                   EtcdClusterManager *etcdCM, SCNotifyWorkerManager *notifyWorkerManager);

    ~StreamMetadata();

    /**
     * @brief Increase a pub node for a stream and rollback when failed.
     * @param[in] producerMeta The producer metadata.
     * @param[in] streamFields The fields for the stream.
     * @return Status of the call.
     */
    Status PubIncreaseNode(const ProducerMetaPb &producerMeta, StreamFields &streamFields);

    /**
     * @brief Increase a pub node for a stream and rollback when failed.
     * @param[in] producerMeta The producer metadata.
     * @param[in] streamFields The fields for the stream.
     * @param[in] pubWorkerAddress The source worker address.
     * @param[in] isRecon Is this part of reconciliation process.
     * @param[in] alreadyLocked Indicate whether StreamMetadata lock is already acquired.
     * @return Status of the call.
     */
    Status PubIncreaseNodeInternal(const ProducerMetaPb &producerMeta, StreamFields &streamFields,
                                   const HostPort &pubWorkerAddress, bool isRecon, bool alreadyLocked = true);

    /**
     * @brief Pre-handling of PubIncreaseNode, including stream fields update and rocksdb update.
     * @param[in] streamFields The fields for the stream.
     * @param[in] producerMeta The producer metadata.
     * @param[in] alreadyLocked Indicate whether StreamMetadata lock is already acquired.
     * @param[out] streamFieldsVerified Indicate whether the stream fields are verified and ref count is incremented.
     * @param[out] saveToRocksdb Indicate whether saved to rocksdb is done.
     * @param[out] isFirstProducer Indicate whether it is the first producer from a worker.
     * @return Status of the call.
     */
    Status PubIncreaseNodeStart(const StreamFields &streamFields, const ProducerMetaPb &producerMeta,
                                bool alreadyLocked, bool &streamFieldsVerified, bool &saveToRocksdb,
                                bool &isFirstProducer);

    /**
     * @brief Post-handling of PubIncreaseNode, including the rollback processing if applicable.
     * @param[in] isFirstProducer Whether this is the first producer.
     * @param[in] needsRollback Whether rollback is needed.
     * @param[in] alreadyLocked Indicate whether StreamMetadata lock is already acquired.
     * @param[in] producerMeta The producer metadata.
     * @param[in] pubWorkerAddress The source worker address.
     * @param[in] streamFieldsVerified Indicate whether the stream fields are verified and ref count is incremented.
     * @param[in] notifyNodeSet The worker list already send notification success.
     * @param[in] saveToRocksdb Indicate whether saved to rocksdb is done.
     * @return Status of the call.
     */
    Status PubIncreaseNodeEnd(bool isFirstProducer, bool needsRollback, bool alreadyLocked,
                              const ProducerMetaPb &producerMeta, const HostPort &pubWorkerAddress,
                              bool streamFieldsVerified, const std::vector<HostPort> &notifyNodeSet,
                              bool saveToRocksdb);

    /**
     * @brief Increase a pub node for a stream.
     * @param[in] pubWorkerAddress The source worker address.
     * @param[out] notifyNodeSet The worker list already send notification success.
     * @param[in] isRecon Is this part of reconciliation process.
     * @return Status of the call.
     */
    Status PubIncreaseNodeImpl(const HostPort &pubWorkerAddress, std::vector<HostPort> &notifyNodeSet, bool isRecon);

    /**
     * @brief Decrease a pub node for a stream.
     * @param[in] producerMeta The producer metadata.
     * @param[in] forceClose If the pub node had a crash or regular close
     * @return Status of the call.
     */
    Status PubDecreaseNode(const ProducerMetaPb &producerMeta, bool forceClose);

    /**
     * @brief Prepare for decrease of a pub node for a stream.
     * @param[in] producerMeta The producer metadata.
     * @param[out] isLastProducer Indicate whether the producer to close is the last producer.
     * @return Status of the call.
     */
    Status PubDecreaseNodeStart(const ProducerMetaPb &producerMeta, bool &isLastProducer);

    /**
     * @brief Increase a sub node for a stream and rollback when failed.
     * @param[in] consumerMeta The consumer meta info which will be transformed into a sub node.
     * @param[in] isRecon Is this part of reconciliation (or migration) process.
     * @return Status of the call.
     */
    Status SubIncreaseNode(const ConsumerMetaPb &consumerMeta, bool isRecon = false);

    /**
     * @brief Increase a sub node for a stream and rollback when failed.
     * @param[in] consumerMeta The consumer meta info which will be transformed into a sub node.
     * @param[in] subWorkerAddress The address of the sub worker.
     * @param[in] isRecon Is this part of reconciliation process.
     * @return Status of the call.
     */
    Status SubIncreaseNodeUnlocked(const ConsumerMetaPb &consumerMeta, const HostPort &subWorkerAddress,
                                   bool isRecon = false);

    /**
     * @brief Increase a sub node for a stream.
     * @param[in] consumerMeta The consumer metadata.
     * @param[in] subWorkerAddress The source worker address.
     * @param[out] saveToRocksdb Indicate whether save to rocksdb.
     * @param[out] sendToSrcNode Indicate whether send rpc to source node.
     * @param[out] notifyNodeSet The worker list already send notification success.
     * @param[in] isRecon Is this part of reconciliation process.
     * @return Status of the call.
     */
    Status SubIncreaseNodeImpl(const ConsumerMetaPb &consumerMeta, const HostPort &subWorkerAddress,
                               bool &saveToRocksdb, bool &sendToSrcNode, std::vector<HostPort> &notifyNodeSet,
                               bool isRecon);
    /**
     * @brief Decrease a sub node for a stream.
     * @param[in] consumerMeta The consumer meta info which will be transformed into a sub node.
     * @return Status of the call.
     */
    Status SubDecreaseNode(const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Start Delete stream request.
     * @param[in] srcNode The source worker address of delete request.
     * @param[out] relatedWorkerSet Set of nodes to notify
     * @return Status of the call.
     */
    Status DeleteStreamStart(const HostPort &srcNode, std::set<HostPort> &relatedWorkerSet);

    /**
     * @brief Delete Stream from the RocksDb.
     * @return Status of the call.
     */
    Status DeleteStreamEnd();

    /**
     * @brief Undo delete state
     * @param[in] decrementRef Whether to decrement reference count or not when undoing delete stream
     */
    void UndoDeleteStream(bool decrementRef);

    /**
     * @brief Recover pub worker node meta on master.
     * @param[in] producerMetaPb The producer metadata.
     * @return K_OK on success; the error code otherwise.
     */
    Status RecoveryPubMeta(const ProducerMetaPb &producerMetaPb);

    /**
     * @brief Recover sub consumer node meta on master.
     * @param consumerMeta The consumer metadata information.
     * @return K_OK on success; the error code otherwise.
     */
    Status RecoverySubMeta(const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Check if the pub/sub exists in the worker node.
     * @param[in] workerAddr The worker address.
     * @return true if exists.
     */
    bool CheckWorkerExistsPubSub(const std::string &workerAddr) const
    {
        return topoManager_->GetProducerCountInWorker(workerAddr) > 0
               || topoManager_->GetConsumerCountInWorker(workerAddr) > 0;
    }

    /**
     * @brief Clear the metadata with empty producer or consumer.
     * @param[in] workerAddr The worker address.
     */
    void ClearEmptyMeta(const std::string &workerAddr) const
    {
        topoManager_->ClearEmptyMeta(workerAddr);
    }

    /**
     * @brief Get the all worker address if exists metadata.
     * @param[in] nodeSet The worker address.
     * @return Status of the call.
     */
    Status GetAllWorkerAddress(std::set<HostPort> &nodeSet) const
    {
        return topoManager_->GetAllRelatedNode(nodeSet);
    }

    /**
     * @brief Get the stream name.
     * @return Stream name.
     */
    const std::string &GetStreamName() const
    {
        return streamName_;
    }

    /**
     * @brief Clear worker metadata.
     * @param[in] workerAddr The worker address.
     * @param[in] forceClose If the pub node had a crash or regular close
     * @param[in] delWorker Delete worker from relatedNodes
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearWorkerMetadata(const HostPort &workerAddr, bool forceClose, bool delWorker = true);

    /**
     * @brief Check metadata with worker.
     * @param[in] meta Received metadata response for the stream from worker.
     * @param[in] workerAddr The worker address.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckMetadata(const GetStreamMetadataRspPb &meta, const HostPort &workerAddr);

    /**
     * @brief Get the producer count in this stream.
     * @return size_t The producer count.
     */
    size_t GetProducerCount() const
    {
        return topoManager_->GetProducerCount();
    }

    /**
     * @brief Get the consumer count in this stream.
     * @return size_t The consumer count.
     */
    size_t GetConsumerCount() const
    {
        return topoManager_->GetConsumerCount();
    }

    /**
     * @brief Get stream metrics
     * @return stream metrics
     */
    auto GetSCStreamMetrics()
    {
        return scStreamMetrics_;
    }

    /**
     * @brief Restores consumer count for stream lifetime from RocksDb.
     * @return K_OK on success; the error code otherwise.
     */
    Status RestoreConsumerLifeCount(const uint32_t consumerCount) const
    {
        LOG(INFO) << "[RetainData] Number of consumers for stream restored to " << consumerCount
                  << " for stream: " << streamName_;
        return topoManager_->RestoreConsumerLifeCount(consumerCount);
    }

    /**
     * @brief Restores consumer count for stream lifetime from RocksDb.
     * @return the consumer life count.
     */
    uint32_t GetConsumerLifeCount() const
    {
        return topoManager_->GetConsumerCountForLife();
    }

    /**
     * @brief Verify the max stream size and the page size.
     * @param streamFields The stream fields to check
     * @return K_OK on success; the error code otherwise.
     */
    Status VerifyStreamFields(const StreamFields &streamFields) const
    {
        CHECK_FAIL_RETURN_STATUS(
            streamFields_.Empty() || (streamFields_ == streamFields), K_INVALID,
            FormatString("[%s] Changing stream fields [max stream size, page size, auto cleanup, retain for num "
                         "consumers, encrypt stream, reserve size] not supported:"
                         "Current: [%zu, %zu, %s, %zu, %s, %zu] Invalid: [%zu, %zu, %s, %zu, %s, %zu]",
                         LogPrefix(), streamFields_.maxStreamSize_, streamFields_.pageSize_,
                         (streamFields_.autoCleanup_ ? "true" : "false"), streamFields_.retainForNumConsumers_,
                         streamFields_.encryptStream_ ? "true" : "false", streamFields_.reserveSize_,
                         streamFields.maxStreamSize_, streamFields.pageSize_,
                         (streamFields.autoCleanup_ ? "true" : "false"), streamFields.retainForNumConsumers_,
                         streamFields.encryptStream_ ? "true" : "false", streamFields.reserveSize_));
        return Status::OK();
    }

    /**
     * @brief Get the stream fields.
     * @return Stream fields.
     */
    const StreamFields &GetStreamFields() const
    {
        return streamFields_;
    }

    /**
     * @brief Get the producer and consumer metadata and related nodes.
     * @return All the producer and consumer metadata regarding this stream.
     */
    void GetAllProducerConsumer(std::vector<ProducerMetaPb> &masterProducers,
                                std::vector<ConsumerMetaPb> &masterConsumers,
                                std::vector<std::string> &producerRelatedNodes,
                                std::vector<std::string> &consumerRelatedNodes);

    /**
     * @brief Initialize producer count and consumer count with zero to keep the related node info.
     * @param[in] producerRelatedNodes The producer related nodes.
     * @param[in] consumerRelatedNodes The consumer related nodes.
     */
    void PreparePubSubRelNodes(const std::vector<std::string> &producerRelatedNodes,
                               const std::vector<std::string> &consumerRelatedNodes);

    /**
     * @brief Clean up stream from rocks db store
     * @param[in] streamName The target stream name.
     * @return All the producer and consumer metadata regarding this stream.
     */
    Status CleanUpStreamPersistent(const std::string &streamName);

    /**
     * @brief Checks if we need to retain data in workers, and also update the state.
     * @return The enum value result.
     */
    RetainDataState::State CheckNUpdateNeedRetainData();

    /**
     * @brief Checks if we need to retain data in workers,
     * returns whether the state changes and caller can choose to not update the state yet.
     * @param[out] retainStateChange Whether there is a state change.
     * @param[in] update Whether to update value the upfront, default to false.
     * @return The enum value result. If update is false, it is the current state.
     */
    RetainDataState::State CheckNeedRetainData(bool &retainStateChange, const bool update = false);

    /**
     * @brief Checks if all consumers are closed.
     * @param[in] workerAdress The worker need to be checked.
     * @return T/F
     */
    bool IsAllConsumerClosed(const std::string &workerAddress)
    {
        return topoManager_->GetConsumerCountInWorker(workerAddress) == 0;
    }

    /**
     * @brief Initializes the stream metrics with master stream metrics.
     * @return Status of the call.
     */
    Status InitStreamMetrics();

    /**
     * @brief Updates the stream metrics with master stream metrics.
     */
    void UpdateStreamMetrics();

    /**
     * @brief Auto stream clean up.
     * @param[in] srcHost Last host with the last consumer/producer closed.
     * @return Status of the call.
     */
    Status AutoCleanupIfNeeded(const HostPort &srcHost);

private:
    /**
     * @brief Updates the stream fields.
     * @param[in] streamFields New stream fields to update on.
     * @return Status of the call.
     */
    Status UpdateStreamFields(const StreamFields &streamFields);

    /**
     * @brief Notify all related nodes to stop retaining data.
     * @param[in] subWorkerAddress Target source node.
     * @param[in] retainStateChange Node set which needs to be notified
     * @return Status of the call.
     */
    Status NotifyStopRetainData(const HostPort &subWorkerAddress, bool retainStateChange);

    /**
     * @brief Remove source node from node set.
     * @param[in] srcWorkerAddress Target source node.
     * @param[out] nodeSet Node set which has been modified.
     * @return Status of the call.
     */
    static Status RemoveSourceWorker(const HostPort &srcWorkerAddress, std::set<HostPort> &nodeSet);

    /**
     * @brief Get log prefix.
     * @return The log prefix.
     */
    std::string LogPrefix() const;

    /**
     * @brief Clear metadata of pub nodes and sub nodes .
     * @param[in] workerAddr Target source node.
     * @param[in] producerMap The  producers that should be delete.
     * @param[in] consumerMap The consumers that should be delete.
     * @param[in] forceClose If the node had a crash or regular close
     * @param[in] delWorker Delete worker from relatedNodes
     * @return Status of the call.
     */
    Status ClearPubSubMetaData(const HostPort &workerAddr,
                               const std::unordered_map<std::string, ProducerMetaPb> &producerMap,
                               const std::unordered_map<std::string, ConsumerMetaPb> &consumerMap, bool forceClose,
                               bool delWorker);

    /**
     * @brief Add async clear notification to worker.
     * @param[in] workerAddr The worker address.
     * @param[in] pubNodeDelete Notify worker to clear pub node.
     * @param[in] consumerMap The consumers list.
     * @param[in] forceClose If the node had a crash or regular close
     * @return Status of the call.
     */
    Status AddAsyncClearNotification(const HostPort &workerAddr, bool pubNodeDelete,
                                     const std::unordered_map<std::string, ConsumerMetaPb> &consumerMap,
                                     bool forceClose);

    /**
     * @brief Auto stream clean up.
     * @param[in] srcHost Last host with the last consumer/producer closed.
     * @return Status of the call.
     */
    Status AutoCleanupIfNeededNotLocked(const HostPort &srcHost);

    /**
     * @brief Check worker status.
     * @param[in] workerHostPort The target worker address.
     * @return Status of the call.
     */
    Status CheckWorkerStatus(const HostPort &workerHostPort);

    /**
     * @brief Clear all remote pub node for target stream on src node.
     * @param[in] masterWorkerApi The api of src node.
     * @param[in] subWorkerAddress The address of src node.
     * @return Status of the call.
     */
    Status ProcessClearAllRemotePub(const std::shared_ptr<MasterWorkerSCApi> &masterWorkerApi,
                                    const HostPort &subWorkerAddress);

    /**
     * @brief Verify/Update the stream fields.
     * @param[in] streamFields New stream fields to update on.
     * @return Status of the call.
     */
    Status VerifyAndUpdateStreamFields(const StreamFields &streamFields)
    {
        RETURN_IF_NOT_OK(VerifyStreamFields(streamFields));
        if (streamFields_ != streamFields) {
            RETURN_IF_NOT_OK(UpdateStreamFields(streamFields));
        }
        return Status::OK();
    }

    std::string streamName_;
    StreamFields streamFields_;
    // This reference count is for rollback purposes.
    // Rollback on the stream fields will proceed only if ref count is 1.
    uint32_t streamFieldsRefcount_ = { 0 };
    mutable std::shared_timed_mutex mutex_;  // To lock all the process
    int deleterRefCount_ = 0;

    std::unique_ptr<HostPort> masterAddress_{ nullptr };
    // Key: streamName, Value: TopologyManager
    std::unique_ptr<TopologyManager> topoManager_;
    RocksStreamMetaStore *streamStore_;
    bool alive_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::shared_ptr<RpcSessionManager> rpcSessionManager_{ nullptr };
    RetainDataState retainData_;
    EtcdClusterManager *etcdCM_{ nullptr };
    SCNotifyWorkerManager *notifyWorkerManager_{ nullptr };
    std::shared_ptr<SCStreamMetrics> scStreamMetrics_{ nullptr };
};

/**
 * @brief Compare the metadata and erase the metadata if match.
 * @tparam Meta The consumer or producer metadata type.
 * @tparam F The function type.
 * @param[in/out] workerMetas The metadata from worker.
 * @param[in/out] masterMetas The metadata from master.
 * @param f The function to get the id from metadata.
 */
template <typename Meta, typename F>
void CompareAndErase(std::vector<Meta> &workerMetas, std::unordered_map<std::string, Meta> &masterMetas, F &&f)
{
    for (auto iterWorker = workerMetas.begin(); iterWorker != workerMetas.end();) {
        auto iterMaster = masterMetas.find(f(*iterWorker));
        if (iterMaster != masterMetas.end()) {
            masterMetas.erase(iterMaster);
            iterWorker = workerMetas.erase(iterWorker);
        } else {
            ++iterWorker;
        }
    }
}

/**
 * @brief Convert GetStreamMetadataRspPb to StreamFields.
 * @param[in] pb GetStreamMetadataRspPb.
 * @return StreamFields.
 */
StreamFields ConvertGetStreamMetadataRspPb2StreamFields(const GetStreamMetadataRspPb &pb);
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_STREAM_CACHE_STREAM_METADATA_H
