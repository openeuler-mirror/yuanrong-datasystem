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
 * Description: Module responsible for managing the stream cache metadata on the master.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_SC_METADATA_MANAGER_H
#define DATASYSTEM_MASTER_STREAM_CACHE_SC_METADATA_MANAGER_H

#include <shared_mutex>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/metadata_redirect_helper.h"
#include "datasystem/master/stream_cache/sc_notify_worker_manager.h"
#include "datasystem/master/stream_cache/stream_metadata.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace master {
using TbbMetaHashmap = tbb::concurrent_hash_map<std::string, std::shared_ptr<StreamMetadata>>;

class SCMetadataManager : public MetadataRedirectHelper {
public:
    /**
     * @brief Construct a new SCMetadataManager instance.
     * @param[in] masterHostPort The master address.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] rpcSessionManager Master to Worker session manager.
     * @param[in] cm The etcd cluster manager instance.
     * @param[in] rocksStore The rocks store instance.
     * @param[in] dbName The db name.
     */
    SCMetadataManager(const HostPort &masterHostPort, std::shared_ptr<AkSkManager> akSkManager,
                      std::shared_ptr<RpcSessionManager> rpcSessionManager, EtcdClusterManager *cm,
                      RocksStore *rocksStore, const std::string &dbName);

    /**
     * @brief Shutdown the sc metadata manager module.
     */
    void Shutdown() override;

    /**
     * @brief WorkerOCServer uses the SetClusterManager method to directly pass the std::unique_ptr address of
     * etcdCM_ to SCMetadataManager. If the etcdCM_ destructor releases the memory, a core dump occurs when the
     * SCMetadataManager object that holds the pointer address operates the address. Therefore, the
     * EtcdClusterManager needs to notify the SCMetadataManager before exiting.
     */
    void SetClusterManagerToNullptr();

    /**
     * @brief Initialize SCMetadataManager.
     * @return Status of the call.
     */
    Status Init();

    ~SCMetadataManager();

    /**
     * @brief Check if the metadata is available for given stream name.
     * @param[in] streamName The stream name.
     * @return Returns true if meta is found in meta table.
     */
    bool MetaIsFound(const std::string &streamName) override;

    /**
     * @brief Get streams that meet the meta conditions
     * @param[in] matchFunc The conditions to meet.
     * @param[out] streamNames Streams that meet the meta conditions.
     * @param[in] exitEarly Whether to exit cycle early.
     */
    void GetMetasMatch(std::function<bool(const std::string &)> &&matchFunc, std::vector<std::string> &streamNames,
                       bool *exitEarly = nullptr);

    /**
     * @brief Saves metadata migrated from other masters.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the result.
     */
    Status SaveMigrationMetadata(const MigrateSCMetadataReqPb &req, MigrateSCMetadataRspPb &rsp);

    /**
     * @brief Save migration data.
     * @param[in] streamMeta The stream meta.
     * @param[out] rsp The rpc response protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return status of the call.
     */
    Status SaveMigrationData(const MetaForSCMigrationPb &streamMeta, Status &status, MigrateSCMetadataRspPb &rsp);

    /**
     * @brief Fill in the data to be migrated.
     * @param[in] streamName The stream name to be midrated.
     * @param[out] meta Data to be sent.
     * @return Status of the result.
     */
    Status FillMetadataForMigration(const std::string &streamName, MetaForSCMigrationPb *meta);

    /**
     * @brief Handling data migration failed.
     * @param[in] streamMeta Metadata to be migrated
     */
    void HandleMetaDataMigrationFailed(const MetaForSCMigrationPb &streamMeta);

    /**
     * @brief Delete the migrated metadata.
     * @param[in] streamName The stream name to be deleted.
     */
    void HandleMetaDataMigrationSuccess(const std::string &streamName);

    /**
     * @brief Create a producer, i.e., register a publisher to a stream. Similar to worker::CreateProducer.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateProducer(const CreateProducerReqPb &req, CreateProducerRspPb &rsp);

    /**
     * @brief Close a producer, force flushing and page seal, unregister a publisher to a stream.
     * Similar to worker::CloseProducer.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducer(const CloseProducerReqPb &req, CloseProducerRspPb &rsp);

    /**
     * @brief Subscribe to a stream, using a subscription name, i.e., register a consumer to a subscription.
     * Similar to worker::Subscribe.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status Subscribe(const SubscribeReqPb &req, SubscribeRspPb &rsp);

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * Similar to worker::CloseConsumer.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumer(const CloseConsumerReqPb &req, CloseConsumerRspPb &rsp);

    /**
     * @brief Delete a stream.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status DeleteStream(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp);

    /**
     * @brief Query global producers for a stream.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalProducersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp);

    /**
     * @brief Query global consumers for a stream.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalConsumersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp);

    /**
     * @brief Clear worker metadata.
     * @param[in] workerAddr The worker address.
     * @param[in] forceClose If the node had a crash or regular close
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearWorkerMetadata(const HostPort &workerAddr, const bool forceClose = false);

    /**
     * @brief Check metadata with worker.
     * @param[in] workerAddrs The worker address.
     * @param[in] hashRanges The optional hash ranges, for passive scale down recovery purposes.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckMetadata(const std::vector<HostPort> &workerAddrs, const worker::HashRange &hashRanges = {});

    /**
     * @brief Start check metadata with worker.
     * @param[in] workerAddr The worker address.
     */
    void StartCheckMetadata(const HostPort &workerAddr);

    /**
     * @brief Start clear worker metadata.
     * @param[in] workerAddr The worker address.
     */
    void StartClearWorkerMetadata(const HostPort &workerAddr);

    /**
     * @brief Get rocksdb name.
     * @return std::string the rocksdb name.
     */
    std::string GetDbName()
    {
        return dbName_;
    }

    /**
     * @brief Check meta table is empty;
     * @return meta table is empty or not
     */
    bool CheckMetaTableEmpty();

    /**
     * @brief Check if there is SCmeta in the worker node.
     * @param[in] workerAddr The worker address n.
     * @return true if exist, false if not exist.
     */
    bool CheckSCMetaExist(const HostPort &workerAddr);

private:
    friend SCNotifyWorkerManager;

    /**
     * @brief Check metadata with worker.
     * @param[in] workerAddrs The worker address.
     * @param[in] hashRanges The optional hash ranges, for passive scale down recovery purposes.
     * @param[in/out] streamsMayNeedAutoDelete Streams that may need to be automatically deleted.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckMetadataImpl(const HostPort &workerAddr, const worker::HashRange &hashRanges,
                             std::unordered_set<std::string> &streamsMayNeedAutoDelete);

    /**
     * @brief Create stream metadata if not exist and add to the hashmap.
     * @note If already exists or not the first to create, we will find the slot and get the accessor lock.
     * @param[in] streamName Name of stream.
     * @param[out] accessor Lock of stream entry.
     */
    Status CreateOrGetStreamMetadata(const std::string &streamName, TbbMetaHashmap::accessor &accessor);

    /**
     * @brief Create stream metadata if not exist and add to the hashmap.
     * @param[in] streamName Name of stream.
     */
    Status CreateStreamMetadata(const std::string &streamName);

    /**
     * @brief Get stream metadata without lock, accessor can either be TbbMetaHashmap::accessor or
     * TbbMetaHashmap::const_accessor.
     * @param[in] streamName Name of stream.
     * @param[out] accessor Lock of stream entry.
     */
    template <typename T>
    Status GetStreamMetadataNoLock(const std::string &streamName, T &accessor)
    {
        if (!streamMetaManagerDict_.find(accessor, streamName)) {
            RETURN_STATUS(K_NOT_FOUND, FormatString("stream [%s] not found", streamName));
        }
        return Status::OK();
    }

    /**
     * @brief Get stream metadata, accessor can either be TbbMetaHashmap::accessor or TbbMetaHashmap::const_accessor.
     * @param[in] streamName Name of stream.
     * @param[out] accessor Lock of stream entry.
     */
    template <typename T>
    Status GetStreamMetadata(const std::string &streamName, T &accessor)
    {
        ReadLockHelper rlocker(LOCK_ARGS_MSG(metaDictMutex_, streamName));
        return GetStreamMetadataNoLock(streamName, accessor);
    }

    /**
     * @brief Get the stream metadata object by worker address.
     * @param[in] workerAddr The worker address.
     * @param[in] bool Whether to clear metadata.
     * @return The stream metadata list.
     */
    std::vector<std::shared_ptr<StreamMetadata>> GetStreamMetaByWorkerAddr(const HostPort &workerAddr,
                                                                           bool clearMeta = false);

    /**
     * @brief Get all stream metadata object.
     * @param[out] streamMetas The stream metadata list.
     * @return The stream metadata list.
     */
    std::vector<std::shared_ptr<StreamMetadata>> GetAllStreamMeta();

    /**
     * @brief If the input rc is an error case, then set the response rc only if this is the first error.
     * Then append the producer to the response pb for list of failed producers.  No-op if the input rc is ok,
     * but track the successful producer in the response success list.
     * @param[in/out] firstError Indicator if this is the first time an error was hit.
     * @param[in] rc The rc to process
     * @param[in] info The current producer that was being closed
     * @param[out] rsp The CloseProducer response proto to adjust if there was an error.
     * @return true if an error was handled. false if there was no error in the input rc.
     */
    bool HandleCloseProducerError(bool &firstError, const Status &rc, const ProducerInfoPb &info,
                                  CloseProducerRspPb &rsp);

    /**
     * @brief Get the log prefix.
     * @return The log prefix.
     */
    std::string LogPrefix() const;

    /**
     * @brief Load stream meta from rocksdb store for last runtime into memory.
     * @return Status of the call.
     */
    Status LoadMeta();

    /**
     * @brief Update metadata in master received from worker.
     * @param[in] metaRsp The vector of metadata responses received from worker
     * @param[in] workerAddr The target worker address for reconciliation
     * @param[in/out] streamsMayNeedAutoDelete Streams that may need to be automatically deleted.
     * @param[in] hashRanges The optional hash ranges, for passive scale down recovery purposes.
     * @return K_OK on success; the error code otherwise
     */
    Status UpdateMetadata(std::vector<GetStreamMetadataRspPb> &metaRsp, const HostPort &workerAddr,
                          std::unordered_set<std::string> &streamsMayNeedAutoDelete,
                          const worker::HashRange &hashRanges);

    /**
     * @brief Recover metadata of faulty worker from the other workers.
     * @param[in] workerUuids The uuids to be recovered.
     * @param[in] extraRanges The hash range of faulty worker.
     * @return Status of the result.
     */
    Status RecoverMetadataOfFaultyWorker(const std::vector<std::string> &workerUuids,
                                         const worker::HashRange &extraRanges);

    /**
     * @brief Send Delete Stream Context to a worker
     * @param[in] streamName The stream to be deleted.
     * @param[in] workerNode Worker in which stream needs to be deleted.
     * @return Status of the result.
     */
    Status SendDeleteStreamReqToWorker(const std::string &streamName, const HostPort workerNode);

    /**
     * @brief Send delete stream requests to all related workers
     * @param[in] streamName The name of the stream getting deleted.
     * @param[in] relatedWorkerSet The list of workers.
     * @return Status of the result.
     */
    Status SendDeleteStreamReq(const std::string &streamName, std::set<HostPort> &relatedWorkerSet);

    /**
     * @brief Check worker status.
     * @param[in] workerHostPort The target worker address.
     * @return Status of the call.
     */
    Status CheckWorkerStatus(const HostPort &workerHostPort);

    /**
     * @brief Verify Stream Mode.
     * @param[in] streamMode The stream mode.
     * @param[in] consumerNumAfterModify The consumer number after modify.
     * @param[in] producerNumAfterModify The producer number after modify.
     * @return Status of the call.
     */
    static Status VerifyStreamMode(StreamMode streamMode, size_t consumerNumAfterModify, size_t producerNumAfterModify);

    /**
     * @brief Check metadata with worker and retry for rpc timeout.
     * @param[in] workerAddr The worker address.
     */
    void CheckMetadataWithAsyncRetry(const HostPort &workerAddr, std::shared_ptr<Timer> timer, size_t retryTimes = 0);

    /**
     * @brief Actively trigger automatic deletion
     * @param[in] streamsMayNeedAutoDelete Streams that may need to be automatically deleted.
     */
    void TriggerAutoDelActively(const std::unordered_set<std::string> &streamsMayNeedAutoDelete);

    /**
     * @brief Now that we have complete metadata, we need to do some cleanup.
     * @param[in] streamsMayNeedAutoDelete Streams that may need to be automatically deleted.
     */
    void PostCheckMetadata(const std::unordered_set<std::string> &streamsMayNeedAutoDelete)
    {
        TriggerAutoDelActively(streamsMayNeedAutoDelete);
    }

    mutable std::shared_timed_mutex metaDictMutex_;

    HostPort masterAddress_;

    std::unique_ptr<SCNotifyWorkerManager> notifyWorkerManager_;

    // key:streamName value:StreamMetadata pointer.
    TbbMetaHashmap streamMetaManagerDict_;

    std::shared_ptr<RocksStreamMetaStore> streamMetaStore_{ nullptr };

    std::unique_ptr<ThreadPool> asyncReconciliationPool_{ nullptr };
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::shared_ptr<RpcSessionManager> rpcSessionManager_{ nullptr };
    std::shared_ptr<std::atomic_bool> exitFlag_;

    const std::string dbName_;
    const std::string eventName_;
};
}  // namespace master
}  // namespace datasystem

#endif  // DATASYSTEM_MASTER_STREAM_CACHE_SC_METADATA_MANAGER_H
