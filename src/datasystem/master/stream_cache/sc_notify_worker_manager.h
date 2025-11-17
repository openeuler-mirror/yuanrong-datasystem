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
 * Description: Managing notifications sent to workers.
 */
#ifndef DATASYSTEM_MASTER_STREAM_CACHE_SC_NOTIFY_WORKER_MANAGER_H
#define DATASYSTEM_MASTER_STREAM_CACHE_SC_NOTIFY_WORKER_MANAGER_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/master/stream_cache/store/rocks_stream_meta_store.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace master {
static constexpr int WAIT_TIME_BETWEEN_DELSTREAM_MS = 10000;  // waiting time between retries.
struct PendingNotification {
    PendingNotification(std::string streamName, std::string workerAddr)
        : streamName(std::move(streamName)), workerAddr(std::move(workerAddr))
    {
    }
    ~PendingNotification() = default;

    /**
     * @brief Generate the notification request.
     * @param[out] req The notification request.
     */
    void ConstructRequest(UpdateTopoNotificationReq &req)
    {
        req.set_stream_name(streamName);
        for (const auto &kv : pubs) {
            *req.add_pubs() = kv.second;
        }
        for (const auto &kv : subs) {
            *req.add_subs() = kv.second;
        }
        req.set_retain_data(retainData);
    }

    bool Empty()
    {
        return pubs.empty() && subs.empty() && retainData == RetainDataState::State::INIT;
    }

    std::string streamName;
    // the notification send to this worker.
    std::string workerAddr;
    // notification to stop retaining data
    RetainDataState::State retainData{ RetainDataState::State::INIT };
    // worker address -> NotifyPubPb
    std::unordered_map<std::string, NotifyPubPb> pubs;
    // consumer id -> ConsumerMetaPb
    std::unordered_map<std::string, NotifyConsumerPb> subs;
};

// worker address -> stream name ->  PendingNotification.
using TbbNotifyWorkerMap =
    tbb::concurrent_hash_map<std::string, std::unordered_map<std::string, std::shared_ptr<PendingNotification>>>;
using RocksStreamMetaStore = stream_cache::RocksStreamMetaStore;

class SCMetadataManager;
class SCNotifyWorkerManager {
public:
    /**
     * @brief Construct a new SCNotifyWorkerManager instance.
     * @param[in] streamMetaStore The stream rocksdb store object.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] rpcSessionManager Master to Worker session manager.
     * @param[in] cm The etcd cluster manager instance.
     * @param[in] scMetadataManager The sc metadata manager instance.
     */
    SCNotifyWorkerManager(std::shared_ptr<RocksStreamMetaStore> streamMetaStore,
                          std::shared_ptr<AkSkManager> akSkManager,
                          std::shared_ptr<RpcSessionManager> rpcSessionManager, EtcdClusterManager *cm,
                          SCMetadataManager *scMetadataManager);

    ~SCNotifyWorkerManager();

    /**
     * @brief Initialization.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief New pub node notification.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] streamName The stream name.
     * @param[in] streamFields The stream fields.
     * @param[in] srcWorkerAddr The source worker address.
     * @return Status of the call.
     */
    Status NotifyNewPubNode(const HostPort &workerAddr, const std::string &streamName, const StreamFields &streamFields,
                            const HostPort &srcWorkerAddr);

    /**
     * @brief Del pub node notification.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] streamName The stream name.
     * @param[in] srcWorkerAddr The source worker address.
     * @param[in] forceClose If the pub node had a crash or regular close
     * @return Status of the call.
     */
    Status NotifyDelPubNode(const HostPort &workerAddr, const std::string &streamName, const HostPort &srcWorkerAddr,
                            bool forceClose);

    /**
     * @brief New consumer notification.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] consumerMeta The consumer metadata.
     * @param[in] retainData Wether to retain data or not
     * @return Status of the call.
     */
    Status NotifyNewConsumer(const HostPort &workerAddr, const ConsumerMetaPb &consumerMeta,
                             const RetainDataState::State retainData = RetainDataState::State::INIT);

    /**
     * @brief Del consumer notification.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] consumerMeta The consumer metadata.
     * @return Status of the call.
     */
    Status NotifyDelConsumer(const HostPort &workerAddr, const ConsumerMetaPb &consumerMeta);

    /**
     * @brief Add the async pub change notification.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] pub The producer change notification.
     * @param[in] needPersist Indicate whether need persist to rocksdb.
     * @return Status of the call.
     */
    Status AddAsyncPubNotification(const std::string &workerAddr, const NotifyPubPb &pub, bool needPersist = true);

    /**
     * @brief Add the async consumer change notification.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] sub The consumer change notification.
     * @param[in] needPersist Indicate whether need persist to rocksdb.
     * @return Status of the call.
     */
    Status AddAsyncSubNotification(const std::string &workerAddr, const NotifyConsumerPb &sub, bool needPersist = true);

    /**
     * @brief Clear all pending notification send to the specific worker.
     * @param[in] workerAddr The worker address.
     * @return Status of the call.
     */
    Status ClearPendingNotification(const std::string &workerAddr);

    /**
     * @brief Check whether the stream exists pending notification.
     * @param[in] streamName The stream name.
     * @return True if the stream exists pending notification.
     */
    bool ExistsPendingNotification(const std::string &streamName);

    /**
     * @brief Shutdown the sc notify manager module.
     */
    void Shutdown();

    /**
     * @brief Add an async notice to drop the stream when there are no more consumer/producer globally
     * @param streamName
     * @return Status of the call.
     */
    Status AddAsyncDeleteNotification(const std::string &streamName);

    /**
     * @brief Add an async notice to stop retaining data at producers
     * @param[in] workerAddr Producer worker address
     * @param[in] streamName Name of the stream
     * @return Status of the call
     */
    Status AddAsyncStopDataRetentionNotification(const HostPort &workerAddr, const std::string &streamName);

    /**
     * @brief Get the async notifications related to a stream name.
     * @param[in] streamName Name of the stream.
     * @param[out] notifications The async notifications.
     * @return Status of the call.
     */
    Status GetPendingNotificationByStreamName(const std::string &streamName,
                                              std::vector<AsyncNotificationPb> &notifications);

    /**
     * @brief Add async notifications for a stream.
     * @param[in] streamFields The stream fields.
     * @param[in] streamName Name of the stream.
     * @param[in] streamMeta The stream migration meta, containing the notifications.
     * @return Status of the call.
     */
    Status AddAsyncNotifications(const StreamFields &streamFields, const std::string &streamName,
                                 const MetaForSCMigrationPb &streamMeta);

    /**
     * @brief Remove the async notifications related to a stream name.
     * @param[in] streamName Name of the stream.
     * @return Status of the call.
     */
    Status RemovePendingNotificationByStreamName(const std::string &streamName);

private:
    /**
     * @brief Notification sending process.
     */
    Status ProcessAsyncNotify();

    /**
     * @brief Delete stream sending process.
     */
    Status ProcessDeleteStreams();

    /**
     * @brief Check worker status.
     * @param[in] workerAddr The target worker address.
     * @return Status of the call.
     */
    Status CheckWorkerStatus(const std::string &workerAddr);

    /**
     * @brief if the target worker is abnormal will add an async sending task otherwise it will be sent directly
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] streamName The stream name.
     * @param[in] streamFields The stream fields
     * @param[in] srcWorkerAddr The source worker address.
     * @param[in] isClose True for create producer, False for close producer.
     * @param[in] forceClose If the pub node had a crash or regular close when isClose = true
     * @param[in] asyncMode Skip the sync call and always send async call.
     * @return Status of the call.
     */
    Status NotifyPubNodeImpl(const HostPort &workerAddr, const std::string &streamName,
                             const StreamFields &streamFields, const HostPort &srcWorkerAddr, bool isClose,
                             bool forceClose, bool asyncMode);

    /**
     * @brief if the target worker is abnormal will add an async sending task otherwise it will be sent directly
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] consumerMeta The consumer metadata.
     * @param[in] isClose True for create consumer, False for close consumer.
     * @param[in] retainData Tell worker to stop retaining data.
     * @param[in] asyncMode Skip the sync call and always send async call.
     * @return Status of the call.
     */
    Status NotifyConsumerImpl(const HostPort &workerAddr, const ConsumerMetaPb &consumerMeta, bool isClose,
                              RetainDataState::State retainData, bool asyncMode);

    /**
     * @brief Get the or create pending notification task.
     * @param[in] workerAddr The worker address.
     * @param[in] streamName The stream name.
     * @param[out] accessor The tbb accessor.
     * @return The pending notification object.
     */
    std::shared_ptr<PendingNotification> GetOrCreatePendingNotification(const std::string &workerAddr,
                                                                        const std::string &streamName,
                                                                        TbbNotifyWorkerMap::accessor &accessor);
    /**
     * @brief Get the pending notification task.
     * @param[in] workerAddr The worker address.
     * @param[in] streamName The stream name.
     * @param[out] accessor The tbb accessor.
     * @return The pending notification object.
     */
    std::shared_ptr<PendingNotification> GetPendingNotification(const std::string &workerAddr,
                                                                const std::string &streamName,
                                                                TbbNotifyWorkerMap::accessor &accessor);

    /**
     * @brief Send the pending notification.
     * @param[in] streamList The list of streams with pending notifications.
     * @return Status of the call.
     */
    Status SendPendingNotification(std::vector<std::pair<std::string, std::string>> &streamList);

    /**
     * @brief Remove the async notification.
     * @param[in] accessor The tbb accessor.
     * @param[in] streamName The stream name.
     * @param[in] task The pending notification task.
     * @return Status of the call.
     */
    Status RemoveAsyncNotification(TbbNotifyWorkerMap::accessor &accessor, const std::string &streamName,
                                   std::shared_ptr<PendingNotification> task);

    /**
     * @brief Send the topo change notification to worker.
     * @param[in] workerAddr The worker address which the notification send to.
     * @param[in] req The notification request.
     * @return Status of the call.
     */
    Status SendNotification(const HostPort &workerAddr, UpdateTopoNotificationReq &req);

    /**
     * @brief Recover notification from rocksdb.
     * @return Status of the call.
     */
    Status RecoverNotification();

    /**
     * @brief Handle the exists pub notification.
     * @param[in] task The pending notification task.
     * @param[in] workerAddr The worker address.
     * @param[in] isClose Is close scenario or not.
     * @param[in] needPersist Indicate whether need persist to rocksdb.
     * @param[out] exists The worker already exists notification.
     * @return Status of the call.
     */
    Status HandleExistsPubNotification(std::shared_ptr<PendingNotification> task, const std::string &workerAddr,
                                       bool isClose, bool needPersist, bool &exists);

    /**
     * @brief Handle the exists sub notification.
     * @param[in] task The pending notification task.
     * @param[in] consumerId The consumer id.
     * @param[in] isClose Is close scenario or not.
     * @param[in] needPersist Indicate whether need persist to rocksdb.
     * @param[out] exists The consumer already exists notification.
     * @return Status of the call.
     */
    Status HandleExistsSubNotification(std::shared_ptr<PendingNotification> task, const std::string &consumerId,
                                       bool isClose, bool needPersist, bool &exists);

    /**
     * @brief Async delete streams
     * @param deleteStreams
     * @return
     */
    Status DeleteStreams(std::set<std::string> &deleteStreams);

    /**
     * @brief Determines if we need retry DeleteStream yet
     * @param[in] streamName Stream name
     * @return true if DeleteStream can be retried false if not
     */
    bool CanRetryDeleteStream(const std::string &streamName);

    /**
     * @brief Send the pending notification for stream.
     * @return Status of the call.
     */
    Status SendPendingNotificationForStream(const std::string &workerAddr, const std::string &streamName);

    const int ASYNC_NOTIFY_TIME_MS = 100;  // Time interval between two async update object.
    std::unique_ptr<ThreadPool> notifyThreadPool_{ nullptr };
    std::unique_ptr<ThreadPool> deleteThreadPool_{ nullptr };
    std::future<Status> notifyFut_;
    std::future<Status> deleteFut_;
    WaitPost cvLock_;
    std::atomic<bool> interruptFlag_{ false };

    // Protect notifyWorkerMap_.
    std::shared_timed_mutex notifyMutex_;
    TbbNotifyWorkerMap notifyWorkerMap_;
    // Protect pendingDeleteStreams_ and pendingDeleteStreamsLastRetry_.
    std::shared_timed_mutex deleteMutex_;
    std::set<std::string> pendingDeleteStreams_;
    std::unordered_map<std::string, std::chrono::high_resolution_clock::time_point> pendingDeleteStreamsLastRetry_;

    std::shared_ptr<RocksStreamMetaStore> streamMetaStore_{ nullptr };

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    std::shared_ptr<RpcSessionManager> rpcSessionManager_{ nullptr };
    EtcdClusterManager *etcdCM_{ nullptr };
    SCMetadataManager *scMetadataManager_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_STREAM_CACHE_SC_NOTIFY_WORKER_MANAGER_H
