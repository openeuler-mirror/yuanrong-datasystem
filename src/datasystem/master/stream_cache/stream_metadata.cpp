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
 * Description: The definition of stream metadata object.
 */
#include "datasystem/master/stream_cache/stream_metadata.h"

#include <utility>
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/master/stream_cache/sc_notify_worker_manager.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"

namespace datasystem {
namespace master {
StreamMetadata::StreamMetadata(std::string streamName, const StreamFields &streamFields,
                               RocksStreamMetaStore *streamMetaStore, std::shared_ptr<AkSkManager> akSkManager,
                               std::shared_ptr<RpcSessionManager> rpcSessionManager, EtcdClusterManager *etcdCM,
                               SCNotifyWorkerManager *notifyWorkerManager)
    : streamName_(std::move(streamName)),
      streamFields_(streamFields),
      topoManager_(std::make_unique<TopologyManager>(streamName_)),
      streamStore_(streamMetaStore),
      alive_(true),
      akSkManager_(std::move(akSkManager)),
      rpcSessionManager_(std::move(rpcSessionManager)),
      etcdCM_(etcdCM),
      notifyWorkerManager_(notifyWorkerManager)
{
}

StreamMetadata::~StreamMetadata()
{
    // Update stream metrics final time before exit
    if (scStreamMetrics_) {
        UpdateStreamMetrics();
    }
}

Status StreamMetadata::PubIncreaseNode(const ProducerMetaPb &producerMeta, StreamFields &streamFields)
{
    HostPort pubWorkerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    LOG(INFO) << "Topo for stream " << streamName_ << ":" << *topoManager_;
    // Check topo manager status and return if stream is getting deleted.
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));
    bool isRecon = false;
    // The procedure is not entirely locked in this code path to allow parallel CreateProducer on master.
    // PubIncreaseNodeStart and PubIncreaseNodeEnd will acquire unique lock when necessary.
    // While the RPC requests can be sent in parallel without lock.
    bool alreadyLocked = false;
    RETURN_IF_NOT_OK(PubIncreaseNodeInternal(producerMeta, streamFields, pubWorkerAddress, isRecon, alreadyLocked));
    return Status::OK();
}

Status StreamMetadata::UpdateStreamFields(const StreamFields &streamFields)
{
    RETURN_IF_NOT_OK(streamStore_->UpdateStream(streamName_, streamFields));
    streamFields_ = streamFields;
    LOG(INFO) << FormatString(
        "[%s] Stream configuration updated with max stream size: %zu, page size: %zu, "
        "auto cleanup: %s, retainDataForNumConsumers: %zu, encrypt stream: %s, and reserve size: %zu",
        LogPrefix(), streamFields_.maxStreamSize_, streamFields_.pageSize_,
        streamFields_.autoCleanup_ ? "true" : "false", streamFields_.retainForNumConsumers_,
        streamFields_.encryptStream_ ? "true" : "false", streamFields_.reserveSize_);
    retainData_.Init(streamFields_.retainForNumConsumers_);
    return Status::OK();
}

Status StreamMetadata::PubIncreaseNodeStart(const StreamFields &streamFields, const ProducerMetaPb &producerMeta,
                                            bool alreadyLocked, bool &streamFieldsVerified, bool &saveToRocksdb,
                                            bool &isFirstProducer)
{
    WriteLockHelper wlocker(DEFER_LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    if (!alreadyLocked) {
        wlocker.AcquireLock();
    }
    // Check timeout before processing isFirstProducer, it might happen that the request already timed out,
    // so another CreateProducer request of the stream comes through from the same worker.
    CHECK_FAIL_RETURN_STATUS(scTimeoutDuration.CalcRealRemainingTime() > 0, K_RPC_DEADLINE_EXCEEDED,
                             "CreateProducer RPC timeout.");

    RETURN_IF_NOT_OK(VerifyStreamFields(streamFields));
    if (streamFields_ != streamFields) {
        RETURN_IF_NOT_OK(UpdateStreamFields(streamFields));
    }
    streamFieldsRefcount_++;
    streamFieldsVerified = true;

    // Update topological structure on master node.
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));
    RETURN_IF_NOT_OK(topoManager_->PubIncreaseNode(producerMeta, isFirstProducer));
    // Save it in memory and rocksdb.
    RETURN_IF_NOT_OK(streamStore_->AddPubNode(producerMeta));
    saveToRocksdb = true;

    return Status::OK();
}

Status StreamMetadata::PubIncreaseNodeEnd(bool isFirstProducer, bool needsRollback, bool alreadyLocked,
                                          const ProducerMetaPb &producerMeta, const HostPort &pubWorkerAddress,
                                          bool streamFieldsVerified, const std::vector<HostPort> &notifyNodeSet,
                                          bool saveToRocksdb)
{
    WriteLockHelper wlocker(DEFER_LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    if (!alreadyLocked) {
        wlocker.AcquireLock();
    }

    if (isFirstProducer) {
        RETURN_IF_NOT_OK(topoManager_->PubNodeFirstOrLastDone(producerMeta));
    }

    // Early return if rollback is not needed.
    RETURN_OK_IF_TRUE(!needsRollback);

    // If other CreateProducer requests come with the same stream fields,
    // then there is no need to rollback the stream field settings.
    // So only rollback when ref count is 0 after decrement.
    if (streamFieldsVerified) {
        streamFieldsRefcount_--;
        if (streamFieldsRefcount_ == 0) {
            // Rollback stream fields to empty.
            streamFields_ = StreamFields();
            RETURN_IF_NOT_OK(streamStore_->UpdateStream(streamName_, StreamFields()));
            // Retain data need to be unset.
            retainData_.RollBackToInit();
        }
    }

    for (const auto &node : notifyNodeSet) {
        NotifyPubPb pub;
        pub.set_is_close(true);
        pub.set_stream_name(streamName_);
        pub.set_worker_addr(pubWorkerAddress.ToString());
        RETURN_IF_NOT_OK(notifyWorkerManager_->AddAsyncPubNotification(node.ToString(), pub));
    }

    if (saveToRocksdb) {
        RETURN_IF_NOT_OK(topoManager_->PubDecreaseNode(producerMeta, false));
        RETURN_IF_NOT_OK(streamStore_->DelPubNode(producerMeta));
    }
    return Status::OK();
}

Status StreamMetadata::PubIncreaseNodeInternal(const ProducerMetaPb &producerMeta, StreamFields &streamFields,
                                               const HostPort &pubWorkerAddress, bool isRecon, bool alreadyLocked)
{
    bool saveToRocksdb = false;
    bool isFirstProducer = false;
    bool streamFieldsVerified = false;
    const auto &streamName = producerMeta.stream_name();
    CHECK_FAIL_RETURN_STATUS(streamName == streamName_, K_INVALID,
                             FormatString("Stream name mismatch, expected %s, received %s", streamName_, streamName));
    std::vector<HostPort> notifyNodeSet;

    bool needsRollback = true;
    Raii pubIncreaseNodeEnd([&]() {
        LOG_IF_ERROR(PubIncreaseNodeEnd(isFirstProducer, needsRollback, alreadyLocked, producerMeta, pubWorkerAddress,
                                        streamFieldsVerified, notifyNodeSet, saveToRocksdb),
                     "PubIncreaseNodeEnd rollback failed.");
    });

    RETURN_IF_NOT_OK(PubIncreaseNodeStart(streamFields, producerMeta, alreadyLocked, streamFieldsVerified,
                                          saveToRocksdb, isFirstProducer));
    if (isFirstProducer) {
        Status rc = PubIncreaseNodeImpl(pubWorkerAddress, notifyNodeSet, isRecon);
        LOG(INFO) << FormatString("[%s] Add new pub node:<%s> finish with %s.", LogPrefix(),
                                  pubWorkerAddress.ToString(), rc.GetMsg());
        RETURN_IF_NOT_OK(rc);
    }
    needsRollback = false;
    return Status::OK();
}

RetainDataState::State StreamMetadata::CheckNUpdateNeedRetainData()
{
    bool retainStateChange = false;
    const bool update = true;
    return CheckNeedRetainData(retainStateChange, update);
}

RetainDataState::State StreamMetadata::CheckNeedRetainData(bool &retainStateChange, const bool update)
{
    // If stream is initialized (at least a producer is created)
    // And we are currently retaining data
    if (!streamFields_.Empty() && retainData_.GetRetainDataState() != RetainDataState::State::NOT_RETAIN) {
        // We have at least as many consumers that user asked for
        if (topoManager_->GetConsumerCountForLife() >= streamFields_.retainForNumConsumers_) {
            // then do not retain data
            if (update) {
                retainData_.SetRetainDataState(RetainDataState::State::NOT_RETAIN);
            }
            retainStateChange = true;
        }
    }
    return retainData_.GetRetainDataState();
}

Status StreamMetadata::PubIncreaseNodeImpl(const HostPort &pubWorkerAddress, std::vector<HostPort> &notifyNodeSet,
                                           bool isRecon)
{
    // Now, we have two things to do, the first one is to Sync subTopoSet(in consumer sense) for src node
    // consumerNodeSet = {consumer1, consumer2, ..., consumerN}, this set will sync to src pub node as remote consumers.
    auto consumerMetaList = topoManager_->GetAllConsumerNotFromSrc(pubWorkerAddress.ToString());
    auto retainData = CheckNUpdateNeedRetainData();  // Check if we have to retain the data
    std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
    RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(pubWorkerAddress, masterWorkerApi, akSkManager_));
    if (!consumerMetaList.empty()
        || (retainData == RetainDataState::State::NOT_RETAIN && streamFields_.retainForNumConsumers_)) {
        LOG(INFO) << "[RetainData] Master sending SyncConsumerNode for stream " << streamName_ << "Remote producer "
                  << pubWorkerAddress.ToString() << " Current state " << retainData;
        RETURN_IF_NOT_OK_EXCEPT(masterWorkerApi->SyncConsumerNode(streamName_, consumerMetaList, retainData, isRecon),
                                StatusCode::K_DUPLICATED);
    }

    INJECT_POINT("master.PubIncreaseNodeImpl.beforeSendNotification");

    // And the second is to broadcast to each sub node of this stream the new producer comes.
    std::set<HostPort> subNodeSet;
    RETURN_IF_NOT_OK(topoManager_->GetAllSubNode(subNodeSet));
    RETURN_IF_NOT_OK(RemoveSourceWorker(pubWorkerAddress, subNodeSet));
    for (const auto &subNode : subNodeSet) {
        RETURN_IF_NOT_OK_EXCEPT(
            notifyWorkerManager_->NotifyNewPubNode(subNode, streamName_, streamFields_, pubWorkerAddress),
            StatusCode::K_DUPLICATED);
        notifyNodeSet.emplace_back(subNode);
        INJECT_POINT("master.PubIncreaseNodeImpl.afterSendNotification");
    }
    // Check after the UpdateTopoNotification, the RPC can be done through local bypass, that would run without
    // scTimeoutDuration. If it timeout, we should rollback because worker side should have stopped waiting for
    // response.
    CHECK_FAIL_RETURN_STATUS(scTimeoutDuration.CalcRealRemainingTime() > 0, K_RPC_DEADLINE_EXCEEDED,
                             "CreateProducer RPC timeout.");
    return Status::OK();
}

Status StreamMetadata::PubDecreaseNodeStart(const ProducerMetaPb &producerMeta, bool &isLastProducer)
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    // Check if this worker's last producer is already closed
    CHECK_FAIL_RETURN_STATUS(topoManager_->ExistsProducer(producerMeta), K_SC_PRODUCER_NOT_FOUND,
                             "producer not exists");
    RETURN_IF_NOT_OK(topoManager_->PubDecreaseNodeStart(producerMeta, isLastProducer));
    return Status::OK();
}

Status StreamMetadata::PubDecreaseNode(const ProducerMetaPb &producerMeta, const bool forceClose)
{
    LOG(INFO) << "Topo for stream " << streamName_ << ":" << *topoManager_;
    // Check topo manager status and return if stream is getting deleted.
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));

    // Pre-handling to check whether it is the last producer.
    // Requests should have been serialized by worker level create lock in most cases.
    // But still set firstProducerProcessing_ to stop
    // other CreateProducer request of the same worker from happening in timeout cases.
    bool isLastProducer = false;
    RETURN_IF_NOT_OK(PubDecreaseNodeStart(producerMeta, isLastProducer));
    std::unique_ptr<Raii> pubDecreaseNodeEnd = std::make_unique<Raii>([&]() {
        if (isLastProducer) {
            LOG_IF_ERROR(topoManager_->PubNodeFirstOrLastDone(producerMeta), "");
        }
    });
    HostPort pubWorkerAddress(producerMeta.worker_address().host(), producerMeta.worker_address().port());
    if (isLastProducer) {
        INJECT_POINT("master.PubDecreaseNode.beforeSendNotification");
        // Only send notification when force close is true.
        if (forceClose) {
            // Construct rpc channel between master and all sub node.
            std::set<HostPort> subNodeSet;
            RETURN_IF_NOT_OK(topoManager_->GetAllSubNode(subNodeSet));
            RETURN_IF_NOT_OK(RemoveSourceWorker(pubWorkerAddress, subNodeSet));
            for (const auto &subNode : subNodeSet) {
                auto rc = notifyWorkerManager_->NotifyDelPubNode(subNode, streamName_, pubWorkerAddress, forceClose);
                if (rc.IsError() && rc.GetCode() != StatusCode::K_NOT_FOUND
                    && rc.GetCode() != StatusCode::K_SC_STREAM_NOT_FOUND) {
                    LOG(ERROR) << "NotifyDelPubNode failed " << rc.GetMsg();
                    return rc;
                }
            }
        }
        INJECT_POINT("master.PubDecreaseNode.afterSendNotification");
    } else {
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Producer decrease S:%s. Not the last producer for worker %s",
                                                  streamName_, pubWorkerAddress.ToString());
    }

    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    // In the happy path, unset firstProducerProcessing_ state under lock protection to avoid timing hole.
    pubDecreaseNodeEnd.reset();
    RETURN_IF_NOT_OK(topoManager_->PubDecreaseNode(producerMeta, false));
    LOG_IF_ERROR(streamStore_->DelPubNode(producerMeta), "Delete pub node failed in rocksdb");
    // If auto clean up is true and there is no more producer/consumer, delete the stream.
    if (isLastProducer && streamFields_.autoCleanup_) {
        RETURN_IF_NOT_OK(AutoCleanupIfNeededNotLocked(pubWorkerAddress));
    }
    return Status::OK();
}

Status StreamMetadata::SubIncreaseNode(const ConsumerMetaPb &consumerMeta, bool isRecon)
{
    HostPort subWorkerAddress(consumerMeta.worker_address().host(), consumerMeta.worker_address().port());
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    INJECT_POINT("master.SubIncreaseNode.afterLock");
    CHECK_FAIL_RETURN_STATUS(scTimeoutDuration.CalcRealRemainingTime() > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("[%s] Subscribe Request timeout.", LogPrefix()));
    LOG(INFO) << "Topo for stream " << streamName_ << ":" << *topoManager_;
    // Check topo manager status and return if stream is getting deleted.
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));
    return SubIncreaseNodeUnlocked(consumerMeta, subWorkerAddress, isRecon);
}

Status StreamMetadata::SubIncreaseNodeUnlocked(const ConsumerMetaPb &consumerMeta, const HostPort &subWorkerAddress,
                                               bool isRecon)
{
    bool saveToRocksdb = false;
    bool sendToSrcNode = false;
    std::vector<HostPort> notifyNodeSet;
    CHECK_FAIL_RETURN_STATUS(
        consumerMeta.stream_name() == streamName_, K_INVALID,
        FormatString("Stream name mismatch, expected %s, received %s", streamName_, consumerMeta.stream_name()));
    Status rc =
        SubIncreaseNodeImpl(consumerMeta, subWorkerAddress, saveToRocksdb, sendToSrcNode, notifyNodeSet, isRecon);
    LOG(INFO) << FormatString("[%s, C:%s] Add new consumer finish with %s.", LogPrefix(),
                              LogHelper::IgnoreSensitive(consumerMeta), rc.GetMsg());

    RETURN_OK_IF_TRUE(rc.IsOk());

    // Add async sub close notification to rollback.
    for (const auto &node : notifyNodeSet) {
        NotifyConsumerPb sub;
        *sub.mutable_consumer() = consumerMeta;
        sub.set_is_close(true);
        RETURN_IF_NOT_OK(notifyWorkerManager_->AddAsyncSubNotification(node.ToString(), sub));
    }

    if (sendToSrcNode) {
        std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
        RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(subWorkerAddress, masterWorkerApi, akSkManager_));
        RETURN_IF_NOT_OK_EXCEPT(masterWorkerApi->ClearAllRemotePub(streamName_), K_NOT_FOUND);
    }

    if (saveToRocksdb) {
        RETURN_IF_NOT_OK(topoManager_->SubDecreaseNode(consumerMeta, true, false));
        RETURN_IF_NOT_OK(streamStore_->DelSubNode(streamName_, consumerMeta.consumer_id()));
        // Update count for rollback
        RETURN_IF_NOT_OK(
            streamStore_->UpdateLifeTimeConsumerCount(streamName_, topoManager_->GetConsumerCountForLife()));
    }
    return rc;
}

Status StreamMetadata::NotifyStopRetainData(const HostPort &subWorkerAddress, bool retainStateChange)
{
    // Once the request is complete, retainData state has changed inform all related nodes
    if (retainStateChange) {
        // Relative node set for the producers
        std::set<HostPort> relatedNodeSet;
        RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(relatedNodeSet, retainStateChange));
        RETURN_IF_NOT_OK(RemoveSourceWorker(subWorkerAddress, relatedNodeSet));
        for (const auto &relatedNode : relatedNodeSet) {
            LOG(INFO) << "[RetainData] Master sending NotifyStopRetention for stream " << streamName_
                      << "Remote producer " << relatedNode.ToString();
            RETURN_IF_NOT_OK(notifyWorkerManager_->AddAsyncStopDataRetentionNotification(relatedNode, streamName_));
        }
    }
    return Status::OK();
}

Status StreamMetadata::SubIncreaseNodeImpl(const ConsumerMetaPb &consumerMeta, const HostPort &subWorkerAddress,
                                           bool &saveToRocksdb, bool &sendToSrcNode,
                                           std::vector<HostPort> &notifyNodeSet, bool isRecon)
{
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));
    RETURN_IF_NOT_OK(topoManager_->CheckNewConsumer(consumerMeta));

    // Save it in memory and rocksdb.
    bool isFirstConsumer = false;
    RETURN_IF_NOT_OK(topoManager_->SubIncreaseNode(consumerMeta, isFirstConsumer));
    RETURN_IF_NOT_OK(streamStore_->AddSubNode(consumerMeta));
    RETURN_IF_NOT_OK(streamStore_->UpdateLifeTimeConsumerCount(streamName_, topoManager_->GetConsumerCountForLife()));
    saveToRocksdb = true;

    bool retainStateChange = false;
    (void)CheckNeedRetainData(retainStateChange);

    // Notify the source node to add remote producer information.
    std::set<HostPort> pubNodeSet;
    RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(pubNodeSet));
    RETURN_IF_NOT_OK(RemoveSourceWorker(subWorkerAddress, pubNodeSet));
    if (isFirstConsumer && !pubNodeSet.empty()) {
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("consumer meta:<%s>, isFirstConsumer:<%d>",
                                                  LogHelper::IgnoreSensitive(consumerMeta), isFirstConsumer);
        std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
        RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(subWorkerAddress, masterWorkerApi, akSkManager_));
        RETURN_IF_NOT_OK_EXCEPT(masterWorkerApi->SyncPubNode(streamName_, pubNodeSet, isRecon),
                                StatusCode::K_DUPLICATED);
        sendToSrcNode = true;
    }

    // Inform all nodes that have a producer or previously had a producer until retainData is flipped
    if (retainData_.IsDataRetained() || retainStateChange) {
        // When state changed, Inform all producers (already closed ones too) of new consumer
        std::set<HostPort> relatedNodeSet;
        RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(relatedNodeSet, true));
        RETURN_IF_NOT_OK(RemoveSourceWorker(subWorkerAddress, relatedNodeSet));
        pubNodeSet = relatedNodeSet;
        LOG(INFO) << "[RetainData] Informing all related nodes of SubIncrease " << pubNodeSet.size();
    }
    // Construct rpc channel between master and all pub node, broadcast this new consumer to all pub worker node.
    // We send this notifications to all pub nodes including the ones that have previous producer closed
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("New Sub of Stream: [%s], notify [%zu] pubNodes", streamName_,
                                              pubNodeSet.size());
    for (const auto &pubNode : pubNodeSet) {
        RETURN_IF_NOT_OK_EXCEPT(notifyWorkerManager_->NotifyNewConsumer(pubNode, consumerMeta),
                                StatusCode::K_DUPLICATED);
        notifyNodeSet.emplace_back(pubNode);
        INJECT_POINT("master.SubIncreaseNodeImpl.afterSendNotification");
    }

    // Notify all nodes of state change
    RETURN_IF_NOT_OK(NotifyStopRetainData(subWorkerAddress, retainStateChange));
    if (retainStateChange) {
        retainData_.SetRetainDataState(RetainDataState::State::NOT_RETAIN);
    }
    CHECK_FAIL_RETURN_STATUS(scTimeoutDuration.CalcRealRemainingTime() > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("[%s] Request timeout.", LogPrefix()));
    return Status::OK();
}

Status StreamMetadata::SubDecreaseNode(const ConsumerMetaPb &consumerMeta)
{
    // Check topo manager status and return if stream is getting deleted.
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    LOG(INFO) << "Topo for stream " << streamName_ << ":" << *topoManager_;
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));
    CHECK_FAIL_RETURN_STATUS(topoManager_->ExistsConsumer(consumerMeta.consumer_id()), K_SC_CONSUMER_NOT_FOUND,
                             "consumer not exists");

    // Construct rpc channel between master and all pub node.
    HostPort subWorkerAddress(consumerMeta.worker_address().host(), consumerMeta.worker_address().port());

    std::set<HostPort> pubNodeSet;
    // Inform all nodes that have a producer or previously had a producer until retainData is flipped
    if (retainData_.IsDataRetained()) {
        // When state changed, Inform all producers (already closed ones too) of new consumer
        RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(pubNodeSet, true));
        RETURN_IF_NOT_OK(RemoveSourceWorker(subWorkerAddress, pubNodeSet));
        LOG(INFO) << "[RetainData] Informing all related nodes of SubDecrease " << pubNodeSet.size();
    } else {
        RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(pubNodeSet));
        RETURN_IF_NOT_OK(RemoveSourceWorker(subWorkerAddress, pubNodeSet));
    }

    for (const auto &pubNode : pubNodeSet) {
        auto rc = notifyWorkerManager_->NotifyDelConsumer(pubNode, consumerMeta);
        if (rc.IsError() && rc.GetCode() != StatusCode::K_SC_CONSUMER_NOT_FOUND
            && rc.GetCode() != StatusCode::K_SC_STREAM_NOT_FOUND) {
            LOG(ERROR) << "NotifyDelConsumer failed " << rc.GetMsg();
            return rc;
        }
        INJECT_POINT("master.SubDecreaseNode.afterSendNotification");
    }

    RETURN_IF_NOT_OK(topoManager_->SubDecreaseNode(consumerMeta, false, false));
    LOG_IF_ERROR(streamStore_->DelSubNode(streamName_, consumerMeta.consumer_id()),
                 "Delete sub node failed in rocksdb");
    LOG(INFO) << FormatString("[%s, C:%s] Delete consumer success.", LogPrefix(),
                              LogHelper::IgnoreSensitive(consumerMeta));
    // If there is no more consumer and auto clean up is on
    // Also consider the case that
    // (a) consumer is created first
    // (b) consumer crash and the application will not create any producer
    // We will take this chance to clean up the stream
    bool lastConsumer = topoManager_->GetConsumerCountInWorker(subWorkerAddress.ToString()) == 0;
    if ((streamFields_.Empty() || streamFields_.autoCleanup_) && lastConsumer) {
        RETURN_IF_NOT_OK(AutoCleanupIfNeededNotLocked(subWorkerAddress));
    }
    return Status::OK();
}

Status StreamMetadata::DeleteStreamStart(const HostPort &srcNode, std::set<HostPort> &relatedWorkerSet)
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    // 1. Check if stream is already getting deleted
    CHECK_FAIL_RETURN_STATUS(!topoManager_->GetStreamStatus(), StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS,
                             FormatString("Stream <%s> is undergoing deletion, do not operate it", streamName_));
    // 2. Deal with concurrent delete, directly return if this stream has been deleted.
    RETURN_OK_IF_TRUE(alive_ == false);

    // 3. Check if any producer and consumers exists for this stream
    // We do a check on the target stream to find out all producer and consumer in global scope has been closed.
    std::set<HostPort> pubWorkerSet;
    RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(pubWorkerSet));
    CHECK_FAIL_RETURN_STATUS(
        pubWorkerSet.empty(), StatusCode::K_SC_STREAM_IN_USE,
        FormatString("Stream:<%s>, State:<Has pub node in global scope>, Number:<%d>, Request src:<%s>", streamName_,
                     pubWorkerSet.size(), srcNode.ToString()));
    std::set<HostPort> subWorkerSet;
    RETURN_IF_NOT_OK(topoManager_->GetAllSubNode(subWorkerSet));
    CHECK_FAIL_RETURN_STATUS(
        subWorkerSet.empty(), StatusCode::K_SC_STREAM_IN_USE,
        FormatString("Stream:<%s>, State:<Has sub node in global scope>, Number:<%d>, Request src:<%s>", streamName_,
                     subWorkerSet.size(), srcNode.ToString()));

    // 4. Check if any notifications are still needed to be sent
    CHECK_FAIL_RETURN_STATUS(!notifyWorkerManager_->ExistsPendingNotification(streamName_),
                             StatusCode::K_SC_STREAM_NOTIFICATION_PENDING,
                             FormatString("Stream <%s> exists pending notification, try again later", streamName_));

    // If we pass all checks, we set this stream state as [isDeleting], and do the following process.
    RETURN_IF_NOT_OK(topoManager_->SetDeletingStatus());
    RETURN_IF_NOT_OK(topoManager_->GetAllRelatedNode(relatedWorkerSet));
    relatedWorkerSet.erase(srcNode);
    deleterRefCount_ += 1;
    LOG(INFO) << FormatString("[%s] Delete stream request started, ref count increase to %d.", LogPrefix(),
                              deleterRefCount_);
    return Status::OK();
}

Status StreamMetadata::DeleteStreamEnd()
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));

    RETURN_IF_NOT_OK(streamStore_->DelStream(streamName_));
    alive_ = false;
    deleterRefCount_ -= 1;
    LOG(INFO) << FormatString("[%s] Delete stream from streamStore success. Ref Count decrease to %d", LogPrefix(),
                              deleterRefCount_);
    return Status::OK();
}

void StreamMetadata::UndoDeleteStream(bool decrementRef)
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    if (decrementRef) {
        deleterRefCount_ -= 1;
        LOG(INFO) << FormatString("[%s] Ref Count decrease to %d", LogPrefix(), deleterRefCount_);
    }
    // only undo if there are no other deletes running
    if (deleterRefCount_ == 0) {
        LOG(INFO) << FormatString("[%s] Undoing Delete stream.", LogPrefix());
        this->topoManager_->UnsetDeletingStatus();
    }
}

std::string StreamMetadata::LogPrefix() const
{
    return FormatString("S:%s", streamName_);
}

Status StreamMetadata::RecoveryPubMeta(const ProducerMetaPb &producerMetaPb)
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    bool isFirstProducer = false;
    RETURN_IF_NOT_OK(topoManager_->PubIncreaseNode(producerMetaPb, isFirstProducer));
    retainData_.Init(streamFields_.retainForNumConsumers_);
    // Recovery code path would not need the protection, so directly call PubNodeFirstOrLastDone to unset if applicable.
    if (isFirstProducer) {
        RETURN_IF_NOT_OK(topoManager_->PubNodeFirstOrLastDone(producerMetaPb));
    }
    LOG(INFO) << FormatString("[%s] Recovery pub node meta:<%s> on master success, isFirstProducer on that node:%s",
                              LogPrefix(), LogHelper::IgnoreSensitive(producerMetaPb),
                              isFirstProducer ? "true" : "false");
    return Status::OK();
}

Status StreamMetadata::RecoverySubMeta(const ConsumerMetaPb &consumerMeta)
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    bool isFirstConsumer = false;
    RETURN_IF_NOT_OK(topoManager_->SubIncreaseNode(consumerMeta, isFirstConsumer));
    LOG(INFO) << FormatString("[%s] Recovery consumer meta:<%s> on master success, isFirstConsumer on that node:%s",
                              LogPrefix(), LogHelper::IgnoreSensitive(consumerMeta),
                              isFirstConsumer ? "true" : "false");
    return Status::OK();
}

Status StreamMetadata::RemoveSourceWorker(const HostPort &srcWorkerAddress, std::set<HostPort> &nodeSet)
{
    auto totalSize = nodeSet.size();
    if (nodeSet.find(srcWorkerAddress) != nodeSet.end()) {
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Src worker node:<%s>, State:<Exist in node set>",
                                                  srcWorkerAddress.ToString());
        CHECK_FAIL_RETURN_STATUS(nodeSet.erase(srcWorkerAddress) == 1, StatusCode::K_RUNTIME_ERROR,
                                 "Runtime error in set erase");
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Origin set size:<%d>, Actual set size:<%d>, Operation:<Broadcast>",
                                              totalSize, nodeSet.size());
    return Status::OK();
}

Status StreamMetadata::ClearPubSubMetaData(const HostPort &workerAddr,
                                           const std::unordered_map<std::string, ProducerMetaPb> &producerMap,
                                           const std::unordered_map<std::string, ConsumerMetaPb> &consumerMap,
                                           const bool forceClose, const bool delWorker)
{
    INJECT_POINT("StreamMetadata.ClearPubSubMetaData.sleep");
    bool pubExists = !producerMap.empty();
    bool subExists = !consumerMap.empty();
    LOG(INFO) << FormatString("[%s] ClearPubSubMetaData for worker [%s] producerCount:%d, consumerCount:%d",
                              LogPrefix(), workerAddr.ToString(), producerMap.size(), consumerMap.size());
    CHECK_FAIL_RETURN_STATUS(pubExists || subExists, K_NOT_FOUND,
                             FormatString("[%s] No meta on worker %s", LogPrefix(), workerAddr.ToString()));
    Status status;
    for (const auto &kvPub : producerMap) {
        ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status, streamStore_->DelPubNode(kvPub.second), "DelPubNode failed");
        ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status, topoManager_->PubDecreaseNode(kvPub.second, delWorker),
                                         "PubDecreaseNode failed");
    }
    bool producerCnt = topoManager_->GetProducerCountInWorker(workerAddr.ToString());
    bool pubNodeDelete = !producerMap.empty() && producerCnt == 0;

    for (const auto &kvSub : consumerMap) {
        ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status, streamStore_->DelSubNode(streamName_, kvSub.second.consumer_id()),
                                         "DelSubNode failed");
        ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status, topoManager_->SubDecreaseNode(kvSub.second, false, delWorker),
                                         "SubDecreaseNode failed");
    }

    ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status,
                                     AddAsyncClearNotification(workerAddr, pubNodeDelete, consumerMap, forceClose),
                                     "AddAsyncNotification failed");

    // Check if we can delete the stream
    bool lastProducerCleanup = (producerCnt == 0 && streamFields_.autoCleanup_);
    bool lastConsumerCleanup = (topoManager_->GetConsumerCountInWorker(workerAddr.ToString()) == 0)
                               && (streamFields_.Empty() || streamFields_.autoCleanup_);
    // If auto clean up is true and there is no more producer/consumer, delete the stream.
    if (lastProducerCleanup && lastConsumerCleanup) {
        ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status, AutoCleanupIfNeededNotLocked(workerAddr),
                                         "AutoCleanupIfNeededNotLocked failed");
    }
    return status;
}

Status StreamMetadata::AddAsyncClearNotification(const HostPort &workerAddr, bool pubNodeDelete,
                                                 const std::unordered_map<std::string, ConsumerMetaPb> &consumerMap,
                                                 const bool forceClose)
{
    std::set<HostPort> pubNodeSet;
    std::set<HostPort> subNodeSet;
    std::set<HostPort> allNodeSet;
    RETURN_IF_NOT_OK(topoManager_->GetAllPubNode(pubNodeSet));
    RETURN_IF_NOT_OK(topoManager_->GetAllSubNode(subNodeSet));
    RETURN_IF_NOT_OK(topoManager_->GetAllRelatedNode(allNodeSet));

    Status status;
    for (const auto &nodeAddr : allNodeSet) {
        if (nodeAddr == workerAddr) {
            continue;
        }

        if (pubNodeDelete && subNodeSet.count(nodeAddr) > 0) {
            NotifyPubPb pub;
            pub.set_is_close(true);
            pub.set_force_close(forceClose);
            pub.set_stream_name(streamName_);
            pub.set_worker_addr(workerAddr.ToString());
            pub.set_max_stream_size(streamFields_.maxStreamSize_);
            pub.set_page_size(streamFields_.pageSize_);
            ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status,
                                             notifyWorkerManager_->AddAsyncPubNotification(nodeAddr.ToString(), pub),
                                             "AddAsyncPubNotification failed");
        }

        if (!consumerMap.empty() && pubNodeSet.count(nodeAddr) > 0) {
            for (const auto &kv : consumerMap) {
                NotifyConsumerPb sub;
                *sub.mutable_consumer() = kv.second;
                sub.set_is_close(true);
                ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(
                    status, notifyWorkerManager_->AddAsyncSubNotification(nodeAddr.ToString(), sub),
                    "AddAsyncSubNotification failed");
            }
        }
    }
    return status;
}

Status StreamMetadata::ClearWorkerMetadata(const HostPort &workerAddr, const bool forceClose, bool delWorker)
{
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));
    std::unordered_map<std::string, ProducerMetaPb> producerMap;
    RETURN_IF_NOT_OK(topoManager_->GetAllProducerFromWorker(workerAddr, producerMap));
    auto consumerMap = topoManager_->GetAllConsumerFromWorker(workerAddr);
    // Delete the related worker from master metadata as its faulty in this case
    RETURN_IF_NOT_OK(ClearPubSubMetaData(workerAddr, producerMap, consumerMap, forceClose, delWorker));
    return Status::OK();
}

Status StreamMetadata::CheckMetadata(const GetStreamMetadataRspPb &meta, const HostPort &workerAddr)
{
    VLOG(SC_INTERNAL_LOG_LEVEL) << "Check metadata for stream " << streamName_;
    WriteLockHelper wlocker(LOCK_ARGS_MSG_FN(mutex_, LogPrefix));

    // Get worker metadata.
    VLOG(SC_INTERNAL_LOG_LEVEL) << "Check metadata response from worker: " << LogHelper::IgnoreSensitive(meta);
    std::vector<ProducerMetaPb> workerProducers(meta.producers().begin(), meta.producers().end());
    std::vector<ConsumerMetaPb> workerConsumers(meta.consumers().begin(), meta.consumers().end());

    // Get master metadata.
    std::unordered_map<std::string, ProducerMetaPb> masterProducerMap;
    RETURN_IF_NOT_OK(topoManager_->GetAllProducerFromWorker(workerAddr, masterProducerMap));
    auto masterConsumerMap = topoManager_->GetAllConsumerFromWorker(workerAddr);

    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("Check metadata master producer count: %d, consumer count: %d",
                                                masterProducerMap.size(), masterConsumerMap.size());

    // Compare and get the difference.
    CompareAndErase(workerProducers, masterProducerMap,
                    [](const ProducerMetaPb &meta) { return HostPb2Str(meta.worker_address()); });
    CompareAndErase(workerConsumers, masterConsumerMap, [](const ConsumerMetaPb &meta) { return meta.consumer_id(); });

    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
        "Check result: <worker has extra pub:%zu sub:%zu> <master has extra pub:%zu sub:%zu>", workerProducers.size(),
        workerConsumers.size(), masterProducerMap.size(), masterConsumerMap.size());

    // Update missing pub-sub metadata.
    auto streamFields = ConvertGetStreamMetadataRspPb2StreamFields(meta);
    if (!workerProducers.empty() || !workerConsumers.empty()) {
        // Update consumers first, so that we have count for retain consumers
        for (auto &consumerMeta : workerConsumers) {
            RETURN_IF_NOT_OK(SubIncreaseNodeUnlocked(consumerMeta, workerAddr, true));
        }
        for (auto &producerMeta : workerProducers) {
            RETURN_IF_NOT_OK(PubIncreaseNodeInternal(producerMeta, streamFields, workerAddr, true));
        }
    } else if (topoManager_->RecoverEmptyMetaIfNeeded(workerAddr)) {
        RETURN_IF_NOT_OK(VerifyAndUpdateStreamFields(streamFields));
    }

    // Notify clear pub and sub.
    if (!masterProducerMap.empty() || !masterConsumerMap.empty()) {
        // Do not delete the worker from metadata as its not faulty in this case
        RETURN_IF_NOT_OK_EXCEPT(ClearPubSubMetaData(workerAddr, masterProducerMap, masterConsumerMap, false, false),
                                K_NOT_FOUND);
    } else {
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString(
            "no need to clear pub and sub in master after comparison with worker addr %s", workerAddr.ToString());
    }

    // Clear remote pubs in sub node if last consumer.
    bool isAllConsumerClosed = IsAllConsumerClosed(workerAddr.ToString());
    if (!meta.is_remote_pub_empty() && isAllConsumerClosed && CheckWorkerStatus(workerAddr).IsOk()) {
        std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
        RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(workerAddr, masterWorkerApi, akSkManager_));
        RETURN_IF_NOT_OK(masterWorkerApi->ClearAllRemotePub(meta.stream_name()));
    }

    VLOG(SC_INTERNAL_LOG_LEVEL) << "Check metadata for stream " << streamName_ << " finish.";
    return Status::OK();
}

void StreamMetadata::GetAllProducerConsumer(std::vector<ProducerMetaPb> &masterProducers,
                                            std::vector<ConsumerMetaPb> &masterConsumers,
                                            std::vector<std::string> &producerRelatedNodes,
                                            std::vector<std::string> &consumerRelatedNodes)
{
    LOG_IF_ERROR(topoManager_->GetAllProducer(masterProducers), "failed to get producer data");
    masterConsumers = topoManager_->GetAllConsumer();
    topoManager_->GetAllRelatedNode(producerRelatedNodes, consumerRelatedNodes);
}

void StreamMetadata::PreparePubSubRelNodes(const std::vector<std::string> &producerRelatedNodes,
                                           const std::vector<std::string> &consumerRelatedNodes)
{
    topoManager_->PreparePubSubRelNodes(producerRelatedNodes, consumerRelatedNodes);
}

Status StreamMetadata::CleanUpStreamPersistent(const std::string &streamName)
{
    RETURN_IF_NOT_OK(streamStore_->DelStream(streamName));
    return Status::OK();
}

Status StreamMetadata::AutoCleanupIfNeeded(const HostPort &srcHost)
{
    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    RETURN_IF_NOT_OK(AutoCleanupIfNeededNotLocked(srcHost));
    return Status::OK();
}

Status StreamMetadata::AutoCleanupIfNeededNotLocked(const HostPort &srcHost)
{
    RETURN_OK_IF_TRUE(!streamFields_.autoCleanup_ && !streamFields_.Empty());
    auto cleanup = topoManager_->CheckIfAllPubSubHaveClosed();
    RETURN_OK_IF_TRUE(!cleanup);
    LOG(INFO) << FormatString("[%s] Driving auto stream delete%s", LogPrefix(),
                              srcHost.Empty() ? "" : " from worker " + srcHost.ToString());
    RETURN_IF_NOT_OK(notifyWorkerManager_->AddAsyncDeleteNotification(streamName_));
    return Status::OK();
}

Status StreamMetadata::CheckWorkerStatus(const HostPort &workerHostPort)
{
    if (etcdCM_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_INVALID, "ETCD cluster manager is nullptr.");
    }
    auto rc = etcdCM_->CheckConnection(workerHostPort);
    if (rc.IsError()) {
        RETURN_STATUS_LOG_ERROR(K_WORKER_ABNORMAL, FormatString("The worker %s is abnormal, detail: %s",
                                                                workerHostPort.ToString(), rc.GetMsg()));
    }
    return Status::OK();
}

Status StreamMetadata::ProcessClearAllRemotePub(const std::shared_ptr<MasterWorkerSCApi> &masterWorkerApi,
                                                const HostPort &subWorkerAddress)
{
    RETURN_RUNTIME_ERROR_IF_NULL(masterWorkerApi);
    static const int RETRY_TIMEOUT_MS = 60'000;  // 1 min
    const std::unordered_set<StatusCode> &retryOn = { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED,
                                                      StatusCode::K_RPC_DEADLINE_EXCEEDED,
                                                      StatusCode::K_RPC_UNAVAILABLE };
    Status rc;
    switch (masterWorkerApi->TypeId()) {
        case MasterWorkerSCApiType::MasterLocalWorkerSCApi:
            RETURN_IF_NOT_OK(RetryOnError(
                RETRY_TIMEOUT_MS,
                [&masterWorkerApi, this](int32_t) { return masterWorkerApi->ClearAllRemotePub(streamName_); },
                []() { return Status::OK(); }, retryOn));
            break;
        case MasterWorkerSCApiType::MasterRemoteWorkerSCApi:
            auto masterRemoteWorkerOCApi = dynamic_cast<MasterRemoteWorkerSCApi *>(masterWorkerApi.get());
            int32_t maxRpcTimeoutMs = 5'000;  // 5s
            int64_t tagId;
            auto timer = Timer(RETRY_TIMEOUT_MS);
            RETURN_IF_NOT_OK(RetryOnError(
                RETRY_TIMEOUT_MS,
                [&masterRemoteWorkerOCApi, &subWorkerAddress, &tagId, this](int32_t timeoutMs) {
                    RETURN_IF_NOT_OK(CheckWorkerStatus(subWorkerAddress));
                    scTimeoutDuration.Init(timeoutMs);
                    return masterRemoteWorkerOCApi->ClearAllRemotePubAsynWrite(streamName_, tagId);
                },
                []() { return Status::OK(); }, retryOn, maxRpcTimeoutMs));
            int waitIntervalMs1 = 10, waitIntervalMs2 = 1'000, fastReadingMaxNum = 10;
            int retryNum = 0;
            do {
                RETURN_IF_NOT_OK(CheckWorkerStatus(subWorkerAddress));
                rc = masterRemoteWorkerOCApi->ClearAllRemotePubAsynRead(tagId, RpcRecvFlags::DONTWAIT);
                if (rc.IsOk()) {
                    break;
                }
                auto waitIntervalMs = retryNum > fastReadingMaxNum ? waitIntervalMs2 : waitIntervalMs1;
                std::this_thread::sleep_for(std::chrono::milliseconds(waitIntervalMs));
                ++retryNum;
            } while (timer.GetRemainingTimeMs() > 0);
            if (rc.IsError()) {
                (void)masterRemoteWorkerOCApi->ClearAllRemotePubAsynRead(tagId, RpcRecvFlags::NONE);
            }
            break;
    }
    return rc;
}

Status StreamMetadata::InitStreamMetrics()
{
    return ScMetricsMonitor::Instance()->AddStreamMeta(streamName_, weak_from_this(), scStreamMetrics_);
}

void StreamMetadata::UpdateStreamMetrics()
{
    if (scStreamMetrics_) {
        scStreamMetrics_->LogMetric(StreamMetric::NumProducersMaster, GetProducerCount());
        scStreamMetrics_->LogMetric(StreamMetric::NumConsumersMaster, GetConsumerCount());
    }
}

StreamFields ConvertGetStreamMetadataRspPb2StreamFields(const GetStreamMetadataRspPb &pb)
{
    return { pb.max_stream_size(), static_cast<size_t>(pb.page_size()),
             pb.auto_cleanup(),    pb.retain_num_consumer(),
             pb.encrypt_stream(),  pb.reserve_size(),
             pb.stream_mode() };
}
}  // namespace master
}  // namespace datasystem
