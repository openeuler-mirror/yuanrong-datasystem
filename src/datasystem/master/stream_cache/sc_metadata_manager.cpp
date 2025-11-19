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
#include "datasystem/master/stream_cache/sc_metadata_manager.h"

#include <unordered_set>
#include <utility>

#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/master/stream_cache/sc_notify_worker_manager.h"
#include "datasystem/master/stream_cache/stream_metadata.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"

DS_DECLARE_string(rocksdb_store_dir);
DS_DECLARE_int32(sc_regular_socket_num);
DS_DECLARE_int32(sc_stream_socket_num);
DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
constexpr size_t THREAD_POOL_SIZE = 8;
const std::string SC_METADATA_MANAGER = "SCMetadataManager-";
namespace {
inline void HostPb2Host(const HostPortPb &hostPb, HostPort &host) noexcept
{
    host = HostPort(hostPb.host(), hostPb.port());
}

bool EnableSCService()
{
    return FLAGS_sc_regular_socket_num > 0 && FLAGS_sc_stream_socket_num > 0;
}
}  // namespace

SCMetadataManager::SCMetadataManager(const HostPort &masterHostPort, std::shared_ptr<AkSkManager> akSkManager,
                                     std::shared_ptr<RpcSessionManager> rpcSessionManager, EtcdClusterManager *cm,
                                     RocksStore *rocksStore, const std::string &dbName)
    : MetadataRedirectHelper(cm),
      masterAddress_(masterHostPort),
      akSkManager_(std::move(akSkManager)),
      rpcSessionManager_(std::move(rpcSessionManager)),
      dbName_(dbName),
      eventName_(SC_METADATA_MANAGER + dbName)
{
    streamMetaStore_ = std::make_shared<RocksStreamMetaStore>(rocksStore);
    exitFlag_ = std::make_shared<std::atomic_bool>(false);
}

SCMetadataManager::~SCMetadataManager()
{
    LOG(INFO) << "Destroy SCMetadataManager.";
    Shutdown();
}

void SCMetadataManager::Shutdown()
{
    if (exitFlag_->load()) {
        return;
    }
    exitFlag_->store(true);
    LOG(INFO) << "Start shutdown ScMetadataManager for " << dbName_;
    if (!EnableSCService()) {
        return;
    }
    CheckNewNodeMetaEvent::GetInstance().RemoveSubscriber(eventName_);
    StartClearWorkerMeta::GetInstance().RemoveSubscriber(eventName_);
    ClearWorkerMeta::GetInstance().RemoveSubscriber(eventName_);
    HashRingEvent::RecoverMetaRanges::GetInstance().RemoveSubscriber(eventName_);
    if (notifyWorkerManager_ != nullptr) {
        notifyWorkerManager_->Shutdown();
    }
    if (asyncReconciliationPool_ != nullptr) {
        asyncReconciliationPool_.reset();
    }
}

void SCMetadataManager::SetClusterManagerToNullptr()
{
    MetadataRedirectHelper::Shutdown();
}

Status SCMetadataManager::Init()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    CHECK_FAIL_RETURN_STATUS(rpcSessionManager_ != nullptr, StatusCode::K_RUNTIME_ERROR,
                             "Runtime error, failed to get RpcSessionManager");
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_rocksdb_store_dir, "~/.datasystem/rocksdb", "/master"));
    RETURN_IF_NOT_OK(streamMetaStore_->Init());
    RETURN_IF_EXCEPTION_OCCURS(asyncReconciliationPool_ =
                                   std::make_unique<ThreadPool>(0, THREAD_POOL_SIZE, "MScAsyncReconcilation", false));

    notifyWorkerManager_ =
        std::make_unique<SCNotifyWorkerManager>(streamMetaStore_, akSkManager_, rpcSessionManager_, etcdCM_, this);
    RETURN_IF_NOT_OK(notifyWorkerManager_->Init());
    RETURN_IF_NOT_OK(LoadMeta());
    CheckNewNodeMetaEvent::GetInstance().AddSubscriber(eventName_, [this](const HostPort &eventNodeKey) {
        StartCheckMetadata(eventNodeKey);
        return Status::OK();
    });
    StartClearWorkerMeta::GetInstance().AddSubscriber(eventName_, [this](const HostPort &eventNodeKey) {
        StartClearWorkerMetadata(eventNodeKey);
        return Status::OK();
    });
    ClearWorkerMeta::GetInstance().AddSubscriber(
        eventName_, [this](const HostPort &eventNodeKey) { return ClearWorkerMetadata(eventNodeKey); });
    HashRingEvent::RecoverMetaRanges::GetInstance().AddSubscriber(
        eventName_, [this](const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges) {
            return RecoverMetadataOfFaultyWorker(workerUuids, extraRanges);
        });
    LOG(INFO) << FormatString("[%s] Initialize success", LogPrefix());
    return Status::OK();
}

bool SCMetadataManager::MetaIsFound(const std::string &streamName)
{
    ReadLockHelper rlocker(LOCK_ARGS_MSG(metaDictMutex_, streamName));
    TbbMetaHashmap::const_accessor accessor;
    return streamMetaManagerDict_.find(accessor, streamName);
}

bool SCMetadataManager::CheckMetaTableEmpty()
{
    ReadLockHelper rlocker(LOCK_ARGS(metaDictMutex_));
    return streamMetaManagerDict_.empty();
}

void SCMetadataManager::GetMetasMatch(std::function<bool(const std::string &)> &&matchFunc,
                                      std::vector<std::string> &streamNames, bool *exitEarly)
{
    int timeoutSec = 300;
    INJECT_POINT("SCMetadataManager.GetMetasMatch.timeout", [&timeoutSec] (int timeout) {
        timeoutSec = timeout;
    });
    WriteLockHelper wlocker(DEFER_LOCK_ARGS(metaDictMutex_));
    if (!wlocker.TryLock(timeoutSec)) {
        LOG(WARNING) << "[GetMetasMatch] Failed to acquire lock within " << timeoutSec
                     << " seconds, aborting metadata matching operation";
        return;
    }
    for (const auto &it : streamMetaManagerDict_) {
        if (exitEarly && *exitEarly) {
            break;
        }
        if (it.first.empty()) {
            LOG(ERROR) << "[GetMetasMatch] stream name is empty!";
            continue;
        }
        if (matchFunc(it.first)) {
            streamNames.emplace_back(it.first);
        }
    }
}

Status SCMetadataManager::SaveMigrationMetadata(const MigrateSCMetadataReqPb &req, MigrateSCMetadataRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(etcdCM_->CheckReceiveMigrateInfo(), K_NOT_READY,
                             "wait and retry, worker don't receive addnode info");
    LOG(INFO) << "Recv migrate metadata msg. source:" << req.source_addr()
              << ", stream count:" << req.stream_metas().size();

    auto injectTest = []() {
        INJECT_POINT("master.sc.fail_save_migration_data", []() { return true; });
        return false;
    };
    Status status;
    for (auto &streamMeta : req.stream_metas()) {
        if (injectTest()) {
            rsp.add_results(MigrateSCMetadataRspPb::FAILED);
            continue;
        }

        if (SaveMigrationData(streamMeta, status, rsp).IsError()) {
            rsp.add_results(MigrateSCMetadataRspPb::FAILED);
            continue;
        }
        rsp.add_results(MigrateSCMetadataRspPb::SUCCESSFUL);
    }
    return Status::OK();
}

Status SCMetadataManager::SaveMigrationData(const MetaForSCMigrationPb &streamMeta, Status &status,
                                            MigrateSCMetadataRspPb &rsp)
{
    (void)status;
    (void)rsp;
    ReadLockHelper rlocker(LOCK_ARGS(metaDictMutex_));
    const auto &meta = streamMeta.meta();
    const std::string &streamName = meta.stream_name();
    StreamFields streamFields(meta.max_stream_size(), meta.page_size(), meta.auto_cleanup(), meta.retain_num_consumer(),
                              meta.encrypt_stream(), meta.reserve_size(), meta.stream_mode());
    bool needRevert = true;
    // clang-format off
    CHECK_FAIL_RETURN_STATUS(streamMetaManagerDict_.emplace(
        streamName, std::make_shared<StreamMetadata>(streamName, streamFields, streamMetaStore_.get(),
        akSkManager_, rpcSessionManager_, etcdCM_, notifyWorkerManager_.get())),
        StatusCode::K_RUNTIME_ERROR, "Load meta reconstruction insertion failed");
    // clang-format on
    TbbMetaHashmap::accessor accessor;
    RETURN_IF_NOT_OK(GetStreamMetadata(streamName, accessor));
    RaiiPlus raiiP([this, &needRevert, &accessor]() {
        if (needRevert) {
            streamMetaManagerDict_.erase(accessor);
        }
    });
    StreamMetadata *metadata = accessor->second.get();
    CHECK_FAIL_RETURN_STATUS(metadata != nullptr, K_RUNTIME_ERROR, "metadata is null");
    if (ScMetricsMonitor::Instance()->IsEnabled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            metadata->InitStreamMetrics(),
            FormatString("[%s, S:%s] Init master sc metrics failed", LogPrefix(), streamName));
    }
    std::vector<std::string> producerRelatedNodes = { streamMeta.producer_rel_nodes().begin(),
                                                      streamMeta.producer_rel_nodes().end() };
    std::vector<std::string> consumerRelatedNodes = { streamMeta.consumer_rel_nodes().begin(),
                                                      streamMeta.consumer_rel_nodes().end() };
    metadata->PreparePubSubRelNodes(producerRelatedNodes, consumerRelatedNodes);
    // Make use of recovery logic so no notification is sent.
    for (const auto &producerMetaPb : streamMeta.producers()) {
        RETURN_IF_NOT_OK(metadata->RecoveryPubMeta(producerMetaPb));
        RETURN_IF_NOT_OK(streamMetaStore_->AddPubNode(producerMetaPb));
        raiiP.AddTask([this, &needRevert, &producerMetaPb]() {
            if (needRevert) {
                LOG_IF_ERROR(streamMetaStore_->DelPubNode(producerMetaPb),
                             "Rollback persisted producer failed in migration failure.");
            }
        });
    }
    for (const auto &consumerMetaPb : streamMeta.consumers()) {
        RETURN_IF_NOT_OK(metadata->RecoverySubMeta(consumerMetaPb));
        RETURN_IF_NOT_OK(streamMetaStore_->AddSubNode(consumerMetaPb));
        raiiP.AddTask([this, &needRevert, &consumerMetaPb]() {
            if (needRevert) {
                LOG_IF_ERROR(streamMetaStore_->DelSubNode(consumerMetaPb.stream_name(), consumerMetaPb.consumer_id()),
                             "Rollback persisted consumer failed in migration failure.");
            }
        });
    }
    // Recover the lifetime consumer count and retain data state.
    const auto &consumerLifeCount = meta.consumer_life_count();
    RETURN_IF_NOT_OK(metadata->RestoreConsumerLifeCount(consumerLifeCount));
    auto currentState = metadata->CheckNUpdateNeedRetainData();
    VLOG(SC_NORMAL_LOG_LEVEL) << "[RetainData] RetainData state is restored for stream: " << streamName << " to "
                              << currentState;

    RETURN_IF_NOT_OK(streamMetaStore_->AddStream(streamName, streamFields));
    raiiP.AddTask([this, &needRevert, &streamName]() {
        if (needRevert) {
            LOG_IF_ERROR(streamMetaStore_->DelStream(streamName),
                         "Rollback persisted stream failed in migration failure.");
        }
    });
    RETURN_IF_NOT_OK(streamMetaStore_->UpdateLifeTimeConsumerCount(streamName, consumerLifeCount));
    // Migrate the async notifications.
    RETURN_IF_NOT_OK(notifyWorkerManager_->AddAsyncNotifications(streamFields, streamName, streamMeta));
    // If auto clean up is true and there is no more producer/consumer, delete the stream.
    RETURN_IF_NOT_OK(metadata->AutoCleanupIfNeeded(HostPort()));
    needRevert = false;
    return Status::OK();
}

Status SCMetadataManager::FillMetadataForMigration(const std::string &streamName, MetaForSCMigrationPb *meta)
{
    TbbMetaHashmap::accessor accessor;
    RETURN_IF_NOT_OK(GetStreamMetadata(streamName, accessor));
    // Marker is migrating
    migratingItems_.insert({ streamName, true });

    // Fill metadata
    StreamMetadata *metadata = accessor->second.get();
    StreamMetaPb *streamMetaPb = meta->mutable_meta();
    streamMetaPb->set_stream_name(streamName);
    const auto &streamFields = metadata->GetStreamFields();
    streamMetaPb->set_max_stream_size(streamFields.maxStreamSize_);
    streamMetaPb->set_page_size(streamFields.pageSize_);
    streamMetaPb->set_auto_cleanup(streamFields.autoCleanup_);
    streamMetaPb->set_retain_num_consumer(streamFields.retainForNumConsumers_);
    streamMetaPb->set_encrypt_stream(streamFields.encryptStream_);
    streamMetaPb->set_reserve_size(streamFields.reserveSize_);
    streamMetaPb->set_stream_mode(streamFields.streamMode_);
    streamMetaPb->set_consumer_life_count(metadata->GetConsumerLifeCount());

    std::vector<std::string> producerRelatedNodes;
    std::vector<std::string> consumerRelatedNodes;
    std::vector<ProducerMetaPb> masterProducers;
    std::vector<ConsumerMetaPb> masterConsumers;
    metadata->GetAllProducerConsumer(masterProducers, masterConsumers, producerRelatedNodes, consumerRelatedNodes);
    *meta->mutable_producers() = { masterProducers.begin(), masterProducers.end() };
    *meta->mutable_consumers() = { masterConsumers.begin(), masterConsumers.end() };
    *meta->mutable_producer_rel_nodes() = { producerRelatedNodes.begin(), producerRelatedNodes.end() };
    *meta->mutable_consumer_rel_nodes() = { consumerRelatedNodes.begin(), consumerRelatedNodes.end() };
    std::vector<AsyncNotificationPb> notifications;
    notifyWorkerManager_->GetPendingNotificationByStreamName(streamName, notifications);
    *meta->mutable_notifications() = { notifications.begin(), notifications.end() };
    return Status::OK();
}

void SCMetadataManager::HandleMetaDataMigrationSuccess(const std::string &streamName)
{
    Raii outer([this, &streamName]() { migratingItems_.erase(streamName); });

    auto func = [this, &streamName]() {
        TbbMetaHashmap::accessor accessor;
        ReadLockHelper rlocker(LOCK_ARGS_MSG(metaDictMutex_, streamName));
        RETURN_IF_NOT_OK(GetStreamMetadataNoLock(streamName, accessor));
        // Cleanup rocksdb after migration, so it will not be reloaded after a restart
        StreamMetadata *metadata = accessor->second.get();
        RETURN_IF_NOT_OK(metadata->CleanUpStreamPersistent(streamName));
        // Also cleanup the notifications since they are also migrated already
        RETURN_IF_NOT_OK(notifyWorkerManager_->RemovePendingNotificationByStreamName(streamName));
        CHECK_FAIL_RETURN_STATUS(streamMetaManagerDict_.erase(accessor) == 1, StatusCode::K_NOT_FOUND,
                                 FormatString("Stream <%s> does not exist on master", streamName));
        return Status::OK();
    };
    LOG_IF_ERROR(func(), "");
}

void SCMetadataManager::HandleMetaDataMigrationFailed(const MetaForSCMigrationPb &streamMeta)
{
    (void)streamMeta;
    // Placeholder, do nothing
}

Status SCMetadataManager::CreateProducer(const CreateProducerReqPb &req, CreateProducerRspPb &rsp)
{
    INJECT_POINT("SCMetadataManager.CreateProducer.wait");
    const auto &producerMeta = req.producer_meta();
    const std::string &streamName = producerMeta.stream_name();
    bool redirect = req.redirect();
    FillRedirectResponseInfo(rsp, streamName, redirect);
    RETURN_OK_IF_TRUE(redirect);
    StreamFields streamFields(req.max_stream_size(), req.page_size(), req.auto_cleanup(), req.retain_num_consumer(),
                              req.encrypt_stream(), req.reserve_size(), req.stream_mode());
    // Hold const_accessor to allow parallel CreateProducer.
    TbbMetaHashmap::const_accessor accessor;
    Status rc = GetStreamMetadata(streamName, accessor);
    if (rc.GetCode() == K_NOT_FOUND) {
        RETURN_IF_NOT_OK(CreateStreamMetadata(streamName));
        RETURN_IF_NOT_OK(GetStreamMetadata(streamName, accessor));
    }
    StreamMetadata *metadata = accessor->second.get();
    RETURN_IF_NOT_OK(VerifyStreamMode(static_cast<StreamMode>(req.stream_mode()), metadata->GetConsumerCount(),
                                      metadata->GetProducerCount() + 1));
    RETURN_IF_NOT_OK(metadata->PubIncreaseNode(producerMeta, streamFields));
    LOG(INFO) << FormatString("[%s, S:%s, W:%s] Create producer on master success", LogPrefix(), streamName,
                              HostPb2Str(producerMeta.worker_address()));
    return Status::OK();
}

bool SCMetadataManager::HandleCloseProducerError(bool &firstError, const Status &rc, const ProducerInfoPb &info,
                                                 CloseProducerRspPb &rsp)
{
    if (rc.IsError()) {
        LOG(INFO) << FormatString("[%s, S:%s ] CloseProducer failed on master: %s", LogPrefix(), info.stream_name(),
                                  rc.ToString());
        INJECT_POINT("master.sc.close_producer_error", []() { return true; });
        if (firstError) {
            // The first time that an error is hit, we save it into the response struct.
            rsp.mutable_err()->set_error_code(rc.GetCode());
            rsp.mutable_err()->set_error_msg(rc.GetMsg());
            firstError = false;
        }
        // Add this producer to the failed list
        auto failedProducerPb = rsp.add_failed_producers();
        failedProducerPb->CopyFrom(info);
        return true;
    }
    return false;
}

Status SCMetadataManager::CloseProducer(const CloseProducerReqPb &req, CloseProducerRspPb &rsp)
{
    INJECT_POINT("SCMetadataManager.CloseProducer.wait");
    int numProducers = req.producer_infos_size();
    VLOG(SC_NORMAL_LOG_LEVEL) << "Starting to close " << numProducers << " producers in master from worker "
                              << HostPb2Str(req.worker_address());
    const bool forceClose = req.force_close();
    int numSuccess = 0;
    bool firstError = true;
    // Check redirect for producers
    std::vector<std::string> streams;
    for (const ProducerInfoPb &currProducer : req.producer_infos()) {
        streams.emplace_back(currProducer.stream_name());
    }
    bool redirect = req.redirect();
    FillRedirectResponseInfos(rsp, streams, redirect);
    // For now the close producer request is grouped by stream name, so if redirect is needed for any,
    // all of them will need the redirect, otherwise it will be empty.
    RETURN_OK_IF_TRUE(streams.empty());
    // For producer that does not need redirect, drive close logic against them.
    // Any failed producers are tracked, and then return to the caller.
    for (const ProducerInfoPb &currProducer : req.producer_infos()) {
        const std::string &streamName = currProducer.stream_name();

        // Hold const_accessor to allow parallel CloseProducer.
        TbbMetaHashmap::const_accessor accessor;
        Status rc = GetStreamMetadata(streamName, accessor);
        if (HandleCloseProducerError(firstError, rc, currProducer, rsp)) {
            continue;  // loop to next producer in the list. This one failed.
        }

        ProducerMetaPb producerMeta;
        producerMeta.set_stream_name(currProducer.stream_name());
        producerMeta.mutable_worker_address()->CopyFrom(req.worker_address());
        StreamMetadata *metadata = accessor->second.get();

        rc = metadata->PubDecreaseNode(producerMeta, forceClose);
        if (HandleCloseProducerError(firstError, rc, currProducer, rsp)) {
            continue;  // loop to next producer in the list. This one failed.
        }
        ++numSuccess;
        auto successProducerPb = rsp.add_success_producers();
        successProducerPb->CopyFrom(currProducer);
        VLOG(SC_NORMAL_LOG_LEVEL) << "Producer for stream " << currProducer.stream_name()
                                  << " successfully closed on master.";
    }

    VLOG(SC_NORMAL_LOG_LEVEL) << "Finished closing producers in master. Num successful: " << numSuccess
                              << ". Num failed: " << rsp.failed_producers_size();

    // Always return success. The error code for any failures is packed into the rsp structure that the sending side
    // must unpack.
    return Status::OK();
}

Status SCMetadataManager::Subscribe(const SubscribeReqPb &req, SubscribeRspPb &rsp)
{
    INJECT_POINT("SCMetadataManager.Subscribe.wait");
    CHECK_FAIL_RETURN_STATUS(req.has_consumer_meta(), StatusCode::K_RUNTIME_ERROR,
                             "Runtime error in get consumer_meta");
    const auto &consumerMeta = req.consumer_meta();
    const auto &streamName = consumerMeta.stream_name();
    bool redirect = req.redirect();
    FillRedirectResponseInfo(rsp, streamName, redirect);
    RETURN_OK_IF_TRUE(redirect);

    // Accessor as write lock for this stream.
    // A procedure lock for the stream.
    TbbMetaHashmap::accessor accessor;
    RETURN_IF_NOT_OK(CreateOrGetStreamMetadata(streamName, accessor));
    StreamMetadata *metadata = accessor->second.get();
    RETURN_IF_NOT_OK(VerifyStreamMode(metadata->GetStreamFields().streamMode_, metadata->GetConsumerCount() + 1,
                                      metadata->GetProducerCount()));
    RETURN_IF_NOT_OK(metadata->SubIncreaseNode(consumerMeta));
    const StreamFields &streamFields = metadata->GetStreamFields();
    rsp.set_max_stream_size(streamFields.maxStreamSize_);
    rsp.set_page_size(streamFields.pageSize_);
    rsp.set_auto_cleanup(streamFields.autoCleanup_);
    rsp.set_retain_num_consumer(streamFields.retainForNumConsumers_);
    rsp.set_retain_data(metadata->CheckNUpdateNeedRetainData());
    rsp.set_encrypt_stream(streamFields.encryptStream_);
    rsp.set_reserve_size(streamFields.reserveSize_);
    rsp.set_stream_mode(streamFields.streamMode_);
    LOG(INFO) << FormatString("[%s, S:%s, C:%s, W:%s] Create consumer on master success", LogPrefix(), streamName,
                              consumerMeta.consumer_id(), HostPb2Str(req.consumer_meta().worker_address()));
    return Status::OK();
}

Status SCMetadataManager::CloseConsumer(const CloseConsumerReqPb &req, CloseConsumerRspPb &rsp)
{
    INJECT_POINT("SCMetadataManager.CloseConsumer.wait");
    (void)rsp;
    CHECK_FAIL_RETURN_STATUS(req.has_consumer_meta(), StatusCode::K_RUNTIME_ERROR,
                             "Runtime error in get consumer_meta");
    const auto &consumerMeta = req.consumer_meta();
    const auto &streamName = consumerMeta.stream_name();
    bool redirect = req.redirect();
    FillRedirectResponseInfo(rsp, streamName, redirect);
    RETURN_OK_IF_TRUE(redirect);

    // Accessor as write lock for this stream.
    // A procedure lock for the stream.
    TbbMetaHashmap::accessor accessor;
    RETURN_IF_NOT_OK(GetStreamMetadata(streamName, accessor));
    StreamMetadata *metadata = accessor->second.get();
    RETURN_IF_NOT_OK(metadata->SubDecreaseNode(consumerMeta));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s, C:%s, W:%s] Close consumer on master success", LogPrefix(),
                                              streamName, consumerMeta.consumer_id(),
                                              HostPb2Str(req.consumer_meta().worker_address()));
    return Status::OK();
}

Status SCMetadataManager::SendDeleteStreamReqToWorker(const std::string &streamName, const HostPort workerNode)
{
    std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
    RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(workerNode, masterWorkerApi, akSkManager_));
    RETURN_IF_NOT_OK_EXCEPT(masterWorkerApi->DelStreamContextBroadcast(streamName, false),
                            StatusCode::K_SC_STREAM_NOT_FOUND);
    return Status::OK();
}

Status SCMetadataManager::SendDeleteStreamReq(const std::string &streamName, std::set<HostPort> &relatedWorkerSet)
{
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Sending DelStreamContextBroadcast request to %d workers",
                                              LogPrefix(), streamName, relatedWorkerSet.size());
    std::unordered_map<std::shared_ptr<MasterWorkerSCApi>, int64_t> tagIds;
    bool local = false;
    for (const auto &workerNode : relatedWorkerSet) {
        // Deal with local worker separately.
        if (workerNode == masterAddress_) {
            local = true;
            continue;
        } else if (CheckWorkerStatus(workerNode).IsError()) {
            // Avoid unlimited retry due to node lost.
            continue;
        }
        // Each related node maybe a worker which has not been recorded, so we should initialize rpc channel.
        int64_t tagId;
        std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
        RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(workerNode, masterWorkerApi, akSkManager_));
        RETURN_IF_NOT_OK(masterWorkerApi->DelStreamContextBroadcastAsyncWrite(streamName, false, tagId));
        tagIds.emplace(masterWorkerApi, tagId);
    }
    // Process the local bypass request to local worker in between AsyncWrite and AsyncRead for better time utilization.
    if (local) {
        RETURN_IF_NOT_OK(SendDeleteStreamReqToWorker(streamName, masterAddress_));
    }
    for (const auto &pair : tagIds) {
        RETURN_IF_NOT_OK_EXCEPT(pair.first->DelStreamContextBroadcastAsyncRead(pair.second, RpcRecvFlags::NONE),
                                StatusCode::K_SC_STREAM_NOT_FOUND);
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] DelStreamContextBroadcast requests are done", LogPrefix(),
                                              streamName);
    return Status::OK();
}

Status SCMetadataManager::DeleteStream(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp)
{
    INJECT_POINT("SCMetadataManager.DeleteStream.sleep");
    (void)rsp;
    const std::string &streamName = req.stream_name();
    bool redirect = req.redirect();
    FillRedirectResponseInfo(rsp, streamName, redirect);
    RETURN_OK_IF_TRUE(redirect);
    HostPort srcWorkerAddr;
    HostPb2Host(req.src_node_addr(), srcWorkerAddr);
    std::set<HostPort> relatedWorkerSet;
    bool decrementRef = false;
    Raii deleter([this, streamName, &decrementRef]() {
        TbbMetaHashmap::const_accessor accessor;
        Status rc = GetStreamMetadata(streamName, accessor);
        if (rc.IsOk()) {
            // undo delete if stream still exists
            StreamMetadata *metadata = accessor->second.get();
            RETURN_RUNTIME_ERROR_IF_NULL(metadata);
            metadata->UndoDeleteStream(decrementRef);
        }
        // ignore K_NOT_FOUND error as stream might have deleted
        return Status::OK();
    });

    // Accessor as write lock for this stream.
    // A procedure lock for the stream.
    // Only change local metadata under lock.
    {
        TbbMetaHashmap::accessor accessor;
        RETURN_IF_NOT_OK(GetStreamMetadata(streamName, accessor));
        StreamMetadata *metadata = accessor->second.get();
        RETURN_RUNTIME_ERROR_IF_NULL(metadata);
        RETURN_IF_NOT_OK(metadata->DeleteStreamStart(srcWorkerAddr, relatedWorkerSet));
        decrementRef = true;
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Started to Delete stream on master", LogPrefix(),
                                                  streamName);
    }

    // Send requests to all workers without a lock
    // here all other requests to the stream will be rejected (including another delete)
    // based on delete stream state we set above so need a lock to
    // prevent this
    INJECT_POINT("SCMetadataManager.DeleteStream.SendReqs");
    RETURN_IF_NOT_OK(SendDeleteStreamReq(streamName, relatedWorkerSet));
    INJECT_POINT("SCMetadataManager.DeleteStream.SentReqs");
    // We need lock here as someone else might be accessing streamMetaManagerDict_
    {
        TbbMetaHashmap::accessor accessor;
        ReadLockHelper rlocker(LOCK_ARGS_MSG(metaDictMutex_, streamName));
        RETURN_IF_NOT_OK(GetStreamMetadataNoLock(streamName, accessor));
        StreamMetadata *metadata = accessor->second.get();
        RETURN_RUNTIME_ERROR_IF_NULL(metadata);
        RETURN_IF_NOT_OK(metadata->DeleteStreamEnd());
        CHECK_FAIL_RETURN_STATUS(streamMetaManagerDict_.erase(accessor) == 1, StatusCode::K_NOT_FOUND,
                                 FormatString("Stream <%s> does not exist on master", streamName));
    }

    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Delete stream on master success", LogPrefix(), streamName);
    return Status::OK();
}

Status SCMetadataManager::QueryGlobalProducersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    const auto &streamName = req.stream_name();
    bool redirect = req.redirect();
    FillRedirectResponseInfo(rsp, streamName, redirect);
    RETURN_OK_IF_TRUE(redirect);
    // Accessor as read lock for this stream.
    // A procedure lock for the stream.
    TbbMetaHashmap::const_accessor accessor;
    Status rc = GetStreamMetadata(streamName, accessor);
    RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
    RETURN_IF_NOT_OK(rc);
    StreamMetadata *metadata = accessor->second.get();
    auto count = metadata->GetProducerCount();
    rsp.set_global_count(count);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] The global producer number is %ld", LogPrefix(), streamName,
                                              count);
    return Status::OK();
}

Status SCMetadataManager::QueryGlobalConsumersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    const auto &streamName = req.stream_name();
    bool redirect = req.redirect();
    FillRedirectResponseInfo(rsp, streamName, redirect);
    RETURN_OK_IF_TRUE(redirect);
    // Accessor as read lock for this stream.
    // A procedure lock for the stream.
    TbbMetaHashmap::const_accessor accessor;
    Status rc = GetStreamMetadata(streamName, accessor);
    RETURN_OK_IF_TRUE(rc.GetCode() == StatusCode::K_NOT_FOUND);
    RETURN_IF_NOT_OK(rc);
    StreamMetadata *metadata = accessor->second.get();
    auto count = metadata->GetConsumerCount();
    rsp.set_global_count(count);
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] The global consumer number is %ld", LogPrefix(), streamName,
                                              count);
    return Status::OK();
}

std::string SCMetadataManager::LogPrefix() const
{
    return FormatString("MasterSvc, Node:%s", masterAddress_.ToString());
}

Status SCMetadataManager::CreateOrGetStreamMetadata(const std::string &streamName, TbbMetaHashmap::accessor &accessor)
{
    ReadLockHelper rlocker(LOCK_ARGS_MSG(metaDictMutex_, streamName));

    // Accessor work as a procedure lock of a stream.
    bool isFirst = streamMetaManagerDict_.insert(accessor, streamName);
    if (isFirst) {
        StreamFields streamFields(0, 0, false, 0, false, 0,
                                  StreamMode::MPMC);  // empty stream fields to start for a new entry
        // If this stream just created on master, we save it into rocksdb
        auto status = streamMetaStore_->AddStream(streamName, streamFields);
        if (status.IsError()) {
            // Allow stream can be recreated correctly.
            (void)streamMetaManagerDict_.erase(accessor);
            return status;
        }
        // Initialize consumer count for the stream
        status = streamMetaStore_->UpdateLifeTimeConsumerCount(streamName, 0);
        if (status.IsError()) {
            // Rollback previous changes
            streamMetaStore_->DelStream(streamName);
            // Allow stream can be recreated correctly.
            (void)streamMetaManagerDict_.erase(accessor);
            return status;
        }
        accessor->second =
            std::make_shared<StreamMetadata>(streamName, streamFields, streamMetaStore_.get(), akSkManager_,
                                             rpcSessionManager_, etcdCM_, notifyWorkerManager_.get());
        if (ScMetricsMonitor::Instance()->IsEnabled()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                accessor->second->InitStreamMetrics(),
                FormatString("[%s, S:%s] Init master sc metrics failed", LogPrefix(), streamName));
        }
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Create new StreamMetaManager success", LogPrefix(),
                                                  streamName);
    } else {
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] StreamMetaManager already exists", LogPrefix(),
                                                  streamName);
    }
    return Status::OK();
}

Status SCMetadataManager::CreateStreamMetadata(const std::string &streamName)
{
    TbbMetaHashmap::accessor accessor;
    INJECT_POINT("SCMetadataManager.CreateStreamMetadata", [this, &accessor](const std::string &stream) {
        (void)CreateOrGetStreamMetadata(stream, accessor);
        return Status::OK();
    });
    RETURN_IF_NOT_OK(CreateOrGetStreamMetadata(streamName, accessor));
    return Status::OK();
}

Status SCMetadataManager::LoadMeta()
{
    LOG(INFO) << "Start to load meta data from rocksdb into memory";
    INJECT_POINT("master.SCMetadataManager.LoadMeta");
    std::vector<StreamMetaPb> streamMetas;
    RETURN_IF_NOT_OK(streamMetaStore_->GetAllStream(streamMetas));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Total stream number:<%d>", streamMetas.size());
    for (const auto &meta : streamMetas) {
        const auto &streamName = meta.stream_name();
        // No need to have lock here, because we are not provide rpc service to others before we finish.
        StreamFields streamFields(meta.max_stream_size(), meta.page_size(), meta.auto_cleanup(),
                                  meta.retain_num_consumer(), meta.encrypt_stream(), meta.reserve_size(),
                                  meta.stream_mode());

        // clang-format off
        CHECK_FAIL_RETURN_STATUS(streamMetaManagerDict_.emplace(
            streamName, std::make_shared<StreamMetadata>(streamName, streamFields, streamMetaStore_.get(),
            akSkManager_, rpcSessionManager_, etcdCM_, notifyWorkerManager_.get())),
            StatusCode::K_RUNTIME_ERROR, "Load meta reconstruction insertion failed");
        // clang-format on
        TbbMetaHashmap::accessor accessor;
        RETURN_IF_NOT_OK(GetStreamMetadata(streamName, accessor));
        StreamMetadata *metadata = accessor->second.get();
        CHECK_FAIL_RETURN_STATUS(metadata != nullptr, K_RUNTIME_ERROR, "metadata is null");
        if (ScMetricsMonitor::Instance()->IsEnabled()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                metadata->InitStreamMetrics(),
                FormatString("[%s, S:%s] Init master sc metrics failed", LogPrefix(), streamName));
        }
        std::vector<ProducerMetaPb> producerMetaPbVector;
        std::vector<ConsumerMetaPb> consumerMetaPbVector;
        RETURN_IF_NOT_OK(streamMetaStore_->GetOneStreamProducers(streamName, producerMetaPbVector));
        RETURN_IF_NOT_OK(streamMetaStore_->GetOneStreamConsumers(streamName, consumerMetaPbVector));
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Stream:<%s>, Pub Worker number:<%d>, Consumer number:<%d>",
                                                  streamName, producerMetaPbVector.size(), consumerMetaPbVector.size());

        for (const auto &producerMetaPb : producerMetaPbVector) {
            RETURN_IF_NOT_OK(metadata->RecoveryPubMeta(producerMetaPb));
        }
        for (const auto &consumerMetaPb : consumerMetaPbVector) {
            // We assume only master restart, so the topo on worker does not need to sync.
            RETURN_IF_NOT_OK(metadata->RecoverySubMeta(consumerMetaPb));
        }

        // Recover the consumer count
        uint32_t consumerLifeCount = 0;
        WARN_IF_ERROR(streamMetaStore_->GetLifeTimeConsumerCount(streamName, consumerLifeCount),
                      "Reading an older version of Metadata, without ConsumerLifeCount field.");
        RETURN_IF_NOT_OK(metadata->RestoreConsumerLifeCount(consumerLifeCount));

        // Recover the Retain data state
        auto currentState = metadata->CheckNUpdateNeedRetainData();
        VLOG(SC_NORMAL_LOG_LEVEL) << "[RetainData] RetainData state is restored for stream: " << streamName << " to "
                                  << currentState;
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Stream:<%s>, Status:<Load meta data success>", streamName);
    }
    LOG(INFO) << "Recovery meta data into memory success";
    return Status::OK();
}

std::vector<std::shared_ptr<StreamMetadata>> SCMetadataManager::GetStreamMetaByWorkerAddr(const HostPort &workerAddr,
                                                                                          bool clearMeta)
{
    std::vector<std::shared_ptr<StreamMetadata>> streamMetadatas;
    WriteLockHelper wlocker(LOCK_ARGS(metaDictMutex_));
    for (const auto &streamMetadata : streamMetaManagerDict_) {
        const auto &workerAddress = workerAddr.ToString();
        INJECT_POINT_NO_RETURN("SCMetadataManager.SkipClearEmptyMeta", [&clearMeta]() { clearMeta = false; });
        if (clearMeta) {
            streamMetadata.second->ClearEmptyMeta(workerAddress);
        }
        if (streamMetadata.second->CheckWorkerExistsPubSub(workerAddr.ToString())) {
            streamMetadatas.push_back(streamMetadata.second);
        }
    }
    return streamMetadatas;
}

std::vector<std::shared_ptr<StreamMetadata>> SCMetadataManager::GetAllStreamMeta()
{
    std::vector<std::shared_ptr<StreamMetadata>> streamMetadatas;
    WriteLockHelper wlocker(LOCK_ARGS(metaDictMutex_));
    for (const auto &streamMetadata : streamMetaManagerDict_) {
        streamMetadatas.push_back(streamMetadata.second);
    }
    return streamMetadatas;
}

Status SCMetadataManager::ClearWorkerMetadata(const HostPort &workerAddr, const bool forceClose)
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    LOG(INFO) << "Start clear metadata from " << workerAddr.ToString() << " ,forceClose " << forceClose;

    // Clear the existing task.
    RETURN_IF_NOT_OK(notifyWorkerManager_->ClearPendingNotification(workerAddr.ToString()));

    bool clearMeta = true;
    std::vector<std::shared_ptr<StreamMetadata>> streamMetas = GetStreamMetaByWorkerAddr(workerAddr, clearMeta);

    Status status;
    for (auto &streamMeta : streamMetas) {
        LOG(INFO) << FormatString("ClearWorkerMetadata for stream [%s]", streamMeta->GetStreamName());
        auto tmpRc = streamMeta->ClearWorkerMetadata(workerAddr, forceClose);
        if (tmpRc.IsError() && tmpRc.GetCode() != K_NOT_FOUND) {
            LOG(ERROR) << FormatString("Clear worker meta for stream [%s] failed", streamMeta->GetStreamName());
            status = std::move(tmpRc);
        }
    }
    LOG(INFO) << "Metadata cleared for " << workerAddr.ToString();
    return status;
}

Status SCMetadataManager::CheckMetadata(const std::vector<HostPort> &workerAddrs, const worker::HashRange &hashRanges)
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    Status lastRc;
    std::unordered_set<std::string> streamsMayNeedAutoDelete;
    for (const auto &workerAddr : workerAddrs) {
        auto rc = CheckMetadataImpl(workerAddr, hashRanges, streamsMayNeedAutoDelete);
        if (rc.IsError()) {
            LOG(ERROR) << "CheckMetadata for " << workerAddr << " failed, detail: " << rc.ToString();
            lastRc = std::move(rc);
        }
    }
    // Now that we have complete metadata, we need to do some cleanup.
    LOG(INFO) << "streamsMayNeedAutoDelete: " << VectorToString(streamsMayNeedAutoDelete);
    PostCheckMetadata(streamsMayNeedAutoDelete);
    return lastRc;
}

Status SCMetadataManager::CheckMetadataImpl(const HostPort &workerAddr, const worker::HashRange &hashRanges,
                                            std::unordered_set<std::string> &streamsMayNeedAutoDelete)
{
    LOG(INFO) << "Started CheckMetadata with worker: " << workerAddr;
    std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
    RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(workerAddr, masterWorkerApi, akSkManager_));
    auto *localApi = dynamic_cast<MasterLocalWorkerSCApi *>(masterWorkerApi.get());
    GetMetadataAllStreamReqPb req;
    std::vector<GetStreamMetadataRspPb> metaRsp;
    bool isPassiveScaleDown = !hashRanges.empty();
    if (!isPassiveScaleDown) {
        req.set_master_address(masterAddress_.ToString());
    }
    for (const auto &range : hashRanges) {
        GetMetadataAllStreamReqPb::RangePb rangePb;
        rangePb.set_from(range.first);
        rangePb.set_end(range.second);
        req.mutable_hash_ranges()->Add(std::move(rangePb));
    }
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    if (localApi) {
        GetMetadataAllStreamRspPb rsp;
        RETURN_IF_NOT_OK_APPEND_MSG(localApi->QueryMetadata(req, rsp), "QueryMetadata send to worker failed");
        metaRsp.insert(metaRsp.end(), rsp.stream_meta().begin(), rsp.stream_meta().end());
    } else {
        // non-local worker, use the stream api for this query
        std::unique_ptr<ClientWriterReader<GetMetadataAllStreamReqPb, GetStreamMetadataRspPb>> stream;
        INJECT_POINT("worker.MasterRemoteWorkerSCApi.QueryMetadata");
        RETURN_IF_NOT_OK_APPEND_MSG(masterWorkerApi->QueryMetadata(stream), "QueryMetadata send to worker failed");
        RETURN_IF_NOT_OK(stream->Write(req));
        Status rc;
        do {
            GetStreamMetadataRspPb rsp;
            rc = stream->Read(rsp);
            if (rc.IsOk()) {
                metaRsp.emplace_back(rsp);
            }
        } while (rc.IsOk());
        CHECK_FAIL_RETURN_STATUS(rc.GetCode() == K_RPC_STREAM_END, rc.GetCode(), rc.GetMsg());
        LOG_IF_ERROR(stream->Finish(), "Closing of stream rpc failed in master");
    }
    RETURN_IF_NOT_OK_APPEND_MSG(UpdateMetadata(metaRsp, workerAddr, streamsMayNeedAutoDelete, hashRanges),
                                "UpdateMetadata failed");
    LOG(INFO) << "Finish CheckMetadata with " << workerAddr;
    return Status::OK();
}

Status SCMetadataManager::UpdateMetadata(std::vector<GetStreamMetadataRspPb> &metaRsp, const HostPort &workerAddr,
                                         std::unordered_set<std::string> &streamsMayNeedAutoDelete,
                                         const worker::HashRange &hashRanges)
{
    Status status;
    std::vector<std::string> receivedStreams;
    for (const auto &meta : metaRsp) {
        const auto &streamName = meta.stream_name();
        receivedStreams.push_back(streamName);
        StatusCode code = static_cast<StatusCode>(meta.error().error_code());
        if (code != StatusCode::K_OK) {
            status = Status(code, meta.error().error_msg());
            LOG(ERROR) << "Master failed to receive meta for stream: " << streamName
                       << " from worker: " << workerAddr.ToString() << " with message: " << status.GetMsg();
            continue;
        }
        UpdateSetByCondition(streamName, meta.producers().empty() && meta.consumers().empty(),
                             streamsMayNeedAutoDelete);
        TbbMetaHashmap::accessor accessor;
        status = CreateOrGetStreamMetadata(streamName, accessor);
        if (status.IsError()) {
            continue;
        }
        StreamMetadata *metadata = accessor->second.get();
        status = metadata->CheckMetadata(meta, workerAddr);
    }
    // Clear metadata for streams not found in worker but master has metadata for worker.
    // Fixme: It is too wasteful to traverse all the streams. These streams should be classified according to the
    // granularity of the worker.
    WriteLockHelper wlocker(LOCK_ARGS(metaDictMutex_));
    for (const auto &streamMetadata : streamMetaManagerDict_) {
        const auto &streamName = streamMetadata.first;
        if ((hashRanges.empty() || etcdCM_->IsInRange(hashRanges, streamName, ""))
            && std::find(receivedStreams.begin(), receivedStreams.end(), streamName) == receivedStreams.end()) {
            auto rc = streamMetadata.second->ClearWorkerMetadata(workerAddr, false, false);
            LOG_IF_ERROR_EXCEPT(rc,
                                FormatString("ClearWorkerMetadata for stream[%s] on worker[%s] failed, detail: %s",
                                             streamMetadata.first, workerAddr.ToString(), rc.ToString()),
                                K_NOT_FOUND);
            if (rc.IsOk()) {
                streamsMayNeedAutoDelete.insert(streamMetadata.first);
            }
        }
    }
    return status;
}

void SCMetadataManager::CheckMetadataWithAsyncRetry(const HostPort &workerAddr, std::shared_ptr<Timer> timer,
                                                    size_t retryTimes)
{
    const int64_t secToMs = 1000;
    int64_t elapsedMs = static_cast<int64_t>(std::round(timer->ElapsedMilliSecond()));
    int64_t remaining = static_cast<int64_t>(FLAGS_node_dead_timeout_s) * secToMs - elapsedMs;
    if (remaining <= 0) {
        LOG(WARNING) << FormatString("CheckMetadata with %s timeout.", workerAddr.ToString());
        return;
    }
    Status rc = CheckMetadata({ workerAddr });
    if (IsRpcTimeoutOrTryAgain(rc)) {
        static std::vector<uint64_t> retryDelaySec = { 1, 2, 4, 8, 16, 32, 64 };
        uint64_t delaySec = retryTimes < retryDelaySec.size() ? retryDelaySec[retryTimes] : retryDelaySec.back();
        uint64_t delayMs = std::min<uint64_t>(remaining, delaySec * secToMs);

        auto traceID = Trace::Instance().GetTraceID();
        auto delayTask = [this, workerAddr, retryTimes, traceID, timer = std::move(timer), exitFlag = exitFlag_] {
            if (exitFlag->load()) {
                return;
            }
            asyncReconciliationPool_->Execute([this, workerAddr, retryTimes, traceID, timer = std::move(timer)] {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
                CheckMetadataWithAsyncRetry(workerAddr, std::move(timer), retryTimes + 1);
            });
        };
        TimerQueue::TimerImpl timerImpl;
        TimerQueue::GetInstance()->AddTimer(delayMs, delayTask, timerImpl);
    }
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("CheckMetadata with %s failed: %s", workerAddr.ToString(), rc.GetMsg());
    } else {
        LOG(INFO) << FormatString("CheckMetadata done with %s ", workerAddr.ToString());
    }
}

void SCMetadataManager::StartCheckMetadata(const HostPort &workerAddr)
{
    if (!EnableSCService()) {
        return;
    }
    if (!asyncReconciliationPool_) {
        LOG(WARNING) << "reconciliation pool not exist.";
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    asyncReconciliationPool_->Execute([this, workerAddr, traceID] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << FormatString("CheckMetadata starts with %s ", workerAddr.ToString());
        auto timer = std::make_shared<Timer>();
        CheckMetadataWithAsyncRetry(workerAddr, std::move(timer));
    });
}

void SCMetadataManager::StartClearWorkerMetadata(const HostPort &workerAddr)
{
    if (!EnableSCService()) {
        return;
    }
    if (!asyncReconciliationPool_) {
        LOG(WARNING) << "reconciliation pool not exist.";
        return;
    }
    auto traceID = Trace::Instance().GetTraceID();
    asyncReconciliationPool_->Execute([this, workerAddr, traceID] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        Status rc = ClearWorkerMetadata(workerAddr, true);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("ClearWorkerMetadata with %s failed: %s", workerAddr.ToString(), rc.GetMsg());
        }
    });
}

Status SCMetadataManager::RecoverMetadataOfFaultyWorker(const std::vector<std::string> &workerUuids,
                                                        const worker::HashRange &extraRanges)
{
    if (extraRanges.empty()) {
        LOG_IF(INFO, !workerUuids.empty()) << "Only supports RecoverMetadataOfFaultyWorker by hash ranges for SC";
        return Status::OK();
    }
    LOG(INFO) << "Start RecoverMetadataOfFaultyWorker by ranges";
    auto func = [this](const worker::HashRange &extraRanges) {
        std::vector<HostPort> nodeAddrs;
        RETURN_IF_NOT_OK(etcdCM_->GetNodeAddrListFromEtcd(nodeAddrs));
        RETURN_IF_NOT_OK(CheckMetadata(nodeAddrs, extraRanges));
        return Status::OK();
    };
    LOG_IF_ERROR(func(extraRanges), "RecoverMetadataOfFaultyWorker failed");
    LOG(INFO) << "Finish RecoverMetadataOfFaultyWorker";
    return Status::OK();
}

Status SCMetadataManager::CheckWorkerStatus(const HostPort &workerHostPort)
{
    if (etcdCM_ == nullptr) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_INVALID, "ETCD cluster manager is nullptr.");
    }
    auto rc = etcdCM_->CheckConnection(workerHostPort);
    if (rc.IsError()) {
        RETURN_STATUS_LOG_ERROR(K_WORKER_ABNORMAL,
                                FormatString("The Worker %s is abnormal.", workerHostPort.ToString()));
    }
    return rc;  // Status is OK
}

Status SCMetadataManager::VerifyStreamMode(StreamMode streamMode, size_t consumerNumAfterModify,
                                           size_t producerNumAfterModify)
{
    CHECK_FAIL_RETURN_STATUS(
        streamMode == StreamMode::MPMC || consumerNumAfterModify <= 1, K_INVALID,
        FormatString("There can be at most one consumer in this stream mode: %d. Consumer num after modify: %zd",
                     static_cast<int32_t>(streamMode), consumerNumAfterModify));
    CHECK_FAIL_RETURN_STATUS(
        streamMode != StreamMode::SPSC || producerNumAfterModify <= 1, K_INVALID,
        FormatString("There can be at most one producer in this stream mode: %d. Producer num after modify: %zd",
                     static_cast<int32_t>(streamMode), producerNumAfterModify));
    return Status::OK();
}

bool SCMetadataManager::CheckSCMetaExist(const HostPort &workerAddr)
{
    std::vector<std::shared_ptr<StreamMetadata>> streamMetas = GetStreamMetaByWorkerAddr(workerAddr);
    return !streamMetas.empty();
}

void SCMetadataManager::TriggerAutoDelActively(const std::unordered_set<std::string> &streamsMayNeedAutoDelete)
{
    for (const auto &stream : streamsMayNeedAutoDelete) {
        TbbMetaHashmap::accessor accessor;
        auto rc = GetStreamMetadata(stream, accessor);
        LOG_IF_ERROR_EXCEPT(rc, "TriggerAutoDelActively failed", K_NOT_FOUND);
        if (rc.IsError()) {
            continue;
        }
        StreamMetadata *metadata = accessor->second.get();
        LOG_IF_ERROR(metadata->AutoCleanupIfNeeded(HostPort()), "TriggerAutoDelActively failed");
    }
}
}  // namespace master
}  // namespace datasystem
