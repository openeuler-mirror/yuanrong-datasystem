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

#include "datasystem/master/stream_cache/sc_notify_worker_manager.h"

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/protos/master_stream.pb.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

namespace datasystem {
namespace master {
const size_t ASYNC_NOTIFY_THREAD_NUM = 8;
const size_t DELETE_STREAM_THREAD_NUM = 8;
SCNotifyWorkerManager::SCNotifyWorkerManager(std::shared_ptr<RocksStreamMetaStore> streamMetaStore,
                                             std::shared_ptr<AkSkManager> akSkManager,
                                             std::shared_ptr<RpcSessionManager> rpcSessionManager,
                                             EtcdClusterManager *cm, SCMetadataManager *scMetadataManager)
    : streamMetaStore_(std::move(streamMetaStore)),
      akSkManager_(std::move(akSkManager)),
      rpcSessionManager_(std::move(rpcSessionManager)),
      etcdCM_(cm),
      scMetadataManager_(scMetadataManager)
{
}

SCNotifyWorkerManager::~SCNotifyWorkerManager()
{
    LOG(INFO) << "Destroy SCNotifyWorkerManager.";
    if (!interruptFlag_) {
        Shutdown();
    }
}

Status SCNotifyWorkerManager::Init()
{
    LOG(INFO) << "Init SCNotifyWorkerManager";
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RecoverNotification(), "Recover notification for rocksdb failed.");
    notifyThreadPool_ = std::make_unique<ThreadPool>(1, ASYNC_NOTIFY_THREAD_NUM, "ScNotify");
    notifyFut_ = notifyThreadPool_->Submit(&SCNotifyWorkerManager::ProcessAsyncNotify, this);
    deleteThreadPool_ = std::make_unique<ThreadPool>(1, DELETE_STREAM_THREAD_NUM, "ScDelete");
    deleteFut_ = deleteThreadPool_->Submit(&SCNotifyWorkerManager::ProcessDeleteStreams, this);
    return Status::OK();
}

void SCNotifyWorkerManager::Shutdown()
{
    LOG(INFO) << "SCNotifyWorkerManager shutdown.";
    if (interruptFlag_.exchange(true)) {
        return;
    }
    cvLock_.Set();
    WARN_IF_ERROR(notifyFut_.get(), "");
    WARN_IF_ERROR(deleteFut_.get(), "");
    notifyThreadPool_.reset();
    deleteThreadPool_.reset();
}

Status SCNotifyWorkerManager::ProcessAsyncNotify()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "Starting async notify thread.";
    const size_t numNotifyThread = ASYNC_NOTIFY_THREAD_NUM - 1;
    while (!interruptFlag_) {
        INJECT_POINT("master.ProcessAsyncNotify");
        // Group notifications by stream name and partition them to the threads.
        std::unordered_map<std::string, size_t> streamNameToPartitionNum;
        std::vector<std::vector<std::pair<std::string, std::string>>> parts(numNotifyThread);
        size_t index = 0;
        {
            std::lock_guard<std::shared_timed_mutex> locker(notifyMutex_);
            for (const auto &kv : notifyWorkerMap_) {
                for (auto const &item : kv.second) {
                    auto &streamName = item.first;
                    size_t partitionNum;
                    auto it = streamNameToPartitionNum.find(streamName);
                    if (it == streamNameToPartitionNum.end()) {
                        partitionNum = index % numNotifyThread;
                        streamNameToPartitionNum.emplace(streamName, partitionNum);
                        index += 1;
                    } else {
                        partitionNum = it->second;
                    }
                    // worker addr and stream name.
                    parts[partitionNum].emplace_back(kv.first, streamName);
                }
            }
        }

        std::vector<std::future<Status>> futures;
        for (auto &streamList : parts) {
            if (!streamList.empty()) {
                futures.emplace_back(
                    notifyThreadPool_->Submit([this, &streamList] { return SendPendingNotification(streamList); }));
            }
        }

        for (auto &fut : futures) {
            LOG_IF_ERROR(fut.get(), "SendPendingNotification failed");
        }
        cvLock_.WaitFor(ASYNC_NOTIFY_TIME_MS);
    }
    LOG(INFO) << "Terminating async notify thread.";
    return Status::OK();
}

Status SCNotifyWorkerManager::ProcessDeleteStreams()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "Starting async delete streams thread.";
    const size_t numDeleteThread = DELETE_STREAM_THREAD_NUM - 1;
    while (!interruptFlag_) {
        INJECT_POINT("master.ProcessDeleteStreams");
        // Get the pending delete stream list.
        std::set<std::string> deleteStreamList;
        {
            std::lock_guard<std::shared_timed_mutex> locker(deleteMutex_);
            deleteStreamList.swap(pendingDeleteStreams_);
        }
        if (deleteStreamList.empty()) {
            cvLock_.WaitFor(ASYNC_NOTIFY_TIME_MS);
            continue;
        }

        // partition based on the number of delete stream threads
        size_t index = 0;
        std::vector<std::set<std::string>> parts(numDeleteThread);
        for (auto &streamName : deleteStreamList) {
            parts[index % numDeleteThread].insert(streamName);
            index += 1;
        }

        // submit to thread pool.
        std::vector<std::future<Status>> futures;
        for (auto &streams : parts) {
            if (!streams.empty()) {
                futures.emplace_back(deleteThreadPool_->Submit([this, &streams] { return DeleteStreams(streams); }));
            }
        }
        for (auto &fut : futures) {
            LOG_IF_ERROR(fut.get(), "DeleteStreams failed");
        }
        cvLock_.WaitFor(ASYNC_NOTIFY_TIME_MS);
    }
    LOG(INFO) << "Terminating async delete streams thread.";
    return Status::OK();
}

Status SCNotifyWorkerManager::NotifyNewPubNode(const HostPort &workerAddr, const std::string &streamName,
                                               const StreamFields &streamFields, const HostPort &srcWorkerAddr)
{
    bool forceClose = false;
    bool asyncMode = false;
    return NotifyPubNodeImpl(workerAddr, streamName, streamFields, srcWorkerAddr, false, forceClose, asyncMode);
}

Status SCNotifyWorkerManager::NotifyDelPubNode(const HostPort &workerAddr, const std::string &streamName,
                                               const HostPort &srcWorkerAddr, const bool forceClose)
{
    // Sending an async updateTopo
    bool asyncMode = true;
    StreamFields streamFields(0, 0, false, 0, false, 0, StreamMode::MPMC);
    return NotifyPubNodeImpl(workerAddr, streamName, streamFields, srcWorkerAddr, true, forceClose, asyncMode);
}

Status SCNotifyWorkerManager::NotifyNewConsumer(const HostPort &workerAddr, const ConsumerMetaPb &consumerMeta,
                                                const RetainDataState::State retainData)
{
    bool asyncMode = false;
    return NotifyConsumerImpl(workerAddr, consumerMeta, false, retainData, asyncMode);
}

Status SCNotifyWorkerManager::NotifyDelConsumer(const HostPort &workerAddr, const ConsumerMetaPb &consumerMeta)
{
    // Sending an async updateTopo
    bool asyncMode = true;
    return NotifyConsumerImpl(workerAddr, consumerMeta, true, RetainDataState::State::INIT, asyncMode);
}

Status SCNotifyWorkerManager::ClearPendingNotification(const std::string &workerAddress)
{
    LOG(INFO) << "Clear pending notification send to " << workerAddress;
    std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
    TbbNotifyWorkerMap::accessor accessor;
    if (notifyWorkerMap_.find(accessor, workerAddress)) {
        RETURN_IF_NOT_OK(streamMetaStore_->RemoveNotificationByWorker(workerAddress));
        CHECK_FAIL_RETURN_STATUS(notifyWorkerMap_.erase(accessor) == true, K_RUNTIME_ERROR,
                                 "erase worker address failed");
    } else {
        LOG(INFO) << "Not exists pending notification for " << workerAddress;
    }
    return Status::OK();
}

Status SCNotifyWorkerManager::SendNotification(const HostPort &workerAddr, UpdateTopoNotificationReq &req)
{
    std::shared_ptr<MasterWorkerSCApi> masterWorkerApi = nullptr;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Stream:<%s>, Dest:<%s>, UpdateTopoNotification req: %s",
                                              req.stream_name(), workerAddr.ToString(),
                                              LogHelper::IgnoreSensitive(req));
    RETURN_IF_NOT_OK(rpcSessionManager_->GetRpcSession(workerAddr, masterWorkerApi, akSkManager_));
    Status status = masterWorkerApi->UpdateTopoNotification(req);
    LOG(INFO) << FormatString("Stream:<%s>, Dest:<%s>, UpdateTopoNotification result: %s", req.stream_name(),
                              workerAddr.ToString(), status.GetMsg());
    return status;
}

Status SCNotifyWorkerManager::SendPendingNotification(std::vector<std::pair<std::string, std::string>> &streamList)
{
    INJECT_POINT("master.SendPendingNotification");
    for (const auto &item : streamList) {
        auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-sync");
        const auto &workerAddr = item.first;
        const auto &streamName = item.second;
        LOG_IF_ERROR(SendPendingNotificationForStream(workerAddr, streamName),
                     FormatString("SendPendingNotification to %s for stream %s failed", workerAddr, streamName));
    }
    return Status::OK();
};

Status SCNotifyWorkerManager::SendPendingNotificationForStream(const std::string &workerAddr,
                                                               const std::string &streamName)
{
    Status checkRc = CheckWorkerStatus(workerAddr);
    // skip notify if target worker not exists in cluster.
    bool skipNotify = checkRc.GetCode() == K_NOT_FOUND;
    RETURN_OK_IF_TRUE(checkRc.IsError() && !skipNotify);

    TbbMetaHashmap::accessor streamAccessor;
    Status status = scMetadataManager_->GetStreamMetadata(streamName, streamAccessor);
    if (status.IsError()) {
        LOG(INFO) << "GetStreamMetadata failed:" << status.GetMsg();
        skipNotify = true;
    }

    std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
    TbbNotifyWorkerMap::accessor accessor;
    auto task = GetPendingNotification(workerAddr, streamName, accessor);
    RETURN_OK_IF_TRUE(task == nullptr);

    if (skipNotify || task->Empty()) {
        LOG(INFO) << "Skip send notify to " << workerAddr << " for stream " << streamName;
        return RemoveAsyncNotification(accessor, streamName, task);
    }

    UpdateTopoNotificationReq req;
    task->ConstructRequest(req);
    HostPort address;
    RETURN_IF_NOT_OK(address.ParseString(workerAddr));
    status = SendNotification(address, req);
    if (status.IsError()) {
        LOG(WARNING) << "SendNotification failed:" << status.GetMsg();
        if (IsRpcTimeout(status)) {
            return Status::OK();
        }
    }
    return RemoveAsyncNotification(accessor, streamName, task);
}

Status SCNotifyWorkerManager::RemoveAsyncNotification(TbbNotifyWorkerMap::accessor &accessor,
                                                      const std::string &streamName,
                                                      std::shared_ptr<PendingNotification> task)
{
    for (const auto &kv : task->pubs) {
        RETURN_IF_NOT_OK(streamMetaStore_->RemoveNotifyPub(accessor->first, kv.second));
    }

    for (const auto &kv : task->subs) {
        RETURN_IF_NOT_OK(streamMetaStore_->RemoveNotifySub(accessor->first, kv.second));
    }
    (void)accessor->second.erase(streamName);
    return Status::OK();
}

Status SCNotifyWorkerManager::NotifyPubNodeImpl(const HostPort &workerAddr, const std::string &streamName,
                                                const StreamFields &streamFields, const HostPort &srcWorkerAddr,
                                                bool isClose, const bool forceClose, bool asyncMode)
{
    NotifyPubPb pub;
    pub.set_is_close(isClose);
    pub.set_force_close(forceClose);
    pub.set_stream_name(streamName);
    pub.set_worker_addr(srcWorkerAddr.ToString());
    pub.set_max_stream_size(streamFields.maxStreamSize_);
    pub.set_page_size(streamFields.pageSize_);
    pub.set_auto_cleanup(streamFields.autoCleanup_);
    pub.set_retain_num_consumer(streamFields.retainForNumConsumers_);
    pub.set_encrypt_stream(streamFields.encryptStream_);
    pub.set_reserve_size(streamFields.reserveSize_);
    pub.set_stream_mode(streamFields.streamMode_);

    Status status = CheckWorkerStatus(workerAddr.ToString());
    INJECT_POINT_NO_RETURN("SCNotifyWorkerManager.ForceAsyncNotification",
                           [&status]() { status = Status(StatusCode::K_RPC_UNAVAILABLE, ""); });
    if (status.IsError()) {
        LOG(WARNING) << "Worker abnormal, async send notification. " << status.GetMsg();
        return AddAsyncPubNotification(workerAddr.ToString(), pub);
    }

    {
        std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
        TbbNotifyWorkerMap::accessor accessor;
        auto task = GetPendingNotification(workerAddr.ToString(), streamName, accessor);
        bool exists = false;
        RETURN_IF_NOT_OK(HandleExistsPubNotification(task, srcWorkerAddr.ToString(), isClose, true, exists));
        if (exists) {
            LOG(INFO) << FormatString("Exists notification send to %s, NotifyPubPb: %s", workerAddr.ToString(),
                                      LogHelper::IgnoreSensitive(pub));
            return Status::OK();
        }
    }

    // If async mode is set, just enqueue the request
    if (asyncMode) {
        return AddAsyncPubNotification(workerAddr.ToString(), pub);
    }

    UpdateTopoNotificationReq req;
    req.set_stream_name(streamName);
    *req.add_pubs() = pub;
    status = SendNotification(workerAddr, req);
    if (IsRpcTimeout(status)) {
        LOG(WARNING) << "RPC timeout, async send notification. " << status.GetMsg();
        return AddAsyncPubNotification(workerAddr.ToString(), pub);
    }
    return status;
}

Status SCNotifyWorkerManager::AddAsyncStopDataRetentionNotification(const HostPort &workerAddr,
                                                                    const std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
    TbbNotifyWorkerMap::accessor accessor;
    auto task = GetOrCreatePendingNotification(workerAddr.ToString(), streamName, accessor);
    CHECK_FAIL_RETURN_STATUS(task != nullptr, K_RUNTIME_ERROR, "task is null");

    // If its same return okay
    if (task->retainData == RetainDataState::State::NOT_RETAIN) {
        return Status::OK();
    } else {
        // Else change state and store it in meta store
        task->retainData = RetainDataState::State::NOT_RETAIN;
    }

    LOG(INFO) << FormatString("Stream:<%s>, Dest:<%s>, AsyncStopRetentionState", streamName, workerAddr.ToString());
    return Status::OK();
}

Status SCNotifyWorkerManager::NotifyConsumerImpl(const HostPort &workerAddr, const ConsumerMetaPb &consumerMeta,
                                                 bool isClose, RetainDataState::State retainData, bool asyncMode)
{
    NotifyConsumerPb sub;
    *sub.mutable_consumer() = consumerMeta;
    sub.set_is_close(isClose);

    Status status = CheckWorkerStatus(workerAddr.ToString());
    INJECT_POINT_NO_RETURN("SCNotifyWorkerManager.ForceAsyncNotification",
                           [&status]() { status = Status(StatusCode::K_RPC_UNAVAILABLE, ""); });
    if (status.IsError()) {
        LOG(WARNING) << "Worker abnormal, async send notification. " << status.GetMsg();
        return AddAsyncSubNotification(workerAddr.ToString(), sub, true);
    }

    {
        std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
        TbbNotifyWorkerMap::accessor accessor;
        auto task = GetPendingNotification(workerAddr.ToString(), consumerMeta.stream_name(), accessor);

        bool exists = false;
        RETURN_IF_NOT_OK(HandleExistsSubNotification(task, consumerMeta.consumer_id(), isClose, true, exists));
        if (exists) {
            LOG(INFO) << FormatString("Exists notification send to %s, NotifyConsumerPb: %s", workerAddr.ToString(),
                                      LogHelper::IgnoreSensitive(sub));
            return Status::OK();
        }
    }

    UpdateTopoNotificationReq req;
    req.set_stream_name(consumerMeta.stream_name());
    *req.add_subs() = sub;
    req.set_retain_data(retainData);

    // If async mode is set, just enqueue the request
    if (asyncMode) {
        return AddAsyncSubNotification(workerAddr.ToString(), sub, true);
    }

    status = SendNotification(workerAddr, req);
    if (IsRpcTimeout(status)) {
        LOG(WARNING) << "RPC timeout, async send notification. " << status.GetMsg();
        return AddAsyncSubNotification(workerAddr.ToString(), sub, true);
    }
    return status;
}

Status SCNotifyWorkerManager::HandleExistsPubNotification(std::shared_ptr<PendingNotification> task,
                                                          const std::string &workerAddr, bool isClose, bool needPersist,
                                                          bool &exists)
{
    RETURN_OK_IF_TRUE(task == nullptr);
    Status rc;
    exists = false;
    auto iter = task->pubs.find(workerAddr);
    if (iter != task->pubs.end()) {
        exists = true;
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
            "Exists pending pub notification send to <%s>, isClose:%d, detail: %s", task->workerAddr, isClose,
            LogHelper::IgnoreSensitive(iter->second));
        if (iter->second.is_close() == isClose) {
            // Exists duplicate notification.
            return Status::OK();
        } else {
            // Exists opposite notification
            if (needPersist) {
                rc = streamMetaStore_->RemoveNotifyPub(task->workerAddr, iter->second);
            }
            (void)task->pubs.erase(iter);
            return rc;
        }
    }
    return rc;
}

Status SCNotifyWorkerManager::HandleExistsSubNotification(std::shared_ptr<PendingNotification> task,
                                                          const std::string &consumerId, bool isClose, bool needPersist,
                                                          bool &exists)
{
    RETURN_OK_IF_TRUE(task == nullptr);
    Status rc;
    exists = false;
    auto iter = task->subs.find(consumerId);
    if (iter != task->subs.end()) {
        exists = true;
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
            "Exists pending sub notification send to <%s>, isClose:%d, detail: %s", task->workerAddr, isClose,
            LogHelper::IgnoreSensitive(iter->second));
        if (iter->second.is_close() == isClose) {
            // Exists duplicate notification.
            return Status::OK();
        } else {
            // Exists opposite notification
            if (needPersist) {
                rc = streamMetaStore_->RemoveNotifySub(task->workerAddr, iter->second);
            }
            (void)task->subs.erase(iter);
            return rc;
        }
    }
    return rc;
}

Status SCNotifyWorkerManager::AddAsyncDeleteNotification(const std::string &streamName)
{
    LOG(INFO) << FormatString("Enqueuing AutoDelete request for stream %s", streamName);
    std::unique_lock<std::shared_timed_mutex> lock(deleteMutex_);
    pendingDeleteStreams_.insert(streamName);
    return Status::OK();
}

Status SCNotifyWorkerManager::AddAsyncPubNotification(const std::string &workerAddr, const NotifyPubPb &pub,
                                                      bool needPersist)
{
    const auto &streamName = pub.stream_name();
    std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
    TbbNotifyWorkerMap::accessor accessor;
    auto task = GetOrCreatePendingNotification(workerAddr, streamName, accessor);
    CHECK_FAIL_RETURN_STATUS(task != nullptr, K_RUNTIME_ERROR, "task is null");
    bool exists = false;
    Status rc = HandleExistsPubNotification(task, pub.worker_addr(), pub.is_close(), needPersist, exists);
    if (exists) {
        return rc;
    } else {
        (void)task->pubs.emplace(pub.worker_addr(), pub);
    }
    LOG(INFO) << FormatString("Stream:<%s>, Dest:<%s>, AsyncSendPubNodeChange: %s", streamName, workerAddr,
                              LogHelper::IgnoreSensitive(pub));
    if (needPersist) {
        RETURN_IF_NOT_OK(streamMetaStore_->AddNotifyPub(workerAddr, pub));
    }
    return Status::OK();
}

Status SCNotifyWorkerManager::AddAsyncSubNotification(const std::string &workerAddr, const NotifyConsumerPb &sub,
                                                      bool needPersist)
{
    const auto &consumerMeta = sub.consumer();
    const auto &streamName = consumerMeta.stream_name();

    std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
    TbbNotifyWorkerMap::accessor accessor;
    auto task = GetOrCreatePendingNotification(workerAddr, streamName, accessor);
    CHECK_FAIL_RETURN_STATUS(task != nullptr, K_RUNTIME_ERROR, "task is null");
    bool exists = false;
    // no need to check retainData as same event (consumer id and close) will have same retainData
    Status rc = HandleExistsSubNotification(task, consumerMeta.consumer_id(), sub.is_close(), needPersist, exists);
    if (exists) {
        return rc;
    } else {
        (void)task->subs.emplace(consumerMeta.consumer_id(), sub);
    }

    LOG(INFO) << FormatString("Stream:<%s>, Dest:<%s>, AsyncSendConsumerChange: %s", streamName, workerAddr,
                              LogHelper::IgnoreSensitive(sub));
    if (needPersist) {
        RETURN_IF_NOT_OK(streamMetaStore_->AddNotifySub(workerAddr, sub));
    }
    return Status::OK();
}

std::shared_ptr<PendingNotification> SCNotifyWorkerManager::GetOrCreatePendingNotification(
    const std::string &workerAddr, const std::string &streamName, TbbNotifyWorkerMap::accessor &accessor)
{
    // if other thread insert success, insert will return false and the accessor can get the new value.
    if (notifyWorkerMap_.insert(accessor, workerAddr)) {
        std::unordered_map<std::string, std::shared_ptr<PendingNotification>> data;
        accessor->second = std::move(data);
    }
    auto iter = accessor->second.find(streamName);
    if (iter == accessor->second.end()) {
        auto data = std::make_shared<PendingNotification>(streamName, workerAddr);
        iter = accessor->second.emplace(streamName, std::move(data)).first;
    }
    return iter->second;
}

std::shared_ptr<PendingNotification> SCNotifyWorkerManager::GetPendingNotification(
    const std::string &workerAddr, const std::string &streamName, TbbNotifyWorkerMap::accessor &accessor)
{
    if (!notifyWorkerMap_.find(accessor, workerAddr)) {
        return nullptr;
    }
    auto iter = accessor->second.find(streamName);
    if (iter == accessor->second.end()) {
        return nullptr;
    }
    return iter->second;
}

bool SCNotifyWorkerManager::ExistsPendingNotification(const std::string &streamName)
{
    {
        std::shared_lock<std::shared_timed_mutex> locker(notifyMutex_);
        if (notifyWorkerMap_.empty()) {
            return false;
        }
    }

    std::lock_guard<std::shared_timed_mutex> locker(notifyMutex_);
    for (const auto &kv : notifyWorkerMap_) {
        auto iter = kv.second.find(streamName);
        if (iter != kv.second.end() && !iter->second->Empty()) {
            return true;
        }
    }
    return false;
}

Status SCNotifyWorkerManager::CheckWorkerStatus(const std::string &workerAddr)
{
    // Check connection returns an error if the node is down or there is a problem, such as K_RPC_UNAVAILABLE
    // If any error is given, change the rc to be K_WORKER_ABNORMAL
    HostPort workerHostPort;
    workerHostPort.ParseString(workerAddr);
    if (etcdCM_ == nullptr) {
        RETURN_STATUS(StatusCode::K_INVALID, "ETCD cluster manager is nullptr.");
    }
    return etcdCM_->CheckConnection(workerHostPort);
}

Status SCNotifyWorkerManager::RecoverNotification()
{
    CHECK_FAIL_RETURN_STATUS(streamMetaStore_ != nullptr, K_RUNTIME_ERROR, "streamMetaStore_ is null");
    std::vector<std::pair<std::string, NotifyPubPb>> pubs;
    RETURN_IF_NOT_OK(streamMetaStore_->GetAllNotifyPub(pubs));
    std::vector<std::pair<std::string, NotifyConsumerPb>> subs;
    RETURN_IF_NOT_OK(streamMetaStore_->GetAllNotifySub(subs));

    // the key is WorkerAddr_StreamName_PubWorkerAddr
    for (const auto &kv : pubs) {
        auto keyVec = Split(kv.first, "_");
        if (keyVec.size() > 1) {
            (void)AddAsyncPubNotification(keyVec[0], kv.second, false);
        }
    }

    // the key is WorkerAddr_StreamName_ConsumerId
    for (const auto &kv : subs) {
        auto keyVec = Split(kv.first, "_");
        if (keyVec.size() > 1) {
            (void)AddAsyncSubNotification(keyVec[0], kv.second, false);
        }
    }

    return Status::OK();
}

bool SCNotifyWorkerManager::CanRetryDeleteStream(const std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> lock(deleteMutex_);
    auto itr = pendingDeleteStreamsLastRetry_.find(streamName);
    if (itr == pendingDeleteStreamsLastRetry_.end()) {
        // if entry does not exists this is first retry
        return true;
    } else {
        // Check if it exceeded 60 secs
        auto start = itr->second;
        auto now = std::chrono::high_resolution_clock::now();
        auto escapedTimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
        return (escapedTimeMs >= WAIT_TIME_BETWEEN_DELSTREAM_MS);
    }
}

Status SCNotifyWorkerManager::DeleteStreams(std::set<std::string> &deleteStreams)
{
    INJECT_POINT("SCNotifyWorkerManager.DeleteStreams");
    auto iter = deleteStreams.begin();
    while (iter != deleteStreams.end()) {
        auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-del");
        const std::string streamName = *iter;
        if (!CanRetryDeleteStream(streamName)) {
            // Do not retry the stream yet skip to the next one
            ++iter;
            continue;
        }
        const std::string infoMsg = FormatString("AutoDelete stream %s", streamName);
        LOG(INFO) << infoMsg;
        DeleteStreamReqPb req;
        DeleteStreamRspPb rsp;
        req.set_stream_name(streamName);
        req.mutable_src_node_addr()->set_host("");
        req.mutable_src_node_addr()->set_port(-1);
        Status rc = scMetadataManager_->DeleteStream(req, rsp);
        // Stop retrying if delete is successful or stream not found or if stream is in use (has a consumer or producer)
        // We will retry if notifications are pending
        if (rc.IsOk() || rc.GetCode() == StatusCode::K_NOT_FOUND || rc.GetCode() == StatusCode::K_SC_STREAM_IN_USE) {
            LOG(INFO) << "AutoDelete for stream: " << streamName << " done with status " << rc.ToString();
            iter = deleteStreams.erase(iter);
            std::unique_lock<std::shared_timed_mutex> lock(deleteMutex_);
            pendingDeleteStreamsLastRetry_.erase(streamName);
        } else {
            LOG(INFO) << FormatString("%s AutoDelete failed with error %s", infoMsg, rc.ToString());
            ++iter;
            std::unique_lock<std::shared_timed_mutex> lock(deleteMutex_);
            pendingDeleteStreamsLastRetry_[streamName] = std::chrono::high_resolution_clock::now();
        }
    }
    if (!deleteStreams.empty()) {
        std::unique_lock<std::shared_timed_mutex> lock(deleteMutex_);
        pendingDeleteStreams_.insert(deleteStreams.begin(), deleteStreams.end());
    }
    return Status::OK();
}

Status SCNotifyWorkerManager::GetPendingNotificationByStreamName(const std::string &streamName,
                                                                 std::vector<AsyncNotificationPb> &notifications)
{
    std::lock_guard<std::shared_timed_mutex> locker(notifyMutex_);
    for (const auto &kv : notifyWorkerMap_) {
        auto iter = kv.second.find(streamName);
        if (iter != kv.second.end() && !iter->second->Empty()) {
            // Get notifications including notifyPub, notifySub and also stopRetention notifications.
            for (auto &pair : iter->second->pubs) {
                auto &pub = pair.second;
                notifications.emplace_back();
                auto &notification = notifications.back();
                notification.set_is_pub(true);
                notification.set_is_close(pub.is_close());
                notification.set_target_worker(kv.first);
                notification.set_id(pub.worker_addr());
                notification.set_force_close(pub.force_close());
                notification.set_retain_data(RetainDataState::State::INIT);
            }
            for (auto &pair : iter->second->subs) {
                auto &sub = pair.second;
                notifications.emplace_back();
                auto &notification = notifications.back();
                notification.set_is_pub(false);
                notification.set_is_close(sub.is_close());
                notification.set_target_worker(kv.first);
                notification.set_id(sub.consumer().consumer_id());
                notification.set_retain_data(RetainDataState::State::INIT);
            }
            notifications.emplace_back();
            auto &notification = notifications.back();
            notification.set_retain_data(iter->second->retainData);
            notification.set_target_worker(kv.first);
        }
    }
    return Status::OK();
}

Status SCNotifyWorkerManager::AddAsyncNotifications(const StreamFields &streamFields, const std::string &streamName,
                                                    const MetaForSCMigrationPb &streamMeta)
{
    for (const auto &notification : streamMeta.notifications()) {
        if (notification.retain_data() != RetainDataState::State::INIT) {
            HostPort targetAddress;
            RETURN_IF_NOT_OK(targetAddress.ParseString(notification.target_worker()));
            RETURN_IF_NOT_OK(AddAsyncStopDataRetentionNotification(targetAddress, streamName));
        } else if (notification.is_pub()) {
            NotifyPubPb pub;
            pub.set_is_close(notification.is_close());
            pub.set_force_close(notification.force_close());
            pub.set_stream_name(streamName);
            pub.set_worker_addr(notification.id());
            pub.set_max_stream_size(streamFields.maxStreamSize_);
            pub.set_page_size(streamFields.pageSize_);
            pub.set_auto_cleanup(streamFields.autoCleanup_);
            pub.set_retain_num_consumer(streamFields.retainForNumConsumers_);
            pub.set_encrypt_stream(streamFields.encryptStream_);
            pub.set_reserve_size(streamFields.reserveSize_);
            pub.set_stream_mode(streamFields.streamMode_);
            RETURN_IF_NOT_OK(AddAsyncPubNotification(notification.target_worker(), pub));
        } else {
            NotifyConsumerPb sub;
            bool found = false;
            for (const auto &consumerMetaPb : streamMeta.consumers()) {
                if (consumerMetaPb.consumer_id() == notification.id()) {
                    *sub.mutable_consumer() = consumerMetaPb;
                    found = true;
                    break;
                }
            }
            CHECK_FAIL_RETURN_STATUS(found, K_RUNTIME_ERROR, "Migrate async consumer notification failed.");
            sub.set_is_close(notification.is_close());
            RETURN_IF_NOT_OK(AddAsyncSubNotification(notification.target_worker(), sub));
        }
    }
    return Status::OK();
}

Status SCNotifyWorkerManager::RemovePendingNotificationByStreamName(const std::string &streamName)
{
    std::lock_guard<std::shared_timed_mutex> locker(notifyMutex_);
    for (auto &kv : notifyWorkerMap_) {
        auto iter = kv.second.find(streamName);
        if (iter != kv.second.end() && !iter->second->Empty()) {
            for (const auto &pubPair : iter->second->pubs) {
                RETURN_IF_NOT_OK(streamMetaStore_->RemoveNotifyPub(kv.first, pubPair.second));
            }
            for (const auto &subPair : iter->second->subs) {
                RETURN_IF_NOT_OK(streamMetaStore_->RemoveNotifySub(kv.first, subPair.second));
            }
            (void)kv.second.erase(iter);
        }
    }
    return Status::OK();
}
}  // namespace master
}  // namespace datasystem
