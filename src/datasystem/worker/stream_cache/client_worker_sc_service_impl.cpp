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

#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"

#include <cstdint>
#include <functional>
#include <sstream>
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/stream_cache/util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/master/stream_cache/master_sc_service_impl.h"
#include "datasystem/protos/stream_posix.service.rpc.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/stream_manager.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"

DS_DECLARE_uint32(page_size);
DS_DECLARE_int32(zmq_chunk_sz);
DS_DECLARE_uint64(client_dead_timeout_s);
DS_DEFINE_int32(sc_thread_num, 128, "Number of threads for (non rpc) stream cache service work");
DS_DEFINE_validator(sc_thread_num, &Validator::ValidateThreadNum);
DS_DEFINE_uint32(sc_gc_interval_ms, 50, "Memory resource clean up interval. Default to 50ms");
DS_DEFINE_bool(enable_stream_data_verification, false, "Option to verify if data from a producer is out of order");
DS_DECLARE_uint32(sc_shared_page_size_mb);

namespace datasystem {
namespace worker {
namespace stream_cache {
static const std::string CLIENT_WORKER_SC_SERVICE_IMPL = "ClientWorkerSCServiceImpl";
template class BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>;
template class MemAllocRequestList<CreateShmPageRspPb, CreateShmPageReqPb>;
template class BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>;
template class MemAllocRequestList<CreateLobPageRspPb, CreateLobPageReqPb>;
ClientWorkerSCServiceImpl::ClientWorkerSCServiceImpl(HostPort serverAddr, HostPort masterAddr,
                                                     master::MasterSCServiceImpl *masterSCService,
                                                     std::shared_ptr<AkSkManager> akSkManager,
                                                     std::shared_ptr<WorkerSCAllocateMemory> manager)
    : localWorkerAddress_(std::move(serverAddr)),
      masterAddress_(std::move(masterAddr)),
      scAllocateManager_(std::move(manager)),
      akSkManager_(std::move(akSkManager)),
      interrupt_(false)
{
    workerMasterApiManager_ =
        std::make_shared<WorkerMasterSCApiManager>(localWorkerAddress_, akSkManager_, masterSCService);
}

Status ClientWorkerSCServiceImpl::Init()
{
    remoteWorkerManager_ = std::make_unique<RemoteWorkerManager>(this, akSkManager_, scAllocateManager_);
    RETURN_IF_NOT_OK(remoteWorkerManager_->Init());
    LOG(INFO) << FormatString("[%S] Initialize success", LogPrefix());
    // Create a thread pool for async request handling in the service
    const size_t MIN_THREADS = 4;
    size_t minThreads = std::min<size_t>(MIN_THREADS, FLAGS_sc_thread_num);
    RETURN_IF_EXCEPTION_OCCURS(threadPool_ =
                                   std::make_shared<ThreadPool>(minThreads, FLAGS_sc_thread_num, "ScThreads"));
    // Also create a similar pool but just the purpose of managing CreateShmPage and AllocBigShmMemory rpc
    // (both internal and external)
    RETURN_IF_EXCEPTION_OCCURS(memAllocPool_ =
                                   std::make_shared<ThreadPool>(minThreads, FLAGS_sc_thread_num, "memThreads"));
    // We will further let another thread pool to do the memory cleanup work.
    // But we don't want to overload the pool if we have thousands of streams, and
    // we will limit to a small number of threads at a time.
    constexpr size_t NUM_ACK_THREADS = 2;
    RETURN_IF_EXCEPTION_OCCURS(ackPool_ = std::make_shared<ThreadPool>(NUM_ACK_THREADS, NUM_ACK_THREADS, "ackThreads"));
    // Kick off a thread to do garbage collection
    autoAck_ = threadPool_->Submit([this]() {
        auto traceId = GetStringUuid();
        auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
        LOG(INFO) << FormatString("[%s] Ack thread starts.", LogPrefix());
        const uint64_t timeoutS = FLAGS_client_dead_timeout_s + 5;
        std::deque<AckTask> ackList;
        while (!interrupt_) {
            AutoAckImpl(ackList, timeoutS);
            std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_sc_gc_interval_ms));
        }
    });
    threadPool_->SetWarnLevel(ThreadPool::WarnLevel::LOW);
    ackPool_->SetWarnLevel(ThreadPool::WarnLevel::LOW);
    EraseFailedNodeApiEvent::GetInstance().AddSubscriber(CLIENT_WORKER_SC_SERVICE_IMPL,
                                                         [this](HostPort &node) { EraseFailedWorkerMasterApi(node); });
    LOG_IF_ERROR(ScMetricsMonitor::Instance()->StartMonitor(), "Failed to start ScMetrics monitor");
    return ClientWorkerSCService::Init();
}

Status ClientWorkerSCServiceImpl::ValidateWorkerState()
{
    if (!IsHealthy()) {
        RETURN_STATUS(K_NOT_READY, "Worker not ready");
    }
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CreateProducer(
    std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    CreateProducerReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");

    auto recorder = std::make_shared<AccessRecorderStreamWrap>(AccessRecorderKey::DS_POSIX_CREATE_PRODUCER);
    recorder->reqParam.streamName = req.stream_name();
    recorder->reqParam.producerId = req.producer_id();
    recorder->reqParam.pageSize = Optional<int64_t>(req.page_size());
    recorder->reqParam.maxStreamSize = Optional<uint64_t>(req.max_stream_size());
    recorder->reqParam.autoCleanup = Optional<bool>(req.auto_cleanup());
    recorder->reqParam.retainForNumConsumers = Optional<uint64_t>(req.retain_num_consumer());
    recorder->reqParam.encryptStream = Optional<bool>(req.encrypt_stream());
    recorder->reqParam.reserveSize = Optional<uint64_t>(req.reserve_size());
    recorder->reqParam.streamMode = Optional<int32_t>(req.stream_mode());
    auto rc = CreateProducerInternal(req, recorder, serverApi);
    recorder->SetStatus(rc);
    return rc;
}

Status ClientWorkerSCServiceImpl::CreateProducerInternal(
    const CreateProducerReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
    std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> serverApi)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    LOG(INFO) << "Worker received CreateProducer request: " << LogHelper::IgnoreSensitive(req);
    TimeoutDuration parentDuration = scTimeoutDuration;
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckConnection(namespaceUri), "worker check connection failed");

    PerfPoint point(PerfKey::WORKER_CREATE_PRODUCER_ALL);

    // The real work of the close will be driven in another thread. Launch it now and then release this current thread
    // so that it does not hold up the rpc threads.
    auto traceId = Trace::Instance().GetTraceID();
    threadPool_->Execute([=]() mutable {
        scTimeoutDuration = parentDuration;  // lambda capture gets the parents copy. assign the copy to thread local
        Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        CreateProducerRspPb rsp;
        Status rc = CreateProducerImpl(namespaceUri, req, rsp);
        CheckErrorReturn(rc, rsp, FormatString("[S:%s] CreateProducerImpl failed with rc ", namespaceUri), serverApi);
        recorder->SetStatus(rc);
        if (rc.IsOk()) {
            recorder->rspParam.senderProducerNo = Optional<uint64_t>(rsp.sender_producer_no());
            recorder->rspParam.enableDataVerification = Optional<bool>(rsp.enable_data_verification());
            recorder->rspParam.streamNo = Optional<uint64_t>(rsp.stream_no());
            recorder->rspParam.sharedPageSize = Optional<uint64_t>(rsp.shared_page_size());
            recorder->rspParam.enableSharedPage = Optional<bool>(rsp.enable_shared_page());
        }
        // recorder should destroy before traceGuard, otherwise, the traceid will be cleaned up
        recorder.reset();
    });

    return Status::OK();
}

Status ClientWorkerSCServiceImpl::GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr)
{
    std::string dbName;
    RETURN_IF_NOT_OK(etcdCM_->GetPrimaryReplicaLocationByAddr(srcAddr, destAddr, dbName));
    g_MetaRocksDbName = dbName;
    return Status::OK();
}

void ClientWorkerSCServiceImpl::ConstructCreateProducerPb(const std::string &streamName,
                                                          const Optional<StreamFields> &streamFields,
                                                          master::CreateProducerReqPb &out) const noexcept
{
    DLOG(INFO) << "Start to construct ProducerMetaPb";
    auto &producerMetaPb = *out.mutable_producer_meta();
    producerMetaPb.set_stream_name(streamName);
    producerMetaPb.mutable_worker_address()->set_host(localWorkerAddress_.Host());
    producerMetaPb.mutable_worker_address()->set_port(localWorkerAddress_.Port());
    out.set_max_stream_size(streamFields->maxStreamSize_);
    out.set_page_size(streamFields->pageSize_);
    out.set_auto_cleanup(streamFields->autoCleanup_);
    out.set_retain_num_consumer(streamFields->retainForNumConsumers_);
    out.set_encrypt_stream(streamFields->encryptStream_);
    out.set_reserve_size(streamFields->reserveSize_);
    out.set_stream_mode(streamFields->streamMode_);
    out.set_redirect(true);
}

Status ClientWorkerSCServiceImpl::CreateProducerHandleSend(std::shared_ptr<WorkerMasterSCApi> api,
                                                           const std::string &streamName,
                                                           const Optional<StreamFields> &streamFields)
{
    auto createProducerFn = [&]() {
        master::CreateProducerReqPb masterReq;
        ConstructCreateProducerPb(streamName, streamFields, masterReq);
        master::CreateProducerRspPb masterRsp;
        std::function<Status(master::CreateProducerReqPb & req, master::CreateProducerRspPb & rsp)> func =
            [&api](master::CreateProducerReqPb &req, master::CreateProducerRspPb &rsp) {
                return api->CreateProducer(req, rsp);
            };
        RETURN_IF_NOT_OK(RedirectRetryWhenMetaMoving(masterReq, masterRsp, api, func));
        return Status::OK();
    };
    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(createProducerFn));
}

Status ClientWorkerSCServiceImpl::CreateProducerImpl(const std::string &namespaceUri, const CreateProducerReqPb &req,
                                                     CreateProducerRspPb &rsp)
{
    // We need to serialize on CreateProducer and Subscribe.
    // That is, we must wait for the previous one to finish (or rollback) successfully
    // before the next pub/sub can proceed
    CreatePubSubCtrl::Accessor createLock;
    createStreamLocks_.Insert(createLock, namespaceUri);
    Raii releaseLock([this, &createLock] { createStreamLocks_.TryErase(createLock); });

    // If reserve size is 0, default it to page size.
    uint64_t reserveSize =
        req.reserve_size() == 0 ? static_cast<uint64_t>(req.page_size()) : static_cast<uint64_t>(req.reserve_size());
    const Optional<StreamFields> streamFields(req.max_stream_size(), req.page_size(), req.auto_cleanup(),
                                              req.retain_num_consumer(), req.encrypt_stream(), reserveSize,
                                              req.stream_mode());
    const std::string &producerId = req.producer_id();
    const std::string &clientId = req.client_id();

    // Next to create the stream manager if it doesn't exist.
    INJECT_POINT("ClientWorkerSCServiceImpl.CreateProducerImpl.sleep");
    std::shared_ptr<StreamManagerWithLock> streamMgrWithLock;
    bool streamExisted;
    RETURN_IF_NOT_OK(CreateStreamManagerIfNotExist(namespaceUri, streamFields, streamMgrWithLock, streamExisted));
    bool blockMemoryReclaim = false;
    bool rollbackProducer = false;
    auto streamMgr = streamMgrWithLock->mgr_;
    uint64_t streamNo = streamMgr->GetStreamNo();
    // If we hit any error below, we will erase the StreamManager from the tbb provided it is this thread
    // that creates the stream manager. We will need an exclusive accessor
    Raii raii([this, &namespaceUri, &streamMgrWithLock, &streamMgr, &blockMemoryReclaim, &rollbackProducer,
               &producerId]() {
        if (blockMemoryReclaim) {
            streamMgr->UnblockMemoryReclaim();
        }
        // Unblock reclaim first, because CloseProducer can trigger early reclaim.
        if (rollbackProducer) {
            LOG_IF_ERROR(streamMgr->CloseProducer(producerId, true), "StreamManager rollback close producer failed");
        }
        streamMgrWithLock->CleanUp(std::bind(&ClientWorkerSCServiceImpl::EraseFromStreamMgrDictWithoutLck, this,
                                             namespaceUri, std::placeholders::_1));
    });
    if (streamExisted) {
        // An existing stream was found.
        // Serialize with EarlyReclaim() so our reserved pages will not be reclaimed.
        streamMgr->BlockMemoryReclaim();
        blockMemoryReclaim = true;
        bool existsLocalConsumer = streamMgr->CheckConsumerExist(localWorkerAddress_.ToString()).IsOk();
        bool reserveShm = !StreamManager::EnableSharedPage(streamFields->streamMode_) || existsLocalConsumer;
        RETURN_IF_NOT_OK(PostCreateStreamManager(streamMgr, streamFields, reserveShm));
    }
    bool firstProducer = (streamMgr->GetLocalProducerCount() == 0);
    CHECK_FAIL_RETURN_STATUS(firstProducer || streamFields->streamMode_ != StreamMode::SPSC, K_INVALID,
                             FormatString("There can be at most one producer in this stream mode: %d.",
                                          static_cast<int32_t>(streamFields->streamMode_)));
    INJECT_POINT("ClientWorkerSCServiceImpl.CreateProducerImpl.WaitBeforeAdd");
    // Get a unique number to identify the new producer within the stream locally.
    DataVerificationHeader::SenderProducerNo senderProducerNo;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamMgr->AddProducer(producerId, senderProducerNo),
                                     "streamMgr add producer failed");
    // We will let go the accessor at this point to prevent deadlock. The master may send back a SyncConsumerNode
    // rpc back to this worker if this is the first producer. We are still protected by the createLock
    streamMgrWithLock->Release();
    if (firstProducer) {
        LOG(INFO) << FormatString("[%s, S:%s, P:%s] First CreateProducer request sending to master.", LogPrefix(),
                                  namespaceUri, producerId);
        auto api = workerMasterApiManager_->GetWorkerMasterApi(namespaceUri, etcdCM_);
        CHECK_FAIL_RETURN_STATUS(api != nullptr, K_RUNTIME_ERROR, "Get WorkerMasterApi failed of " + namespaceUri);
        // Only first producer sends CreateProducer request, so use local address as producer id
        Status rc = CreateProducerHandleSend(api, namespaceUri, streamFields);
        if (rc.IsError() && rc.GetCode() != StatusCode::K_DUPLICATED) {
            LOG(ERROR) << FormatString("Create Producer [%s] failed in master %s: %s", producerId, api->Address(),
                                       rc.GetMsg());
            // If fail on master, we should roll back this operation
            rollbackProducer = true;
            return rc;
        }
        streamMgr->InitRetainData(req.retain_num_consumer());
        LOG(INFO) << FormatString("[%s, S:%s, P:%s] CreateProducer success on master %s.", LogPrefix(), namespaceUri,
                                  producerId, api->Address());
    }

    ShmView shmViewOfCursor, shmViewOfStreamMeta;
    RETURN_IF_NOT_OK(streamMgr->AddCursorForProducer(producerId, shmViewOfCursor));

    if (StreamManager::EnableSharedPage(streamFields->streamMode_)) {
        RETURN_IF_NOT_OK(streamMgr->GetOrCreateShmMeta(TenantAuthManager::Instance()->ExtractTenantId(namespaceUri),
                                                       shmViewOfStreamMeta));
        ShmViewPb shmViewOfStreamMetaPb;
        shmViewOfStreamMetaPb.set_fd(shmViewOfStreamMeta.fd);
        shmViewOfStreamMetaPb.set_mmap_size(shmViewOfStreamMeta.mmapSz);
        shmViewOfStreamMetaPb.set_size(shmViewOfStreamMeta.sz);
        shmViewOfStreamMetaPb.set_offset(shmViewOfStreamMeta.off);
        rsp.mutable_stream_meta_view()->CopyFrom(shmViewOfStreamMetaPb);
    }

    ShmViewPb shmViewOfCursorPb;
    shmViewOfCursorPb.set_fd(shmViewOfCursor.fd);
    shmViewOfCursorPb.set_mmap_size(shmViewOfCursor.mmapSz);
    shmViewOfCursorPb.set_offset(shmViewOfCursor.off);
    shmViewOfCursorPb.set_size(shmViewOfCursor.sz);
    rsp.mutable_page_view()->CopyFrom(shmViewOfCursorPb);
    rsp.set_enable_data_verification(FLAGS_enable_stream_data_verification);
    rsp.set_sender_producer_no(senderProducerNo);
    rsp.set_stream_no(streamNo);
    rsp.set_shared_page_size(FLAGS_sc_shared_page_size_mb * MB_TO_BYTES);
    rsp.set_enable_shared_page(StreamManager::EnableSharedPage(static_cast<StreamMode>(req.stream_mode())));

    {
        std::unique_lock<std::shared_timed_mutex> lock(clearMutex_);
        (void)clientProducers_[clientId].emplace_back(namespaceUri, producerId);
    }
    streamMgrWithLock->needCleanUp = false;
    createStreamLocks_.BlockingErase(createLock);
    LOG(INFO) << FormatString("[%s, S:%s, P:%s] CreateProducer success.", LogPrefix(), namespaceUri, producerId);
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CloseProducer(
    std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    CloseProducerReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    auto recorder = std::make_shared<AccessRecorderStreamWrap>(AccessRecorderKey::DS_POSIX_CLOSE_PRODUCER);
    recorder->reqParam.streamName = req.stream_name();
    recorder->reqParam.producerId = req.producer_id();
    recorder->reqParam.clientId = req.client_id();
    auto rc = CloseProducerInternal(req, recorder, serverApi);
    recorder->SetStatus(rc);
    return rc;
}

Status ClientWorkerSCServiceImpl::CloseProducerInternal(
    const CloseProducerReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
    std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> serverApi)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    TimeoutDuration parentDuration = scTimeoutDuration;
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    LOG(INFO) << "Worker received CloseProducer request:" << LogHelper::IgnoreSensitive(req);
    const std::string &producerId = req.producer_id();
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckConnection(namespaceUri), "worker check connection failed");
    const std::string &clientId = req.client_id();
    PerfPoint point(PerfKey::WORKER_CLOSE_PRODUCER_ALL);

    // The real work of the close will be driven in another thread. Launch it now and then release this current thread
    // so that it does not hold up the rpc threads.
    auto traceId = Trace::Instance().GetTraceID();
    threadPool_->Execute([=]() mutable {
        scTimeoutDuration = parentDuration;  // lambda capture gets the parents copy. assign the copy to thread local
        Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        CloseProducerRspPb rsp;
        Status rc = CloseProducerImpl(producerId, namespaceUri, true);
        if (rc.IsOk() || rc.GetCode() == StatusCode::K_SC_PRODUCER_NOT_FOUND) {
            std::unique_lock<std::shared_timed_mutex> lock(clearMutex_);
            clientProducers_[clientId].remove_if([&namespaceUri, &producerId](const StreamProducer &data) {
                return (data.streamName_ == namespaceUri && data.producerId_ == producerId);
            });
        }
        LOG(INFO) << FormatString("[%s, S:%s, P:%s] CloseProducer finish with %s", LogPrefix(), namespaceUri,
                                  producerId, rc.ToString());
        CheckErrorReturn(rc, rsp, FormatString("[S:%s] CloseProducerImpl failed with rc ", namespaceUri), serverApi);
        recorder->SetStatus(rc);
        // recorder should destroy before traceGuard, otherwise, the traceid will be cleaned up
        recorder.reset();
    });

    return Status::OK();
}

void ClientWorkerSCServiceImpl::ConstructCloseProducerReq(std::list<std::string> &streamList, bool forceClose,
                                                          master::CloseProducerReqPb &req) const noexcept
{
    // Common fields for all producers in this list-based CloseProducer call
    req.mutable_worker_address()->set_host(localWorkerAddress_.Host());
    req.mutable_worker_address()->set_port(localWorkerAddress_.Port());
    req.set_force_close(forceClose);
    req.set_redirect(true);

    // Loop over all of the producers and construct the repeating field of the protobuf
    for (const auto &currStream : streamList) {
        auto producerInfoPb = req.add_producer_infos();
        producerInfoPb->set_stream_name(currStream);
    }
    // Clear the stream list. After the master call, this list may be repopulated with any of the producers that got
    // a failure (or remain empty if all closes were successful).
    streamList.clear();
}

Status ClientWorkerSCServiceImpl::HandleCloseProducerRsp(std::list<std::string> &failedList,
                                                         const master::CloseProducerRspPb &rsp) const
{
    Status rc = Status::OK();
    if (rsp.has_err()) {
        rc = Status(static_cast<StatusCode>(rsp.err().error_code()), rsp.err().error_msg());
        std::string failedProds(" [");
        for (const auto &currProducer : rsp.failed_producers()) {
            failedProds += " <S:" + currProducer.stream_name() + ">";
            failedList.emplace_back(currProducer.stream_name());
        }
        LOG(ERROR) << "Worker->Master CloseProducer request failed. Failed producer list:" << failedProds << " ]"
                   << "\nrc: " << rc.ToString();
    }
    return rc;
}

Status ClientWorkerSCServiceImpl::CloseProducerHandleSend(std::shared_ptr<WorkerMasterSCApi> api,
                                                          std::list<std::string> &streamList, bool forceClose)
{
    auto closeProducerFn = [&] {
        std::list<std::string> needCloseList = streamList;
        master::CloseProducerReqPb req;
        ConstructCloseProducerReq(needCloseList, forceClose, req);
        master::CloseProducerRspPb rsp;
        std::function<Status(master::CloseProducerReqPb & req, master::CloseProducerRspPb & rsp)> func =
            [&api](master::CloseProducerReqPb &req, master::CloseProducerRspPb &rsp) {
                return api->CloseProducer(req, rsp);
            };
        // Even for the list batch version of CloseProducer currently groups by the same stream name,
        // so if redirect is needed, all should be retried, so here the retry is part of RedirectRetryWhenMetasMoving
        RETURN_IF_NOT_OK(RedirectRetryWhenMetasMoving(req, rsp, api, func));
        // CloseProducer packs a return code in its rsp. Handle this now and populate the producerList with the
        // producers that failed to close and return if error.
        RETURN_IF_NOT_OK(HandleCloseProducerRsp(needCloseList, rsp));
        INJECT_POINT("CloseProducer.TimeoutInMaster");
        streamList = needCloseList;
        return Status::OK();
    };
    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(closeProducerFn));
}

Status ClientWorkerSCServiceImpl::CloseProducerImpl(const std::string &producerId, const std::string &streamName,
                                                    bool notifyMaster)
{
    // Serialize CreateProducer requests
    CreatePubSubCtrl::Accessor createLock;
    createStreamLocks_.Insert(createLock, streamName);
    Raii releaseLock([this, &createLock] { createStreamLocks_.TryErase(createLock); });
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    bool lastProducer = (streamMgr->GetLocalProducerCount() == 1);
    // We only need to inform master of producer close on last local producer
    if (notifyMaster && lastProducer) {
        auto api = workerMasterApiManager_->GetWorkerMasterApi(streamName, etcdCM_);
        if (api != nullptr) {
            std::list<std::string> streamList;
            streamList.emplace_back(streamName);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                CloseProducerHandleSend(api, streamList, false),
                FormatString("Close Producer [%s] failed in master %s", producerId, api->Address()));
        } else {
            // api object is nullptr and its not force close so dont delete local data
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Worker disconnected from master, stream name = " + streamName);
        }
    }
    // This is a normal close
    RETURN_IF_NOT_OK(streamMgr->CloseProducer(producerId, false));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CloseProducerLocallyOnForceClose(std::list<StreamProducer> &producerList,
                                                                   std::set<std::string> &streamListForNotifications)
{
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Closing %d producers in local stream manager", producerList.size());
    Status returnRc = Status::OK();
    auto iter = std::begin(producerList);
    while (iter != std::end(producerList)) {
        // If any of these calls give an error, record the latest error to the returnRc but continue looping.
        Status rc;
        StreamManagerMap::const_accessor accessor;
        rc = GetStreamManager(iter->streamName_, accessor);
        if (rc.IsError()) {
            returnRc = rc;
            iter++;
            continue;
        }

        std::shared_ptr<StreamManager> streamMgr = accessor->second;
        // This is a force close
        rc = streamMgr->CloseProducer(iter->producerId_, true);
        if (rc.IsError()) {
            returnRc = rc;
            iter++;
            continue;
        }

        // We will later check if its a last producer under lock to send notifications to master
        (void)streamListForNotifications.emplace(iter->streamName_);

        // This one closed successfully. Remove it from the list so that at the end of the call, only the failed ones
        // will remain in the list
        iter = producerList.erase(iter);
    }
    return returnRc;
}

Status ClientWorkerSCServiceImpl::UnlockAndProtect(std::list<StreamProducer> &producerList, uint32_t lockId,
                                                   ProduceGrpByStreamList &producersGrpStreamName)
{
    // Before driving the close work, protect the delete code path from concurrent closes for all of these producers
    auto iter = std::begin(producerList);
    while (iter != std::end(producerList)) {
        auto streamProducer = *iter;
        auto &streamName = streamProducer.streamName_;
        LOG(INFO) << FormatString("Start close producer [%s] in stream [%s] for client lost.",
                                  streamProducer.producerId_, streamName);
        // Only try to unlock once for each stream.
        if (producersGrpStreamName.find(streamName) == producersGrpStreamName.end()) {
            StreamManagerMap::const_accessor accessor;
            Status rc = GetStreamManager(streamName, accessor);
            // The only error here is K_SC_STREAM_NOT_FOUND
            // The stream is likely already deleted so we can remove the producer from the list.
            if (rc.IsError()) {
                iter = producerList.erase(iter);
                continue;
            }
            std::shared_ptr<StreamManager> streamMgr = accessor->second;
            streamMgr->ForceUnlockByCursor(streamProducer.producerId_, true, lockId);
        }
        producersGrpStreamName[streamName].push_back(streamProducer);
        iter++;
    }
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::SendBatchedCloseProducerReq(std::set<std::string> &streamList,
                                                              std::vector<std::string> &failedList)
{
    Status masterCloseRc = Status::OK();
    for (auto &streamName : streamList) {
        CreatePubSubCtrl::Accessor createLock;
        (void)createStreamLocks_.Insert(createLock, streamName);
        Raii releaseLock([this, &createLock] { createStreamLocks_.BlockingErase(createLock); });
        StreamManagerMap::const_accessor accessor;
        RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
        std::shared_ptr<StreamManager> streamMgr = accessor->second;
        // If not last producer dont send the request to master yet
        if (streamMgr->GetLocalProducerCount()) {
            // Ignore the stream dont add it to the failedList
            continue;
        }
        // A custom retry loop to provide some protection around close failures on the master call.
        const int maxRetries = 5;
        int numRetries = 0;
        Status masterCloseRcPerCall;
        std::string masterAddr;
        do {
            CHECK_FAIL_RETURN_STATUS(scTimeoutDuration.CalcRealRemainingTime() > 0, K_RPC_DEADLINE_EXCEEDED,
                                     "Rpc timeout");

            // We only send a CloseProducer Request on last producer close
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[S:%s] Sending close producer to master. Attempt: %d",
                                                      streamName, numRetries);
            auto api = workerMasterApiManager_->GetWorkerMasterApi(streamName, etcdCM_);
            if (api != nullptr) {
                // force close is true
                std::list<std::string> streamList;
                streamList.emplace_back(streamName);
                masterCloseRcPerCall = CloseProducerHandleSend(api, streamList, true);
            } else {
                masterCloseRcPerCall = { StatusCode::K_RPC_UNAVAILABLE, "Master not available for the stream." };
            }
            ++numRetries;
        } while (masterCloseRcPerCall.IsError() && numRetries < maxRetries);

        if (masterCloseRcPerCall.IsError()) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("CloseProducer failed for stream %s with status: %s",
                                                        streamName, masterCloseRcPerCall.ToString());
            masterCloseRc = masterCloseRcPerCall;
            failedList.emplace_back(streamName);
        }
    }
    return masterCloseRc;
}

Status ClientWorkerSCServiceImpl::CloseProducerImplForceClose(uint32_t lockId, std::list<StreamProducer> &producerList)
{
    ProduceGrpByStreamList producersGrpStreamName;

    // Unlock page lock if its hold by crashed producer
    // Get a stream manager accessor on each stream
    // group producers by stream name
    RETURN_IF_NOT_OK(UnlockAndProtect(producerList, lockId, producersGrpStreamName));

    // First locally delete the metadata in worker
    // And Get List of streams that have 0 producers after closing force closed ones.
    std::set<std::string> streamList;
    Status streamCloseRc = CloseProducerLocallyOnForceClose(producerList, streamList);

    INJECT_POINT("ClientWorkerSCServiceImpl.CloseProducerImplForceClose.sleep");

    // Then if required send Close Producer/Consumer to master
    std::vector<std::string> failedStreamList;
    Status masterCloseRc = SendBatchedCloseProducerReq(streamList, failedStreamList);

    producerList.clear();
    // Get all failed producers from streamList and return back to caller
    for (auto &streamName : failedStreamList) {
        producerList.splice(producerList.end(), producersGrpStreamName[streamName]);
    }

    // As this is force close, we do not handle any errors. We just return errors for logging.
    // if both masterCloseRc and StreamCloseRc is set, return the master rc.
    // master closes worked fine, but there were problems closing the producers locally.
    return masterCloseRc.IsError() ? masterCloseRc : streamCloseRc;
}

Status ClientWorkerSCServiceImpl::Subscribe(
    std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    SubscribeReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    auto recorder = std::make_shared<AccessRecorderStreamWrap>(AccessRecorderKey::DS_POSIX_SUBSCRIBE);
    recorder->reqParam.streamName = req.stream_name();
    recorder->reqParam.consumerId = req.consumer_id();
    recorder->reqParam.clientId = req.client_id();
    auto rc = SubscribeInternal(req, recorder, serverApi);
    recorder->SetStatus(rc);
    return rc;
}

Status ClientWorkerSCServiceImpl::SubscribeInternal(
    const SubscribeReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
    std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> serverApi)
{
    LOG(INFO) << "Worker received Subscribe request:" << LogHelper::IgnoreSensitive(req);
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    TimeoutDuration parentDuration = scTimeoutDuration;
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    PerfPoint point(PerfKey::WORKER_CREATE_SUB_ALL);

    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckConnection(namespaceUri), "worker check connection failed");

    // The real work of the close will be driven in another thread. Launch it now and then release this current thread
    // so that it does not hold up the rpc threads.
    auto traceId = Trace::Instance().GetTraceID();
    threadPool_->Execute([=]() mutable {
        scTimeoutDuration = parentDuration;  // lambda capture gets the parents copy. assign the copy to thread local
        Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        SubscribeRspPb rsp;
        Status rc = SubscribeImpl(namespaceUri, req, rsp);
        CheckErrorReturn(rc, rsp, "SubscribeImpl failed with rc ", serverApi);
        recorder->SetStatus(rc);
        // recorder should destroy before traceGuard, otherwise, the traceid will be cleaned up
        recorder.reset();
    });

    return Status::OK();
}

void ClientWorkerSCServiceImpl::ConstructConsumerMetaPb(const std::string &streamName, const std::string &consumerId,
                                                        uint64_t lastAckCursor, const SubscriptionConfig &config,
                                                        const std::string &clientId, ConsumerMetaPb &out) const noexcept
{
    out.set_stream_name(streamName);
    out.mutable_worker_address()->set_host(localWorkerAddress_.Host());
    out.mutable_worker_address()->set_port(localWorkerAddress_.Port());
    out.set_consumer_id(consumerId);
    out.mutable_sub_config()->set_subscription_name(config.subscriptionName);
    out.mutable_sub_config()->set_subscription_type(SubscriptionTypePb(config.subscriptionType));
    out.set_last_ack_cursor(lastAckCursor);
    out.set_client_id(clientId);
}

Status ClientWorkerSCServiceImpl::SubscribeHandleSend(std::shared_ptr<StreamManager> streamMgr,
                                                      const std::string &streamName, const std::string &consumerId,
                                                      uint64_t lastAckCursor, const SubscriptionConfig &config,
                                                      const std::string &clientId, Optional<StreamFields> &streamFields,
                                                      std::string &masterAddress)
{
    auto subscribeFn = [&] {
        std::shared_ptr<WorkerMasterSCApi> api;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(streamName, etcdCM_, api),
                                         "Getting master api failed. stream name = " + streamName);
        masterAddress = api->Address();
        master::SubscribeReqPb masterReq;
        auto &consumerMetaPb = *masterReq.mutable_consumer_meta();
        ConstructConsumerMetaPb(streamName, consumerId, lastAckCursor, config, clientId, consumerMetaPb);
        masterReq.set_redirect(true);
        master::SubscribeRspPb masterRsp;
        std::function<Status(master::SubscribeReqPb & req, master::SubscribeRspPb & rsp)> func =
            [&api](master::SubscribeReqPb &req, master::SubscribeRspPb &rsp) { return api->Subscribe(req, rsp); };
        Status rc = RedirectRetryWhenMetaMoving(masterReq, masterRsp, api, func);
        // The purpose of the SetRetainData is to notify the local producer,
        // in that case we should not set it from INIT to RETAIN if there is no local producer.
        if (rc.IsOk() && masterRsp.retain_data() == RetainDataState::State::NOT_RETAIN) {
            streamMgr->SetRetainData(masterRsp.retain_data());
        }
        Optional<StreamFields> rspStreamFields(masterRsp.max_stream_size(), masterRsp.page_size(),
                                               masterRsp.auto_cleanup(), masterRsp.retain_num_consumer(),
                                               masterRsp.encrypt_stream(), masterRsp.reserve_size(),
                                               masterRsp.stream_mode());
        // Assign the initialized Optional to the output fields if the data is not empty.
        // Otherwise the optional value will remain false.
        if (!rspStreamFields.value().Empty()) {
            streamFields = rspStreamFields;
        }
        INJECT_POINT("ClientWorkerSC.Subscribe.TimingHole");
        return rc;
    };
    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(subscribeFn));
}

Status ClientWorkerSCServiceImpl::SubscribeImpl(const std::string &namespaceUri, const SubscribeReqPb &req,
                                                SubscribeRspPb &rsp)
{
    // We need to serialize on CreateProducer and Subscribe.
    // That is, we must wait for the previous one to finish (or rollback) successfully
    // before the next pub/sub can proceed
    CreatePubSubCtrl::Accessor createLock;
    createStreamLocks_.Insert(createLock, namespaceUri);
    Raii releaseLock([this, &createLock] { createStreamLocks_.TryErase(createLock); });

    // Find the StreamManager by request.streamName.
    const std::string &subName = req.subscription_config().subscription_name();
    const std::string &consumerId = req.consumer_id();

    std::shared_ptr<StreamManager> streamMgr;
    Optional<StreamFields> streamFields;  // optional is false to start

    // Next to create the stream manager if it doesn't exist.
    std::shared_ptr<StreamManagerWithLock> streamMgrWithLock;
    bool streamExisted;
    RETURN_IF_NOT_OK(CreateStreamManagerIfNotExist(namespaceUri, streamFields, streamMgrWithLock, streamExisted));
    streamMgr = streamMgrWithLock->mgr_;
    if (streamExisted) {
        // An existing stream was found.
        RETURN_IF_NOT_OK(streamMgr->CheckIfStreamActive());
    }
    // If we hit any error below, we will erase the StreamManager from the tbb provided it is this thread
    // that creates the stream manager. We will need an exclusive accessor
    Raii raii([this, &namespaceUri, &streamMgrWithLock]() {
        streamMgrWithLock->CleanUp(std::bind(&ClientWorkerSCServiceImpl::EraseFromStreamMgrDictWithoutLck, this,
                                             namespaceUri, std::placeholders::_1));
    });

    // Add consumer into the SubscriptionGroup we found above.
    SubscriptionConfig config(subName, static_cast<SubscriptionType>(req.subscription_config().subscription_type()));

    uint64_t lastAckCursor = 0;
    // Request master to update topological structure for every new consumer.
    ShmView waView;
    // Shm page reservation happens after adding to subs_, so we do not need to synchronize with EarlyReclaim().
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamMgr->AddConsumer(config, consumerId, lastAckCursor, waView),
                                     "streamMgr add consumer failed");

    // We will let go the accessor at this point to prevent deadlock. The master may send back a SyncPubNode
    // rpc back to this worker if this is the first consumer. We are still protected by the createLock
    streamMgrWithLock->Release();
    std::string masterAddress;
    Status rc = SubscribeHandleSend(streamMgr, namespaceUri, consumerId, lastAckCursor, config, req.client_id(),
                                    streamFields, masterAddress);
    // Ignore if the master doesn't have the stream fields yet. These will be set by CreateProducer later and the master
    // will drive notification to set it here in the consumer.
    if ((rc.IsOk() || rc.GetCode() == StatusCode::K_DUPLICATED) && streamFields && !streamFields->Empty()) {
        // Reserve one page of memory if we know the page size from the master.
        rc = PostCreateStreamManager(streamMgr, streamFields, true);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[%s, S:%s, Sub:%s, C:%s] AddConsumer results in %s. Undo master meta data",
                                       LogPrefix(), namespaceUri, subName, consumerId, rc.ToString());
            LOG_IF_ERROR(CloseConsumerImpl(consumerId, namespaceUri, subName, true, WORKER_LOCK_ID, false),
                         "Undo subscription");
            return rc;
        }
        if (StreamManager::EnableSharedPage(streamFields->streamMode_)) {
            ShmView shmViewOfStreamMeta;
            RETURN_IF_NOT_OK(streamMgr->GetOrCreateShmMeta(TenantAuthManager::Instance()->ExtractTenantId(namespaceUri),
                                                           shmViewOfStreamMeta));
        }
    }
    if (rc.IsError() && rc.GetCode() != StatusCode::K_DUPLICATED) {
        LOG(ERROR) << FormatString("Create Consumer [%s] failed in master [%s], error msg: %s", consumerId,
                                   masterAddress, rc.GetMsg());
        // If fail on master, we should roll back this operation on local node.
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamMgr->CloseConsumer(subName, consumerId),
                                         "streamMgr close consumer failed");
        return rc;
    }

    // Response for client.
    rsp.set_last_recv_cursor(lastAckCursor);
    rsp.set_worker_fd(waView.fd);
    rsp.set_mmap_size(waView.mmapSz);
    rsp.set_offset(waView.off);
    rsp.set_size(waView.sz);
    {
        std::unique_lock<std::shared_timed_mutex> lock(clearMutex_);
        clientConsumers_[req.client_id()].emplace_back(namespaceUri, subName, consumerId);
    }
    LOG(INFO) << FormatString("[%s, S:%s, C:%s] Subscribe(create consumer) success on master %s", LogPrefix(),
                              namespaceUri, consumerId, masterAddress);
    streamMgrWithLock->needCleanUp = false;
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CloseConsumer(
    std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    CloseConsumerReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    auto recorder = std::make_shared<AccessRecorderStreamWrap>(AccessRecorderKey::DS_POSIX_CLOSE_CONSUMER);
    recorder->reqParam.streamName = req.stream_name();
    recorder->reqParam.consumerId = req.consumer_id();
    recorder->reqParam.subscriptionName = req.subscription_name();
    recorder->reqParam.clientId = req.client_id();
    auto rc = CloseConsumerInternal(req, recorder, serverApi);
    recorder->SetStatus(rc);
    return rc;
}

Status ClientWorkerSCServiceImpl::CloseConsumerInternal(
    const CloseConsumerReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
    std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> serverApi)
{
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    TimeoutDuration parentDuration = scTimeoutDuration;
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    LOG(INFO) << "Worker received CloseConsumer request:" << LogHelper::IgnoreSensitive(req);
    const std::string &consumerId = req.consumer_id();
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckConnection(namespaceUri), "worker check connection failed");
    const std::string &clientId = req.client_id();

    // The real work of the close will be driven in another thread. Launch it now and then release this current thread
    // so that it does not hold up the rpc threads.
    auto traceId = Trace::Instance().GetTraceID();
    threadPool_->Execute([=]() mutable {
        scTimeoutDuration = parentDuration;  // lambda capture gets the parents copy. assign the copy to thread local
        Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Status rc = CloseConsumerImpl(consumerId, namespaceUri, req.subscription_name(), true, WORKER_LOCK_ID);
        if (rc.IsOk() || rc.GetCode() == StatusCode::K_SC_CONSUMER_NOT_FOUND) {
            std::unique_lock<std::shared_timed_mutex> lock(clearMutex_);
            clientConsumers_[clientId].remove_if([&namespaceUri, &consumerId](const SubInfo &data) {
                return (data.streamName == namespaceUri && data.consumerId == consumerId);
            });
        }
        LOG(INFO) << FormatString("[%s, S:%s, C:%s] CloseConsumer finish with %s", LogPrefix(), namespaceUri,
                                  consumerId, rc.ToString());
        // Flow replies back for unary rpc
        if (rc.IsOk()) {
            // Success case, flow the response back to client (rc of OK is inferred)
            CloseConsumerRspPb rsp;
            LOG_IF_ERROR(serverApi->Write(rsp), "Write reply to client stream failed");
        } else {
            // Error case, flow the rc back to client
            LOG_IF_ERROR(serverApi->SendStatus(rc), "Write reply to client stream failed");
        }
        recorder->SetStatus(rc);
        // recorder should destroy before traceGuard, otherwise, the traceid will be cleaned up
        recorder.reset();
    });

    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CloseConsumerImpl(const std::string &consumerId, const std::string &streamName,
                                                    const std::string &subName, bool notifyMaster, uint32_t lockId,
                                                    bool forceClose)
{
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    if (lockId > WORKER_LOCK_ID) {
        streamMgr->ForceUnlockByCursor(consumerId, false, lockId);
    }
    SubscriptionType subType;
    // Obtain subscription type to construct subscription config.
    RETURN_IF_NOT_OK(streamMgr->GetSubType(subName, subType));
    SubscriptionConfig subConfig(subName, subType);
    Status rc;
    if (notifyMaster) {
        std::string masterAddr;
        auto closeConsumerFn = [&] {
            // We don't care about lastAckCursor change when close consumer, so we set it as 0.
            std::shared_ptr<WorkerMasterSCApi> api;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(streamName, etcdCM_, api),
                                             "Getting master api failed. stream name = " + streamName);
            masterAddr = api->Address();
            master::CloseConsumerReqPb req;
            auto &consumerMetaPb = *req.mutable_consumer_meta();
            ConstructConsumerMetaPb(streamName, consumerId, 0, subConfig, "", consumerMetaPb);
            req.set_redirect(true);
            master::CloseConsumerRspPb rsp;
            std::function<Status(master::CloseConsumerReqPb & req, master::CloseConsumerRspPb & rsp)> func =
                [&api](master::CloseConsumerReqPb &req, master::CloseConsumerRspPb &rsp) {
                    return api->CloseConsumer(req, rsp);
                };
            return RedirectRetryWhenMetaMoving(req, rsp, api, func);
        };
        rc = WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                               std::move(closeConsumerFn));
        LOG_IF_ERROR(rc, FormatString("Close Consumer [%s] failed in master [%s]: %s. ForceClose: %s", consumerId,
                                      masterAddr, rc.GetMsg(), forceClose ? "true" : "false"));
    }
    if (rc.IsError() && !forceClose) {
        return rc;
    }
    return streamMgr->CloseConsumer(subName, consumerId);
}

Status ClientWorkerSCServiceImpl::GetDataPage(
    std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    GetDataPageReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Worker(%s) receive GetDataPage request, namespaceUri: %s",
                                              localWorkerAddress_.ToString(), namespaceUri);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetStreamManager(namespaceUri, accessor), "worker get stream manager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    RETURN_IF_NOT_OK(streamManager->CheckIfStreamActive());  // Check for Reset or Delete
    std::shared_ptr<Subscription> subscription;
    RETURN_IF_NOT_OK(streamManager->GetSubscription(req.subscription_name(), subscription));
    return streamManager->GetDataPage(req, subscription, serverApi);
}

Status ClientWorkerSCServiceImpl::GetLastAppendCursor(const LastAppendCursorReqPb &req, LastAppendCursorRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetStreamManager(namespaceUri, accessor), "worker get stream manager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    rsp.set_last_append_cursor(streamManager->GetLastAppendCursor());

    return Status::OK();
}

template <typename W, typename R>
void ClientWorkerSCServiceImpl::AsyncSendMemReq(const std::string &namespaceUri)
{
    auto traceId = Trace::Instance().GetTraceID();
    memAllocPool_->Execute([this, namespaceUri, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Status rc = this->template HandleBlockedRequestImpl<W, R>(namespaceUri);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("HandleBlockedRequestImpl failed for %s. %s", namespaceUri, rc.ToString());
        }
    });
}

Status ClientWorkerSCServiceImpl::CreateShmPage(
    std::shared_ptr<ServerUnaryWriterReader<CreateShmPageRspPb, CreateShmPageReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    PerfPoint point(PerfKey::WORKER_CREATE_WRITE_PAGE_ALL);
    CreateShmPageReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");

    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");

    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] CreateShmPage request: %s", LogPrefix(), namespaceUri,
                                              LogHelper::IgnoreSensitive(req));
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(namespaceUri, accessor));
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    RETURN_IF_NOT_OK(streamManager->CheckIfStreamActive());  // Check for Reset or Delete
    // Create a blocked request for FIFO. There can be some previous requests that got OOM and are still waiting.
    auto fn = std::bind(&StreamManager::AllocDataPage, streamManager, std::placeholders::_1);
    auto blockedReq = std::make_unique<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>>(
        namespaceUri, req, streamManager->GetStreamPageSize(), serverApi, fn);
    // Lock to compete with StreamManager::UnblockProducers
    RETURN_IF_NOT_OK(streamManager->AddBlockedCreateRequest(this, std::move(blockedReq), true));
    AsyncSendMemReq<CreateShmPageRspPb, CreateShmPageReqPb>(namespaceUri);
    return Status::OK();
}

template <typename W, typename R>
Status ClientWorkerSCServiceImpl::HandleBlockedRequestImpl(const std::string &streamName)
{
    StreamManagerMap::const_accessor accessor;
    PerfPoint point1(PerfKey::WORKER_CREATE_PAGE_GET_STREAM_MANAGER);
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    point1.Record();
    // Check the next request.
    size_t nextReqSz;
    bool bigElement;
    std::tie(nextReqSz, bigElement) = streamMgr->GetNextBlockedRequestSize();
    // Big element doesn't go through the ack chain, and we will focus more
    // on CreateShmPage request which incur a lot of lock conflicts with the ack thread
    if (!(bigElement || streamMgr->CheckHadEnoughMem(nextReqSz))) {
        // We will try to release pages (if any) as if this thread is doing ack manually
        // Part of the ack process may also call StreamManager::HandleBlockedRequestImpl
        // in which case we can expect StreamMgr::GetBlockedCreateRequest below can return
        // K_TRY_AGAIN
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s S:%s] Most likely OOM. Reclaim memory", LogPrefix(), streamName);
        streamMgr->AckCursors();
    }
    std::shared_ptr<BlockedCreateRequest<W, R>> blockedReq;
    INJECT_POINT("HandleBlockedRequestImpl.sleep");
    auto rc = streamMgr->GetBlockedCreateRequest(blockedReq);
    RETURN_OK_IF_TRUE(rc.GetCode() == K_TRY_AGAIN);
    RETURN_IF_NOT_OK(rc);
    // Treat OOM as normal. HandleBlockedRequestImpl will re-queue the request
    RETURN_IF_NOT_OK_EXCEPT(streamMgr->template HandleBlockedRequestImpl(std::move(blockedReq), true), K_OUT_OF_MEMORY);
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::AllocBigShmMemory(
    std::shared_ptr<ServerUnaryWriterReader<CreateLobPageRspPb, CreateLobPageReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    CreateLobPageReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Worker(%s) receive AllocBigShmMemory request, namespaceUri: %s",
                                              localWorkerAddress_.ToString(), namespaceUri);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetStreamManager(namespaceUri, accessor), "worker get stream manager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    RETURN_IF_NOT_OK(streamManager->CheckIfStreamActive());  // Check for Reset or Delete
    // Create a blocked request for FIFO. There can be some previous requests that got OOM and are still waiting.
    auto fn = std::bind(&StreamManager::AllocBigShmMemory, streamManager, std::placeholders::_1);
    auto blockedReq = std::make_unique<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>>(
        namespaceUri, req, req.page_size(), serverApi, fn);
    // Lock to compete with StreamManager::UnblockProducers
    RETURN_IF_NOT_OK(streamManager->AddBlockedCreateRequest(this, std::move(blockedReq), true));
    AsyncSendMemReq<CreateLobPageRspPb, CreateLobPageReqPb>(namespaceUri);
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::ReleaseBigShmMemory(
    std::shared_ptr<ServerUnaryWriterReader<ReleaseLobPageRspPb, ReleaseLobPageReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    ReleaseLobPageReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Worker(%s) receive ReleaseBigShmMemory request, namespaceUri: %s",
                                              localWorkerAddress_.ToString(), namespaceUri);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetStreamManager(namespaceUri, accessor), "worker get stream manager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    RETURN_IF_NOT_OK(streamManager->CheckIfStreamActive());  // Check for Reset or Delete
    RETURN_IF_NOT_OK(streamManager->ReleaseBigShmMemory(serverApi, req));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::DeleteStreamLocally(StreamManagerMap::accessor &accessor)
{
    std::shared_ptr<StreamManager> streamManager = accessor->second;
    auto streamName = streamManager->GetStreamName();
    // Earlier topo change should have removed the stream from RWM. But no harm to call it again
    // but just expect the error stream not found
    RETURN_IF_NOT_OK_EXCEPT(remoteWorkerManager_->DeleteStream(streamName), K_SC_STREAM_NOT_FOUND);
    bool success = streamMgrDict_.erase(accessor);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_RUNTIME_ERROR,
                                         FormatString("Failed erase stream %s from streamMgrDict_", streamName));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::DeleteStream(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp)
{
    AccessRecorder recorder(AccessRecorderKey::DS_POSIX_DELETE_STREAM);
    auto rc = DeleteStreamImpl(req, rsp);
    StreamRequestParam reqParam;
    reqParam.streamName = req.stream_name();
    reqParam.clientId = req.client_id();
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status ClientWorkerSCServiceImpl::DeleteStreamImpl(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    (void)rsp;
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    PerfPoint point(PerfKey::WORKER_DELETE_STREAM_ALL);
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckConnection(namespaceUri), "worker check connection failed");
    LOG(INFO) << FormatString("Worker(%s) received DeleteStream request, namespaceUri: %s",
                              localWorkerAddress_.ToString(), namespaceUri);
    bool needsRollback = true;
    {
        StreamManagerMap::const_accessor accessor;
        if (GetStreamManager(namespaceUri, accessor).IsOk()) {  // If exists on local worker node
            std::shared_ptr<StreamManager> streamManager = accessor->second;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamManager->CheckDeleteStreamCondition(),
                                             "streamManager check delete stream condition failed");
            // Set the state to DELETE_IN_PROGRESS to prevent a timing hole after
            // the master is updated but before the stream manager is removed from tbb.
            Status rc = streamManager->SetDeleteState();
            if (rc.IsError()) {
                // If delete is already in progress
                if (rc.GetCode() == StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS) {
                    // Then this function is a no op
                    // No need to decrement because without ignore, RefCount does not increase
                    return Status::OK();
                }
                // No need to decrement because on error, RefCount does not increase
                return rc;  // Else return any other error
            }
        }
        accessor.release();
    }
    Raii unsetDelete([this, &needsRollback, &namespaceUri]() {
        StreamManagerMap::const_accessor accessor;
        if (needsRollback && GetStreamManager(namespaceUri, accessor).IsOk()) {
            std::shared_ptr<StreamManager> streamManager = accessor->second;
            LOG(INFO) << FormatString("[S:%s] Setting Active State", namespaceUri);
            streamManager->SetActiveState();
        }
    });
    INJECT_POINT("ClientWorkerSCServiceImpl.DELETE_IN_PROGRESS.sleep");
    // We call master to process broadcast to all other workers, except this node, that's related to this stream.
    Status rc = DeleteStreamHandleSend(namespaceUri);
    // Still proceed to handle DeleteStreamLocally even if K_NOT_FOUND is returned.
    INJECT_POINT("ClientWorkerSCServiceImpl.DeleteStreamHandleSend.sleep");
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    // Now we need an exclusive accessor lock when we remove the stream from the tbb map
    LOG(INFO) << FormatString("[S:%s] Waiting for stream to be free of use", namespaceUri);
    INJECT_POINT("ClientWorkerSCServiceImpl.DeleteStreamLocally.sleep");
    bool success;
    {
        ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        StreamManagerMap::accessor accessor;
        success = streamMgrDict_.find(accessor, namespaceUri);
        if (success) {
            RETURN_IF_NOT_OK(DeleteStreamLocally(accessor));
        }
    }
    LOG_IF(INFO, !success) << FormatString("[S:%s] Stream manager is already gone", namespaceUri);
    LOG(INFO) << FormatString("[%s, S:%s] DeleteStream success.", LogPrefix(true), namespaceUri);
    needsRollback = false;
    return success ? Status::OK() : rc;
}

Status ClientWorkerSCServiceImpl::DeleteStreamHandleSend(const std::string &streamName)
{
    auto deleteFn = [&] {
        std::shared_ptr<WorkerMasterSCApi> api;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(streamName, etcdCM_, api),
                                         "Getting master api failed. stream name = " + streamName);
        master::DeleteStreamReqPb masterReq;
        masterReq.set_stream_name(streamName);
        masterReq.mutable_src_node_addr()->set_host(localWorkerAddress_.Host());
        masterReq.mutable_src_node_addr()->set_port(localWorkerAddress_.Port());
        masterReq.set_redirect(true);
        master::DeleteStreamRspPb masterRsp;
        std::function<Status(master::DeleteStreamReqPb & req, master::DeleteStreamRspPb & rsp)> func =
            [&api](master::DeleteStreamReqPb &req, master::DeleteStreamRspPb &rsp) {
                return api->DeleteStream(req, rsp);
            };
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RedirectRetryWhenMetaMoving(masterReq, masterRsp, api, func),
                                         "workerMasterApi delete stream failed on master " + api->Address());
        return Status::OK();
    };
    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(deleteFn));
}

Status ClientWorkerSCServiceImpl::QueryGlobalProducersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    AccessRecorder recorder(AccessRecorderKey::DS_POSIX_QUERY_PRODUCERS_NUM);
    auto rc = QueryGlobalProducersNumImpl(req, rsp);
    StreamRequestParam reqParam;
    reqParam.streamName = req.stream_name();
    reqParam.clientId = req.client_id();
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    rspParam.count = Optional<uint64_t>(rsp.global_count());
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status ClientWorkerSCServiceImpl::QueryGlobalProducersNumImpl(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    LOG(INFO) << FormatString("Worker(%s) received QueryGlobalProducersNum request, namespaceUri: %s",
                              localWorkerAddress_.ToString(), namespaceUri);
    auto queryFn = [&] {
        std::shared_ptr<WorkerMasterSCApi> api;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(namespaceUri, etcdCM_, api),
                                         "Getting master api failed. stream name = " + namespaceUri);
        master::QueryGlobalNumReqPb masterReq;
        masterReq.set_stream_name(namespaceUri);
        masterReq.set_redirect(true);
        master::QueryGlobalNumRsqPb masterRsp;
        std::function<Status(master::QueryGlobalNumReqPb & req, master::QueryGlobalNumRsqPb & rsp)> func =
            [&api](master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) {
                return api->QueryGlobalProducersNum(req, rsp);
            };
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RedirectRetryWhenMetaMoving(masterReq, masterRsp, api, func),
            "workerMasterApi query global producers number failed on master" + api->Address());
        rsp.set_global_count(masterRsp.global_count());
        LOG(INFO) << "worker QueryGlobalProducersNum done, namespaceUri: " << namespaceUri;
        return Status::OK();
    };
    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(queryFn));
}

Status ClientWorkerSCServiceImpl::QueryGlobalConsumersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    AccessRecorder recorder(AccessRecorderKey::DS_POSIX_QUERY_CONSUMERS_NUM);
    auto rc = QueryGlobalConsumersNumImpl(req, rsp);
    StreamRequestParam reqParam;
    reqParam.streamName = req.stream_name();
    reqParam.clientId = req.client_id();
    StreamResponseParam rspParam;
    rspParam.msg = rc.GetMsg();
    rspParam.count = Optional<uint64_t>(rsp.global_count());
    recorder.Record(rc.GetCode(), reqParam, rspParam);
    return rc;
}

Status ClientWorkerSCServiceImpl::QueryGlobalConsumersNumImpl(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.stream_name());
    LOG(INFO) << FormatString("Worker(%s) received QueryGlobalConsumersNum request, namespaceUri: %s",
                              localWorkerAddress_.ToString(), namespaceUri);
    auto queryFn = [&] {
        std::shared_ptr<WorkerMasterSCApi> api;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApiManager_->GetWorkerMasterApi(namespaceUri, etcdCM_, api),
                                         "Getting master api failed. stream name = " + namespaceUri);
        master::QueryGlobalNumReqPb masterReq;
        masterReq.set_stream_name(namespaceUri);
        masterReq.set_redirect(true);
        master::QueryGlobalNumRsqPb masterRsp;
        std::function<Status(master::QueryGlobalNumReqPb & req, master::QueryGlobalNumRsqPb & rsp)> func =
            [&api](master::QueryGlobalNumReqPb &req, master::QueryGlobalNumRsqPb &rsp) {
                return api->QueryGlobalConsumersNum(req, rsp);
            };
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RedirectRetryWhenMetaMoving(masterReq, masterRsp, api, func),
            "workerMasterApi query global consumers number failed on master " + api->Address());
        rsp.set_global_count(masterRsp.global_count());
        LOG(INFO) << "worker QueryGlobalConsumersNum done, namespaceUri: " << namespaceUri;
        return Status::OK();
    };

    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(queryFn));
}

std::string ClientWorkerSCServiceImpl::LogPrefix(bool withAddress) const
{
    if (withAddress) {
        return FormatString("ClientWorkerSvc, Node:%s", localWorkerAddress_.ToString());
    } else {
        return "ClientWorkerSvc";
    }
}

Status ClientWorkerSCServiceImpl::SendBlockProducerReq(const std::string &streamName,
                                                       const std::string &remoteWorkerAddr)
{
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    RETURN_IF_NOT_OK(streamMgr->BlockProducer(remoteWorkerAddr, false));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::SendUnBlockProducerReq(const std::string &streamName,
                                                         const std::string &remoteWorkerAddr)
{
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    RETURN_IF_NOT_OK(streamMgr->UnBlockProducer(remoteWorkerAddr));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CreateStreamManagerIfNotExist(
    const std::string &streamName, const Optional<StreamFields> &streamFields,
    std::shared_ptr<StreamManagerWithLock> &streamMgrWithLock, bool &streamExisted)
{
    auto rlock = std::make_unique<StreamManagerMap::const_accessor>();
    streamExisted = streamMgrDict_.find(*rlock, streamName);
    if (streamExisted) {
        // An existing stream was found.
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Stream already exists in StreamManager", LogPrefix(),
                                                  streamName);
        auto streamMgr = (*rlock)->second;
        streamMgrWithLock =
            std::make_shared<StreamManagerWithLock>(streamMgr, rlock.release(), false, shared_from_this());
    } else {
        auto xlock = std::make_unique<StreamManagerMap::accessor>();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateStreamManagerImpl(streamName, streamFields, *xlock),
                                         "worker create stream manager failed");
        auto streamMgr = (*xlock)->second;
        streamMgrWithLock =
            std::make_shared<StreamManagerWithLock>(streamMgr, xlock.release(), true, shared_from_this());
    }
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::PostCreateStreamManager(const std::shared_ptr<StreamManager> &streamManager,
                                                          const Optional<StreamFields> &streamFields, bool reserveShm)
{
    RETURN_IF_NOT_OK(streamManager->CheckIfStreamActive());
    // If the stream fields are passed in then assign them to this existing stream
    // manager. This code may fail a verification check if the existing stream was not empty and has mismatching
    // settings.
    if (streamFields && !streamFields.value().Empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(streamManager->UpdateStreamFields(streamFields.value(), reserveShm),
                                         "streamMgr verify failed");
    }
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::CreateStreamManagerImpl(const std::string &streamName,
                                                          const Optional<StreamFields> &streamFields,
                                                          StreamManagerMap::accessor &accessor)
{
    PerfPoint point(PerfKey::WORKER_CREATE_STREAM_MANAGER_LOGIC);
    auto streamNo = ++lifetimeLocalStreamCount_;
    bool needRollback = streamMgrDict_.emplace(
        accessor, std::make_pair(streamName, std::make_shared<StreamManager>(
                                                 streamName, remoteWorkerManager_.get(), localWorkerAddress_.ToString(),
                                                 akSkManager_, weak_from_this(), scAllocateManager_,
                                                 workerWorkerSCService_, streamNo)));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        needRollback, K_DUPLICATED,
        FormatString("[%s, S:%s] Stream already exists in StreamManager", LogPrefix(), streamName));
    // If we hit any error below, we will erase the StreamManager from the tbb
    Raii raii([this, &accessor, &needRollback]() {
        if (needRollback) {
            streamMgrDict_.erase(accessor);
        }
    });
    INJECT_POINT("ClientWorkerSCServiceImpl.CreateStreamManagerImpl.StreamNo_Sleep");
    RETURN_IF_NOT_OK(AddStreamNo(streamNo, streamName));
    auto &streamManager = accessor->second;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s, S:%s] Create stream in StreamManager.", LogPrefix(), streamName);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        streamManager->CreatePageQueueHandler(streamFields),
        FormatString("[%s, S:%s] Fail to create page queue handler", LogPrefix(), streamName));
    if (ScMetricsMonitor::Instance()->IsEnabled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            streamManager->InitStreamMetrics(),
            FormatString("[%s, S:%s] Fail to init stream metrics", LogPrefix(), streamName));
    }
    needRollback = false;
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::GetStreamManager(const std::string &streamName,
                                                   StreamManagerMap::const_accessor &accessor)
{
    PerfPoint point(PerfKey::WORKER_CREATE_STREAM_MANAGER_GET_LOCK);
    ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    point.RecordAndReset(PerfKey::WORKER_GET_STREAM_MANAGER);
    auto success = streamMgrDict_.find(accessor, streamName);
    CHECK_FAIL_RETURN_STATUS(success, K_SC_STREAM_NOT_FOUND,
                             FormatString("[%s] Stream %s not found", LogPrefix(), streamName));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::ClosePubSubForClientLost(const std::string &clientId)
{
    std::shared_ptr<ClientInfo> clientInfo;
    clientInfo = ClientManager::Instance().GetClientInfo(clientId);
    if (clientInfo == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "invalid client id");
    }
    uint32_t lockId;
    RETURN_IF_NOT_OK(clientInfo->GetLockId(lockId));
    LOG(INFO) << "Client Lost: Begin to close producers and consumers for client: " << clientId
              << " lock id: " << lockId;

    std::list<StreamProducer> producerList;
    std::list<SubInfo> consumerList;
    Status returnRc = Status::OK();
    {
        std::lock_guard<std::shared_timed_mutex> lock(clearMutex_);
        producerList = std::move(clientProducers_[clientId]);
        consumerList = std::move(clientConsumers_[clientId]);
        (void)clientProducers_.erase(clientId);
        (void)clientConsumers_.erase(clientId);
    }
    std::set<std::string> streams;
    for (const auto &producer : producerList) {
        (void)streams.emplace(producer.streamName_);
    }
    for (const auto &consumer : consumerList) {
        (void)streams.emplace(consumer.streamName);
    }

    ForceUnlockMemViemForPages(streams, lockId);

    if (!producerList.empty()) {
        Status rc = CloseProducerImplForceClose(lockId, producerList);
        LOG(INFO) << FormatString("Close producers for client %s finished with %s", clientId, rc.ToString());
        if (rc.IsError()) {
            // Unsuccessful to close at least one of the producers. Add any failed producers back to the client list
            // for this client. Do not quit this function yet.  Continue to try to close amy consumers as well.
            std::lock_guard<std::shared_timed_mutex> lock(clearMutex_);
            clientProducers_[clientId] = std::move(producerList);
            returnRc = rc;
        }
    }

    // Consumer close does not have list-based close at this time. Loop over each consumer and close it.
    std::list<SubInfo> failedConsumers;
    const bool forceClose = true;
    for (const auto &consumerInfo : consumerList) {
        Status rc = CloseConsumerImpl(consumerInfo.consumerId, consumerInfo.streamName, consumerInfo.subName, true,
                                      lockId, forceClose);
        LOG(INFO) << FormatString("Close consumer [%s] in stream [%s] for client %s finished with %s",
                                  consumerInfo.consumerId, consumerInfo.streamName, clientId, rc.ToString());
        if (rc.IsError()) {
            // Track the consumer that failed.
            failedConsumers.emplace_back(consumerInfo.streamName, consumerInfo.subName, consumerInfo.consumerId);
            returnRc = rc;
        }
    }

    if (!failedConsumers.empty()) {
        // Add any consumers that failed to close back to the client tracking
        std::lock_guard<std::shared_timed_mutex> lock(clearMutex_);
        clientConsumers_[clientId] = std::move(failedConsumers);
    }

    // At this point, if returnRc is not OK, then the client has not been properly closed and there still exists
    // producers and/or consumers that have not been cleaned up yet.
    return returnRc;
}

void ClientWorkerSCServiceImpl::ForceUnlockMemViemForPages(const std::set<std::string> &streams, uint32_t lockId)
{
    Timer timer;
    for (const auto &streamName : streams) {
        StreamManagerMap::const_accessor accessor;
        Status rc = GetStreamManager(streamName, accessor);
        if (rc.IsError()) {
            continue;
        }
        accessor->second->ForceUnlockMemViemForPages(lockId);
    }
    LOG(INFO) << "ForceUnlockMemViemForPages for stream count: " << streams.size()
              << ", cost:" << timer.ElapsedMilliSecond() << "ms";
}

void ClientWorkerSCServiceImpl::GetProducerConsumerMetadata(
    std::vector<std::string> &localProducers, std::vector<std::pair<std::string, SubscriptionConfig>> &localConsumers,
    GetStreamMetadataRspPb *meta, const std::string &streamName, HostPortPb &hostPortPb)
{
    // If there exists a producer, set the ProducerPb
    if (localProducers.size()) {
        auto producerPb = meta->add_producers();
        producerPb->set_stream_name(streamName);
        producerPb->mutable_worker_address()->CopyFrom(hostPortPb);
        // This number will be 1 as master dont have to know about actual count
        producerPb->set_producer_count(1);
    }

    for (const auto &consumer : localConsumers) {
        std::string foundClientId = "";
        auto comparator = [&consumer](const SubInfo conInfo) { return consumer.first == conInfo.consumerId; };
        for (const auto &[clientId, conList] : clientConsumers_) {
            auto iter = std::find_if(conList.begin(), conList.end(), comparator);
            if (iter != conList.end()) {
                foundClientId = clientId;
                break;
            }
        }
        if (foundClientId.empty()) {
            LOG(ERROR) << "Client id not found for consumer: " << consumer.first;
            continue;
        }
        auto consumerPb = meta->add_consumers();
        consumerPb->set_client_id(std::move(foundClientId));
        consumerPb->set_consumer_id(consumer.first);
        consumerPb->set_stream_name(streamName);
        const auto &config = consumer.second;
        consumerPb->mutable_sub_config()->set_subscription_name(config.subscriptionName);
        consumerPb->mutable_sub_config()->set_subscription_type(SubscriptionTypePb(config.subscriptionType));
        consumerPb->mutable_worker_address()->CopyFrom(hostPortPb);
    }
}

Status ClientWorkerSCServiceImpl::GetStreamMetadata(const std::string &streamName, GetStreamMetadataRspPb *meta)
{
    std::shared_lock<std::shared_timed_mutex> lock(clearMutex_);
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetStreamManager(streamName, accessor), "GetStreamManager failed");
    std::shared_ptr<StreamManager> streamManager = accessor->second;

    // Get pub and sub metadata.
    std::vector<std::string> localProducers;
    std::vector<std::pair<std::string, SubscriptionConfig>> localConsumers;
    streamManager->GetLocalProducers(localProducers);
    streamManager->GetLocalConsumers(localConsumers);
    meta->set_is_remote_pub_empty(streamManager->IsRemotePubEmpty());

    StreamFields fields;
    streamManager->GetStreamFields(fields);
    meta->set_max_stream_size(fields.maxStreamSize_);
    meta->set_page_size(fields.pageSize_);
    meta->set_auto_cleanup(fields.autoCleanup_);
    meta->set_retain_num_consumer(fields.retainForNumConsumers_);
    meta->set_encrypt_stream(fields.encryptStream_);
    meta->set_reserve_size(fields.reserveSize_);
    meta->set_stream_mode(fields.streamMode_);

    HostPortPb hostPortPb;
    hostPortPb.set_host(localWorkerAddress_.Host());
    hostPortPb.set_port(localWorkerAddress_.Port());

    GetProducerConsumerMetadata(localProducers, localConsumers, meta, streamName, hostPortPb);
    return Status::OK();
}

std::vector<std::string> ClientWorkerSCServiceImpl::GetStreamNameList()
{
    std::vector<std::string> streamNames;
    Timer t;
    WriteLockHelper wlock(STREAM_COMMON_LOCK_ARGS(mutex_));
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Time to acquire mutex_: [%.6lf]s", LogPrefix(),
                                                t.ElapsedSecond());
    auto iter = streamMgrDict_.begin();
    while (iter != streamMgrDict_.end()) {
        streamNames.emplace_back(iter->first);
        ++iter;
    }
    return streamNames;
}

Status ClientWorkerSCServiceImpl::SendAllStreamMetadata(
    const GetMetadataAllStreamReqPb &req,
    std::shared_ptr<ServerWriterReader<GetStreamMetadataRspPb, GetMetadataAllStreamReqPb>> &streamRpc)
{
    Status status;
    const std::string &masterAddr = req.master_address();
    worker::HashRange hashRanges;
    hashRanges.reserve(req.hash_ranges_size());
    for (const auto &range : req.hash_ranges()) {
        hashRanges.emplace_back(range.from(), range.end());
    }

    auto streamNames = GetStreamNameList();
    for (const auto &streamName : streamNames) {
        if (CheckConditionsForStream(streamName, masterAddr, hashRanges)) {
            // This stream goes to the requesting master.
            GetStreamMetadataRspPb rsp;
            Status rc = GetStreamMetadata(streamName, &rsp);
            rsp.set_stream_name(streamName);
            rsp.mutable_error()->set_error_code(rc.GetCode());
            rsp.mutable_error()->set_error_msg(rc.GetMsg());
            ASSIGN_IF_NOT_OK_PRINT_ERROR_MSG(status, streamRpc->Write(rsp),
                                             FormatString("Write metadata to master failed for stream %s", streamName));
        }
    }
    return status;
}

Status ClientWorkerSCServiceImpl::GetAllStreamMetadata(const GetMetadataAllStreamReqPb &req,
                                                       GetMetadataAllStreamRspPb &rsp)
{
    const std::string &masterAddr = req.master_address();
    worker::HashRange hashRanges;
    hashRanges.reserve(req.hash_ranges_size());
    for (const auto &range : req.hash_ranges()) {
        hashRanges.emplace_back(range.from(), range.end());
    }

    auto streamNames = GetStreamNameList();
    for (const auto &streamName : streamNames) {
        if (CheckConditionsForStream(streamName, masterAddr, hashRanges)) {
            // This stream goes to the requesting master.
            auto streamMetaPb = rsp.add_stream_meta();
            streamMetaPb->set_stream_name(streamName);
            Status rc = GetStreamMetadata(streamName, streamMetaPb);
            streamMetaPb->mutable_error()->set_error_code(rc.GetCode());
            streamMetaPb->mutable_error()->set_error_msg(rc.GetMsg());
        }
    }
    return Status::OK();
}

bool ClientWorkerSCServiceImpl::CheckConditionsForStream(const std::string &streamName, const std::string &masterAddr,
                                                         const worker::HashRange &hashRanges)
{
    if (!masterAddr.empty()) {
        MetaAddrInfo metaAddrInfo;
        auto rc = etcdCM_->GetMetaAddress(streamName, metaAddrInfo);
        if (rc.IsError()) {
            LOG(ERROR) << rc.ToString();
            return false;
        }
        auto masterAddress = metaAddrInfo.GetAddressAndSaveDbName();
        return masterAddress.ToString() == masterAddr;
    }
    return etcdCM_->IsInRange(hashRanges, streamName, "");
}

Status ClientWorkerSCServiceImpl::CheckConnection(const std::string &streamName)
{
    auto func = [&] {
        Status status = etcdCM_->CheckConnection(streamName);
        if (status.IsError()) {
            std::stringstream ss;
            ss << "Worker disconnected from master, error msg: " << status.ToString();
            LOG(ERROR) << ss.str();
            RETURN_STATUS(StatusCode::K_RPC_UNAVAILABLE, ss.str());
        }
        return Status::OK();
    };
    return WorkerMasterOcApiManager::RetryForReplicaNotReady(scTimeoutDuration.CalcRealRemainingTime(),
                                                             std::move(func));
}

Status ClientWorkerSCServiceImpl::UnblockProducer(
    std::shared_ptr<ServerUnaryWriterReader<UnblockProducerRspPb, UnblockProducerReqPb>> serverApi)
{
    UnblockProducerReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const auto &streamName = req.stream_name();
    const auto &workerAddr = req.worker_addr();
    LOG(INFO) << "UnBlocking Producer for stream: " << streamName << " From remote worker: " << workerAddr;
    HostPort workerHostPort;
    RETURN_IF_NOT_OK(workerHostPort.ParseString(workerAddr));
    RETURN_IF_NOT_OK(remoteWorkerManager_->ToggleStreamBlocking(workerAddr, streamName, false));
    RETURN_IF_NOT_OK(serverApi->Write(UnblockProducerRspPb()));
    VLOG(SC_NORMAL_LOG_LEVEL) << "UnBlocking Producer Request for stream: " << streamName
                              << " From remote worker: " << workerAddr << " Successful";
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::BlockProducer(
    std::shared_ptr<ServerUnaryWriterReader<BlockProducerRspPb, BlockProducerReqPb>> serverApi)
{
    BlockProducerReqPb req;
    RETURN_IF_NOT_OK(serverApi->Read(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    const auto &streamName = req.stream_name();
    const auto &workerAddr = req.worker_addr();
    LOG(INFO) << "Blocking Producer for stream: " << streamName << " From remote worker: " << workerAddr;
    HostPort workerHostPort;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerHostPort.ParseString(workerAddr), "ParseString error");
    RETURN_IF_NOT_OK(remoteWorkerManager_->ToggleStreamBlocking(workerAddr, streamName, true));
    RETURN_IF_NOT_OK(serverApi->Write(BlockProducerRspPb()));
    VLOG(SC_NORMAL_LOG_LEVEL) << "Blocking Producer for stream: " << streamName << " From remote worker: " << workerAddr
                              << " Successful";
    return Status::OK();
}

std::string ClientWorkerSCServiceImpl::GetTotalStreamCount()
{
    return std::to_string(streamMgrDict_.size());
}

Status ClientWorkerSCServiceImpl::DeleteStreamContext(const std::string &streamName, bool forceDelete, int64_t timeout)
{
    LOG(INFO) << FormatString("[%s, S:%s] DelStreamContext request started with forceDelete: %d", LogPrefix(true),
                              streamName, forceDelete);
    scTimeoutDuration.Init(timeout);
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    std::shared_ptr<StreamManager> streamManager;
    INJECT_POINT("ClientWorkerSCServiceImpl.DeleteStreamContext.timeout", [](int timeout) {
        scTimeoutDuration.Init(timeout);
        return Status::OK();
    });
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    streamManager = accessor->second;
    if (forceDelete) {
        // Since the pub/sub is closed before calling DeleteStreamContext, the pub/sub list is empty for the stream.
        std::vector<std::string> pubSubList;
        RETURN_IF_NOT_OK(streamManager->ResetStreamStart(pubSubList));
        streamManager->ForceCloseClients();
    }

    Status rc = streamManager->SetDeleteState(true);
    if (rc.GetCode() == StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS) {
        LOG(INFO) << FormatString("[%s] Ignore DELETE_IN_PROGRESS", streamName);
    } else if (rc.IsError()) {
        // Any error except delete-in-progress
        return rc;
    }
    bool needsRollback = true;
    Raii unsetDelete([this, &needsRollback, &streamName]() {
        StreamManagerMap::const_accessor accessor;
        if (needsRollback && GetStreamManager(streamName, accessor).IsOk()) {
            std::shared_ptr<StreamManager> streamManager = accessor->second;
            LOG(INFO) << FormatString("[S:%s] Context Undo, Setting Active State", streamName);
            streamManager->SetActiveState();
        }
    });
    // Now we need an exclusive accessor lock when we remove the stream from the tbb map
    accessor.release();
    LOG(INFO) << FormatString("[%s, S:%s] Starting to delete stream due to DeleteStreamContext", LogPrefix(true),
                              streamName);
    {
        ReadLockHelper rlock(STREAM_COMMON_LOCK_ARGS(mutex_));
        StreamManagerMap::accessor xAccessor;
        LOG(INFO) << FormatString("[%s, S:%s] Waiting for stream to be free of use", LogPrefix(true), streamName);
        // At this point
        // Master should have checked stream dont have any consumers or producers
        // we should have set delete state to avoid new operations after master call
        // So this should be fast
        bool success = streamMgrDict_.find(xAccessor, streamName);
        // Earlier topo change should have removed the stream from RWM. But no harm to call it again
        // but just expect the error stream not found
        RETURN_IF_NOT_OK_EXCEPT(remoteWorkerManager_->DeleteStream(streamName), K_SC_STREAM_NOT_FOUND);
        // If stream not found ignore and return ok
        if (success) {
            success = streamMgrDict_.erase(xAccessor);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                success, K_RUNTIME_ERROR, FormatString("Failed erase stream %s from streamMgrDict_", streamName));
        }
    }
    LOG(INFO) << FormatString("[%s, S:%s] DelStreamContext (Notified by master to clear stream data) success.",
                              LogPrefix(true), streamName);

    // Blocked request contain shared_ptr to StreamManager. To prevent a memory leak, we need to clear all blocked
    // request in StreamManager.
    streamManager->ClearBlockedList();

    needsRollback = false;
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::GetWorkerStub(const HostPort &workerHostPort,
                                                std::shared_ptr<ClientWorkerSCService_Stub> &stub)
{
    std::lock_guard<std::mutex> lock(remotePubStubMutex_);
    auto workerAddr = workerHostPort.ToString();
    auto it = remotePubStubs_.find(workerAddr);
    if (it == remotePubStubs_.end()) {
        RpcCredential cred;
        RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateCredentials(WORKER_SERVER_NAME, cred));
        auto channel = std::make_shared<RpcChannel>(workerHostPort, cred);
        stub = std::make_shared<ClientWorkerSCService_Stub>(channel);
        remotePubStubs_.emplace(workerAddr, stub);
    } else {
        stub = it->second;
    }
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::ResetStreams(
    std::shared_ptr<ServerUnaryWriterReader<ResetOrResumeStreamsRspPb, ResetOrResumeStreamsReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    ResetOrResumeStreamsReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");

    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");

    const auto &streamNamesRepeated = req.stream_names();
    std::unordered_set<std::string> streams{ streamNamesRepeated.begin(), streamNamesRepeated.end() };

    LOG(INFO) << "ResetStreams request start, clientId: " << req.client_id()
              << ", streams: " << VectorToString(streams);
    // Divide Streams into 2 Lists based on current Reset state
    std::unordered_set<std::string> doneList, errorList;

    // Start Reset Streams for all streams
    for (auto &stream : streams) {
        // Get Stream Manager for the stream
        auto namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, stream);
        LOG(INFO) << "Begin to clear data for stream Id: " << namespaceUri;
        StreamManagerMap::const_accessor accessor;
        Status rc = GetStreamManager(namespaceUri, accessor);
        if (rc.IsError()) {
            LOG(ERROR) << "Could not get stream manager for stream: " << namespaceUri;
            // If stream manager is not found, the stream is considered to be reset.
            doneList.insert(stream);
            continue;
        }
        std::shared_ptr<StreamManager> streamMgr = accessor->second;
        // If stream is getting deleted dont allow Reset on it
        rc = streamMgr->CheckIfStreamActive();
        if (rc.GetCode() == StatusCode::K_SC_STREAM_DELETE_IN_PROGRESS) {
            LOG(ERROR) << "Delete in progress for stream: " << namespaceUri;
            // If stream is getting deleted, the stream is considered to be reset.
            doneList.insert(stream);
            continue;
        }

        std::vector<std::string> prodConList;
        (void)GetPubSubForClientStream(req.client_id(), namespaceUri, prodConList);
        // Send Reset Start Request
        rc = streamMgr->ResetStreamStart(prodConList);
        if (rc.IsError()) {
            // Check if reset completed or is in progress by another client.
            if (streamMgr->CheckIfStreamInState(StreamState::RESET_COMPLETE)
                || streamMgr->CheckIfStreamInState(StreamState::DELETE_IN_PROGRESS)) {
                // Reset is done for the stream
                doneList.insert(stream);
            } else {
                LOG(ERROR) << rc.GetMsg();
                errorList.insert(stream);
            }
            continue;
        }
        // Reset Stream Start Successful
        doneList.insert(stream);
    }

    // We are done starting up Reset and Now we wait for Reset to be done
    RETURN_IF_NOT_OK(ResetStreamsReply(serverApi, streams.size(), doneList.size(), errorList.size()));
    LOG(INFO) << "ResetStreams request end";
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::ResetStreamsReply(
    std::shared_ptr<ServerUnaryWriterReader<ResetOrResumeStreamsRspPb, ResetOrResumeStreamsReqPb>> serverApi,
    size_t streamsSize, size_t doneListSize, size_t errListSize)
{
    Status retStatus;
    // We got OK for all streams requested
    ResetOrResumeStreamsRspPb rsp;
    if (doneListSize == streamsSize) {  // We got all the streams reset
        VLOG(SC_INTERNAL_LOG_LEVEL) << "ResetStreams Done";
        retStatus = Status::OK();
        serverApi->Write(rsp);
    } else if (errListSize != 0) {  // Some of them has errors
        retStatus = Status(K_RUNTIME_ERROR, "Got error while resetting stream");
        CheckErrorReturn(retStatus, rsp, "Reset failed with rc ", serverApi);
    } else {
        LOG(ERROR) << "Size of completed streams is not same as total resetting streams";
    }
    // We will not reply if its timeout, its handled by the client
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::ResumeStreams(
    std::shared_ptr<ServerUnaryWriterReader<ResetOrResumeStreamsRspPb, ResetOrResumeStreamsReqPb>> serverApi)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(), "validate worker state failed");
    ResetOrResumeStreamsReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");

    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Authenticate(akSkManager_, req, tenantId, req.client_id()),
                                     "Authenticate failed.");

    const auto &streamNamesRepeated = req.stream_names();
    std::unordered_set<std::string> streams{ streamNamesRepeated.begin(), streamNamesRepeated.end() };
    LOG(INFO) << "ResumeStreams request start, clientId: " << req.client_id()
              << ", streams: " << VectorToString(streams);
    Status rc = Status::OK();
    for (const auto &stream : streams) {
        auto namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, stream);
        LOG(INFO) << "Begin to resume stream " << namespaceUri;
        StreamManagerMap::const_accessor accessor;
        // If StreamManager is not found, may be stream is deleted while reset was in progress. Log error, return OK.
        if (GetStreamManager(namespaceUri, accessor).IsOk()) {
            std::shared_ptr<StreamManager> streamMgr = accessor->second;
            Status status = streamMgr->ResumeStream();
            if (status.IsError()) {
                LOG(ERROR) << status.GetMsg();
                rc = status;
            }
        } else {
            LOG(ERROR) << "Could not get stream manager for stream: " << namespaceUri;
        }
    }
    ResetOrResumeStreamsRspPb rsp;
    CheckErrorReturn(rc, rsp, "Reset failed with rc ", serverApi);
    LOG(INFO) << "ResumeStreams request end";
    return Status::OK();
}

std::string ClientWorkerSCServiceImpl::GetSCRemoteSendSuccessRate() const
{
    if (remoteWorkerManager_ == nullptr) {
        return "";
    }
    return remoteWorkerManager_->GetSCRemoteSendSuccessRate();
}

void ClientWorkerSCServiceImpl::WaitForAckTask(std::deque<AckTask> &ackList, const uint64_t waitTimeS)
{
    typedef std::chrono::duration<float, std::milli> millisecond;
    const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
    constexpr static int K_FUTURE = 0;
    constexpr static int K_NAME = 1;
    constexpr static int K_TIME = 2;
    auto iter = ackList.begin();
    while (iter != ackList.end()) {
        if (interrupt_) {
            return;
        }
        auto streamName = std::get<K_NAME>(*iter);
        auto &fut = std::get<K_FUTURE>(*iter);
        auto now = std::chrono::high_resolution_clock::now();
        auto duration = now - std::get<K_TIME>(*iter);
        // Wait for them to come back
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s] Wait for AckCursors thread execution", streamName);
        // Report warning if the ack thread is blocked for a long time
        auto realWaitTimeS = waitTimeS > std::numeric_limits<std::chrono::seconds::rep>::max()
                                 ? std::chrono::seconds::max()
                                 : std::chrono::seconds(waitTimeS);
        auto status = fut.wait_for(realWaitTimeS);
        INJECT_POINT("AutoAckImpl.WaitAndRetry", [&status]() mutable {
            status = std::future_status::timeout;
            return;
        });
        if (status == std::future_status::timeout) {
            LOG(WARNING) << FormatString(
                "[S:%s] Waited for %zu second and AckCursors thread hasn't returned yet. Will try again", streamName,
                realWaitTimeS.count());
            // Save this future for the next round and move on.
            ++iter;
            continue;
        }
        const Status rc = fut.get();
        LOG_IF_EVERY_N(ERROR, rc.IsError(), logPerCount)
            << FormatString("[S:%s] Auto Ack error %s", streamName, rc.ToString());
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s] Done AckCursors thread execution. Execution time [%6lf]ms",
                                                    streamName,
                                                    std::chrono::duration_cast<millisecond>(duration).count());
        iter = ackList.erase(iter);
    }
}

void ClientWorkerSCServiceImpl::AutoAckImpl(std::deque<AckTask> &ackList, const uint64_t waitTimeS)
{
    constexpr static int K_NAME = 1;
    // Get a list of those streams that hasn't ack back yet from previous round.
    std::set<std::string> streamNoAckResponse;
    for (const auto &ele : ackList) {
        streamNoAckResponse.insert(std::get<K_NAME>(ele));
    }
    // If some streams do not ack back from previous round, keep them
    // in the FIFO queue and preserve their orders in the queue.
    // Another reason is these streams are already holding the
    // const accessor. We will deadlock ourselves.
    const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
    // Get a snapshot of all the streams without holding the lock for too long
    std::vector<std::string> streams = GetStreamNameList();
    if (!streams.empty()) {
        LOG_EVERY_N(INFO, logPerCount) << FormatString("Begin to process AutoAckImpl ack logic for %d streams",
                                                       streams.size() - streamNoAckResponse.size());
    }
    auto traceId = Trace::Instance().GetTraceID();
    for (auto const &streamName : streams) {
        if (interrupt_) {
            return;
        }
        if (streamNoAckResponse.count(streamName) > 0) {
            continue;  // Already handled.
        }
        auto ackFunc = [this, streamName, traceId]() {
            auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
            auto rlock = std::make_unique<StreamManagerMap::const_accessor>();
            RETURN_IF_NOT_OK(GetStreamManager(streamName, *rlock));
            auto streamMgr = (*rlock)->second;
            return streamMgr->AckCursors();
        };
        ackList.emplace_back(ackPool_->Submit(ackFunc), streamName, std::chrono::high_resolution_clock::now());
    }
    // Wait for the tasks to come back.
    WaitForAckTask(ackList, waitTimeS);
    VLOG(SC_INTERNAL_LOG_LEVEL) << "Done to process AutoAckImpl ack logic";
}

ClientWorkerSCServiceImpl::~ClientWorkerSCServiceImpl()
{
    EraseFailedNodeApiEvent::GetInstance().RemoveSubscriber(CLIENT_WORKER_SC_SERVICE_IMPL);
    interrupt_ = true;
    if (autoAck_.valid()) {
        autoAck_.get();
    }
}

void ClientWorkerSCServiceImpl::EraseFailedWorkerMasterApi(HostPort &masterAddr)
{
    workerMasterApiManager_->EraseFailedWorkerMasterApi(masterAddr, StubType::WORKER_MASTER_SC_SVC);
}

Status ClientWorkerSCServiceImpl::GetPubSubForClientStream(const std::string &clientId, const std::string &streamName,
                                                           std::vector<std::string> &prodConList)
{
    prodConList.clear();
    bool found = false;
    std::shared_lock<std::shared_timed_mutex> locker(clearMutex_);
    // First collect all the producer Ids if there exists any for the given client.
    auto iter = clientProducers_.find(clientId);
    if (iter != clientProducers_.end()) {
        for (auto &streamProducer : iter->second) {
            if (streamProducer.streamName_ == streamName) {
                prodConList.emplace_back(streamProducer.producerId_);
            }
        }
        found = true;
    }

    // Next collect all the consumer Ids if there exists any for the given client.
    auto iter2 = clientConsumers_.find(clientId);
    if (iter2 != clientConsumers_.end()) {
        for (auto &subInfo : iter2->second) {
            if (subInfo.streamName == streamName) {
                prodConList.emplace_back(subInfo.consumerId);
            }
        }
        found = true;
    }
    if (!found) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("No producer or consumer found for the client: %s", clientId));
    }
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::ReserveMemoryFromUsageMonitor(const std::string &streamName, size_t reserveSize)
{
    auto workerWorkerSCServicePtr = workerWorkerSCService_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(workerWorkerSCServicePtr, K_RUNTIME_ERROR,
                                         FormatString("WorkerWorkerSCService shutdown"));
    RETURN_IF_NOT_OK(workerWorkerSCServicePtr->GetUsageMonitor().ReserveMemory(streamName, reserveSize));
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::UndoReserveMemoryFromUsageMonitor(const std::string &streamName)
{
    auto workerWorkerSCServicePtr = workerWorkerSCService_.lock();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(workerWorkerSCServicePtr, K_RUNTIME_ERROR,
                                         FormatString("WorkerWorkerSCService shutdown"));
    workerWorkerSCServicePtr->GetUsageMonitor().UndoReserveMemory(streamName);
    return Status::OK();
}

template <>
Status ClientWorkerSCServiceImpl::HandleBlockedCreateTimeout<CreateShmPageRspPb, CreateShmPageReqPb>(
    const std::string &streamName, const std::string &producerId, const std::string &traceId, int64_t subTimeout,
    const std::chrono::steady_clock::time_point &startTime)
{
    INJECT_POINT("ClientWorkerSCServiceImpl.HandleBlockedCreateTimeout.sleep");
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
    LOG(INFO) << "Blocked CreateShmPage request timer expired. Return OOM to client for stream " << streamName
              << " with producer " << producerId << " and timeout " << subTimeout;
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    RETURN_IF_NOT_OK(streamMgr->CheckIfStreamActive());
    streamMgr->HandleBlockedCreateTimeout<CreateShmPageRspPb, CreateShmPageReqPb>(producerId, subTimeout, startTime);
    return Status::OK();
}

template <>
Status ClientWorkerSCServiceImpl::HandleBlockedCreateTimeout<CreateLobPageRspPb, CreateLobPageReqPb>(
    const std::string &streamName, const std::string &producerId, const std::string &traceId, int64_t subTimeout,
    const std::chrono::steady_clock::time_point &startTime)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
    LOG(INFO) << "Blocked AllocBigElement request timer expired. Return OOM to client for stream " << streamName
              << " with producer " << producerId << " and timeout " << subTimeout;
    StreamManagerMap::const_accessor accessor;
    RETURN_IF_NOT_OK(GetStreamManager(streamName, accessor));
    std::shared_ptr<StreamManager> streamMgr = accessor->second;
    RETURN_IF_NOT_OK(streamMgr->CheckIfStreamActive());
    streamMgr->HandleBlockedCreateTimeout<CreateLobPageRspPb, CreateLobPageReqPb>(producerId, subTimeout, startTime);
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::StreamNoToName(uint64_t streamNo, std::string &streamName)
{
    std::shared_lock<std::shared_timed_mutex> locker(mappingMutex_);
    auto iter = streamNum2StreamName_.find(streamNo);
    CHECK_FAIL_RETURN_STATUS(iter != streamNum2StreamName_.end(), K_SC_STREAM_NOT_FOUND,
                             FormatString("Stream number %zu not found", streamNo));
    streamName = iter->second;
    return Status::OK();
}

Status ClientWorkerSCServiceImpl::AddStreamNo(uint64_t streamNo, const std::string &streamName)
{
    std::lock_guard<std::shared_timed_mutex> locker(mappingMutex_);
    bool success = streamNum2StreamName_.emplace(streamNo, streamName).second;
    CHECK_FAIL_RETURN_STATUS(success, K_RUNTIME_ERROR, FormatString("Duplicated stream number %zu", streamNo));
    return Status::OK();
}

void ClientWorkerSCServiceImpl::RemoveStreamNo(uint64_t streamNo)
{
    std::lock_guard<std::shared_timed_mutex> locker(mappingMutex_);
    streamNum2StreamName_.erase(streamNo);
}

template <typename W, typename R>
BlockedCreateRequest<W, R>::BlockedCreateRequest(std::string streamName, const R &req, size_t reqSz,
                                                 std::shared_ptr<ServerUnaryWriterReader<W, R>> serverApi,
                                                 BlockedCreateReqFn fn)
    : req_(req),
      reqSize_(reqSz),
      serverApi_(std::move(serverApi)),
      startTime_(std::chrono::steady_clock::now()),
      retryCount_(0),
      streamName_(std::move(streamName)),
      traceId_(Trace::Instance().GetTraceID()),
      callBackFn_(fn),
      ack_(AckVal::NONE),
      timer_(nullptr),
      timeSpent_(req_.sub_timeout())
{
    // Set up a return rc. If we have chance to execute this request, the defaultRc_ will be
    // overridden by AllocMemory call. Otherwise, this request will then time out, and we return OOM.
    if (req_.sub_timeout() > 0) {
        auto subTimeout = req_.sub_timeout();
        auto producerId = req_.producer_id();
        defaultRc_ = Status(K_OUT_OF_MEMORY,
                            FormatString("[S:%s, P:%s] Blocked CreateShmPage request timer expired. timeout %d.",
                                         streamName_, producerId, subTimeout));
    }
}

template <typename W, typename R>
void BlockedCreateRequest<W, R>::SetTimer(std::unique_ptr<TimerQueue::TimerImpl> timer)
{
    timer_ = std::move(timer);
}

template <typename W, typename R>
void BlockedCreateRequest<W, R>::CancelTimer()
{
    if (timer_) {
        INJECT_POINT("do.not.cancel.timer", [] { return; });
        (void)TimerQueue::GetInstance()->Cancel(*timer_);
        timer_.reset();
    }
}

template <typename W, typename R>
int64_t BlockedCreateRequest<W, R>::GetRemainingTimeMs()
{
    return timeSpent_.GetRemainingTimeMs();
}

template <typename W, typename R>
R BlockedCreateRequest<W, R>::GetCreateRequest() const
{
    return req_;
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::SendStatus(const Status &rc)
{
    if (serverApi_) {
        return serverApi_->SendStatus(rc);
    } else {
        wp_.Set();
        return rc;
    }
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::Write()
{
    if (serverApi_) {
        return serverApi_->Write(rsp_);
    } else {
        // Before wake up the receiver, flip the atomic variable from 0 to 1
        uint32_t val = AckVal::NONE;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ack_.compare_exchange_strong(val, AckVal::DONE), K_RUNTIME_ERROR,
                                             FormatString("Unexpected CAS error from 0 -> 1. Current val %d", val));
        wp_.Set();
    }
    return Status::OK();
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::Wait(uint64_t timeoutMs)
{
    auto success = wp_.WaitFor(timeoutMs);
    if (success) {
        // Map the return code. Depends on the timing, we may get K_NOT_FOUND
        // when the memory was allocated but then undone because this thread is slow
        // to get to the ack phase.
        switch (defaultRc_.GetCode()) {
            case K_OK:
            case K_OUT_OF_MEMORY:
                return defaultRc_;
            default:
                RETURN_STATUS(K_TRY_AGAIN, defaultRc_.GetMsg());
        }
    }
    return { StatusCode::K_TRY_AGAIN, FormatString("Waited for %zu ms. No response", timeoutMs) };
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::SenderHandShake()
{
    // Can't do handshake with RPC caller.
    RETURN_OK_IF_TRUE(serverApi_);
    INJECT_POINT("BlockedCreateRequest.ReceiverHandShake.Rollback");
    // Wait for a few ms for the Receiver to acknowledge
    auto okay = ackWp_.WaitFor(RPC_POLL_TIME);
    // No need to do anything if we are waken up by the receiver
    RETURN_OK_IF_TRUE(okay);
    // Now we flip back from 1 to 0.
    uint32_t expectedVal = AckVal::DONE;
    bool success = ack_.compare_exchange_strong(expectedVal, AckVal::NONE);
    // There is a still a chance that the receiver wake up the same time as us.
    // If the receiver can flip the ack from 1 to 0, we still treat the whole handshake
    // as successful. That means, if our CAS fail, we treat it as handshake success
    RETURN_OK_IF_TRUE(!success);
    // Lastly we inform the caller the receiver has gone.
    return { StatusCode::K_NOT_FOUND, "Receiver has gone" };
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::ReceiverHandShake()
{
    // Can't do handshake with RPC caller.
    RETURN_OK_IF_TRUE(serverApi_);
    INJECT_POINT("BlockedCreateRequest.ReceiverHandShake.sleep");
    // Drive a compare and swap. We expect it is 1, and will flip it back to 0.
    // If it is 0, that means the sender has already undone the memory allocation
    uint32_t expectedVal = AckVal::DONE;
    bool success = ack_.compare_exchange_strong(expectedVal, AckVal::NONE);
    ackWp_.Set();
    RETURN_OK_IF_TRUE(success);
    return { StatusCode::K_TRY_AGAIN, "Sender has undone the changes" };
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::HandleBlockedCreateTimeout()
{
    if (req_.sub_timeout() > 0 && GetRemainingTimeMs() == 0) {
        auto elapsed = static_cast<int64_t>(timeSpent_.ElapsedMilliSecond());
        LOG(ERROR) << FormatString("[S:%s, P:%s] RPC timeout. time elapsed %zu, subTimeout: %zu", streamName_,
                                   req_.producer_id(), elapsed, req_.sub_timeout());
        if (serverApi_) {
            LOG_IF_ERROR(serverApi_->SendStatus(defaultRc_), "send status failed");
        } else {
            wp_.Set();
        }
        return defaultRc_;
    }
    return Status::OK();
}

template <typename W, typename R>
Status BlockedCreateRequest<W, R>::operator()()
{
    ++retryCount_;
    defaultRc_ = callBackFn_(this);
    return defaultRc_;
}

template <typename W, typename R>
bool BlockedCreateRequest<W, R>::HasStartTime(const std::chrono::steady_clock::time_point &startTime)
{
    return startTime == startTime_;
}

template <typename W, typename R>
bool BlockedCreateRequest<W, R>::HasRequestPbOlderThan(const BlockedCreateRequest<W, R> &inputRequest)
{
    uint64_t inputRequestPbTimestamp = inputRequest.req_.timestamp();
    uint64_t currentRequestPbTimestamp = req_.timestamp();
    LOG(INFO) << FormatString(
        "Producer: %s, Other request: traceid %s timestamp %llu, "
        "This request: traceid %s timestamp %llu",
        req_.producer_id(), inputRequest.traceId_, inputRequestPbTimestamp, traceId_, currentRequestPbTimestamp);
    return currentRequestPbTimestamp < inputRequestPbTimestamp;
}

template <typename W, typename R>
Status MemAllocRequestList<W, R>::AddBlockedCreateRequest(ClientWorkerSCServiceImpl *scSvc,
                                                          std::shared_ptr<BlockedCreateRequest<W, R>> blockedReq)
{
    auto subTimeout = blockedReq->GetRemainingTimeMs();
    const auto req = blockedReq->GetCreateRequest();
    const auto &producerId = req.producer_id();
    const auto streamName = blockedReq->streamName_;
    const std::chrono::steady_clock::time_point startTime = blockedReq->startTime_;
    VLOG(SC_NORMAL_LOG_LEVEL) << "Adding a blocked request to the blocked queue for stream " << streamName
                              << " with producer " << producerId << " and timeout " << subTimeout;
    std::unique_lock<std::shared_timed_mutex> lock(blockedListMutex_);
    std::shared_ptr<BlockedCreateRequest<W, R>> savedReq = blockedReq;  // save the ptr for later
    // Add the entry to the blocked list and queue
    auto it = blockedList_.find(producerId);
    if (it != blockedList_.end()) {
        // There is an old request by the same producer not processed.
        LOG(WARNING) << FormatString("Duplicate blocked create page requests for stream %s with producer %s",
                                     streamName, producerId);
        Status rc = Status(K_DUPLICATED, FormatString("[S:%s, P:%s] Receive a newer request from producer, this "
                                                      "request become expired and not processed.",
                                                      streamName, producerId));
        if (it->second->HasRequestPbOlderThan(*blockedReq)) {
            // We have a new request with newer timestamp, remove the old request.
            // Get old request
            std::shared_ptr<BlockedCreateRequest<W, R>> oldBlockedReq = std::move(it->second);

            // Cancel the timer for the old request since we will take action here
            oldBlockedReq->CancelTimer();

            // Remove the old request
            // A memory request is stored in two data structures. One is a hashed map and one is a priority queue.
            // We can't take out one but leaves the other.
            (void)blockedList_.erase(it);
            RemoveBlockedCreateRequestFromQueueLocked(oldBlockedReq.get());

            // Send status for the old request.
            LOG_IF_ERROR(oldBlockedReq->SendStatus(rc), "Send status to client failed");
        } else {
            // We have a new request with older timestamp than the request currently in the blocked list,
            // do not process the new request with older timestamp.
            LOG_IF_ERROR(blockedReq->SendStatus(rc), "Send status to client failed");
            return Status::OK();
        }
    }

    // Now any old request by the same producer has been processed, insert the new request.
    bool success;
    std::tie(std::ignore, success) = blockedList_.emplace(producerId, std::move(blockedReq));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        success, StatusCode::K_RUNTIME_ERROR,
        FormatString("Fail to insert BlockedCreateRequest for stream %s with producer %s and timeout %lld", streamName,
                     producerId, subTimeout));
    queue_.template emplace(savedReq.get());
    // Early exit if we don't have to create a timer
    RETURN_OK_IF_TRUE(req.sub_timeout() == 0);
    // Create a timer to throw OOM and clean up if the timer expires
    TimerQueue::TimerImpl timer;
    auto traceID = savedReq->traceId_;
    INJECT_POINT("AddBlockedCreateRequest.subTimeout", [&subTimeout]() mutable {
        subTimeout = 0;
        return Status::OK();
    });
    // If the timer has expired already, let StreamManager::HandleBlockedRequestImpl handle it. No need
    // to create a timer.
    RETURN_OK_IF_TRUE(subTimeout == 0);
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        subTimeout,
        [scSvc, streamName, producerId, traceID, subTimeout, startTime]() {
            LOG_IF_ERROR(
                (scSvc->HandleBlockedCreateTimeout<W, R>(streamName, producerId, traceID, subTimeout, startTime)),
                "HandleBlockedCreateTimeout");
        },
        timer));

    savedReq->SetTimer(std::make_unique<TimerQueue::TimerImpl>(timer));
    return Status::OK();
}

template <typename W, typename R>
void MemAllocRequestList<W, R>::RemoveBlockedCreateRequestFromQueueLocked(
    const BlockedCreateRequest<W, R> *blockedReqPtr)
{
    // The tricky part is the priority queue which we can only pop the top.
    std::vector<BlockedCreateRequest<W, R> *> list;
    while (!queue_.empty()) {
        auto *ptr = queue_.top();
        queue_.pop();
        if (ptr == blockedReqPtr) {
            break;
        } else {
            // requests that we must put back
            list.push_back(ptr);
        }
    }
    for (auto p : list) {
        queue_.push(p);
    }
}

template <typename W, typename R>
void MemAllocRequestList<W, R>::HandleBlockedCreateTimeout(const std::string &streamName, const std::string &producerId,
                                                           int64_t subTimeout,
                                                           const std::chrono::steady_clock::time_point &startTime)
{
    std::lock_guard<std::shared_timed_mutex> lock(blockedListMutex_);
    auto it = blockedList_.find(producerId);
    if (it == std::end(blockedList_) || !(it->second->HasStartTime(startTime))) {
        // A race between a thread doing free vs this timeout. The other thread won so this is a no-op
        // Log it for information only.
        LOG(INFO) << "Blocked CreateShmPage request timer expired. The page was already handled for stream "
                  << streamName << " with producer " << producerId << " and timeout " << subTimeout;
        return;
    }
    // Return original OOM here and log timer expired message into worker log
    std::shared_ptr<BlockedCreateRequest<W, R>> blockedReq = std::move(it->second);
    auto rc = blockedReq->defaultRc_;
    LOG(ERROR) << FormatString("[S:%s, P:%s] timeout %zu, %s", streamName, producerId, subTimeout, rc.ToString());
    // A memory request is stored in two data structures. One is a hashed map and one is a priority queue.
    // We can't take out one but leaves the other.
    (void)blockedList_.erase(it);
    RemoveBlockedCreateRequestFromQueueLocked(blockedReq.get());
    LOG_IF_ERROR(blockedReq->SendStatus(rc), "Send status to client failed");
}

template <typename W, typename R>
Status MemAllocRequestList<W, R>::GetBlockedCreateRequest(std::shared_ptr<BlockedCreateRequest<W, R>> &out)
{
    INJECT_POINT("GetBlockedCreateRequest.sleep");
    std::lock_guard<std::shared_timed_mutex> lock(blockedListMutex_);
    CHECK_FAIL_RETURN_STATUS(!queue_.empty(), StatusCode::K_TRY_AGAIN, "No outstanding memory request");
    auto *blockedReq = queue_.top();
    queue_.pop();
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(blockedReq->traceId_);
    const auto req = blockedReq->GetCreateRequest();
    const auto producerId = req.producer_id();
    const std::string streamName = blockedReq->streamName_;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[S:%s, P:%s] Handle alloc memory request", streamName, producerId);
    auto it = blockedList_.find(producerId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        it != blockedList_.end(), K_RUNTIME_ERROR,
        FormatString("[S:%s, P:%s] Alloc memory request not found", streamName, producerId));
    // Cancel the timer for this entry since we will take action here
    it->second->CancelTimer();
    out = std::move(it->second);
    (void)blockedList_.erase(it);
    return Status::OK();
}

template <typename W, typename R>
bool MemAllocRequestList<W, R>::Empty()
{
    std::shared_lock<std::shared_timed_mutex> lock(blockedListMutex_);
    return queue_.empty();
}

template <typename W, typename R>
size_t MemAllocRequestList<W, R>::Size()
{
    std::shared_lock<std::shared_timed_mutex> lock(blockedListMutex_);
    return queue_.size();
}

template <typename W, typename R>
size_t MemAllocRequestList<W, R>::GetNextBlockedRequestSize()
{
    std::shared_lock<std::shared_timed_mutex> lock(blockedListMutex_);
    if (queue_.empty()) {
        return 0;
    }
    return queue_.top()->reqSize_;
}

template <typename W, typename R>
void MemAllocRequestList<W, R>::ClearBlockedList()
{
    std::shared_lock<std::shared_timed_mutex> lock(blockedListMutex_);
    blockedList_.clear();
    queue_ = std::priority_queue<BlockedCreateRequest<W, R> *, std::vector<BlockedCreateRequest<W, R> *>, Compare>();
}

StreamManagerWithLock::StreamManagerWithLock(std::shared_ptr<StreamManager> mgr, void *accessor, bool exclusive,
                                             std::shared_ptr<ClientWorkerSCServiceImpl> service)
    : mgr_(std::move(mgr)), accessor_(accessor), exclusive_(exclusive), service_(std::move(service))
{
    rlock_ = std::make_unique<ReadLockHelperType>(LOCK_ARGS_MSG_FN(service_->mutex_, service_->LogPrefix));
}

StreamManagerWithLock::~StreamManagerWithLock()
{
    Release();
}

void StreamManagerWithLock::Release()
{
    if (accessor_) {
        if (exclusive_) {
            auto accessor =
                std::unique_ptr<StreamManagerMap::accessor>(reinterpret_cast<StreamManagerMap::accessor *>(accessor_));
            accessor->release();
        } else {
            auto accessor = std::unique_ptr<StreamManagerMap::const_accessor>(
                reinterpret_cast<StreamManagerMap::const_accessor *>(accessor_));
            accessor->release();
        }
        accessor_ = nullptr;
    }
    if (rlock_->owns_lock()) {
        rlock_->unlock();
    }
}

void StreamManagerWithLock::CleanUp(std::function<void(StreamManagerMap::accessor *accessor)> &&callback)
{
    if (!needCleanUp || !exclusive_) {
        return;
    }

    if (!rlock_->owns_lock()) {
        rlock_->AcquireLock();
    }
    if (accessor_ != nullptr) {
        auto accessor =
            std::unique_ptr<StreamManagerMap::accessor>(reinterpret_cast<StreamManagerMap::accessor *>(accessor_));
        callback(accessor.get());
        accessor_ = nullptr;
    } else {
        callback(nullptr);
    }
    rlock_->unlock();
}

void ClientWorkerSCServiceImpl::EraseFromStreamMgrDictWithoutLck(const std::string &namespaceUri,
                                                                 StreamManagerMap::accessor *accessor)
{
    if (accessor) {
        streamMgrDict_.erase(*accessor);
    } else {
        streamMgrDict_.erase(namespaceUri);
    }
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
