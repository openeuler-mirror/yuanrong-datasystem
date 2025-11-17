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
 * Description: Implement the stream cache services on the master.
 */
#include "datasystem/master/stream_cache/master_sc_service_impl.h"

#include <utility>

#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/stream_cache/util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/worker/stream_cache/master_worker_sc_service_impl.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"

DS_DEFINE_int32(master_sc_thread_num, 128, "Max number of threads for (non rpc) master stream cache service work");

namespace datasystem {
namespace master {
MasterSCServiceImpl::MasterSCServiceImpl(const HostPort &masterAddress, std::shared_ptr<AkSkManager> akSkManager,
                                         ReplicaManager *replicaManager)
    : MasterSCService(masterAddress), akSkManager_(std::move(akSkManager)), replicaManager_(replicaManager)
{
}

void MasterSCServiceImpl::Shutdown()
{
    LOG(INFO) << "MasterSCServiceImpl shutdown.";
    SCMigrateMetadataManager::Instance().Shutdown();
}

Status MasterSCServiceImpl::Init()
{
    RETURN_IF_NOT_OK(MasterSCService::Init());
    const size_t MIN_THREADS = 1;
    size_t minThreads = std::min<size_t>(MIN_THREADS, FLAGS_master_sc_thread_num);
    RETURN_IF_EXCEPTION_OCCURS(threadPool_ =
                                   std::make_unique<ThreadPool>(minThreads, FLAGS_master_sc_thread_num, "MScThreads"));
    RETURN_IF_NOT_OK(SCMigrateMetadataManager::Instance().Init(GetLocalAddr(), akSkManager_, etcdCM_, replicaManager_));
    VLOG(SC_NORMAL_LOG_LEVEL) << "MasterSCServiceImpl initialization success";
    return Status::OK();
}

Status MasterSCServiceImpl::CreateProducer(
    std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> serverApi)
{
    CreateProducerReqPb req;
    CreateProducerRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    return CreateProducerImpl(serverApi, req, rsp);
}

Status MasterSCServiceImpl::CreateProducerImpl(
    const std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> &serverApi,
    const CreateProducerReqPb &req, CreateProducerRspPb &rsp)
{
    Timer timer(req.timeout());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master receive create producer request: <%s> with timeout: %d",
                              LogHelper::IgnoreSensitive(req.producer_meta()), req.timeout());
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.CreateProducer");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    if (serverApi) {
        // Launch child thread to run the real logic and then return this thread. This avoids the rpc thread being
        // active during the logic of this request so that it can be re-used by other requests.
        auto traceId = Trace::Instance().GetTraceID();
        threadPool_->Execute([=]() mutable {
            scTimeoutDuration.Init(timer.GetRemainingTimeMs());
            Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            Status rc = scMetadataManager->CreateProducer(req, rsp);
            CheckErrorReturn(
                rc, rsp, FormatString("[S:%s] CreateProducerImpl failed with rc ", req.producer_meta().stream_name()),
                serverApi);
        });
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->CreateProducer(req, rsp), "CreateProducer failed");
    }
    LOG(INFO) << FormatString("Master create producer request: <%s> Successful",
                              LogHelper::IgnoreSensitive(req.producer_meta()));
    return Status::OK();
}

Status MasterSCServiceImpl::CloseProducer(
    std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> serverApi)
{
    CloseProducerReqPb req;
    CloseProducerRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    return CloseProducerImpl(serverApi, req, rsp);
}

Status MasterSCServiceImpl::CloseProducerImpl(
    const std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> &serverApi,
    const CloseProducerReqPb &req, CloseProducerRspPb &rsp)
{
    Timer timer(req.timeout());
    INJECT_POINT("master.CloseProducerImpl");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::string infoMsg;
    // If there's more than one producer to close, only log the count. If there is only one, show the detail
    if (req.producer_infos_size() == 1) {
        infoMsg = FormatString("S:%s", req.producer_infos(0).stream_name());
    } else {
        infoMsg = FormatString("Number of producers: %d", req.producer_infos_size());
    }
    LOG(INFO) << "Master receive close producer request: " << infoMsg<< " with timeout: "<<req.timeout();
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.CloseProducer");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    if (serverApi) {
        // Launch child thread to run the real logic and then return this thread. This avoids the rpc thread being
        // active during the logic of this request so that it can be re-used by other requests.
        auto traceId = Trace::Instance().GetTraceID();
        threadPool_->Execute([=]() mutable {
            scTimeoutDuration.Init(timer.GetRemainingTimeMs());
            Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            Status rc = scMetadataManager->CloseProducer(req, rsp);
            CheckErrorReturn(rc, rsp, "CloseProducerImpl failed with rc", serverApi);
        });
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->CloseProducer(req, rsp), "CloseProducer failed");
    }

    return Status::OK();
}

Status MasterSCServiceImpl::Subscribe(
    std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> serverApi)
{
    SubscribeReqPb req;
    SubscribeRspPb rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    return SubscribeImpl(serverApi, req, rsp);
}

Status MasterSCServiceImpl::SubscribeImpl(
    const std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> &serverApi,
    const SubscribeReqPb &req, SubscribeRspPb &rsp)
{
    Timer timer(req.timeout());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master receive subscribe request: <%s> with timeout: %d",
                              LogHelper::IgnoreSensitive(req.consumer_meta()), req.timeout());
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.Subscribe");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    if (serverApi) {
        // Launch child thread to run the real logic and then return this thread. This avoids the rpc thread being
        // active during the logic of this request so that it can be re-used by other requests.
        auto traceId = Trace::Instance().GetTraceID();
        threadPool_->Execute([=]() mutable {
            scTimeoutDuration.Init(timer.GetRemainingTimeMs());
            Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            Status rc = scMetadataManager->Subscribe(req, rsp);
            CheckErrorReturn(
                rc, rsp, FormatString("[S:%s] SubscribeImpl failed with rc",
                req.consumer_meta().stream_name()), serverApi);
        });
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->Subscribe(req, rsp), "Subscribe failed");
    }
    return Status::OK();
}

Status MasterSCServiceImpl::CloseConsumer(
    std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> serverApi)
{
    CloseConsumerReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    CloseConsumerRspPb rsp;
    return CloseConsumerImpl(serverApi, req, rsp);
}

Status MasterSCServiceImpl::CloseConsumerImpl(
    const std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> &serverApi,
    const CloseConsumerReqPb &req, CloseConsumerRspPb &rsp)
{
    Timer timer(req.timeout());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << FormatString("Master receive close consumer request: <%s> with timeout: %d",
                              LogHelper::IgnoreSensitive(req), req.timeout());
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.CloseConsumer");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    if (serverApi) {
        // Launch child thread to run the real logic and then return this thread. This avoids the rpc thread being
        // active during the logic of this request so that it can be re-used by other requests.
        auto traceId = Trace::Instance().GetTraceID();
        threadPool_->Execute([=]() mutable {
            scTimeoutDuration.Init(timer.GetRemainingTimeMs());
            Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            Status rc = scMetadataManager->CloseConsumer(req, rsp);
            CheckErrorReturn(rc, rsp, FormatString("[S:%s] CloseConsumer failed with rc",
                req.consumer_meta().stream_name()), serverApi);
        });
    } else {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->CloseConsumer(req, rsp), "CloseConsumer failed");
        scTimeoutDuration.Reset();
    }

    return Status::OK();
}

Status MasterSCServiceImpl::DeleteStream(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp)
{
    scTimeoutDuration.Init(req.timeout());
    Raii outerResetDuration([]() { scTimeoutDuration.Reset(); });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.DeleteStream");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    LOG(INFO) << FormatString("Master receive delete stream request: <%s> with timeout: %d",
                              LogHelper::IgnoreSensitive(req), req.timeout());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->DeleteStream(req, rsp), "DeleteStream failed");
    return Status::OK();
}

Status MasterSCServiceImpl::QueryGlobalProducersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Master receive query producer number request: <%s>",
                                              LogHelper::IgnoreSensitive(req));
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.QueryGlobalProducersNum");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->QueryGlobalProducersNum(req, rsp),
                                     "QueryGlobalProducersNum failed");
    return Status::OK();
}

Status MasterSCServiceImpl::QueryGlobalConsumersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Master receive query consumer number request: <%s>",
                                              LogHelper::IgnoreSensitive(req));
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.QueryGlobalConsumersNum");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(scMetadataManager->QueryGlobalConsumersNum(req, rsp),
                                     "QueryGlobalConsumersNum failed");
    return Status::OK();
}

Status MasterSCServiceImpl::StartCheckMetadata()
{
    bool isRestart = false;
    RETURN_IF_NOT_OK(etcdCM_->IsRestart(isRestart));
    if (!isRestart || !etcdCM_->IsEtcdAvailableWhenStart()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(etcdCM_->CheckWaitNodeTableComplete());
    std::vector<HostPort> nodeAddrs;
    // Why does it get node list from etcd instead of cluster manager or hashring?
    // Because in case of centralized master, we have no hashring, and can get list from only etcd and cluster manager.
    // We want a complete list without absence of any running node. Since the list in cluster manager is from etcd,
    // getting list directly from etcd can avoid possible delay in rpc resulting in miss of nodes.
    RETURN_IF_NOT_OK(etcdCM_->GetNodeAddrListFromEtcd(nodeAddrs));
    const size_t maxThreadNum = 20;
    // Add a condition to forbid thread pool size creation with minThreadNum 0 to avoid cpp runtime exception.
    if (nodeAddrs.empty()) {
        return Status::OK();
    }
    auto checkPool = std::make_unique<ThreadPool>(std::min(maxThreadNum, nodeAddrs.size()), 0, "MScCheck");
    // broadcast over all active masters
    std::vector<std::future<void>> rcs(nodeAddrs.size());
    for (size_t i = 0; i < nodeAddrs.size(); ++i) {
        rcs[i] = checkPool->Submit([this, i, &nodeAddrs]() {
            auto func = [i, &nodeAddrs](const std::string &dbName, MetadataManager metadataMansger) {
                auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-sc-check");
                LOG(INFO) << "Check metadata for db name " << dbName;
                if (metadataMansger.sc != nullptr) {
                    metadataMansger.sc->StartCheckMetadata(nodeAddrs[i]);
                }
                return Status::OK();
            };
            (void)replicaManager_->ApplyForAllMetaManager(func);
        });
    }
    // wait for the end of all reconciliations
    // we do not check the results and ignore the failed status
    for (const auto &rc : rcs) {
        rc.wait();
    }
    return Status::OK();
}

Status MasterSCServiceImpl::MigrateSCMetadata(const MigrateSCMetadataReqPb &req, MigrateSCMetadataRspPb &rsp)
{
    masterOperationTimeCost.Clear();
    Timer timer;
    auto copyReq = req;
    for (int i = 0; i < copyReq.stream_metas_size(); ++i) {
        auto *meta = copyReq.mutable_stream_metas(i);
        if (meta != nullptr) {
            meta->clear_notifications();
        }
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(copyReq), "AK/SK failed.");
    std::shared_ptr<SCMetadataManager> scMetadataManager;
    INJECT_POINT("master.MigrateSCMetadata");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(GetDbName(), scMetadataManager),
                                     "GetScMetadataManager failed");
    RETURN_IF_NOT_OK(scMetadataManager->SaveMigrationMetadata(req, rsp));
    masterOperationTimeCost.Append("Total MigrateMetadata", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of SC master MigrateMetadata %s", masterOperationTimeCost.GetInfo());
    return Status::OK();
}

std::string MasterSCServiceImpl::GetDbName()
{
    if (replicaManager_->MultiReplicaEnabled()) {
        return g_MetaRocksDbName;
    }
    return replicaManager_->GetCurrentWorkerUuid();
}
}  // namespace master
}  // namespace datasystem
