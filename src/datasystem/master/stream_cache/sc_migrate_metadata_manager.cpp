/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Migrating Data in Scaling Scenarios for Stream Cache.
 */
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"

#include <algorithm>
#include <exception>

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"

DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
static constexpr int MOVE_THREAD_NUM = 4;
static constexpr int MAX_MIGRATE_CNT_PER_STREAM = 30;

namespace {
Status CheckMigrationTargetConnection(const cluster::MembershipEndpointView *membership, const HostPort &address)
{
    CHECK_FAIL_RETURN_STATUS(membership != nullptr, K_NOT_READY, "Topology membership view is not available");
    cluster::MemberEndpoint endpoint;
    auto rc = membership->ResolveByAddress(address.ToString(), endpoint);
    if (rc.GetCode() == K_NOT_READY || rc.GetCode() == K_NOT_FOUND) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(rc);
    CHECK_FAIL_RETURN_STATUS(endpoint.localAvailability != cluster::EndpointAvailability::UNREACHABLE,
                             K_MASTER_TIMEOUT, "Migration target is unreachable");
    return Status::OK();
}
}  // namespace

MasterMasterSCApi::MasterMasterSCApi(const HostPort &hostPort, const HostPort &localHostPort,
                                     std::shared_ptr<AkSkManager> akSkManager)
    : destHostPort_(hostPort), localHostPort_(localHostPort), akSkManager_(std::move(akSkManager))
{
}

Status MasterMasterSCApi::Init()
{
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateCredentials(WORKER_SERVER_NAME, cred));
    auto channel = std::make_shared<RpcChannel>(destHostPort_, cred);
    rpcSession_ = std::make_unique<master::MasterSCService_Stub>(channel);
    LOG(INFO) << FormatString("start stream meta client: %s", destHostPort_.ToString());
    return Status::OK();
}

Status MasterMasterSCApi::MigrateSCMetadata(MigrateSCMetadataReqPb &req, MigrateSCMetadataRspPb &rsp)
{
    return rpcSession_->MigrateSCMetadata(req, rsp);
}

SCMigrateMetadataManager &SCMigrateMetadataManager::Instance()
{
    static SCMigrateMetadataManager instance;
    return instance;
}

Status SCMigrateMetadataManager::Init(
    const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
    const cluster::MembershipEndpointView *membership, MetadataManagerHolder *metadataManagerHolder)
{
    localHostPort_ = localHostPort;
    akSkManager_ = std::move(akSkManager);
    topologyMembership_ = membership;
    threadPool_ = std::make_unique<ThreadPool>(0, MOVE_THREAD_NUM, "ScMigrateMetadata");
    metadataManagerHolder_ = metadataManagerHolder;

    return Status::OK();
}

SCMigrateMetadataManager::~SCMigrateMetadataManager()
{
    Shutdown();
    LOG(INFO) << "~SCMigrateMetadataManager";
}

void SCMigrateMetadataManager::Shutdown()
{
    exitFlag_ = true;
    topologyMembership_ = nullptr;
}

Status SCMigrateMetadataManager::MigrateTopologyMetadata(
    const cluster::TopologyPhaseAction &action, const cluster::IKeyFilter &filter,
    const std::string &businessOperationId, std::chrono::steady_clock::time_point deadline,
    const cluster::CancellationToken &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(action.source.has_value() && action.target.has_value(), K_INVALID,
                             "topology stream migration lacks source or target");
    CHECK_FAIL_RETURN_STATUS(!businessOperationId.empty(), K_INVALID, "empty topology business operation id");
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology stream migration cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology stream migration deadline exceeded");
    std::shared_ptr<master::SCMetadataManager> metadata;
    RETURN_IF_NOT_OK(metadataManagerHolder_->GetScMetadataManager(metadata));
    MigrateMetaInfo info;
    info.destAddr = action.target->address;
    info.operationId = businessOperationId;
    info.topologyVersion = action.topologyVersion;
    info.batchEpoch = action.batchEpoch;
    info.sourceMemberId = action.source->id;
    info.targetMemberId = action.target->id;
    info.deadline = deadline;
    info.cancellation = cancellation;
    metadata->GetMetasMatch([&filter](const std::string &name) { return filter.Contains(name); }, info.streamNames);
    if (info.streamNames.empty()) {
        return Status::OK();
    }
    auto rc = RunTopologyMigration(metadata, std::move(info), deadline, cancellation);
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology stream migration deadline exceeded");
    return rc;
}

Status SCMigrateMetadataManager::RunTopologyMigration(
    const std::shared_ptr<master::SCMetadataManager> &metadata, MigrateMetaInfo info,
    std::chrono::steady_clock::time_point deadline, const cluster::CancellationToken &cancellation)
{
    info.deadline = deadline;
    info.cancellation = cancellation;
    const auto futureKey = std::make_pair(info.destAddr, info.operationId);
    TbbFutureThreadTable::accessor accessor;
    if (!futureThread_.find(accessor, futureKey)) {
        auto traceId = Trace::Instance().GetTraceID();
        auto future = threadPool_->Submit([this, metadata, info = std::move(info), traceId]() mutable {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            return AsyncMigrateMetadata(metadata, info);
        });
        futureThread_.emplace(accessor, futureKey, std::move(future));
    }
    while (std::chrono::steady_clock::now() < deadline && !cancellation.IsCancelled()) {
        const auto pollDeadline = std::min(deadline, std::chrono::steady_clock::now() + TOPOLOGY_CANCELLATION_POLL);
        if (accessor->second.wait_until(pollDeadline) == std::future_status::ready) {
            try {
                auto result = accessor->second.get();
                futureThread_.erase(accessor);
                RETURN_IF_NOT_OK(result.first);
                CHECK_FAIL_RETURN_STATUS(result.second.empty(), K_TRY_AGAIN,
                                         "topology stream migration has failed items");
                return Status::OK();
            } catch (const std::exception &error) {
                futureThread_.erase(accessor);
                RETURN_STATUS(K_RUNTIME_ERROR, std::string("topology stream migration exception: ") + error.what());
            }
        }
    }
    cancellation.SynchronizeSourceMutations();
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology stream migration cancelled");
    RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "topology stream migration deadline exceeded");
}

Status SCMigrateMetadataManager::CheckTopologyExecution(const MigrateMetaInfo &info) const
{
    if (info.topologyVersion == 0) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(!info.cancellation.IsCancelled(), K_NOT_READY,
                             "topology stream migration cancelled before source commit");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < info.deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology stream migration expired before source commit");
    return Status::OK();
}

void SCMigrateMetadataManager::HandleMigrationFailed(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info,
    std::unordered_map<std::string, int> &retryCounter)
{
    info.streamNames.clear();
    for (const auto &stream : info.failedStreamNames) {
        if (++retryCounter[stream] > MAX_MIGRATE_CNT_PER_STREAM) {
            scMetadataManager->HandleMetaDataMigrationSuccess(stream);
            LOG(WARNING) << "Stream " << stream << " abandoned after multiple failed migration retries";
        } else {
            info.streamNames.emplace_back(stream);
        }
    }
    return;
}

Status SCMigrateMetadataManager::MigrateMetaDataWithRetry(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info, bool isNetworkRecovery)
{
    int timeInterval = 500;
    INJECT_POINT("SCMigrateMetadataManager.MigrateMetaDataWithRetry.interval", [&timeInterval] (int interval) {
        timeInterval = interval;
        return Status::OK();
    });
    Status status;
    Timer timer;
    HostPort destAddr;
    RETURN_IF_NOT_OK(destAddr.ParseString(info.destAddr));
    std::unordered_map<std::string, int> retryCounter;
    Raii clean([&scMetadataManager, &info]() { scMetadataManager->CleanMigratingItems(info.streamNames); });

    while (!exitFlag_) {
        if ((!isNetworkRecovery && CheckMigrationTargetConnection(topologyMembership_, destAddr).IsError())
            || (isNetworkRecovery && timer.ElapsedSecond() > FLAGS_node_timeout_s)) {
            break;
        }
        status = MigrateMetaData(scMetadataManager, info);
        if (status.IsError() && timer.ElapsedSecond() < FLAGS_node_dead_timeout_s) {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeInterval));
            continue;
        }

        if (!info.failedStreamNames.empty()) {
            HandleMigrationFailed(scMetadataManager, info, retryCounter);
            if (!info.streamNames.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(timeInterval));
                continue;
            }
        }
        info.streamNames.clear();
        LOG(INFO) << "Migrate to " << info.destAddr << " success.";
        return Status::OK();
    }

    return Status(K_RPC_UNAVAILABLE,
                  FormatString("LastStatus: %s. The connection to %s is %u. Unfinished stream size %u. "
                               "Time elapsed %d seconds. isNetworkRecovery %s",
                               status.ToString(), info.destAddr,
                               CheckMigrationTargetConnection(topologyMembership_, destAddr).IsOk(),
                               info.streamNames.size(), timer.ElapsedSecond(), isNetworkRecovery));
}

Status SCMigrateMetadataManager::MigrateMetaData(const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
                                                 MigrateMetaInfo &info)
{
    auto status = StartMigrateMetadataForScaleout(scMetadataManager, info);
    if (status.IsError()) {
        LOG(ERROR) << "Submit migrate task failed: " << status.GetMsg();
        return status;
    }
    auto workerId = info.operationId.empty() ? scMetadataManager->GetWorkerId() : info.operationId;
    status = GetMigrateMetadataResult(workerId, info.destAddr, info.failedStreamNames);
    if (status.IsError()) {
        LOG(ERROR) << "GetMigrateMetadataResult failed. " << status.GetMsg();
    }
    return status;
}

Status SCMigrateMetadataManager::StartMigrateMetadataForScaleout(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info)
{
    auto futureKey = std::make_pair(info.destAddr, scMetadataManager->GetWorkerId());
    if (!info.operationId.empty()) {
        futureKey.second = info.operationId;
    }
    TbbFutureThreadTable::accessor accessor;
    if (futureThread_.find(accessor, futureKey)) {
        RETURN_STATUS(
            StatusCode::K_TRY_AGAIN,
            FormatString("The destination address[%s] has unfinished tasks. Please try again later.", info.destAddr));
    } else {
        auto traceId = Trace::Instance().GetTraceID();
        std::future<std::pair<Status, std::vector<std::string>>> future =
            threadPool_->Submit([this, scMetadataManager, &info, traceId] {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                return AsyncMigrateMetadata(scMetadataManager, info);
            });
        futureThread_.emplace(accessor, futureKey, std::move(future));
    }
    return Status::OK();
}

Status SCMigrateMetadataManager::GetMigrateMetadataResult(const std::string &workerId, const std::string &destination,
                                                          std::vector<std::string> &failedStreams)
{
    auto futureKey = std::make_pair(destination, workerId);
    TbbFutureThreadTable::accessor accessor;
    auto found = futureThread_.find(accessor, futureKey);
    CHECK_FAIL_RETURN_STATUS(found, StatusCode::K_RUNTIME_ERROR, "Can't find async future.");
    accessor->second.wait();
    auto result = accessor->second.get();
    futureThread_.erase(accessor);
    failedStreams = result.second;
    return result.first;
}

std::pair<Status, std::vector<std::string>> SCMigrateMetadataManager::AsyncMigrateMetadata(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info)
{
    LOG(INFO) << "Start migrate metadata. destination:" << info.destAddr
              << ", source workerId:" << scMetadataManager->GetWorkerId() << ", dest workerId:" << info.destWorkerId
              << ", stream count:" << info.streamNames.size();

    std::unique_ptr<MasterMasterSCApi> api;
    auto CreateApi = [this, &info, &api]() -> Status {
        HostPort dest;
        RETURN_IF_NOT_OK(dest.ParseString(info.destAddr));
        api = std::make_unique<MasterMasterSCApi>(dest, localHostPort_, akSkManager_);
        RETURN_IF_NOT_OK(api->Init());
        return Status::OK();
    };

    Status s = CreateApi();
    if (s.IsError()) {
        return make_pair(s, info.streamNames);
    }

    std::vector<std::string> failedStreams;
    s = MigrateMetadataForScaleout(scMetadataManager, api, info, failedStreams);
    LOG(INFO) << "Final migrate metadata. destination: " << info.destAddr
              << ", source workerId:" << scMetadataManager->GetWorkerId() << ", dest workerId:" << info.destWorkerId
              << ", stream count: " << info.streamNames.size() << ", failed stream count: " << failedStreams.size()
              << ", status: " << s.ToString();
    return make_pair(s, failedStreams);
}

void SCMigrateMetadataManager::RecordBatchMigrationFailure(
    const MetaForSCMigrationPb &metadata, const std::shared_ptr<master::SCMetadataManager> &scMetadataManager,
    std::vector<std::string> &failedStreams)
{
    scMetadataManager->HandleMetaDataMigrationFailed(metadata);
    failedStreams.emplace_back(metadata.meta().stream_name());
}

Status SCMigrateMetadataManager::ApplyBatchMigrationResponse(
    const MigrateSCMetadataReqPb &req, const MigrateSCMetadataRspPb &rsp,
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, std::vector<std::string> &failedStreams,
    const MigrateMetaInfo &info)
{
    if (rsp.results_size() != req.stream_metas_size()) {
        for (const auto &metadata : req.stream_metas()) {
            RecordBatchMigrationFailure(metadata, scMetadataManager, failedStreams);
        }
        RETURN_STATUS(K_TRY_AGAIN, "stream metadata migration response size mismatch");
    }
    for (int index = 0; index < rsp.results_size(); ++index) {
        const auto &metadata = req.stream_metas(index);
        if (rsp.results(index) == MigrateSCMetadataRspPb::SUCCESSFUL) {
            const auto commit = [&] {
                scMetadataManager->HandleMetaDataMigrationSuccess(metadata.meta().stream_name());
                return Status::OK();
            };
            const auto commitStatus = info.topologyVersion == 0
                                          ? commit()
                                          : info.cancellation.CommitSourceMutationIfActive(info.deadline, commit);
            if (commitStatus.IsError()) {
                for (int remaining = index; remaining < req.stream_metas_size(); ++remaining) {
                    RecordBatchMigrationFailure(req.stream_metas(remaining), scMetadataManager, failedStreams);
                }
                return commitStatus;
            }
        } else {
            RecordBatchMigrationFailure(metadata, scMetadataManager, failedStreams);
        }
    }
    return Status::OK();
}

Status SCMigrateMetadataManager::BatchMigrateMetadata(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, std::unique_ptr<MasterMasterSCApi> &api,
    MigrateSCMetadataReqPb &req, std::vector<std::string> &failedStreams, const MigrateMetaInfo &info)
{
    INJECT_POINT("BatchMigrateMetadata.delay", [](uint32_t delay_s) {
        sleep(delay_s);
        return Status::OK();
    });
    auto copyReq = req;
    for (auto &metadata : *copyReq.mutable_stream_metas()) {
        metadata.clear_notifications();
    }
    MigrateSCMetadataRspPb rsp;
    auto sendStatus = CheckTopologyExecution(info);
    if (sendStatus.IsOk()) {
        sendStatus = akSkManager_->GenerateSignature(copyReq);
    }
    if (sendStatus.IsOk()) {
        req.set_access_key(copyReq.access_key());
        req.set_timestamp(copyReq.timestamp());
        req.set_signature(copyReq.signature());
        sendStatus = api->MigrateSCMetadata(req, rsp);
    }
    if (sendStatus.IsOk()) {
        sendStatus = CheckTopologyExecution(info);
    }
    if (sendStatus.IsError()) {
        LOG(WARNING) << "Send metadata for migration failed. s=" << sendStatus.ToString();
        for (const auto &metadata : req.stream_metas()) {
            RecordBatchMigrationFailure(metadata, scMetadataManager, failedStreams);
        }
        return sendStatus;
    }
    RETURN_IF_NOT_OK(ApplyBatchMigrationResponse(req, rsp, scMetadataManager, failedStreams, info));
    INJECT_POINT("BatchMigrateMetadata.finish");
    return Status::OK();
}

Status SCMigrateMetadataManager::MigrateMetadataForScaleout(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, std::unique_ptr<MasterMasterSCApi> &api,
    const MigrateMetaInfo &info, std::vector<std::string> &failedStreams)
{
    MigrateSCMetadataReqPb req;
    uint32_t objBatch = 300;  // Comparison test: The performance is optimal when the batch number is 300.
    uint32_t count = 0;
    InitializeMigrationRequest(info, req);
    Status lastRc;
    for (const auto &streamName : info.streamNames) {
        Status s = scMetadataManager->FillMetadataForMigration(streamName, req.add_stream_metas());
        if (s.IsError()) {
            LOG(WARNING) << "Fill metadata for migration failed. s=" << s.ToString();
            req.mutable_stream_metas()->RemoveLast();
            scMetadataManager->CleanMigratingItems({ streamName });
            continue;
        }
        ++count;
        if (count >= objBatch) {
            auto rc = BatchMigrateMetadata(scMetadataManager, api, req, failedStreams, info);
            lastRc = rc.IsError() ? rc : lastRc;
            req.Clear();
            InitializeMigrationRequest(info, req);
            count = 0;
        }
    }

    if (count > 0) {
        auto rc = BatchMigrateMetadata(scMetadataManager, api, req, failedStreams, info);
        lastRc = rc.IsError() ? rc : lastRc;
    }
    return lastRc;
}

void SCMigrateMetadataManager::InitializeMigrationRequest(const MigrateMetaInfo &info,
                                                          MigrateSCMetadataReqPb &req) const
{
    req.set_source_addr(localHostPort_.ToString());
    if (info.topologyVersion == 0) {
        return;
    }
    req.set_topology_version(info.topologyVersion);
    req.set_batch_epoch(info.batchEpoch);
    req.set_source_member_id(info.sourceMemberId);
    req.set_target_member_id(info.targetMemberId);
}
}  // namespace master
}  // namespace datasystem
