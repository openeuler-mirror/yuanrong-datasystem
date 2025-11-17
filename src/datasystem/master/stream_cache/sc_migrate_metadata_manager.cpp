/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Migrating Data in Scaling Scenarios for Stream Cache.
 */
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"

DS_DECLARE_uint32(node_dead_timeout_s);

namespace datasystem {
namespace master {
static constexpr int MOVE_THREAD_NUM = 4;
static constexpr int MAX_MIGRATE_CNT_PER_STREAM = 30;
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

Status SCMigrateMetadataManager::Init(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
                                      EtcdClusterManager *cm, ReplicaManager *replicaManager)
{
    localHostPort_ = localHostPort;
    akSkManager_ = std::move(akSkManager);
    cm_ = cm;
    threadPool_ = std::make_unique<ThreadPool>(0, MOVE_THREAD_NUM, "ScMigrateMetadata");
    replicaManager_ = replicaManager;

    HashRingEvent::MigrateRanges::GetInstance().AddSubscriber(
        "SCMigrateMetadataManager",
        [this](const std::string &dbName, const std::string &dest, const std::string &destDbName,
               const worker::HashRange &ranges, bool isNetworkRecovery) {
            return MigrateByRanges(dbName, dest, destDbName, ranges, isNetworkRecovery);
        });
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
    HashRingEvent::MigrateRanges::GetInstance().RemoveSubscriber("SCMigrateMetadataManager");
    cm_ = nullptr;
}

Status SCMigrateMetadataManager::MigrateByRanges(const std::string &dbName, const std::string &dest,
                                                 const std::string &destDbName, const worker::HashRange &ranges,
                                                 bool isNetworkRecovery)
{
    CHECK_FAIL_RETURN_STATUS(cm_ != nullptr, K_RUNTIME_ERROR, "SCMigrateMetadataManager has not inited.");

    std::shared_ptr<master::SCMetadataManager> scMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetScMetadataManager(dbName, scMetadataManager),
                                     "dbName not exists");
    MigrateMetaInfo info;
    info.destAddr = dest;
    info.destDbName = destDbName;
    scMetadataManager->GetMetasMatch(
        [this, &ranges](const std::string &objKey) { return cm_->IsInRange(ranges, objKey, ""); }, info.streamNames);

    return MigrateMetaDataWithRetry(scMetadataManager, info, isNetworkRecovery);
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
        if ((!isNetworkRecovery && cm_->CheckConnection(destAddr).IsError())
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
                               status.ToString(), info.destAddr, cm_->CheckConnection(destAddr).IsOk(),
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
    auto dbName = scMetadataManager->GetDbName();
    status = GetMigrateMetadataResult(dbName, info.destAddr, info.failedStreamNames);
    if (status.IsError()) {
        LOG(ERROR) << "GetMigrateMetadataResult failed. " << status.GetMsg();
    }
    return status;
}

Status SCMigrateMetadataManager::StartMigrateMetadataForScaleout(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, MigrateMetaInfo &info)
{
    auto futureKey = std::make_pair(info.destAddr, scMetadataManager->GetDbName());
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

Status SCMigrateMetadataManager::GetMigrateMetadataResult(const std::string &dbName, const std::string &destination,
                                                          std::vector<std::string> &failedStreams)
{
    auto futureKey = std::make_pair(destination, dbName);
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
              << ", source dbName:" << scMetadataManager->GetDbName() << ", dest dbName:" << info.destDbName
              << ", stream count:" << info.streamNames.size();

    std::unique_ptr<MasterMasterSCApi> api;
    auto CreateApi = [this, &info, &api]() -> Status {
        HostPort dest;
        RETURN_IF_NOT_OK(dest.ParseString(info.destAddr));
        api = std::make_unique<MasterMasterSCApi>(dest, localHostPort_, akSkManager_);
        RETURN_IF_NOT_OK(api->Init());
        g_MetaRocksDbName = info.destDbName;
        return Status::OK();
    };

    Status s = CreateApi();
    if (s.IsError()) {
        return make_pair(s, info.streamNames);
    }

    std::vector<std::string> failedStreams;
    s = MigrateMetadataForScaleout(scMetadataManager, api, info.streamNames, failedStreams);
    LOG(INFO) << "Final migrate metadata. destination: " << info.destAddr
              << ", source dbName:" << scMetadataManager->GetDbName() << ", dest dbName:" << info.destDbName
              << ", stream count: " << info.streamNames.size() << ", failed stream count: " << failedStreams.size()
              << ", status: " << s.ToString();
    return make_pair(s, failedStreams);
}

Status SCMigrateMetadataManager::BatchMigrateMetadata(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, std::unique_ptr<MasterMasterSCApi> &api,
    MigrateSCMetadataReqPb &req, std::vector<std::string> &failedStreams)
{
    INJECT_POINT("BatchMigrateMetadata.delay", [](uint32_t delay_s) {
        sleep(delay_s);
        return Status::OK();
    });

    MigrateSCMetadataRspPb rsp;
    auto streamSendData = [this, &api, &req, &rsp]() -> Status {
        auto copyReq = req;
        for (int i = 0; i < copyReq.stream_metas_size(); ++i) {
            auto *meta = copyReq.mutable_stream_metas(i);
            if (meta != nullptr) {
                meta->clear_notifications();
            }
        }
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(copyReq));
        req.set_access_key(copyReq.access_key());
        req.set_timestamp(copyReq.timestamp());
        req.set_signature(copyReq.signature());
        RETURN_IF_NOT_OK(api->MigrateSCMetadata(req, rsp));
        return Status::OK();
    };

    Status s = streamSendData();
    if (s.IsError()) {
        LOG(WARNING) << "Send metadata for migration failed. s=" << s.ToString();
        for (const auto &meta : req.stream_metas()) {
            scMetadataManager->HandleMetaDataMigrationFailed(meta);
            failedStreams.emplace_back(meta.meta().stream_name());
        }
        return s;
    } else {
        int num = 0;
        for (auto &result : rsp.results()) {
            auto &meta = req.stream_metas()[num];
            if (result == MigrateSCMetadataRspPb::SUCCESSFUL) {
                scMetadataManager->HandleMetaDataMigrationSuccess(meta.meta().stream_name());
            } else {
                scMetadataManager->HandleMetaDataMigrationFailed(meta);
                failedStreams.emplace_back(meta.meta().stream_name());
            }
            ++num;
        }
    }
    INJECT_POINT("BatchMigrateMetadata.finish");
    return Status::OK();
}

Status SCMigrateMetadataManager::MigrateMetadataForScaleout(
    const std::shared_ptr<master::SCMetadataManager> &scMetadataManager, std::unique_ptr<MasterMasterSCApi> &api,
    const std::vector<std::string> &streamNames, std::vector<std::string> &failedStreams)
{
    MigrateSCMetadataReqPb req;
    uint32_t objBatch = 300;  // Comparison test: The performance is optimal when the batch number is 300.
    uint32_t count = 0;
    req.set_source_addr(localHostPort_.ToString());
    Status lastRc;
    for (auto &streamName : streamNames) {
        req.set_source_addr(localHostPort_.ToString());
        Status s = scMetadataManager->FillMetadataForMigration(streamName, req.add_stream_metas());
        if (s.IsError()) {
            LOG(WARNING) << "Fill metadata for migration failed. s=" << s.ToString();
            req.mutable_stream_metas()->RemoveLast();
            scMetadataManager->CleanMigratingItems({ streamName });
            continue;
        }
        ++count;
        if (count >= objBatch) {
            auto rc = BatchMigrateMetadata(scMetadataManager, api, req, failedStreams);
            lastRc = rc.IsError() ? rc : lastRc;
            req.Clear();
            req.set_source_addr(localHostPort_.ToString());
            count = 0;
        }
    }

    if (count > 0) {
        auto rc = BatchMigrateMetadata(scMetadataManager, api, req, failedStreams);
        lastRc = rc.IsError() ? rc : lastRc;
    }
    return lastRc;
}
}  // namespace master
}  // namespace datasystem
