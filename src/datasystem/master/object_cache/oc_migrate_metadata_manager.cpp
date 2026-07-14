/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Migrating Data in Scaling Scenarios
 */
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"

#include <algorithm>
#include <exception>
#include <unordered_set>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/worker_topology_references.h"

namespace datasystem {
namespace master {
static constexpr int MOVE_THREAD_NUM = 4;
constexpr uint32_t OBJECT_BATCH = 300;  // Comparison test: The performance is optimal when the batch number is 300.

Status BuildMigrationFailureStatus(const Status &lastStatus,
                                   const OCMigrateMetadataManager::MigrateMetaInfo &info,
                                   double elapsedSeconds, bool isNetworkRecovery, bool connected)
{
    return Status(K_RPC_UNAVAILABLE,
                  FormatString("LastStatus: %s. The connection to %s is %s. Unfinished obj size %zu. "
                               "Time elapsed %.3f seconds. isNetworkRecovery %s",
                               lastStatus.ToString(), info.destAddr, connected ? "ready" : "unavailable",
                               info.objectKeys.size(), elapsedSeconds, isNetworkRecovery ? "true" : "false"));
}

OCMigrateMetadataManager &OCMigrateMetadataManager::Instance()
{
    static OCMigrateMetadataManager instance;
    return instance;
}

Status OCMigrateMetadataManager::Init(
    const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
    worker::WorkerTopologyReferences *cm, MetadataManagerHolder *metadataManagerHolder)
{
    localHostPort_ = localHostPort;
    akSkManager_ = std::move(akSkManager);
    cm_ = cm;
    metadataManagerHolder_ = metadataManagerHolder;
    threadPool_ = std::make_unique<ThreadPool>(0, MOVE_THREAD_NUM, "MigrateMetadataThreadPool");

    return Status::OK();
}

OCMigrateMetadataManager::~OCMigrateMetadataManager()
{
    Shutdown();
    LOG(INFO) << "~OCMigrateMetadataManager";
}

void OCMigrateMetadataManager::Shutdown()
{
    exitFlag_ = true;
    cm_ = nullptr;
}

Status OCMigrateMetadataManager::MigrateTopologyMetadata(
    const cluster::TopologyPhaseAction &action, const cluster::IKeyFilter &filter,
    const std::string &businessOperationId, std::chrono::steady_clock::time_point deadline,
    const cluster::CancellationToken &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(action.source.has_value() && action.target.has_value(), K_INVALID,
                             "topology metadata migration lacks source or target");
    CHECK_FAIL_RETURN_STATUS(!businessOperationId.empty(), K_INVALID, "empty topology business operation id");
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology metadata migration cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology metadata migration deadline exceeded");
    std::shared_ptr<master::OCMetadataManager> metadata;
    RETURN_IF_NOT_OK(metadataManagerHolder_->GetOcMetadataManager(metadata));
    MigrateMetaInfo info;
    info.destAddr = action.target->address;
    info.operationId = businessOperationId;
    info.topologyVersion = action.topologyVersion;
    info.batchEpoch = action.batchEpoch;
    info.sourceMemberId = action.source->id;
    info.targetMemberId = action.target->id;
    metadata->GetMetasMatch([&filter](const std::string &key) { return filter.Contains(key); }, info.objectKeys);
    CollectTopologyMigrationKeys(metadata, filter, info);
    if (info.selectedKeys.empty()) {
        return Status::OK();
    }
    auto rc = RunTopologyMigration(metadata, std::move(info), deadline, cancellation);
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology metadata migration deadline exceeded");
    return rc;
}

void OCMigrateMetadataManager::CollectTopologyMigrationKeys(
    const std::shared_ptr<master::OCMetadataManager> &metadata, const cluster::IKeyFilter &filter,
    MigrateMetaInfo &info)
{
    const auto matches = [&filter](const std::string &key) { return filter.Contains(key); };
    info.selectedKeys.insert(info.objectKeys.begin(), info.objectKeys.end());
    std::vector<std::string> vectorKeys;
    metadata->GetRemoteClientIdsMatch(matches, vectorKeys);
    info.selectedKeys.insert(vectorKeys.begin(), vectorKeys.end());
    vectorKeys.clear();
    metadata->GetSubscibeInfoMatch(matches, vectorKeys);
    info.selectedKeys.insert(vectorKeys.begin(), vectorKeys.end());
    std::unordered_set<std::string> setKeys;
    metadata->GetObjRefsMatch(matches, setKeys);
    info.selectedKeys.insert(setKeys.begin(), setKeys.end());
    setKeys.clear();
    metadata->GetNestedRefsMatch(matches, setKeys);
    info.selectedKeys.insert(setKeys.begin(), setKeys.end());
    GlobalDeleteInfoMap deleteKeys;
    metadata->GetObjGlobalCacheDeletesMatch(matches, deleteKeys);
    for (const auto &entry : deleteKeys) {
        info.selectedKeys.emplace(entry.first);
    }
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> asyncKeys;
    metadata->GetMetasInAsyncQueueMatch(matches, asyncKeys);
    for (const auto &entry : asyncKeys) {
        info.selectedKeys.emplace(entry.first);
    }
    vectorKeys.clear();
    // Device metadata is represented by one pseudo-key and migrates as an indivisible table.
    metadata->GetDeviceOcManager()->GetDeviceMetasMatch(matches, vectorKeys);
    info.selectedKeys.insert(vectorKeys.begin(), vectorKeys.end());
}

Status OCMigrateMetadataManager::RunTopologyMigration(
    const std::shared_ptr<master::OCMetadataManager> &metadata, MigrateMetaInfo info,
    std::chrono::steady_clock::time_point deadline, const cluster::CancellationToken &cancellation)
{
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
                return result.first;
            } catch (const std::exception &error) {
                futureThread_.erase(accessor);
                RETURN_STATUS(K_RUNTIME_ERROR, std::string("topology object migration exception: ") + error.what());
            }
        }
    }
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology object migration cancelled");
    RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED, "topology object migration deadline exceeded");
}

Status OCMigrateMetadataManager::MigrateMetaDataWithRetry(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadatManager, MigrateMetaInfo &info, bool isNetworkRecovery)
{
    const int timeInterval = 500;
    Status status;
    Timer timer;
    HostPort destAddr;
    RETURN_IF_NOT_OK(destAddr.ParseString(info.destAddr));
    int retryTimes = 0;
    Raii clean([&ocMetadatManager, &info]() { ocMetadatManager->CleanMigratingItems(info.objectKeys); });
    while (!exitFlag_) {
        // This judgment includes two situations:
        // 1. Scale-down before scale-up is completed;
        // 2. Submit the scale-up task first, and then process the put event of the clusterNodeTable of the scale-up
        // node, retry until success.
        // 3. for dest worker timeout, retry for 3 times, interval is 100 ms
        if ((!isNetworkRecovery && (status = worker::CheckTopologyMemberConnection(cm_, destAddr, true)).IsError())) {
            LOG(ERROR) << "Check connection of " << info.destAddr << " failed: " << status.ToString();
            static const int totleRetryTimes = 3;
            static const int sleepTimeMs = 100;
            auto validWorkers = worker::GetValidTopologyMembers(cm_);
            if (validWorkers.find(destAddr.ToString()) == validWorkers.end()
                || worker::IsTopologyMemberPreLeaving(cm_, destAddr.ToString())) {
                // worker is scale down
                LOG(WARNING) << "The dest node cannot be found, so the migration task is abandoned.";
                break;
            } else if (status.GetCode() == K_NOT_FOUND) {
                std::this_thread::sleep_for(std::chrono::milliseconds(timeInterval));
                continue;
            } else if (retryTimes < totleRetryTimes) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
                retryTimes++;
                continue;
            } else {
                LOG(WARNING) << "The dest node cannot connect after retry 3 times.";
                break;
            }
        }
        if (isNetworkRecovery && timer.ElapsedSecond() > FLAGS_node_timeout_s) {
            break;
        }
        status = MigrateMetaData(ocMetadatManager, info);
        if (status.IsError()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(timeInterval));
            continue;
        }

        if (!info.failedIds.empty()) {
            info.objectKeys = std::move(info.failedIds);
            continue;
        }
        info.objectKeys.clear();
        LOG(INFO) << "Migrate to " << info.destAddr << " success.";
        return Status::OK();
    }

    const bool connected = worker::CheckTopologyMemberConnection(cm_, destAddr, true).IsOk();
    return BuildMigrationFailureStatus(status, info, timer.ElapsedSecond(), isNetworkRecovery, connected);
}

Status OCMigrateMetadataManager::MigrateMetaData(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                                 MigrateMetaInfo &info)
{
    auto status = StartMigrateMetadataForScaleout(ocMetadataManager, info);
    if (status.IsError()) {
        LOG(ERROR) << "Submit migrate task failed: " << status.GetMsg();
        return status;
    }
    auto workerId = info.operationId.empty() ? ocMetadataManager->GetWorkerId() : info.operationId;
    status = GetMigrateMetadataResult(workerId, info.destAddr, info.failedIds);
    if (status.IsError()) {
        LOG(ERROR) << "GetMigrateMetadataResult failed. " << status.GetMsg();
    }
    return status;
}

Status OCMigrateMetadataManager::StartMigrateMetadataForScaleout(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, MigrateMetaInfo &info)
{
    auto futureKey = std::make_pair(info.destAddr, ocMetadataManager->GetWorkerId());
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
            threadPool_->Submit([this, &info, ocMetadataManager, traceId] {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                return AsyncMigrateMetadata(ocMetadataManager, info);
            });
        futureThread_.emplace(accessor, futureKey, std::move(future));
    }
    return Status::OK();
}

Status OCMigrateMetadataManager::GetMigrateMetadataResult(const std::string &workerId, const std::string &destination,
                                                          std::vector<std::string> &failedObjectKeys)
{
    auto futureKey = std::make_pair(destination, workerId);
    TbbFutureThreadTable::accessor accessor;
    auto found = futureThread_.find(accessor, futureKey);
    CHECK_FAIL_RETURN_STATUS(found, StatusCode::K_RUNTIME_ERROR, "Can't find async future.");
    accessor->second.wait();
    auto result = accessor->second.get();
    futureThread_.erase(accessor);
    failedObjectKeys = result.second;
    return result.first;
}

std::pair<Status, std::vector<std::string>> OCMigrateMetadataManager::AsyncMigrateMetadata(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, MigrateMetaInfo &info)
{
    LOG(INFO) << "Start migrate metadata. destination:" << info.destAddr
              << ", source workerId:" << ocMetadataManager->GetWorkerId() << ", dest workerId:" << info.destWorkerId
              << ", object count:" << info.objectKeys.size();

    std::unique_ptr<MasterMasterOCApi> api;
    auto createApi = [this, &info, &api]() -> Status {
        HostPort dest;
        RETURN_IF_NOT_OK(dest.ParseString(info.destAddr));
        api = std::make_unique<MasterMasterOCApi>(dest, localHostPort_, akSkManager_);
        RETURN_IF_NOT_OK(api->Init());
        return Status::OK();
    };

    Status s = createApi();
    if (s.IsError()) {
        return make_pair(s, info.objectKeys);
    }
    std::vector<std::string> failedIds;
    s = MigrateMetadataForScaleout(ocMetadataManager, api, info, failedIds);
    LOG(INFO) << "Final migrate metadata. destination:" << info.destAddr
              << ", source workerId:" << ocMetadataManager->GetWorkerId() << ", object count:" << info.objectKeys.size()
              << ", failed object count:" << failedIds.size() << ", s=" << s.ToString();
    return make_pair(s, failedIds);
}

Status OCMigrateMetadataManager::BatchMigrateMetadata(
    std::unique_ptr<MasterMasterOCApi> &api, MigrateMetadataReqPb &req,
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, std::vector<std::string> &failedObjectKeys,
    const std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncMap)
{
    INJECT_POINT("BatchMigrateMetadata.delay", [](uint32_t delay_s) {
        sleep(delay_s);
        return Status::OK();
    });

    MigrateMetadataRspPb rsp;
    auto streamSendData = [this, &api, &req, &rsp]() -> Status {
        RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
        INJECT_POINT("BatchMigrateMetadata.rpc.error");
        RETURN_IF_NOT_OK(api->MigrateMetadata(req, rsp));
        return Status::OK();
    };

    Status s = streamSendData();
    if (s.IsError()) {
        LOG(WARNING) << "Fill metadata for migration failed. s=" << s.ToString();
        INJECT_POINT("BatchMigrateMetadata.HandleFailed.before");
        for (const auto &meta : req.object_metas()) {
            ocMetadataManager->HandleMetaDataMigrationFailed(meta, asyncMap);
            if (std::find(failedObjectKeys.begin(), failedObjectKeys.end(), meta.object_key())
                == failedObjectKeys.end()) {
                failedObjectKeys.emplace_back(meta.object_key());
            }
        }
        INJECT_POINT("BatchMigrateMetadata.HandleFailed.after");
        return s;
    }
    int num = 0;
    for (auto &result : rsp.results()) {
        auto &meta = req.object_metas()[num];
        if (result == MigrateMetadataRspPb::SUCCESSFUL) {
            ocMetadataManager->HandleMetaDataMigrationSuccess(meta.object_key());
            std::vector<std::string> remoteClientIds{ meta.client_ids().begin(), meta.client_ids().end() };
            ocMetadataManager->HandleObjRefDataMigrationOnSuccess(meta.object_key(), remoteClientIds);
            ocMetadataManager->HandleNestedRefMigrateSuccess(meta.object_key());
        } else {
            ocMetadataManager->HandleMetaDataMigrationFailed(meta, asyncMap);
            if (std::find(failedObjectKeys.begin(), failedObjectKeys.end(), meta.object_key())
                == failedObjectKeys.end()) {
                failedObjectKeys.emplace_back(meta.object_key());
            }
        }
        ++num;
    }
    if (!req.sub_metas().empty()) {
        ocMetadataManager->HandleSubDataMigrateSuccess(req);
    }
    if (!ocMetadataManager->GetDeviceOcManager()->CheckDeviceMetasMigrateInfoIsEmpty(req)) {
        LOG(INFO) << "Device meta migrate ok, handle success";
        ocMetadataManager->GetDeviceOcManager()->HandleMigrateSuccess();
    }
    return Status::OK();
}

bool OCMigrateMetadataManager::TryFillObjectNestedRefMeta(
    const std::string &objectKey, const std::unordered_set<std::string> &objectRefs,
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, MetaForMigrationPb &meta)
{
    auto it = objectRefs.find(objectKey);
    if (it == objectRefs.end()) {
        return false;
    }
    ocMetadataManager->FillObjRefsForMigration(objectKey, meta);
    ocMetadataManager->FillNestedInfoForMigration(objectKey, meta);
    return true;
}

bool OCMigrateMetadataManager::TryFillObjectRefMeta(const std::string &objectKey,
                                                    const std::unordered_set<std::string> &objectRefs,
                                                    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                                    MetaForMigrationPb &meta)
{
    auto it = objectRefs.find(objectKey);
    if (it == objectRefs.end()) {
        return false;
    }
    ocMetadataManager->FillObjRefsForMigration(objectKey, meta);
    return true;
}

bool OCMigrateMetadataManager::TryFillGlobalCacheDeleteMeta(const std::string &objectKey,
                                                            const GlobalDeleteInfoMap &globalCacheDeletes,
                                                            MetaForMigrationPb &meta)
{
    auto it = globalCacheDeletes.find(objectKey);
    if (it == globalCacheDeletes.end()) {
        return false;
    }
    for (const auto &versions : it->second) {
        VLOG(1) << "[Migrate Metadata] Global cache delelte id: " << objectKey << ", object version: " << versions.first
                << ", delelte version: " << versions.second.deleteVersion;
        auto *deletePb = meta.add_global_cache_dels();
        deletePb->set_object_version(versions.first);
        deletePb->set_delete_version(versions.second.deleteVersion);
        deletePb->set_target_worker_address(versions.second.targetWorkerAddress);
    }
    return true;
}

bool OCMigrateMetadataManager::TryFillAsyncL2Meta(
    const std::string &objectKey,
    const std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncL2Metas,
    MetaForMigrationPb &meta)
{
    auto it = asyncL2Metas.find(objectKey);
    if (it == asyncL2Metas.end()) {
        return false;
    }
    for (const auto &ele : it->second) {
        VLOG(1) << "[Migrate Metadata] Async element: " << *ele;
        auto *op = meta.add_wait_async_to_l2_elements();
        op->set_key(ele->Key());
        op->set_table(ele->Table());
        op->set_value(ele->Value());
        op->set_op(static_cast<uint32_t>(ele->RequestType()));
        op->set_timestamp(ele->BeginTimestampUs());
        op->set_trace_id(ele->TraceID());
    }
    return true;
}

void OCMigrateMetadataManager::FillRemoteClientIds(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                                   const std::unordered_set<std::string> &selectedKeys,
                                                   const std::string &destAddr,
                                                   MigrateMetadataReqPb &req)
{
    std::vector<std::string> migrationClientIds;
    ocMetadataManager->GetRemoteClientIdsMatch(
        [&selectedKeys](const std::string &objKey) { return selectedKeys.count(objKey) > 0; },
        migrationClientIds);
    for (auto &remoteClientId : migrationClientIds) {
        ocMetadataManager->FillClientIdRefsForMigration(remoteClientId, destAddr, req.add_client_id_refs());
    }
}

void OCMigrateMetadataManager::FillSubscribeInfos(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                                  const std::unordered_set<std::string> &selectedKeys,
                                                  MigrateMetadataReqPb &req)
{
    std::vector<std::string> subObjs;
    std::vector<SubscribeInfoPb> subInfos;
    ocMetadataManager->GetSubscibeInfoMatch(
        [&selectedKeys](const std::string &objKey) { return selectedKeys.count(objKey) > 0; },
        subObjs);
    ocMetadataManager->FillSubMetas(subObjs, subInfos);
    *req.mutable_sub_metas() = { subInfos.begin(), subInfos.end() };
}

Status OCMigrateMetadataManager::MigrateMetadataForScaleout(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, std::unique_ptr<MasterMasterOCApi> &api,
    MigrateMetaInfo &info, std::vector<std::string> &failedIds)
{
    MigrateMetadataReqPb req;
    uint32_t count = 0;
    InitializeMigrationRequest(info, req);
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> asyncMap;
    for (const auto &objectKey : info.objectKeys) {
        MetaForMigrationPb meta;
        ocMetadataManager->FillObjRefsForMigration(objectKey, meta);
        ocMetadataManager->FillNestedInfoForMigration(objectKey, meta);
        Status s = ocMetadataManager->FillMetadataForMigration(objectKey, meta, asyncMap);
        if (s.IsError()) {
            // if meta not found or CreateSerializedStringForMeta failed status will be error
            // if status is error, not add meta to req.
            LOG(WARNING) << "Fill metadata for migration failed. s=" << s.ToString();
            ocMetadataManager->CleanMigratingItems({ objectKey });
            continue;
        }
        req.mutable_object_metas()->Add(std::move(meta));
        ++count;
        if (count >= OBJECT_BATCH) {
            RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds, asyncMap));
            req.Clear();
            InitializeMigrationRequest(info, req);
            count = 0;
        }
    }
    // left obj meta.
    if (count > 0) {
        INJECT_POINT("BatchMigrateMetadata.delay.left");
        RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds, asyncMap));
    }

    MigrateNoMetaInfoForScaleout(ocMetadataManager, api, info, failedIds);
    MigrateDeviceMetaForScaleout(ocMetadataManager, api, info);
    return Status::OK();
}

void OCMigrateMetadataManager::InitializeMigrationRequest(const MigrateMetaInfo &info,
                                                          MigrateMetadataReqPb &req) const
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

void OCMigrateMetadataManager::MigrateDeviceMetaForScaleout(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, std::unique_ptr<MasterMasterOCApi> &api,
    MigrateMetaInfo &info)
{
    MigrateMetadataReqPb req;
    InitializeMigrationRequest(info, req);
    GetAndFillMigrateDeviceMetaInfo(ocMetadataManager, info.selectedKeys, req);
    std::vector<std::string> failedIds;
    if (!ocMetadataManager->GetDeviceOcManager()->CheckDeviceMetasMigrateInfoIsEmpty(req)) {
        LOG(INFO) << "Device metas are not empty, start to migrate device meta.";
        BatchMigrateMetadata(api, req, ocMetadataManager, failedIds);
    }
}

void OCMigrateMetadataManager::GetAndFillMigrateDeviceMetaInfo(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
    const std::unordered_set<std::string> &selectedKeys, MigrateMetadataReqPb &req)
{
    std::vector<std::string> migrationKeys;
    ocMetadataManager->GetDeviceOcManager()->GetDeviceMetasMatch(
        [&selectedKeys](const std::string &objKey) { return selectedKeys.count(objKey) > 0; },
        migrationKeys);
    if (migrationKeys.size() > 0) {
        ocMetadataManager->GetDeviceOcManager()->FillDeviceMetas(req);
    }
}

Status OCMigrateMetadataManager::MigrateNoMetaInfoForScaleout(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, std::unique_ptr<MasterMasterOCApi> &api,
    MigrateMetaInfo &info, std::vector<std::string> &failedIds)
{
    MigrateMetadataReqPb req;
    InitializeMigrationRequest(info, req);
    uint64_t count = 0;

    // migrate object subscribe info.
    FillSubscribeInfos(ocMetadataManager, info.selectedKeys, req);
    // migrate master app ref
    FillRemoteClientIds(ocMetadataManager, info.selectedKeys, info.destAddr, req);
    if (static_cast<uint64_t>(req.remote_client_ids_size() + req.sub_metas_size()) >= OBJECT_BATCH) {
        RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds));
        req.Clear();
        count = 0;
        InitializeMigrationRequest(info, req);
    } else {
        count += (static_cast<uint64_t>(req.remote_client_ids_size()) + static_cast<uint64_t>(req.sub_metas_size()));
    }

    NoMetaMigrationKeys keys;
    CollectNoMetaMigrationKeys(ocMetadataManager, info.selectedKeys, keys);
    for (auto it = keys.allKeys.begin(); it != keys.allKeys.end(); ++it) {
        const auto &objectKey = *it;
        MetaForMigrationPb meta;
        meta.set_only_ref(true);  // This flag means the object has no ObjectMetaPb.
        meta.set_object_key(objectKey);
        bool add = TryFillObjectRefMeta(objectKey, keys.objectRefs, ocMetadataManager, meta)
                   | TryFillObjectNestedRefMeta(objectKey, keys.objectNestedRefs, ocMetadataManager, meta)
                   | TryFillGlobalCacheDeleteMeta(objectKey, keys.globalCacheDeletes, meta)
                   | TryFillAsyncL2Meta(objectKey, keys.asyncL2Metas, meta);
        if (add) {
            req.mutable_object_metas()->Add(std::move(meta));
            ++count;
        }
        if (count >= OBJECT_BATCH || (count > 0 && std::next(it) == keys.allKeys.end())) {
            RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds));
            req.Clear();
            count = 0;
            InitializeMigrationRequest(info, req);
        }
    }
    if (count > 0) {
        RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds));
    }
    return Status::OK();
}

void OCMigrateMetadataManager::CollectNoMetaMigrationKeys(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
    const std::unordered_set<std::string> &selectedKeys, NoMetaMigrationKeys &keys)
{
    auto matchFunc = [&selectedKeys](const std::string &objectKey) { return selectedKeys.count(objectKey) > 0; };
    ocMetadataManager->GetObjRefsMatch(matchFunc, keys.objectRefs);
    keys.allKeys = keys.objectRefs;
    ocMetadataManager->GetNestedRefsMatch(matchFunc, keys.objectNestedRefs);
    keys.allKeys.insert(keys.objectNestedRefs.begin(), keys.objectNestedRefs.end());
    ocMetadataManager->GetObjGlobalCacheDeletesMatch(matchFunc, keys.globalCacheDeletes);
    std::transform(keys.globalCacheDeletes.begin(), keys.globalCacheDeletes.end(),
                   std::inserter(keys.allKeys, keys.allKeys.begin()),
                   [](const auto &pair) { return pair.first; });
    ocMetadataManager->GetMetasInAsyncQueueMatch(matchFunc, keys.asyncL2Metas);
    std::transform(keys.asyncL2Metas.begin(), keys.asyncL2Metas.end(),
                   std::inserter(keys.allKeys, keys.allKeys.begin()),
                   [](const auto &pair) { return pair.first; });
}

}  // namespace master
}  // namespace datasystem
