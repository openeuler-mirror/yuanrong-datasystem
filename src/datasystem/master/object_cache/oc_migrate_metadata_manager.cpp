/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Migrating Data in Scaling Scenarios
 */
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"

#include <unordered_set>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"

namespace datasystem {
namespace master {
static constexpr int MOVE_THREAD_NUM = 4;
constexpr uint32_t OBJECT_BATCH = 300;  // Comparison test: The performance is optimal when the batch number is 300.
OCMigrateMetadataManager &OCMigrateMetadataManager::Instance()
{
    static OCMigrateMetadataManager instance;
    return instance;
}

Status OCMigrateMetadataManager::Init(const HostPort &localHostPort, std::shared_ptr<AkSkManager> akSkManager,
                                      EtcdClusterManager *cm, ReplicaManager *replicaManager)
{
    localHostPort_ = localHostPort;
    akSkManager_ = std::move(akSkManager);
    cm_ = cm;
    replicaManager_ = replicaManager;
    threadPool_ = std::make_unique<ThreadPool>(0, MOVE_THREAD_NUM, "MigrateMetadataThreadPool");

    HashRingEvent::MigrateRanges::GetInstance().AddSubscriber(
        "OCMigrateMetadataManager",
        [this](const std::string &dbName, const std::string &dest, const std::string &destDbName,
               const worker::HashRange &ranges, bool isNetworkRecovery) {
            return MigrateByRanges(dbName, dest, destDbName, ranges, isNetworkRecovery);
        });
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
    HashRingEvent::MigrateRanges::GetInstance().RemoveSubscriber("OCMigrateMetadataManager");
    cm_ = nullptr;
}

Status OCMigrateMetadataManager::MigrateByRanges(const std::string &dbName, const std::string &dest,
                                                 const std::string &destDbName, const worker::HashRange &ranges,
                                                 bool isNetworkRecovery)
{
    CHECK_FAIL_RETURN_STATUS(cm_ != nullptr, K_RUNTIME_ERROR, "OCMigrateMetadataManager has not inited.");
    std::string rangesStr;
    for (const auto &range : ranges) {
        rangesStr += "[" + std::to_string(range.first) + ", " + std::to_string(range.second) + "], ";
    }
    LOG(INFO) << "migrate task excute, src db name : " << dbName << " , address: " << localHostPort_.ToString()
              << ", to dest workerAddr: " << dest << ", dest db name: " << destDbName << ", ranges: " << rangesStr;
    INJECT_POINT("MigrateByRanges.Delay");
    std::vector<std::string> objRefToBeMigrated;
    std::vector<std::string> remoteClientIds;
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->GetOcMetadataManager(dbName, ocMetadataManager),
                                     "dbName not exists");
    MigrateMetaInfo info;
    info.destAddr = dest;
    info.destDbName = destDbName;
    info.ranges = ranges;
    ocMetadataManager->GetMetasMatch(
        [this, &ranges, &dbName](const std::string &objKey) { return cm_->IsInRange(ranges, objKey, dbName); },
        info.objectKeys);

    return MigrateMetaDataWithRetry(ocMetadataManager, info, isNetworkRecovery);
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
        if ((!isNetworkRecovery && (status = cm_->CheckConnection(destAddr, true)).IsError())) {
            LOG(ERROR) << "Check connection of " << info.destAddr << " failed: " << status.ToString();
            static const int totleRetryTimes = 3;
            static const int sleepTimeMs = 100;
            if (cm_->CheckWorkerIsScaleDown(destAddr.ToString())) {
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

    return Status(K_RPC_UNAVAILABLE,
                  FormatString("LastStatus: %s. The connection to %s is %u. Unfinished obj size %u. "
                               "Time elapsed %d seconds. isNetworkRecovery %s",
                               status.ToString(), info.destAddr, cm_->CheckConnection(destAddr).IsOk(),
                               info.objectKeys.size(), timer.ElapsedSecond(), isNetworkRecovery));
}

Status OCMigrateMetadataManager::MigrateMetaData(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                                 MigrateMetaInfo &info)
{
    auto status = StartMigrateMetadataForScaleout(ocMetadataManager, info);
    if (status.IsError()) {
        LOG(ERROR) << "Submit migrate task failed: " << status.GetMsg();
        return status;
    }
    auto dbName = ocMetadataManager->GetDbName();
    status = GetMigrateMetadataResult(dbName, info.destAddr, info.failedIds);
    if (status.IsError()) {
        LOG(ERROR) << "GetMigrateMetadataResult failed. " << status.GetMsg();
    }
    return status;
}

Status OCMigrateMetadataManager::StartMigrateMetadataForScaleout(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, MigrateMetaInfo &info)
{
    auto futureKey = std::make_pair(info.destAddr, ocMetadataManager->GetDbName());
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

Status OCMigrateMetadataManager::GetMigrateMetadataResult(const std::string &dbName, const std::string &destination,
                                                          std::vector<std::string> &failedObjectKeys)
{
    auto futureKey = std::make_pair(destination, dbName);
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
              << ", source dbName:" << ocMetadataManager->GetDbName() << ", dest dbName:" << info.destDbName
              << ", object count:" << info.objectKeys.size();

    std::unique_ptr<MasterMasterOCApi> api;
    auto createApi = [this, &info, &api]() -> Status {
        HostPort dest;
        RETURN_IF_NOT_OK(dest.ParseString(info.destAddr));
        api = std::make_unique<MasterMasterOCApi>(dest, localHostPort_, akSkManager_);
        RETURN_IF_NOT_OK(api->Init());
        g_MetaRocksDbName = info.destDbName;
        return Status::OK();
    };

    Status s = createApi();
    if (s.IsError()) {
        return make_pair(s, info.objectKeys);
    }
    std::vector<std::string> failedIds;
    s = MigrateMetadataForScaleout(ocMetadataManager, api, info, failedIds);
    LOG(INFO) << "Final migrate metadata. destination:" << info.destAddr
              << ", source dbName:" << ocMetadataManager->GetDbName() << ", object count:" << info.objectKeys.size()
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

bool OCMigrateMetadataManager::TryFillGlobalCacheDeleteMeta(
    const std::string &objectKey,
    const std::unordered_map<std::string, std::unordered_map<uint64_t, uint64_t>> &globalCacheDeletes,
    MetaForMigrationPb &meta)
{
    auto it = globalCacheDeletes.find(objectKey);
    if (it == globalCacheDeletes.end()) {
        return false;
    }
    for (const auto &versions : it->second) {
        VLOG(1) << "[Migrate Metadata] Global cache delelte id: " << objectKey << ", object version: " << versions.first
                << ", delelte version: " << versions.second;
        auto *deletePb = meta.add_global_cache_dels();
        deletePb->set_object_version(versions.first);
        deletePb->set_delete_version(versions.second);
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
                                                   const worker::HashRange &ranges, const std::string &destAddr,
                                                   MigrateMetadataReqPb &req)
{
    std::vector<std::string> migrationClientIds;
    ocMetadataManager->GetRemoteClientIdsMatch(
        [this, &ranges, &ocMetadataManager](const std::string &objKey) {
            return cm_->IsInRange(ranges, objKey, ocMetadataManager->GetDbName());
        },
        migrationClientIds);
    for (auto &remoteClientId : migrationClientIds) {
        ocMetadataManager->FillClientIdRefsForMigration(remoteClientId, destAddr, req.add_client_id_refs());
    }
}

void OCMigrateMetadataManager::FillSubscribeInfos(const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager,
                                                  const worker::HashRange &ranges, MigrateMetadataReqPb &req)
{
    std::vector<std::string> subObjs;
    std::vector<SubscribeInfoPb> subInfos;
    ocMetadataManager->GetSubscibeInfoMatch(
        [this, &ranges, &ocMetadataManager](const std::string &objKey) {
            return cm_->IsInRange(ranges, objKey, ocMetadataManager->GetDbName());
        },
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
    req.set_source_addr(localHostPort_.ToString());
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
            req.set_source_addr(localHostPort_.ToString());
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

void OCMigrateMetadataManager::MigrateDeviceMetaForScaleout(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, std::unique_ptr<MasterMasterOCApi> &api,
    MigrateMetaInfo &info)
{
    MigrateMetadataReqPb req;
    req.set_source_addr(localHostPort_.ToString());
    GetAndFillMigrateDeviceMetaInfo(ocMetadataManager, info.ranges, req);
    std::vector<std::string> failedIds;
    if (!ocMetadataManager->GetDeviceOcManager()->CheckDeviceMetasMigrateInfoIsEmpty(req)) {
        LOG(INFO) << "Device metas are not empty, start to migrate device meta.";
        BatchMigrateMetadata(api, req, ocMetadataManager, failedIds);
    }
}

void OCMigrateMetadataManager::GetAndFillMigrateDeviceMetaInfo(
    const std::shared_ptr<master::OCMetadataManager> &ocMetadataManager, const worker::HashRange &ranges,
    MigrateMetadataReqPb &req)
{
    std::vector<std::string> migrationKeys;
    ocMetadataManager->GetDeviceOcManager()->GetDeviceMetasMatch(
        [this, &ranges, &ocMetadataManager](const std::string &objKey) {
            return cm_->IsInRange(ranges, objKey, ocMetadataManager->GetDbName());
        },
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
    req.set_source_addr(localHostPort_.ToString());
    uint64_t count = 0;

    // migrate object subscribe info.
    FillSubscribeInfos(ocMetadataManager, info.ranges, req);
    // migrate master app ref
    FillRemoteClientIds(ocMetadataManager, info.ranges, info.destAddr, req);
    if (static_cast<uint64_t>(req.remote_client_ids_size() + req.sub_metas_size()) >= OBJECT_BATCH) {
        RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds));
        req.Clear();
        count = 0;
        req.set_source_addr(localHostPort_.ToString());
    } else {
        count += (static_cast<uint64_t>(req.remote_client_ids_size()) + static_cast<uint64_t>(req.sub_metas_size()));
    }

    auto matchFunc = [this, &info, &ocMetadataManager](const std::string &objKey) {
        return cm_->IsInRange(info.ranges, objKey, ocMetadataManager->GetDbName());
    };

    std::unordered_set<std::string> allKeys;
    // migrate objs only have ref.
    std::unordered_set<std::string> objectRefs;
    ocMetadataManager->GetObjRefsMatch(matchFunc, objectRefs);
    allKeys = objectRefs;

    // migrate left nested keys.
    std::unordered_set<std::string> objectNestedRefs;
    ocMetadataManager->GetNestedRefsMatch(matchFunc, objectNestedRefs);
    allKeys.insert(objectNestedRefs.begin(), objectNestedRefs.end());

    // migrate objs only have global cache
    std::unordered_map<std::string, std::unordered_map<uint64_t, uint64_t>> globalCacheDeletes;
    ocMetadataManager->GetObjGlobalCacheDeletesMatch(matchFunc, globalCacheDeletes);
    std::transform(globalCacheDeletes.begin(), globalCacheDeletes.end(), std::inserter(allKeys, allKeys.begin()),
                   [](const auto &pair) { return pair.first; });

    // migrate objs only wait for async delete.
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> asyncL2Metas;
    ocMetadataManager->GetMetasInAsyncQueueMatch(matchFunc, asyncL2Metas);
    std::transform(asyncL2Metas.begin(), asyncL2Metas.end(), std::inserter(allKeys, allKeys.begin()),
                   [](const auto &pair) { return pair.first; });

    for (auto it = allKeys.begin(); it != allKeys.end(); ++it) {
        const auto &objectKey = *it;
        MetaForMigrationPb meta;
        meta.set_only_ref(true);  // This flag means the object has no ObjectMetaPb.
        meta.set_object_key(objectKey);
        bool add = TryFillObjectRefMeta(objectKey, objectRefs, ocMetadataManager, meta)
                   | TryFillObjectNestedRefMeta(objectKey, objectNestedRefs, ocMetadataManager, meta)
                   | TryFillGlobalCacheDeleteMeta(objectKey, globalCacheDeletes, meta)
                   | TryFillAsyncL2Meta(objectKey, asyncL2Metas, meta);
        if (add) {
            req.mutable_object_metas()->Add(std::move(meta));
            ++count;
        }
        if (count >= OBJECT_BATCH || (count > 0 && std::next(it) == allKeys.end())) {
            RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds));
            req.Clear();
            count = 0;
            req.set_source_addr(localHostPort_.ToString());
        }
    }
    if (count > 0) {
        RETURN_IF_NOT_OK(BatchMigrateMetadata(api, req, ocMetadataManager, failedIds));
    }
    return Status::OK();
}

}  // namespace master
}  // namespace datasystem
