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
#include "datasystem/master/object_cache/oc_notify_worker_manager.h"

#include <cstdint>
#include <iterator>
#include <memory>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/master/metadata_redirect_helper.h"
#include "datasystem/master/object_cache/master_master_oc_api.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"

DS_DECLARE_string(worker_address);

namespace datasystem {
namespace master {
OCNotifyWorkerManager::OCNotifyWorkerManager(std::shared_ptr<ObjectMetaStore> objectStore, bool backendStoreExist,
                                             std::shared_ptr<AkSkManager> akSkManager,
                                             OCMetadataManager *ocMetadataManager)
    : objectStore_(std::move(objectStore)),
      interruptFlag_(false),
      backendStoreExist_(backendStoreExist),
      akSkManager_(akSkManager),
      ocMetadataManager_(ocMetadataManager)
{
    subscriberPrefix_ = GetStringUuid();
}

OCNotifyWorkerManager::~OCNotifyWorkerManager()
{
    if (!interruptFlag_) {
        Shutdown();
    }
}

struct SendResult {
    std::shared_ptr<MasterWorkerOCApi> api;
    int64_t tag = -1;
    std::string address;
    Status status;
};  // Result bundle for a single DeleteObject notification

Status OCNotifyWorkerManager::Init()
{
    LOG(INFO) << "init OCNotifyWorkerManager" << this;
    thread_ = std::make_unique<Thread>(&OCNotifyWorkerManager::ProcessAsyncNotifyOp, this);
    thread_->set_name("ProcessAsyncNotifyOp");
    deleteThreadPool_ =
        std::make_unique<datasystem::ThreadPool>(minDeleteThreadSize, maxDeleteThreadSize, "NotifyDeleteSend");
    EraseFailedNodeApiEvent::GetInstance().AddSubscriber(subscriberPrefix_ + "OCNotifyWorkerManager",
                                                         [this](HostPort &node) { EraseMasterWorkerApi(node); });
    RemoveDeadWorkerEvent::GetInstance().AddSubscriber(
        subscriberPrefix_ + "OCNotifyWorkerManager",
        [this](const std::string &workerAddr) { RemoveFaultWorker(workerAddr); });
    return Status::OK();
}

void OCNotifyWorkerManager::Shutdown()
{
    if (!thread_) {
        return;
    }
    EraseFailedNodeApiEvent::GetInstance().RemoveSubscriber(subscriberPrefix_ + "OCNotifyWorkerManager");
    RemoveDeadWorkerEvent::GetInstance().RemoveSubscriber(subscriberPrefix_ + "OCNotifyWorkerManager");
    interruptFlag_ = true;
    cvLock_.Set();
    thread_->join();
    LOG(INFO) << "OCNotifyWorkerManager shut down";
}

void OCNotifyWorkerManager::ProcessAsyncNotifyOp()
{
    auto traceId = GetStringUuid().substr(0, SHORT_TRACEID_SIZE);
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
    LOG(INFO) << "Starting processing asynchronous notification operation thread.";
    while (!interruptFlag_) {
        if (!notifyWorkerOpTable_.empty()) {
            Status rc = ProcessAsyncNotifyOpImpl();
            if (rc.IsError()) {
                LOG(ERROR) << "Process asynchronous notification operation failed, msg: " << rc.ToString();
            }
        }
        cvLock_.WaitFor(ASYNC_SEND_UPDATE_TIME_MS);
    }
    LOG(INFO) << "Terminating processing asynchronous notification operation thread.";
}

Status OCNotifyWorkerManager::ProcessAsyncNotifyOpImpl()
{
    INJECT_POINT("OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess");
    std::vector<std::string> workerIds;
    {
        std::lock_guard<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
        for (const auto &cache : notifyWorkerOpTable_) {
            workerIds.emplace_back(cache.first);
        }
    }

    INJECT_POINT("master.send_cache_invalid", [&workerIds](std::string workerId) {
        auto it = workerIds.begin();
        while (it != workerIds.end()) {
            *it == workerId ? workerIds.erase(it++) : it++;
        }
        return Status::OK();
    });

    for (const auto &workerId : workerIds) {
        Status status = CheckWorkerIsHealthy(workerId);
        if (status.IsError()) {
            LOG(WARNING) << "[async notify] Worker " << workerId << " is unhealthy, detail: " << status.ToString();
            continue;
        }
        std::unordered_set<std::string> objsNeedCacheInvalid;
        {
            std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
            TbbNotifyWorkerOpTable::accessor accessor;
            if (notifyWorkerOpTable_.find(accessor, workerId)) {
                for (const auto &it : accessor->second) {
                    if (TESTFLAG(it.second.type, NotifyWorkerOpType::CACHE_INVALID)) {
                        (void)objsNeedCacheInvalid.emplace(it.first);
                    }
                }
            }
        }
        if (!objsNeedCacheInvalid.empty()) {
            LOG_IF_ERROR(SendCacheInvalidToWorker(workerId, objsNeedCacheInvalid), "");
        }
    }
    return Status::OK();
}

Status OCNotifyWorkerManager::ProcessAsyncDeleteNotifyOpImpl()
{
    LOG(INFO) << "ProcessAsyncDeleteNotifyOpImpl";
    std::vector<std::string> workerIds;
    {
        std::lock_guard<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
        for (const auto &cache : notifyWorkerOpTable_) {
            workerIds.emplace_back(cache.first);
        }
    }
    std::unordered_map<std::shared_ptr<MasterWorkerOCApi>, DeleteApiInfo> api2Tag;
    for (const auto &workerId : workerIds) {
        if (workerId == "") {
            continue;
        }
        LOG(INFO) << "send delete to worker: " << workerId;
        Status status = CheckWorkerIsHealthy(workerId);
        if (status.IsError()) {
            LOG(WARNING) << "[async notify] Worker " << workerId << " is unhealthy, detail: " << status.ToString();
            continue;
        }
        std::unordered_map<std::string, std::uint64_t> objNeedDelete;
        {
            std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
            TbbNotifyWorkerOpTable::const_accessor accessor;
            if (notifyWorkerOpTable_.find(accessor, workerId)) {
                for (const auto &it : accessor->second) {
                    if (TESTFLAG(it.second.type, NotifyWorkerOpType::DELETE)) {
                        (void)objNeedDelete.emplace(it.first, it.second.delObjectVersion);
                    }
                }
            }
        }

        if (objNeedDelete.empty()) {
            continue;
        }

        std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
        status = GetMasterWorkerApi(workerId, masterWorkerApi);
        if (status.IsError()) {
            LOG(WARNING) << "GetMasterWorkerApi failed, error:" << status.ToString();
            continue;
        }
        auto request = std::make_unique<DeleteObjectReqPb>();
        for (const auto &info : objNeedDelete) {
            request->add_object_keys(info.first);
            request->add_versions(info.second);
        }
        request->set_is_async(false);
        DeleteApiInfo info;
        info.objs = { request->object_keys().begin(), request->object_keys().end() };
        info.workerAddr = workerId;
        int64_t tag;
        LOG(INFO) << FormatString("Send delete notify to: %s, objects[%s]", workerId,
                                  VectorToString(request->object_keys()));
        status = masterWorkerApi->DeleteNotificationSend(std::move(request), tag);
        if (status.IsOk()) {
            info.apiTag = tag;
            api2Tag.emplace(masterWorkerApi, info);
        } else {
            LOG(ERROR) << "DeleteNotificationSend failed: status: " << status.ToString();
        }
    }
    for (const auto &kv : api2Tag) {
        DeleteObjectRspPb response;
        Status status = kv.first->DeleteNotificationReceive(kv.second.apiTag, response);
        if (status.IsOk()) {
            RemoveAsyncWorkerOp(kv.second.workerAddr, kv.second.objs, NotifyWorkerOpType::DELETE);
        } else {
            LOG(ERROR) << "DeleteNotificationReceive failed : status: " << status.ToString();
        }
    }
    return Status::OK();
}

Status OCNotifyWorkerManager::SendCacheInvalidToWorker(const std::string &workerId,
                                                       std::unordered_set<std::string> &objectKeys)
{
    UpdateObjectReqPb req;
    UpdateObjectRspPb rsp;
    INJECT_POINT("Asyncsend.cacheinvalid");
    std::unordered_map<std::string, uint64_t> objectVersions;
    for (const auto &objectKey : objectKeys) {
        UpdateObjectInfoPb info;
        if (FillUpdateObjectInfoPb(objectKey, &info).IsOk() && info.address() != workerId) {
            req.mutable_object_infos()->Add(std::move(info));
            objectVersions.emplace(objectKey, info.version());
        } else {
            RETURN_IF_NOT_OK(RemoveAsyncWorkerOp(workerId, { objectKey }, NotifyWorkerOpType::CACHE_INVALID));
        }
    }
    if (req.object_infos().empty()) {
        LOG(INFO) << "Cache invalid no need to notify to " << workerId;
        return Status::OK();
    }
    std::shared_ptr<MasterWorkerOCApi> masterWorkerOcApi;
    RETURN_IF_NOT_OK(GetMasterWorkerApi(workerId, masterWorkerOcApi));
    CHECK_FAIL_RETURN_STATUS(masterWorkerOcApi != nullptr, K_RUNTIME_ERROR,
                             "Send cache invalidation failed, masterworkerocapi is null");
    INJECT_POINT("master.send_cache_invalid.before_notify");
    RETURN_IF_NOT_OK(masterWorkerOcApi->UpdateNotification(req, rsp));
    for (const auto &objectKey : rsp.failed_ids()) {
        (void)objectVersions.erase(objectKey);
    }
    INJECT_POINT("master.send_cache_invalid.before_remove_location");
    std::vector<std::string> removeIds;
    std::transform(objectVersions.begin(), objectVersions.end(), std::back_inserter(removeIds),
                   [](const auto &kv) { return kv.first; });
    RETURN_IF_NOT_OK(RemoveAsyncWorkerOp(workerId, removeIds, NotifyWorkerOpType::CACHE_INVALID));
    return ClearAddressCacheInvalid(workerId, objectVersions);
}

void OCNotifyWorkerManager::RecoverCacheInvalidAndRemoveMeta2EtcdKeyMap(
    std::vector<std::pair<std::string, std::string>> &cacheInvalids)
{
    // The key is WorkerAddr_ObjectKey, so the number of parsed strings is 2.
    const int validSize = 2;

    for (auto &info : cacheInvalids) {
        std::vector<std::string> keyVec;
        std::string::size_type pos = info.first.find("_", 0);
        if (pos != info.first.npos) {
            keyVec.push_back(info.first.substr(0, pos));
            keyVec.push_back(info.first.substr(pos + 1, info.first.npos));
        }

        if (keyVec.size() != validSize) {
            continue;
        }

        auto op = ParseNotifyWorkerOpFromL2Cache(info.second);
        if (!TESTFLAG(op.type, NotifyWorkerOpType::CACHE_INVALID) && !TESTFLAG(op.type, NotifyWorkerOpType::REMOVE_META)
            && !TESTFLAG(op.type, NotifyWorkerOpType::DELETE)) {
            continue;
        }

        if (TESTFLAG(op.type, NotifyWorkerOpType::CACHE_INVALID)) {
            LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                      static_cast<uint32_t>(op.type), keyVec[1], keyVec[0]);

            (void)InsertAsyncWorkerOp(keyVec[0], keyVec[1], { NotifyWorkerOpType::CACHE_INVALID }, false);
            ObjectMetaStore::WriteType type;
            if (ocMetadataManager_->GetObjectMetaType(keyVec[1], type)) {
                uint32_t hash;
                std::string table;
                auto key = keyVec[0] + "_" + keyVec[1];
                objectStore_->GetHashAndTable(keyVec[1], ETCD_ASYNC_WORKER_OP_TABLE_PREFIX, hash, table);
                objectStore_->InsertToEtcdKeyMap(table, key, hash, type);
            }
        }

        if (TESTFLAG(op.type, NotifyWorkerOpType::REMOVE_META)) {
            LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                      static_cast<uint32_t>(op.type), keyVec[1], keyVec[0]);

            (void)InsertAsyncWorkerOp(keyVec[0], keyVec[1], { NotifyWorkerOpType::REMOVE_META, op.removeMetaVersion },
                                      false);
            ObjectMetaStore::WriteType type;
            if (ocMetadataManager_->GetObjectMetaType(keyVec[1], type)) {
                uint32_t hash;
                std::string table;
                auto key = keyVec[0] + "_" + keyVec[1];
                objectStore_->GetHashAndTable(keyVec[1], ETCD_ASYNC_WORKER_OP_TABLE_PREFIX, hash, table);
                objectStore_->InsertToEtcdKeyMap(table, key, hash, type);
            }
        }
        if (TESTFLAG(op.type, NotifyWorkerOpType::DELETE)) {
            LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                      static_cast<uint32_t>(op.type), keyVec[1], keyVec[0]);
            (void)InsertAsyncWorkerOp(keyVec[0], keyVec[1], { NotifyWorkerOpType::DELETE, op.delObjectVersion }, false);
            uint32_t hash;
            std::string table;
            auto key = keyVec[0] + "_" + keyVec[1];
            objectStore_->GetHashAndTable(keyVec[1], ETCD_ASYNC_WORKER_OP_TABLE_PREFIX, hash, table);
            objectStore_->InsertToEtcdKeyMap(table, key, hash, true);
        }
    }
}

Status OCNotifyWorkerManager::RecoverCacheInvalidAndRemoveMeta(bool isFromRocksdb, const worker::HashRange &extraRanges)
{
    std::vector<std::pair<std::string, std::string>> cacheInvalids;
    if (!objectStore_->IsRocksdbRunning()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetFromEtcd(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX,
                                                                   ASYNC_WORKER_OP_TABLE, extraRanges, cacheInvalids),
                                         "Load meta from etcd into memory failed.");
        for (const auto &iter : cacheInvalids) {
            RETURN_IF_NOT_OK(objectStore_->PutToRocksStore(ASYNC_WORKER_OP_TABLE, iter.first, iter.second));
        }
    } else {
        if (isFromRocksdb && objectStore_->IsRocksdbEnableWriteMeta()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetAllFromRocks(ASYNC_WORKER_OP_TABLE, cacheInvalids),
                                             "Load meta from rocksdb into memory failed.");
        } else {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                objectStore_->GetFromEtcd(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX, ASYNC_WORKER_OP_TABLE, extraRanges,
                                          cacheInvalids),
                "Load meta from etcd into memory failed.");
        }
    }

    RecoverCacheInvalidAndRemoveMeta2EtcdKeyMap(cacheInvalids);

    return Status::OK();
}

Status OCNotifyWorkerManager::ClearAsyncWorkerOp(const std::string &workerAddr)
{
    TbbNotifyWorkerOpTable::accessor accessor;
    std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
    RETURN_OK_IF_TRUE(!notifyWorkerOpTable_.find(accessor, workerAddr));
    for (auto iter = accessor->second.begin(); iter != accessor->second.end();) {
        auto beforeModify = iter->second.type;
        iter->second.type =
            static_cast<NotifyWorkerOpType>(ClearUint32EvenBits(static_cast<uint32_t>(iter->second.type)));
        if (static_cast<uint32_t>(iter->second.type) == 0) {
            auto objKey = iter->first;
            iter = accessor->second.erase(iter);
            // DFX
            LOG_IF_ERROR(objectStore_->RemoveAsyncWorkerOp(workerAddr, objKey),
                         "remove async worker op in l2 cacahe failed, key: " + objKey);
            continue;
        }
        if (beforeModify != iter->second.type) {
            auto writeType = GetWriteType(iter->first);
            // DFX
            LOG_IF_ERROR(objectStore_->AddAsyncWorkerOp(workerAddr, iter->first, iter->second, writeType),
                         "modify async worker op in l2 cacahe failed, key: " + iter->first);
        }
        iter++;
    }
    return Status::OK();
}

bool OCNotifyWorkerManager::CheckExistAsyncWorkerOp(const std::string &workerId, const std::string &objectKey,
                                                    NotifyWorkerOpType op)
{
    std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
    TbbNotifyWorkerOpTable::const_accessor accessor;
    if (notifyWorkerOpTable_.find(accessor, workerId)) {
        auto iter = accessor->second.find(objectKey);
        if (iter != accessor->second.end() && TESTANYFLAG(iter->second.type, op)) {
            return true;
        }
    }
    return false;
}

std::vector<std::pair<std::string, NotifyWorkerOp>> OCNotifyWorkerManager::GetObjectAsyncWorkerOp(
    const std::string &objectKey)
{
    std::vector<std::pair<std::string, NotifyWorkerOp>> result;
    std::lock_guard<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
    for (const auto &cache : notifyWorkerOpTable_) {
        auto iter = cache.second.find(objectKey);
        if (iter != cache.second.end()) {
            result.emplace_back(std::make_pair(cache.first, iter->second));
        }
    }
    return result;
}

Status OCNotifyWorkerManager::SyncSendUpdateObject(const std::string &objectKey, uint64_t timestamp,
                                                   const std::string &sourceWorker, ObjectMeta &objectMeta,
                                                   ObjectLifeState lifeState, const std::vector<std::string> &fields)
{
    for (auto iter = objectMeta.locations.begin(); iter != objectMeta.locations.end();) {
        const std::string &address = iter->first;
        if (address == sourceWorker) {
            ++iter;
            continue;
        }
        LOG(INFO) << FormatString("Sync notify update object to workers: %s, source: %s, objectKey: %s", address,
                                  sourceWorker, objectKey);
        if (CheckWorkerIsHealthy(address).IsError()) {
            if (objectMeta.IsBinary()) {
                // If the worker is faulty, the message is placed in the asynchronous queue
                // and sent when the worker recovers.
                RETURN_IF_NOT_OK(AsyncSendUpdateObject(objectKey, sourceWorker, objectMeta));
            }
            ++iter;
            continue;
        }
        std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
        RETURN_IF_NOT_OK(GetMasterWorkerApi(address, masterWorkerApi));

        UpdateObjectReqPb request;
        UpdateObjectInfoPb *req = request.add_object_infos();
        request.set_sync(true);
        UpdateObjectRspPb response;
        req->set_address(sourceWorker);
        req->set_life_state(static_cast<uint32_t>(lifeState));
        req->set_object_key(objectKey);
        req->set_version(timestamp);
        *req->mutable_secondary_keys() = { fields.begin(), fields.end() };
        RETURN_IF_NOT_OK(masterWorkerApi->UpdateNotification(request, response));
        CHECK_FAIL_RETURN_STATUS(response.failed_ids().empty(), StatusCode::K_RUNTIME_ERROR, "worker update failed");
        (void)objectStore_->RemoveObjectLocation(objectKey, address);
        iter = objectMeta.locations.erase(iter);
    }
    LOG(INFO) << "Notify update object done.";
    return Status::OK();
}

Status OCNotifyWorkerManager::AsyncSendUpdateObject(const std::string &objectKey, const std::string &sourceWorker,
                                                    const ObjectMeta &objectMeta)
{
    RETURN_IF_NOT_OK(RemoveAsyncWorkerOp(sourceWorker, { objectKey }, NotifyWorkerOpType::CACHE_INVALID));
    for (const auto &address : objectMeta.locations) {
        if (address.first == sourceWorker) {
            continue;
        }
        LOG(INFO) << FormatString("Insert async worker operation(%d), workerId:%s",
                                  static_cast<uint32_t>(NotifyWorkerOpType::CACHE_INVALID), address.first);
        RETURN_IF_NOT_OK(
            InsertAsyncWorkerOp(address.first, objectKey, { NotifyWorkerOpType::CACHE_INVALID }, true,
                                OCMetadataManager::WriteMode2MetaType(objectMeta.meta.config().write_mode())));
    }
    return Status::OK();
}

Status OCNotifyWorkerManager::NotifySubscribeMeta(const std::string &objectKey, const ObjectMeta &objectMeta,
                                                  const std::string &subAddress, uint64_t &subTimeoutMs)
{
    LOG(INFO) << FormatString("Notify object meta to subscriber: %s, objectKey: %s", subAddress, objectKey);
    RETURN_IF_NOT_OK(CheckWorkerIsHealthy(subAddress));
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    RETURN_IF_NOT_OK(GetMasterWorkerApi(subAddress, masterWorkerApi));

    PublishMetaReqPb request;
    PublishMetaRspPb response;
    request.mutable_meta()->CopyFrom(objectMeta.meta);
    request.mutable_meta()->set_object_key(objectKey);
    request.set_address(objectMeta.meta.primary_address());
    request.set_timeout(subTimeoutMs);
    RETURN_IF_NOT_OK(masterWorkerApi->PublishMeta(request, response));
    LOG(INFO) << FormatString("Notify object meta %s done.", objectKey);
    return Status::OK();
}

Status OCNotifyWorkerManager::DoNotifyWorkerDelete(
    const std::string &sourceWorker,
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
    bool isAsync, std::unordered_set<std::string> &failedObjects)
{
    std::unordered_map<std::shared_ptr<MasterWorkerOCApi>, std::pair<int64_t, std::string>> api2Tag;
    Status lastErr = DoNotifyWorkerDeleteSendRequest(sourceWorker, replicas2Obj, isAsync, failedObjects, api2Tag);
    for (const auto &kv : api2Tag) {
        const auto &masterWorkerApi = kv.first;
        const auto &tag = kv.second.first;
        const auto &address = kv.second.second;
        auto successIds = replicas2Obj[address];
        DeleteObjectRspPb response;
        Status status = masterWorkerApi->DeleteNotificationReceive(tag, response);
        if (isAsync) {
            continue;
        }
        if (status.IsError()) {
            LOG(ERROR) << FormatString("DeleteNotificationReceive failed from worker %s, error: %s", address,
                                       status.ToString());
            lastErr = lastErr.GetCode() == K_WORKER_TIMEOUT ? lastErr : status;
            continue;
        }

        Status recvRc(static_cast<StatusCode>(response.last_rc().error_code()), response.last_rc().error_msg());
        if (recvRc.IsError()) {
            LOG(ERROR) << FormatString("DeleteNotificationReceive failed from worker %s, response %s, error: %s",
                                       address, LogHelper::IgnoreSensitive(response), recvRc.ToString());
            lastErr = lastErr.GetCode() == K_WORKER_TIMEOUT ? lastErr : recvRc;
            failedObjects.insert(response.failed_object_keys().begin(), response.failed_object_keys().end());
            for (const auto &id : response.failed_object_keys()) {
                successIds.erase(id);
            }
        }

        std::ostringstream oss;
        for (const auto &id : successIds) {
            oss << " {" << id.first << ": {" << id.second.first << ": " << id.second.second << "}}";
            (void)ocMetadataManager_->RemoveMetaLocation(id.first, address);
        }
        VLOG(1) << FormatString("Start to remove meta location for objects[%s]", oss.str());
        replicas2Obj.erase(address);
    }
    return lastErr;
}

Status OCNotifyWorkerManager::ClearDataWithoutMeta(const worker::HashRange &ranges, const std::string &workerAddr,
                                                   const std::vector<std::string> &objsMigrateFinished)
{
    RETURN_IF_NOT_OK(CheckWorkerIsHealthy(workerAddr));
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    RETURN_IF_NOT_OK(GetMasterWorkerApi(workerAddr, masterWorkerApi));
    ClearDataReqPb req;
    ClearDataRspPb rsp;
    req.set_standby_worker(masterAddr_.ToString());
    for (const auto &range : ranges) {
        auto *tempRange = req.add_ranges();
        tempRange->set_from(range.first);
        tempRange->set_end(range.second);
    }
    *req.mutable_objkeys_migrate_finished() = { objsMigrateFinished.begin(), objsMigrateFinished.end() };
    return masterWorkerApi->ClearData(req, rsp);
}

void OCNotifyWorkerManager::SetDeleteObjectReq(
    std::unique_ptr<DeleteObjectReqPb> &request, bool isAsync, const std::string &sourceWorker,
    const std::unordered_map<std::string, std::pair<int64_t, uint32_t>> &objectItem)
{
    for (const auto &item : objectItem) {
        request->add_object_keys(item.first);
        request->add_versions(item.second.first);
    }
    request->set_address(sourceWorker);
    request->set_is_async(isAsync);
    VLOG(1) << "Notify worker to delete the object " << LogHelper::IgnoreSensitive(*request);
}

Status OCNotifyWorkerManager::DoNotifyWorkerDeleteSendRequest(
    const std::string &sourceWorker,
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
    bool isAsync, std::unordered_set<std::string> &failedObjects,
    std::unordered_map<std::shared_ptr<MasterWorkerOCApi>, std::pair<int64_t, std::string>> &api2Tag)
{
    std::vector<std::tuple<std::string, std::string, uint32_t, uint64_t>> asyncNotifyIds;
    Timer timer;
    int64_t realTimeoutMs = timeoutDuration.CalcRealRemainingTime();
    std::string traceID = Trace::Instance().GetTraceID();
    std::vector<std::future<SendResult>> futures;
    futures.reserve(replicas2Obj.size());
    std::atomic<bool> needAbort{ false };
    for (const auto &item : replicas2Obj) {
        const auto &address = item.first;
        const auto &objectItem = item.second;
        if (objectItem.empty()) {
            continue;
        }
        if (address.empty()) {
            LOG(ERROR) << "DoNotifyWorkerDeleteSendRequest: The address is empty.";
            continue;
        }
        if (!HandleWorkerDisconnection(address, objectItem, asyncNotifyIds)) {
            continue;
        }
        if (needAbort.load()) {
            LOG(WARNING) << "Aborting remaining tasks due to timeout.";
            break;
        }
        futures.emplace_back(deleteThreadPool_->Submit([=, &needAbort, &timer]() -> SendResult {
            ScopedRequestContext ctx;
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            int64_t elapsed = static_cast<int64_t>(timer.ElapsedMilliSecond());
            if (elapsed >= realTimeoutMs) {
                LOG(ERROR) << "RPC timeout. time elapsed " << elapsed << ", realTimeoutMs:" << realTimeoutMs
                           << ", NotifyDeleteSend threads Statistics: " << deleteThreadPool_->GetStatistics();
                needAbort.store(true);
                return { nullptr, -1, address, Status(StatusCode::K_RUNTIME_ERROR, "Rpc timeout") };
            }
            timeoutDuration.Init(realTimeoutMs - elapsed);
            std::shared_ptr<MasterWorkerOCApi> api;
            Status st = GetMasterWorkerApi(address, api);
            int64_t tag = -1;
            if (st.IsOk()) {
                auto req = std::make_unique<DeleteObjectReqPb>();
                SetDeleteObjectReq(req, isAsync, sourceWorker, objectItem);
                st = api->DeleteNotificationSend(std::move(req), tag);
            }
            return { api, tag, address, st };
        }));
    }
    Status lastErr;
    SendResult res;
    for (auto &f : futures) {
        res = f.get();
        if (res.status.IsError()) {
            LOG(ERROR) << "Send delete to " << res.address << " failed: " << res.status.ToString();
            if (!isAsync) {
                lastErr = res.status;
            }
        } else {
            api2Tag.emplace(res.api, std::make_pair(res.tag, res.address));
        }
    }
    RETURN_IF_NOT_OK(AsyncNotifyWorkerDelete(asyncNotifyIds, replicas2Obj, failedObjects));
    return lastErr;
}

bool OCNotifyWorkerManager::HandleWorkerDisconnection(
    const std::string &address, const std::unordered_map<std::string, std::pair<int64_t, uint32_t>> &objectItem,
    std::vector<std::tuple<std::string, std::string, uint32_t, uint64_t>> &asyncNotifyIds)
{
    if (CheckWorkerIsHealthy(address).IsError()) {
        // If the worker is faulty, the message is placed in the asynchronous queue
        // and sent when the worker recovers.
        std::transform(objectItem.begin(), objectItem.end(), std::back_inserter(asyncNotifyIds),
                       [&address](const auto &entry) {
                           return std::make_tuple(entry.first, address, entry.second.second, entry.second.first);
                       });
        return false;
    }
    return true;
}

Status OCNotifyWorkerManager::SyncNotifyWorkerDelete(
    std::shared_ptr<MasterWorkerOCApi> &masterWorkerApi, const std::string &address,
    std::unique_ptr<DeleteObjectReqPb> &request,
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
    std::unordered_set<std::string> &failedObjects, Status &lastErr)
{
    DeleteObjectRspPb response;
    Status status = masterWorkerApi->DeleteNotification(std::move(request), response);
    if (status.IsOk()) {
        failedObjects.insert(response.failed_object_keys().begin(), response.failed_object_keys().end());

        Status recvRc(static_cast<StatusCode>(response.last_rc().error_code()), response.last_rc().error_msg());
        if (recvRc.IsError()) {
            LOG(ERROR) << FormatString("DeleteNotificationReceive failed from worker %s, response %s, error: %s",
                                       address, LogHelper::IgnoreSensitive(response), recvRc.ToString());
            lastErr = lastErr.GetCode() == K_WORKER_TIMEOUT ? lastErr : recvRc;
            failedObjects.insert(response.failed_object_keys().begin(), response.failed_object_keys().end());
        }

        // the upper business will retry DoNotifyWorkerDelete on rpc error, this operation result has store in
        // failedObjects, so the operation of the address can't repeat in retry loop.
        auto it = replicas2Obj.find(address);
        if (it != replicas2Obj.end()) {
            it->second.clear();
        }
    }
    return status;
}

Status OCNotifyWorkerManager::AsyncNotifyWorkerDelete(
    std::vector<std::tuple<std::string, std::string, uint32_t, uint64_t>> &asyncNotifyIds,
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> &replicas2Obj,
    std::unordered_set<std::string> &failedObjects)
{
    Status rc = Status::OK();
    for (const auto &item : asyncNotifyIds) {
        const auto &objectKey = std::get<0>(item);
        const auto &address = std::get<1>(item);
        const auto &writeMode = std::get<2>(item);
        const auto &objVersion = std::get<3>(item);
        LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                  static_cast<uint32_t>(NotifyWorkerOpType::DELETE), objectKey, address);
        NotifyWorkerOp op = { .type = NotifyWorkerOpType::DELETE };
        op.delObjectVersion = objVersion;
        Status status = InsertAsyncWorkerOp(address, objectKey, { NotifyWorkerOpType::DELETE }, true,
                                            OCMetadataManager::WriteMode2MetaType(writeMode));
        if (status.IsError()) {
            LOG(ERROR) << FormatString("InsertAsyncWorkerOp failed, address: %s, error: %s", address,
                                       status.ToString());
            rc = status;
            (void)failedObjects.emplace(objectKey);
        } else {
            // the upper business will retry DoNotifyWorkerDelete on rpc error, async operation not need repeat in
            // retry loop.
            (void)replicas2Obj[address].erase(objectKey);
        }
    }
    return rc;
}

Status OCNotifyWorkerManager::GetMasterWorkerApi(const std::string &workerAddr,
                                                 std::shared_ptr<MasterWorkerOCApi> &resultApi)
{
    HostPort hostPort;
    RETURN_IF_NOT_OK(hostPort.ParseString(workerAddr));
    auto masterWorkerApi =
        MasterWorkerOCApi::CreateMasterWorkerOCApi(hostPort, masterAddr_, akSkManager_, masterWorkerOCService_);
    RETURN_IF_NOT_OK(masterWorkerApi->Init());
    resultApi = masterWorkerApi;
    return Status::OK();
}

void OCNotifyWorkerManager::EraseMasterWorkerApi(HostPort &nodePort)
{
    auto rc = RpcStubCacheMgr::Instance().Remove(nodePort, StubType::MASTER_WORKER_OC_SVC);
    if (rc.IsOk() || rc.GetCode() == K_NOT_FOUND) {
        return;
    }
    LOG(ERROR) << FormatString("Erase master worker api[%s] failed: %s", nodePort.ToString(), rc.ToString());
}

Status OCNotifyWorkerManager::InsertAsyncWorkerOp(const std::string &workerId, const std::string &objectKey,
                                                  const NotifyWorkerOp &op, bool needPersist,
                                                  ObjectMetaStore::WriteType type)
{
    INJECT_POINT("OCNotifyWorkerManager.InsertAsyncWorkerOp.Fail");
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
    GetMasterTimeCost().Append("InsertAsyncWorkerOp get lock", timer.ElapsedMilliSecond());
    TbbNotifyWorkerOpTable::accessor accessor;
    NotifyWorkerOp opAfterModify = op;

    if (!notifyWorkerOpTable_.find(accessor, workerId)) {
        std::unordered_map<std::string, NotifyWorkerOp> objectKeys;
        (void)objectKeys.emplace(objectKey, op);
        (void)notifyWorkerOpTable_.emplace(accessor, workerId, objectKeys);
    }

    auto itr = accessor->second.find(objectKey);
    if (itr == accessor->second.end()) {
        (void)accessor->second.emplace(objectKey, op);
        return !needPersist ? Status::OK() : objectStore_->AddAsyncWorkerOp(workerId, objectKey, opAfterModify, type);
    }

    auto notifyMasterOp = ClearNotifyWorkerOp(op.type);
    auto notifyWorkerOp = ClearNotifyMasterOp(op.type);
    if (static_cast<uint32_t>(notifyMasterOp) != 0) {
        SETFLAG(itr->second.type, notifyMasterOp);
        if (TESTFLAG(op.type, NotifyWorkerOpType::REMOVE_META)) {
            itr->second.removeMetaVersion = op.removeMetaVersion;
            itr->second.removeMetaAzNames.insert(op.removeMetaAzNames.begin(), op.removeMetaAzNames.end());
        }
        if (TESTFLAG(op.type, NotifyWorkerOpType::DELETE_ALL_COPY_META)) {
            itr->second.deleteAllCopyMetaVersion = op.deleteAllCopyMetaVersion;
            itr->second.deleteAllCopyMetaAzNames.insert(op.deleteAllCopyMetaAzNames.begin(),
                                                        op.deleteAllCopyMetaAzNames.end());
        }
    }
    if (static_cast<uint32_t>(notifyWorkerOp) != 0) {
        auto currWorkerOp = ClearNotifyMasterOp(itr->second.type);
        if (static_cast<uint32_t>(currWorkerOp) >= static_cast<uint32_t>(op.type)) {
            VLOG(1) << FormatString(
                "The existing operation(%d) of the object is greater than or equal to the new operation(%d). "
                "objectKey:%s, workerId:%s",
                static_cast<uint32_t>(itr->second.type), static_cast<uint32_t>(op.type), objectKey, workerId);
        } else {
            CLEARFLAG(itr->second.type, currWorkerOp);
            SETFLAG(itr->second.type, op.type);
        }
    }
    opAfterModify = itr->second;
    return !needPersist ? Status::OK() : objectStore_->AddAsyncWorkerOp(workerId, objectKey, opAfterModify, type);
}

ObjectMetaStore::WriteType OCNotifyWorkerManager::GetWriteType(const std::string &objKey)
{
    auto writeMode = static_cast<WriteMode>(ocMetadataManager_->GetL2CacheType(objKey));
    ObjectMetaStore::WriteType writeType = ObjectMetaStore::WriteType::ROCKS_ONLY;
    switch (writeMode) {
        case WriteMode::WRITE_THROUGH_L2_CACHE:
            writeType = ObjectMetaStore::WriteType::ROCKS_SYNC_ETCD;
            break;
        case WriteMode::WRITE_BACK_L2_CACHE:
        case WriteMode::WRITE_BACK_L2_CACHE_EVICT:
            writeType = ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD;
            break;
        case WriteMode::NONE_L2_CACHE:
        case WriteMode::NONE_L2_CACHE_EVICT:
        default:
            writeType = ObjectMetaStore::WriteType::ROCKS_ONLY;
            break;
    }
    return writeType;
}

Status OCNotifyWorkerManager::RemoveAsyncWorkerOp(const std::string &workerId,
                                                  const std::vector<std::string> &objectKeys, NotifyWorkerOpType op,
                                                  bool isDataMigration)
{
    std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
    TbbNotifyWorkerOpTable::accessor accessor;
    RETURN_OK_IF_TRUE(!notifyWorkerOpTable_.find(accessor, workerId));
    for (const auto &id : objectKeys) {
        auto itr = accessor->second.find(id);
        if (itr == accessor->second.end()) {
            continue;
        }
        auto beforeModify = itr->second.type;
        if (static_cast<uint32_t>(CLEARFLAG(itr->second.type, op)) == 0) {
            (void)accessor->second.erase(id);
            LOG_IF_ERROR(objectStore_->RemoveAsyncWorkerOp(workerId, id, !isDataMigration),
                         "remove async worker op in l2 cacahe failed, key: " + id);
            continue;
        }
        if (beforeModify != itr->second.type) {
            auto writeType = isDataMigration ? ObjectMetaStore::WriteType::ROCKS_ONLY : GetWriteType(itr->first);
            // DFX
            LOG_IF_ERROR(objectStore_->AddAsyncWorkerOp(workerId, itr->first, itr->second, writeType),
                         "modify async worker op in l2 cacahe failed, key: " + itr->first);
        }
    }
    return Status::OK();
}

Status OCNotifyWorkerManager::ClearAddressCacheInvalid(const std::string &workerId,
                                                       const std::unordered_map<std::string, uint64_t> &objectVersions)
{
    LOG(INFO) << FormatString("[ObjectKeys %s] Start to remove meta location %s", MapToString(objectVersions),
                              workerId);
    for (const auto &kv : objectVersions) {
        RETURN_IF_NOT_OK(ocMetadataManager_->RemoveMetaLocation(kv.first, workerId, kv.second));
    }
    return Status::OK();
}

Status OCNotifyWorkerManager::FillUpdateObjectInfoPb(const std::string &objectKey, UpdateObjectInfoPb *objectInfoPb)
{
    auto &shard = ocMetadataManager_->GetShardFor(objectKey);
    std::shared_lock<std::shared_timed_mutex> lck(shard.mutex);
    TbbMetaTable ::const_accessor accessor;
    if (!shard.table.find(accessor, objectKey)) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "FillUpdateObjectInfoPb failed. objectKey:" + objectKey);
    }
    objectInfoPb->set_object_key(objectKey);
    objectInfoPb->set_address(accessor->second.meta.primary_address());
    objectInfoPb->set_life_state(accessor->second.meta.life_state());
    objectInfoPb->set_version(accessor->second.meta.version());
    return Status::OK();
}

void OCNotifyWorkerManager::SetFaultWorker(const std::string &workerAddr)
{
    LOG(INFO) << "add fault worker: " << workerAddr;
    std::lock_guard<std::shared_timed_mutex> lck(faultWorkerMutex_);
    (void)faultWorkers_.emplace(workerAddr);
}

void OCNotifyWorkerManager::RemoveFaultWorker(const std::string &workerAddr)
{
    Timer timer;
    std::lock_guard<std::shared_timed_mutex> lck(faultWorkerMutex_);
    LOG(INFO) << "remove fault worker: " << workerAddr;
    GetMasterTimeCost().Append("RemoveFaultWorker get lock", timer.ElapsedMilliSecond());
    (void)faultWorkers_.erase(workerAddr);
}

Status OCNotifyWorkerManager::CheckWorkerIsHealthy(const std::string &workerAddr)
{
    INJECT_POINT("OCNotifyWorkerManager.CheckWorkerIsHealth.worker.unhealthy");
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(faultWorkerMutex_);
    GetMasterTimeCost().Append("CheckWorkerIsHealthy get lock", timer.ElapsedMilliSecond());
    if (faultWorkers_.find(workerAddr) != faultWorkers_.end()) {
        RETURN_STATUS(K_WORKER_ABNORMAL, "The worker status is abnormal. workerAddr:" + workerAddr);
    }
    return Status::OK();
}

void OCNotifyWorkerManager::AsyncChangePrimaryCopy(
    const std::unordered_map<std::string, std::unordered_set<std::string>> &toBeChanged, bool ifvoluntaryScaleDown)
{
    ocMetadataManager_->ExecuteAsyncTask(&OCNotifyWorkerManager::ProcessChangePrimaryCopy, this, toBeChanged,
                                         ifvoluntaryScaleDown);
}

Status OCNotifyWorkerManager::SendChangePrimaryCopy(const std::string &workerAddr,
                                                    const std::unordered_set<std::string> &objectKeys,
                                                    std::unordered_set<std::string> &successIds)
{
    RETURN_IF_NOT_OK(CheckWorkerIsHealthy(workerAddr));
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    RETURN_IF_NOT_OK(GetMasterWorkerApi(workerAddr, masterWorkerApi));
    ChangePrimaryCopyReqPb req;
    ChangePrimaryCopyRspPb rsp;
    *req.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterWorkerApi->ChangePrimaryCopy(req, rsp),
                                     "Send ChangePrimaryCopy failed to worker " + workerAddr);
    successIds = { rsp.success_ids().begin(), rsp.success_ids().end() };
    return Status::OK();
}

void OCNotifyWorkerManager::ProcessChangePrimaryCopy(
    const std::unordered_map<std::string, std::unordered_set<std::string>> &input, bool ifvoluntaryScaleDown)
{
    constexpr int64_t maxRpcTimeoutMs = 3 * 1000;            // Each ChangePrimaryCopy RPC timeout is 3 seconds.
    constexpr int64_t maxProcessTimeoutMs = 30 * 60 * 1000;  // Stop retrying after 30 minutes.
    Timer processTimer;
    Raii resetDuration([]() { timeoutDuration.Reset(); });
    std::unordered_map<std::string, std::unordered_set<std::string>> toBeChanged = input;
    // Key is object key, value is the set of excluded workers.
    std::unordered_map<std::string, std::unordered_set<std::string>> needReselectPrimary;
    do {
        if (processTimer.ElapsedMilliSecond() > maxProcessTimeoutMs) {
            LOG(WARNING) << "ProcessChangePrimaryCopy timeout after " << processTimer.ElapsedMilliSecond()
                         << " ms, stop retrying. Pending worker count: " << toBeChanged.size()
                         << ", pending reselect object count: " << needReselectPrimary.size();
            return;
        }
        for (auto &it : toBeChanged) {
            if (it.second.empty()) {
                continue;
            }
            if (IsTermSignalReceived() && !ifvoluntaryScaleDown) {
                LOG(INFO) << "ProcessChangePrimaryCopy finish, current worker recieved term signal";
                return;
            }
            timeoutDuration.Init(maxRpcTimeoutMs);
            std::unordered_set<std::string> successIds;
            (void)SendChangePrimaryCopy(it.first, it.second, successIds);
            for (const auto &id : it.second) {
                if (successIds.find(id) == successIds.end()) {
                    (void)needReselectPrimary[id].emplace(it.first);
                } else {
                    (void)needReselectPrimary.erase(id);
                    ocMetadataManager_->ModifyPrimaryCopy(id, it.first, ifvoluntaryScaleDown);
                }
            }
        }
        toBeChanged.clear();
        for (auto &it : needReselectPrimary) {
            std::string newPrimaryCopy;
            auto &shard = ocMetadataManager_->GetShardFor(it.first);
            std::shared_lock<std::shared_timed_mutex> lck(shard.mutex);
            TbbMetaTable::accessor accessor;
            if (ocMetadataManager_->ReselectPrimaryCopy(it.first, it.second, accessor, newPrimaryCopy).IsOk()) {
                (void)toBeChanged[newPrimaryCopy].emplace(it.first);
            }
        }
    } while (!toBeChanged.empty());
    LOG(INFO) << "ProcessChangePrimaryCopy finish";
}

void OCNotifyWorkerManager::AsyncPushMetaToWorker(const std::string &workerAddr, int64_t timestamp, bool isRestart)
{
    auto traceID = Trace::Instance().GetTraceID();
    auto func = [this, workerAddr, timestamp, isRestart, traceID]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        PushMetaToWorker(workerAddr, timestamp, isRestart);
    };
    ocMetadataManager_->ExecuteAsyncTask(std::move(func));
}

void OCNotifyWorkerManager::AsyncDecNestedRefs(const std::string &workerAddr,
                                               const std::vector<std::string> &objectKeys)
{
    ocMetadataManager_->ExecuteAsyncTask(&OCNotifyWorkerManager::DecNestedRefs, this, workerAddr, objectKeys);
}

Status OCNotifyWorkerManager::IncNestedRefs(const std::string &workerAddr, const std::vector<std::string> &objectKeys)
{
    LOG(INFO) << "IncNestedRefs start. workerAddr:" << workerAddr;
    RETURN_IF_NOT_OK(CheckWorkerIsHealthy(workerAddr));

    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    RETURN_IF_NOT_OK_APPEND_MSG(
        GetMasterWorkerApi(workerAddr, masterWorkerApi),
        FormatString("Get MasterWorkerOCApi failed is abnormal during IncNestedRefs. workerAddr: %s", workerAddr));

    NotifyMasterIncNestedReqPb req;
    NotifyMasterIncNestedResPb rsp;
    *req.mutable_nested_object_keys() = { objectKeys.begin(), objectKeys.end() };

    Status rc = RetryOnRPCError(
        [&masterWorkerApi, &req, &rsp]() { return masterWorkerApi->NotifyMasterIncNestedRefs(req, rsp); });
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("IncNestedRefs failed. workerAddr: %s, status: %s", workerAddr, rc.ToString());
        return rc;
    }
    LOG(INFO) << "IncNestedRefs end. workerAddr:" << workerAddr;
    return Status::OK();
}

void OCNotifyWorkerManager::DecNestedRefs(const std::string &workerAddr, const std::vector<std::string> &objectKeys)
{
    LOG(INFO) << "DecNestedRefs begin, workerAddr:" << workerAddr;
    Status rc = CheckWorkerIsHealthy(workerAddr);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("The worker status is abnormal during DecNestedRefs. workerAddr: %s, status: %s",
                                     workerAddr, rc.ToString());
        return;
    }
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    rc = GetMasterWorkerApi(workerAddr, masterWorkerApi);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString(
            "Get MasterWorkerOCApi failed is abnormal during DecNestedRefs. workerAddr:%s, status:%s", workerAddr,
            rc.ToString());
        return;
    }

    static const uint64_t batchSize = 10000;
    auto objSize = objectKeys.size();
    uint64_t batchCount = (objSize + batchSize - 1) / batchSize;

    for (uint64_t i = 0; i < batchCount; i++) {
        // Asynchronous call, shared by current thread using timeoutDuration.
        timeoutDuration.Init(RPC_TIMEOUT);
        NotifyMasterDecNestedReqPb req;
        NotifyMasterDecNestedResPb rsp;
        uint64_t start = i * batchSize;
        uint64_t end = (i == batchCount - 1) ? objSize : start + batchSize;
        *req.mutable_nested_object_keys() = { objectKeys.begin() + start, objectKeys.begin() + end };
        rc = RetryOnRPCError(
            [&masterWorkerApi, &req, &rsp]() { return masterWorkerApi->NotifyMasterDecNestedRefs(req, rsp); });
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("DecNestedRefs failed. workerAddr: %s, status:%s", workerAddr, rc.ToString());
            return;
        }
    }

    LOG(INFO) << "DecNestedRefs end. workerAddr:" << workerAddr;
}

Status OCNotifyWorkerManager::RequestMetaFromWorker(const std::string &masterAddr, const std::string &workerAddr,
                                                    RequestMetaFromWorkerRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckWorkerIsHealthy(workerAddr),
                                     "Worker is offline to make the rpc call RequestMetaFromWorker");
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetMasterWorkerApi(workerAddr, masterWorkerApi),
                                     "Could not get MasterWorkerOCApi for the given worker address");
    RequestMetaFromWorkerReqPb req;
    req.set_address(masterAddr);
    Status rc =
        RetryOnRPCError([&masterWorkerApi, &req, &rsp]() { return masterWorkerApi->RequestMetaFromWorker(req, rsp); });
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("RequestMetaFromWorker failed. workerAddr:%s, status:%s", workerAddr, rc.ToString());
        return rc;
    }
    LOG(INFO) << "RequestMetaFromWorker success. workerAddr:" << workerAddr;
    return Status::OK();
}

void OCNotifyWorkerManager::PushMetaToWorker(const std::string &workerAddr, int64_t timestamp, bool isRestart)
{
    LOG(INFO) << "PushMetaToWorker start. From master: " << masterAddr_.ToString() << " to worker:" << workerAddr;
    std::vector<std::string> refIds;
    ocMetadataManager_->globalRefTable_->GetClientRefIds(workerAddr, refIds);
    PushMetaToWorkerReqPb req;
    PushMetaToWorkerRspPb rsp;
    req.set_is_restart(isRestart);
    req.set_source_address(masterAddr_.ToString());
    req.set_event_timestamp(timestamp);
    *req.mutable_gref_object_keys() = { refIds.begin(), refIds.end() };
    Status rc = CheckWorkerIsHealthy(workerAddr);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("The worker status is abnormal during PushMetaToWorker. workerAddr:%s, status:%s",
                                     workerAddr, rc.ToString());
        return;
    }
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    rc = GetMasterWorkerApi(workerAddr, masterWorkerApi);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString(
            "Get MasterWorkerOCApi failed is abnormal during PushMetaToWorker. workerAddr:%s, status:%s", workerAddr,
            rc.ToString());
        return;
    }

    static const int RETRY_TIMEOUT_MS = 60000;  // 1 min
    const std::unordered_set<StatusCode> &retryOn = { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED,
                                                      StatusCode::K_RPC_DEADLINE_EXCEEDED,
                                                      StatusCode::K_RPC_UNAVAILABLE };
    rc = RetryOnError(
        RETRY_TIMEOUT_MS,
        [&masterWorkerApi, &req, &rsp](int32_t) { return masterWorkerApi->PushMetaToWorker(req, rsp); },
        []() { return Status::OK(); }, retryOn);
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("PushMetaToWorker failed. workerAddr:%s, status:%s", workerAddr, rc.ToString());
        return;
    }
    LOG(INFO) << "PushMetaToWorker end. workerAddr:" << workerAddr;
}

void OCNotifyWorkerManager::AsyncNotifyOpToWorker(const std::string &workerAddr, int64_t timestamp)
{
    ocMetadataManager_->ExecuteAsyncTask(&OCNotifyWorkerManager::NotifyOpToWorker, this, workerAddr, timestamp);
}

void OCNotifyWorkerManager::NotifyOpToWorker(const std::string &workerAddr, int64_t timestamp)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    LOG(INFO) << "NotifyOpToWorker start. workerAddr:" << workerAddr;
    Status rc = CheckWorkerIsHealthy(workerAddr);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("The worker status is abnormal during NotifyOpToWorker. workerAddr:%s, status:%s",
                                     workerAddr, rc.ToString());
        return;
    }
    std::shared_ptr<MasterWorkerOCApi> masterWorkerApi;
    rc = GetMasterWorkerApi(workerAddr, masterWorkerApi);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString(
            "Get MasterWorkerOCApi failed is abnormal during NotifyOpToWorker. workerAddr:%s, status:%s", workerAddr,
            rc.ToString());
        return;
    }

    {
        PushMetaToWorkerReqPb req;
        PushMetaToWorkerRspPb rsp;
        req.set_is_restart(false);
        req.set_event_timestamp(timestamp);
        std::shared_lock<std::shared_timed_mutex> lck(notifyWorkerOpMutex_);
        TbbNotifyWorkerOpTable::accessor accessor;
        if (!notifyWorkerOpTable_.find(accessor, workerAddr)) {
            return;
        }
        for (const auto &it : accessor->second) {
            if (TESTFLAG(it.second.type, NotifyWorkerOpType::CACHE_INVALID)) {
                (void)FillUpdateObjectInfoPb(it.first, req.add_cache_invalids());
            } else if (TESTFLAG(it.second.type, NotifyWorkerOpType::DELETE)) {
                req.add_delete_object_keys(it.first);
            } else if (TESTFLAG(it.second.type, NotifyWorkerOpType::PRIMARY_COPY_INVALID)) {
                req.add_primary_copy_invalid_ids(it.first);
            }
            // There is no need to process the request to notify the master here.
        }

        rc = RetryOnRPCError([&masterWorkerApi, &req, &rsp]() { return masterWorkerApi->PushMetaToWorker(req, rsp); });
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("PushMetaToWorker failed. workerAddr:%s, status:%s", workerAddr, rc.ToString());
            return;
        }
        LOG(INFO) << "PushMetaToWorker end. workerAddr:" << workerAddr;
    }
    ClearAsyncWorkerOp(workerAddr);
}

void OCNotifyWorkerManager::AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *service,
                                              const HostPort &masterAddr)
{
    masterWorkerOCService_ = service;
    masterAddr_ = masterAddr;
}

NotifyWorkerOp OCNotifyWorkerManager::ParseNotifyWorkerOpFromMigration(const ObjectAsyncOpDetailPb &pb)
{
    NotifyWorkerOp op;
    op.type = static_cast<NotifyWorkerOpType>(pb.op_type());
    if (TESTFLAG(op.type, NotifyWorkerOpType::REMOVE_META)) {
        op.removeMetaVersion = static_cast<int64_t>(pb.remove_meta_version());
        op.removeMetaAzNames.insert(pb.remove_meta_az_names().begin(), pb.remove_meta_az_names().end());
    }
    if (TESTFLAG(op.type, NotifyWorkerOpType::DELETE_ALL_COPY_META)) {
        op.deleteAllCopyMetaVersion = static_cast<int64_t>(pb.delete_all_copy_version());
        op.deleteAllCopyMetaAzNames.insert(pb.delete_all_copy_az_names().begin(), pb.delete_all_copy_az_names().end());
    }
    return op;
}

NotifyWorkerOp OCNotifyWorkerManager::ParseNotifyWorkerOpFromL2Cache(const std::string &serializedStr)
{
    NotifyWorkerOp op;
    ObjectAsyncOpDetailPb pb;
    pb.ParseFromString(serializedStr);
    return ParseNotifyWorkerOpFromMigration(pb);
}

}  // namespace master
}  // namespace datasystem
