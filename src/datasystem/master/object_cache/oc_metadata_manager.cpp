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
 * Description: Module responsible for managing the object cache metadata on the master.
 */
#include "datasystem/master/object_cache/oc_metadata_manager.h"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rdma/urma_manager_wrapper.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/id_tool.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/master/object_cache/oc_notify_worker_manager.h"
#include "datasystem/master/object_cache/store/meta_async_queue.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/master_worker_oc_service_impl.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"

DS_DEFINE_string(rocksdb_store_dir, "~/datasystem/rocksdb",
                 "The path of persistent gcs meta data and must "
                 "specify in rocksdb scenario. The rocksdb database is used to persistently store the metadata "
                 "in the master, so that the metadata before the restart can be re-obtained when the master restarts.");
DS_DEFINE_validator(rocksdb_store_dir, &Validator::ValidatePathString);
DS_DEFINE_bool(enable_redirect, "true",
               "enable query meta redirect when scale up or voluntary scale down, default is false");

DS_DECLARE_string(etcd_address);
DS_DECLARE_bool(async_delete);
DS_DECLARE_int32(rpc_thread_num);
DS_DECLARE_bool(enable_meta_replica);
DS_DECLARE_bool(oc_io_from_l2cache_need_metadata);
DS_DECLARE_bool(enable_reconciliation);
DS_DECLARE_string(other_cluster_names);
DS_DECLARE_string(rocksdb_write_mode);

namespace datasystem {
namespace master {
static constexpr int DEBUG_LOG_LEVEL = 1;
static constexpr int MIN_TTL_SECOND = 0;
static constexpr int ASYNC_MIN_THREAD_NUM = 2;
static constexpr int ASYNC_MAX_THREAD_NUM = 5;
static const std::string OC_METADATA_MANAGER = "OCMetadataManager-";
static constexpr int MSET_PENDING_TTL_US = 60'000'000;  // 60s

OCMetadataManager::OCMetadataManager(std::shared_ptr<AkSkManager> akSkManager, RocksStore *rocksStore,
                                     EtcdStore *etcdStore, std::shared_ptr<PersistenceApi> persistApi,
                                     const std::string &masterAddress, EtcdClusterManager *cm,
                                     const std::string &dbName, bool newNode)
    : MetadataRedirectHelper(cm),
      masterAddress_(masterAddress),
      akSkManager_(std::move(akSkManager)),
      eventName_(OC_METADATA_MANAGER + dbName),
      persistApi_(persistApi),
      newNode_(newNode)
{
    bool isEnabled = FLAGS_rocksdb_write_mode != "none" || FLAGS_oc_io_from_l2cache_need_metadata;
    if (FLAGS_enable_meta_replica && !etcdCM_->IsCentralized()) {
        objectStore_ = std::make_shared<ObjectMetaStore>(rocksStore, nullptr, isEnabled);
    } else {
        objectStore_ = std::make_shared<ObjectMetaStore>(rocksStore, etcdStore, isEnabled);
    }
    if (etcdCM_ != nullptr && !etcdCM_->IsCentralized()) {
        dbName_ = dbName;
    }
}
Status OCMetadataManager::Init()
{
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, K_RUNTIME_ERROR, "EtcdClusterManager must be inited firstly");
    objectStore_->SetClusterManager(etcdCM_);
    RETURN_IF_NOT_OK(objectStore_->Init());
    if (newNode_) {
        RETURN_IF_NOT_OK(objectStore_->AddRocksdbHealthTag());
    }
    // let it skips recovery from etcd
    bool skipRecoveryFromEtcd = objectStore_->CheckHealth();

    nestedRefManager_ = std::make_unique<OCNestedManager>(objectStore_, etcdCM_);
    RETURN_IF_NOT_OK(InitGlobalRef());
    asyncPool_ = std::make_unique<ThreadPool>(ASYNC_MIN_THREAD_NUM, ASYNC_MAX_THREAD_NUM, "OcAsyncTask");
    notifyWorkerManager_ =
        std::make_unique<OCNotifyWorkerManager>(objectStore_, skipRecoveryFromEtcd, akSkManager_, etcdCM_, this);
    RETURN_IF_NOT_OK(notifyWorkerManager_->Init());
    globalCacheDeleteManager_ =
        std::make_unique<OCGlobalCacheDeleteManager>(objectStore_, persistApi_, skipRecoveryFromEtcd);
    RETURN_IF_NOT_OK(globalCacheDeleteManager_->Init());
    expiredObjectManager_ = std::make_unique<ExpiredObjectManager>(masterAddress_, this);
    expiredObjectManager_->Init();
    RETURN_IF_NOT_OK(LoadMeta(skipRecoveryFromEtcd));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(notifyWorkerManager_->RecoverCacheInvalidAndRemoveMeta(true),
                                     "Recover cache invalid for rocksdb failed.");

    if (!objectStore_->IsRocksdbRunning()) {
        RETURN_IF_NOT_OK(objectStore_->AddRocksdbHealthTag());
    }
    asyncTaskPool_ = std::make_unique<ThreadPool>(FLAGS_rpc_thread_num, FLAGS_rpc_thread_num, "AsyncTaskPool");
    InitSubscribeEvent();
    StartMetaMonitor();
    masterDevOcManager_ = std::make_shared<MasterDevOcManager>();
    masterDevOcManager_->Init();
    return Status::OK();
}

void OCMetadataManager::StartMetaMonitor()
{
    monitor_ = std::make_unique<Thread>([this] {
        const uint64_t timeout = 60000;  // 60s.
        const uint64_t interval = 100;   // 100ms
        Timer timer;
        while (!interruptFlag_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
            if (timer.ElapsedMilliSecond() < timeout) {
                continue;
            }
            timer.Reset();
            std::stringstream ss;
            ss << "Metadata size info: {";
            ss << "metaTable:" << metaTable_.size();
            ss << ", request2SubMeta:" << request2SubMeta_.size();
            ss << ", objKey2ReqId:" << objKey2ReqId_.size();
            ss << ", processLocks:" << processLocks_.size();
            ss << ", migratingObjectKeys:" << migratingItems_.size();
            ss << ", clientIdRefTable:" << clientIdRefTable_.size();
            ss << ", clientRefTable:" << globalRefTable_->GetClientRefCount();
            ss << ", objectRefTable:" << globalRefTable_->GetObjectRefCount();
            ss << ", remoteClientIdTable:" << globalRefTable_->GetRemoteClientCount();
            ss << ", globalCacheDeleteManager:" << globalCacheDeleteManager_->GetDeletingObjectCount();
            ss << "}";
            LOG(INFO) << ss.str();
        }
    });
    monitor_->set_name("OcMetaMonitor");
}

void OCMetadataManager::InitSubscribeEvent()
{
    NodeTimeoutEvent::GetInstance().AddSubscriber(
        eventName_, [this](const std::string &workerAddr, bool changePrimary, bool removeMeta, bool isOtherAzNode) {
            (void)isOtherAzNode;
            return ProcessWorkerTimeout(workerAddr, changePrimary, removeMeta);
        });
    NodeNetworkRecoveryEvent::GetInstance().AddSubscriber(
        eventName_, [this](const std::string &workerAddr, int64_t timestamp, bool isOffline) {
            return ProcessWorkerNetworkRecovery(workerAddr, timestamp, isOffline);
        });
    ChangePrimaryCopy::GetInstance().AddSubscriber(
        eventName_, [this](const std::string &workerAddr, bool ifvoluntaryScaleDown) {
            ProcessPrimaryCopyByWorkerTimeout(workerAddr, ifvoluntaryScaleDown);
            return Status::OK();
        });
    RequestMetaFromWorkerEvent::GetInstance().AddSubscriber(
        eventName_, [this](const std::string &masterAddr, const std::string &workerAddr) {
            return RequestMetaFromWorker(masterAddr, workerAddr);
        });
    NodeRestartEvent::GetInstance().AddSubscriber(eventName_,
                                                  [this](const std::string &workerAddr, int64_t timestamp, bool sync) {
                                                      return ProcessWorkerRestart(workerAddr, timestamp, sync);
                                                  });
    RecoverMasterAppRefEvent::GetInstance().AddSubscriber(
        eventName_, [this](std::function<bool(const std::string &)> func, const std::string &standbyWorker) {
            return RecoverMasterAppRef(func, standbyWorker);
        });
    HashRingEvent::RecoverMetaRanges::GetInstance().AddSubscriber(
        eventName_, [this](const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges) {
            return RecoverDataOfFaultyWorker(workerUuids, extraRanges);
        });
    HashRingEvent::RecoverAsyncTaskRanges::GetInstance().AddSubscriber(
        eventName_, [this](const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges) {
            return RecoverAsyncTask(workerUuids, extraRanges);
        });
    HashRingEvent::ClearDataWithoutMeta::GetInstance().AddSubscriber(
        eventName_, [this](const worker::HashRange &ranges, const std::string &workerAddr,
                           const worker::HashRange &halfCompletedRanges, const std::vector<std::string> &uuids) {
            return ClearDataWithoutMeta(ranges, workerAddr, halfCompletedRanges, uuids);
        });
    HashRingEvent::ClearDevClientMetaForScaledInWorker::GetInstance().AddSubscriber(
        eventName_, [this](const std::vector<std::string> removeNodes) {
            return ClearDevClientMetaForScaledInWorker(removeNodes);
        });
    HashRingEvent::OtherAzNodeDeadEvent::GetInstance().AddSubscriber(
        eventName_, [this](const std::string &workerAddr) { return ProcessOtherAzWorkerDead(workerAddr); });
}

OCMetadataManager::~OCMetadataManager()
{
    if (!interruptFlag_) {
        Shutdown();
    }
}

void OCMetadataManager::Shutdown()
{
    LOG(INFO) << "Start shutdown OcMetadataManager for " << dbName_;
    if (interruptFlag_.exchange(true)) {
        return;
    }

    NodeTimeoutEvent::GetInstance().RemoveSubscriber(eventName_);
    NodeNetworkRecoveryEvent::GetInstance().RemoveSubscriber(eventName_);
    ChangePrimaryCopy::GetInstance().RemoveSubscriber(eventName_);
    RequestMetaFromWorkerEvent::GetInstance().RemoveSubscriber(eventName_);
    NodeRestartEvent::GetInstance().RemoveSubscriber(eventName_);
    HashRingEvent::RecoverMetaRanges::GetInstance().RemoveSubscriber(eventName_);
    HashRingEvent::RecoverAsyncTaskRanges::GetInstance().RemoveSubscriber(eventName_);
    HashRingEvent::ClearDataWithoutMeta::GetInstance().RemoveSubscriber(eventName_);
    HashRingEvent::ClearDevClientMetaForScaledInWorker::GetInstance().RemoveSubscriber(eventName_);
    HashRingEvent::OtherAzNodeDeadEvent::GetInstance().RemoveSubscriber(eventName_);

    asyncPool_.reset();
    if (monitor_ != nullptr && monitor_->joinable()) {
        monitor_->join();
    }
    if (notifyWorkerManager_ != nullptr) {
        notifyWorkerManager_->Shutdown();
    }
    if (globalCacheDeleteManager_ != nullptr) {
        globalCacheDeleteManager_->Shutdown();
    }
}

Status OCMetadataManager::InitGlobalRef()
{
    globalRefTable_ = std::make_unique<object_cache::ObjectGlobalRefTable<ImmutableString>>();
    // Recovery GLOBAL_REF_TABLE from the Rocksdb.
    RETURN_IF_NOT_OK(LoadRefFromRocks(
        GLOBAL_REF_TABLE, [this](const std::string &key, const std::string &objKey, bool isRemoteClient) {
            std::vector<std::string> failedIncIds;
            std::vector<std::string> firstIncIds;
            Status s = globalRefTable_->GIncreaseRef(key, { objKey }, failedIncIds, firstIncIds, isRemoteClient);
            if (s.IsError()) {
                LOG(WARNING) << "Recovery GLOBAL_REF_TABLE from Rocksdb failed. key:" << key << ", objKey" << objKey;
            }
        }));
    // Recovery REMOTE_CLIENT_REF_TABLE from the Rocksdb.
    RETURN_IF_NOT_OK(
        LoadRefFromRocks(REMOTE_CLIENT_REF_TABLE, [this](const std::string &remoteClientId, const std::string &addr) {
            std::shared_lock<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
            TbbRemoteClientIdRefTable::accessor objAccessor;
            if (!clientIdRefTable_.find(objAccessor, remoteClientId)) {
                clientIdRefTable_.insert(objAccessor, remoteClientId);
            }
            objAccessor->second.emplace(addr);
        }));

    globalRefTable_->RegisterPersistenceFunc(
        std::bind(&ObjectMetaStore::AddGlobalRef, objectStore_.get(), std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3),
        std::bind(&ObjectMetaStore::RemoveGlobalRef, objectStore_.get(), std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3));
    return Status::OK();
}

Status OCMetadataManager::LoadRefFromRocks(const std::string &tableName,
                                           std::function<void(const std::string &, const std::string &)> func)
{
    std::vector<std::pair<std::string, std::string>> globalRefs;

    if (objectStore_->IsRocksdbRunning() || etcdCM_->IsCentralized()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetAllFromRocks(tableName, globalRefs),
                                         "Load global ref from Rocksdb failed.");
    } else {
        LOG(WARNING) << FormatString("Table[%s] does not support using etcd as a l2_cache, just ignore it",
                                     GLOBAL_REF_TABLE);
    };

    for (const auto &info : globalRefs) {
        std::vector<std::string> keyVec;
        std::string::size_type pos = info.first.find("_", 0);
        if (pos != info.first.npos) {
            keyVec.push_back(info.first.substr(0, pos));
            keyVec.push_back(info.first.substr(pos + 1, info.first.npos));
        }
        // The key is WorkerAddr_ObjectKey, so the number of parsed strings is 2.
        if (keyVec.size() == 2) {
            func(keyVec[0], keyVec[1]);
        }
    }
    return Status::OK();
}

Status OCMetadataManager::LoadRefFromRocks(const std::string &tableName,
                                           std::function<void(const std::string &, const std::string &, bool)> func)
{
    std::vector<std::pair<std::string, std::string>> globalRefs;

    if (objectStore_->IsRocksdbRunning() || etcdCM_->IsCentralized()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetAllFromRocks(tableName, globalRefs),
                                         "Load global ref from Rocksdb failed.");
    } else {
        LOG(WARNING) << FormatString("Table[%s] does not support using etcd as a l2_cache, just ignore it",
                                     GLOBAL_REF_TABLE);
    };

    for (const auto &info : globalRefs) {
        // The info format is as follows:
        // workerAddr + "_" + objectKey  or  remoteClientId + "_" + objectKey + REMOTE_CLIENT_FLAG
        // The REMOTE_CLIENT_FLAG is used to mark whether the reference is inside or outside the cloud.
        std::string::size_type pos = info.first.find("_", 0);
        if (pos != info.first.npos) {
            std::string workerAddrOrRemoteClientId = info.first.substr(0, pos);
            bool isRemoteClient = info.first.find(REMOTE_CLIENT_FLAG) != std::string::npos;
            auto objKeySize = isRemoteClient ? (info.first.size() - pos - 1 - REMOTE_CLIENT_FLAG.size())
                                             : (info.first.size() - pos - 1);
            std::string objectKey = info.first.substr(pos + 1, objKeySize);
            func(workerAddrOrRemoteClientId, objectKey, isRemoteClient);
        } else {
            LOG(WARNING) << "The meta restored from rockdb is in an invalid format:"
                         << " key=" << info.first << ", value=" << info.second;
        }
    }
    return Status::OK();
}

Status OCMetadataManager::IncreaseNestedRefCnt(const GIncNestedRefReqPb &req, GIncNestedRefRspPb &resp)
{
    std::vector<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    Status rc = Status::OK();
    RedirectObjRefs(resp, req.redirect(), objectKeys);
    if (resp.ref_is_moving()) {
        return Status::OK();
    }
    for (std::string &objectKey : objectKeys) {
        Status cRC = nestedRefManager_->IncreaseNestedRefCnt(objectKey);
        if (cRC.IsError()) {
            resp.add_failed_object_keys(objectKey);
            rc = cRC;
        }
    }
    return rc;
}

Status OCMetadataManager::DecreaseNestedRefCnt(const GDecNestedRefReqPb &req, GDecNestedRefRspPb &resp)
{
    std::vector<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    Status rc = Status::OK();
    RedirectObjRefs(resp, req.redirect(), objectKeys);
    if (resp.ref_is_moving()) {
        return Status::OK();
    }
    for (std::string &objectKey : objectKeys) {
        Status cRC = nestedRefManager_->DecreaseNestedRefCnt(objectKey);
        if (cRC.IsError()) {
            LOG(ERROR) << "failed to decrease nested ref, objectKey: " << objectKey;
            resp.add_failed_object_keys(objectKey);
            rc = cRC;
            continue;
        }
        // ObjectKey has no more references
        if (nestedRefManager_->CheckIsNoneNestedRefById(objectKey)) {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::accessor accessor;
            auto found = metaTable_.find(accessor, objectKey);
            // Check for object end of life
            if (found && globalRefTable_->GetRefWorkerCount(objectKey) == 0) {
                resp.add_no_ref_ids(objectKey);  // Collect all objects that reached end of the life
            }
        }
    }

    // Delete all the objects that reached to the end of the life
    if (resp.no_ref_ids_size() > 0) {
        DeleteAllCopyMetaReqPb deleteReq;
        *deleteReq.mutable_object_keys() = { resp.no_ref_ids().begin(), resp.no_ref_ids().end() };
        deleteReq.set_address(req.address());
        DeleteAllCopyMetaRspPb deleteRsp;
        VLOG(DEBUG_LOG_LEVEL) << "Request deleteReq: " << LogHelper::IgnoreSensitive(deleteReq)
                              << ", obj size: " << deleteReq.object_keys_size();
        DeleteAllCopyMeta(deleteReq, deleteRsp);
        Status respRc(static_cast<StatusCode>(deleteRsp.last_rc().error_code()), deleteRsp.last_rc().error_msg());
        if (respRc.IsError()) {
            for (const auto &id : deleteRsp.failed_object_keys()) {
                resp.add_failed_object_keys(id);
            }
        }
    }
    return rc;
}

void OCMetadataManager::SetMetaInfo(const ObjectMetaPb &newMeta, const std::string &address, int64_t version,
                                    ObjectMeta &metaCache)
{
    metaCache.meta = newMeta;
    // Object key is the key in a key/value pair for the metadata table.
    // Storing the same object key in the "value" part of the kv is redundant and
    // deprecated. Save memory and resources by removing this from the value.
    // The field itself cannot be removed due to down-level support since this ObjectMeta pb
    // is stored on disk (rocksdb). In future it could be fully removed since its not used
    // anymore.
    metaCache.meta.set_allocated_object_key(NULL);
    metaCache.meta.set_version(version);
    metaCache.meta.set_primary_address(address);
    metaCache.meta.set_ttl_second(newMeta.ttl_second());
    (void)metaCache.locations.emplace(address);
}

Status OCMetadataManager::NotifyOtherAzNodeRemoveMeta(const std::string &objectKey, int64_t version,
                                                      ObjectMetaStore::WriteType type)
{
    RETURN_OK_IF_TRUE(FLAGS_other_cluster_names.empty());
    LOG(INFO) << "Notify nodes in other clusters to remove meta for object: " << objectKey;
    std::unordered_map<std::string, MetaAddrInfo> metaAddrInfos;
    RETURN_IF_NOT_OK(etcdCM_->GetAllNodesInOtherAzsByHash(objectKey, metaAddrInfos, true));
    for (const auto &item : metaAddrInfos) {
        HostPort masterHostPort = item.second.GetAddressAndSaveDbName();
        std::unordered_set<std::string> failedObjs;
        auto rc = notifyWorkerManager_->NotifyMasterRemoveMeta(masterHostPort, { { objectKey, version } }, failedObjs);
        if (rc.IsError() || !failedObjs.empty()) {
            LOG(WARNING) << "Fail to notify other az's node to remove meta: " << rc.ToString();
            // DFX
            LOG_IF_ERROR(notifyWorkerManager_->InsertAsyncWorkerOp(
                             "", objectKey, { NotifyWorkerOpType::REMOVE_META, version, { item.first } }, true, type),
                         "Insert remote meta notification to AsyncWorkerOpTable failed, obj: " + objectKey);
        }
    }
    return Status::OK();
}

Status OCMetadataManager::CreateMetaFirstTime(const ObjectMetaPb &newMeta, const std::string &address, int64_t version,
                                              const std::set<ImmutableString> &nestedObjectKeys,
                                              TbbMetaTable::accessor &accessor)
{
    const std::string &objectKey = newMeta.object_key();
    ObjectMetaStore::WriteType type = WriteMode2MetaType(newMeta.config().write_mode());
    ObjectMeta metaCache;
    SetMetaInfo(newMeta, address, version, metaCache);
    accessor->second = metaCache;
    // multi create meta save meta to rocksdb and etcd when commit meta.
    std::string serializedStr;
    RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr));
    // Create meta info in rocksDB.
    RETURN_IF_NOT_OK(objectStore_->CreateOrUpdateMeta(objectKey, serializedStr, type));
    accessor.release();

    if (!HasWorkerId(objectKey)) {
        RETURN_IF_NOT_OK(NotifyOtherAzNodeRemoveMeta(objectKey, version, type));
    }

    // Update subscribeCache. if multiset_state == pending, create not finish, don't update subscribe.
    UpdateSubscribeCache(objectKey, metaCache);

    // Update nested reference count to maintain dependencies.
    if ((ObjectLifeState(newMeta.life_state()) != ObjectLifeState::OBJECT_INVALID) && !nestedObjectKeys.empty()) {
        VLOG(DEBUG_LOG_LEVEL) << datasystem::FormatString("Nested dependency set for object: %s, the nested keys: %s",
                                                          objectKey, VectorToString(nestedObjectKeys));
        // Store nested relationships
        RETURN_IF_NOT_OK(nestedRefManager_->IncreaseNestedRefCnt(objectKey, nestedObjectKeys));
        // Increase the count for objectKeys that current object is dependent on
        std::vector<std::string> toBeNotifiedNestedRefs;
        HostPort masterAddr;
        RETURN_IF_NOT_OK(masterAddr.ParseString(masterAddress_));
        MetaAddrInfo targetMetaAddrInfo(masterAddr, dbName_);
        for (const auto &nestedObjectKey : nestedObjectKeys) {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            MetaAddrInfo masterAddrInfo;
            etcdCM_->GetMetaAddress(nestedObjectKey, masterAddrInfo);
            INJECT_POINT("IncreaseNestedRefCnt.local.addr", [this, &nestedObjectKey]() {
                RETURN_IF_NOT_OK(nestedRefManager_->IncreaseNestedRefCnt(nestedObjectKey));
                return Status::OK();
            });
            // Check if object to add belongs to this master and then add locally.
            // if scale up and obj is hash to find master, need redirect will be true, if obj spilt with workerid,
            // masterAddrInfo is master addr.
            if (masterAddrInfo == targetMetaAddrInfo && !etcdCM_->NeedRedirect(nestedObjectKey, masterAddr)) {
                RETURN_IF_NOT_OK(nestedRefManager_->IncreaseNestedRefCnt(nestedObjectKey));
            } else {
                VLOG(1) << "nested object meta is not in local address, objectkey:" << nestedObjectKey;
                toBeNotifiedNestedRefs.emplace_back(nestedObjectKey);
            }
        }

        if (toBeNotifiedNestedRefs.size() > 0) {
            // send notifications to all masters through sourceWorker
            RETURN_IF_NOT_OK(notifyWorkerManager_->IncNestedRefs(address, toBeNotifiedNestedRefs));
        }
    }
    return Status::OK();
}

Status OCMetadataManager::CheckBinaryFormatParamMatch(const std::string &objectKey, const ObjectMeta &prevMeta,
                                                      const BinaryFormatParamsStruct &newMeta,
                                                      const std::set<ImmutableString> &nestedObjectKeys)
{
    const auto &prevConfig = prevMeta.meta.config();
    const auto oldConsistencyType = prevConfig.consistency_type();
    const auto oldWriteMode = prevConfig.write_mode();
    const auto oldCacheType = prevConfig.cache_type();
    const bool oldIsReplica = prevConfig.is_replica();

    const bool isBinaryFormatConsistent =
        (newMeta.dataFormat == static_cast<uint32_t>(DataFormat::BINARY)) && prevMeta.IsBinary();

    const bool isConfigConsistent = (newMeta.consistencyType == oldConsistencyType)
                                    && (newMeta.writeMode == oldWriteMode) && (newMeta.cacheType == oldCacheType)
                                    && (newMeta.isReplica == oldIsReplica);
    CHECK_FAIL_RETURN_STATUS(
        isBinaryFormatConsistent && isConfigConsistent, StatusCode::K_INVALID,
        FormatString(
            "Inconsistency in publish or set cache whose key already exists in datasystem, please delete the old cache "
            "or keep parameter the same as the old cache, newMeta: (consistency_type, "
            "write_mode, cache_type), (%zu, %zu, %zu); old: (%zu, %zu, %zu)",
            newMeta.consistencyType, newMeta.writeMode, newMeta.cacheType, oldConsistencyType, oldWriteMode,
            oldCacheType));
    CHECK_FAIL_RETURN_STATUS(((prevMeta.meta.life_state() != static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED))),
                             StatusCode::K_OC_ALREADY_SEALED, "Already sealed");

    CHECK_FAIL_RETURN_STATUS(nestedRefManager_->NestedKeysCanSet(objectKey, nestedObjectKeys),
                             StatusCode::K_RUNTIME_ERROR, "Nested keys not match");
    return Status::OK();
}

Status OCMetadataManager::CreatePendingMeta(const ObjectMetaPb &newMeta, const std::string &address, int64_t pendingTtl,
                                            bool &firstOne)
{
    const std::string &objectKey = newMeta.object_key();
    // Validate the input.
    RETURN_IF_NOT_OK(expiredObjectManager_->RemoveObjectIfExist(objectKey));

    INJECT_POINT("master.create_meta_failure");
    VLOG(1) << FormatString("[ObjectKey %s] CreateMeta PreCommit: worker address: %s", objectKey, address);

    if (newMeta.config().consistency_type() == static_cast<uint32_t>(ConsistencyType::CAUSAL)
        && !AddHeavyOp(objectKey)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_WORKER_DEADLOCK, "retry");
    }
    Raii raii([this, &objectKey]() { RemoveHeavyOp({ objectKey }); });
    INJECT_POINT("master.CreateMeta.delay");

    TbbLockTable::accessor pLock;
    Raii eraseLock([this, &pLock]() { (void)processLocks_.erase(pLock); });
    (void)processLocks_.insert(pLock, objectKey);

    // Case 1: not first time create meta.
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("CreatePendingMeta get lock", timer.ElapsedMilliSecond());
    TbbMetaTable::accessor accessor;
    firstOne = metaTable_.insert(accessor, objectKey);
    Status rc = CheckExistenceOpt(accessor->second, objectKey, newMeta.existence(), firstOne);
    switch (rc.GetCode()) {
        case K_OC_KEY_ALREADY_EXIST:
            return rc;
        case K_TRY_AGAIN:
            // If the timestamp of the object does not exceed multiSetTimestamp, return K_TRY_AGAIN.
            // Except for the same address, we can refresh meta.
            if (address != accessor->second.meta.primary_address()
                && GetSystemClockTimeStampUs() < accessor->second.multiSetTimestamp) {
                return rc;
            }
            LOG(INFO) << FormatString("[ObjectKey %s] PreCommit changed from %s to %s", objectKey,
                                      accessor->second.meta.primary_address(), address);
            break;
        default:
            break;
    };
    ObjectMeta metaCache;
    SetMetaInfo(newMeta, address, 0, metaCache);
    metaCache.multiSetState = PENDING;
    metaCache.multiSetTimestamp = pendingTtl;
    accessor->second = std::move(metaCache);
    accessor.release();
    return Status::OK();
}

Status OCMetadataManager::CheckExistenceOpt(const ObjectMeta &meta, const std::string &objectKey,
                                            const ExistenceOptPb &existence, bool &firstOne)
{
    bool noL2CacheAndNoCopy =
        meta.locations.empty() && WriteMode(meta.meta.config().write_mode()) == WriteMode::NONE_L2_CACHE;
    if (!firstOne && existence == ExistenceOptPb::NX && !noL2CacheAndNoCopy) {
        if (meta.multiSetState == IDLE) {
            RETURN_STATUS(K_OC_KEY_ALREADY_EXIST, "object[" + objectKey + "] already exist");
        } else {
            RETURN_STATUS(K_TRY_AGAIN, "object[" + objectKey + "] is creating");
        }
    }
    return Status::OK();
}

void OCMetadataManager::MarkUpdatingAndUpdateRemoveMetaNotification(const std::string &objectKey, int64_t version,
                                                                    RaiiPlus &raiiP)
{
    {
        std::lock_guard<std::shared_timed_mutex> lck(updatingObjsTableMutex_);
        updatingObjsTable_.emplace(objectKey, version);
    }
    raiiP.AddTask([this, &objectKey]() {
        std::lock_guard<std::shared_timed_mutex> lck(updatingObjsTableMutex_);
        updatingObjsTable_.erase(objectKey);
    });
    notifyWorkerManager_->UpdateRemoteMetaNotification(objectKey, version);
}

Status OCMetadataManager::CreateMetaForBinaryFormat(const ObjectMetaPb &newMeta, const std::string &address,
                                                    const std::set<ImmutableString> &nestedObjectKeys, int64_t &version,
                                                    bool &firstOne)
{
    const std::string &objectKey = newMeta.object_key();
    // Case 1: not first time create meta.
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::accessor accessor;
    firstOne = metaTable_.insert(accessor, objectKey);
    // In the NX Set scenario, when the worker restarts, if there is data in the L2 cache,
    // it is not allowed to double Set.
    RETURN_IF_NOT_OK(CheckExistenceOpt(accessor->second, objectKey, newMeta.existence(), firstOne));
    version = static_cast<int64_t>(GetSystemClockTimeStampUs());

    RaiiPlus raiiP;
    if (!firstOne && !HasWorkerId(objectKey)) {
        MarkUpdatingAndUpdateRemoveMetaNotification(objectKey, version, raiiP);
    }

    if (!firstOne) {
        auto &prevMeta = accessor->second;
        CHECK_FAIL_RETURN_STATUS(
            prevMeta.multiSetState != PENDING, K_TRY_AGAIN,
            FormatString("update meta failed, multi meta objectKey(%s) is creating, wait and try again", objectKey));
        BinaryFormatParamsStruct newMateDate = { .writeMode = newMeta.config().write_mode(),
                                                 .dataFormat = newMeta.config().data_format(),
                                                 .consistencyType = newMeta.config().consistency_type(),
                                                 .cacheType = newMeta.config().cache_type(),
                                                 .isReplica = newMeta.config().is_replica() };
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            CheckBinaryFormatParamMatch(objectKey, prevMeta, newMateDate, nestedObjectKeys), "Check format failed");

        // Cache Invalidation Logic.
        Status s = DoBinaryCacheInvalidationUnlocked(objectKey, prevMeta,
                                                     { .newAddress = address,
                                                       .newVersion = version,
                                                       .newDataSz = newMeta.data_size(),
                                                       .newLifeState = newMeta.life_state(),
                                                       .newBlobSizes = newMeta.device_info().blob_sizes() });
        if (s.IsError()) {
            // If the cache invalid processing fails, delete the address from the meta.
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->RemoveObjectLocation(objectKey, address),
                                             "Remove location failed from rocksdb.");
            (void)prevMeta.locations.erase(address);
        }

        if (!nestedObjectKeys.empty() && nestedRefManager_->IsNestedKeysDiff(objectKey, nestedObjectKeys)) {
            RETURN_IF_NOT_OK(nestedRefManager_->IncreaseNestedRefCnt(objectKey, nestedObjectKeys));
        }
        RETURN_IF_NOT_OK(expiredObjectManager_->InsertObject(objectKey, version, newMeta.ttl_second()));
        return s;
    }
    // Case 2: first time creating meta.
    RETURN_IF_NOT_OK(CreateMetaFirstTime(newMeta, address, version, nestedObjectKeys, accessor));
    RETURN_IF_NOT_OK(expiredObjectManager_->InsertObject(objectKey, version, newMeta.ttl_second()));
    VLOG(1) << FormatString("[ObjectKey %s] CreateMeta finished: objectKey: %s, worker address: %s", objectKey,
                            objectKey, address);
    return Status::OK();
}

Status OCMetadataManager::CreateMultiMeta(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp)
{
    PerfPoint point(PerfKey::MASTER_CREATE_MULTI_META);
    std::vector<std::string> objectKeys;
    for (const auto &info : req.metas()) {
        objectKeys.emplace_back(info.object_key());
    }
    LOG(INFO) << FormatString("Processing CreateMultiMeta objectKeys: %s, source: %s", VectorToString(objectKeys),
                              req.address());
    FillRedirectResponseInfos(rsp, objectKeys, req.redirect());
    RETURN_OK_IF_TRUE(!rsp.info().empty());
    if (req.istx()) {
        return CreateMultiMetaTx(req, rsp);
    }
    return CreateMultiMetaNtx(req, rsp);
}

Status OCMetadataManager::UpdateMeta(ObjectMeta &meta, const ObjectMetaPb &newMeta, const std::string &address,
                                     int64_t &version)
{
    const std::string &objectKey = newMeta.object_key();
    // In the NX Set scenario, when the worker restarts, if there is data in the L2 cache,
    // it is not allowed to double Set.
    bool firstOne = false;
    RETURN_IF_NOT_OK(CheckExistenceOpt(meta, objectKey, newMeta.existence(), firstOne));
    RaiiPlus raiiP;
    if (!HasWorkerId(objectKey)) {
        MarkUpdatingAndUpdateRemoveMetaNotification(objectKey, version, raiiP);
    }
    CHECK_FAIL_RETURN_STATUS(
        meta.multiSetState != PENDING, K_TRY_AGAIN,
        FormatString("update meta failed, multi meta objectKey(%s) is creating, wait and try again", objectKey));
    BinaryFormatParamsStruct newMateDate = { .writeMode = newMeta.config().write_mode(),
                                             .dataFormat = newMeta.config().data_format(),
                                             .consistencyType = newMeta.config().consistency_type(),
                                             .cacheType = newMeta.config().cache_type(),
                                             .isReplica = newMeta.config().is_replica() };
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CheckBinaryFormatParamMatch(objectKey, meta, newMateDate), "Check format failed");

    // Cache Invalidation Logic.
    Status s = DoBinaryCacheInvalidationUnlocked(objectKey, meta,
                                                 { .newAddress = address,
                                                   .newVersion = version,
                                                   .newDataSz = newMeta.data_size(),
                                                   .newLifeState = newMeta.life_state(),
                                                   .newBlobSizes = newMeta.device_info().blob_sizes() });
    if (s.IsError()) {
        // If the cache invalid processing fails, delete the address from the meta.
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->RemoveObjectLocation(objectKey, address),
                                         "Remove location failed from rocksdb.");
        (void)meta.locations.erase(address);
    }

    RETURN_IF_NOT_OK(expiredObjectManager_->InsertObject(objectKey, version, newMeta.ttl_second()));
    return s;
}

Status OCMetadataManager::CreateMeta(const std::string &objectKey, ObjectMeta &newMeta, const std::string &address,
                                     int64_t &version, bool &firstOne)
{
    INJECT_POINT("master.create_meta_failure");
    auto &metaPb = newMeta.meta;
    const auto ttl = metaPb.ttl_second();
    ObjectMetaStore::WriteType type = WriteMode2MetaType(metaPb.config().write_mode());
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::accessor accessor;
    firstOne = metaTable_.insert(accessor, objectKey);
    if (!firstOne) {
        return UpdateMeta(accessor->second, metaPb, address, version);
    }
    ObjectMeta metaCache;
    if (objectStore_->IsPersistenceEnabled()) {
        std::string serializedStr;
        RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objectKey, metaPb, serializedStr));
        RETURN_IF_NOT_OK(objectStore_->CreateOrUpdateMeta(objectKey, serializedStr, type));
    }
    accessor->second = std::move(newMeta);
    accessor.release();
    if (!FLAGS_other_cluster_names.empty() && !HasWorkerId(objectKey)) {
        RETURN_IF_NOT_OK(NotifyOtherAzNodeRemoveMeta(objectKey, version, type));
    }
    return expiredObjectManager_->InsertObject(objectKey, version, ttl);
}

void OCMetadataManager::ConstructMetaInfo(const CreateMultiMetaReqPb &req, const ObjectBaseInfoPb &info,
                                          int64_t version, ObjectMetaPb &meta)
{
    meta.set_object_key(info.object_key());
    meta.set_data_size(info.data_size());
    meta.set_version(version);
    meta.set_life_state(req.life_state());
    *meta.mutable_config() = req.config();
    meta.set_primary_address(req.address());
    meta.set_ttl_second(req.ttl_second());
    meta.set_existence(req.existence());
    if (info.has_device_info()) {
        *meta.mutable_device_info() = info.device_info();
    }
}
Status OCMetadataManager::CreateMultiMetaNtx(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp)
{
    std::vector<std::string> rollBackIds;
    Status lastRc;
    if (req.address().empty()) {
        return Status(K_INVALID, "CreateMeta: Cannot CreateMeta with server address.");
    }
    PerfPoint point(PerfKey::MASTER_CREATE_MULTI_META_CONSTRUCT);
    std::vector<std::string> objsFirst;
    objsFirst.reserve(req.metas_size());
    int64_t version = static_cast<int64_t>(GetSystemClockTimeStampUs());
    std::vector<ObjectMeta> newMetas;
    newMetas.reserve((req.metas_size()));
    for (int i = 0; i < req.metas_size(); i++) {
        const ObjectBaseInfoPb &info = req.metas(i);
        ObjectMeta &meta = newMetas.emplace_back();
        meta.locations.emplace(req.address());
        ConstructMetaInfo(req, info, version, meta.meta);
    }
    point.RecordAndReset(PerfKey::MASTER_CREATE_MULTI_META_IMPL);
    for (int i = 0; i < req.metas_size(); i++) {
        const auto &objectKey = req.metas(i).object_key();
        if (objectKey.empty()) {
            rsp.add_failed_object_keys(objectKey);
            lastRc = Status(K_INVALID, "CreateMeta: Cannot CreateMeta with server address.");
            continue;
        }
        bool firstOne = false;
        auto status = CreateMeta(objectKey, newMetas[i], req.address(), version, firstOne);
        if (firstOne) {
            objsFirst.emplace_back(objectKey);
        }
        if (status.IsError()) {
            // meta maybe already insert to metatable. if not first one, no need delete old meta.
            if (firstOne) {
                rollBackIds.emplace_back(objectKey);
            }
            rsp.add_failed_object_keys(objectKey);
            lastRc = status;
        }
    }
    point.RecordAndReset(PerfKey::MASTER_CREATE_MULTI_META_ASYN_EXEC);
    ExecuteAsyncTask([this, objsFirst]() {
        for (const auto &objKey : objsFirst) {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::const_accessor accessor;
            if (!metaTable_.find(accessor, objKey)) {
                LOG(WARNING) << "Object " << objKey << " can't found in metaTable, notify subscribe failed";
                continue;
            }
            ObjectMeta metaCache = accessor->second;
            accessor.release();
            UpdateSubscribeCache(objKey, metaCache);
        }
    });
    point.RecordAndReset(PerfKey::MASTER_CREATE_MULTI_META_POST_PROCESS);
    RollBackMultiMetaWhenCreateFailed(rollBackIds, req.address());
    rsp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
    rsp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    rsp.set_version(version);
    return Status::OK();
}

Status OCMetadataManager::CreateMultiMetaTx(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp)
{
    std::vector<std::string> successIds;
    int64_t pendingTtl = GetSystemClockTimeStampUs() + MSET_PENDING_TTL_US;
    INJECT_POINT("master.CreateMultiMetaTx.pendingTtl", [&pendingTtl](int ttlUs) {
        pendingTtl = GetSystemClockTimeStampUs() + ttlUs;
        return Status::OK();
    });
    for (const auto &metaInfo : req.metas()) {
        if (metaInfo.object_key().empty() || req.address().empty()) {
            RollBackMultiMetaWhenCreateFailed(successIds, req.address());
            RETURN_STATUS(K_INVALID, "CreateMeta: Cannot CreateMeta with empty objectKey or server address.");
        }
        ObjectMetaPb meta;
        ConstructMetaInfo(req, metaInfo, 0, meta);
        bool firstOne = false;
        auto status = CreatePendingMeta(meta, req.address(), pendingTtl, firstOne);
        if (status.IsError()) {
            // meta maybe already insert to metatable. if not first one, no need delete old meta.
            if (firstOne) {
                successIds.emplace_back(metaInfo.object_key());
            }
            RollBackMultiMetaWhenCreateFailed(successIds, req.address());
            return status;
        } else {
            successIds.emplace_back(metaInfo.object_key());
        }
    }
    INJECT_POINT("OCMetadataManager.createMultiMeta.delay");
    if (req.is_pre_commit()) {
        return Status::OK();
    }
    auto type = WriteMode2MetaType(req.config().write_mode());
    uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    auto status = PublishMultiMeta(successIds, req.address(), type, version, rsp);
    if (status.IsError()) {
        RollBackMultiMetaWhenCreateFailed(successIds, req.address(), version);
    }
    return status;
}

Status OCMetadataManager::CreateMultiMetaPhaseTwo(const CreateMultiMetaPhaseTwoReqPb &req, CreateMultiMetaRspPb &rsp)
{
    std::vector<std::string> objectKeys;
    for (const auto &obj : req.object_keys()) {
        objectKeys.emplace_back(obj);
    }
    std::sort(objectKeys.begin(), objectKeys.end());  // To prevent deadlock.
    LOG(INFO) << FormatString("[ObjectKey %s] CreateMeta PhaseTwo: worker address: %s", VectorToString(objectKeys),
                              req.address());
    FillRedirectResponseInfos(rsp, objectKeys, req.redirect());
    RETURN_OK_IF_TRUE(!rsp.info().empty());

    if (req.consistency_type() == static_cast<uint32_t>(ConsistencyType::CAUSAL) && !AddHeavyOp(objectKeys)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_WORKER_DEADLOCK, "retry");
    }
    Raii raii([this, &objectKeys]() { RemoveHeavyOp(objectKeys); });

    std::vector<TbbLockTable::accessor> pLocks(objectKeys.size());
    for (size_t i = 0; i < objectKeys.size(); i++) {
        (void)processLocks_.insert(pLocks[i], objectKeys[i]);
    }
    Raii eraseLocks([this, &pLocks]() {
        for (auto &pLock : pLocks) {
            (void)processLocks_.erase(pLock);
        }
    });

    auto type = WriteMode2MetaType(req.write_mode());
    uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    auto status = PublishMultiMeta(objectKeys, req.address(), type, version, rsp);
    if (status.IsError()) {
        RollBackMultiMetaWhenCreateFailed(objectKeys, req.address(), version);
    }
    return status;
}

Status OCMetadataManager::PublishMultiMeta(const std::vector<std::string> &objectKeys, const std::string &address,
                                           ObjectMetaStore::WriteType type, uint64_t version, CreateMultiMetaRspPb &rsp)
{
    std::unordered_map<std::string, std::string> metaInfos;
    for (const auto &objKey : objectKeys) {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            metaTable_.find(accessor, objKey), K_RUNTIME_ERROR,
            FormatString("[ObjectKey %s] The object key not exists in metaTable_", objKey));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(address == accessor->second.meta.primary_address(), K_OC_KEY_ALREADY_EXIST,
                                             FormatString("[ObjectKey %s] The object key was seized by %s", objKey,
                                                          accessor->second.meta.primary_address()));
        accessor->second.meta.set_version(version);
        accessor->second.multiSetState = IDLE;
        ObjectMetaPb &objectMeta = accessor->second.meta;
        if (objectMeta.config().data_format() != (uint64_t)DataFormat::HASH_MAP) {
            UpdateSubscribeCache(objKey, accessor->second);
        }
        RETURN_IF_NOT_OK(expiredObjectManager_->InsertObject(objKey, version, objectMeta.ttl_second()));
        std::string serializedStr;
        RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objKey, objectMeta, serializedStr));
        metaInfos.emplace(objKey, serializedStr);
    }
    rsp.set_version(version);
    return objectStore_->CreateOrUpdateBatchMeta(metaInfos, type);
}

void OCMetadataManager::RollBackMultiMetaWhenCreateFailed(const std::vector<std::string> &rollBackIds,
                                                          const std::string address, uint64_t version)
{
    LOG(INFO) << FormatString("Start to rollback multiMeta for objectKey(%s)", VectorToString(rollBackIds));
    for (const auto &objKey : rollBackIds) {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        if (!metaTable_.find(accessor, objKey)) {
            LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objKey);
        } else if (accessor->second.meta.primary_address() == address
                   && (accessor->second.multiSetState == PENDING || accessor->second.meta.version() == version)) {
            (void)metaTable_.erase(accessor);
        } else {
            LOG(WARNING) << FormatString(
                "[ObjectKey %s] Skip rollback, meta not match, address: %s vs %s, version: %s vs %s", objKey, address,
                accessor->second.meta.primary_address(), version, accessor->second.meta.version());
        }
    }
}

Status OCMetadataManager::CreateMeta(const CreateMetaReqPb &request, CreateMetaRspPb &response)
{
    const std::set<ImmutableString> nestedObjectKeys = { request.nested_keys().begin(), request.nested_keys().end() };
    const std::string &objectKey = request.meta().object_key();
    const std::string address = request.address();
    bool redirect = request.redirect();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!objectKey.empty() && !request.address().empty(), K_INVALID,
                                         "CreateMeta: Cannot CreateMeta with empty objectKey or server address.");
    FillRedirectResponseInfo(response, objectKey, redirect);
    RETURN_OK_IF_TRUE(redirect);
    int64_t version = 0;
    bool firstOne = false;
    RETURN_IF_NOT_OK(CreateMeta(request.meta(), request.address(), nestedObjectKeys, version, firstOne));
    response.set_version(version);
    return Status::OK();
}

Status OCMetadataManager::CreateMeta(const ObjectMetaPb &newMeta, const std::string &address,
                                     const std::set<ImmutableString> &nestedObjectKeys, int64_t &version,
                                     bool &firstOne)
{
    const std::string &objectKey = newMeta.object_key();
    // Validate the input.
    RETURN_IF_NOT_OK(expiredObjectManager_->RemoveObjectIfExist(objectKey));

    INJECT_POINT("master.create_meta_failure");
    VLOG(1) << FormatString("[ObjectKey %s] CreateMeta begin: worker address: %s", objectKey, address);

    if (newMeta.config().consistency_type() == static_cast<uint32_t>(ConsistencyType::CAUSAL)
        && !AddHeavyOp(objectKey)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_WORKER_DEADLOCK, "retry");
    }
    Raii raii([this, &objectKey]() { RemoveHeavyOp({ objectKey }); });

    TbbLockTable::accessor pLock;
    Raii eraseLock([this, &pLock]() { (void)processLocks_.erase(pLock); });
    (void)processLocks_.insert(pLock, objectKey);

    // Condition 1: Create meta for hash format.
    if (newMeta.config().data_format() == (uint64_t)DataFormat::HASH_MAP) {
        return CreateHashMeta(newMeta, address);
    }

    if (newMeta.config().data_format() == (uint64_t)DataFormat::HETERO) {
        return CreateDeviceMeta(newMeta, address);
    }
    // Condition 2: Create meta for binary format.
    return CreateMetaForBinaryFormat(newMeta, address, nestedObjectKeys, version, firstOne);
}

Status OCMetadataManager::CreateCopyMeta(const CreateCopyMetaReqPb &request, CreateCopyMetaRspPb &response)
{
    // Validate the input.
    const std::string &objectKey = request.object_key();
    const std::string &address = request.address();
    bool redirect = request.redirect();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        !objectKey.empty() && !address.empty(), K_INVALID,
        "CreateCopyMeta: Cannot CreateCopyMeta with empty objectKey or server address.");
    FillRedirectResponseInfo(response, objectKey, redirect);
    RETURN_OK_IF_TRUE(redirect);
    {
        // Check meta info in cache and rocksdb.
        Timer timer;
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        masterOperationTimeCost.Append("CreateCopyMeta get lock", timer.ElapsedMilliSecond());
        TbbMetaTable::accessor accessor;
        auto found = metaTable_.find(accessor, objectKey);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            found, StatusCode::K_NOT_FOUND,
            FormatString("The objectKey(%s) does not exist, can not create copy meta.", objectKey));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(accessor->second.meta.config().data_format() == request.data_format(),
                                             K_INVALID,
                                             FormatString("Invalid data format of objectKey(%s)", objectKey));

        response.set_version(accessor->second.meta.version());
        response.set_life_state(accessor->second.meta.life_state());
        // If the address already exists, return success.
        if (!accessor->second.locations.insert(address).second) {
            return Status::OK();
        }
        accessor.release();
    }
    return objectStore_->AddObjectLocation(objectKey, address);
}

std::string OCMetadataManager::SelectObjectLocation(const std::string &objectKey, const std::string &sourceWorker,
                                                    const std::unordered_set<ImmutableString> &locations)
{
    PerfPoint point(PerfKey::MASTER_SELECT_LOCATION);
    static thread_local std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
    if (!locations.empty()) {
        INJECT_POINT("master.select_location", [](std::string addr) { return addr; });
        std::vector<std::string> locationsVec = { locations.begin(), locations.end() };
        std::shuffle(locationsVec.begin(), locationsVec.end(), gen);
        for (const auto &addr : locationsVec) {
            if (sourceWorker != addr
                && !notifyWorkerManager_->CheckExistAsyncWorkerOp(addr, objectKey, NotifyWorkerOpType::CACHE_INVALID)
                && !notifyWorkerManager_->CheckExistAsyncWorkerOp(addr, objectKey,
                                                                  NotifyWorkerOpType::PRIMARY_COPY_INVALID)) {
                return addr;  // Return the valid address.
            }
        }
    }
    return "";
}

Status OCMetadataManager::GetObjectMetaType(const std::string &objectKey, ObjectMetaStore::WriteType &type)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::const_accessor accessor;
    if (metaTable_.find(accessor, objectKey)) {
        auto writeMode = accessor->second.meta.config().write_mode();
        type = WriteMode2MetaType(writeMode);
    } else {
        return Status(K_NOT_FOUND, "object not found");
    }
    return Status::OK();
}

Status OCMetadataManager::QueryMeta(const QueryMetaReqPb &req, QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    PerfPoint point(PerfKey::MASTER_QUERY_META_FILL_REDIRECT);
    std::vector<std::string> notRedirectObjectKeys = { req.ids().begin(), req.ids().end() };
    const auto &address = req.address();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!address.empty(), StatusCode::K_RUNTIME_ERROR, "Address is empty");
    FillRedirectResponseInfos(rsp, notRedirectObjectKeys, req.redirect());
    point.RecordAndReset(PerfKey::MASTER_QUERY_META_BATCH_LOCK);
    std::set<std::string> sortedObjectKeys = { notRedirectObjectKeys.begin(), notRedirectObjectKeys.end() };
    std::vector<TbbLockTable::accessor> pLocks(sortedObjectKeys.size());
    int pos = 0;
    for (const auto &objectKey : sortedObjectKeys) {
        (void)processLocks_.insert(pLocks[pos], objectKey);
        pos++;
    }

    Raii eraseLocks([this, &pLocks]() {
        PerfPoint point(PerfKey::MASTER_QUERY_META_BATCH_UNLOCK);
        for (auto &l : pLocks) {
            processLocks_.erase(l);
            l.release();
        }
    });
    INJECT_POINT("OCMetadataManager.QueryMeta,wait");
    point.RecordAndReset(PerfKey::MASTER_QUERY_META_FROM_META_TABLE);
    std::vector<std::string> tmpNotExistObjectKeys;
    RETURN_IF_NOT_OK(QueryMetaFromMetaTable(req, notRedirectObjectKeys, rsp, payloads, tmpNotExistObjectKeys));
    point.RecordAndReset(PerfKey::MASTER_QUERY_META_FILL_REDIRECT_AGAIN);
    FillRedirectResponseInfos(rsp, tmpNotExistObjectKeys, req.redirect());
    point.RecordAndReset(PerfKey::MASTER_QUERY_META_SET_RSP);
    std::list<std::string> notExistObjectKeys = { tmpNotExistObjectKeys.begin(), tmpNotExistObjectKeys.end() };
    std::vector<uint64_t> deletingVersions;
    if (FLAGS_oc_io_from_l2cache_need_metadata) {
        deletingVersions.resize(notExistObjectKeys.size());
    } else {
        globalCacheDeleteManager_->GetDeletingVersions(notExistObjectKeys, deletingVersions);
    }
    *rsp.mutable_not_exist_ids() = { notExistObjectKeys.begin(), notExistObjectKeys.end() };
    *rsp.mutable_deleting_versions() = { deletingVersions.begin(), deletingVersions.end() };
    INJECT_POINT("master.slow_query_meta");
    point.RecordAndReset(PerfKey::MASTER_QUERY_META_SUBSCRIBE);
    return TryToSubscribeCache(req.sub_timeout(), req, notExistObjectKeys);
}

Status OCMetadataManager::QueryMetaFromMetaTable(const QueryMetaReqPb &req, const std::vector<std::string> &objectKeys,
                                                 QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads,
                                                 std::vector<std::string> &notExistObjectKeys)
{
    const auto &address = req.address();
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("QueryMeta get lock", timer.ElapsedMilliSecond());
    uint64_t payloadSize = 0;
    for (auto it = objectKeys.begin(); it != objectKeys.end(); ++it) {
        std::string objectKey = *it;
        TbbMetaTable::accessor accessor;
        if (metaTable_.find(accessor, objectKey) && accessor->second.multiSetState != PENDING) {
            auto *queryMeta = rsp.add_query_metas();
            queryMeta->mutable_meta()->CopyFrom(accessor->second.meta);
            queryMeta->mutable_meta()->set_object_key(objectKey);
            queryMeta->set_address(SelectObjectLocation(objectKey, address, accessor->second.locations));
            queryMeta->set_single_copy(accessor->second.IsPrimaryWithoutCopy(accessor->second.meta.primary_address()));
            TryGetObjectData(objectKey, accessor, payloadSize, *queryMeta, payloads);
            bool updateLocation =
                (ConsistencyType)(accessor->second.meta.config().consistency_type()) == ConsistencyType::PRAM;
            if (updateLocation && accessor->second.locations.insert(address).second) {
                RETURN_IF_NOT_OK(objectStore_->AddObjectLocation(objectKey, address));
            }
            accessor.release();
            continue;
        }
        notExistObjectKeys.emplace_back(objectKey);
    }
    RETURN_OK_IF_TRUE(notExistObjectKeys.empty());
    LOG(INFO) << "Can not found some objects, size: " << notExistObjectKeys.size()
              << ", ObjectKeys: " << VectorToString(notExistObjectKeys);
    return Status::OK();
}

Status OCMetadataManager::TryToSubscribeCache(int64_t timeout, const QueryMetaReqPb &reqPb,
                                              std::list<std::string> &objectKeys)
{
    if (timeout != 0 && !objectKeys.empty()) {
        LOG(INFO) << "Try to subscribe, sub_timeout: " << timeout;
        auto subMeta = std::make_shared<SubscribeMeta>(reqPb.request_id(), std::move(objectKeys), reqPb.address(),
                                                       reqPb.is_from_other_az());
        TimerQueue::TimerImpl timer;
        auto traceID = Trace::Instance().GetTraceID();
        auto weakThis = weak_from_this();
        auto func = [weakThis, timeout, reqId = reqPb.request_id(), traceID]() {
            auto ocMetamanager = weakThis.lock();
            if (ocMetamanager == nullptr) {
                return;
            }
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            LOG(ERROR) << FormatString("The sub request timeout, request id: %s, timeout: %s", reqId, timeout);
            ocMetamanager->RemoveSubscribeCache(reqId);
        };
        RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(timeout, func, timer));
        subMeta->timer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
        RETURN_IF_NOT_OK(AddSubscribeCache(std::move(subMeta)));
    }
    return Status::OK();
}

void OCMetadataManager::ProcessRemoveMetaNotifyFromOtherAz(const RemoveMetaReqPb &request, RemoveMetaRspPb &response)
{
    std::vector<std::string> needHandleObjs;
    std::unordered_map<std::string, uint64_t> objs2Version;
    for (const auto &idWithVersion : request.id_with_version()) {
        needHandleObjs.emplace_back(idWithVersion.id());
        objs2Version.insert({ idWithVersion.id(), idWithVersion.version() });
    }
    FillRedirectResponseInfos(response, needHandleObjs, request.redirect());
    if (response.meta_is_moving()) {
        return;
    }

    for (auto itr = needHandleObjs.begin(); itr != needHandleObjs.end();) {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::const_accessor accessor;
        if (!metaTable_.find(accessor, *itr)) {
            itr = needHandleObjs.erase(itr);
            continue;
        }
        if (objs2Version.at(*itr) < accessor->second.meta.version()) {
            response.add_outdated_ids(*itr);
            itr = needHandleObjs.erase(itr);
            continue;
        }
        ++itr;
    }

    VLOG(1) << "Objs need to be processed: " << VectorToString(needHandleObjs)
            << "; Outdated objs: " << VectorToString(response.outdated_ids());

    DeleteAllCopyMetaReqPb deleteReq;
    for (const auto &needHandleObj : needHandleObjs) {
        auto *id2Version = deleteReq.add_ids_with_version();
        id2Version->set_id(needHandleObj);
        id2Version->set_version(objs2Version.at(needHandleObj));
    }
    if (!needHandleObjs.empty()) {
        deleteReq.set_address(request.address());
        deleteReq.set_redirect(true);
        DeleteAllCopyMetaRspPb deleteRsp;
        DeleteAllCopyMeta(deleteReq, deleteRsp);
        for (const auto &outdatedObj : deleteRsp.outdated_objs()) {
            response.add_outdated_ids(outdatedObj);
        }
        *response.mutable_failed_ids() = { deleteRsp.failed_object_keys().begin(),
                                           deleteRsp.failed_object_keys().end() };
    }
    INJECT_POINT_NO_RETURN("OCMetadataManager.ProcessRemoveMetaNotifyFromOtherAz.ReturnSlowly");
}

Status OCMetadataManager::RemoveMeta(const RemoveMetaReqPb &request, RemoveMetaRspPb &response)
{
    // Validate the input.
    const std::string &address = request.address();
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_INVALID, "RemoveMeta: Cannot RemoveMeta with empty server address.");
    PerfPoint point(PerfKey::MASTER_REMOVE_META_SINGLE);
    switch (request.cause()) {
        case RemoveMetaReqPb_Cause_NORMAL:
        case RemoveMetaReqPb_Cause_EVICTION:
            RemoveMetaLocation(request, address, response, request.version());
            break;
        case RemoveMetaReqPb_Cause_INVALID_BUFFER:
            RemoveMetaForInvalidateBuffer(request, address, response);
            break;
        case RemoveMetaReqPb_Cause_GIVEUP_PRIMARY:
            GiveUpPrimaryLocation(request, address, response);
            break;
        case RemoveMetaReqPb_Cause_OTHER_AZ_META_UPDATE:
            ProcessRemoveMetaNotifyFromOtherAz(request, response);
            break;
        default:
            LOG(WARNING) << "Unsupported type: " << request.cause();
            break;
    }
    point.RecordAndReset(PerfKey::MASTER_REMOVE_META_SINGLE);

    LOG(INFO) << FormatString(
        "RemoveMeta finished, receive id size: %d, success size: %d, need wait size: %d, need data size: %d, failed "
        "size: %d, outdated size: %d",
        request.ids_size() + request.id_with_version_size(), response.success_ids_size(), response.need_wait_ids_size(),
        response.need_data_ids_size(), response.failed_ids_size(), response.outdated_ids_size());
    return Status::OK();
}

bool OCMetadataManager::IsPrimaryCopyWithCopy(const ObjectMeta &meta, const std::string &address)
{
    bool result = meta.IsPrimaryWithoutCopy(address);
    if (result) {
        return true;
    }
    INJECT_POINT("OCMetadataManager.IsPrimaryCopyWithCopy", []() { return false; });
    bool allCopyIsExitingNode = true;
    for (const auto &loc : meta.locations) {
        if (loc != address && !etcdCM_->IsPreLeaving(loc)) {
            allCopyIsExitingNode = false;
            break;
        }
    }
    return allCopyIsExitingNode;
}

void OCMetadataManager::GiveUpPrimaryLocation(const RemoveMetaReqPb &request, const std::string &address,
                                              RemoveMetaRspPb &response)
{
    std::vector<std::string> notRedirectObjectKeys = { request.ids().begin(), request.ids().end() };
    std::unordered_map<std::string, std::unordered_set<std::string>> workerForChangePrimaryIds;
    FillRedirectResponseInfos(response, notRedirectObjectKeys, request.redirect());
    if (response.meta_is_moving()) {
        return;
    }
    LOG(INFO) << FormatString("[Objects %s] Start to give up meta location %s", VectorToString(notRedirectObjectKeys),
                              address);
    std::unordered_set<std::string> needRemoveIds;
    for (const auto &objectKey : notRedirectObjectKeys) {
        if (etcdCM_->CheckLocalNodeIsExiting()) {
            LOG(WARNING) << FormatString("[ObjectKey %s] Node exiting, give up primary failed.", objectKey);
            response.add_failed_ids(objectKey);
            continue;
        }
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        if (!metaTable_.find(accessor, objectKey)) {
            LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objectKey);
            response.add_success_ids(objectKey);
            continue;
        }
        ObjectMeta &meta = accessor->second;
        bool IsPrimaryWithoutCopy = IsPrimaryCopyWithCopy(meta, address);
        VLOG(1) << FormatString("[Objects %s] Write mode: %d, is primary without other: %d", objectKey,
                                meta.meta.config().write_mode(), IsPrimaryWithoutCopy);
        if (meta.IsNoneL2CacheEvict() && IsPrimaryWithoutCopy) {
            (void)metaTable_.erase(accessor);
            needRemoveIds.emplace(objectKey);
            response.add_success_ids(objectKey);
            continue;
        } else if (meta.IsNoneL2Cache() && IsPrimaryWithoutCopy) {
            response.add_need_data_ids(objectKey);
            continue;
        } else if (meta.IsWriteBackL2Cache() && IsPrimaryWithoutCopy) {
            response.add_need_wait_ids(objectKey);
            continue;
        }
        bool foundCopy = false;
        for (const auto &addr : accessor->second.locations) {
            if (addr != address && !etcdCM_->IsPreLeaving(addr)) {
                workerForChangePrimaryIds[addr].insert(objectKey);
                foundCopy = true;
                break;
            }
        }
        if (!foundCopy) {
            response.add_need_data_ids(objectKey);
            continue;
        }
        (void)accessor->second.locations.erase(address);
        (void)objectStore_->RemoveObjectLocation(objectKey, address);
    }
    for (const auto &objectKey : needRemoveIds) {
        LOG_IF_ERROR(objectStore_->RemoveObjectLocation(objectKey, address), "Remove location failed");
        LOG_IF_ERROR(objectStore_->RemoveMeta(objectKey, false), "Remove meta failed");
    }
    SendChangePrimaryCopy(workerForChangePrimaryIds, response);
    if (!workerForChangePrimaryIds.empty()) {
        RetryForFailedIds(workerForChangePrimaryIds, response);
    }
}

void OCMetadataManager::SendChangePrimaryCopy(
    std::unordered_map<std::string, std::unordered_set<std::string>> &workerForChangePrimaryIds, RemoveMetaRspPb &rsp)
{
    for (auto info = workerForChangePrimaryIds.begin(); info != workerForChangePrimaryIds.end();) {
        std::string primaryAddr = info->first;
        std::unordered_set<std::string> successIds;
        auto status = notifyWorkerManager_->SendChangePrimaryCopy(primaryAddr, info->second, successIds);
        INJECT_POINT("SendChangePrimaryCopy.failed", [&status] {
            status = Status(K_RUNTIME_ERROR, "send failed");
            return;
        });
        if (status.IsError()) {
            LOG(ERROR) << "failed to send change primary copy: " << status.ToString();
            info++;
            continue;
        }
        for (const auto &id : successIds) {
            (void)workerForChangePrimaryIds[primaryAddr].erase(id);
            if (ChangePrimaryCopy(primaryAddr, id).IsOk()) {
                rsp.add_success_ids(id);
            } else {
                rsp.add_failed_ids(id);
            }
        }
        if (info->second.empty()) {
            info = workerForChangePrimaryIds.erase(info);
        } else {
            info++;
        }
    }
}

void OCMetadataManager::RetryForFailedIds(
    const std::unordered_map<std::string, std::unordered_set<std::string>> &workerForChangePrimaryIds,
    RemoveMetaRspPb &rsp)
{
    for (const auto &info : workerForChangePrimaryIds) {
        for (const auto &id : info.second) {
            std::unordered_set<ImmutableString> locations;
            {
                std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
                TbbMetaTable::accessor accessor;
                if (!metaTable_.find(accessor, id)) {
                    LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", id);
                    rsp.add_success_ids(id);
                    continue;
                }
                locations = accessor->second.locations;
            }
            bool success = false;
            for (const auto &addr : locations) {
                if (addr == info.first || etcdCM_->IsPreLeaving(addr)) {
                    continue;
                }
                std::unordered_set<std::string> successIds;
                auto injectTest = []() {
                    INJECT_POINT("master.RetryForFailedIds.success", []() { return true; });
                    return false;
                };
                if (injectTest()) {
                    successIds.insert(id);
                } else {
                    (void)notifyWorkerManager_->SendChangePrimaryCopy(addr, { id }, successIds);
                }
                if (successIds.find(id) == successIds.end()) {
                    continue;
                }
                success = ChangePrimaryCopy(addr, id).IsOk();
                break;
            }
            if (success) {
                rsp.add_success_ids(id);
            } else {
                rsp.add_failed_ids(id);
            }
        }
    }
}

Status OCMetadataManager::ChangePrimaryCopy(const std::string &primaryAddr, const std::string &objectKey)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::accessor accessor;
    if (etcdCM_->CheckLocalNodeIsExiting()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Node exiting, change primary copy failed.", objectKey);
        return Status(StatusCode::K_TRY_AGAIN, "Try again");
    }
    if (!metaTable_.find(accessor, objectKey)) {
        LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objectKey);
        return Status::OK();
    }
    accessor->second.meta.set_primary_address(primaryAddr);
    VLOG(1) << objectKey << " change primary copy from worker to: " << primaryAddr;
    std::string serializedStr;
    LOG_IF_ERROR(objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr),
                 "serialize meta to rocksdb failed");
    LOG_IF_ERROR(objectStore_->CreateOrUpdateMeta(objectKey, serializedStr,
                                                  WriteMode2MetaType(accessor->second.meta.config().write_mode())),
                 "Create meta to rocksdb failed");
    return Status::OK();
}

void OCMetadataManager::RemoveMetaLocation(const RemoveMetaReqPb &request, const std::string &address,
                                           RemoveMetaRspPb &response, uint64_t version)
{
    std::vector<std::string> notRedirectObjectKeys = { request.ids().begin(), request.ids().end() };
    FillRedirectResponseInfos(response, notRedirectObjectKeys, request.redirect());
    if (response.meta_is_moving()) {
        return;
    }
    LOG(INFO) << FormatString("[Objects %s] Start to remove meta location %s", VectorToString(notRedirectObjectKeys),
                              address);
    for (const auto &objectKey : notRedirectObjectKeys) {
        if (etcdCM_->CheckLocalNodeIsExiting()) {
            response.add_failed_ids(objectKey);
            LOG(WARNING) << FormatString("[ObjectKey %s] Node exiting, remove meta location failed.", objectKey);
            continue;
        }
        {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::accessor accessor;
            if (!metaTable_.find(accessor, objectKey)) {
                LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objectKey);
                response.add_success_ids(objectKey);
                continue;
            }
            auto latestVersion = accessor->second.meta.version();
            if (version < latestVersion) {
                VLOG(1) << FormatString("Remove meta version: %zu, latest version: %zu", version, latestVersion);
                response.add_success_ids(objectKey);
                continue;
            }
            (void)accessor->second.locations.erase(address);
        }
        (void)objectStore_->RemoveObjectLocation(objectKey, address);
        response.add_success_ids(objectKey);
    }
}

Status OCMetadataManager::RemoveMetaLocation(const std::string &objectKey, const std::string &address, uint64_t version)
{
    VLOG(1) << FormatString("[ObjectKey %s] Start to remove meta location %s", objectKey, address);
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("RemoveMetaLocation get lock", timer.ElapsedMilliSecond());
    TbbMetaTable::accessor accessor;
    if (!metaTable_.find(accessor, objectKey)) {
        LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objectKey);
        return Status::OK();
    }
    auto latestVersion = accessor->second.meta.version();
    if (version < latestVersion) {
        VLOG(1) << FormatString("Remove meta version: %zu, latest version: %zu", version, latestVersion);
        return Status::OK();
    }
    (void)accessor->second.locations.erase(address);
    (void)objectStore_->RemoveObjectLocation(objectKey, address);
    return Status::OK();
}

void OCMetadataManager::RemoveMetaForInvalidateBuffer(const RemoveMetaReqPb &request, const std::string &address,
                                                      RemoveMetaRspPb &response)
{
    std::vector<std::string> notRedirectObjectKeys = { request.ids().begin(), request.ids().end() };
    std::unordered_map<std::string, std::unordered_set<std::string>> toBeChanged;
    FillRedirectResponseInfos(response, notRedirectObjectKeys, request.redirect());
    if (response.meta_is_moving()) {
        return;
    }
    LOG(INFO) << FormatString("[Objects %s] Start to remove meta for invalidating buffer operation",
                              VectorToString(notRedirectObjectKeys));
    for (const auto &objectKey : notRedirectObjectKeys) {
        {
            Timer timer;
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            masterOperationTimeCost.Append("RemoveMetaForInvalidateBuffer get lock", timer.ElapsedMilliSecond());
            TbbMetaTable::accessor accessor;
            if (!metaTable_.find(accessor, objectKey)) {
                LOG(ERROR) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objectKey);
                continue;
            }

            // If multiple workers hold data and the current node is primary data,
            // we need to select a new primary worker.
            if (accessor->second.locations.size() <= 1 || accessor->second.meta.primary_address() != address) {
                response.add_success_ids(objectKey);
                continue;
            }
            (void)accessor->second.locations.erase(address);

            if (objectStore_->RemoveObjectLocation(objectKey, address).IsError()) {
                LOG(ERROR) << FormatString("[ObjectKey %s] Remove location failed from rocksdb.", objectKey);
                continue;
            }

            std::string newPrimaryCopy;
            Status rc = ReselectPrimaryCopy(objectKey, {}, accessor, newPrimaryCopy);
            if (rc.GetCode() == K_UNKNOWN_ERROR) {
                LOG(ERROR) << FormatString("[ObjectKey %s] reselect primary copy failed, ignore it.", objectKey);
                continue;
            }
            toBeChanged[newPrimaryCopy].emplace(objectKey);
            if (!metaTable_.find(accessor, objectKey)) {
                LOG(ERROR) << FormatString(
                    "[ObjectKey %s] The object key does not exist in id2location "
                    "when update new primary copy address.",
                    objectKey);
                continue;
            }
            accessor->second.meta.set_primary_address(newPrimaryCopy);
            accessor.release();
        }
        response.add_success_ids(objectKey);
        notifyWorkerManager_->AsyncChangePrimaryCopy(toBeChanged);
    }
}

void OCMetadataManager::TransferSyncDeleteRequest(
    DeleteObjectMediator &deleteMediator, DeleteAllCopyMetaRspPb &response,
    const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi)
{
    LOG(INFO) << "Transfer to delete threads to notify worker delete";
    auto ids = deleteMediator.GetObjKeys();
    if (!AddHeavyOp(ids)) {
        std::unordered_set<std::string> failedIds{ ids.begin(), ids.end() };
        SetDeleteAllCopyMetaRspPb(Status(StatusCode::K_WORKER_DEADLOCK, "retry"), failedIds, response);
        LOG_IF_ERROR(serverApi->Write(response), "Write reply to client stream failed.");
        return;
    }
    std::string traceID = Trace::Instance().GetTraceID();
    int64_t timeout = timeoutDuration.CalcRealRemainingTime();
    Timer timer;
    asyncTaskPool_->Execute([this, deleteMediator, serverApi, response, traceID, timer, timeout]() mutable {
        auto ids = deleteMediator.GetObjKeys();
        Raii raii([&ids, this]() { RemoveHeavyOp(ids); });
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "RPC timeout. time elapsed " << elapsed << ", timeout:" << timeout
                       << ", DeleteAllCopy threads Statistics: " << asyncTaskPool_->GetStatistics();
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
            return;
        }
        timeoutDuration.Init(timeout - elapsed);
        NotifyDeleteAndClearMeta(deleteMediator, false);
        SetDeleteAllCopyMetaRspPb(deleteMediator.GetStatus(), deleteMediator.GetFailedObjs(), response);
        LOG_IF_ERROR(serverApi->Write(response), "Write reply to client stream failed.");
        LOG(INFO) << "DeleteAllCopyMeta send response to worker finished, objectKeys: "
                  << VectorToString(deleteMediator.GetObjKeys());
    });
}

void OCMetadataManager::ProcessForwardDeleteAllCopyMetaInCurrAz(
    const std::string &otherAZName,
    const std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster,
    std::unordered_set<std::string> &objsNeedTryInOtherAz,
    std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify,
    DeleteObjectMediator &deleteMediator)
{
    for (auto &item : objKeysGrpByMaster) {
        HostPort masterHostPort = item.first.GetAddressAndSaveDbName();
        std::unordered_set<std::string> failedObjsInThisLoop;
        std::unordered_set<std::string> objsWithoutMetaInThisLoop;
        auto rc = notifyWorkerManager_->NotifyMasterDeleteAllCopyMeta(masterHostPort, item.second, failedObjsInThisLoop,
                                                                      objsWithoutMetaInThisLoop);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString(
                "Fail to notify other az[%s]'s node[%s] to delete all copy meta, rc: %s, objs: %s", otherAZName,
                masterHostPort.ToString(), rc.ToString(), VectorToString(item.second));
            for (const auto &objectKey : item.second) {
                objsNeedAsyncNotify[objectKey].emplace_back(otherAZName);
            }
            objsNeedTryInOtherAz.insert(std::make_move_iterator(item.second.begin()),
                                        std::make_move_iterator(item.second.end()));
            continue;
        }
        // The failed objs means that there is meta on this az; now that we have found the metadata, we do not
        // need to go to other az to retry this key; but we must remember to return this failure message.
        deleteMediator.AddFailedDelIds(failedObjsInThisLoop);
        // Some metadata cannot be found in this az. Record these keys and continue trying in the next az.
        objsNeedTryInOtherAz.insert(std::make_move_iterator(objsWithoutMetaInThisLoop.begin()),
                                    std::make_move_iterator(objsWithoutMetaInThisLoop.end()));
    }
}

void OCMetadataManager::ForwardDeleteAllCopyMeta2OtherAz(std::unordered_set<std::string> &&objsNeedTryInOtherAz,
                                                         DeleteObjectMediator &deleteMediator)
{
    // The current target node may fail, so some notifications need to be asynchronous. <objectKey, azName>
    std::unordered_map<std::string, std::vector<std::string>> objsNeedAsyncNotify;
    const std::vector<std::string> &otherAZNames = etcdCM_->GetOtherAzNames();
    for (const auto &otherAZName : otherAZNames) {
        std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMaster;
        std::vector<std::string> groupFailedObjs;
        auto rc = etcdCM_->GroupHashObjsInGivenOtherAz(otherAZName, objsNeedTryInOtherAz, objKeysGrpByMaster,
                                                       groupFailedObjs);
        // This means that the hash ring of the az cannot be found. This may only happen when the corresponding az is
        // started for the first time. In this case, just skip the az.
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("Group hash objs in az[%s] failed, rc: %s", otherAZName, rc.ToString());
            continue;
        }
        objsNeedTryInOtherAz.clear();
        // If the group process fails, it means that the target node in the az may have failed. In this case, an
        // asynchronous retry is required.
        for (const auto &objectKey : groupFailedObjs) {
            objsNeedAsyncNotify[objectKey].emplace_back(otherAZName);
        }
        objsNeedTryInOtherAz.insert(std::make_move_iterator(groupFailedObjs.begin()),
                                    std::make_move_iterator(groupFailedObjs.end()));

        ProcessForwardDeleteAllCopyMetaInCurrAz(otherAZName, objKeysGrpByMaster, objsNeedTryInOtherAz,
                                                objsNeedAsyncNotify, deleteMediator);

        // Clean up redundant asynchronous notifications.
        if (objsNeedTryInOtherAz.empty()) {
            objsNeedAsyncNotify.clear();
            break;
        }
        for (auto iter = objsNeedAsyncNotify.begin(); iter != objsNeedAsyncNotify.end();) {
            if (objsNeedTryInOtherAz.find(iter->first) == objsNeedTryInOtherAz.end()) {
                iter = objsNeedAsyncNotify.erase(iter);
            } else {
                ++iter;
            }
        }
    }

    AsyncNotifyCrossAzDelete(objsNeedAsyncNotify);
}

void OCMetadataManager::AsyncNotifyCrossAzDelete(
    const std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify)
{
    std::stringstream asyncNotifyMsg;
    auto deleteAllCopyMetaVersion = static_cast<int64_t>(GetSystemClockTimeStampUs());
    for (auto &kv : objsNeedAsyncNotify) {
        // 1. delete l2 cache at first, in case a get operation later retrive data from l2 cache
        Status status = globalCacheDeleteManager_->InsertDeletedObject(
            kv.first, UINT64_MAX, deleteAllCopyMetaVersion, true, ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD);
        if (status.IsError()) {
            LOG(ERROR) << FormatString("[ObjectKey %s] Global cache delete failed, error: %s", kv.first,
                                       status.ToString());
        }
        // 2. insert an async notification
        asyncNotifyMsg << kv.first << ",";
        NotifyWorkerOp op;
        op.type = NotifyWorkerOpType::DELETE_ALL_COPY_META;
        op.deleteAllCopyMetaAzNames.insert(std::make_move_iterator(kv.second.begin()),
                                           std::make_move_iterator(kv.second.end()));
        op.deleteAllCopyMetaVersion = deleteAllCopyMetaVersion;
        // In this case, the type of persistence cannot be determined.
        LOG_IF_ERROR(notifyWorkerManager_->InsertAsyncWorkerOp("", kv.first, op, true), "");
    }
    LOG_IF(INFO, !objsNeedAsyncNotify.empty())
        << "objs need to async notify delete all copy meta: " << asyncNotifyMsg.str();
}

void OCMetadataManager::ProcessHashObjsWithoutMetaWhenDeleteAllCopyMeta(
    const DeleteAllCopyMetaReqPb &request, std::unordered_set<std::string> &&hashObjsWithoutMeta,
    DeleteAllCopyMetaRspPb &response, DeleteObjectMediator &deleteMediator)
{
    if (request.need_forward_objs_without_meta()) {
        LOG(INFO) << "Notify nodes in other clusters to delete all copy meta for object: "
                  << VectorToString(hashObjsWithoutMeta);
        ForwardDeleteAllCopyMeta2OtherAz(std::move(hashObjsWithoutMeta), deleteMediator);
    } else {
        *response.mutable_objs_without_meta() = { hashObjsWithoutMeta.begin(), hashObjsWithoutMeta.end() };
    }
}

void OCMetadataManager::DeleteAllCopyMetaImpl(
    const DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &response,
    const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi,
    bool needReleaseRpc)
{
    const std::string &sourceWorker = request.address();
    std::vector<std::string> objectKeys = { request.object_keys().begin(), request.object_keys().end() };
    std::unordered_map<std::string, uint64_t> objKey2Version;
    for (const auto &objWithVersion : request.ids_with_version()) {
        objectKeys.emplace_back(objWithVersion.id());
        objKey2Version.emplace(objWithVersion.id(), objWithVersion.version());
    }
    if (sourceWorker.empty()) {
        SetDeleteAllCopyMetaRspPb({ K_INVALID, "Cannot RemoveMeta with empty server address." }, objectKeys, response);
        if (serverApi != nullptr) {
            LOG_IF_ERROR(serverApi->Write(response), "Write reply to client stream failed.");
        }
        return;
    }
    FillRedirectResponseInfos(response, objectKeys, request.redirect());
    VLOG(1) << "DeleteAllCopyMeta begin, sourceWorker: " << sourceWorker;
    std::unordered_map<std::string, bool> requestObjKeyMap;
    std::transform(objectKeys.begin(), objectKeys.end(), std::inserter(requestObjKeyMap, requestObjKeyMap.end()),
                   [](auto &objectKey) { return std::make_pair(objectKey, true); });
    DeleteObjectMediator deleteMediator(sourceWorker, requestObjKeyMap);
    deleteMediator.SetObjKey2Version(std::move(objKey2Version));
    if (FLAGS_async_delete && request.need_forward_objs_without_meta()) {
        AsyncDeleteByExpired(deleteMediator);
    } else {
        FindNeedDeleteIds(deleteMediator);
        std::unordered_set<std::string> hashObjsWithoutMeta = deleteMediator.GetHashObjsWithoutMeta();
        ProcessHashObjsWithoutMetaWhenDeleteAllCopyMeta(request, std::move(hashObjsWithoutMeta), response,
                                                        deleteMediator);
        bool processFinished = deleteMediator.CheckNoNeedToNotifyWorker();
        if (processFinished || !needReleaseRpc || serverApi == nullptr) {
            NotifyDeleteAndClearMeta(deleteMediator, false);
            // Note that if we want to set this field, the request cannot be returned asynchronously. We need to check
            // the version again when we actually delete the metadata.
            *response.mutable_outdated_objs() = { deleteMediator.GetOutdatedObjs().begin(),
                                                  deleteMediator.GetOutdatedObjs().end() };
        } else {
            TransferSyncDeleteRequest(deleteMediator, response, serverApi);
            return;
        }
    }
    if (!deleteMediator.GetStatus().IsOk()) {
        LOG(ERROR) << "Delete failed with error: " << deleteMediator.GetStatus().ToString();
    } else {
        LOG(INFO) << "Delete success";
    }
    SetDeleteAllCopyMetaRspPb(deleteMediator.GetStatus(), deleteMediator.GetFailedObjs(), response);
    if (serverApi != nullptr) {
        LOG_IF_ERROR(serverApi->Write(response), "Write reply to client stream failed.");
    }
}

void OCMetadataManager::DeleteAllCopyMeta(const DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &response)
{
    // create a empty serverApi and don't use it.
    std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> serverApi;
    DeleteAllCopyMetaImpl(request, response, serverApi, false);
}

void OCMetadataManager::DeleteAllCopyMetaWithServerApi(
    const DeleteAllCopyMetaReqPb &request,
    const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi)
{
    DeleteAllCopyMetaRspPb response;
    DeleteAllCopyMetaImpl(request, response, serverApi, true);
}

void OCMetadataManager::FindNeedDeleteIds(DeleteObjectMediator &delMediator)
{
    std::vector<std::string> objectKeys;
    for (const auto &kv : delMediator.GetReqIdsMap()) {
        if (kv.second) {
            objectKeys.emplace_back(kv.first);
        }
    }
    std::unordered_set<std::string> needDeleteIds;
    std::vector<uint32_t> refCounts;
    globalRefTable_->GetRefWorkerCounts(objectKeys, refCounts);
    for (size_t index = 0; index < objectKeys.size(); index++) {
        auto &objectKey = objectKeys[index];
        if ((refCounts[index] != 0) || !nestedRefManager_->CheckIsNoneNestedRefById(objectKey)) {
            delMediator.AddFailedDelId(objectKey);
            LOG(ERROR) << FormatString("[ObjKey %s] Object reference count not 0", objectKey);
            Status rc = { K_RUNTIME_ERROR, "Object global reference count not 0" };
            delMediator.SetStatus(rc);
            continue;
        }
        needDeleteIds.insert(objectKey);
    }
    std::unordered_map<std::string, DeleteStruct> sendAllDelObjs;
    std::vector<std::string> toBeNotifiedNestedRefs;
    GetAndClearAllUnKeepMetas(needDeleteIds, sendAllDelObjs, toBeNotifiedNestedRefs, delMediator);
    // Not send notify to source worker.
    for (const auto &kv : delMediator.GetReqIdsMap()) {
        auto it = sendAllDelObjs.find(kv.first);
        if (it != sendAllDelObjs.end()) {
            (void)it->second.locations.erase(delMediator.GetSourceWorker());
        }
    }
    delMediator.SetIdsNeedToNotifyWorker(std::move(sendAllDelObjs));
    delMediator.SetToBeNotifiedNestedRefs(std::move(toBeNotifiedNestedRefs));
}

void OCMetadataManager::NotifyDeleteAndClearMeta(DeleteObjectMediator &delMediator, bool isExpired)
{
    std::unordered_set<std::string> failedNotifyObjects;
    const auto &sendAllDelObjs = delMediator.GetIdsNeedToNotifyWorker();
    INJECT_POINT_NO_RETURN("NotifyDeleteAndClearMeta");
    Status lastErr = NotifyWorkerDelete(delMediator.GetSourceWorker(), sendAllDelObjs, false, failedNotifyObjects);
    Raii removeIsDeletingObjs([&sendAllDelObjs, this]() {
        for (const auto &info : sendAllDelObjs) {
            std::lock_guard<std::shared_mutex> l(isDeletingObjMutex_);
            isDeletingObjs_.erase(info.first);
        }
    });
    delMediator.SetStatusIfError(lastErr);
    INJECT_POINT("master.before_delete_metadata", []() { return; });
    INJECT_POINT_NO_RETURN("OCMetadataManager.NotifyDeleteAndClearMeta.ProcessSlowly");
    Status status = ClearMetaInfo(sendAllDelObjs, isExpired, failedNotifyObjects, delMediator);
    // Don't overwrite the previous error: K_WORKER_DEADLOCK
    if (status.IsError() && lastErr.GetCode() != K_WORKER_DEADLOCK) {
        LOG(ERROR) << "Notify worker delete failed. " << status.ToString();
        delMediator.SetStatus(status);
    }
    auto toBeNotifiedNestedRefs = delMediator.GetToBeNotifiedNestedRefs();
    if (!toBeNotifiedNestedRefs.empty()) {
        notifyWorkerManager_->AsyncDecNestedRefs(delMediator.GetSourceWorker(), toBeNotifiedNestedRefs);
    }

    for (const auto &kv : sendAllDelObjs) {
        const auto &objectKey = kv.first;
        if (failedNotifyObjects.count(objectKey) > 0) {
            // return the failed object key exists in request.
            if (delMediator.GetReqIdsMap().count(objectKey) > 0) {
                delMediator.AddFailedDelId(objectKey);
            }
        } else {
            delMediator.AddSuccessDelId(objectKey);
        }
    }
}

void OCMetadataManager::GetAndClearAllUnKeepMetas(const std::unordered_set<std::string> &objectKeys,
                                                  std::unordered_map<std::string, DeleteStruct> &sendAllReplicas,
                                                  std::vector<std::string> &toBeNotifiedNestedRefs,
                                                  DeleteObjectMediator &delMediator)
{
    std::vector<std::string> finalDeadObjects;
    LOG_IF_ERROR(BFSGetDeadObjects(objectKeys, finalDeadObjects, toBeNotifiedNestedRefs),
                 "Fail with BFSGetDeadObjects");

    finalDeadObjects.insert(finalDeadObjects.end(), objectKeys.begin(), objectKeys.end());

    for (const auto &deadId : finalDeadObjects) {
        {
            std::lock_guard<std::shared_mutex> lck(isDeletingObjMutex_);
            isDeletingObjs_.emplace(deadId);
        }
        auto &replicas = sendAllReplicas[deadId];
        LOG_IF_ERROR(GetMetaInfoAndSetDeleting(deadId, replicas, delMediator), "Get meta info failed");
    }
}

Status OCMetadataManager::ClearMetaInfo(const std::unordered_map<std::string, DeleteStruct> &sendAllDelObjs,
                                        bool isExpired, std::unordered_set<std::string> &failedObjects,
                                        DeleteObjectMediator &delMediator)
{
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("ClearMetaInfo get lock", timer.ElapsedMilliSecond());
    Status lastErr;
    for (auto const &info : sendAllDelObjs) {
        auto &objectKey = info.first;
        // isExpired is true: delete global cache for failed object.
        // isExpired is false: not delete global cache for failed object.
        if (!isExpired && failedObjects.count(objectKey) > 0) {
            LOG(ERROR) << FormatString("[ObjectKey %s] worker delete object failed", objectKey);
            continue;
        }

        TbbMetaTable::const_accessor accessor;
        if (!metaTable_.find(accessor, objectKey)) {
            LOG(ERROR) << "Meta not exist, objectKey:" << objectKey;
            // metadata is not present in master. If metadata is not stored in etcd,
            // try to delete all versions of the object from L2 Cache using async delete.
            if (!FLAGS_oc_io_from_l2cache_need_metadata) {
                auto maxVersionToDel = static_cast<int64_t>(GetSystemClockTimeStampUs());
                (void)globalCacheDeleteManager_->InsertDeletedObject(objectKey, UINT64_MAX, maxVersionToDel);
            }
            continue;
        } else if (accessor->second.meta.version() > static_cast<uint64_t>(info.second.version)) {
            LOG(INFO) << FormatString("[ObjectKey %s] Has re-set, metadata not need to be cleared.", objectKey);
            delMediator.SetOutdatedObj(objectKey);
            continue;
        } else if (accessor->second.multiSetState == PENDING) {
            LOG(INFO) << FormatString("[ObjectKey %s] is creating, metadata not need to be cleared.", objectKey);
            continue;
        }

        // 1. async delete global cache.
        if (accessor->second.HasL2Cache() && accessor->second.IsBinary()) {
            uint64_t objectVersion = accessor->second.meta.version();
            uint64_t delVersion = objectVersion;
            Status status = globalCacheDeleteManager_->InsertDeletedObject(
                objectKey, objectVersion, delVersion, true,
                WriteMode2MetaType(accessor->second.meta.config().write_mode()));
            if (status.IsError()) {
                LOG(ERROR) << FormatString("[ObjectKey %s] Global cache delete failed, error: %s", objectKey,
                                           status.ToString());
                lastErr = status;
                failedObjects.insert(objectKey);
                continue;
            }
        }

        if (isExpired && failedObjects.count(objectKey) > 0) {
            LOG(ERROR) << FormatString("[ObjectKey %s] worker delete object failed", objectKey);
            continue;
        }

        // 2. delete meta info
        Status rc = ClearOneMetaInfo(accessor);
        if (rc.IsError()) {
            lastErr = rc;
            failedObjects.insert(objectKey);
            continue;
        }
        (void)metaTable_.erase(accessor);
        if (!isExpired) {
            (void)expiredObjectManager_->RemoveObjectIfExist(objectKey);
        }
    }
    return lastErr;
}

Status OCMetadataManager::ClearOneMetaInfo(const TbbMetaTable::const_accessor &accessor, bool isDataMigration)
{
    const auto &objectKey = accessor->first;
    // remove object location.
    for (const auto &address : accessor->second.locations) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->RemoveObjectLocation(objectKey, address),
                                         FormatString("[ObjectKey %s] RemoveObjectLocation failed", objectKey));
    }
    // remote meta info
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->RemoveMeta(objectKey, !isDataMigration),
                                     FormatString("[ObjectKey %s] RemoveMeta failed", objectKey));
    return Status::OK();
}

Status OCMetadataManager::BFSGetDeadObjects(const std::unordered_set<std::string> &beginDeadObject,
                                            std::vector<std::string> &finalDeadObjects,
                                            std::vector<std::string> &toBeNotifiedNestedRefs)
{
    std::vector<std::string> zeroRefIds;
    for (const auto &objKey : beginDeadObject) {
        LOG_IF_ERROR(nestedRefManager_->DecreaseNestedRefCnt(objKey, zeroRefIds), "DecreaseNestedRefCnt failed");
    }

    std::unordered_set<std::string> deadObject;
    std::vector<std::string> tempToBeNotifiedNestedRefs;
    std::vector<uint32_t> refCounts;
    globalRefTable_->GetRefWorkerCounts(zeroRefIds, refCounts);
    {
        Timer timer;
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        masterOperationTimeCost.Append("BFSGetDeadObjects get lock", timer.ElapsedMilliSecond());
        for (size_t index = 0; index < zeroRefIds.size(); index++) {
            auto &objectKey = zeroRefIds[index];
            TbbMetaTable::const_accessor accessor;
            auto found = metaTable_.find(accessor, objectKey);
            if (found && refCounts[index] == 0) {
                deadObject.emplace(objectKey);
            }
            if (!found) {
                LOG(WARNING) << FormatString("Object %s does not exist, should be erased", objectKey);
                deadObject.emplace(objectKey);

                // If objectKey is not found in metaTable_ we assume that its owned by a different master
                // ToDo: Handle the case where objectKey belongs to the master but no entry in metaTable_
                // Previous behaviour if object is not found in meta table add it to deadObject list
                tempToBeNotifiedNestedRefs.emplace_back(objectKey);
            }
        }
    }
    if (!deadObject.empty()) {
        LOG_IF_ERROR(BFSGetDeadObjects(deadObject, finalDeadObjects, toBeNotifiedNestedRefs),
                     "BFSGetDeadObjects failed");
    }
    (void)finalDeadObjects.insert(finalDeadObjects.end(), deadObject.begin(), deadObject.end());
    (void)toBeNotifiedNestedRefs.insert(toBeNotifiedNestedRefs.end(), tempToBeNotifiedNestedRefs.begin(),
                                        tempToBeNotifiedNestedRefs.end());

    LOG_IF(INFO, !finalDeadObjects.empty())
        << "BFSGetDeadObjects[%s] to be deleted" << VectorToString(finalDeadObjects);
    LOG_IF(INFO, !tempToBeNotifiedNestedRefs.empty())
        << "BFSGetDeadObjects[%s] to be notified" << VectorToString(tempToBeNotifiedNestedRefs);
    return Status::OK();
}

Status OCMetadataManager::GetMetaInfoAndSetDeleting(const std::string &objectKey, DeleteStruct &replicas,
                                                    DeleteObjectMediator &delMediator)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::const_accessor accessor;
    if (!metaTable_.find(accessor, objectKey)) {
        LOG(INFO) << FormatString("[ObjectKey %s] Object does not exist", objectKey);
        if (!HasWorkerId(objectKey)) {
            delMediator.AddHashObjsWithoutMeta(objectKey);
        }
        return Status::OK();
    } else if (accessor->second.multiSetState == PENDING) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_NOT_FOUND, "Object does not exist, multi object is creating");
    }

    if (delMediator.CheckIfExpired(objectKey, accessor->second.meta.version())) {
        LOG(INFO) << FormatString("The version[%lld] of object[%s] in request is outdated, current version: %lld",
                                  delMediator.GetObjectVersionInRequest(objectKey), objectKey,
                                  accessor->second.meta.version());
        delMediator.SetOutdatedObj(objectKey);
        return Status::OK();
    }

    for (const auto &address : accessor->second.locations) {
        replicas.locations.emplace(address);
    }
    auto version = delMediator.GetObjectVersionInRequest(objectKey);
    replicas.version = version == -1 ? accessor->second.meta.version() : static_cast<uint64_t>(version);
    replicas.writeMode = accessor->second.meta.config().write_mode();
    return Status::OK();
}

Status OCMetadataManager::NotifyWorkerDelete(const std::string &sourceWorker,
                                             const std::unordered_map<std::string, DeleteStruct> &sendAllDelObjs,
                                             bool isAsync, std::unordered_set<std::string> &failedObjects)
{
    VLOG(1) << "NotifyWorkerDelete begin";
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<int64_t, uint32_t>>> replicas2Obj;
    for (const auto &kv : sendAllDelObjs) {
        const auto &objectKey = kv.first;
        const auto &replicas = kv.second;
        for (const auto &loc : replicas.locations) {
            (void)replicas2Obj[loc].emplace(objectKey, std::make_pair(replicas.version, replicas.writeMode));
        }
    }

    int64_t timeoutMs = timeoutDuration.CalcRealRemainingTime();
    INJECT_POINT("OCMetadataManager.NotifyWorkerDelete.timeoutMs", [&timeoutMs](int time) {
        timeoutMs = time;
        return Status::OK();
    });

    Status status = RetryOnErrorRepent(
        timeoutMs,
        [this, &sourceWorker, &replicas2Obj, isAsync, &failedObjects](int32_t) {
            return notifyWorkerManager_->DoNotifyWorkerDelete(sourceWorker, replicas2Obj, isAsync, failedObjects);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });

    for (const auto &rpcFailedItem : replicas2Obj) {
        // rpc failed, actually it can't determine whether worker process success or not, in this scenario, we always
        // consider the worker failed to process, the datasystem user client can try again.
        std::transform(rpcFailedItem.second.begin(), rpcFailedItem.second.end(),
                       std::inserter(failedObjects, failedObjects.end()), [](auto &item) { return item.first; });
    }

    LOG(INFO) << "Notify delete object done with status: " << status.ToString();
    return status;
}

Status OCMetadataManager::DoBinaryCacheInvalidationUnlocked(const std::string &objectKey, ObjectMeta &prevMeta,
                                                            const ChangedMeta &changedMeta)
{
    INJECT_POINT("master.cache_invalid_failed");
    // Step 1: Notify worker to update.
    if (prevMeta.IsReplica()) {
        VLOG(1) << "Object " << objectKey << " is replica, no need to invalid cache";
    } else if (prevMeta.IsCausal()
               || changedMeta.newLifeState == static_cast<uint32_t>(ObjectLifeState::OBJECT_SEALED)) {
        RETURN_IF_NOT_OK(notifyWorkerManager_->SyncSendUpdateObject(objectKey, changedMeta.newVersion,
                                                                    changedMeta.newAddress, prevMeta,
                                                                    ObjectLifeState(changedMeta.newLifeState), {}));
    } else if (prevMeta.IsPram()) {
        RETURN_IF_NOT_OK(notifyWorkerManager_->AsyncSendUpdateObject(objectKey, changedMeta.newAddress, prevMeta));
    }
    // Step 2: Update local meta.
    (void)prevMeta.locations.emplace(changedMeta.newAddress);
    prevMeta.meta.set_version(changedMeta.newVersion);
    prevMeta.meta.set_life_state(changedMeta.newLifeState);
    prevMeta.meta.set_primary_address(changedMeta.newAddress);
    prevMeta.meta.set_data_size(changedMeta.newDataSz);
    prevMeta.meta.mutable_device_info()->clear_blob_sizes();
    prevMeta.meta.mutable_device_info()->mutable_blob_sizes()->Add(changedMeta.newBlobSizes.begin(),
                                                                   changedMeta.newBlobSizes.end());
    std::string serializedStr;
    RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objectKey, prevMeta.meta, serializedStr));
    RETURN_IF_NOT_OK(objectStore_->CreateOrUpdateMeta(objectKey, serializedStr,
                                                      WriteMode2MetaType(prevMeta.meta.config().write_mode())));
    return Status::OK();
}

Status OCMetadataManager::UpdateMetaByState(const UpdateMetaReqPb &request, ObjectMeta &objectMeta,
                                            UpdateMetaRspPb &response)
{
    const std::string &objectKey = request.object_key();
    const std::string &address = request.address();
    const std::set<ImmutableString> nestedObjectKeys = { request.nested_keys().begin(), request.nested_keys().end() };
    if (objectMeta.IsBinary()) {
        auto &newMeta = request.binary_format_params();
        RETURN_IF_NOT_OK(CheckBinaryFormatParamMatch(
            objectKey, objectMeta,
            BinaryFormatParamsStruct{ newMeta.write_mode(), newMeta.data_format(), newMeta.consistency_type(),
                                      newMeta.cache_type(), false },
            nestedObjectKeys));
    }
    int64_t version = GetSystemClockTimeStampUs();

    RaiiPlus raiiP;
    if (!HasWorkerId(objectKey)) {
        MarkUpdatingAndUpdateRemoveMetaNotification(objectKey, version, raiiP);
    }

    response.set_version(version);
    Status s = DoBinaryCacheInvalidationUnlocked(
        objectKey, objectMeta,
        ChangedMeta{
            address, static_cast<int64_t>(response.version()), request.data_size(), request.life_state(), {} });
    if (s.IsError()) {
        LOG(ERROR) << "DoBinaryCacheInvalidationUnlocked failed, status : " << s.ToString();
        // If the cache invalid processing fails, delete the address from the meta.
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->RemoveObjectLocation(objectKey, address),
                                         "Remove location failed from rocksdb.");
        (void)objectMeta.locations.erase(address);
        return s;
    }

    if (nestedRefManager_->IsNestedKeysDiff(objectKey, nestedObjectKeys)) {
        RETURN_IF_NOT_OK(nestedRefManager_->IncreaseNestedRefCnt(objectKey, nestedObjectKeys));
    }
    LOG(INFO) << "UpdateMeta finished";
    return expiredObjectManager_->InsertObject(objectKey, version, request.ttl_second());
}

Status OCMetadataManager::UpdateMeta(const UpdateMetaReqPb &request, UpdateMetaRspPb &response)
{
    INJECT_POINT("master.UpdateMeta");
    const std::string &objectKey = request.object_key();
    const std::string &address = request.address();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!objectKey.empty() && !address.empty(), K_INVALID,
                                         "UpdateMeta: Cannot UpdateMeta with empty objectKey or server address.");
    bool redirect = request.redirect();
    FillRedirectResponseInfo(response, objectKey, redirect);
    RETURN_OK_IF_TRUE(redirect);

    RETURN_IF_NOT_OK(expiredObjectManager_->RemoveObjectIfExist(objectKey));
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("UpdateMeta get lock", timer.ElapsedMilliSecond());
    VLOG(1) << FormatString("Update start: objectKey: %s, worker address: %s", request.object_key(), request.address());
    TbbMetaTable::accessor accessor;
    CHECK_FAIL_RETURN_STATUS(metaTable_.find(accessor, objectKey), StatusCode::K_NOT_FOUND,
                             FormatString("[ObjectKey %s] does not exist", objectKey));
    ObjectMeta &objectMeta = accessor->second;
    CHECK_FAIL_RETURN_STATUS(
        objectMeta.multiSetState != PENDING, K_TRY_AGAIN,
        FormatString("update meta failed, multi meta objectKey(%s) is creating, wait and try again", objectKey));
    if (objectMeta.IsCausal() && !AddHeavyOp(objectKey)) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_WORKER_DEADLOCK, "retry");
    }
    Raii raii([this, &objectKey]() { RemoveHeavyOp({ objectKey }); });

    const std::set<ImmutableString> nestedObjectKeys = { request.nested_keys().begin(), request.nested_keys().end() };

    return UpdateMetaByState(request, objectMeta, response);
}

Status OCMetadataManager::RecoverObjectLocations(
    const std::unordered_map<std::string, std::vector<std::string>> &objLocMap)
{
    INJECT_POINT("OCNotifyWorkerManager.NoNeedRecoveryMeta");
    auto workers = etcdCM_->GetValidWorkersInHashRing();
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    for (const auto &it : objLocMap) {
        const std::string &objKey = it.first;
        const std::vector<std::string> &locations = it.second;
        TbbMetaTable::accessor accessor;
        if (!metaTable_.find(accessor, objKey)) {
            continue;
        }
        for (const auto &loc : locations) {
            if (workers.find(loc) != workers.end()) {
                LOG_IF_ERROR(AddLocation(accessor->second, loc, objKey, accessor->second.meta), "Add location failed.");
            } else {
                accessor->second.locations.erase(loc);
                (void)objectStore_->RemoveObjectLocation(objKey, loc);
            }
        }
    }
    return Status::OK();
}

Status OCMetadataManager::LoadObjectLocations(bool isFromRocksdb,
                                              std::unordered_map<std::string, std::vector<std::string>> &objLocMap)
{
    INJECT_POINT("OCNotifyWorkerManager.NoNeedRecoveryMeta");
    std::vector<std::pair<std::string, std::string>> objectLocations;
    if (isFromRocksdb) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetAllFromRocks(LOCATION_TABLE, objectLocations),
                                         "Load object location from rocksdb into memory failed.");
    }
    for (auto &info : objectLocations) {
        // key format: WorkerAddr_ObjectKey
        std::string::size_type pos = info.first.find("_", 0);
        if (pos != info.first.npos) {
            objLocMap[info.first.substr(pos + 1)].push_back(info.first.substr(0, pos));
        }
    }
    return Status::OK();
}

std::string OCMetadataManager::SelectPrimaryCopyWhenScaleIn(
    const std::string &objectKey, const std::string &primaryAddress,
    const std::unordered_map<std::string, std::vector<std::string>> &objLocMap)
{
    INJECT_POINT("master.SelectPrimaryCopy", [this] { return masterAddress_; });
    auto failedWorkers = etcdCM_->GetFailedWorkers();
    // case 1: if old primary copy is alive, return old primary copy address
    if (failedWorkers.find(primaryAddress) == failedWorkers.end()) {
        return primaryAddress;
    }
    // case 2: if old primary copy is dead, reselect new primary copy address from locations. If there is no location
    // or all locations are failed nodes, return current meta node address as primary copy address.
    auto it = objLocMap.find(objectKey);
    if (it == objLocMap.end()) {
        return masterAddress_;
    }
    for (auto addr : it->second) {
        if (failedWorkers.find(addr) == failedWorkers.end()) {
            VLOG(1) << "old primaryAddr:" << primaryAddress << " new primaryAddr:" << addr;
            return addr;
        }
    }
    return masterAddress_;
}

void OCMetadataManager::InsertExpireObjects(ObjectMetaPb &metaPb,
                                            std::vector<std::tuple<std::string, uint64_t, uint32_t>> &expireObjects)
{
    if (metaPb.ttl_second() > 0) {
        INJECT_POINT("master.LoadMeta.steadyClockIsDifferent", [&metaPb]() { metaPb.set_version(0); });
        long curSystemClock = GetSystemClockTimeStampUs();
        expireObjects.emplace_back(metaPb.object_key(), curSystemClock, metaPb.ttl_second());
    }
}

Status OCMetadataManager::HandleLoadMeta(std::vector<std::pair<std::string, std::string>> &metas,
                                         std::vector<std::tuple<std::string, uint64_t, uint32_t>> &expireObjects,
                                         std::unordered_map<std::string, std::vector<std::string>> objLocMap,
                                         bool &isFromRocksdb, const std::vector<std::string> &workerUuids,
                                         const worker::HashRange &extraRanges)
{
    for (const auto &meta : metas) {
        ObjectMetaPb metaPb;
        if (!metaPb.ParseFromString(meta.second)) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Parse string to objectMetaPb failed.");
        }
        InsertExpireObjects(metaPb, expireObjects);
        const std::string &objectKey = metaPb.object_key();
        ObjectMeta metaCache;
        metaCache.meta = metaPb;
        if (!workerUuids.empty() || !extraRanges.empty()) {
            // Specifies the worker UUID used in scale-in scenarios. The primary address needs to be changed.
            metaCache.meta.set_primary_address(
                SelectPrimaryCopyWhenScaleIn(objectKey, metaCache.meta.primary_address(), objLocMap));
            std::string serializedStr;
            LOG_IF_ERROR(objectStore_->CreateSerializedStringForMeta(objectKey, metaCache.meta, serializedStr),
                         "Failed to serialize meta: " + objectKey);
            LOG_IF_ERROR(objectStore_->CreateOrUpdateMeta(objectKey, serializedStr),
                         "Failed to update meta: " + objectKey);
        }
        if (isFromRocksdb) {
            InsertToEtcdTableInMemory(objectKey, metaPb, ETCD_META_TABLE_PREFIX, objectKey);
        }

        auto workers = etcdCM_->GetValidWorkersInHashRing();
        if (etcdCM_->IsCentralized() || workers.find(metaCache.meta.primary_address()) != workers.end()) {
            if (metaCache.meta.primary_address().empty()) {
                LOG(ERROR) << FormatString("[Obj: %s] primary address is empty", objectKey);
            } else {
                metaCache.locations.emplace(metaCache.meta.primary_address());
            }
        }
        // Object key is the key in a key/value pair for the metadata table.
        // Storing the same object key in the "value" part of the kv is redundant and
        // deprecated. Save memory and resources by removing this from the value.
        // The field itself cannot be removed due to down-level support since this ObjectMeta pb
        // is stored on disk (rocksdb). In future it could be fully removed since its not used
        // anymore.
        metaCache.meta.set_allocated_object_key(NULL);
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        (void)metaTable_.insert({ objectKey, metaCache });
    }
    return Status::OK();
}

Status OCMetadataManager::LoadMeta(bool isFromRocksdb, const std::vector<std::string> &workerUuids,
                                   const worker::HashRange &extraRanges)
{
    std::vector<std::tuple<std::string, uint64_t, uint32_t>> expireObjects;
    std::vector<std::pair<std::string, std::string>> metas;

    RETURN_IF_NOT_OK(CheckRocksdbStatusAndLoadL2Table(ETCD_META_TABLE_PREFIX, META_TABLE, isFromRocksdb, workerUuids,
                                                      extraRanges, metas));
    std::unordered_map<std::string, std::vector<std::string>> objLocMap;
    RETURN_IF_NOT_OK(LoadObjectLocations(isFromRocksdb, objLocMap));
    RETURN_IF_NOT_OK(HandleLoadMeta(metas, expireObjects, objLocMap, isFromRocksdb, workerUuids, extraRanges));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RecoverObjectLocations(objLocMap), "Recovery object locations into memory failed");
    if (isFromRocksdb && objectStore_->IsRocksdbEnableWriteMeta()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(nestedRefManager_->RecoverRelationshipData(NESTED_TABLE, NESTED_COUNT_TABLE),
                                         "Load Nested relationship for rocksdb failed.");
    }
    expiredObjectManager_->ReloadExpireObjects(expireObjects);
    VLOG(1) << "Succeed to load metas into memory.";
    return Status::OK();
}

void OCMetadataManager::GetWorkerAddress(std::set<std::string> &workerAddresses)
{
    std::lock_guard<std::shared_timed_mutex> lck(metaTableMutex_);
    for (const auto &meta : metaTable_) {
        workerAddresses.insert(meta.second.locations.begin(), meta.second.locations.end());
    }
}

Status OCMetadataManager::AddSubscribeCache(const std::shared_ptr<SubscribeMeta> &subMeta)
{
    RETURN_RUNTIME_ERROR_IF_NULL(subMeta);
    LOG(INFO) << FormatString("Add subscribe cache, sub objects: %s, requestId: %s", VectorToString(subMeta->objects_),
                              subMeta->reqId_);
    for (const auto &id : subMeta->objects_) {
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbReqIdTable::accessor accessor;
        if (objKey2ReqId_.find(accessor, id)) {
            (void)accessor->second.insert(subMeta->reqId_);
        } else {
            std::set<ImmutableString> reqIds;
            reqIds.emplace(subMeta->reqId_);
            if (!objKey2ReqId_.emplace(accessor, id, reqIds)) {
                (void)accessor->second.insert(subMeta->reqId_);
            }
        }
    }
    (void)request2SubMeta_.emplace(subMeta->reqId_, subMeta);
    return Status::OK();
}

void OCMetadataManager::UpdateSubscribeCache(const std::string &objectKey, const ObjectMeta &objectMeta)
{
    VLOG(1) << "Update subscribe cache with key: " << objectKey;
    // Query requests on objectKey.
    std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
    TbbReqIdTable::accessor accessor;
    auto found = objKey2ReqId_.find(accessor, objectKey);
    if (!found || accessor->second.empty()) {
        return;
    }

    for (const auto &reqId : accessor->second) {
        TbbSubMetaTable::const_accessor subConstAccessor;
        if (!request2SubMeta_.find(subConstAccessor, reqId)) {
            continue;
        }

        VLOG(1) << "Notify the sub request: " << reqId;
        auto subMeta = subConstAccessor->second;
        // Notify subscribe meta info.
        if (subMeta->address_ != objectMeta.meta.primary_address()) {
            Status status = notifyWorkerManager_->NotifySubscribeMeta(objectKey, objectMeta, subMeta->address_,
                                                                      subMeta->isFromOtherAz_);
            if (status.IsError()) {
                LOG(ERROR) << FormatString("Notify subscribe of worker: %s for object: %s failed, status: %s.",
                                           subMeta->address_, objectKey, status.ToString());
            }
        }
        subConstAccessor.release();

        TbbSubMetaTable::accessor subAccessor;
        if (!request2SubMeta_.find(subAccessor, reqId)) {
            continue;
        }
        subMeta = subAccessor->second;
        // Update sub meta.
        subMeta->objects_.remove(objectKey);
        if (subMeta->objects_.empty()) {
            // If all request object has been notified, clear cache and cancel timer.
            if (subMeta->timer_ != nullptr) {
                TimerQueue::GetInstance()->Cancel(*(subMeta->timer_));
                subMeta->timer_.reset();
            }
            request2SubMeta_.erase(subAccessor);
        }
    }

    // Remove requests on this object.
    (void)objKey2ReqId_.erase(accessor);
    VLOG(1) << "Update subscribe cache done.";
}

void OCMetadataManager::RemoveSubscribeCache(const std::string &requestId)
{
    VLOG(1) << "Remove subscribe cache for request: " << requestId;
    std::shared_ptr<SubscribeMeta> subMeta;
    {
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbSubMetaTable::accessor subAccessor;
        if (!request2SubMeta_.find(subAccessor, requestId)) {
            return;
        }
        subMeta = subAccessor->second;
        (void)request2SubMeta_.erase(subAccessor);
    }

    auto injectFunc = []() {
        INJECT_POINT("master.RemoveSubscribeCache.deadlock");
        return Status::OK();
    };
    injectFunc();

    for (const auto &objectKey : subMeta->objects_) {
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbReqIdTable::accessor accessor;
        if (!objKey2ReqId_.find(accessor, objectKey)) {
            continue;
        }
        auto &requests = accessor->second;
        (void)requests.erase(requestId);
        if (requests.empty()) {
            (void)objKey2ReqId_.erase(accessor);
        }
    }
    if (subMeta->timer_ != nullptr) {
        TimerQueue::GetInstance()->Cancel(*(subMeta->timer_));
        subMeta->timer_.reset();
    }
}

Status OCMetadataManager::GetObjectLocations(const GetObjectLocationsReqPb &req,
                                             std::vector<ObjectLocationInfoPb> &locations)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    std::list<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    for (const auto &objectKey : objectKeys) {
        TbbMetaTable::const_accessor accessor;
        if (metaTable_.find(accessor, objectKey)) {
            LOG(INFO) << FormatString("[ObjectKey %s] GetObjectLocations: get object location from cache", objectKey);
            ObjectLocationInfoPb location;
            location.set_object_key(objectKey);
            if (!accessor->second.locations.empty()) {
                *location.mutable_object_locations() = { accessor->second.locations.begin(),
                                                         accessor->second.locations.end() };
            }
            location.set_object_size(accessor->second.meta.data_size());
            locations.emplace_back(std::move(location));
        } else {
            ObjectLocationInfoPb location;
            location.set_object_key(objectKey);
            location.set_object_size(0);
            locations.emplace_back(std::move(location));
        }
    }
    return Status::OK();
}

void OCMetadataManager::GetObjRefsMatch(const std::function<bool(const std::string &)> &matchFunc,
                                        std::unordered_set<std::string> &objKeys)
{
    if (globalRefTable_ == nullptr) {
        return;
    }
    std::unordered_map<std::string, std::unordered_set<std::string>> refTable;
    globalRefTable_->GetAllRef(refTable);
    for (const auto &it : refTable) {
        if (matchFunc(it.first)) {
            VLOG(1) << "MigrateobjKey:" << it.first;
            objKeys.emplace(it.first);
        }
    }
}

void OCMetadataManager::GetObjGlobalCacheDeletesMatch(
    const std::function<bool(const std::string &)> &matchFunc,
    std::unordered_map<std::string, std::unordered_map<uint64_t, uint64_t>> &objectDeleteInfos)
{
    if (globalCacheDeleteManager_ == nullptr) {
        return;
    }
    objectDeleteInfos = globalCacheDeleteManager_->GetDeletedInfosMatch(matchFunc);
}

void OCMetadataManager::GetRemoteClientIdsMatch(const std::function<bool(const std::string &)> &matchFunc,
                                                std::vector<std::string> &remoteClientIds)
{
    std::lock_guard<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
    for (const auto &it : clientIdRefTable_) {
        if (matchFunc(it.first)) {
            VLOG(1) << "Migrateclientid:" << it.first;
            remoteClientIds.emplace_back(it.first);
        }
    }
}

void OCMetadataManager::GetNestedRefsMatch(const std::function<bool(const std::string &)> &matchFunc,
                                           std::unordered_set<std::string> &nestedObjKeys)
{
    if (nestedRefManager_ == nullptr) {
        return;
    }
    std::vector<std::string> allNestedKeys;
    nestedRefManager_->GetAllNestedKeys(allNestedKeys);
    for (const auto &it : allNestedKeys) {
        if (matchFunc(it)) {
            VLOG(1) << "Migrate nested objKey:" << it;
            nestedObjKeys.emplace(it);
        }
    }
}

void OCMetadataManager::FillSubMetas(const std::vector<std::string> &objKeys, std::vector<SubscribeInfoPb> &subMetas)
{
    std::unordered_map<std::string, std::vector<std::string>> reqIdToObjs;
    for (const auto &id : objKeys) {
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbReqIdTable::const_accessor subReqAccessor;
        if (objKey2ReqId_.find(subReqAccessor, id)) {
            for (const auto &reqId : subReqAccessor->second) {
                reqIdToObjs[reqId].emplace_back(id);
            }
        }
    }

    for (const auto &info : reqIdToObjs) {
        SubscribeInfoPb meta;
        meta.set_request_id(info.first);
        *meta.mutable_objectkeys() = { info.second.begin(), info.second.end() };
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbSubMetaTable::const_accessor accessor;
        if (request2SubMeta_.find(accessor, info.first)) {
            meta.set_sub_address(accessor->second->address_);
            meta.set_is_from_other_az(accessor->second->isFromOtherAz_);
            auto now = TimerQueue::GetInstance()->CurrentTimeMs();
            auto subTime = accessor->second->timer_->GetTimestamp();
            if (now < subTime) {
                meta.set_timeout(subTime - now);
            }
            subMetas.emplace_back(meta);
        }
    }
}

void OCMetadataManager::GetSubscibeInfoMatch(std::function<bool(const std::string &)> matchFunc,
                                             std::vector<std::string> &objKeys)
{
    std::lock_guard<std::shared_timed_mutex> l(subTableMutex_);
    for (TbbReqIdTable::const_iterator iter = objKey2ReqId_.begin(); iter != objKey2ReqId_.end(); iter++) {
        if (matchFunc(iter->first)) {
            objKeys.emplace_back(iter->first);
        }
    }
}

void OCMetadataManager::FillNestedInfoForMigration(const std::string &objectKey, MetaForMigrationPb &meta)
{
    std::vector<std::string> nestedKeys;
    nestedRefManager_->GetNestedRelationship(objectKey, nestedKeys);
    *meta.mutable_nested_object_keys() = { nestedKeys.begin(), nestedKeys.end() };
    uint32_t ref = nestedRefManager_->GetNestedKeyRef(objectKey);
    meta.set_nested_ref(ref);
}

void OCMetadataManager::HandleNestedRefMigrateSuccess(const std::string &id)
{
    nestedRefManager_->RemoveRelationshipData(id);
    nestedRefManager_->RemoveNestIdsRef({ id });
}

void OCMetadataManager::HandleSubDataMigrateSuccess(const MigrateMetadataReqPb &req)
{
    std::unordered_set<std::string> objKeys;
    for (const auto &info : req.sub_metas()) {
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbSubMetaTable::accessor subAccessor;
        if (!request2SubMeta_.find(subAccessor, info.request_id())) {
            continue;
        }
        auto subMeta = subAccessor->second;
        // Update sub meta.
        for (const auto &objKey : info.objectkeys()) {
            objKeys.insert(objKey);
            subMeta->objects_.remove(objKey);
        }
        if (subMeta->objects_.empty()) {
            // If all request object has been migrated, clear cache and cancel timer.
            if (subMeta->timer_ != nullptr) {
                TimerQueue::GetInstance()->Cancel(*(subMeta->timer_));
                subMeta->timer_.reset();
            }
            request2SubMeta_.erase(subAccessor);
        }
    }
    // remove objKey2ReqId_
    for (const auto &id : objKeys) {
        std::shared_lock<std::shared_timed_mutex> l(subTableMutex_);
        TbbReqIdTable::accessor reqAccessor;
        if (!objKey2ReqId_.find(reqAccessor, id)) {
            continue;
        }
        objKey2ReqId_.erase(reqAccessor);
    }
}

void OCMetadataManager::FillClientIdRefsForMigration(const std::string &remoteClientId, const std::string &destination,
                                                     ClientIdRefsForMigrationPb *clientIdRefs)
{
    VLOG(1) << "Migrate clientid:" << remoteClientId;
    std::unordered_set<std::string> masterAddrs;
    {
        std::shared_lock<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
        TbbRemoteClientIdRefTable::accessor clientAccessor;
        if (clientIdRefTable_.find(clientAccessor, remoteClientId)) {
            for (const auto &masterAddress : clientAccessor->second) {
                masterAddrs.insert(masterAddress);
            }
        }
    }
    masterAddrs.insert(destination);
    clientIdRefs->set_remote_client_id(remoteClientId);
    *clientIdRefs->mutable_master_addrs() = { masterAddrs.begin(), masterAddrs.end() };
}

void OCMetadataManager::FillObjRefsForMigration(const std::string &objectKey, MetaForMigrationPb &objectPb)
{
    VLOG(1) << "MigrateObj: " << objectKey;
    std::vector<std::string> remoteClientIds;
    globalRefTable_->GetObjRefIds(objectKey, remoteClientIds);
    if (!remoteClientIds.empty()) {
        *objectPb.mutable_client_ids() = { remoteClientIds.begin(), remoteClientIds.end() };
    }
}

void OCMetadataManager::FillRemoteClientIdForMigration(MigrateMetadataReqPb &req)
{
    std::unordered_set<std::string> remoteClientIds;
    globalRefTable_->GetRemoteClientIds(remoteClientIds);
    *req.mutable_remote_client_ids() = { remoteClientIds.begin(), remoteClientIds.end() };
}

Status OCMetadataManager::SaveSubscribeData(const MigrateMetadataReqPb &req)
{
    LOG(INFO) << "Recv migrate subscribe data, src:" << req.source_addr();
    for (const auto &info : req.sub_metas()) {
        if (info.timeout() > 0) {
            QueryMetaReqPb queryReq;
            queryReq.set_address(info.sub_address());
            queryReq.set_request_id(info.request_id());
            queryReq.set_is_from_other_az(info.is_from_other_az());
            std::list<std::string> ids = { info.objectkeys().begin(), info.objectkeys().end() };
            TryToSubscribeCache(info.timeout(), queryReq, ids);
        } else {
            LOG(INFO) << "sub timeout, no need to sub, requestId: " << info.request_id()
                      << " , objects: " << VectorToString(info.objectkeys()) << " , subtimeout: " << info.timeout();
        }
    }
    return Status::OK();
}

bool OCMetadataManager::SaveOneMigrationObjRefData(const std::string &objKey, const MetaForMigrationPb &objMeta,
                                                   const std::vector<std::string> &allRemoteClientIds)
{
    VLOG(1) << "MigrateObj:" << objKey;
    for (const auto &clientId : objMeta.client_ids()) {
        Status rc;
        std::vector<std::string> tempFailedIds;
        std::vector<std::string> tempFirstIds;
        if (std::find(allRemoteClientIds.begin(), allRemoteClientIds.end(), clientId) == allRemoteClientIds.end()) {
            // The in-cloud reference counting.
            rc = globalRefTable_->GIncreaseRef(clientId, { objKey }, tempFailedIds, tempFirstIds, false);
            if (rc.IsOk()) {
                continue;
            }
            LOG_IF_ERROR(rc, FormatString("SaveMigrationObjRefdata, objKey: %s", objKey));
            (void)globalRefTable_->GDecreaseRef(clientId, { objKey }, tempFailedIds, tempFirstIds);
            return false;
        }

        // out-cloud reference counting.
        bool isFirstAppearRemoteClientId = globalRefTable_->IsNotExistRemoteClientId(clientId);
        rc = globalRefTable_->GIncreaseRef(clientId, { objKey }, tempFailedIds, tempFirstIds, true);
        // If the remoteClientId migrates to the current master and is present there for the first time, A request
        // needs to be sent to the hash master to record the location of the remoteClientId
        if (rc.IsOk() && isFirstAppearRemoteClientId) {
            rc = GIncreaseRemoteClientIdToMaster(clientId);
        }
        if (rc.IsError()) {
            LOG_IF_ERROR(rc, FormatString("SaveMigrationObjRefdata, objKey: %s", objKey));
            (void)globalRefTable_->GDecreaseRef(clientId, { objKey }, tempFailedIds, tempFirstIds);
            return false;
        }
    }
    return true;
}

Status OCMetadataManager::SaveMigrationRemoteClientRefData(const MigrateMetadataReqPb &req)
{
    // SaveMigration remoteClientId
    for (auto &remoteClientIdRefs : req.client_id_refs()) {
        const std::string &clientId = remoteClientIdRefs.remote_client_id();
        VLOG(1) << "Migrate clientid:" << clientId;
        std::vector<std::string> masterAddrs{ remoteClientIdRefs.master_addrs().begin(),
                                              remoteClientIdRefs.master_addrs().end() };
        std::shared_lock<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
        TbbRemoteClientIdRefTable::accessor objAccessor;
        if (!clientIdRefTable_.find(objAccessor, clientId)) {
            (void)clientIdRefTable_.insert(objAccessor, clientId);
        }
        for (auto &masterAddr : masterAddrs) {
            VLOG(1) << "Migrate masterAddr:" << masterAddr;
            objAccessor->second.emplace(masterAddr);
            // if store to rocksdb failed, after restart, remote client ReleaseGRefs client in failed master cant not
            // release ref.
            objectStore_->AddRemoteClientRef(clientId, masterAddr);
        }
    }
    return Status::OK();
}

void OCMetadataManager::HandleObjRefDataMigrationOnSuccess(const std::string &objKey,
                                                           const std::vector<std::string> &remoteClientIds)
{
    std::vector<std::string> failedDecIds;
    std::vector<std::string> finishDecIds;
    for (auto &remoteClientId : remoteClientIds) {
        VLOG(1) << "Ref Migrati Success remoteClientId:" << remoteClientId << ", objKey:" << objKey;
        Status rc = globalRefTable_->GDecreaseRef(remoteClientId, { objKey }, failedDecIds, finishDecIds);
        LOG_IF_ERROR(rc, FormatString("GDecreaseRef failed obj:%s, remoteClientId: %s", objKey, remoteClientId));
    }
}

Status OCMetadataManager::GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    (void)resp;
    std::string newAddr;
    bool needRedirect = RedirectClientIdRef(req.remote_client_id(), req.redirect(), newAddr);
    resp.set_ref_is_moving(needRedirect);
    if (needRedirect) {
        RedirectMetaInfo *info = resp.add_infos();
        info->set_redirect_meta_address(newAddr);
        info->add_change_meta_ids(req.remote_client_id());
        return Status::OK();
    }
    std::shared_lock<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
    TbbRemoteClientIdRefTable::accessor objAccessor;
    if (!clientIdRefTable_.find(objAccessor, req.remote_client_id())) {
        clientIdRefTable_.insert(objAccessor, req.remote_client_id());
    }
    objAccessor->second.emplace(req.address());
    objectStore_->AddRemoteClientRef(req.remote_client_id(), req.address());
    return Status::OK();
}

Status OCMetadataManager::GetPrimaryReplicaAddr(const std::string &masterAddr, HostPort &primaryAddr)
{
    std::string dbName;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->GetPrimaryReplicaLocationByAddr(masterAddr, primaryAddr, dbName),
                                     "GetPrimaryReplicaAddr failed");
    g_MetaRocksDbName = dbName;
    return Status::OK();
}

Status OCMetadataManager::GIncreaseRemoteClientIdToMaster(const std::string &remoteClientId, HostPort masterAddr)
{
    bool checkRedirect = false;
    if (masterAddr.Empty()) {
        RETURN_IF_NOT_OK(etcdCM_->GetMasterAddr(remoteClientId, masterAddr));
        checkRedirect = true;
    }
    RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(masterAddr.ToString(), masterAddr));
    // create master api
    VLOG(1) << "GInc RemoteClientIdToMaster dest:" << masterAddr.ToString() << ", remoteClientId:" << remoteClientId;
    HostPort localAddr;
    RETURN_IF_NOT_OK(localAddr.ParseString(masterAddress_));
    std::unique_ptr<MasterMasterOCApi> api;
    api = std::make_unique<MasterMasterOCApi>(masterAddr, localAddr, akSkManager_);
    RETURN_IF_NOT_OK(api->Init());

    GIncreaseReqPb req;
    GIncreaseRspPb rsp;
    req.set_address(masterAddress_);
    req.set_remote_client_id(remoteClientId);
    req.set_redirect(checkRedirect);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    Status rc = RetryOnRPCError([&api, &req, &rsp, &localAddr, this]() {
        Status res = api->GIncreaseMasterAppRef(req, rsp);
        if (rsp.ref_is_moving() && !rsp.infos().empty()) {
            HostPort newMetaAddr;
            RETURN_IF_NOT_OK(newMetaAddr.ParseString(rsp.infos()[0].redirect_meta_address()));
            LOG(INFO) << "clientId ref has been migrated to the new master[%s]" << newMetaAddr.ToString();
            api = std::make_unique<MasterMasterOCApi>(newMetaAddr, localAddr, akSkManager_);
            RETURN_IF_NOT_OK(api->Init());
            req.set_redirect(false);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
            static const int sleepTimeMs = 200;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
            res = api->GIncreaseMasterAppRef(req, rsp);
        }
        return res;
    });
    LOG_IF_ERROR(
        rc, FormatString("GIncreaseMasterAppRef fail masterAddr:%s, status:%s", masterAddr.ToString(), rc.ToString()));
    return rc;
}

Status OCMetadataManager::GIncreaseRefWithRemoteClientId(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    std::vector<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    std::vector<std::string> failedIncIds;
    std::vector<std::string> firstIncIds;
    std::string remoteClientId = req.remote_client_id();
    LOG(INFO) << "GIncreaseRefWithRemoteClientId remoteClientId:" << remoteClientId;
    // If object is migrating, return rsp and let worker retry later
    bool needRedirect = req.redirect();
    RedirectObjRefs(resp, needRedirect, objectKeys);
    if (resp.ref_is_moving()) {
        return Status::OK();
    }

    bool isFirstAppearRemoteClientId = globalRefTable_->IsNotExistRemoteClientId(remoteClientId);
    Status rc = globalRefTable_->GIncreaseRef(remoteClientId, objectKeys, failedIncIds, firstIncIds, true);

    // if remoteClientId appear first time, hash remoteClientId and notify master
    if (isFirstAppearRemoteClientId) {
        rc = GIncreaseRemoteClientIdToMaster(remoteClientId);
        if (rc.IsError()) {
            std::vector<std::string> temFailedIds;
            std::vector<std::string> temFirstIds;
            (void)globalRefTable_->GDecreaseRef(remoteClientId, objectKeys, temFailedIds, temFirstIds, true);
            (void)failedIncIds.insert(failedIncIds.end(), objectKeys.begin(), objectKeys.end());
        }
        if (!failedIncIds.empty()) {
            *resp.mutable_failed_object_keys() = { failedIncIds.begin(), failedIncIds.end() };
        }
    }
    return rc;
}

Status OCMetadataManager::RecoverMasterAppRef(std::function<bool(const std::string &)> matchFunc,
                                              const std::string &standbyWorker)
{
    std::unordered_set<std::string> remoteClientIds;
    LOG(INFO) << "recover remote client master app ref";
    globalRefTable_->GetRemoteClientIds(remoteClientIds);
    for (const auto &id : remoteClientIds) {
        if (matchFunc(id)) {
            HostPort addr;
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(addr.ParseString(standbyWorker), "master addr parse failed");
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GIncreaseRemoteClientIdToMaster(id, addr),
                                             "Recover master app ref failed");
        }
    }
    return Status::OK();
}

bool OCMetadataManager::RedirectClientIdRef(const std::string &remoteClientId, bool needRedirect, std::string &newAddr)
{
    if (!needRedirect || !FLAGS_enable_redirect) {
        VLOG(1) << "receive redirect object: " << remoteClientId;
        return false;
    }
    HostPort masterAddr;
    bool redirect = etcdCM_->NeedRedirect(remoteClientId, masterAddr);
    if (redirect) {
        newAddr = masterAddr.ToString();
        LOG(WARNING) << FormatString("ref need redirect, ClientId: %s, redirect address %s", remoteClientId, newAddr);
    }
    return redirect;
}

void OCMetadataManager::RedirectObjRefs(std::string &objectKey, bool &needRedirect, std::string &newAddr,
                                        bool &isMoving)
{
    if (!FLAGS_enable_redirect) {
        needRedirect = false;
        return;
    }
    HostPort masterAddr;
    needRedirect = etcdCM_->NeedRedirect(objectKey, masterAddr);
    if (!needRedirect) {
        return;
    }
    if (globalRefTable_->GetRefWorkerCount(objectKey) > 0) {
        // refs is migrating, need to wait meta migrate done
        isMoving = true;
        needRedirect = true;
        newAddr = masterAddr.ToString();
        LOG(WARNING) << FormatString("objectKey %s ref is moving", objectKey);
        return;
    }
    if (!nestedRefManager_->CheckIsNoneNestedRefById(objectKey)) {
        // nested ref is moving, need to wait meta migrate done
        isMoving = true;
        needRedirect = true;
        newAddr = masterAddr.ToString();
        LOG(WARNING) << FormatString("objectKey %s ref is moving", objectKey);
        return;
    }
    // ref already migrated and meta is migrating, need to wait meta migrate done
    TbbMetaTable::const_accessor accessor;
    if (metaTable_.find(accessor, objectKey)) {
        isMoving = true;
        LOG(WARNING) << FormatString("objectKey %s ref is moved, meta is moving", objectKey);
    }
    needRedirect = true;
    newAddr = masterAddr.ToString();
    LOG(WARNING) << FormatString("ref need redirect, objectKey: %s, redirect address %s", objectKey, newAddr);
}

Status OCMetadataManager::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    if (!req.remote_client_id().empty()) {
        return GIncreaseRefWithRemoteClientId(req, resp);
    }
    std::vector<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    RedirectObjRefs(resp, req.redirect(), objectKeys);
    if (resp.ref_is_moving()) {
        return Status::OK();
    }
    std::vector<std::string> failedIncIds;
    std::vector<std::string> firstIncIds;
    Status rc = globalRefTable_->GIncreaseRef(req.address(), objectKeys, failedIncIds, firstIncIds);
    if (!failedIncIds.empty()) {
        *resp.mutable_failed_object_keys() = { failedIncIds.begin(), failedIncIds.end() };
    }
    return rc;
}

void OCMetadataManager::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    // Step 1: Query the master address where the remote_client_id metadata is stored.
    std::vector<std::string> masterAddrs;
    const std::string &remoteClientId = req.remote_client_id();
    {
        std::shared_lock<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
        TbbRemoteClientIdRefTable::accessor clientAccessor;
        if (!clientIdRefTable_.find(clientAccessor, remoteClientId)) {
            LOG(WARNING) << FormatString("ReleaseGRefs: remoteClientId does not exist: %s", remoteClientId);
            return;
        }
        for (const auto &masterAddress : clientAccessor->second) {
            masterAddrs.emplace_back(masterAddress);
        }
    }
    // Step 2: Notify the corresponding master to clear metadata.
    Status rc;
    Status lastErr;
    for (auto masterAddress : masterAddrs) {
        rc = ReleaseGRefsToMaster(remoteClientId, masterAddress);
        LOG_IF_ERROR(rc, FormatString("ReleaseGRefsToMaster failed. rc:%s", rc.ToString()));
        rc.IsError() ? lastErr = rc : lastErr;
    }
    resp.mutable_last_rc()->set_error_code(lastErr.GetCode());
    resp.mutable_last_rc()->set_error_msg(lastErr.GetMsg());
}

Status OCMetadataManager::ReleaseGRefsToMaster(const std::string &remoteClientId, const std::string &masterAddress)
{
    HostPort masterAddr;
    RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(masterAddress, masterAddr));
    VLOG(1) << "ReleaseGRefsToMaster dest:" << masterAddress << ", remoteClientId:" << remoteClientId;
    HostPort localAddr;
    RETURN_IF_NOT_OK(localAddr.ParseString(masterAddress_));
    // create master to master api
    std::unique_ptr<MasterMasterOCApi> api;
    api = std::make_unique<MasterMasterOCApi>(masterAddr, localAddr, akSkManager_);
    RETURN_IF_NOT_OK(api->Init());

    ReleaseGRefsReqPb req;
    ReleaseGRefsRspPb rsp;
    int64_t remainingTime = timeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    req.set_timeout(remainingTime);
    req.set_remote_client_id(remoteClientId);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        RetryOnRPCError([&api, &req, &rsp]() { return api->ReleaseGRefsOfRemoteClientId(req, rsp); }),
        FormatString("ReleaseGRefsToMaster failed. masterAddr:%s", masterAddr.ToString()));
    std::shared_lock<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
    TbbRemoteClientIdRefTable::accessor accessor;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(clientIdRefTable_.find(accessor, remoteClientId), StatusCode::K_RUNTIME_ERROR,
                                         FormatString("Fail to find remoteClientId: %s", masterAddress));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(accessor->second.erase(masterAddress), StatusCode::K_RUNTIME_ERROR,
                                         FormatString("Fail to erase masterAddress: %s", masterAddress));
    RETURN_IF_NOT_OK(objectStore_->RemoveRemoteClientRef(remoteClientId, masterAddress));
    VLOG(1) << "ReleaseGRefsToMaster success. master:" << masterAddr.ToString() << ",remoteClientId:" << remoteClientId;
    return Status::OK();
}

Status OCMetadataManager::ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    (void)resp;
    const std::string &remoteClientId = req.remote_client_id();
    std::vector<std::string> objectKeys;
    globalRefTable_->GetClientRefIds(remoteClientId, objectKeys);
    master::GDecreaseReqPb decReq;
    int64_t remainingTime_ = timeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime_ > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime_));
    decReq.set_timeout(remainingTime_);
    *decReq.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    decReq.set_address(req.address());
    decReq.set_remote_client_id(remoteClientId);
    decReq.set_redirect(false);
    master::GDecreaseRspPb decRsp;
    std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi;
    GDecreaseRefImplWithRemoteClientId(decReq, decRsp, serverApi, false);
    resp.mutable_last_rc()->set_error_code(decRsp.mutable_last_rc()->error_code());
    resp.mutable_last_rc()->set_error_msg(decRsp.mutable_last_rc()->error_msg());
    VLOG(1) << "recv ReleaseGRefsOfRemoteClientId src:" << req.address() << ", remoteClientId:" << remoteClientId;
    return Status::OK();
}

void OCMetadataManager::ConstructRequestObjectKeyMap(const std::vector<std::string> failedDecIds,
                                                     const std::vector<std::string> finishDecIds,
                                                     std::unordered_map<std::string, bool> &requestObjectKeyMap)
{
    for (const auto &objKey : finishDecIds) {
        bool needDelete = false;
        if (std::find(failedDecIds.begin(), failedDecIds.end(), objKey) == failedDecIds.end()) {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::const_accessor accessor;
            auto found = metaTable_.find(accessor, objKey);
            needDelete = found && nestedRefManager_->CheckIsNoneNestedRefById(objKey);
        }
        requestObjectKeyMap.emplace(objKey, needDelete);
    }
}

void OCMetadataManager::GDecreaseRefImplWithRemoteClientId(
    const GDecreaseReqPb &req, GDecreaseRspPb &resp,
    const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi, bool needReleaseRpc)
{
    VLOG(1) << "GDecreaseRefImplWithRemoteClientId clientId:" << req.remote_client_id();
    std::vector<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    std::vector<std::string> failedDecIds;
    std::vector<std::string> finishDecIds;
    RedirectObjRefs(resp, req.redirect(), objectKeys);
    if (resp.ref_is_moving()) {
        if (serverApi != nullptr) {
            LOG_IF_ERROR(serverApi->Write(resp), "Write reply to client stream failed.");
        }
        return;
    }
    Status lastErr =
        globalRefTable_->GDecreaseRef(req.remote_client_id(), objectKeys, failedDecIds, finishDecIds, true);
    std::unordered_map<std::string, bool> requestObjectKeyMap;
    ConstructRequestObjectKeyMap(failedDecIds, finishDecIds, requestObjectKeyMap);
    DeleteObjectMediator delMediator(req.address(), requestObjectKeyMap);
    delMediator.SetStatusIfError(lastErr);
    if (FLAGS_async_delete) {
        AsyncDeleteByExpired(delMediator);
    } else {
        FindNeedDeleteIds(delMediator);
        bool processFinished = delMediator.CheckNoNeedToNotifyWorker();
        if (processFinished || !needReleaseRpc || serverApi == nullptr) {
            NotifyDeleteAndClearMeta(delMediator, false);
        } else {
            AsyncNotifyWorkerGDec(delMediator, failedDecIds, resp, serverApi, req.remote_client_id());
            return;
        }
    }
    RollbackIfGDecRefFail(delMediator, failedDecIds, req.remote_client_id());
    SetGDecreaseRefRspPb(delMediator.GetStatus(), std::move(failedDecIds), delMediator.GetNotRefIds(), resp);
    if (serverApi != nullptr) {
        LOG_IF_ERROR(serverApi->Write(resp), "Write reply to client stream failed.");
    }
}

void OCMetadataManager::AsyncNotifyWorkerGDec(
    DeleteObjectMediator &delMediator, std::vector<std::string> &failedDecIds, GDecreaseRspPb &resp,
    const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> &serverApi,
    const std::string &remoteClientId)
{
    LOG(INFO) << "Async notify worker GDecreaseRef result";
    int64_t timeout = timeoutDuration.CalcRealRemainingTime();
    Timer timer;
    std::string traceID = Trace::Instance().GetTraceID();
    asyncTaskPool_->Execute([=]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "RPC timeout. time elapsed " << elapsed << ", timeout:" << timeout
                       << ", GDecreaseRef threads Statistics: " << asyncTaskPool_->GetStatistics();
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
            return;
        }
        timeoutDuration.Init(timeout - elapsed);
        NotifyDeleteAndClearMeta(delMediator, false);
        RollbackIfGDecRefFail(delMediator, failedDecIds, remoteClientId);
        SetGDecreaseRefRspPb(delMediator.GetStatus(), std::move(failedDecIds), delMediator.GetNotRefIds(), resp);
        LOG_IF_ERROR(serverApi->Write(resp), "Write reply to client stream failed.");
        LOG(INFO) << "GDecreaseRef send response to worker in async finished. objectKeys: "
                  << VectorToString(delMediator.GetObjKeys());
    });
}

void OCMetadataManager::GDecreaseRefImpl(
    const GDecreaseReqPb &req, GDecreaseRspPb &resp,
    const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> &serverApi, bool needReleaseRpc)
{
    std::vector<std::string> objectKeys = { req.object_keys().begin(), req.object_keys().end() };
    std::vector<std::string> failedDecIds;
    std::vector<std::string> finishDecIds;
    RedirectObjRefs(resp, req.redirect(), objectKeys);
    if (resp.ref_is_moving()) {
        return;
    }
    Status lastErr = globalRefTable_->GDecreaseRef(req.address(), objectKeys, failedDecIds, finishDecIds);

    std::unordered_map<std::string, bool> requestObjectKeyMap;
    for (const auto &objKey : finishDecIds) {
        bool needDelete = false;
        if (std::find(failedDecIds.begin(), failedDecIds.end(), objKey) == failedDecIds.end()) {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::const_accessor accessor;
            auto found = metaTable_.find(accessor, objKey);
            Status rc = Status::OK();
            if (found && nestedRefManager_->CheckIsNoneNestedRefById(objKey)) {
                needDelete = true;
            } else {
                rc = CheckAndClearDeviceMeta::GetInstance().NotifyAll(objKey);
                if (rc.IsOk()) {
                    LOG(INFO) << FormatString("[ObjectKey %s] is device object, ignore", objKey);
                }
            }

            if (!found && rc.IsError() && !FLAGS_oc_io_from_l2cache_need_metadata) {
                // Could not find the object in meta table (for both regular and device object) and not a device object.
                // Try to delete all versions of the object form L2Cache.
                needDelete = true;
            }
        }
        requestObjectKeyMap.emplace(objKey, needDelete);
    }
    DeleteObjectMediator delMediator(req.address(), requestObjectKeyMap);
    delMediator.SetStatusIfError(lastErr);
    if (FLAGS_async_delete) {
        // Case 1: Asynchronous deletion. Set the TTL of the object to MIN_TTL_SECOND(is 0) and use the background
        // ExpiredObjectManager to automatically delete the object.
        AsyncDeleteByExpired(delMediator);
    } else {
        FindNeedDeleteIds(delMediator);
        bool processFinished = delMediator.CheckNoNeedToNotifyWorker();
        if (processFinished || !needReleaseRpc || serverApi == nullptr) {
            // Case 2: Don't need to notify workers. Clear metadata in the current thread.
            NotifyDeleteAndClearMeta(delMediator, false);
        } else {
            // Case 3: Need to notify workers. Clear metadata in the asynchronous thread.
            AsyncNotifyWorkerGDec(delMediator, failedDecIds, resp, serverApi);
            return;
        }
    }
    RollbackIfGDecRefFail(delMediator, failedDecIds);
    SetGDecreaseRefRspPb(delMediator.GetStatus(), std::move(failedDecIds), delMediator.GetNotRefIds(), resp);
    if (serverApi != nullptr) {
        LOG_IF_ERROR(serverApi->Write(resp), "Write reply to client stream failed.");
    }
}

void OCMetadataManager::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi;
    if (!req.remote_client_id().empty()) {
        GDecreaseRefImplWithRemoteClientId(req, resp, serverApi, false);
    } else {
        GDecreaseRefImpl(req, resp, serverApi, false);
    }
}

void OCMetadataManager::GDecreaseRefWithServerApi(
    const GDecreaseReqPb &req,
    const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> &serverApi)
{
    GDecreaseRspPb resp;
    if (!req.remote_client_id().empty()) {
        GDecreaseRefImplWithRemoteClientId(req, resp, serverApi, true);
    } else {
        GDecreaseRefImpl(req, resp, serverApi, true);
    }
}

void OCMetadataManager::SetGDecreaseRefRspPb(const Status &status, const std::vector<std::string> &&failedDecIds,
                                             std::vector<std::string> &&noRefIds, GDecreaseRspPb &resp)
{
    for (const auto &failedId : failedDecIds) {
        resp.add_failed_object_keys(failedId);
    }
    for (const auto &unAliveId : noRefIds) {
        VLOG(1) << "unAliveId:" << unAliveId;
        resp.add_no_ref_ids(unAliveId);
    }
    resp.mutable_last_rc()->set_error_code(status.GetCode());
    resp.mutable_last_rc()->set_error_msg(status.GetMsg());
}

void OCMetadataManager::RollbackIfGDecRefFail(DeleteObjectMediator &delMediator, std::vector<std::string> &failedDecIds,
                                              const std::string &remoteClientId)
{
    if (delMediator.GetStatus().IsOk()) {
        return;
    }
    LOG(ERROR) << "Delete failed in GDecreaseRef. " << delMediator.GetStatus().ToString();
    std::vector<std::string> failedIds = { delMediator.GetFailedObjs().begin(), delMediator.GetFailedObjs().end() };
    std::vector<std::string> tempFailedIds;
    std::vector<std::string> tempFinishIds;
    failedDecIds.insert(failedDecIds.end(), failedIds.begin(), failedIds.end());
    if (!remoteClientId.empty()) {
        (void)globalRefTable_->GIncreaseRef(remoteClientId, failedIds, tempFailedIds, tempFinishIds, true);
        return;
    }
    (void)globalRefTable_->GIncreaseRef(delMediator.GetSourceWorker(), failedIds, tempFailedIds, tempFinishIds);
}

Status OCMetadataManager::CreateHashMeta(const ObjectMetaPb &meta, const std::string &address)
{
    const std::string &objectKey = meta.object_key();
    ObjectMeta metaCache;
    metaCache.meta = meta;
    // Object key is the key in a key/value pair for the metadata table.
    // Storing the same object key in the "value" part of the kv is redundant and
    // deprecated. Save memory and resources by removing this from the value.
    // The field itself cannot be removed due to down-level support since this ObjectMeta pb
    // is stored on disk (rocksdb). In future it could be fully removed since its not used
    // anymore.
    metaCache.meta.set_allocated_object_key(NULL);
    metaCache.meta.set_primary_address(address);

    {
        // Check meta info in cache and rocksdb.
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        auto found = metaTable_.find(accessor, objectKey);
        if (found) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
                accessor->second.meta.config().data_format() == meta.config().data_format(), K_INVALID,
                FormatString("Invalid data format of objectKey(%s)", objectKey));
            (void)accessor->second.locations.emplace(address);
        } else {
            INJECT_POINT("master.CreateHashMeta.new_object");
            (void)metaCache.locations.emplace(address);
            if (!metaTable_.emplace(accessor, objectKey, metaCache)) {
                (void)accessor->second.locations.emplace(address);
            }
        }
    }
    std::string serializedStr;
    RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objectKey, metaCache.meta, serializedStr));
    return objectStore_->CreateOrUpdateMeta(objectKey, serializedStr,
                                            WriteMode2MetaType(metaCache.meta.config().write_mode()));
}

void OCMetadataManager::GetMetasMatch(std::function<bool(const std::string &)> &&matchFunc,
                                      std::vector<std::string> &objKeys, bool *exitEarly)
{
    std::lock_guard<std::shared_timed_mutex> lck(metaTableMutex_);
    for (const auto &it : metaTable_) {
        if (exitEarly && *exitEarly) {
            break;
        }
        if (matchFunc(it.first)) {
            objKeys.emplace_back(it.first);
        }
    }
}

void OCMetadataManager::GetMetasInAsyncQueueMatch(
    std::function<bool(const std::string &)> &&matchFunc,
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &objAsyncMap)
{
    objectStore_->GetMetasMatch(std::forward<std::function<bool(const std::string &)>>(matchFunc), objAsyncMap);
}

void OCMetadataManager::GetAsyncElementsByObjectKey(const std::string &objectKey,
                                                    std::unordered_set<std::shared_ptr<AsyncElement>> &elements)
{
    objectStore_->PollAsyncElementsByObjectKey(objectKey, elements);
}

Status OCMetadataManager::RemoveMetaByWorker(const std::string &workerAddr)
{
    INJECT_POINT("OCMetadataManager.RemoveMetaByWorker.delay");
    std::list<std::string> removeObjectKeys;
    {
        Timer timer;
        std::lock_guard<std::shared_timed_mutex> lck(metaTableMutex_);
        masterOperationTimeCost.Append("RemoveMetaByWorker get lock", timer.ElapsedMilliSecond());
        for (const auto &it : metaTable_) {
            const std::string &objectKey = it.first;
            if (it.second.locations.count(workerAddr) || it.second.meta.primary_address() == workerAddr) {
                removeObjectKeys.emplace_back(objectKey);
            }
        }
    }
    RETURN_OK_IF_TRUE(removeObjectKeys.empty());
    LOG(INFO) << FormatString("[ObjectKeys %s] Start to remove meta location %s",
                              VectorToString(removeObjectKeys, false), workerAddr);
    for (const auto &objectKey : removeObjectKeys) {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        if (!metaTable_.find(accessor, objectKey)) {
            LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", objectKey);
            continue;
        }
        (void)accessor->second.locations.erase(workerAddr);
        (void)objectStore_->RemoveObjectLocation(objectKey, workerAddr);
        // If the object type is NONE_L2_CACHE_EVICT and the only data location has been lost,
        // just delete it because its reliability is already poor.
        if (accessor->second.locations.empty() && accessor->second.IsNoneL2CacheEvict()) {
            (void)objectStore_->RemoveMeta(objectKey, false);
            (void)metaTable_.erase(accessor);
            continue;
        }
        if (workerAddr == accessor->second.meta.primary_address()) {
            std::string newPrimaryCopy;
            if (ReselectPrimaryCopy(objectKey, {}, accessor, newPrimaryCopy).IsOk()) {
                accessor->second.meta.set_primary_address(newPrimaryCopy);
                VLOG(1) << FormatString("[Objects %s] Primary copy changes from %s to %s", objectKey, workerAddr,
                                        newPrimaryCopy);
            } else {
                accessor->second.meta.clear_primary_address();
                VLOG(1) << FormatString("[Objects %s] Primary copy(%s) was cleaned up", objectKey, workerAddr);
            }
            std::string serializedStr;
            RETURN_IF_NOT_OK(
                objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr));
            (void)objectStore_->CreateOrUpdateMeta(objectKey, serializedStr,
                                                   WriteMode2MetaType(accessor->second.meta.config().write_mode()));
        }
    }
    return Status::OK();
}

void OCMetadataManager::ModifyPrimaryCopy(const std::string &objectKey, const std::string &workerId,
                                          bool ifvoluntaryScaleDown)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::accessor accessor;
    if (metaTable_.find(accessor, objectKey)) {
        std::string oldPrimaryCopy = accessor->second.meta.primary_address();
        if (notifyWorkerManager_->CheckWorkerIsHealthy(oldPrimaryCopy).IsError() || ifvoluntaryScaleDown) {
            accessor->second.meta.set_primary_address(workerId);
            std::string serializedStr;
            (void)objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr);
            (void)objectStore_->CreateOrUpdateMeta(objectKey, serializedStr,
                                                   WriteMode2MetaType(accessor->second.meta.config().write_mode()));
            if (!ifvoluntaryScaleDown) {
                LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                          static_cast<uint32_t>(NotifyWorkerOpType::PRIMARY_COPY_INVALID), objectKey,
                                          oldPrimaryCopy);
                (void)notifyWorkerManager_->InsertAsyncWorkerOp(
                    oldPrimaryCopy, objectKey, { NotifyWorkerOpType::PRIMARY_COPY_INVALID }, true,
                    WriteMode2MetaType(accessor->second.meta.config().write_mode()));
            }
            LOG(INFO) << FormatString("The primary copy of the object(%s) is changed to %s.", objectKey, workerId);
        }
    }
}

Status OCMetadataManager::ReselectPrimaryCopy(const std::string &objectKey,
                                              const std::unordered_set<std::string> &excludedAddr,
                                              TbbMetaTable::accessor &accessor, std::string &primaryCopy)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    if (metaTable_.find(accessor, objectKey)) {
        if (notifyWorkerManager_->CheckWorkerIsHealthy(accessor->second.meta.primary_address()).IsOk()) {
            // If the primary copy is normal, we do not need to reselect primary copy and return an error.
            RETURN_STATUS(StatusCode::K_INVALID, "The primary copy is normal. objectKey:" + objectKey);
        }
        for (const auto &addr : accessor->second.locations) {
            if (addr != accessor->second.meta.primary_address() && excludedAddr.find(addr) == excludedAddr.end()) {
                primaryCopy = addr;
                return Status::OK();
            }
        }
    }
    RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, "Failed to select a new primary copy. objectKey:" + objectKey);
}

void OCMetadataManager::ProcessPrimaryCopyByWorkerTimeout(const std::string &workerAddr, bool ifvoluntaryScaleDown)
{
    std::vector<std::string> primaryCopyObjs;
    {
        std::lock_guard<std::shared_timed_mutex> lck(metaTableMutex_);
        for (const auto &it : metaTable_) {
            if (it.second.meta.primary_address() == workerAddr) {
                primaryCopyObjs.emplace_back(it.first);
            }
        }
    }

    std::unordered_map<std::string, std::unordered_set<std::string>> toBeChanged;
    for (const auto &id : primaryCopyObjs) {
        std::string newPrimaryCopy;
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        if (ReselectPrimaryCopy(id, {}, accessor, newPrimaryCopy).IsOk()) {
            toBeChanged[newPrimaryCopy].emplace(id);
        }
    }
    // Only timeout worker needs to add PRIMARY_COPY_INVALID async table.
    notifyWorkerManager_->AsyncChangePrimaryCopy(toBeChanged, ifvoluntaryScaleDown);
}

Status OCMetadataManager::ProcessOtherAzWorkerDead(const std::string &workerAddr)
{
    return notifyWorkerManager_->ClearAsyncWorkerOp(workerAddr);
}

Status OCMetadataManager::ProcessWorkerTimeout(const std::string &workerAddr, bool changePrimaryCopy,
                                               bool removeFailWorkerMetaData)
{
    // ignore for current node timeout.
    if (workerAddr == masterAddress_) {
        LOG(INFO) << "ignore for current node timeout " << workerAddr;
        return Status::OK();
    }
    LOG(WARNING) << "ProcessWorkerTimeout start. lost worker : " << workerAddr
                 << ", isDead:" << removeFailWorkerMetaData;
    notifyWorkerManager_->SetFaultWorker(workerAddr);
    if (changePrimaryCopy) {
        ProcessPrimaryCopyByWorkerTimeout(workerAddr);
    }
    if (removeFailWorkerMetaData) {
        {
            // clear remoteclient master address
            std::lock_guard<std::shared_timed_mutex> lck(clientIdRefTableMutex_);
            for (auto &iter : clientIdRefTable_) {
                iter.second.erase(workerAddr);
                objectStore_->RemoveRemoteClientRef(iter.first, workerAddr);
            }
        }
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveMetaByWorker(workerAddr),
                                         "RemoveMetaByWorker failed in ProcessWorkerTimeout");
        std::vector<std::string> refIds;
        globalRefTable_->GetClientRefIds(workerAddr, refIds);
        GDecreaseReqPb req;
        GDecreaseRspPb resp;
        *req.mutable_object_keys() = { refIds.begin(), refIds.end() };
        req.set_address(workerAddr);
        OCMetadataManager::GDecreaseRef(req, resp);
        Status respRc(static_cast<StatusCode>(resp.last_rc().error_code()), resp.last_rc().error_msg());
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(respRc, "GDecreaseRef failed in ProcessWorkerTimeout");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(notifyWorkerManager_->ClearAsyncWorkerOp(workerAddr),
                                         "ClearAsyncWorkerOp failed in ProcessWorkerTimeout");
    }
    return Status::OK();
}

Status OCMetadataManager::ProcessWorkerRestart(const std::string &workerAddr, int64_t timestamp, bool sync)
{
    INJECT_POINT("ProcessWorkerRestart");
    LOG(INFO) << "ProcessWorkerRestart. lost worker : " << workerAddr << ", dbName:" << dbName_;
    WaitInitializaiton();
    notifyWorkerManager_->RemoveFaultWorker(workerAddr);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RemoveMetaByWorker(workerAddr),
                                     "RemoveMetaByWorker failed in ProcessWorkerRestart");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(notifyWorkerManager_->ClearAsyncWorkerOp(workerAddr),
                                     "ClearAsyncWorkerOp failed in ProcessWorkerRestart");
    if (FLAGS_enable_reconciliation) {
        if (sync) {
            notifyWorkerManager_->PushMetaToWorker(workerAddr, timestamp, true);
        } else {
            notifyWorkerManager_->AsyncPushMetaToWorker(workerAddr, timestamp, true);
        }
    }

    if (workerAddr == masterAddress_) {
        ExecuteAsyncTask([this]() { notifyWorkerManager_->ProcessAsyncDeleteNotifyOpImpl(); });
    }
    return Status::OK();
}

Status OCMetadataManager::ProcessWorkerNetworkRecovery(const std::string &workerAddr, int64_t timestamp, bool isOffline)
{
    LOG(INFO) << "ProcessWorkerNetworkRecovery. Lost worker:" << workerAddr << ", isOffline:" << isOffline;
    notifyWorkerManager_->RemoveFaultWorker(workerAddr);
    if (!isOffline) {
        notifyWorkerManager_->AsyncPushMetaToWorker(workerAddr, timestamp, false);
        notifyWorkerManager_->AsyncNotifyOpToWorker(workerAddr, timestamp);
    }
    if (workerAddr == masterAddress_) {
        ExecuteAsyncTask([this]() { notifyWorkerManager_->ProcessAsyncDeleteNotifyOpImpl(); });
    }
    return Status::OK();
}

Status OCMetadataManager::RequestMetaFromWorker(const std::string &masterAddr, const std::string &workerAddr)
{
    LOG(INFO) << "Requesting metadata from worker. Worker meta is lost as the worker is marked Offline";
    RequestMetaFromWorkerRspPb rsp;
    RETURN_IF_NOT_OK(notifyWorkerManager_->RequestMetaFromWorker(masterAddr, dbName_, workerAddr, rsp));
    VLOG(1) << "master RequestMetaFromWorker rsp:" << LogHelper::IgnoreSensitive(rsp);
    for (auto &meta : rsp.metas()) {
        Status s = RecoveryMetaFromWorker(rsp.address(), meta);
        if (s.IsError()) {
            LOG(WARNING) << FormatString("RequestMetaFromWorker failed. objectKey:%s, status:%s", meta.object_key(),
                                         s.ToString());
        }
    }
    std::vector<std::string> objectKeys = { rsp.gref_object_keys().begin(), rsp.gref_object_keys().end() };
    std::vector<std::string> failedIncIds;
    std::vector<std::string> firstIncIds;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        globalRefTable_->GIncreaseRef(rsp.address(), objectKeys, failedIncIds, firstIncIds),
        "GIncreaseRef failed in RequestMetaFromWorker");
    return Status::OK();
}

Status OCMetadataManager::RecoveryMetaFromWorker(const std::string &workerAddr, const ObjectMetaPb &meta)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!workerAddr.empty(), K_INVALID,
                                         "Cannot RecoveryMetaFromWorker with empty worker address.");
    LOG(INFO) << "Start recovery meta from worker, workerAddr:" << workerAddr << ", objectKey:" << meta.object_key();
    const std::string &objectKey = meta.object_key();
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("RecoveryMetaFromWorker get lock", timer.ElapsedMilliSecond());
    TbbMetaTable::accessor accessor;
    auto found = metaTable_.find(accessor, objectKey);
    if (found) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            accessor->second.meta.config().data_format() == meta.config().data_format(), K_INVALID,
            FormatString("Invalid data format of objectKey(%s)", objectKey));
        if (meta.version() < accessor->second.meta.version()) {
            // If the version on the master is newer, send cache invalid message to the worker.
            LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                      static_cast<uint32_t>(NotifyWorkerOpType::CACHE_INVALID), objectKey, workerAddr);
            return notifyWorkerManager_->InsertAsyncWorkerOp(workerAddr, objectKey,
                                                             { NotifyWorkerOpType::CACHE_INVALID }, true,
                                                             WriteMode2MetaType(meta.config().write_mode()));
        }
        (void)accessor->second.locations.emplace(workerAddr);
        accessor.release();
    } else {
        ObjectMeta metaCache;
        metaCache.meta = meta;
        metaCache.meta.set_primary_address(workerAddr);
        // Object key is the key in a key/value pair for the metadata table.
        // Storing the same object key in the "value" part of the kv is redundant and
        // deprecated. Save memory and resources by removing this from the value.
        // The field itself cannot be removed due to down-level support since this ObjectMeta pb
        // is stored on disk (rocksdb). In future it could be fully removed since its not used
        // anymore.
        metaCache.meta.set_allocated_object_key(NULL);
        (void)metaCache.locations.emplace(workerAddr);
        (void)metaTable_.emplace(accessor, objectKey, metaCache);
        std::string serializedStr;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr),
            "serialize meta to rocksdb failed");
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            objectStore_->CreateOrUpdateMeta(objectKey, serializedStr, WriteMode2MetaType(meta.config().write_mode())),
            "Create meta to rocksdb failed");
    }
    return Status::OK();
}

Status OCMetadataManager::ProcessWorkerPushMeta(const PushMetaToMasterReqPb &req, PushMetaToMasterRspPb &rsp)
{
    (void)rsp;
    LOG(INFO) << "Recv PushMetaToMasterReqPb:" << LogHelper::IgnoreSensitive(req);
    for (auto &meta : req.metas()) {
        Status s = RecoveryMetaFromWorker(req.address(), meta);
        if (s.IsError()) {
            LOG(WARNING) << FormatString("RecoveryMetaFromWorker failed. objectKey:%s, status:%s", meta.object_key(),
                                         s.ToString());
        }
    }
    std::vector<std::string> objectKeys = { req.gref_object_keys().begin(), req.gref_object_keys().end() };
    std::vector<std::string> failedIncIds;
    std::vector<std::string> firstIncIds;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        globalRefTable_->GIncreaseRef(req.address(), objectKeys, failedIncIds, firstIncIds),
        "GIncreaseRef failed in ProcessWorkerPushMeta");
    return Status::OK();
}

Status OCMetadataManager::RollbackSeal(const RollbackSealReqPb &req, RollbackSealRspPb &rsp)
{
    (void)rsp;
    LOG(INFO) << FormatString("[ObjectKey %s] Start to rollback seal", req.object_key());
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::accessor accessor;
    if (!metaTable_.find(accessor, req.object_key())) {
        LOG(WARNING) << FormatString("[ObjectKey %s] The object key not exists in metaTable_", req.object_key());
        return Status::OK();
    }
    if (req.old_life_state() == static_cast<uint32_t>(ObjectLifeState::OBJECT_PUBLISHED)) {
        (void)accessor->second.locations.erase(req.address());
        (void)objectStore_->RemoveObjectLocation(req.object_key(), req.address());
        accessor->second.meta.set_life_state(req.old_life_state());
        std::string serializedStr;
        (void)objectStore_->CreateSerializedStringForMeta(req.object_key(), accessor->second.meta, serializedStr);
        (void)objectStore_->CreateOrUpdateMeta(req.object_key(), serializedStr,
                                               WriteMode2MetaType(accessor->second.meta.config().write_mode()));
    } else {
        RETURN_IF_NOT_OK(objectStore_->RemoveMeta(req.object_key()));
        (void)metaTable_.erase(accessor);
    }
    return Status::OK();
}

void OCMetadataManager::AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *masterWorkerService,
                                          object_cache::WorkerWorkerOCServiceImpl *workerWorkerService,
                                          const HostPort &masterAddr)
{
    localApi_ = std::make_unique<object_cache::WorkerLocalWorkerOCApi>(workerWorkerService, akSkManager_);
    notifyWorkerManager_->AssignLocalWorker(masterWorkerService, masterAddr);
    // We set initialized_ as true at the end of OCMetadataManager::AssignLocalWorker, because if
    // OCMetadataManager::AssignLocalWorker is not called, the masterAddr_ in OCNotifyWorkerManager will be empty
    // and thus OCMetadataManager is not fully initialized.
    initialized_.store(true);
}

ObjectMetaStore::WriteType OCMetadataManager::WriteMode2MetaType(uint32_t writeMode)
{
    switch (writeMode) {
        case static_cast<uint32_t>(WriteMode::NONE_L2_CACHE):
            return ObjectMetaStore::WriteType::ROCKS_ONLY;
        case static_cast<uint32_t>(WriteMode::WRITE_BACK_L2_CACHE):
            return ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD;
        case static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE):
            return ObjectMetaStore::WriteType::ROCKS_SYNC_ETCD;
        default:
            return ObjectMetaStore::WriteType::ROCKS_ONLY;
    }
}

void OCMetadataManager::AsyncDeleteByExpired(DeleteObjectMediator &mediator)
{
    // For those objs that do not have metadata on this node, we will notify other az masters in the asynchronous queue
    // to delete the metadata.
    for (auto &objectKey : mediator.GetObjKeys()) {
        uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
        Status rc = expiredObjectManager_->InsertObject(objectKey, version, MIN_TTL_SECOND, true);
        // if object is being delete, don't need to insert again.
        if (rc.IsOk() || rc.GetCode() == K_TRY_AGAIN) {
            mediator.AddSuccessDelId(objectKey);
        } else {
            LOG(ERROR) << FormatString("[ObjKey %s] insert to ExpiredManager failed: %s", objectKey, rc.ToString());
            mediator.AddFailedDelId(objectKey);
            mediator.SetStatus(rc);
        }
    }
}

bool OCMetadataManager::HaveAsyncMetaRequest()
{
    if (objectStore_ == nullptr) {
        return false;
    }
    return !objectStore_->AsyncQueueEmpty();
}

Status OCMetadataManager::SaveMigrationData(const std::string &objectKey, ObjectMeta &metaCache, Status &status)
{
    std::string serializedStr;
    status = objectStore_->CreateSerializedStringForMeta(objectKey, metaCache.meta, serializedStr);
    if (status.IsError()) {
        LOG(WARNING) << "Save migrate data failed. objectKey[" << objectKey << "] Msg: " << status.GetMsg();
        return status;
    }
    status = objectStore_->CreateOrUpdateMeta(objectKey, serializedStr);
    if (status.IsError()) {
        LOG(WARNING) << "Save migrate data failed. objectKey[" << objectKey << "] Msg: " << status.GetMsg();
        return status;
    }

    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("SaveMigrationMetadata get lock", timer.ElapsedMilliSecond());
    (void)metaTable_.insert({ objectKey, std::move(metaCache) });

    return Status::OK();
}

void OCMetadataManager::SaveNestedMigrationMetadata(const MetaForMigrationPb &objMeta)
{
    if (!objMeta.nested_object_keys().empty()) {
        std::set<ImmutableString> nestedObjectKeys = { objMeta.nested_object_keys().begin(),
                                                       objMeta.nested_object_keys().end() };
        LOG_IF_ERROR(nestedRefManager_->IncreaseNestedRefCnt(objMeta.object_key(), nestedObjectKeys),
                     FormatString("IncreaseNested nestedKeys failed, objKey: %s", objMeta.object_key()));
    }
    if (objMeta.nested_ref() > 0) {
        LOG_IF_ERROR(nestedRefManager_->IncreaseNestedRefCnt(objMeta.object_key(), objMeta.nested_ref()),
                     FormatString("IncreaseNested ref failed, objKey: %s", objMeta.object_key()));
    }
}

bool OCMetadataManager::SaveOneMeta(const MetaForMigrationPb &objMeta, Status &status)
{
    ObjectMetaPb metaPb;
    if (!metaPb.ParseFromString(objMeta.meta())) {
        LOG(WARNING) << "Parse MigrationMetadata failed. objectKey:" << objMeta.object_key();
        return false;
    }
    const std::string &objectKey = metaPb.object_key();
    VLOG(1) << "receive migrate object meta:" << objectKey;
    ObjectMeta metaCache;
    metaCache.meta = metaPb;
    for (const auto &loc : objMeta.locations()) {
        LOG_IF_ERROR(AddLocation(metaCache, loc, objectKey, metaPb), "AddLocation failed.");
    }
    metaCache.value = static_cast<int64_t>(objMeta.value());
    if (SaveMigrationData(objectKey, metaCache, status).IsError()) {
        return false;
    }

    uint64_t currentTime = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    // Theoretically, inserts don't fail.
    (void)expiredObjectManager_->InsertObject(objectKey, currentTime, objMeta.remain_ttl_second(),
                                              objMeta.enable_ttl());

    // insert etcd map in memory.
    InsertToEtcdTableInMemory(objectKey, metaPb, ETCD_META_TABLE_PREFIX, objectKey);
    for (const auto &op : objMeta.async_ops()) {
        LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                  static_cast<uint32_t>(op.async_op().op_type()), objectKey, op.worker_addr());
        auto notifyWorkerOp = notifyWorkerManager_->ParseNotifyWorkerOpFromMigration(op.async_op());

        (void)notifyWorkerManager_->InsertAsyncWorkerOp(op.worker_addr(), objectKey, notifyWorkerOp, true,
                                                        WriteMode2MetaType(metaPb.config().write_mode()));
    }

    // global cache delete.
    for (const auto &op : objMeta.global_cache_dels()) {
        VLOG(1) << FormatString("Insert global cache delete for object:%s, version:%d, delete version: %d", objectKey,
                                op.object_version(), op.delete_version());
        (void)globalCacheDeleteManager_->InsertDeletedObject(objectKey, op.object_version(), op.delete_version(), true);
    }
    return true;
}

Status OCMetadataManager::SaveMigrationMetadata(const MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp)
{
    CHECK_FAIL_RETURN_STATUS(etcdCM_->CheckReceiveMigrateInfo(), K_RUNTIME_ERROR,
                             "wait and retry, worker don't receive addnode info");
    LOG(INFO) << "Recv migrate metadata msg. source:" << req.source_addr()
              << ", object count:" << req.object_metas().size();

    auto injectTest = []() {
        INJECT_POINT("master.save_minration_data_failed", []() { return true; });
        return false;
    };
    Status status;
    std::vector<std::string> allRemoteClientIds = { req.remote_client_ids().begin(), req.remote_client_ids().end() };
    for (auto &objMeta : req.object_metas()) {
        if (injectTest()) {
            rsp.add_results(MigrateMetadataRspPb::FAILED);
            continue;
        }
        if (!objMeta.only_ref()) {
            if (!SaveOneMeta(objMeta, status)) {
                rsp.add_results(MigrateMetadataRspPb::FAILED);
                continue;
            }
        }
        // insert global cache delete keys.
        globalCacheDeleteManager_->InsertDeletedObjectFromMigrateNode(objMeta);
        // insert wait for deleted etcd keys.
        for (const auto &ele : objMeta.wait_async_to_l2_elements()) {
            VLOG(1) << FormatString(
                "Save one migrate async element, object key: %s, table: %s, key: %s, request type: %d",
                objMeta.object_key(), ele.table(), ele.key(), ele.op());
            objectStore_->InsertWaitAsyncElements(objMeta.object_key(), ele.table(), ele.key(), ele.value(),
                                                  static_cast<AsyncElement::ReqType>(ele.op()), ele.timestamp(),
                                                  ele.trace_id());
        }
        if (!SaveOneMigrationObjRefData(objMeta.object_key(), objMeta, allRemoteClientIds)) {
            rsp.add_results(MigrateMetadataRspPb::FAILED);
            continue;
        }
        SaveNestedMigrationMetadata(objMeta);

        rsp.add_results(MigrateMetadataRspPb::SUCCESSFUL);
    }
    return Status::OK();
}

Status OCMetadataManager::AddLocation(ObjectMeta &metaCache, const std::string &addr, const std::string objectKey,
                                      const ObjectMetaPb &metaPb)
{
    if (addr.empty()) {
        return { K_INVALID, "The location address is empty." };
    }
    metaCache.locations.emplace(addr);
    std::string key = addr + "_" + objectKey;
    InsertToEtcdTableInMemory(objectKey, metaPb, ETCD_LOCATION_TABLE_PREFIX, key);
    return Status::OK();
}

void OCMetadataManager::InsertToEtcdTableInMemory(const std::string &objectKey, const ObjectMetaPb &metaPb,
                                                  const std::string &tableName, const std::string &key)
{
    // insert etcd map in memory.
    auto writeType = WriteMode2MetaType(metaPb.config().write_mode());
    if (writeType != ObjectMetaStore::WriteType::ROCKS_ONLY) {
        uint32_t hash;
        std::string table;
        objectStore_->GetHashAndTable(objectKey, tableName, hash, table);
        objectStore_->InsertToEtcdKeyMap(table, key, hash, writeType == ObjectMetaStore::WriteType::ROCKS_ASYNC_ETCD);
    }
}

Status OCMetadataManager::ClearDataWithoutMeta(const worker::HashRange &ranges, const std::string &workerAddr,
                                               const worker::HashRange &halfCompletedRanges,
                                               const std::vector<std::string> &uuids)
{
    HostPort hostPort;
    RETURN_IF_NOT_OK(hostPort.ParseString(workerAddr));
    if (etcdCM_->CheckConnection(hostPort).IsError()) {
        return Status::OK();
    }
    std::vector<std::string> objsMigrateFinished;
    if (!halfCompletedRanges.empty()) {
        GetMetasMatch([this, &halfCompletedRanges](
                          const std::string &objKey) { return etcdCM_->IsInRange(halfCompletedRanges, objKey, ""); },
                      objsMigrateFinished);
    }
    return notifyWorkerManager_->ClearDataWithoutMeta(ranges, workerAddr, objsMigrateFinished, uuids);
}

Status OCMetadataManager::ClearDevClientMetaForScaledInWorker(const std::vector<std::string> &removeNodes)
{
    MetaAddrInfo metaAddrInfo;
    auto rc = etcdCM_->GetMetaAddress(P2P_DEFAULT_MASTER, metaAddrInfo);
    // The master node managing heterogeneous metadata has voluntarily scaled down.
    if (rc.GetCode() == StatusCode::K_RPC_UNAVAILABLE) {
        return Status::OK();
    }

    // If this node is the heterogeneous metadata master, clean up client metadata
    // associated with the scaled-in worker nodes
    auto devMasterAddr = metaAddrInfo.GetAddressAndSaveDbName().ToString();
    auto localWorkerAddr = etcdCM_->GetWorkerAddress();
    if (localWorkerAddr == devMasterAddr) {
        return masterDevOcManager_->ReleaseClientMetaForScaledInWorker(removeNodes);
    }
    return Status::OK();
}

bool OCMetadataManager::CheckMetaTableEmpty()
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    return metaTable_.empty();
}

void OCMetadataManager::FillWaitAsyncElements(const std::unordered_set<std::shared_ptr<AsyncElement>> &elements,
                                              MetaForMigrationPb &meta)
{
    for (const auto &ele : elements) {
        if (ele == nullptr) {
            continue;
        }
        VLOG(1) << "Migrate async element: " << *ele;
        auto *eleOp = meta.add_wait_async_to_l2_elements();
        eleOp->set_key(ele->Key());
        eleOp->set_table(ele->Table());
        eleOp->set_value(ele->Value());
        eleOp->set_op(static_cast<uint32_t>(ele->RequestType()));
        eleOp->set_timestamp(ele->BeginTimestampUs());
        eleOp->set_trace_id(ele->TraceID());
    }
}

Status OCMetadataManager::FillMetadataForMigration(
    const std::string &objectKey, MetaForMigrationPb &meta,
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncMap)
{
    {
        INJECT_POINT("check.expiredObject", [this, &objectKey, &meta] {
            uint32_t time = 0;
            if (expiredObjectManager_->GetObjectRemainTimeAndRemove(objectKey, time).IsOk()) {
                meta.set_enable_ttl(true);
            }
            return Status::OK();
        });
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        auto found = metaTable_.find(accessor, objectKey);
        // Check for object end of life
        if (!found) {
            RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, FormatString("Can't find the object[%s] meta.", objectKey));
        }
        VLOG(1) << "FillMetadataForMigration obj: " << objectKey;
        std::string serializedStr;
        RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr));

        // Marker is migrating
        migratingItems_.insert({ objectKey, true });
        if (accessor->second.IsWriteBackL2Cache() && !(FLAGS_enable_meta_replica && !etcdCM_->IsCentralized())) {
            std::unordered_set<std::shared_ptr<AsyncElement>> elements;
            GetAsyncElementsByObjectKey(objectKey, elements);
            FillWaitAsyncElements(elements, meta);
            asyncMap.emplace(objectKey, std::move(elements));
        }

        // Fill metadata
        meta.set_object_key(objectKey);
        meta.set_meta(serializedStr);
        for (const auto &loc : accessor->second.locations) {
            meta.add_locations(loc);
        }
        meta.set_value(accessor->second.value);
    }
    uint32_t remainingTime = 0;
    if (expiredObjectManager_->GetObjectRemainTimeAndRemove(objectKey, remainingTime).IsOk()
        || expiredObjectManager_->CheckObjectInAsyncDeleteWithLock(objectKey)) {
        meta.set_remain_ttl_second(remainingTime);
        meta.set_enable_ttl(true);
    }

    auto operations = notifyWorkerManager_->GetObjectAsyncWorkerOp(objectKey);
    for (const auto &op : operations) {
        auto opPb = meta.add_async_ops();
        opPb->set_worker_addr(op.first);
        auto opDetailPb = *opPb->mutable_async_op();
        opDetailPb.set_op_type(static_cast<uint32_t>(op.second.type));
        opDetailPb.set_remove_meta_version(op.second.removeMetaVersion);
        *opDetailPb.mutable_remove_meta_az_names() = { op.second.removeMetaAzNames.begin(),
                                                       op.second.removeMetaAzNames.end() };
        opDetailPb.set_delete_all_copy_version(op.second.deleteAllCopyMetaVersion);
        *opDetailPb.mutable_delete_all_copy_az_names() = { op.second.deleteAllCopyMetaAzNames.begin(),
                                                           op.second.deleteAllCopyMetaAzNames.end() };
        (void)notifyWorkerManager_->RemoveAsyncWorkerOp(op.first, { objectKey }, op.second.type, true);
    }

    auto delOps = globalCacheDeleteManager_->GetDeletedInfos(objectKey);
    for (const auto &op : delOps) {
        auto opPb = meta.add_global_cache_dels();
        opPb->set_object_version(op.first);
        opPb->set_delete_version(op.second);
    }
    return Status::OK();
}

void OCMetadataManager::HandleMetaDataMigrationFailed(
    const MetaForMigrationPb &objMeta,
    const std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncMap)
{
    expiredObjectManager_->InsertObject(objMeta.object_key(), GetSystemClockTimeStampUs(), objMeta.remain_ttl_second(),
                                        objMeta.enable_ttl());
    for (auto &async_op : objMeta.async_ops()) {
        // FillMetadataForMigration does not delete data from etcd. Therefore, type is set to ROCKS_ONLY.
        LOG(INFO) << FormatString("Insert async worker operation(%d) for object:%s, workerId:%s",
                                  static_cast<uint32_t>(async_op.async_op().op_type()), objMeta.object_key(),
                                  async_op.worker_addr());
        auto notifyWorkerOp = notifyWorkerManager_->ParseNotifyWorkerOpFromMigration(async_op.async_op());
        (void)notifyWorkerManager_->InsertAsyncWorkerOp(async_op.worker_addr(), objMeta.object_key(), notifyWorkerOp,
                                                        true, ObjectMetaStore::WriteType::ROCKS_ONLY);
    }

    auto it = asyncMap.find(objMeta.object_key());
    if (it == asyncMap.end()) {
        VLOG(2) << "Handle migrate failed obejct: " << objMeta.object_key()
                << ", not found in async map, async map size: " << asyncMap.size();
        return;
    }
    for (const auto &ele : it->second) {
        VLOG(1) << FormatString("Handle migrate failed object %s, table: %s, key: %s pull back to object meta store",
                                objMeta.object_key(), ele->Table(), ele->Key());
        objectStore_->InsertWaitAsyncElements(objMeta.object_key(), ele->Table(), ele->Key(), ele->Value(),
                                              ele->RequestType(), ele->BeginTimestampUs(), ele->TraceID());
    }
}

void OCMetadataManager::HandleMetaDataMigrationSuccess(const std::string &objectKey)
{
    Raii outer([this, &objectKey]() { migratingItems_.erase(objectKey); });

    {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::const_accessor accessor;
        auto found = metaTable_.find(accessor, objectKey);
        // Check for object end of life
        if (!found) {
            return;
        }

        Status rc = ClearOneMetaInfo(accessor, true);
        if (rc.IsError()) {
            LOG(WARNING) << "Failed to delete migrated data. rc=" << rc.ToString();
        }
        (void)metaTable_.erase(accessor);
    }
}

bool OCMetadataManager::MetaIsFound(const std::string &objectKey)
{
    {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::const_accessor accessor;
        if (metaTable_.find(accessor, objectKey)) {
            return true;
        }
    }
    {
        std::shared_lock<std::shared_timed_mutex> lck(subTableMutex_);
        TbbReqIdTable::const_accessor accessor;
        if (objKey2ReqId_.find(accessor, objectKey)) {
            return true;
        }
    }
    return false;
}

Status OCMetadataManager::RecoverAsyncTask(const std::vector<std::string> &workerUuids,
                                           const worker::HashRange &extraRanges)
{
    RETURN_IF_NOT_OK_APPEND_MSG(globalCacheDeleteManager_->RecoverDeletedIds(false, workerUuids, extraRanges),
                                "Failed to recover deleting objects from etcd.");
    RETURN_IF_NOT_OK_APPEND_MSG(notifyWorkerManager_->RecoverCacheInvalidAndRemoveMeta(false, workerUuids, extraRanges),
                                "Failed to recover async worker option from etcd.");
    return Status::OK();
}

Status OCMetadataManager::RecoverDataOfFaultyWorker(const std::vector<std::string> &workerUuids,
                                                    const worker::HashRange &extraRanges)
{
    if (workerUuids.empty() && extraRanges.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK_APPEND_MSG(LoadMeta(false, workerUuids, extraRanges), "Failed to recover metas from etcd.");
    return RecoverAsyncTask(workerUuids, extraRanges);
}

void OCMetadataManager::WaitInitializaiton()
{
    using namespace std::chrono;
    static constexpr int CHECK_INTERVAL_MS = 20;  // check every 20 ms
    static constexpr int LOG_INTERVAL_S = 10;     // print log every 10 seconds
    static constexpr int LOG_INTERVAL_TIMES =
        LOG_INTERVAL_S * 1000 / CHECK_INTERVAL_MS;  // interval times in 10 seconds
    int intervalTimes = 0;
    int logTimes = 0;

    if (!initialized_.load()) {
        LOG(INFO) << "Infinitely wait for the initialization of OCMetadataManager complete...";
    }
    // Since the completed initialization of this OCMetadataManager is a prerequisite of worker's health status, and the
    // time to wait is relative to the size of data loaded from database, here set a deadloop waiting.
    while (!initialized_.load() && !interruptFlag_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_INTERVAL_MS));
        ++intervalTimes;
        if (intervalTimes == LOG_INTERVAL_TIMES) {
            ++logTimes;
            LOG(WARNING) << FormatString("Has waited OCMetadataManager for %d seconds, continue waiting...",
                                         logTimes * LOG_INTERVAL_S);
            intervalTimes = 0;
        }
    }
}

void OCMetadataManager::TryGetObjectData(const std::string &objectKey, const TbbMetaTable::accessor &accessor,
                                         uint64_t &payloadSize, QueryMetaInfoPb &queryMeta,
                                         std::vector<RpcMessage> &payloads)
{
    // don't try to query data if enable UB.
    if (IsUrmaEnabled()) {
        return;
    }
    INJECT_POINT("ocMetaManager.noNeedGetFromLocal", []() { return; });
    constexpr uint64_t maxPayloadSize = 512 * 1024ul;
    if (localApi_ == nullptr) {
        return;
    }
    uint64_t dataSize = accessor->second.meta.data_size();
    if (payloadSize > maxPayloadSize || dataSize > maxPayloadSize - payloadSize) {
        return;
    }
    auto iter = accessor->second.locations.find(masterAddress_);
    if (iter == accessor->second.locations.end()) {
        return;
    }
    if (notifyWorkerManager_->CheckExistAsyncWorkerOp(masterAddress_, objectKey, NotifyWorkerOpType::CACHE_INVALID)
        || notifyWorkerManager_->CheckExistAsyncWorkerOp(masterAddress_, objectKey,
                                                         NotifyWorkerOpType::PRIMARY_COPY_INVALID)) {
        return;
    }
    GetObjectRemoteReqPb req;
    GetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> tmpPayloads;
    // We only try to get the object lock to avoid concurrency conflict. If we lock failed,
    // it's Ok and we will tell the worker we can not get the object data directly.
    req.set_try_lock(true);
    req.set_object_key(objectKey);
    req.set_version(accessor->second.meta.version());
    Status status = akSkManager_->GenerateSignature(req);
    if (status.IsError()) {
        LOG(WARNING) << FormatString("[ObjectKey %s] Failed to generate signature, detail: %s", objectKey, status);
        return;
    }
    uint32_t curIndex = payloads.size();
    status = localApi_->GetObjectRemote(req, rsp, tmpPayloads);
    if (status.IsError() || tmpPayloads.empty()) {
        // If we meets K_UNKNOWN_ERROR, it means worker want us to erase the location because the object is bad.
        if (status.GetCode() == StatusCode::K_UNKNOWN_ERROR) {
            INJECT_POINT("master.query_meta.get_object_remote", []() { return; });
            (void)accessor->second.locations.erase(masterAddress_);
        }
        LOG(WARNING) << FormatString("[ObjectKey %s] Failed to get data directly, detail: %s", objectKey, status);
        return;
    }
    payloads.insert(payloads.end(), std::make_move_iterator(tmpPayloads.begin()),
                    std::make_move_iterator(tmpPayloads.end()));
    for (uint32_t i = curIndex; i < payloads.size(); ++i) {
        queryMeta.add_payload_indexs(i);
    }
    payloadSize += dataSize;
}

bool OCMetadataManager::AddHeavyOp(const std::string &objectKey)
{
    std::lock_guard<std::mutex> l(heavyOpMutex_);
    auto result = heavyOps_.emplace(objectKey);
    return result.second;
}

bool OCMetadataManager::AddHeavyOp(const std::vector<std::string> &objectKeys)
{
    std::lock_guard<std::mutex> l(heavyOpMutex_);
    std::unordered_set<std::string> deduplicateIds{ objectKeys.begin(), objectKeys.end() };
    for (auto it = deduplicateIds.begin(); it != deduplicateIds.end(); ++it) {
        const auto &objectKey = *it;
        auto result = heavyOps_.emplace(objectKey);
        if (result.second) {
            continue;
        }

        for (auto newIt = deduplicateIds.begin(); newIt != it; ++newIt) {
            (void)heavyOps_.erase(*newIt);
        }
        return false;
    }
    return true;
}

void OCMetadataManager::RemoveHeavyOp(const std::vector<std::string> &objectKeys)
{
    std::lock_guard<std::mutex> l(heavyOpMutex_);
    std::unordered_set<std::string> deduplicateIds{ objectKeys.begin(), objectKeys.end() };
    for (const auto &objectKey : deduplicateIds) {
        (void)heavyOps_.erase(objectKey);
    }
}

std::string OCMetadataManager::GetETCDAsyncQueueUsage()
{
    if (objectStore_ == nullptr) {
        return "";
    }
    return objectStore_->GetETCDAsyncQueueUsage();
}

std::string OCMetadataManager::GetMasterAsyncPoolUsage()
{
    if (asyncPool_ == nullptr) {
        return "";
    }
    return asyncPool_->GetThreadPoolUsage().ToString();
}

Status OCMetadataManager::CreateDeviceMeta(const ObjectMetaPb &newMeta, const std::string &address)
{
    const std::string &objectKey = newMeta.object_key();
    ObjectMeta objMeta;
    objMeta.meta = newMeta;
    objMeta.meta.set_primary_address(address);
    objMeta.locations.emplace(address);
    LOG(INFO) << "Master create device meta: object_key: " << objectKey << ", worker_address: " << address;
    {
        std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
        TbbMetaTable::accessor accessor;
        auto found = metaTable_.find(accessor, objectKey);
        if (found) {
            return { K_INVALID, FormatString("The object: %s is existed in metaTable.", objectKey) };
        } else if (!metaTable_.emplace(accessor, objectKey, objMeta)) {
            return { K_INVALID, FormatString("The object: %s is existed in metaTable.", objectKey) };
        }
    }
    std::string serializedStr;
    RETURN_IF_NOT_OK(objectStore_->CreateSerializedStringForMeta(objectKey, objMeta.meta, serializedStr));
    UpdateSubscribeCache(objectKey, objMeta);
    return objectStore_->CreateOrUpdateMeta(objectKey, serializedStr);
}

Status OCMetadataManager::CheckRocksdbStatusAndLoadL2Table(const std::string &tablePrefix,
                                                           const std::string &rocksTable, bool isFromRocksdb,
                                                           const std::vector<std::string> &workerUuids,
                                                           const worker::HashRange &extraRanges,
                                                           std::vector<std::pair<std::string, std::string>> &outMetas)
{
    if (!objectStore_->IsRocksdbRunning()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            objectStore_->GetFromEtcd(tablePrefix, rocksTable, workerUuids, extraRanges, outMetas),
            "Load meta from etcd into memory failed.");
        LOG(INFO) << "Load meta from etcd and try put to rocksdb, count:" << outMetas.size();
        for (const auto &iter : outMetas) {
            RETURN_IF_NOT_OK(objectStore_->PutToRocksStore(rocksTable, iter.first, iter.second));
        }
    } else {
        if (isFromRocksdb && objectStore_->IsRocksdbEnableWriteMeta()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectStore_->GetAllFromRocks(rocksTable, outMetas),
                                             "Load meta from rocksdb into memory failed.");
            LOG(INFO) << "Load meta from rocksdb, count:" << outMetas.size();
        } else {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                objectStore_->GetFromEtcd(tablePrefix, rocksTable, workerUuids, extraRanges, outMetas),
                "Load meta from etcd into memory failed.");
            LOG(INFO) << "Load meta from etcd, count:" << outMetas.size();
        }
    }
    return Status::OK();
}

Status OCMetadataManager::ReplacePrimary(const ReplacePrimaryReqPb &req, ReplacePrimaryRspPb &rsp)
{
    INJECT_POINT("OCMetadataManager.ReplacePrimary");
    std::vector<std::string> notRedirectObjectKeys;
    std::transform(req.object_infos().begin(), req.object_infos().end(), std::back_inserter(notRedirectObjectKeys),
                   [](const ReplacePrimaryReqPb::ObjectInfoPb &info) { return info.object_key(); });
    FillRedirectResponseInfos(rsp, notRedirectObjectKeys, req.redirect());
    if (rsp.meta_is_moving()) {
        return Status::OK();
    }

    std::unordered_set<std::string> notRedirectSet{ notRedirectObjectKeys.begin(), notRedirectObjectKeys.end() };
    const auto &infos = req.object_infos();
    VLOG(1) << "Replace primary process object size: " << notRedirectSet.size();
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    for (const auto &info : infos) {
        const auto &objectKey = info.object_key();
        if (notRedirectSet.find(objectKey) == notRedirectSet.end()) {
            VLOG(1) << FormatString("[ObjectKey %s] The object key has been redirect, skip it", objectKey);
            continue;
        }

        uint64_t version = info.version();
        TbbMetaTable::accessor accessor;
        bool isDeleting = false;
        {
            std::shared_lock<std::shared_mutex> l(isDeletingObjMutex_);
            isDeleting = isDeletingObjs_.count(objectKey) > 0;
        }
        if (!metaTable_.find(accessor, objectKey) || isDeleting) {
            VLOG(1) << FormatString("[ObjectKey %s] The object key not exists in metaTable, skip it", objectKey);
            rsp.add_expired_ids(objectKey);
            continue;
        }

        if (accessor->second.meta.version() != version
            || accessor->second.meta.primary_address() != req.origin_primary_addr()) {
            LOG(WARNING) << FormatString(
                "[ObjectKey %s] The object has been updated, expect version: %ld, now version: %ld; expect primary: "
                "%s, "
                "now primary: %s, skip it",
                objectKey, version, accessor->second.meta.version(), req.origin_primary_addr(),
                accessor->second.meta.primary_address());
            rsp.add_expired_ids(objectKey);
            continue;
        }

        accessor->second.meta.set_primary_address(req.new_primary_addr());
        accessor->second.locations.emplace(req.new_primary_addr());
        rsp.add_success_ids(objectKey);
        VLOG(1) << FormatString("[ObjectKey %s] Change primary copy from %s to %s", objectKey,
                                req.origin_primary_addr(), req.new_primary_addr());
        std::string serializedStr;
        LOG_IF_ERROR(objectStore_->CreateSerializedStringForMeta(objectKey, accessor->second.meta, serializedStr),
                     "serialize meta to rocksdb failed");
        LOG_IF_ERROR(objectStore_->CreateOrUpdateMeta(objectKey, serializedStr,
                                                      WriteMode2MetaType(accessor->second.meta.config().write_mode())),
                     "Create meta to rocksdb failed");
    }
    return Status::OK();
}

Status OCMetadataManager::PureQueryMeta(const PureQueryMetaReqPb &req, PureQueryMetaRspPb &rsp)
{
    std::vector<std::string> notRedirectObjectKeys = { req.object_keys().begin(), req.object_keys().end() };
    FillRedirectResponseInfos(rsp, notRedirectObjectKeys, req.redirect());
    if (rsp.meta_is_moving()) {
        return Status::OK();
    }
    Timer timer;
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    masterOperationTimeCost.Append("PureQueryMeta get lock", timer.ElapsedMilliSecond());
    for (const auto &objectKey : notRedirectObjectKeys) {
        TbbMetaTable::const_accessor accessor;
        if (metaTable_.find(accessor, objectKey) && accessor->second.multiSetState != PENDING) {
            auto *queryMeta = rsp.add_query_metas();
            queryMeta->mutable_meta()->CopyFrom(accessor->second.meta);
            queryMeta->mutable_meta()->set_object_key(objectKey);
        } else {
            LOG(WARNING) << FormatString("QueryMeta and not found: %s", objectKey);
        }
    }
    return Status::OK();
}

std::shared_ptr<MasterDevOcManager> OCMetadataManager::GetDeviceOcManager()
{
    return masterDevOcManager_;
}

Status OCMetadataManager::RollbackMultiMeta(const RollbackMultiMetaReqPb &req, RollbackMultiMetaRspPb &rsp)
{
    std::vector<std::string> objectKeys;
    for (const auto &objKey : req.object_keys()) {
        objectKeys.emplace_back(objKey);
    }
    std::sort(objectKeys.begin(), objectKeys.end());  // To prevent deadlock.
    FillRedirectResponseInfos(rsp, objectKeys, req.redirect());
    RETURN_OK_IF_TRUE(!rsp.info().empty());
    for (int i = 0; (size_t)i < objectKeys.size(); ++i) {
        const auto &objKey = objectKeys[i];
        if (req.persistence_only()) {
            if (i >= req.versions_size()) {
                RETURN_STATUS(K_RUNTIME_ERROR, "No object version found");
            }
            if (globalCacheDeleteManager_->InsertDeletedObject(objKey, req.versions(i), req.versions(i), true)
                    .IsError()) {
                rsp.add_failed_object_keys(objKey);
            }
        } else {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::accessor accessor;
            if (!metaTable_.find(accessor, objKey)) {
                LOG(INFO) << FormatString("[ObjectKey %s] Skip rollback because not in the meta table", objKey);
                continue;
            }
            if (req.address() != accessor->second.meta.primary_address()) {
                LOG(INFO) << FormatString(
                    "[ObjectKey %s] Skip rollback because address mismatch, current address is %s", objKey,
                    accessor->second.meta.primary_address());
                continue;
            }
            if (objectStore_->RemoveMeta(objKey).IsError()) {
                rsp.add_failed_object_keys(objKey);
                continue;
            }
            (void)metaTable_.erase(accessor);
        }
    }
    return Status::OK();
}

int OCMetadataManager::GetL2CacheType(const std::string &objKey)
{
    std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
    TbbMetaTable::const_accessor accessor;
    return metaTable_.find(accessor, objKey) ? accessor->second.GetL2CacheType() : INT_MAX;
}

bool OCMetadataManager::CheckIfUpdating(const std::string &objKey, int64_t &version)
{
    std::shared_lock<std::shared_timed_mutex> lck(updatingObjsTableMutex_);
    auto iter = updatingObjsTable_.find(objKey);
    if (iter != updatingObjsTable_.end()) {
        version = iter->second;
        return true;
    }
    return false;
}

bool OCMetadataManager::GetObjectVersion(const std::string &objKey, int64_t &version)
{
    std::shared_lock<std::shared_timed_mutex> rlck(metaTableMutex_);
    TbbMetaTable::const_accessor rAccessor;
    if (!metaTable_.find(rAccessor, objKey)) {
        return false;
    }
    version = static_cast<int64_t>(rAccessor->second.meta.version());
    return true;
}

Status OCMetadataManager::Expire(const ExpireReqPb &req, ExpireRspPb &rsp)
{
    std::vector<std::string> notRedirectObjectKeys = { req.object_keys().begin(), req.object_keys().end() };
    FillRedirectResponseInfos(rsp, notRedirectObjectKeys, req.redirect());
    std::set<std::string> sortedObjectKeys = { notRedirectObjectKeys.begin(), notRedirectObjectKeys.end() };
    std::vector<TbbLockTable::accessor> pLocks(sortedObjectKeys.size());
    int pos = 0;
    for (const auto &objectKey : sortedObjectKeys) {
        (void)processLocks_.insert(pLocks[pos], objectKey);
        pos++;
    }

    Raii eraseLocks([this, &pLocks]() {
        for (auto &l : pLocks) {
            processLocks_.erase(l);
            l.release();
        }
    });
    auto &objectKeys = req.object_keys();
    Status lastRc;
    std::vector<std::string> notExistObjectKeys;
    for (auto it = objectKeys.begin(); it != objectKeys.end(); ++it) {
        std::string objectKey = *it;
        {
            std::shared_lock<std::shared_timed_mutex> lck(metaTableMutex_);
            TbbMetaTable::accessor accessor;
            if (!metaTable_.find(accessor, objectKey)) {
                LOG(INFO) << "The object " << objectKey << " was not found in metaTable_.";
                notExistObjectKeys.emplace_back(objectKey);
                continue;
            }
            accessor->second.meta.set_ttl_second(req.ttl_second());
        }
        uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
        auto rc = expiredObjectManager_->InsertObject(objectKey, version, req.ttl_second());
        if (rc.IsError()) {
            LOG(WARNING) << "Faied to insert object[" << objectKey << "] with new ttl second.";
            rsp.add_failed_object_keys(objectKey);
            lastRc = rc;
        }
    }
    FillRedirectResponseInfos(rsp, notExistObjectKeys, req.redirect());
    if (!notExistObjectKeys.empty()) {
        lastRc = Status(K_NOT_FOUND,
                        FormatString("[Expire] Some object can not be found in metaTable_, size: %d, object: %s",
                                     notExistObjectKeys.size(), VectorToString(notExistObjectKeys)));
        *rsp.mutable_absent_object_keys() = { notExistObjectKeys.begin(), notExistObjectKeys.end() };
    }
    rsp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    rsp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
    return Status::OK();
}
}  // namespace master
}  // namespace datasystem
