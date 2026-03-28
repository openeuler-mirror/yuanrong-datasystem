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
 * Description: The replica manager implement.
 */

#include "datasystem/master/replica_manager.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include <etcd/api/mvccpb/kv.pb.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/master/replica_rpc_channel_impl.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

DS_DEFINE_bool(enable_meta_replica, false, "Controls whether to enable multiple meta replica");
DS_DECLARE_uint32(rolling_update_timeout_s);
DS_DECLARE_string(rocksdb_write_mode);

namespace datasystem {
const std::string REPLICA_MANAGER = "ReplicaManager";
namespace {
ReplicaGroupPb CreateReplicaGroupPb(const std::string &primaryWorkerUuid, const std::set<std::string> &workerUuids)
{
    ReplicaGroupPb replicaGroupPb;
    for (const auto &workerUuid : workerUuids) {
        replicaGroupPb.add_replicas()->set_worker_id(workerUuid);
    }
    replicaGroupPb.set_primary_id(primaryWorkerUuid);
    return replicaGroupPb;
}
}  // namespace

void MetadataManager::Shutdown()
{
    if (oc != nullptr) {
        oc->Shutdown();
    }
    if (sc != nullptr) {
        sc->Shutdown();
    }
}

ReplicaManager::~ReplicaManager()
{
    ReplicaEvent::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    HashRingEvent::ClusterInitFinish::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    NodeTimeoutEvent::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    NodeNetworkRecoveryEvent::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    HashRingEvent::ScaleupFinish::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    HashRingEvent::ScaleDownFinish::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    HashRingEvent::VoluntaryScaleDownFinsih::GetInstance().RemoveSubscriber(REPLICA_MANAGER);
    if (cleanTimer_) {
        (void)TimerQueue::GetInstance()->Cancel(*cleanTimer_);
        cleanTimer_.reset();
    }
    stop_ = true;
}

Status ReplicaManager::Init(ReplicaManagerParam param)
{
    LOG(INFO) << "Init replica manager.";
    if (FLAGS_enable_meta_replica && FLAGS_rocksdb_write_mode == "none") {
        RETURN_STATUS(StatusCode::K_INVALID,
                      "When using enable_meta_replica, rocksdb_write_mode cannot be set to none.");
    }
    dbRootPath_ = std::move(param.dbRootPath);
    currentWorkerId_ = std::move(param.currWorkerId);
    akSkManager_ = param.akSkManager;
    etcdStore_ = param.etcdStore;
    persistenceApi_ = param.persistenceApi;
    masterAddress_ = param.masterAddress;
    etcdCM_ = param.etcdCM;
    masterWorkerService_ = param.masterWorkerService;
    workerWorkerService_ = param.workerWorkerService;
    rpcSessionManager_ = param.rpcSessionManager;
    isOcEnabled_ = param.isOcEnabled;
    isScEnabled_ = param.isScEnabled;
    bool multiReplicaEnabled = MultiReplicaEnabled();
    if (multiReplicaEnabled) {
        const int queueSize = 1024;
        eventQue_ = std::make_unique<Queue<mvccpb::Event>>(queueSize);
        const int maxThreadCount = 2;
        RETURN_IF_EXCEPTION_OCCURS(threadPool_ = std::make_unique<ThreadPool>(0, maxThreadCount, "ReplicaMgr"));
        threadPool_->Execute(&ReplicaManager::HandleEvent, this);
        auto func = [this](const std::string &workerUuid, HostPort &workerAddr) {
            auto hashRing = etcdCM_->GetHashRing();
            return hashRing->GetWorkerAddrByUuidForMultiReplica(workerUuid, workerAddr);
        };
        channel_ = std::make_unique<ReplicaRpcChannelImpl>(akSkManager_, std::move(func));
    }

    bool isNewNode = etcdCM_ == nullptr ? true : etcdCM_->IsNewNode();
    bool isCentralized = etcdCM_ == nullptr ? false : etcdCM_->IsCentralized();
    if (isNewNode && !isCentralized) {
        isNewNode_ = true;
        LOG(INFO) << "Newly-added node, remove backend store if exist.";
        RETURN_IF_NOT_OK(Replica::RemoveRocksFromFileSystem(dbRootPath_, multiReplicaEnabled));
    } else {
        isNewNode_ = false;
        LOG(INFO) << "Restart or centralized node, try recovery from backend store.";
    }
    SetCleanMapTask();
    SubscribeEvent();
    return Status::OK();
}

void ReplicaManager::SubscribeEvent()
{
    ReplicaMagagerEvent::GetPrimaryReplicaInfoInWorker::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::string &workerUuid, std::map<std::string, std::string> &replicaInfos) {
            return GetPrimaryReplicaInfoInWorker(workerUuid, replicaInfos);
        });

    ReplicaMagagerEvent::GetPrimaryReplicaLocation::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::string &srcWorkerUuid, std::string &destWorkerUuid) {
            return GetPrimaryReplicaLocation(srcWorkerUuid, destWorkerUuid);
        });

    ReplicaMagagerEvent::GetPrimaryReplicaDbNames::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::string &workerUuid, std::vector<std::string> &dbNames) {
            return GetPrimaryReplicaDbNames(workerUuid, dbNames);
        });

    if (!MultiReplicaEnabled()) {
        return;
    }
    ReplicaEvent::GetInstance().AddSubscriber(REPLICA_MANAGER,
                                              [this](mvccpb::Event &event) { return EnqueEvent(event); });

    HashRingEvent::ClusterInitFinish::GetInstance().AddSubscriber(REPLICA_MANAGER, [this](const std::string
                                                                                              &primaryWorkerId,
                                                                                          const std::string
                                                                                              &standbyWorkerId) {
        if (currentWorkerId_ != primaryWorkerId) {
            return;
        }
        INJECT_POINT("worker.ClusterInitFinish", [&] {
            ReplicaGroupPb replicaGroupPb = CreateReplicaGroupPb(standbyWorkerId, { primaryWorkerId, standbyWorkerId });
            LOG_IF_ERROR(PutReplicaGroupToEtcd(primaryWorkerId, replicaGroupPb), "PutReplicaGroupToEtcd failed");
        });

        LOG_IF_ERROR(AdjustReplicaLocationImpl(primaryWorkerId, { standbyWorkerId }),
                     "AdjustReplicaLocationImpl failed");
    });

    NodeTimeoutEvent::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::string &workerAddr, bool, bool, bool isOtherAzNode) {
            if (!isOtherAzNode) {
                LOG_IF_ERROR(HandleNodeTimeout(workerAddr), "HandleNodeTimeout failed");
            }
            return Status::OK();
        });
    NodeNetworkRecoveryEvent::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::string &workerAddr, int64_t, bool) {
            LOG_IF_ERROR(HandleNodeNetworkRecovery(workerAddr), "HandleNodeNetworkRecovery failed");
            return Status::OK();
        });

    HashRingEvent::ScaleDownFinish::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::vector<std::string> &removeWorkerUuids) {
            LOG_IF_ERROR(HandleNodeScaleDownFinish(removeWorkerUuids), "HandleNodeScaleupFinish failed");
        });

    HashRingEvent::ScaleupFinish::GetInstance().AddSubscriber(REPLICA_MANAGER, [this](const std::string &workerUuid) {
        LOG_IF_ERROR(HandleNodeScaleupFinish(workerUuid), "HandleNodeScaleupFinish failed");
    });

    HashRingEvent::VoluntaryScaleDownFinsih::GetInstance().AddSubscriber(
        REPLICA_MANAGER, [this](const std::string &workerUuid) {
            LOG_IF_ERROR(HandleVoluntaryScaleDownFinish(workerUuid), "HandleVoluntaryScaleDownFinish failed");
        });
}

bool ReplicaManager::MultiReplicaEnabled()
{
    return FLAGS_enable_meta_replica && (etcdCM_ == nullptr || !etcdCM_->IsCentralized());
}

Status ReplicaManager::CreateMetaManager(const std::string &dbName, RocksStore *objectRocksStore,
                                         RocksStore *streamRocksStore)
{
    (void)streamRocksStore;
    auto iter = metadataManagers_.find(dbName);
    if (iter == metadataManagers_.end()) {
        Timer timer;
        double ocElapsed = 0;
        double scElapsed = 0;
        MetadataManager metadataManager;
        if (isOcEnabled_) {
            // create OCMetadataManager instance
            auto oc =
                std::make_shared<master::OCMetadataManager>(akSkManager_, objectRocksStore, etcdStore_, persistenceApi_,
                                                            masterAddress_.ToString(), etcdCM_, dbName, isNewNode_);
            LOG(INFO) << "Start init OCMetadataManager for " << dbName;
            RETURN_IF_NOT_OK(oc->Init());
            ocElapsed = timer.ElapsedMilliSecond();
            oc->AssignLocalWorker(masterWorkerService_, workerWorkerService_, masterAddress_);
            metadataManager.oc = std::move(oc);
        }

        if (isScEnabled_) {
            // create SCMetadataManager instance
            auto sc = std::make_shared<master::SCMetadataManager>(masterAddress_, akSkManager_, rpcSessionManager_,
                                                                  etcdCM_, streamRocksStore, dbName);
            LOG(INFO) << "Start init SCMetadataManager for " << dbName;
            timer.Reset();
            RETURN_IF_NOT_OK(sc->Init());
            scElapsed = timer.ElapsedMilliSecond();
            metadataManager.sc = std::move(sc);
        }

        metadataManagers_.emplace(dbName, std::move(metadataManager));
        LOG(INFO) << "OCMetadataManager init cost:" << ocElapsed << "ms, SCMetadataManager init cost:" << scElapsed
                  << "ms for " << dbName;
    }
    return Status::OK();
}

Status ReplicaManager::DestroyMetaManager(const std::string &dbName)
{
    auto iter = metadataManagers_.find(dbName);
    if (iter != metadataManagers_.end()) {
        Timer timer;
        iter->second.Shutdown();
        auto elapsed1 = timer.ElapsedMilliSecondAndReset();
        metadataManagers_.erase(iter);
        auto elapsed2 = timer.ElapsedMilliSecond();
        LOG(INFO) << "Shutdown MetadataManager for " << dbName << " cost: " << elapsed1
                  << "ms, erase cost: " << elapsed2 << "ms.";
    }
    return Status::OK();
}

Status ReplicaManager::GetMetadataManager(const std::string &dbName, MetadataManager &metadataManager)
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    auto iter = metadataManagers_.find(dbName);
    if (iter == metadataManagers_.end()) {
        RETURN_STATUS(K_REPLICA_NOT_READY, FormatString("The rocksdb name %s not exists", dbName));
    }
    metadataManager = iter->second;
    return Status::OK();
}

Status ReplicaManager::GetOcMetadataManager(const std::string &dbName,
                                            std::shared_ptr<master::OCMetadataManager> &ocMetadataManager)
{
    MetadataManager metadataManager;
    RETURN_IF_NOT_OK(GetMetadataManager(dbName, metadataManager));
    RETURN_RUNTIME_ERROR_IF_NULL(metadataManager.oc);
    ocMetadataManager = metadataManager.oc;
    return Status::OK();
}

Status ReplicaManager::GetScMetadataManager(const std::string &dbName,
                                            std::shared_ptr<master::SCMetadataManager> &scMetadataManager)
{
    MetadataManager metadataManager;
    RETURN_IF_NOT_OK(GetMetadataManager(dbName, metadataManager));
    RETURN_RUNTIME_ERROR_IF_NULL(metadataManager.sc);
    scMetadataManager = metadataManager.sc;
    return Status::OK();
}

bool ReplicaManager::HaveAsyncMetaRequest()
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    for (const auto &iter : metadataManagers_) {
        if (iter.second.oc != nullptr && iter.second.oc->HaveAsyncMetaRequest()) {
            return true;
        }
    }
    return false;
}

void ReplicaManager::HandleEvent()
{
    LOG(INFO) << "Start replica event handle thread.";
    Timer timer;
    uint64_t locationCheckInterval = 30000;  // 30s.
    INJECT_POINT("ReplicaManager.locationCheckInterval", [&locationCheckInterval]() {
        locationCheckInterval = 2000;  // inject to locationCheckInterval 2000ms.
    });
    while (!stop_) {
        auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-revent");
        INJECT_POINT("worker.CheckReplicaLocation",
                     [&locationCheckInterval](int interval) { locationCheckInterval = interval; });
        if (timer.ElapsedMilliSecond() > locationCheckInterval) {
            timer.Reset();
            CheckReplicaLocation();
        }
        mvccpb::Event event;
        const uint64_t timeout = 100;  // ms;
        Status rc = eventQue_->Poll(&event, timeout);
        if (rc.GetCode() == K_TRY_AGAIN) {
            continue;
        }
        bool isDelete = event.type() == mvccpb::Event_EventType::Event_EventType_DELETE;
        LOG_IF_ERROR(HandleOneEvent(event.kv().key(), event.kv().value(), isDelete),
                     "HandleOneEvent failed for " + event.kv().key());
    }
    LOG(INFO) << "Terminating replica event handle thread.";
}

Status ReplicaManager::HandleOneEvent(const std::string &key, const std::string &value, bool isDelete)
{
    std::string dbName = key;
    dbName.erase(0, dbName.find(ETCD_REPLICA_GROUP_TABLE) + strlen(ETCD_REPLICA_GROUP_TABLE) + 1);
    if (!isDelete) {
        ReplicaGroupPb replicaGroupPb;
        CHECK_FAIL_RETURN_STATUS(replicaGroupPb.ParseFromString(value), K_INVALID,
                                 FormatString("Parse ReplicaGroupPb failed for db name ", dbName));
        LOG(INFO) << "Processing update event on " << currentWorkerId_ << " for " << key << ":"
                  << LogHelper::IgnoreSensitive(replicaGroupPb);
        return HandleUpdateEvent(dbName, replicaGroupPb);
    } else {
        LOG(INFO) << "Processing delete event on " << currentWorkerId_ << " for " << key;
        return HandleDeleteEvent(dbName);
    }
    return Status::OK();
}

Status ReplicaManager::HandleUpdateEvent(const std::string &dbName, const ReplicaGroupPb &replicaGroupPb)
{
    const auto &primaryNodeId = replicaGroupPb.primary_id();
    if (!primaryNodeId.empty()) {
        ClearDelayElectionTask(dbName);
    }

    {
        std::unique_lock<std::shared_timed_mutex> locker(mutex_);
        std::time_t createTime = GetSteadyClockTimeStampUs();
        replicaGroups_[dbName] = { createTime, replicaGroupPb };
        if (!primaryNodeId.empty()) {
            primaryReplicaLocation_[dbName] = primaryNodeId;
        }
    }

    std::vector<ReplicaPb> replicaPbs = { replicaGroupPb.replicas().begin(), replicaGroupPb.replicas().end() };
    // The replica with a larger seq id is more likely to be successfully elected as the primary replica.
    std::sort(replicaPbs.begin(), replicaPbs.end(),
              [](const ReplicaPb &left, const ReplicaPb &right) { return left.seq() > right.seq(); });

    // whether current worker should exists replica.
    bool shouldExists = false;
    size_t delaySec = 0;
    for (size_t i = 0; i < replicaPbs.size(); i++) {
        if (replicaPbs[i].worker_id() == currentWorkerId_) {
            shouldExists = true;
            delaySec = i;
        }
    }
    ReplicaType replicaType = primaryNodeId == currentWorkerId_ ? ReplicaType::Primary : ReplicaType::Backup;
    if (shouldExists) {
        RETURN_IF_NOT_OK(AddOrSwitchTo(dbName, replicaType));
    } else {
        return RemoveReplica(dbName);
    }

    if (primaryNodeId.empty()) {
        return AddDelayElectionTask(delaySec, dbName);
    } else if (replicaType == ReplicaType::Backup) {
        return TryAddPrimary(dbName, primaryNodeId);
    } else {
        return Status::OK();
    }
}

Status ReplicaManager::HandleDeleteEvent(const std::string &dbName)
{
    {
        std::unique_lock<std::shared_timed_mutex> locker(mutex_);
        replicaGroups_.erase(dbName);
        primaryReplicaLocation_.erase(dbName);
    }
    return RemoveReplica(dbName);
}

Status ReplicaManager::AddOrSwitchTo(const std::string &dbName, ReplicaType type)
{
    std::unique_lock<std::shared_timed_mutex> locker(mutex_);
    auto iter = replicas_.find(dbName);
    if (iter == replicas_.end()) {
        // create replica
        LOG(INFO) << "Start create replica for " << dbName << " replica type: " << Replica::ReplicaTypeToString(type);
        auto replica =
            std::make_shared<Replica>(dbName, dbRootPath_, currentWorkerId_, channel_.get(), MultiReplicaEnabled());
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replica->Init(), "Replica init failed");
        if (isOcEnabled_) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Replica::CreateOcTable(replica->GetObjectRocksStore()),
                                             "Replica create oc table failed");
        }
        if (isScEnabled_) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Replica::CreateScTable(replica->GetStreamRocksStore()),
                                             "Replica create sc table failed");
        }
        iter = replicas_.emplace(dbName, std::move(replica)).first;
    }
    auto &replica = *iter->second;
    if (replica.GetReplicaType() == type) {
        return Status::OK();
    }

    LOG(INFO) << "Replica " << dbName << ", switch to " << Replica::ReplicaTypeToString(type);
    replica.SetReplicaType(type);
    if (type == ReplicaType::Primary) {
        RETURN_IF_NOT_OK(CreateMetaManager(dbName, replica.GetObjectRocksStore(), replica.GetStreamRocksStore()));
    } else {
        RETURN_IF_NOT_OK(DestroyMetaManager(dbName));
    }
    return Status::OK();
}

void ReplicaManager::CheckNeedAdjustReplicaWhenScaleDown(const std::string &removeDbName,
                                                         bool &removeWorkerPrimaryReplicaIsLocalNode,
                                                         bool &removeNodeIsCurrentNodeReplica)
{
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto localReplica = replicas_.find(removeDbName);
    if (localReplica != replicas_.end()) {
        removeWorkerPrimaryReplicaIsLocalNode = localReplica->second->GetReplicaType() == ReplicaType::Primary;
    }
    auto currentNodeReplicaGroup = replicaGroups_.find(currentWorkerId_);
    if (currentNodeReplicaGroup != replicaGroups_.end()) {
        for (const auto &replica : currentNodeReplicaGroup->second.second.replicas()) {
            if (replica.worker_id() == removeDbName) {
                removeNodeIsCurrentNodeReplica = true;
            }
        }
    }
}

Status ReplicaManager::HandleNodeScaleDownFinish(const std::vector<std::string> &removeDbNames)
{
    LOG(INFO) << "ScaleDownFinish, try to adjust back up replica and delete replica.";
    for (const auto &removeDbName : removeDbNames) {
        bool removeWorkerPrimaryReplicaIsLocalNode = false;
        bool removeNodeIsCurrentNodeReplica = false;
        CheckNeedAdjustReplicaWhenScaleDown(removeDbName, removeWorkerPrimaryReplicaIsLocalNode,
                                            removeNodeIsCurrentNodeReplica);
        // for priamry replica is scale down node, del replica group from etcd.
        // for backup replica is scale down node, adjust backup replica.
        if (removeWorkerPrimaryReplicaIsLocalNode) {
            LOG(INFO) << "worker: " << removeDbName << " scale down finish, remove replica group";
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->Delete(ETCD_REPLICA_GROUP_TABLE, removeDbName),
                                             FormatString("delete replica group %s from etcd failed", removeDbName));
        }
        if (removeNodeIsCurrentNodeReplica) {
            LOG(INFO) << "curret workerid:" << currentWorkerId_ << " back up replica" << removeDbName
                      << " scale down finish, Adjust backup replica";
            AdjustBackupReplicaLocation(currentWorkerId_);
        }
    }
    return Status::OK();
}

Status ReplicaManager::RemoveReplica(const std::string &dbName)
{
    std::unique_lock<std::shared_timed_mutex> locker(mutex_);
    RETURN_IF_NOT_OK(DestroyMetaManager(dbName));
    auto replicaIter = replicas_.find(dbName);
    if (replicaIter != replicas_.end()) {
        LOG(INFO) << "Start remove replica for " << dbName;
        RETURN_IF_NOT_OK(replicaIter->second->Remove());
        (void)replicas_.erase(dbName);
    }

    return Status::OK();
}

Status ReplicaManager::TryAddPrimary(const std::string &dbName, const std::string &primaryNodeId)
{
    std::unique_lock<std::shared_timed_mutex> locker(mutex_);
    auto replicaIter = replicas_.find(dbName);
    if (replicaIter != replicas_.end()) {
        LOG(INFO) << "Add primary for " << dbName;
        RETURN_IF_NOT_OK(replicaIter->second->AddPrimary(primaryNodeId));
    }
    return Status::OK();
}

Status ReplicaManager::HandleVoluntaryScaleDownFinish(const std::string &workerUuid)
{
    if (currentWorkerId_ != workerUuid) {
        return Status::OK();
    }
    LOG_IF_ERROR(TrySwitchPrimaryToOwnerNode(), "TrySwitchPrimaryToOwnerNode failed");
    return Status::OK();
}

Status ReplicaManager::EnqueEvent(mvccpb::Event event)
{
    return eventQue_->Add(std::move(event));
}

Status ReplicaManager::Election(const std::string &dbName)
{
    ClearDelayElectionTask(dbName);
    return etcdStore_->CAS(
        ETCD_REPLICA_GROUP_TABLE, dbName,
        [this, dbName](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            ReplicaGroupPb replicaGroup;
            if (!replicaGroup.ParseFromString(oldValue)) {
                LOG(WARNING) << "Parse to ReplicaGroupPb failed.";
                return Status::OK();
            }
            if (!replicaGroup.primary_id().empty()) {
                return Status::OK();
            }
            for (auto &item : *replicaGroup.mutable_replicas()) {
                item.set_seq(0);
            }
            replicaGroup.set_primary_id(currentWorkerId_);
            newValue = std::make_unique<std::string>(replicaGroup.SerializeAsString());
            LOG(INFO) << "[Election] Try set primary node to " << currentWorkerId_ << " for db " << dbName;
            return Status::OK();
        });
}

Status ReplicaManager::AddDelayElectionTask(uint64_t delaySec, const std::string &dbName)
{
    // The replica with the largest seq id does not delay the election.
    if (delaySec == 0) {
        return Election(dbName);
    }
    TimerQueue::TimerImpl timer;
    TimerQueue::GetInstance()->AddTimer(
        delaySec * SECTOMILLI, [this, dbName] { Election(dbName); }, timer);

    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    timers_.emplace(dbName, std::make_unique<TimerQueue::TimerImpl>(timer));
    return Status::OK();
}

void ReplicaManager::ClearDelayElectionTask(const std::string &dbName)
{
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    if (timers_.count(dbName) > 0) {
        TimerQueue::GetInstance()->Cancel(*timers_[dbName]);
        timers_.erase(dbName);
    }
}

Status ReplicaManager::GetReplicaType(const std::string &dbName, ReplicaType &replicaType)
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    auto replicaIter = replicas_.find(dbName);
    if (replicaIter == replicas_.end()) {
        RETURN_STATUS(K_REPLICA_NOT_READY, FormatString("The rocksdb name %s not exists", dbName));
    }
    replicaType = replicaIter->second->GetReplicaType();
    return Status::OK();
}

Status ReplicaManager::GetReplica(const std::string &dbName, std::shared_ptr<Replica> &replica)
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    auto replicaIter = replicas_.find(dbName);
    if (replicaIter == replicas_.end()) {
        LOG(ERROR) << FormatString("The rocksdb name %s not exists", dbName);
        return { StatusCode::K_REPLICA_NOT_READY, "Get replica failed." };
    }
    replica = replicaIter->second;
    return Status::OK();
}

void ReplicaManager::Shutdown()
{
    Timer timer;
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        for (auto &iter : metadataManagers_) {
            iter.second.Shutdown();
            LOG(INFO) << "Shutdown MetadataManager for " << iter.first
                      << " finish, cost:" << timer.ElapsedMilliSecondAndReset() << "ms";
        }
    }

    stop_ = true;
    threadPool_ = nullptr;
    LOG(INFO) << "ReplicaManager thread pool shutdown, cost:" << timer.ElapsedMilliSecond() << "ms";
}

Status ReplicaManager::GetPrimaryReplicaLocation(const std::string &srcWorkerUuid, std::string &destWorkerUuid)
{
    if (MultiReplicaEnabled()) {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        auto iter = primaryReplicaLocation_.find(srcWorkerUuid);
        if (iter == primaryReplicaLocation_.end()) {
            // Used to solve the problem that one cluster has turned on the Replica mode, but the other cluster has
            // not turned on it.
            auto azName = etcdCM_->GetOtherAzNameByWorkerIdInefficient(srcWorkerUuid);
            if (!azName.empty()) {
                LOG(WARNING) << FormatString("The replicas[%s] in az[%s] have not yet been subscribed", srcWorkerUuid,
                                             azName);
                destWorkerUuid = srcWorkerUuid;
            } else {
                RETURN_STATUS(K_REPLICA_NOT_READY, FormatString("Replica group %s not exists", srcWorkerUuid));
            }
        } else {
            destWorkerUuid = iter->second;
        }
    } else {
        destWorkerUuid = srcWorkerUuid;
    }
    return Status::OK();
}

Status ReplicaManager::GetPrimaryReplicaDbNames(const std::string &workerUuid, std::vector<std::string> &dbNames)
{
    std::shared_lock<std::shared_timed_mutex> locker(mutex_);
    for (const auto &kv : replicaGroups_) {
        if (kv.second.second.primary_id() == workerUuid) {
            dbNames.emplace_back(kv.first);
        }
    }
    return Status::OK();
}

Status ReplicaManager::GetDeviceOcManager(const std::string &dbName,
                                          std::shared_ptr<master::MasterDevOcManager> &devOcManager)
{
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetOcMetadataManager(dbName, ocMetadataManager), "GetOcMetadataManager failed");
    devOcManager = ocMetadataManager->GetDeviceOcManager();
    return Status::OK();
}

Status ReplicaManager::InitReplicaForStart(bool isRestart, const ClusterInfo &clusterInfo)
{
    // only master node call this function.
    if (!MultiReplicaEnabled()) {
        auto dbName = GetCurrentWorkerUuid();
        LOG(INFO) << "The meta multi-replica not enable, create the unique replica for current worker:"
                  << GetCurrentWorkerUuid() << ", worker addr:" << masterAddress_.ToString();
        return AddOrSwitchTo(dbName, ReplicaType::Primary);
    }
    LOG(INFO) << "The meta multi-replica enable, try create replica for worker start, current worker id:"
              << GetCurrentWorkerUuid() << ", worker addr:" << masterAddress_.ToString();
    // check old replica exsit.
    std::string oldMetaDir = dbRootPath_ + "/" + "object_metadata";
    // query etcd and create replica.

    bool replicaExistsInEtcd = false;
    for (const auto &kv : clusterInfo.replicaGroups) {
        const std::string &dbName = kv.first;
        ReplicaGroupPb replicaGroupPb;
        CHECK_FAIL_RETURN_STATUS(replicaGroupPb.ParseFromString(kv.second), K_INVALID,
                                 FormatString("Parse ReplicaGroupPb failed for db name ", dbName));
        LOG(INFO) << "Try handle fake event for db name " << dbName
                  << ", replicaGroupPb:" << replicaGroupPb.ShortDebugString();
        RETURN_IF_NOT_OK(HandleUpdateEvent(dbName, replicaGroupPb));
        if (dbName == GetCurrentWorkerUuid()) {
            replicaExistsInEtcd = true;
        }
    }

    std::vector<std::pair<std::string, std::string>> otherAzReplicaGroupKvs;
    RETURN_IF_NOT_OK(etcdStore_->GetOtherAzAllValue(ETCD_REPLICA_GROUP_TABLE, 0, otherAzReplicaGroupKvs));
    for (const auto &kv : otherAzReplicaGroupKvs) {
        const std::string &dbName = kv.first;
        ReplicaGroupPb replicaGroupPb;
        CHECK_FAIL_RETURN_STATUS(replicaGroupPb.ParseFromString(kv.second), K_INVALID,
                                 FormatString("Parse ReplicaGroupPb failed for db name ", dbName));
        LOG(INFO) << "Try handle fake event for db name " << dbName
                  << ", replicaGroupPb:" << replicaGroupPb.ShortDebugString();
        RETURN_IF_NOT_OK(HandleUpdateEvent(dbName, replicaGroupPb));
    }

    bool isUpdate = FileExist(oldMetaDir);
    if (replicaExistsInEtcd) {
        LOG(WARNING) << "Replica already exists in etcd, isRestart:" << isRestart << ", isUpdate:" << isUpdate;
        return Status::OK();
    }

    if (isUpdate) {
        LOG(INFO) << "update worker, create replica by old file";
        const int permission = 0700;
        const auto &dbName = GetCurrentWorkerUuid();
        std::string newMetaDir = dbRootPath_ + "/" + META_NAME + "/" + dbName;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(newMetaDir, true, permission),
                                         FormatString("create dir %s failed", newMetaDir));
        LOG_IF_ERROR(MoveAll(oldMetaDir, newMetaDir),
                     FormatString("move from old path:%s to new path:%s failed", oldMetaDir, newMetaDir));
    }

    INJECT_POINT("worker.ClusterInitFinish", [] { return Status::OK(); });
    // create primary replica for new worker start, cluster init or scale up.
    LOG(INFO) << "Create replica for current worker.";
    const auto &dbName = GetCurrentWorkerUuid();
    ReplicaGroupPb replicaGroupPb = CreateReplicaGroupPb(dbName, { dbName });
    RETURN_IF_NOT_OK(HandleUpdateEvent(dbName, replicaGroupPb));
    RETURN_IF_NOT_OK(PutReplicaGroupToEtcd(dbName, replicaGroupPb));
    // write back up replica location to etcd.
    LOG_IF_ERROR(CheckBackupReplicaLocation(), "check back up replica location failed");
    return Status::OK();
}

Status ReplicaManager::PutReplicaGroupToEtcd(const std::string &dbName, const ReplicaGroupPb &replicaGroupPb)
{
    auto value = replicaGroupPb.SerializeAsString();
    return etcdStore_->Put(ETCD_REPLICA_GROUP_TABLE, dbName, value);
}

Status ReplicaManager::AdjustReplicaLocationImpl(const std::string &dbName, const std::set<std::string> &addWorkerUuids,
                                                 const std::set<std::string> &delWorkerUuids,
                                                 const std::string &oldPrimaryLocation)
{
    if (addWorkerUuids.empty() && delWorkerUuids.empty()) {
        return Status::OK();
    }

    LOG(INFO) << "Try adjust backup replica for " << dbName << ", add at " << VectorToString(addWorkerUuids)
              << ", del at " << VectorToString(delWorkerUuids);
    return etcdStore_->CAS(
        ETCD_REPLICA_GROUP_TABLE, dbName,
        [&dbName, &addWorkerUuids, &delWorkerUuids, &oldPrimaryLocation](
            const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            if (oldValue.empty()) {
                LOG(INFO) << "Not found " << dbName << ", give up update ReplicaGroupPb.";
                return Status::OK();
            }

            ReplicaGroupPb replicaGroup;
            if (!replicaGroup.ParseFromString(oldValue)) {
                LOG(WARNING) << "Parse to ReplicaGroupPb failed.";
                return Status::OK();
            }

            auto primaryWorkerUuid = replicaGroup.primary_id();
            if (primaryWorkerUuid.empty()) {
                LOG(INFO) << "The primary id is empty for " << dbName << ", give up update ReplicaGroupPb";
                return Status::OK();
            }
            if (!oldPrimaryLocation.empty() && oldPrimaryLocation != primaryWorkerUuid) {
                LOG(INFO) << "The primary replica changed for " << dbName << ", give up update ReplicaGroupPb";
                return Status::OK();
            }
            if (oldPrimaryLocation.empty()
                && (addWorkerUuids.count(primaryWorkerUuid) > 0 || delWorkerUuids.count(primaryWorkerUuid) > 0)) {
                LOG(INFO) << "Not allow adjust primary replica for " << dbName << ", give up update ReplicaGroupPb";
                return Status::OK();
            } else {
                if (delWorkerUuids.count(primaryWorkerUuid) > 0) {
                    replicaGroup.clear_primary_id();
                }
            }

            auto &replicas = *replicaGroup.mutable_replicas();
            // remove replicas.
            auto iter = std::remove_if(replicas.begin(), replicas.end(), [&delWorkerUuids](const ReplicaPb &replicaPb) {
                return delWorkerUuids.count(replicaPb.worker_id()) > 0;
            });
            bool changed = iter != replicas.end();
            replicas.erase(iter, replicas.end());

            // add replicas.
            std::set<std::string> toAddWorkerUuids = addWorkerUuids;
            for (const auto &item : replicas) {
                toAddWorkerUuids.erase(item.worker_id());
            }

            for (const auto &workerUuid : toAddWorkerUuids) {
                changed = true;
                replicaGroup.add_replicas()->set_worker_id(workerUuid);
            }

            if (changed) {
                newValue = std::make_unique<std::string>(replicaGroup.SerializeAsString());
                LOG(INFO) << "[AdjustBackupReplicaLocation] Try update ReplicaGroupPb to "
                          << replicaGroup.ShortDebugString() << " for db " << dbName;
            }
            return Status::OK();
        });
}

Status ReplicaManager::HandleNodeTimeout(const std::string &workerAddr)
{
    std::string dbNameForTimeoutWorker;
    RETURN_IF_NOT_OK(etcdCM_->GetHashRing()->GetUuidByWorkerAddr(workerAddr, dbNameForTimeoutWorker));
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    std::set<std::string> dbnames{ dbNameForTimeoutWorker };
    if (dbNameForTimeoutWorker == currWorkerUuid) {
        LOG(INFO) << "Received timeout event for current node, need switch all replica to backup.";
        auto traceId = Trace::Instance().GetTraceID();
        threadPool_->Execute([this, traceId] {
            auto guard = Trace::Instance().SetTraceNewID(traceId);
            LOG_IF_ERROR(HandleCurrentNodeTimeout(), "HandleCurrentNodeTimeout failed.");
        });
    } else {
        // the primary replica for current node in timeout node.
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        auto iter = replicaGroups_.find(currWorkerUuid);
        if (iter != replicaGroups_.end() && iter->second.second.primary_id() == dbNameForTimeoutWorker) {
            (void)dbnames.emplace(currWorkerUuid);
        }
    }

    INJECT_POINT("worker.ReplicaManager.HandleNodeTimeout", [&dbnames, &currWorkerUuid] {
        dbnames.erase(currWorkerUuid);
        return Status::OK();
    });

    for (auto &dbName : dbnames) {
        // update etcd to notify worker start election.
        RETURN_IF_NOT_OK(
            NotifyStartElection(dbName, [&currWorkerUuid](const std::string &dbName, ReplicaGroupPb &replicaGroup) {
                const auto &primaryWorkerUuid = replicaGroup.primary_id();
                if (primaryWorkerUuid == currWorkerUuid) {
                    LOG(INFO) << "The primary replica in current node " << currWorkerUuid << " for " << dbName
                              << ", give up update ReplicaGroupPb.";
                    return false;
                }

                for (auto &replica : *replicaGroup.mutable_replicas()) {
                    if (replica.worker_id() == currWorkerUuid) {
                        replica.set_seq(1);
                    }
                }
                return true;
            }));
    }
    return Status::OK();
}

Status ReplicaManager::NotifyStartElection(const std::string &dbName,
                                           std::function<bool(const std::string &, ReplicaGroupPb &)> &&handler)
{
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    std::string dbPath = dbRootPath_ + "/" + META_NAME + "/" + dbName;
    return etcdStore_->CAS(
        ETCD_REPLICA_GROUP_TABLE, dbName,
        [&dbName, &currWorkerUuid, &handler, &dbPath](const std::string &oldValue,
                                                      std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            if (oldValue.empty()) {
                LOG(INFO) << "Not found " << dbName << ", give up update ReplicaGroupPb.";
                return Status::OK();
            }

            ReplicaGroupPb replicaGroup;
            if (!replicaGroup.ParseFromString(oldValue)) {
                LOG(WARNING) << "Parse to ReplicaGroupPb failed.";
                return Status::OK();
            }

            bool found = false;
            for (auto &replica : replicaGroup.replicas()) {
                if (replica.worker_id() == currWorkerUuid) {
                    found = true;
                }
            }

            if (!found) {
                LOG(INFO) << "Current worker not exists in ReplicaGroupPb, ignore.";
                return Status::OK();
            }

            if (!FileExist(dbPath)) {
                LOG(INFO)
                    << "Current worker cant find dbpath, checkout replica will failed, give up update ReplicaGroupPb.";
                return Status::OK();
            }

            if (replicaGroup.primary_id().empty()) {
                LOG(INFO) << "The primary id is empty for " << dbName << ", give up update ReplicaGroupPb.";
                return Status::OK();
            }
            LOG(INFO) << "[NotifyStartElection] Try update ReplicaGroupPb from [" << replicaGroup.ShortDebugString()
                      << "] for db " << dbName;
            if (!handler(dbName, replicaGroup)) {
                return Status::OK();
            }

            replicaGroup.clear_primary_id();
            newValue = std::make_unique<std::string>(replicaGroup.SerializeAsString());
            LOG(INFO) << "[NotifyStartElection] Try update ReplicaGroupPb to [" << replicaGroup.ShortDebugString()
                      << "] for db " << dbName;
            return Status::OK();
        });
}

Status ReplicaManager::HandleCurrentNodeTimeout()
{
    // scenario:
    //  1) worker1 exsits primary replica of worker1 and disconnect with etcd.
    //  2) worker2 exsits backup replica of worker1
    // worker1 switch all primary replica to backup.
    std::vector<std::string> dbNames;
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        for (const auto &iter : metadataManagers_) {
            dbNames.emplace_back(iter.first);
        }
    }

    Status lastRc;
    for (const auto &dbName : dbNames) {
        auto rc = AddOrSwitchTo(dbName, ReplicaType::Backup);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    return lastRc;
}

Status ReplicaManager::HandleNodeNetworkRecovery(const std::string &workerAddr)
{
    std::string workerUuid;
    RETURN_IF_NOT_OK(etcdCM_->GetHashRing()->GetUuidByWorkerAddr(workerAddr, workerUuid));
    LOG(INFO) << "HandleNodeNetworkRecovery workeruuid " << workerUuid;
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    if (workerUuid == currWorkerUuid) {
        return HandleCurrentNodeNetworkRecovery();
    }
    return Status::OK();
}

Status ReplicaManager::HandleCurrentNodeNetworkRecovery()
{
    // scenario:
    // a) worker1 exsits backup replica of worker1 after disconnect with etcd.
    // b) worker2 exsits backup replica of worker1
    // after worker1 reconnect with etcd, worker1 should start election.
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    std::vector<std::string> dbNames;
    RETURN_IF_NOT_OK(GetPrimaryReplicaDbNames(currWorkerUuid, dbNames));
    Status lastRc;

    auto handler = [&currWorkerUuid](const std::string &dbName, ReplicaGroupPb &replicaGroup) {
        const auto &primaryWorkerUuid = replicaGroup.primary_id();
        if (primaryWorkerUuid != currWorkerUuid) {
            LOG(INFO) << "The primary replica is not in current node for " << dbName
                      << ", give up update ReplicaGroupPb.";
            return false;
        }

        for (auto &replica : *replicaGroup.mutable_replicas()) {
            if (replica.worker_id() == currWorkerUuid) {
                replica.set_seq(1);
            }
        }
        return true;
    };
    for (const auto &dbName : dbNames) {
        auto rc = NotifyStartElection(dbName, handler);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    return lastRc;
}

Status ReplicaManager::TrySwitchPrimaryToOwnerNode()
{
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    // Get all primary replica for other node.
    std::vector<std::string> dbNames;
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        for (const auto &iter : metadataManagers_) {
            if (iter.first != currWorkerUuid) {
                dbNames.emplace_back(iter.first);
            }
        }
    }
    LOG_IF(INFO, !dbNames.empty()) << "Current node " << currWorkerUuid << " exists primary replica for "
                                   << VectorToString(dbNames);
    Status lastRc;
    for (const auto &dbName : dbNames) {
        Status rc = TrySwitchBackReplica(dbName);
        if (rc.IsError()) {
            LOG(ERROR) << "TrySwitchReplica failed:" << rc.ToString();
            lastRc = rc;
        }
    }
    return lastRc;
}

Status ReplicaManager::CheckPrimaryReplicaLocation()
{
    // del primary replica location if not exists in hash ring.
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    RETURN_RUNTIME_ERROR_IF_NULL(etcdCM_);
    auto hashRing = etcdCM_->GetHashRing();
    auto hashRingPb = hashRing->GetHashRingPb();
    std::set<std::string> workersInHashRing;
    for (const auto &item : hashRingPb.workers()) {
        workersInHashRing.emplace(item.second.worker_uuid());
    }

    std::string primaryWorkerUuid;
    std::set<std::string> delWorkerUuids;
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        auto iter = replicaGroups_.find(currWorkerUuid);
        if (iter == replicaGroups_.end()) {
            LOG(INFO) << "Not found " << currWorkerUuid << " in replicaGroups_";
            return Status::OK();
        }
        primaryWorkerUuid = iter->second.second.primary_id();
        if (primaryWorkerUuid.empty()) {
            LOG(INFO) << "the primary replica is empty for " << currWorkerUuid;
            return Status::OK();
        }
        std::time_t currentTime = GetSteadyClockTimeStampUs();
        std::time_t timeoutUs = 60 * 1000 * 1000;
        INJECT_POINT("worker.ReplicaManager::CheckPrimaryReplicaLocation", [&timeoutUs](int timeout) {
            timeoutUs = timeout;
            return Status::OK();
        });
        for (const auto &replica : iter->second.second.replicas()) {
            const auto &uuid = replica.worker_id();
            if (uuid == primaryWorkerUuid && workersInHashRing.count(uuid) == 0) {
                if (currentTime - iter->second.first > timeoutUs) {
                    delWorkerUuids.emplace(uuid);
                    LOG(WARNING) << "The node " << primaryWorkerUuid
                                 << " not in hashring but exists primary replica for current node " << currWorkerUuid;
                }
            }
        }
    }
    return AdjustReplicaLocationImpl(currWorkerUuid, {}, delWorkerUuids, primaryWorkerUuid);
}

Status ReplicaManager::CheckBackupReplicaLocation()
{
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    return AdjustBackupReplicaLocation(currWorkerUuid);
}

Status ReplicaManager::AdjustBackupReplicaLocation(const std::string &workerUuid)
{
    RETURN_RUNTIME_ERROR_IF_NULL(etcdCM_);
    std::string backupWorkerUuid;
    auto hashRing = etcdCM_->GetHashRing();
    RETURN_IF_NOT_OK(hashRing->GetNextWorker(workerUuid, backupWorkerUuid));
    std::set<std::string> addWorkerUuids{ workerUuid, backupWorkerUuid };
    std::set<std::string> delWorkerUuids;
    {
        std::shared_lock<std::shared_timed_mutex> locker(mutex_);
        auto iter = replicaGroups_.find(workerUuid);
        if (iter == replicaGroups_.end()) {
            LOG(INFO) << "Not found " << workerUuid << " in replicaGroups_";
            return Status::OK();
        }
        const auto &primaryWorkerUuid = iter->second.second.primary_id();
        if (primaryWorkerUuid.empty()) {
            LOG(INFO) << "the primary replica is empty for " << workerUuid;
            return Status::OK();
        }
        for (const auto &replica : iter->second.second.replicas()) {
            const auto &uuid = replica.worker_id();
            if (uuid == workerUuid || uuid == backupWorkerUuid) {
                addWorkerUuids.erase(uuid);
            } else if (uuid != primaryWorkerUuid) {
                delWorkerUuids.emplace(uuid);
            } else {
                // noop
            }
        }
    }
    return AdjustReplicaLocationImpl(workerUuid, addWorkerUuids, delWorkerUuids);
}

void ReplicaManager::CheckNeedRemoveReplica()
{
    auto hashRing = etcdCM_->GetHashRing();
    std::vector<std::string> currentActiveDbNames;
    hashRing->GetActiveWorkersDbNames(currentActiveDbNames);
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for (const auto &replica : replicaGroups_) {
        std::time_t currentTime = GetSteadyClockTimeStampUs();
        std::time_t timeoutUs = 60 * 1000 * 1000;
        if (std::find(currentActiveDbNames.begin(), currentActiveDbNames.end(), replica.first)
                == currentActiveDbNames.end()
            && currentTime - replica.second.first > timeoutUs) {
            LOG(WARNING) << "worker " << replica.first
                         << " not exists in hash ring, try notify other worker to remove replica.";
            LOG_IF_ERROR(etcdStore_->Delete(ETCD_REPLICA_GROUP_TABLE, replica.first),
                         FormatString("delete replica group %s from etcd failed", replica.first));
        }
    }
}

void ReplicaManager::CheckReplicaLocation()
{
    LOG_IF_ERROR(TrySwitchPrimaryToOwnerNode(), "TrySwitchPrimaryToOwnerNode failed");
    LOG_IF_ERROR(CheckPrimaryReplicaLocation(), "CheckPrimaryReplicaLocation failed");
    LOG_IF_ERROR(CheckBackupReplicaLocation(), "CheckBackupReplicaLocation failed");
    CheckNeedRemoveReplica();
}

Status ReplicaManager::TrySwitchBackReplica(const std::string &dbName)
{
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    if (dbName == currWorkerUuid) {
        return Status::OK();
    }
    uint64_t latestSendSeqNo;
    Status rc = CheckBeforeSwitchReplica(dbName, latestSendSeqNo);
    if (rc.IsError()) {
        LOG(INFO) << "Check failed, give up switch replica for db name " << dbName << ", status:" << rc.ToString();
        return Status::OK();
    }
    RETURN_IF_NOT_OK(AddOrSwitchTo(dbName, ReplicaType::Backup));
    // wait sync task finish and start election.
    RETURN_IF_NOT_OK(NotifyStartElection(
        dbName, [&currWorkerUuid, latestSendSeqNo](const std::string &dbName, ReplicaGroupPb &replicaGroup) {
            const auto &primaryWorkerUuid = replicaGroup.primary_id();
            if (primaryWorkerUuid != currWorkerUuid) {
                LOG(INFO) << "The primary replica is not in current node for " << dbName
                          << ", give up update ReplicaGroupPb.";
                return false;
            }

            for (auto &replica : *replicaGroup.mutable_replicas()) {
                if (replica.worker_id() == dbName) {
                    replica.set_seq(latestSendSeqNo);
                }
            }
            return true;
        }));
    return Status::OK();
}

Status ReplicaManager::CheckBeforeSwitchReplica(const std::string &dbName, uint64_t &latestSendSeqNo)
{
    INJECT_POINT("ReplicaManager.CheckBeforeSwitchReplica");
    auto hashRing = etcdCM_->GetHashRing();
    // 1. Check connection with replica owner node.
    HostPort workerAddr;
    RETURN_IF_NOT_OK(hashRing->GetWorkerAddrByUuidForMultiReplica(dbName, workerAddr));
    RETURN_IF_NOT_OK(etcdCM_->CheckConnection(workerAddr));
    // 2. Get the pending log count, should less than 1024.
    const uint64_t maxPendingLogCount = 1024;
    uint64_t latestSeqNo;
    RETURN_IF_NOT_OK(WithReplica(dbName, [&dbName, &latestSeqNo, &latestSendSeqNo](Replica *replica) {
        return replica->GetPrimaryTaskInfo(dbName, latestSeqNo, latestSendSeqNo);
    }));
    if (UINT64_MAX - latestSendSeqNo < maxPendingLogCount) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("latestSendSeqNo: %zu + 1024 > UINT64_MAX", latestSendSeqNo));
    }
    if (latestSeqNo > latestSendSeqNo + maxPendingLogCount) {
        RETURN_STATUS(
            K_RUNTIME_ERROR,
            FormatString(
                "The logs of backup replica are too far behind the primary, latestSeqNo %zu, latestSendSeqNo %zu.",
                latestSeqNo, latestSendSeqNo));
    }
    // 3. Check etcd .
    CHECK_FAIL_RETURN_STATUS(!etcdStore_->IsKeepAliveTimeout(), K_RUNTIME_ERROR, "etcd is timeout");
    return Status::OK();
}

Status ReplicaManager::HandleNodeScaleupFinish(const std::string &workerUuid)
{
    const auto &currWorkerUuid = GetCurrentWorkerUuid();
    auto hashRing = etcdCM_->GetHashRing();
    if (currWorkerUuid != workerUuid) {
        std::string prevWorkerUuid;
        RETURN_IF_NOT_OK(hashRing->GetPrevWorker(workerUuid, prevWorkerUuid));
        if (prevWorkerUuid != currWorkerUuid) {
            return Status::OK();
        }
    }
    // Change backup replica location.
    // For scaleup node, will add backup replica in the next worker.
    // For the prev of scaleup node, will remove old backup replica add add new backup replica in scaleup node.
    LOG(INFO) << "Worker " << workerUuid << " scale up finish, try adjust backup replica location for "
              << currWorkerUuid << "";

    return AdjustBackupReplicaLocation(currWorkerUuid);
}

bool ReplicaManager::CheckMetaEmpty(const std::string &dbName)
{
    MetadataManager metadataManager;
    Status rc = GetMetadataManager(dbName, metadataManager);
    if (rc.IsError()) {
        return true;
    }

    auto oc = metadataManager.oc;
    auto sc = metadataManager.sc;
    if ((oc != nullptr && !oc->CheckMetaTableEmpty()) || (sc != nullptr && !sc->CheckMetaTableEmpty())) {
        return false;
    }

    return true;
}

void ReplicaManager::GetPrimaryReplicaInfoInWorker(const std::string &workerUuid,
                                                   std::map<std::string, std::string> &replicaInfos)
{
    std::vector<std::string> replicaOwnerWorkerUuids;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        for (const auto &replica : replicaGroups_) {
            if (workerUuid == replica.second.second.primary_id()) {
                replicaOwnerWorkerUuids.emplace_back(replica.first);
            }
        }
    }
    auto hashRing = etcdCM_->GetHashRing();
    for (const auto &uuid : replicaOwnerWorkerUuids) {
        HostPort workerAddr;
        Status rc = hashRing->GetWorkerAddrByUuidForMultiReplica(uuid, workerAddr);
        if (rc.IsError()) {
            LOG(WARNING) << "Get WorkerAddr by workeruuid " << uuid << " failed, message:" << rc.GetMsg();
            continue;
        }
        replicaInfos.emplace(uuid, workerAddr.ToString());
    }
}

void ReplicaManager::SetCleanMapTask()
{
    LOG(INFO) << "Start set clean map task.";
    static const uint64_t SECS_TO_MS = 1000;
    uint64_t cleanMapIntervalMs = FLAGS_rolling_update_timeout_s * SECS_TO_MS;
    INJECT_POINT("Worker.cleanMapIntervalMs",
                 [&cleanMapIntervalMs]() { cleanMapIntervalMs = 10000; });  // Set cycle to 10000ms for test.
    cleanTask_ = [this, cleanMapIntervalMs]() {
        LOG_IF_ERROR(CleanKeyWithWorkerIdMetaMap(), "CleanKeyWithWorkerIdMetaMap failed.");
        if (!stop_) {
            if (cleanTimer_) {
                cleanTimer_.reset();
            }
            TimerQueue::TimerImpl timer;
            if (TimerQueue::GetInstance()->AddTimer(cleanMapIntervalMs, cleanTask_, timer).IsError()) {
                LOG(ERROR) << "Failed to add clean map task";
                return;
            }
            cleanTimer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
        }
    };
    TimerQueue::TimerImpl timer;
    TimerQueue::GetInstance()->AddTimer(cleanMapIntervalMs, cleanTask_, timer);
    cleanTimer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
}

Status ReplicaManager::CheckMappingExpired(std::set<std::string> &expiredUuids)
{
    MetadataManager metadataManager;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetMetadataManager(GetCurrentWorkerUuid(), metadataManager),
                                     "Failed to get metadata manager when check mapping expired");
    std::vector<std::string> objKeys;
    bool isUuidsEmpty = expiredUuids.empty();
    if (metadataManager.oc != nullptr && !isUuidsEmpty) {
        metadataManager.oc->GetMetasMatch(
            [&expiredUuids, &isUuidsEmpty](const std::string &objKey) {
                std::string curUuid;
                if (TrySplitWorkerIdFromObjecId(objKey, curUuid).IsOk() && ContainsKey(expiredUuids, curUuid)) {
                    expiredUuids.erase(curUuid);
                    isUuidsEmpty = expiredUuids.empty();
                    return true;
                }
                return false;
            },
            objKeys, &isUuidsEmpty);
    }

    if (metadataManager.sc != nullptr && !isUuidsEmpty) {
        metadataManager.sc->GetMetasMatch(
            [&expiredUuids, &isUuidsEmpty](const std::string &streamName) {
                std::string curUuid;
                if (TrySplitWorkerIdFromObjecId(streamName, curUuid).IsOk() && ContainsKey(expiredUuids, curUuid)) {
                    expiredUuids.erase(curUuid);
                    isUuidsEmpty = expiredUuids.empty();
                    return true;
                }
                return false;
            },
            objKeys, &isUuidsEmpty);
    }

    return Status::OK();
}

Status ReplicaManager::CleanKeyWithWorkerIdMetaMap()
{
    LOG(INFO) << "Start clean key with worker id meta map.";
    auto hashRing = etcdCM_->GetHashRing();
    if (hashRing == nullptr) {
        RETURN_STATUS(K_RUNTIME_ERROR, "HashRing is null now.");
    }
    std::string curWorkerUuid = GetCurrentWorkerUuid();

    HashRingPb currRing = hashRing->GetHashRingPb();
    if (!(currRing.add_node_info().empty()) || !(currRing.del_node_info().empty())) {
        return Status::OK();
    }

    if (MultiReplicaEnabled()) {
        std::vector<std::string> dbNames;
        RETURN_IF_NOT_OK(GetPrimaryReplicaDbNames(curWorkerUuid, dbNames));
        RETURN_OK_IF_TRUE(!ContainsKey(dbNames, curWorkerUuid));
    }

    std::set<std::string> workerUuids;
    for (const auto &iter : currRing.key_with_worker_id_meta_map()) {
        if (iter.second != masterAddress_.ToString() || iter.first == curWorkerUuid) {
            continue;
        }
        workerUuids.emplace(iter.first);
    }
    std::set<std::string> expiredUuids = workerUuids;
    RETURN_IF_NOT_OK(CheckMappingExpired(expiredUuids));
    RETURN_OK_IF_TRUE(expiredUuids.empty());
    return hashRing->RemoveExpiredMap(expiredUuids);
}
}  // namespace datasystem
