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
 * Description: Defines the worker service processing main class.
 */
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <linux/futex.h>
#include <functional>
#include <future>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <type_traits>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/object_cache/async_rollback_manager.h"
#include "datasystem/worker/object_cache/device/worker_device_oc_manager.h"
#include "datasystem/worker/object_cache/migrate_data_handler.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"

DS_DEFINE_int32(oc_thread_num, 32, "Thread number of worker service");
DS_DEFINE_validator(oc_thread_num, &Validator::ValidateThreadNum);
DS_DEFINE_uint32(client_reconnect_wait_s, 10, "Client reconnect wait seconds, default is 10.");
DS_DEFINE_validator(client_reconnect_wait_s, &Validator::ValidateUint32);
DS_DEFINE_bool(enable_reconciliation, true, "Whether to enable reconciliation, default is true");
DS_DEFINE_bool(oc_io_from_l2cache_need_metadata, true,
               "Whether data read and write from the L2 cache daemon depend on metadata. Note: If set to false, it "
               "indicates that the metadata is not stored in etcd.");
DS_DEFINE_uint32(data_migrate_rate_limit_mb, 40, "Data migrate rate limit for every node when scale down happen");

DS_DECLARE_uint32(max_client_num);
DS_DECLARE_string(worker_address);
DS_DECLARE_string(master_address);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(etcd_address);
DS_DECLARE_bool(cross_az_get_data_from_worker);
DS_DECLARE_bool(cross_az_get_meta_from_worker);
DS_DECLARE_bool(enable_distributed_master);

using namespace datasystem::master;
using namespace datasystem::worker;
namespace datasystem {
namespace object_cache {
static constexpr int DEBUG_LOG_LEVEL = 2;
static constexpr int OLD_VERSION_DEL_THREAD_MIN_NUM = 0;
static constexpr int OLD_VERSION_DEL_THREAD_MAX_NUM = 1;
static constexpr uint32_t SHM_QUEUE_SLOT_NUM = 32;
static const std::string WORKER_OC_SERVICE_IMPL = "WorkerOCServiceImpl";
constexpr size_t GET_MATCH_OBJECT_BATCH = 500;  // batch number is 500.

WorkerOCServiceImpl::WorkerOCServiceImpl(HostPort serverAddr, HostPort masterAddr,
                                         std::shared_ptr<ObjectTable> objectTable, std::shared_ptr<AkSkManager> manager,
                                         std::shared_ptr<WorkerOcEvictionManager> evictionManager,
                                         std::shared_ptr<PersistenceApi> persistApi, EtcdStore *etcdStore,
                                         MasterOCServiceImpl *masterOCService)
    : WorkerOCService(std::move(serverAddr)),
      localMasterAddress_(std::move(masterAddr)),
      persistenceApi_(persistApi),
      objectTable_(std::move(objectTable)),
      evictionManager_(std::move(evictionManager)),
      etcdStore_(etcdStore),
      akSkManager_(std::move(manager))
{
    initOkFuture_ = initOk_.get_future();
    workerMasterApiManager_ = std::make_shared<WorkerMasterOcApiManager>(localAddress_, akSkManager_, masterOCService);
    memoryRefTable_ = std::make_shared<SharedMemoryRefTable>();
    globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    asyncSendManager_ = std::make_shared<AsyncSendManager>(persistApi, evictionManager_);
    asyncRollbackManager_ = std::make_shared<AsyncRollbackManager>();
    exitFlag_ = std::make_shared<std::atomic_bool>(false);
}

WorkerOCServiceImpl::~WorkerOCServiceImpl()
{
    LOG(INFO) << "WorkerOCServiceImpl exit";
    // Ensure that initOk_.set_value() is called to avoid suspension when MasterLocalWorkerOCApi inits.
    if (!setValue_) {
        initOk_.set_value(Status(K_RUNTIME_ERROR, "WorkerOCServiceImpl init fail"));
        setValue_ = true;
    }
    HashRingEvent::BeforeVoluntaryExit::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    AddLocalFailedNodeEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    EraseFailedNodeApiEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    StartNodeCheckEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    exitFlag_->store(true);
    {
        // Avoid read data while modifying the vector.
        std::lock_guard<std::mutex> lock(circularQueueMutex_);
        for (const auto &decreaseRPCQ : circularQueueManager_) {
            // Some test use WorkerOCServiceImpl without init.
            if (decreaseRPCQ != nullptr) {
                decreaseRPCQ->WakeUpQueueProcessAndFinish();
            }
        }
    }
    gcThreadPool_ = nullptr;
    decThreadPool_ = nullptr;
    for (auto &s : TenantAuthManager::Instance()->clientTokenTimer_) {
        TimerQueue::GetInstance()->Cancel(s.second);
    }
}

Status WorkerOCServiceImpl::InitL2Cache()
{
    supportL2Storage_ = GetCurrentStorageType();
    if (persistenceApi_) {
        // only cloud storage need clear old object version
        oldVerDelAsyncPool_ = std::make_shared<ThreadPool>(OLD_VERSION_DEL_THREAD_MIN_NUM,
                                                           OLD_VERSION_DEL_THREAD_MAX_NUM, "DelOldVesrion");
        oldVerDelAsyncPool_->SetWarnLevel(ThreadPool::WarnLevel::LOW);
        asyncPersistenceDelManager_ =
            std::make_shared<AsyncPersistenceDelManager>(oldVerDelAsyncPool_, persistenceApi_);
    }
    if (IsSupportL2Storage(supportL2Storage_)) {
        RETURN_IF_NOT_OK(asyncSendManager_->Init());
    }
    return Status::OK();
}

void WorkerOCServiceImpl::InitServiceImpl()
{
    WorkerOcServiceCrudParam param{
        .workerMasterApiManager = workerMasterApiManager_,
        .workerRequestManager = workerRequestManager_,
        .memoryRefTable = memoryRefTable_,
        .objectTable = objectTable_,
        .evictionManager = evictionManager_,
        .workerDevOcManager = workerDevOcManager_,
        .asyncPersistenceDelManager = asyncPersistenceDelManager_,
        .asyncSendManager = asyncSendManager_,
        .asyncRollbackManager = asyncRollbackManager_,
        .metadataSize = metadataSize_,
        .persistenceApi = persistenceApi_,
        .etcdCM = etcdCM_,
    };
    createProc_ = std::make_shared<WorkerOcServiceCreateImpl>(param, etcdCM_, akSkManager_);

    publishProc_ =
        std::make_shared<WorkerOcServicePublishImpl>(param, etcdCM_, memCpyThreadPool_, akSkManager_, localAddress_);

    multiPublishProc_ = std::make_shared<WorkerOcServiceMultiPublishImpl>(param, etcdCM_, memCpyThreadPool_,
                                                                          threadPool_, akSkManager_, localAddress_);

    getProc_ = std::make_shared<WorkerOcServiceGetImpl>(param, etcdCM_, etcdStore_, memCpyThreadPool_, threadPool_,
                                                        akSkManager_, localAddress_);

    deleteProc_ = std::make_shared<WorkerOcServiceDeleteImpl>(param, etcdCM_, akSkManager_, localAddress_, getProc_);

    gRefProc_ = std::make_shared<WorkerOcServiceGlobalReferenceImpl>(param, etcdCM_, globalRefTable_, akSkManager_,
                                                                     localAddress_);

    gMigrateProc_ = std::make_shared<WorkerOcServiceMigrateImpl>(param, etcdCM_, memCpyThreadPool_, akSkManager_,
                                                                 GetLocalAddr().ToString());

    expireProc_ = std::make_shared<WorkerOcServiceExpireImpl>(param, etcdCM_, akSkManager_);
    initOk_.set_value(Status::OK());
    setValue_ = true;
}

Status WorkerOCServiceImpl::Init()
{
    InitMetaSize();
    RETURN_IF_NOT_OK(ResetHealthProbe());

    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(localMasterAddress_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "Hash master get failed, Init failed");
    const int minOcGetThreadNum = 8;
    RETURN_IF_EXCEPTION_OCCURS(threadPool_ = std::make_shared<ThreadPool>(minOcGetThreadNum, 0, "OcGetThread"));
    RETURN_IF_EXCEPTION_OCCURS(memCpyThreadPool_ = std::make_shared<ThreadPool>(MEMCOPY_THREAD_NUM));
    datasystem::Parallel::InitParallelThreadPool(PARALLEL_THREAD_NUM);
    constexpr uint32_t gcThrdNum = 4;
    RETURN_IF_EXCEPTION_OCCURS(gcThreadPool_ = std::make_unique<ThreadPool>(gcThrdNum, 0, "OcCleanClient"));

#ifndef BUILD_HETERO
    // Worker's thread to handler the requests coming up from hetero client.
    constexpr uint32_t devRpcMangerNum = 1;
    // Worker threads that send hetero request to master, and do not take up devRpcManger thread.
    constexpr uint32_t devThrdNum = 1;
#else
    constexpr uint32_t devRpcMangerNum = 16;
    constexpr uint32_t devThrdNum = 1;
#endif

    RETURN_IF_EXCEPTION_OCCURS(asyncRpcManager_ = std::make_shared<AsyncRpcRequestManager>(devThrdNum));
    RETURN_IF_EXCEPTION_OCCURS(devThreadPool_ = std::make_unique<ThreadPool>(devRpcMangerNum, 0, "devThread"));
    uint32_t decThreadMinNum = 1;
    uint32_t decThreadMaxNum = FLAGS_max_client_num / SHM_QUEUE_SLOT_NUM + 1;  // Keeping in sync with lockId
    RETURN_IF_EXCEPTION_OCCURS(decThreadPool_ =
                                   std::make_unique<ThreadPool>(decThreadMinNum, decThreadMaxNum, "OcDecRef"));
    RETURN_IF_NOT_OK(evictionManager_->Init(globalRefTable_, akSkManager_));
    RETURN_IF_NOT_OK(InitL2Cache());
    WorkerRequestManager::SetDeleteObjectsFunc(
        [this](const std::string &objectKey, uint64_t version) -> Status { return DeleteObject(objectKey, version); });
    workerDevOcManager_ = std::make_shared<WorkerDeviceOcManager>(this);
    lastReconTime_ = GetSteadyClockTimeStampMs();  // Record current timestamp in case we need reconciliation.
    RETURN_IF_NOT_OK(StartDecreaseReferenceProcess());

    asyncRollbackManager_->Init(localAddress_, workerMasterApiManager_, etcdCM_);
    InitServiceImpl();

    HashRingEvent::BeforeVoluntaryExit::GetInstance().AddSubscriber(
        WORKER_OC_SERVICE_IMPL, [this](const std::string &taskId) { return ProcessVoluntaryScaledown(taskId); });
    AddLocalFailedNodeEvent::GetInstance().AddSubscriber(
        WORKER_OC_SERVICE_IMPL, [this](const HostPort &node) { return PushMetadataToMaster(node); });
    EraseFailedNodeApiEvent::GetInstance().AddSubscriber(WORKER_OC_SERVICE_IMPL,
                                                         [this](HostPort &node) { EraseFailedWorkerMasterApi(node); });
    StartNodeCheckEvent::GetInstance().AddSubscriber(WORKER_OC_SERVICE_IMPL, [this] { return GiveUpReconciliation(); });

    return Status::OK();
}

Status WorkerOCServiceImpl::HealthCheck(const HealthCheckRequestPb &req, HealthCheckReplyPb &resp)
{
    INJECT_POINT("worker.HealthCheck.begin");
    ReadLock noRecon;
    auto rc = ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime());
    if (rc.IsError()) {
        LOG(WARNING) << rc;
        return rc;
    }
    if (not req.client_id().empty()) {
        std::string tenantId;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    }
    (void)resp;
    if (etcdCM_ != nullptr && etcdCM_->CheckLocalNodeIsExiting()) {
        constexpr int logInterval = 60;
        LOG_EVERY_T(INFO, logInterval) << "[HealthCheck] Worker is exiting now";
        RETURN_STATUS(StatusCode::K_SCALE_DOWN, "Worker is exiting now");
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::IncNestedRef(const std::vector<std::string> &nestedObjectKeys)
{
    return gRefProc_->IncNestedRef(nestedObjectKeys);
}

Status WorkerOCServiceImpl::DecNestedRef(const std::vector<std::string> &nestedObjectKeys)
{
    std::vector<std::string> unAliveIds;
    gRefProc_->DecNestedRef(nestedObjectKeys, unAliveIds);

    LOG(INFO) << FormatString("Delete unalive objects %s", VectorToString(unAliveIds));
    for (const auto &objectKey : unAliveIds) {
        Status rc = DeleteObject(objectKey);  // Delete the object if it exists locally
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("[ObjectKey %s] DeleteObject failed, error: %s.", objectKey, rc.ToString());
        }
    }
    return Status::OK();
}

size_t WorkerOCServiceImpl::GetTotalObjectSize() const
{
    memory::ShmMemStat stat;
    memory::Allocator::Instance()->GetMemStat(stat);
    return stat.objectMemoryUsage;
}

Status WorkerOCServiceImpl::Publish(const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> payloads)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return publishProc_->Publish(req, resp, payloads);
}

Status WorkerOCServiceImpl::MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                         std::vector<RpcMessage> payloads)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    auto clientId = ClientKey::Intern(req.client_id());
    RETURN_IF_NOT_OK(multiPublishProc_->MultiPublish(req, resp, payloads, clientId));
    if (req.auto_release_memory_ref()) {
        std::set<std::string> failedSet{ resp.failed_object_keys().begin(), resp.failed_object_keys().end() };
        std::vector<ShmKey> shmIds;
        shmIds.reserve(req.object_info_size());
        for (auto &info : req.object_info()) {
            if (failedSet.find(info.object_key()) != failedSet.end()) {
                continue;
            }
            shmIds.emplace_back(ShmKey::Intern(info.shm_id()));
        }
        VLOG(1) << "auto release ref " << VectorToString(shmIds);
        return DecreaseMemoryRef(clientId, shmIds);
    }
    return Status::OK();
}

void WorkerOCServiceImpl::GetObjectsMatch(std::function<bool(const std::string &)> matchFunc,
                                          std::vector<std::string> &objKeys)
{
    LOG(INFO) << "GetNoL2CacheObjectsMatch begin";
    Timer timer;
    std::vector<std::string> objs;
    for (const auto &it : *objectTable_) {
        objs.emplace_back(it.first);
    }
    LOG(INFO) << "Get all object in table ElapsedMilliSecond:" << timer.ElapsedMilliSecond()
              << " object size:" << objs.size();
    timer.Reset();
    std::function batchFun = [this, matchFunc, &objKeys](std::vector<std::string> &objects) {
        for (const auto &id : objects)
            if (matchFunc(id)) {
                std::shared_ptr<SafeObjType> entry;
                if (objectTable_->Get(id, entry).IsError()) {
                    continue;
                }
                if (entry->TryRLock().IsError()) {
                    continue;
                }
                Raii entryUnlock([&entry] { entry->RUnlock(); });
                if (!(*entry)->HasL2Cache()) {
                    objKeys.emplace_back(id);
                }
            }
    };

    std::vector<std::string> tmpIds;
    for (const auto &id : objs) {
        tmpIds.emplace_back(id);
        if (tmpIds.size() < GET_MATCH_OBJECT_BATCH) {
            continue;
        }
        batchFun(tmpIds);
        tmpIds.clear();
    }
    if (!tmpIds.empty()) {
        batchFun(tmpIds);
    }
    LOG(INFO) << "GetNoL2CacheObjectsMatch finish, objectKeys size: " << objKeys.size()
              << " ElapsedMilliSecond: " << timer.ElapsedMilliSecond();
}

Status WorkerOCServiceImpl::GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr)
{
    std::string dbName;
    RETURN_IF_NOT_OK(etcdCM_->GetPrimaryReplicaLocationByAddr(srcAddr, destAddr, dbName));
    g_MetaRocksDbName = dbName;
    return Status::OK();
}

void WorkerOCServiceImpl::GroupAndRemoveMeta(const std::vector<std::string> &objKeys,
                                             const master::RemoveMetaReqPb::Cause &removeCase,
                                             std::vector<std::string> &failedIds,
                                             std::vector<std::string> &needMigrateIds,
                                             std::vector<std::string> &needWaitIds)
{
    INJECT_POINT("ProcessVoluntaryScaledown", [this] {
        Timer timer;
        uint64_t sleepTimeMs = 100;
        uint64_t maxSecond = 5;
        while (timer.ElapsedSecond() < maxSecond) {
            std::string key = std::string(ETCD_RING_PREFIX) + "/";
            RangeSearchResult res;
            if (etcdStore_->RawGet(key, res).IsOk()) {
                HashRingPb newRing;
                if (newRing.ParseFromString(res.value) && !newRing.add_node_info().empty()) {
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        }
        return;
    });
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objKeys);
    for (const auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> currentObjectKeysRemove = item.second;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            failedIds.insert(failedIds.end(), currentObjectKeysRemove.begin(), currentObjectKeysRemove.end());
            LOG(WARNING) << "master address is empty, objectKeys don't belong to any master,"
                         << "remove meta failed, failed ids size is:" << currentObjectKeysRemove.size();
            continue;
        }
        LOG(INFO) << "remove meta req send to master: " << masterAddr.ToString() << ", removeCase: " << removeCase;
        BatchRemoveMeta(currentObjectKeysRemove, workerMasterApi, removeCase, failedIds, needMigrateIds, needWaitIds);
    }
}

Status WorkerOCServiceImpl::ProcessVoluntaryScaledown(const std::string &taskId)
{
    INJECT_POINT("ScaleUpTask.NotRunVoluntaryDownTask");
    LOG(INFO) << "ProcessVoluntaryScaledown.., obj size in worker is: " << objectTable_->GetSize()
              << " task id:" << taskId;
    std::vector<std::string> needMigrateDataIds;
    std::vector<std::string> needWaitIds;
    RETURN_IF_NOT_OK(BeforeMigrateData(taskId, needMigrateDataIds, needWaitIds));
    INJECT_POINT("VoluntaryScaledown.MigrateData.Delay");
    RETURN_IF_NOT_OK(MigrateData(needMigrateDataIds, taskId));
    // When we have finish migrate data task, we can remove the location.
    std::vector<std::string> removeFailedIds;
    GroupAndRemoveMeta(needMigrateDataIds, master::RemoveMetaReqPb::NORMAL, removeFailedIds, needMigrateDataIds,
                       needWaitIds);
    LOG(INFO) << "ProcessVoluntaryScaledown finished";

    std::lock_guard<std::shared_timed_mutex> l(clearIdsMutex_);
    voluntaryScaleDownClearIds_ = std::move(needWaitIds);
    return Status::OK();
}

Status WorkerOCServiceImpl::BeforeMigrateData(const std::string &taskId, std::vector<std::string> &needMigrateDataIds,
                                              std::vector<std::string> &needWaitIds)
{
    std::vector<std::string> objectKeysNeedRemovelocation, objKeysNeedGiveupPrimary;
    for (const auto &kv : *objectTable_) {
        std::shared_ptr<SafeObjType> entry = kv.second;
        if (entry->TryRLock().IsError()) {
            continue;
        }
        Raii readUnlock([&entry]() { entry->RUnlock(); });
        if (!(*entry)->stateInfo.IsPrimaryCopy()) {
            objectKeysNeedRemovelocation.emplace_back(kv.first);
        }
        if ((*entry)->stateInfo.IsPrimaryCopy()) {
            objKeysNeedGiveupPrimary.emplace_back(kv.first);
        }
    }
    std::vector<std::string> removeFailedIds;
    LOG(INFO) << "Need remove location object size: " << objectKeysNeedRemovelocation.size()
              << ", need give up location object size: " << objKeysNeedGiveupPrimary.size();
    GroupAndRemoveMeta(objectKeysNeedRemovelocation, master::RemoveMetaReqPb::NORMAL, removeFailedIds,
                       needMigrateDataIds, needWaitIds);

    std::vector<std::string> giveupMetaFailedIds;
    GroupAndRemoveMeta(objKeysNeedGiveupPrimary, master::RemoveMetaReqPb::GIVEUP_PRIMARY, giveupMetaFailedIds,
                       needMigrateDataIds, needWaitIds);

    // retry for failed ids.
    const int intervalMs = 500;
    const int giveUpIdsMaxRetryTime = 10;
    int retryTime = 0;
    while (true) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!etcdCM_->CheckVoluntaryTaskExpired(taskId), K_RUNTIME_ERROR,
                                             FormatString("task id %s has expired, no need retry", taskId));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!etcdCM_->CheckVoluntaryScaleDown(), K_RUNTIME_ERROR,
                                             FormatString("this node maybe failed or only one node left, no need to "
                                                          "excute voluntary scale down migrate data task, task id: %s",
                                                          taskId));

        if (removeFailedIds.empty() && giveupMetaFailedIds.empty()) {
            break;
        }
        if (retryTime > giveUpIdsMaxRetryTime) {
            needMigrateDataIds.insert(needMigrateDataIds.end(), giveupMetaFailedIds.begin(), giveupMetaFailedIds.end());
            giveupMetaFailedIds.clear();
        }
        objectKeysNeedRemovelocation = removeFailedIds;
        objKeysNeedGiveupPrimary = giveupMetaFailedIds;
        removeFailedIds.clear();
        giveupMetaFailedIds.clear();
        if (!objectKeysNeedRemovelocation.empty()) {
            GroupAndRemoveMeta(objectKeysNeedRemovelocation, master::RemoveMetaReqPb::NORMAL, removeFailedIds,
                               needMigrateDataIds, needWaitIds);
        }
        if (!objKeysNeedGiveupPrimary.empty()) {
            GroupAndRemoveMeta(objKeysNeedGiveupPrimary, master::RemoveMetaReqPb::GIVEUP_PRIMARY, giveupMetaFailedIds,
                               needMigrateDataIds, needWaitIds);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        retryTime++;
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::RemoveWriteBackIdsLocation()
{
    LOG(INFO) << "RemoveWriteBackIdsLocation begin, WriteBackIds size: " << voluntaryScaleDownClearIds_.size();
    std::vector<std::string> clearIds;
    {
        std::lock_guard<std::shared_timed_mutex> l(clearIdsMutex_);
        clearIds = std::move(voluntaryScaleDownClearIds_);
    }
    std::vector<std::string> removeFailedIds;
    std::vector<std::string> needMigrateDataIds;
    std::vector<std::string> needWaitIds;
    GroupAndRemoveMeta(clearIds, master::RemoveMetaReqPb::NORMAL, removeFailedIds, needMigrateDataIds, needWaitIds);

    // if all worker exist, this voluntary sacle down node not exit, remove meta cant success, so we try 10 times for
    // removemeta here
    const int intervalMs = 500;
    const int maxRetryTime = 10;
    int retryNum = 0;
    while (maxRetryTime > retryNum++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        if (removeFailedIds.empty()) {
            break;
        }
        clearIds = removeFailedIds;
        removeFailedIds.clear();
        if (!clearIds.empty()) {
            GroupAndRemoveMeta(clearIds, master::RemoveMetaReqPb::NORMAL, removeFailedIds, needMigrateDataIds,
                               needWaitIds);
        }
    }
    LOG(INFO) << "RemoveWriteBackIdsLocation finished, removeFailedIds :" << VectorToString(removeFailedIds);
    CHECK_FAIL_RETURN_STATUS(removeFailedIds.empty(), K_RUNTIME_ERROR, "RemoveWriteBackIdsLocation failed");
    return Status::OK();
}

Status WorkerOCServiceImpl::RemoveMeta(const std::list<std::string> objectKeysRemoveList,
                                       const std::shared_ptr<WorkerMasterOCApi> &workerMasterApi,
                                       const master::RemoveMetaReqPb::Cause removeCause, const uint64_t version,
                                       bool needRedirct, master::RemoveMetaRspPb &response)
{
    master::RemoveMetaReqPb request;
    request.set_address(localAddress_.ToString());
    request.set_cause(removeCause);
    request.set_version(version);
    request.set_redirect(needRedirct);
    *request.mutable_ids() = { objectKeysRemoveList.begin(), objectKeysRemoveList.end() };
    std::function<Status(RemoveMetaReqPb &, RemoveMetaRspPb &)> func =
        [workerMasterApi](RemoveMetaReqPb &req, RemoveMetaRspPb &rsp) { return workerMasterApi->RemoveMeta(req, rsp); };
    return WorkerOcServiceCrudCommonApi::RedirectRetryWhenMetasMoving(request, response, func);
}

void WorkerOCServiceImpl::BatchRemoveMeta(const std::vector<std::string> &objectKeys,
                                          const std::shared_ptr<WorkerMasterOCApi> &workerMasterApi,
                                          const master::RemoveMetaReqPb::Cause removeCause,
                                          std::vector<std::string> &failedIds, std::vector<std::string> &needMigrateIds,
                                          std::vector<std::string> &needWaitIds)
{
    std::list<std::string> objectKeysRemoveList;
    const uint32_t objBatch = 300;
    uint32_t count = 0;
    uint64_t version = UINT64_MAX;
    for (auto &objectKey : objectKeys) {
        objectKeysRemoveList.emplace_back(objectKey);
        ++count;
        if (count >= objBatch) {
            // dest node failed or local node failed, stop remove.
            if (etcdCM_->CheckVoluntaryScaleDown()) {
                break;
            }
            auto status = etcdCM_->CheckConnection(objectKey);
            if (status.IsError()) {
                LOG(WARNING) << "remove meta failed: " << status.ToString();
                failedIds.insert(failedIds.end(), objectKeysRemoveList.begin(), objectKeysRemoveList.end());
                continue;
            }
            reqTimeoutDuration.Init(RPC_TIMEOUT);
            master::RemoveMetaRspPb response;
            auto result = RemoveMeta(objectKeysRemoveList, workerMasterApi, removeCause, version, true, response);
            if (result.IsError()) {
                LOG(WARNING) << "remove meta failed: " << result.ToString();
                failedIds.insert(failedIds.end(), objectKeysRemoveList.begin(), objectKeysRemoveList.end());
            } else {
                failedIds.insert(failedIds.end(), response.failed_ids().begin(), response.failed_ids().end());
                needMigrateIds.insert(needMigrateIds.end(), response.need_data_ids().begin(),
                                      response.need_data_ids().end());
                needWaitIds.insert(needWaitIds.end(), response.need_wait_ids().begin(), response.need_wait_ids().end());
            }
            RemoveMetadataFromRedirectMaster(response, removeCause, failedIds, needMigrateIds, needWaitIds);
            objectKeysRemoveList.clear();
            count = 0;
        }
    }
    if (count > 0) {
        reqTimeoutDuration.Init(RPC_TIMEOUT);
        master::RemoveMetaRspPb response;
        Status result = RemoveMeta(objectKeysRemoveList, workerMasterApi, removeCause, version, true, response);
        if (result.IsError()) {
            LOG(WARNING) << "remove meta failed: " << result.ToString();
            failedIds.insert(failedIds.end(), objectKeysRemoveList.begin(), objectKeysRemoveList.end());
        } else {
            failedIds.insert(failedIds.end(), response.failed_ids().begin(), response.failed_ids().end());
            needMigrateIds.insert(needMigrateIds.end(), response.need_data_ids().begin(),
                                  response.need_data_ids().end());
            needWaitIds.insert(needWaitIds.end(), response.need_wait_ids().begin(), response.need_wait_ids().end());
        }
        RemoveMetadataFromRedirectMaster(response, removeCause, failedIds, needMigrateIds, needWaitIds);
    }
}

Status WorkerOCServiceImpl::RemoveMetadataFromRedirectMaster(master::RemoveMetaRspPb &rsp,
                                                             const master::RemoveMetaReqPb::Cause removeCause,
                                                             std::vector<std::string> &failedIds,
                                                             std::vector<std::string> &needMigrateIds,
                                                             std::vector<std::string> &needWaitIds)
{
    for (const auto &redirectInfo : rsp.info()) {
        master::RemoveMetaReqPb redirectReq;
        master::RemoveMetaRspPb redirectRsp;
        std::list<std::string> redirectIds = { redirectInfo.change_meta_ids().begin(),
                                               redirectInfo.change_meta_ids().end() };
        HostPort redirectMasterAddr;
        RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr));
        auto status = etcdCM_->CheckConnection(redirectMasterAddr);
        if (status.IsError()) {
            LOG(WARNING) << "remove meta failed: " << status.ToString();
            failedIds.insert(failedIds.end(), redirectIds.begin(), redirectIds.end());
            continue;
        }
        std::shared_ptr<WorkerMasterOCApi> redirectWorkerMasterApi =
            workerMasterApiManager_->GetWorkerMasterApi(redirectMasterAddr);
        if (redirectWorkerMasterApi == nullptr) {
            failedIds.insert(failedIds.end(), redirectIds.begin(), redirectIds.end());
            LOG(ERROR) << "failed to get redirectWorkerMasterApi, masterAddr: " << redirectInfo.redirect_meta_address();
            continue;
        }
        Status result = RemoveMeta(redirectIds, redirectWorkerMasterApi, removeCause, UINT64_MAX, false, redirectRsp);
        // save the result to rsp and payload
        if (result.IsError()) {
            LOG(WARNING) << "remove meta failed: " << result.ToString();
            failedIds.insert(failedIds.end(), redirectIds.begin(), redirectIds.end());
        } else {
            failedIds.insert(failedIds.end(), redirectRsp.failed_ids().begin(), redirectRsp.failed_ids().end());
            needMigrateIds.insert(needMigrateIds.end(), redirectRsp.need_data_ids().begin(),
                                  redirectRsp.need_data_ids().end());
            needWaitIds.insert(needWaitIds.end(), redirectRsp.need_wait_ids().begin(),
                               redirectRsp.need_wait_ids().end());
        }
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                        std::vector<RpcMessage> payloads)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    return gMigrateProc_->MigrateData(req, rsp, std::move(payloads));
}

Status WorkerOCServiceImpl::MigrateData(const std::vector<std::string> &objectKeys, const std::string &taskId,
                                        MigrateStrategy::MigrationStrategyStage stage)
{
    INJECT_POINT("WorkerOCServiceImpl.MigrateData.Delay", [](int sleepMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        return Status::OK();
    });
    if (objectKeys.empty()) {
        LOG(INFO) << "[Migrate Data] No object data need to be migrated, we have finish the job, task id: " << taskId;
        return Status::OK();
    }

    LOG(INFO) << "[Migrate Data] Processing valuntary scale down data migrate begin, object size: " << objectKeys.size()
              << ", task id: " << taskId;
    uint64_t intervalSeconds = 60;
    auto progress = std::make_shared<MigrateProgress>(
        objectKeys.size(), intervalSeconds, [](double elapsedSeconds, uint64_t processCount, uint64_t count) {
            if (processCount < count) {
                LOG(INFO) << FormatString(
                    "[Migrate Data Process] The task has been executed for %.2f seconds, %ld/%ld objects finished, "
                    "still have %ld objects need to migrate data...",
                    elapsedSeconds, processCount, count, (count - processCount));
            } else if (processCount == count) {
                LOG(INFO) << FormatString(
                    "[Migrate Data Process] The task is complete(%ld objects) and takes for %.2f seconds.", count,
                    elapsedSeconds);
            } else {
                LOG(WARNING) << FormatString(
                    "[Migrate Data Process] The task has been executed for %.2f seconds, %ld/%ld objects finished, "
                    "something wrong happen...",
                    elapsedSeconds, processCount, count);
            }
        });

    const uint32_t threadPoolSize = 4;
    auto threadPool = std::make_unique<ThreadPool>(threadPoolSize, 0, "OcMigrateData");
    std::vector<std::future<MigrateDataHandler::MigrateResult>> futures;
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    INJECT_POINT("WorkerOcService.MigrateData.GetMasterAddr", [&objKeysGrpByMaster, &objectKeys]() {
        objKeysGrpByMaster.clear();
        MetaAddrInfo info;
        (void)objKeysGrpByMaster.emplace(info, objectKeys);
        return Status::OK();
    });
    for (const auto &item : objKeysGrpByMaster) {
        futures.emplace_back(MigrateDataByNode(item.first, item.second, progress, threadPool, MigrateStrategy(stage)));
    }

    while (!futures.empty()) {
        std::vector<std::future<MigrateDataHandler::MigrateResult>> newFutures;
        RETURN_IF_NOT_OK(HandleMigrateDataResult(taskId, progress, threadPool, futures, newFutures));
        futures.swap(newFutures);
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::HandleMigrateDataResult(
    const std::string &taskId, const std::shared_ptr<MigrateProgress> progress,
    const std::unique_ptr<ThreadPool> &threadPool, std::vector<std::future<MigrateDataHandler::MigrateResult>> &futures,
    std::vector<std::future<MigrateDataHandler::MigrateResult>> &newFutures)
{
    for (auto &fut : futures) {
        auto result = fut.get();
        LOG(INFO) << MigrateDataHandler::ResultToString(result);
        if (!result.failedIds.empty()) {
            if (!taskId.empty() && etcdCM_->CheckVoluntaryTaskExpired(taskId)) {
                RETURN_STATUS_LOG_ERROR(
                    K_RUNTIME_ERROR,
                    FormatString(
                        "task id has expired, no need to excute voluntary scale down migrate data task, task id: %s",
                        taskId));
            }
            if (!taskId.empty() && etcdCM_->CheckVoluntaryScaleDown()) {
                LOG(ERROR) << "this node maybe failed or only one node left, no need to excute voluntary scale down "
                              "migrate data task, task id:"
                           << taskId;
                break;
            }
            newFutures.emplace_back(RedirectMigrateData(result.address, result.failedIds, progress, threadPool,
                                                        result.migrateDataStrategy));
        }
    }
    return Status::OK();
}

std::future<MigrateDataHandler::MigrateResult> WorkerOCServiceImpl::RedirectMigrateData(
    const std::string &originAddr, const std::unordered_set<ImmutableString> &needRetryIds,
    const std::shared_ptr<MigrateProgress> progress, const std::unique_ptr<ThreadPool> &threadPool,
    MigrateStrategy &migrateDataStrategy)
{
    std::vector<std::string> objectKeys{ needRetryIds.begin(), needRetryIds.end() };
    std::string nextWorker;
    Status status = etcdCM_->GetStandbyWorkerByAddr(originAddr, nextWorker);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] Failed to get [%s]'s next addr: %s", originAddr, status.ToString());
        return ConstructFailedFuture(originAddr, status, objectKeys, migrateDataStrategy);
    }
    if (nextWorker == localAddress_.ToString()) {
        LOG(INFO) << FormatString("[Migrate Data] Skip, [%s]'s next addr is ourselves", originAddr);
        return ConstructFailedFuture(nextWorker, status, objectKeys, migrateDataStrategy);
    }

    migrateDataStrategy.CheckAndUpgradeStage(originAddr);

    HostPort hostPort;
    status = hostPort.ParseString(nextWorker);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("[Migrate Data] Failed to parse worker address [%s]: %s", originAddr,
                                   status.ToString());
        return ConstructFailedFuture(nextWorker, status, objectKeys, migrateDataStrategy);
    }
    MetaAddrInfo addr(hostPort, "");
    return MigrateDataByNode(addr, objectKeys, progress, threadPool, migrateDataStrategy);
}

std::future<MigrateDataHandler::MigrateResult> WorkerOCServiceImpl::MigrateDataByNode(
    const MetaAddrInfo &addr, const std::vector<std::string> &objectKeys,
    const std::shared_ptr<MigrateProgress> progress, const std::unique_ptr<ThreadPool> &threadPool,
    const MigrateStrategy &migrateDataStrategy)
{
    std::future<MigrateDataHandler::MigrateResult> future;
    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteWorkerStub;
    HostPort workerAddr = addr.GetAddressAndSaveDbName();
    INJECT_POINT_NO_RETURN("WorkerOCServiceImpl.MigrateDataByNode",
                           [&workerAddr](const std::string &addr) { workerAddr.ParseString(addr); });
    Status rc = ConnectAndCreateRemoteApi(remoteWorkerStub, workerAddr);
    auto traceID = Trace::Instance().GetTraceID();
    return rc.IsOk()
               ? threadPool->Submit([this, remoteWorkerStub, objectKeys, progress, migrateDataStrategy, traceID]() {
                     TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
                     return MigrateDataByNodeImpl(remoteWorkerStub, objectKeys, progress, migrateDataStrategy);
                 })
               : ConstructFailedFuture(workerAddr.ToString().empty() ? localAddress_.ToString() : workerAddr.ToString(),
                                       rc, objectKeys, migrateDataStrategy);
}

Status WorkerOCServiceImpl::ConnectAndCreateRemoteApi(std::shared_ptr<WorkerRemoteWorkerOCApi> &remoteWorkerStub,
                                                      HostPort workerAddr)
{
    if (workerAddr == localAddress_) {
        return Status(StatusCode::K_NOT_FOUND, __LINE__, __FILE__,
                      FormatString("[Migrate Data] The node [%s] to be migrated is the current node [%s]",
                                   workerAddr.ToString(), localAddress_.ToString()));
    }

    Status rc = etcdCM_->CheckConnection(workerAddr, true);
    if (rc.IsError()) {
        return rc;
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(workerAddr.ToString(), akSkManager_, remoteWorkerStub),
                                     "[Migrate Data] Create remote worker api failed.");
    return Status::OK();
}

MigrateDataHandler::MigrateResult WorkerOCServiceImpl::MigrateDataByNodeImpl(
    const std::shared_ptr<WorkerRemoteWorkerOCApi> &remoteWorkerStub, const std::vector<std::string> &objectKeys,
    const std::shared_ptr<MigrateProgress> progress, const MigrateStrategy &migrateDataStrategy)
{
    std::vector<ImmutableString> needMigrateDataIds{ objectKeys.begin(), objectKeys.end() };
    MigrateDataHandler handler(localAddress_.ToString(), needMigrateDataIds, objectTable_, remoteWorkerStub, progress,
                               migrateDataStrategy);
    return handler.MigrateDataToRemote();
}

void WorkerOCServiceImpl::FindObjectKeyNotInRsp(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                std::vector<std::string> &currentIds,
                                                std::vector<std::string> &objectKeysMayInOtherAz)
{
    std::set<std::string> objectKeysReceivedMeta;
    for (auto &queryMeta : queryMetas) {
        (void)objectKeysReceivedMeta.emplace(queryMeta.meta().object_key());
    }
    for (auto &objKey : currentIds) {
        if (objectKeysReceivedMeta.find(objKey) == objectKeysReceivedMeta.end()) {
            VLOG(1) << "objKey " << objKey << " not in rsp";
            objectKeysMayInOtherAz.emplace_back(std::move(objKey));
        }
    }
}

std::future<MigrateDataHandler::MigrateResult> WorkerOCServiceImpl::ConstructFailedFuture(
    const std::string &workerAddr, const Status &status, const std::vector<std::string> &objectKeys,
    const MigrateStrategy &migrateDataStrategy)
{
    MigrateDataHandler::MigrateResult result;
    result.address = workerAddr;
    result.status = status;
    (void)result.failedIds.insert(objectKeys.begin(), objectKeys.end());
    result.migrateDataStrategy = migrateDataStrategy;
    std::promise<MigrateDataHandler::MigrateResult> p;
    p.set_value(result);
    return p.get_future();
}

Status WorkerOCServiceImpl::FillRequestMetaByMaster(const RequestMetaFromWorkerReqPb &req,
                                                    RequestMetaFromWorkerRspPb &rsp)
{
    const auto &masterAddress = req.address();
    HostPort masterAddr;
    masterAddr.ParseString(masterAddress);
    MetaAddrInfo metaAddrInfo(masterAddr, req.db_name());
    std::vector<std::string> objectKeys;
    GetAllObjectKeys(objectKeys);
    rsp.set_address(localAddress_.ToString());
    ObjectMetaPb *metadata = rsp.add_metas();
    bool isFill = false;
    for (const auto &objectKey : objectKeys) {
        FillMetadata(objectKey, metaAddrInfo, metadata, isFill);
        if (isFill) {
            metadata = rsp.add_metas();
            isFill = false;
        }
    }
    if (!isFill) {
        rsp.mutable_metas()->RemoveLast();
    }

    std::vector<std::string> objKeys;
    FillRefData(metaAddrInfo, objKeys);
    *rsp.mutable_gref_object_keys() = { objKeys.begin(), objKeys.end() };
    return Status::OK();
}

Status WorkerOCServiceImpl::PushMetadataToMaster(const HostPort &masterAddr)
{
    std::vector<std::string> dbNameList;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->GetPrimaryReplicaDbNames(masterAddr, dbNameList), "get db name failed");
    for (const auto &dbName : dbNameList) {
        master::PushMetaToMasterReqPb req;
        master::PushMetaToMasterRspPb rsp;
        MetaAddrInfo metaAddrInfo(masterAddr, dbName);
        // send meta-data for all objects owned by the masterAddr
        RETURN_IF_NOT_OK(FillObjData(req, metaAddrInfo));

        // send all references to object owners
        std::vector<std::string> objectKeys;
        FillRefData(metaAddrInfo, objectKeys);
        *req.mutable_gref_object_keys() = { objectKeys.begin(), objectKeys.end() };

        VLOG(1) << "PushMetadataToMaster: " << LogHelper::IgnoreSensitive(req);

        std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
            workerMasterApiManager_->GetWorkerMasterApi(metaAddrInfo.GetAddressAndSaveDbName());
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "hash master get failed, PushMeta failed");
        RETURN_IF_NOT_OK(workerMasterApi->PushMetadataToMaster(req, rsp));
    }
    VLOG(1) << "Push metadata to master success.";
    return Status::OK();
}

void WorkerOCServiceImpl::GetAllObjectKeys(std::vector<std::string> &objectKeys)
{
    LOG(INFO) << "GetAllObjectKeys";
    for (const auto &kv : *objectTable_) {
        objectKeys.emplace_back(kv.first);
    }
    LOG(INFO) << "GetAllObjectKeys finished, objectKeys size:" << objectKeys.size();
}

Status WorkerOCServiceImpl::GetMetaAddressNotCheckConnection(const std::string &objKey,
                                                             MetaAddrInfo &metaAddrInfo) const
{
    HostPort result;
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, StatusCode::K_NOT_READY, "ETCD cluster manager is not provided.");
    std::optional<RouteInfo> routeInfo;
    RETURN_IF_NOT_OK(etcdCM_->GetMetaAddressNotCheckConnection(objKey, metaAddrInfo, routeInfo));
    return Status::OK();
}

void WorkerOCServiceImpl::FillMetadata(const std::string &objectKey, const MetaAddrInfo &targetMetaAddrInfo,
                                       ObjectMetaPb *metadata, bool &isFill)
{
    MetaAddrInfo metaAddrInfo;
    Status status = GetMetaAddressNotCheckConnection(objectKey, metaAddrInfo);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("[ObjectKey %s] FillMetadata may failed, status: %s", objectKey, status.ToString());
        return;
    }
    if (metaAddrInfo != targetMetaAddrInfo) {
        return;
    }

    std::shared_ptr<SafeObjType> currSafeObj;
    if (objectTable_->Get(objectKey, currSafeObj).IsError()) {
        LOG(ERROR) << FormatString("[ObjectKey %s] FillMetadata Get object failed.", objectKey);
        return;
    }
    if (currSafeObj->RLock().IsError()) {  // lock the object
        LOG(ERROR) << FormatString("[ObjectKey %s] FillMetadata lock object failed.", objectKey);
        return;
    }
    Raii unLockRaii([&currSafeObj]() { currSafeObj->RUnlock(); });
    if ((*currSafeObj)->IsBinary() && (*currSafeObj)->IsInvalid()) {
        // Objects that have not been published or sealed do not need to be updated
        return;
    }
    if ((*currSafeObj)->IsBinary()) {
        SetObjectMetaFields(metadata, objectKey, *currSafeObj);
    }
    ConfigPb *configPb = metadata->mutable_config();
    configPb->set_write_mode((uint64_t)(*currSafeObj)->modeInfo.GetWriteMode());
    configPb->set_data_format((uint64_t)(*currSafeObj)->stateInfo.GetDataFormat());
    isFill = true;
}

Status WorkerOCServiceImpl::FillObjData(master::PushMetaToMasterReqPb &req, const MetaAddrInfo &metaAddrInfo)
{
    INJECT_POINT("worker.FillObjData.Start");
    req.set_address(localAddress_.ToString());
    std::vector<std::string> objectKeys;
    GetAllObjectKeys(objectKeys);
    ObjectMetaPb *metadata = req.add_metas();
    bool isFill = false;
    for (const auto &objectKey : objectKeys) {
        FillMetadata(objectKey, metaAddrInfo, metadata, isFill);
        if (isFill) {
            metadata = req.add_metas();
            isFill = false;
        }
    }
    if (!isFill) {
        req.mutable_metas()->RemoveLast();
    }
    return Status::OK();
}

void WorkerOCServiceImpl::FillRefData(const MetaAddrInfo &targetMetaAddrInfo, std::vector<std::string> &objectKeys)
{
    LOG(INFO) << "Filling Ref data for db name:" << targetMetaAddrInfo.ToString();
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    globalRefTable_->GetAllRef(refTable);
    if (!refTable.empty()) {
        std::vector<std::string> ids;
        std::transform(refTable.begin(), refTable.end(), std::back_inserter(ids), [](auto &kv) { return kv.first; });

        auto filter = [targetMetaAddrInfo, this](const std::string &objectKey) {
            MetaAddrInfo metaAddrInfo;
            Status status = GetMetaAddressNotCheckConnection(objectKey, metaAddrInfo);
            if (status.IsError()) {
                LOG(ERROR) << FormatString("[ObjectKey %s] Get meta address failed: %s", objectKey, status.ToString());
                return true;
            }
            return targetMetaAddrInfo != metaAddrInfo;
        };
        // we only need objectKeys with masterAddr as master
        (void)ids.erase(std::remove_if(ids.begin(), ids.end(), filter), ids.end());

        objectKeys = std::move(ids);
    }
}

void WorkerOCServiceImpl::ClearObject(const std::vector<std::string> objKeys, const ClearDataReqPb &req)
{
    for (const auto &objectKey : objKeys) {
        if (std::find(req.objkeys_migrate_finished().begin(), req.objkeys_migrate_finished().end(), objectKey)
            == req.objkeys_migrate_finished().end()) {
            std::shared_ptr<SafeObjType> entry;
            bool isInsert = false;
            auto status = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert);
            if (status.IsError()) {
                LOG(WARNING) << FormatString("objectKey: %s ReserveGetAndLock failed, status: %s", objectKey,
                                             status.ToString());
                continue;
            }
            Raii unlock([&entry]() { entry->WUnlock(); });
            ObjectKV objectKV(objectKey, *entry);
            LOG_IF_ERROR(deleteProc_->ClearObject(objectKV),
                         FormatString("Failed to erase object %s from object table", objectKey));
        }
    }
}

Status WorkerOCServiceImpl::ClearObject(const ClearDataReqPb &req)
{
    LOG(INFO) << "clear data without meta in worker, standby worker: " << req.standby_worker();
    HashRange ranges;
    std::stringstream rangesStr;
    rangesStr << "ranges: ";
    for (const auto &range : req.ranges()) {
        ranges.emplace_back(range.from(), range.end());
        rangesStr << "[" << range.from() << ", " << range.end() << "],";
    }
    if (ranges.empty() && req.worker_ids().empty()) {
        LOG(INFO) << "range and worker ids all empty";
        return Status::OK();
    }
    std::vector<std::string> uuids;
    std::function<bool(const std::string &obj)> matchFunc;
    if (req.worker_ids().empty()) {
        matchFunc = [this, &ranges](const std::string &objKey) { return etcdCM_->IsInRange(ranges, objKey, ""); };
    } else {
        uuids = { req.worker_ids().begin(), req.worker_ids().end() };
        matchFunc = [this, &ranges, &uuids](const std::string &objKey) {
            return etcdCM_->NeedToClear(objKey, ranges, uuids);
        };
    }
    LOG(INFO) << rangesStr.str() << ", uuids: " << VectorToString(uuids);
    std::vector<std::string> noL2CacheIds;
    GetObjectsMatch(matchFunc, noL2CacheIds);
    RETURN_IF_NOT_OK(gRefProc_->GIncreaseMasterRefWithLock(matchFunc, req.standby_worker()));
    ClearObject(noL2CacheIds, req);
    RETURN_IF_NOT_OK(RecoverMasterAppRefEvent::GetInstance().NotifyAll(matchFunc, req.standby_worker()));
    LOG(INFO) << "clear data without meta in worker finished";
    return Status::OK();
}

Status WorkerOCServiceImpl::ValidateWorkerState(ReadLock &noRecon, int reqTimeoutMs)
{
    Timer timer;
    if (!IsHealthy()) {
        RETURN_STATUS(K_NOT_READY, "Worker not ready");
    }
    using namespace std::chrono;
    static const int SEC_TO_MS = 1000;
    static const int TOTAL_WAIT_TIME_MS = std::min(30 * SEC_TO_MS, reqTimeoutMs);
    static const int INTERVAL_MS = 10;
    auto start = GetSteadyClockTimeStampMs();

    noRecon.Assign(&reconFlag_);
    bool rc = noRecon.TryLockIfUnlocked();
    bool hasLoggedBeforeWait = false;
    while (!rc && GetSteadyClockTimeStampMs() - start < milliseconds(TOTAL_WAIT_TIME_MS).count()) {
        if (!hasLoggedBeforeWait) {
            LOG(INFO) << "Waiting for the reconFlag...";
            hasLoggedBeforeWait = true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(INTERVAL_MS));
        rc = noRecon.TryLockIfUnlocked();
    }
    if (!rc) {
        RETURN_STATUS_LOG_ERROR(K_NOT_READY, "Worker not ready");
    }
    workerOperationTimeCost.Append("ValidateWorkerState", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status WorkerOCServiceImpl::Create(const CreateReqPb &req, CreateRspPb &resp)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return createProc_->Create(req, resp);
}

Status WorkerOCServiceImpl::MultiCreate(const MultiCreateReqPb &req, MultiCreateRspPb &resp)
{
    Status returnStatus;
    AccessRecorder accessPoint(AccessRecorderKey::DS_POSIX_MULTI_CREATE);
    Raii raii([&returnStatus, &accessPoint, &req]() {
        auto &key = req.object_key().empty() ? "" : req.object_key(0);
        accessPoint.Record(returnStatus.GetCode(), std::to_string(key.size()),
                           RequestParam{ .objectKey = ObjectKeysToAbbrStr(req.object_key()) }, returnStatus.GetMsg());
    });
    ReadLock noRecon;
    returnStatus = ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime());
    if (returnStatus.IsError()) {
        LOG(ERROR) << "validate worker state failed:" << returnStatus.ToString();
        return returnStatus;
    }
    returnStatus = createProc_->MultiCreate(req, resp);
    return returnStatus;
}

Status WorkerOCServiceImpl::Reconciliation(const PushMetaToWorkerReqPb &req)
{
    INJECT_POINT("Reconciliation.before");
    reqTimeoutDuration.Init(RPC_TIMEOUT);
    LOG(INFO) << "Reconciliation called between worker: " << localAddress_.ToString()
              << " and master: " << req.source_address();
    if (req.event_timestamp() <= 0) {
        LOG(WARNING) << "timestamp should be greater than 0";
    }
    // If not healthy, it means that we still not give up reconciliation, so lock it.
    WriteLock haveRecon(IsHealthy() ? nullptr : &reconFlag_);
    Status rc;
    if (req.event_timestamp() > timestamp_) {
        numRecon_ = 0;
        timestamp_ = req.event_timestamp();
    } else if (req.event_timestamp() < timestamp_) {
        LOG(WARNING) << "The request is out of date. Reconciling for later event. Timestamp: " << timestamp_;
        return Status::OK();
    }
    ++numRecon_;
    if (req.is_restart()) {
        lastReconTime_ = GetSteadyClockTimeStampMs();
        // Wait for clients just once. No need to use atomic bool since reconciliation is serialized by lock.
        // No need to wait in case of network recovery.
        INJECT_POINT("WorkerOCServiceImpl.Reconciliation.SkipWait", [this]() {
            waited_ = true;
            return Status::OK();
        });
        if (!waited_) {
            const size_t s2ms = 1000;  // seconds to milliseconds.
            clientReconnectPost_.WaitFor(FLAGS_client_reconnect_wait_s * s2ms);
            waited_ = true;
        }
    }
    // reconciliation global references with master.
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    std::vector<std::string> needDelGrefIds;
    globalRefTable_->GetAllRef(refTable);
    for (const auto &id : req.gref_object_keys()) {
        if (refTable.find(id) == refTable.end()) {
            needDelGrefIds.emplace_back(id);
        }
    }
    if (!needDelGrefIds.empty() && FLAGS_enable_reconciliation) {
        auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(needDelGrefIds);
        Status result = ReconciliationDecrRef(objKeysGrpByMaster);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(result, "Decrease gref in master failed. Error: " + rc.ToString());
    }
    LOG(INFO) << "Reconciliation with master " << req.source_address() << " is done.";
    RETURN_IF_NOT_OK(GetReadyToWork(req));

    return Status::OK();
}

Status WorkerOCServiceImpl::GetReadyToWork(const PushMetaToWorkerReqPb &req)
{
    int hashWorkerNum = 0;
    RETURN_IF_NOT_OK(etcdCM_->GetHashRingWorkerNum(hashWorkerNum, true));
    if ((hashWorkerNum >= 0 && hashWorkerNum == numRecon_) || (hashWorkerNum < 0 && numRecon_ == 1)) {
        LOG(INFO) << "Reconciliation with all masters is done.";
        RETURN_IF_NOT_OK(CheckWaitNodeTableComplete());
        if (req.is_restart()) {
            LOG(INFO) << "Restart finish. Set health file.";
            if (!etcdCM_->IsCreateFirstLease() && etcdCM_->IsEtcdAvailableWhenStart()) {
                RETURN_STATUS(K_NOT_READY,
                              "Setting the health file is not allowed before the first lease is successfully created");
            }
            setHealthFile_.store(true);
            RETURN_IF_NOT_OK(SetHealthProbe());
        }
        if (etcdCM_->CheckLocalNodeIsExiting()) {
            INJECT_POINT("recover.toexiting.delay");
            RETURN_IF_NOT_OK(etcdStore_->UpdateNodeState(ETCD_NODE_EXITING));
        } else {
            RETURN_IF_NOT_OK(etcdCM_->InformEtcdReconciliationDone());
        }
    } else {
        LOG(INFO) << "Has finished reconciliation master num: " << numRecon_ << ", total expect num: " << hashWorkerNum;
    }
    INJECT_POINT("WorkerOCServiceImpl.Reconciliation.expectedReconNum", [this](int expectedReconNum) {
        if (!setHealthFile_.load() && expectedReconNum == numRecon_) {
            setHealthFile_.store(true);
            RETURN_IF_NOT_OK(SetHealthProbe());
            RETURN_IF_NOT_OK(etcdStore_->UpdateNodeState(ETCD_NODE_READY));
        }
        return Status::OK();
    });

    return Status::OK();
}

Status WorkerOCServiceImpl::ReconciliationDecrRef(
    const std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster)
{
    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        const std::vector<std::string> &currentNeedDelGrefIds = item.second;
        auto func = [this, &currentNeedDelGrefIds, &masterAddr](int32_t) {
            std::unordered_set<std::string> unAliveIds;
            std::vector<std::string> failDecIds;
            auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
            CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                     "hash master get failed, Reconciliation failed");
            return workerMasterApi->GDecreaseMasterRef(currentNeedDelGrefIds, unAliveIds, failDecIds);
        };
        constexpr int32_t timeoutMs = 1000 * 60;
        Status rc = RetryOnError(
            timeoutMs, func, []() { return Status::OK(); },
            { StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED, StatusCode::K_RPC_UNAVAILABLE });
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Push need deleted metadata to master failed: " + rc.ToString());
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::Get(std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetRspPb, GetReqPb>> serverApi)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return getProc_->Get(serverApi);
}

Status WorkerOCServiceImpl::RefreshMeta(const ClientKey &clientId)
{
    LOG(INFO) << "[RefreshMeta] clear memory reference count for client:" << clientId;
    std::shared_ptr<ClientInfo> clientInfo;
    clientInfo = ClientManager::Instance().GetClientInfo(clientId);
    if (clientInfo == nullptr) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "invalid client id");
    }
    // Release the queue lock form client.
    uint32_t lockId;
    if (clientInfo->GetLockId(lockId).IsOk()) {
        LOG_IF_ERROR(TryUnShmQueueLatch(lockId), "Failed to clear locked id");
    };
    // 0th: Release the object buffer resource
    std::vector<ShmKey> shmIds;
    memoryRefTable_->GetClientRefIds(clientId, shmIds);
    for (const auto &shmId : shmIds) {
        std::shared_ptr<ShmUnit> shmUnit;
        auto stat = memoryRefTable_->GetShmUnit(shmId, shmUnit);
        if (stat.IsOk()) {
            TryUnlatch(shmUnit->pointer, lockId);
        }
    }
    // 1st: Cleanup Shm.
    auto status = memoryRefTable_->RemoveClient(clientId);
    if (status.IsError()) {
        LOG(ERROR) << status.ToString();
    }

    // 2nd: Cleanup GRef.
    if (gcThreadPool_ != nullptr) {
        auto traceId = Trace::Instance().GetTraceID();
        gcThreadPool_->Execute([this, clientId, traceId] {
            auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
            AsyncClearClientRef(clientId);
        });
    }

    // 3rd: Clean up directory metadata
    Status rc = ClearDeviceMetaData(clientId);
    LOG(INFO) << "release metadata " << clientId;
    return rc;
}

Status WorkerOCServiceImpl::ClearDeviceMetaData(const ClientKey &clientId)
{
    if (gcThreadPool_ != nullptr) {
        auto traceId = Trace::Instance().GetTraceID();
        gcThreadPool_->Execute([this, clientId, traceId] {
            auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
            std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
                workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
            if (workerMasterApi == nullptr) {
                LOG(ERROR) << "hash master get failed, GetP2P meta failed";
                return;
            }
            ReleaseMetaDataReqPb req;
            req.set_client_id(clientId);
            req.set_worker_address(localAddress_.ToString());
            ReleaseMetaDataRspPb res;
            LOG_IF_ERROR(workerMasterApi->ReleaseMetaData(req, res), "ReleaseMetaData have error in send");
        });
    } else {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "gcThreadPool or workerMasterApi is nullptr in RefreshMeta");
    }
    return Status::OK();
}

void WorkerOCServiceImpl::AsyncClearClientRef(const ClientKey &clientId, uint64_t retryTimes)
{
    std::vector<std::string> objectKeys;
    globalRefTable_->GetClientRefIds(clientId, objectKeys);
    if (objectKeys.empty()) {
        return;
    }
    LOG(INFO) << "[Ref] AsyncClearClientRef for client id: " << clientId << ", total object count:" << objectKeys.size()
              << ", retryTimes:" << retryTimes;

    auto decRefFunc = [this, &clientId](const std::vector<std::string> &objectKeys) {
        std::vector<std::string> failedIds;
        LOG(INFO) << "[Ref] AsyncClearClientRef for client id: " << clientId
                  << ", objects: " << VectorToString(objectKeys);
        reqTimeoutDuration.Init();
        LOG_IF_ERROR(gRefProc_->GDecreaseRefWithLock(objectKeys, clientId, failedIds), "GDecreaseRef failed.");
        return failedIds.empty();
    };
    bool needRetry = false;
    uint32_t objectBatch = 10000;  // The objects need to be sent in batches if there are millions of objects.
    std::vector<std::string> needDecRefObjectKeys;
    for (auto &objectKey : objectKeys) {
        needDecRefObjectKeys.emplace_back(objectKey);
        if (needDecRefObjectKeys.size() >= objectBatch) {
            if (!decRefFunc(needDecRefObjectKeys)) {
                needRetry = true;
            }
            needDecRefObjectKeys.clear();
        }
    }
    if (needDecRefObjectKeys.size() > 0 && !decRefFunc(needDecRefObjectKeys)) {
        needRetry = true;
    }
    if (needRetry && gcThreadPool_ != nullptr) {
        auto traceId = Trace::Instance().GetTraceID();
        static std::vector<uint64_t> retryDelaySec = { 1, 2, 4, 8, 16, 32, 64 };
        const uint64_t secToMs = 1000;
        uint64_t delaySec = retryTimes < retryDelaySec.size() ? retryDelaySec[retryTimes] : retryDelaySec.back();
        auto delayTask = [this, traceId, clientId, retryTimes, exitFlag = exitFlag_] {
            if (exitFlag->load()) {
                return;
            }
            gcThreadPool_->Execute([this, clientId, retryTimes, traceId] {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                AsyncClearClientRef(clientId, retryTimes + 1);
            });
        };
        TimerQueue::TimerImpl timer;
        TimerQueue::GetInstance()->AddTimer(delaySec * secToMs, delayTask, timer);
    } else {
        LOG(INFO) << "[Ref] AsyncClearClientRef finish for client id: " << clientId;
    }
}

Status WorkerOCServiceImpl::GetObjectFromAnywhere(const ReadKey &readKey, const master::QueryMetaInfoPb &queryMeta,
                                                  std::vector<RpcMessage> &payloads)
{
    return getProc_->GetObjectFromAnywhere(readKey, queryMeta, payloads);
}

Status WorkerOCServiceImpl::GetDataFromL2CacheForPrimaryCopy(const std::string &objectKey, uint64_t version,
                                                             std::shared_ptr<SafeObjType> &safeEntry)
{
    return getProc_->GetDataFromL2CacheForPrimaryCopy(objectKey, version, safeEntry);
}

void WorkerOCServiceImpl::EraseFailedWorkerMasterApi(HostPort &masterAddr)
{
    workerMasterApiManager_->EraseFailedWorkerMasterApi(masterAddr, StubType::WORKER_MASTER_OC_SVC);
}

Status WorkerOCServiceImpl::GetShmQueueUnit(uint32_t lockId, int &fd, uint64_t &mmapSize, ptrdiff_t &offset, ShmKey &id)
{
    size_t index = lockId / SHM_QUEUE_SLOT_NUM;
    std::shared_ptr<ShmCircularQueue> circularQueue;
    {
        constexpr static uint32_t halfData = 2;
        //  Protect circularQueueMutex_ emplace_back and avoid creating two pools at once in case of concurrency.
        std::lock_guard<std::mutex> lock(circularQueueMutex_);
        auto queueSize = circularQueueManager_.size();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(queueSize > 0, K_RUNTIME_ERROR, "Can not find any queue in use.");
        while ((queueSize * SHM_QUEUE_SLOT_NUM - SHM_QUEUE_SLOT_NUM / halfData) < lockId) {
            LOG(INFO) << "DecreaseReference queue count:" << queueSize << ", cap:" << circularQueueManager_.capacity();
            RETURN_IF_NOT_OK(StartDecreaseReferenceProcess());
            queueSize = circularQueueManager_.size();
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            index < queueSize, K_RUNTIME_ERROR,
            FormatString("The index out of range, queueSize %zu, index %zu", queueSize, index));
        circularQueue = circularQueueManager_[index];
    }
    RETURN_RUNTIME_ERROR_IF_NULL(circularQueue);
    VLOG(1) << FormatString("Get queue index is : %d", index);
    return circularQueue->GetQueueShmUnit(fd, mmapSize, offset, id);
}

void WorkerOCServiceImpl::DecreaseHandlerForShmQueue(uint8_t *element)
{
    constexpr static uint32_t waitNum = 1;
    uint32_t *flag = (uint32_t *)element;
    if (*flag != waitNum) {
        LOG(ERROR) << "Client is not ready, get flag is 0!";
        return;
    }
    std::string byteShmId((char *)element + sizeof(uint32_t), UUID_SIZE);
    auto shmId = ShmKey::Intern(BytesUuidToString(byteShmId));
    std::string byteClientId((char *)element + sizeof(uint32_t) + UUID_SIZE, UUID_SIZE);
    auto clientId = ClientKey::Intern(BytesUuidToString(byteClientId));
    // to do clear all client ref with the shmId;
    VLOG(1) << FormatString("Worker get dec [clientId <-> shmId] : [%s<->%s]", clientId, shmId);
    // continue and wake up client process.
    LOG_IF_ERROR(DecreaseMemoryRef(clientId, { shmId }), "Failed to decrease shm ref.");
    *flag = 0;  // reset and notify client wakeup.
    uint8_t retryTime = 3;
    do {
        uint32_t result = Lock::FutexWake(flag);
        VLOG(1) << FormatString("The wake up result is : %d", result);
        if (result == waitNum) {
            break;  // Only one client can hold this flag, waitNum must equal to 1;
        }
        if (result > waitNum) {
            LOG(WARNING) << FormatString("To many client. [shmId : %s] [clientId : %s]", shmId, clientId);
            break;
        }
        if (retryTime < waitNum) {
            LOG(WARNING) << FormatString("Failed to wake up any client[shmId : %s] [clientId : %s]", shmId, clientId);
            break;
        }
        retryTime--;
    } while (retryTime > 0);
}

Status WorkerOCServiceImpl::InitShmCircularQueue(std::shared_ptr<ShmCircularQueue> &decreaseRPCQ)
{
    QueueInfo defaultMeta;
    // struct is : | Flag | lockId | QueueSize | HeadPos | elementSize * n |;
    uint32_t elementSize = defaultMeta.elementFlagSize + defaultMeta.elementDataSize + defaultMeta.elementDataSize;
    uint32_t memorySize = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) + defaultMeta.capacity * elementSize;
    auto shmUnit = std::make_shared<ShmUnit>();
    shmUnit->id = ShmKey::Intern(GetStringUuid());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(shmUnit->AllocateMemory(DEFAULT_TENANT_ID, memorySize, true),
                                     "Allocate memory failed");
    auto result = memset_s(shmUnit->pointer, memorySize, 0, memorySize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(result == EOK, K_RUNTIME_ERROR,
                                         FormatString("Memory set failed, the memset_s return: %d: ", result));
    uint32_t queueSize = defaultMeta.elementFlagSize + defaultMeta.elementDataSize + defaultMeta.elementDataSize;
    decreaseRPCQ = std::make_shared<ShmCircularQueue>(defaultMeta.capacity, queueSize, shmUnit);
    circularQueueManager_.emplace_back(decreaseRPCQ);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(decreaseRPCQ->Init(), "Failed to init the shared memory queue");
    RETURN_IF_NOT_OK(decreaseRPCQ->SetGetDataHandler(
        std::bind(&WorkerOCServiceImpl::DecreaseHandlerForShmQueue, this, std::placeholders::_1)));
    return Status::OK();
}

Status WorkerOCServiceImpl::StartDecreaseReferenceProcess()
{
    std::shared_ptr<ShmCircularQueue> decreaseRPCQ;
    RETURN_IF_NOT_OK(InitShmCircularQueue(decreaseRPCQ));
    decreaseRPCQ->UpdateQueueMeta();
    auto dfxTestFunc = []() -> Status {
        INJECT_POINT("worker.Decrease_Reference_Deadlock");
        return Status::OK();
    };
    decThreadPool_->Execute([this, &dfxTestFunc, decreaseRPCQ]() {
        constexpr int timeKilo = 1000;
        const struct timespec timeoutStruct = { .tv_sec = static_cast<long int>(RPC_TIMEOUT / timeKilo), .tv_nsec = 0 };
        while (!exitFlag_->load()) {
            auto futexRc = decreaseRPCQ->WaitForQueueEmpty(timeoutStruct);
            if (exitFlag_->load()) {
                return;
            }
            if (futexRc.IsError()) {
                if (futexRc.GetCode() == K_UNKNOWN_ERROR) {
                    LOG(ERROR) << "Get result from wait queue empty : " << futexRc.ToString();
                } else {
                    VLOG(DEBUG_LOG_LEVEL) << "Futex wait timeout, no client sending decrease in 60s !";
                }
                continue;  // if wait time out it also need to retry del data.
            }
            auto writeRc = decreaseRPCQ->WriteLock();
            if (dfxTestFunc().IsError()) {
                return;  // deadlock test.
            }
            if (writeRc.IsError()) {
                LOG_IF_ERROR(writeRc, "Failed to add write lock");
                continue;
            }
            {  // Auto release queue write lock.
                Raii unlockAll([decreaseRPCQ]() { decreaseRPCQ->WriteUnlock(); });
                decreaseRPCQ->UpdateQueueMeta();
                if (decreaseRPCQ->Length() == 0) {
                    continue;
                }
                int popSize = decreaseRPCQ->GetAndPopAll();
                if (popSize <= 0) {
                    LOG(ERROR) << "Decrease handler got some error.";
                    continue;
                }
            }
            decreaseRPCQ->NotifyQueueNotFull();
        }
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::DecreaseMemoryRef(const ClientKey &clientId, const std::vector<ShmKey> &shmIds)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    Status decResult = Status::OK();
    for (const auto &shmId : shmIds) {
        Status rc = memoryRefTable_->RemoveShmUnit(clientId, shmId);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("[ObjectKey %s] DoDecrease failed, error: %s", shmId, rc.ToString());
            decResult = rc;
        }
    }
    return decResult;
}

Status WorkerOCServiceImpl::DecreaseReference(const DecreaseReferenceRequest &req, DecreaseReferenceResponse &resp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    if (req.object_keys_size() > 0) {
        LOG(INFO) << FormatString("[shmId %s] [client: %s] DoDecrease", req.object_keys(0), req.client_id());
    }
    std::vector<ShmKey> shmIds;
    // Although the field in pb is called object key, its content is actually shmId, which is very misleading.
    shmIds.reserve(req.object_keys().size());
    std::transform(req.object_keys().begin(), req.object_keys().end(), std::back_inserter(shmIds),
                   [](const auto &key) { return ShmKey::Intern(key); });
    auto rc = DecreaseMemoryRef(ClientKey::Intern(req.client_id()), shmIds);
    if (rc.IsError()) {
        resp.mutable_error()->set_error_code(rc.GetCode());
        resp.mutable_error()->set_error_msg(rc.GetMsg());
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(resp.error().error_code() == 0,
                                         static_cast<StatusCode>(resp.error().error_code()),
                                         "Decrease reference failed");
    workerOperationTimeCost.Append("DecreaseReference", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("[Ref] DecreaseReference finish. The operations of worker DecreaseReference %s",
                              workerOperationTimeCost.GetInfo());
    return Status::OK();
}

Status WorkerOCServiceImpl::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");

    return gRefProc_->ReleaseGRefs(req, resp);
}

Status WorkerOCServiceImpl::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");

    return gRefProc_->GIncreaseRef(req, resp);
}

Status WorkerOCServiceImpl::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");

    return gRefProc_->GDecreaseRef(req, resp);
}

Status WorkerOCServiceImpl::RemoveMetaFromMaster(const std::list<std::string> &objectKeysRemove,
                                                 master::RemoveMetaReqPb::Cause removeCause)
{
    VLOG(1) << "RemoveMetaFromMaster start. object list:" << VectorToString(objectKeysRemove)
            << ", removeCause:" << removeCause;
    PerfPoint pointMeta(PerfKey::WORKER_REMOVE_META);

    // Group ObjectKeys by master
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeysRemove);

    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first.GetAddressAndSaveDbName();
        std::vector<std::string> &currentObjectKeysRemove = item.second;
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
        if (workerMasterApi == nullptr) {
            LOG(WARNING) << "get master api failed. masterAddr=" << masterAddr.ToString();
            continue;
        }
        RemoveMetaReqPb req;
        req.set_version(UINT64_MAX);
        req.set_address(localAddress_.ToString());
        req.set_cause(removeCause);
        *req.mutable_ids() = { currentObjectKeysRemove.begin(), currentObjectKeysRemove.end() };
        RemoveMetaRspPb rsp;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerMasterApi->RemoveMeta(req, rsp), "RemoveMeta failed.");
    }
    pointMeta.Record();
    VLOG(1) << "RemoveMetaFromMaster end";
    return Status::OK();
}

Status WorkerOCServiceImpl::DeleteAllCopy(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    PerfPoint point(PerfKey::WORKER_DELETE_OBJECT);
    if (req.are_device_objects()) {
        return DeleteDevObjects(req, resp);
    }
    return deleteProc_->DeleteAllCopy(req, resp);
}

Status WorkerOCServiceImpl::DeleteCopyNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp)
{
    return deleteProc_->DeleteCopyNotification(req, rsp);
}

Status WorkerOCServiceImpl::InvalidateBuffer(const InvalidateBufferReqPb &req, InvalidateBufferRspPb &rsp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    LOG(INFO) << FormatString("InvalidateBuffer begin, cliendId: %s, objectKey: %s", req.client_id(), req.object_key());
    (void)rsp;
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_key());

    std::shared_ptr<SafeObjType> entry;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectTable_->Get(namespaceUri, entry), "worker objecttable get entry failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(entry->WLock(true), "entry wlock failed");
    Raii unlock([&entry]() { entry->WUnlock(); });
    if (entry->Get() == nullptr) {
        LOG(WARNING) << FormatString("The memory of object %s has been removed, cannot invalidate.", namespaceUri);
        return Status::OK();
    }
    entry->Get()->stateInfo.SetCacheInvalid(true);
    if (entry->Get()->stateInfo.IsPrimaryCopy()) {
        std::list<std::string> objectKeysRemove{ namespaceUri };
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            RemoveMetaFromMaster(objectKeysRemove, master::RemoveMetaReqPb::INVALID_BUFFER), "remove meta failed");
    }
    workerOperationTimeCost.Append("Total InvalidateBuffer", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("InvalidateBuffer end, clientId: %s. The operations of worker InvalidateBuffer %s",
                              req.client_id(), workerOperationTimeCost.GetInfo());
    return Status::OK();
}

Status WorkerOCServiceImpl::QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");

    return gRefProc_->QueryGlobalRefNum(req, rsp);
}

Status WorkerOCServiceImpl::RecoveryClient(const ClientKey &clientId, const std::string &tenantId,
                                           const std::string &reqToken,
                                           const google::protobuf::RepeatedPtrField<google::protobuf::Any> &req)
{
    bool needConstructObjUri = !reqToken.empty() && TenantAuthManager::Instance()->AuthEnabled();
    for (const ::google::protobuf::Any &ext : req) {
        // Recovery the global reference info.
        if (ext.Is<GRefRecoveryPb>()) {
            GRefRecoveryPb gRefInfos;
            ext.UnpackTo(&gRefInfos);
            std::vector<std::string> objectKeys;
            objectKeys.reserve(gRefInfos.object_keys_size());
            for (const auto &objKey : gRefInfos.object_keys()) {
                // client ref table obj key contain tenantId.
                std::string objNameSpaceUri = objKey;
                if (needConstructObjUri) {
                    objNameSpaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, objKey);
                }
                objectKeys.emplace_back(std::move(objNameSpaceUri));
            }
            std::vector<std::string> failedIncIds;
            std::vector<std::string> firstIncIds;
            RETURN_IF_NOT_OK(globalRefTable_->GIncreaseRef(clientId, objectKeys, failedIncIds, firstIncIds));
        }
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::DeleteObject(const std::string &objectKey, uint64_t version)
{
    LOG(INFO) << FormatString("[ObjectKey %s] DeleteObject begin%s.", objectKey,
                              (version > 0 ? ", version " + std::to_string(version) : ""));
    std::shared_ptr<SafeObjType> entry;
    RETURN_IF_NOT_OK(objectTable_->Get(objectKey, entry));
    ObjectKV objectKV(objectKey, *entry);
    auto func = [this, &objectKV, version] {
        uint64_t currentVersion = objectKV.GetObjEntry()->GetCreateTime();
        if (version > 0 && version != currentVersion) {
            LOG(WARNING) << FormatString(
                "[ObjectKey %s] version not match, try delete version is %zu but current version is %zu, skip "
                "delete",
                objectKV.GetObjKey(), version, currentVersion);
            return Status::OK();
        }
        return deleteProc_->ClearObject(objectKV);
    };
    if (entry->IsWLockedByCurrentThread()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(func(),
                                         FormatString("[ObjectKey %s] ClearObjectAndDelL2cache failed.", objectKey));
    } else {
        RETURN_IF_NOT_OK(entry->WLock());
        Raii unlock([&entry]() { entry->WUnlock(); });
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(func(),
                                         FormatString("[ObjectKey %s] ClearObjectAndDelL2cache failed.", objectKey));
    }
    return Status::OK();
}

void WorkerOCServiceImpl::BatchLock(const std::vector<std::string> &objectKeys,
                                    std::map<std::string, std::shared_ptr<SafeObjType>> &lockedEntries)
{
    std::set<std::string> needLock{ objectKeys.begin(), objectKeys.end() };
    for (const auto &objectKey : needLock) {
        std::shared_ptr<SafeObjType> entry;
        if (objectTable_->Get(objectKey, entry).IsOk() && entry->WLock().IsOk()) {
            (void)lockedEntries.emplace(objectKey, std::move(entry));
        }
    }
}

namespace {
constexpr int NO_LOCK_NUM = 0;
constexpr int WRITE_LOCK_NUM = 1;
constexpr int READ_LOCK_NUM = 2;

/**
 * @brief Wake up the waiting lock.
 * @param[in] pointer Shared memory pointer.
 * @return O means success.
 */
long FutexWake(uint32_t *pointer)
{
    return syscall(SYS_futex, pointer, FUTEX_WAKE, INT_MAX, nullptr, nullptr, 0);
}
}  // namespace

Status WorkerOCServiceImpl::TryUnShmQueueLatch(uint32_t lockId)
{
    VLOG(1) << FormatString("Try to unlock lockId : %u", lockId);
    std::shared_ptr<ShmCircularQueue> circularQueue;
    {
        size_t index = static_cast<size_t>(lockId) / SHM_QUEUE_SLOT_NUM;
        std::lock_guard<std::mutex> lock(circularQueueMutex_);
        auto queueSize = circularQueueManager_.size();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(queueSize > 0, K_RUNTIME_ERROR, "Can not find any queue in use.");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            index < queueSize, K_RUNTIME_ERROR,
            FormatString("The index out of range, queueSize %zu, index %zu", queueSize, index));
        circularQueue = circularQueueManager_[index];
    }
    RETURN_RUNTIME_ERROR_IF_NULL(circularQueue);
    circularQueue->TrySharedUnlockByLockId(lockId % SHM_QUEUE_SLOT_NUM);
    return Status::OK();
}

void WorkerOCServiceImpl::TryUnlatch(void *pointer, int lockId)
{
    constexpr uint32_t BYTES = 0x8;
    uint32_t *flag = (uint32_t *)pointer;
    uint8_t *recorder = (uint8_t *)pointer + sizeof(uint32_t) + (lockId / BYTES);
    uint8_t addMask = 1 << (lockId % BYTES);
    uint8_t subMask = 0xFF ^ addMask;

    if (*recorder & addMask) {
        *recorder &= subMask;
        uint32_t lockVal = __atomic_load_n(flag, __ATOMIC_RELAXED);
        if (lockVal == WRITE_LOCK_NUM) {
            uint32_t expectedVal = WRITE_LOCK_NUM;
            __atomic_compare_exchange_n(flag, &expectedVal, NO_LOCK_NUM, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
            FutexWake(flag);
        } else {
            if (__atomic_sub_fetch(flag, READ_LOCK_NUM, __ATOMIC_SEQ_CST) == NO_LOCK_NUM) {
                FutexWake(flag);
            }
        }
    }
}

void WorkerOCServiceImpl::InitMetaSize()
{
    constexpr int alignment = 0x8;
    // Worker set lockId_ = 0(shm_guard), so we need client_nums + 1 bits slot.
    metadataSize_ = FLAGS_max_client_num == 0 ? 0 : FLAGS_max_client_num / alignment + 1;
    metadataSize_ += sizeof(uint32_t) + sizeof(char);
    auto alignCeiling = [](uintptr_t addr, uintptr_t alignment) { return (addr + alignment - 1) & ~(alignment - 1); };
    metadataSize_ = alignCeiling(metadataSize_, 0x40);
}

size_t WorkerOCServiceImpl::GetMetadataSize() const
{
    return metadataSize_;
}

Status WorkerOCServiceImpl::IfNeedTriggerReconciliation()
{
    // only use this function to reconcile with central master when enable_distribute_master is true.
    LOG(INFO) << "Just started. Ask master whether this worker needs reconciliation.";
    HostPort masterAddr(localMasterAddress_);
    auto api = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    CHECK_FAIL_RETURN_STATUS(api != nullptr, StatusCode::K_INVALID,
                             "Getting master api failed. masterAddrs=" + masterAddr.ToString());
    auto traceId = Trace::Instance().GetTraceID();
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);

    ReconciliationQueryPb req;
    ReconciliationRspPb rsp;
    req.set_event_timestamp(std::chrono::system_clock::now().time_since_epoch().count());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(api->IfNeedTriggerReconciliation(req, rsp), "worker reconciliation failed");

    return Status::OK();
}

bool WorkerOCServiceImpl::HaveAsyncTasksRunning()
{
    if (asyncSendManager_ == nullptr) {
        return false;
    }
    return !asyncSendManager_->IsAsyncTasksQueueEmpty();
}

bool WorkerOCServiceImpl::AsyncTaskHealth()
{
    if (asyncSendManager_ == nullptr) {
        return true;
    }
    return asyncSendManager_->CheckHealth();
}

void WorkerOCServiceImpl::RemoveAsyncTasks(const std::vector<std::string> &objectKeys)
{
    if (asyncSendManager_ == nullptr) {
        return;
    }
    for (const auto &objectKey : objectKeys) {
        asyncSendManager_->Remove(objectKey);
    }
}

std::vector<std::string> WorkerOCServiceImpl::StopAndGetAllUnfinishedObjects()
{
    if (asyncSendManager_ == nullptr) {
        return {};
    }
    asyncSendManager_->Stop();
    return asyncSendManager_->GetAllUnfinishedObjects();
}

Status WorkerOCServiceImpl::WhetherNonRestart()
{
    LOG(INFO) << "Trying to determine whether need reconciliation due to restart.";
    bool isRestart = false;
    RETURN_IF_NOT_OK(etcdCM_->IsRestart(isRestart));
    if (!isRestart || !etcdCM_->IsEtcdAvailableWhenStart() || !FLAGS_enable_reconciliation) {
        RETURN_IF_NOT_OK(CheckWaitNodeTableComplete());
        LOG(INFO) << "Did not restart so no need to reconcile. Set health file.";
        setHealthFile_.store(true);
        RETURN_IF_NOT_OK(SetHealthProbe());
        if (etcdCM_->IsEtcdAvailableWhenStart()) {
            RETURN_IF_NOT_OK(etcdStore_->UpdateNodeState(ETCD_NODE_READY));
        }
    } else {
        LOG(INFO) << "Local node restarted. Need reconciliation.";
        if (etcdCM_->IsCentralized()) {
            LOG(INFO) << "Local node has centralized master. Will trigger reconciliation by myself.";
            RETURN_IF_NOT_OK(IfNeedTriggerReconciliation());
        }
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::GiveUpReconciliation()
{
    // In case of centralized master, reconciliation is triggered by starting worker. No need to wait for
    // reconciliation requests from master.
    bool isRestart = false;
    auto rc = etcdCM_->IsRestart(isRestart);
    if (etcdCM_->IsCentralized() || rc.IsError() || !isRestart || !etcdCM_->IsEtcdAvailableWhenStart()) {
        return Status::OK();
    }
    if (FLAGS_enable_reconciliation) {
        static const int64_t MAX_WAIT_TIME_SEC = 60;  // 60s
        auto waitMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds(MAX_WAIT_TIME_SEC)).count();
        INJECT_POINT("WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile", [&waitMs](int waitTimeMs) {
            waitMs = waitTimeMs;
            return Status::OK();
        });

        WriteLock haveRecon;
        haveRecon.Assign(&reconFlag_);
        bool rec = haveRecon.TryLockIfUnlocked();
        if (!rec) {
            return Status::OK();  // loop back and try again later
        }
        int hashWorkerNum = 0;
        RETURN_IF_NOT_OK(etcdCM_->GetHashRingWorkerNum(hashWorkerNum, true));
        // no need to set health or not ready to set health
        if (setHealthFile_ || (numRecon_ < hashWorkerNum && GetSteadyClockTimeStampMs() - lastReconTime_ <= waitMs)
            || !etcdStore_->IsCreateFirstLease()) {
            return Status::OK();
        }

        if (numRecon_ < hashWorkerNum) {
            LOG(ERROR) << "Did not finish reconciling with all masters within " << MAX_WAIT_TIME_SEC
                       << " seconds. Give up."
                       << " Plan to reconciliate with: " << hashWorkerNum << ", had reconciliated with: " << numRecon_;
            etcdCM_->CompleteNodeTableWithFakeNode();
        }
    } else {
        LOG_FIRST_N(INFO, 1) << "enable_reconciliation is false, set worker healthy";
        return Status::OK();
    }
    setHealthFile_.store(true);
    RETURN_IF_NOT_OK(SetHealthProbe());
    RETURN_IF_NOT_OK(etcdStore_->UpdateNodeState(ETCD_NODE_READY));
    return Status::OK();
}

Status WorkerOCServiceImpl::CheckWaitNodeTableComplete()
{
    return etcdCM_->CheckWaitNodeTableComplete();
}

bool WorkerOCServiceImpl::IsInRemoteGetProgress(const std::string &objectKey)
{
    return getProc_->IsInRemoteGetObject(objectKey);
}

bool WorkerOCServiceImpl::IsInRollbackProgress(const std::string &objectKey)
{
    return asyncRollbackManager_->IsObjectsInRollBack({ objectKey });
}

Status WorkerOCServiceImpl::PublishDeviceObject(const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &resp,
                                                std::vector<RpcMessage> payloads)
{
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    std::vector<ShmKey> shmUnits = { ShmKey::Intern(req.shm_id()) };
    RETURN_IF_NOT_OK(
        WorkerOcServiceCrudCommonApi::CheckShmUnitByTenantId(tenantId, clientId, shmUnits, memoryRefTable_));
    PerfPoint point(PerfKey::WORKER_SEAL_OBJECT);
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    const std::string &objectKey = req.dev_object_key();
    std::string namespaceUri = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, objectKey);

    LOG(INFO) << FormatString("DeviceObj[%s, Sz: %zu] is being publishing by client[%s]", objectKey, req.data_size(),
                              clientId);
    (void)resp;
    return workerDevOcManager_->PublishDeviceObject(namespaceUri, req, payloads);
}

Status WorkerOCServiceImpl::GetDeviceObject(
    std::shared_ptr<ServerUnaryWriterReader<GetDeviceObjectRspPb, GetDeviceObjectReqPb>> serverApi)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    GetDeviceObjectReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.device_object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");

    auto clientId = ClientKey::Intern(req.client_id());
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.device_object_keys());
    LOG(INFO) << "GetDeviceObject start from client:" << clientId << ", objects: " << VectorToString(objectKeys);
    int64_t subTimeout = req.sub_timeout();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsInNonNegativeInt32(subTimeout), K_RUNTIME_ERROR,
                                         "SubTimeout is out of range.");

    Timer timer;
    std::string traceID = Trace::Instance().GetTraceID();
    threadPool_->Execute([objectKeys, serverApi, subTimeout, clientId, timer, this, traceID]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Processing GetDeviceObject, threads Statistics: " << threadPool_->GetStatistics();
        auto timeout = std::max<int64_t>(subTimeout, RPC_TIMEOUT);
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "RPC timeout. time elapsed " << elapsed << ", subTimeout:" << subTimeout
                       << ", get threads Statistics: " << threadPool_->GetStatistics();
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
        } else {
            reqTimeoutDuration.Init(timeout - elapsed);
            auto newSubTimeout = std::max<int64_t>(subTimeout - elapsed, 0);
            (void)newSubTimeout;
            workerDevOcManager_->ProcessGetDeviceObjectRequest(objectKeys, serverApi, subTimeout, clientId);
            LOG(INFO) << "Process GetDeviceObject done, threads Statistics: " << threadPool_->GetStatistics();
        }
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    PerfPoint point(PerfKey::WORKER_PUT_P2PMETA);
    std::string tenantId;
    CHECK_FAIL_RETURN_STATUS(req.dev_obj_meta_size() > 0 && req.dev_obj_meta(0).locations_size() > 0, K_INVALID,
                             "The device meta and locations cannot be empty.");
    const auto clientId = ClientKey::Intern(req.dev_obj_meta(0).locations().begin()->client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, PutP2PMeta failed");
    PutP2PMetaReqPb reqCopy = req;
    reqCopy.set_worker_address(localAddress_.ToString());

    // Time complexity O(n), because in putp2pmeta, each obj_meta has only one location
    for (auto &dev_obj_meta : *reqCopy.mutable_dev_obj_meta()) {
        for (auto &location : *dev_obj_meta.mutable_locations()) {
            location.set_worker_ip(localAddress_.Host());
        }
    }
    return workerMasterApi->PutP2PMeta(reqCopy, resp);
}

Status WorkerOCServiceImpl::SubscribeReceiveEvent(
    std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> serverApi)
{
    PerfPoint point(PerfKey::WORKER_SUBSCRIBE_EVENT);
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    SubscribeReceiveEventReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.src_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    Timer timer;
    std::string traceID = Trace::Instance().GetTraceID();
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    devThreadPool_->Execute([=]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Worker processes SubscribeReceiveEvent from srcClientId: " << req.src_client_id()
                  << ", src_device_id: " << req.src_device_id()
                  << ", threads Statistics: " << devThreadPool_->GetStatistics();
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "SubscribeReceiveEvent RPC timeout. time elapsed " << elapsed << ", subTimeout:" << timeout
                       << ", threads Statistics: " << devThreadPool_->GetStatistics();
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
        } else {
            reqTimeoutDuration.Init(timeout - elapsed);
            req.set_worker_ip(localAddress_.Host());
            LOG_IF_ERROR(workerDevOcManager_->ProcessSubscribeReceiveEventRequest(req, serverApi),
                         "Process SubscribeReceiveEvent failed");
            LOG(INFO) << "Process SubscribeReceiveEvent done";
        }
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::GetP2PMeta(
    std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi)
{
    PerfPoint point(PerfKey::WORKER_GET_P2PMEATA);
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    GetP2PMetaReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    CHECK_FAIL_RETURN_STATUS(req.dev_obj_meta_size() > 0 && req.dev_obj_meta(0).locations_size() > 0, K_INVALID,
                             "The device meta and locations cannot be empty.");
    const auto clientId = ClientKey::Intern(req.dev_obj_meta(0).locations().begin()->client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    Timer timer;
    std::string traceID = Trace::Instance().GetTraceID();
    devThreadPool_->Execute([=]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        std::stringstream allKeys;
        bool first = true;
        for (const auto &dev_obj_meta : *req.mutable_dev_obj_meta()) {
            if (!first) {
                allKeys << ", ";
            }
            allKeys << dev_obj_meta.object_key();
            first = false;
        }
        LOG(INFO) << FormatString("Worker processes GetP2PMeta from client: %s, allKeys: [%s], threads Statistics: %s",
                                  clientId, allKeys.str(), devThreadPool_->GetStatistics());
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "GetP2PMeta RPC timeout. time elapsed " << elapsed << ", subTimeout:" << timeout
                       << ", threads Statistics: " << devThreadPool_->GetStatistics();
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
        } else {
            reqTimeoutDuration.Init(timeout - elapsed);
            for (auto &dev_obj_meta : *req.mutable_dev_obj_meta()) {
                for (auto &location : *dev_obj_meta.mutable_locations()) {
                    location.set_worker_ip(localAddress_.Host());
                }
            }
            req.set_worker_address(localAddress_.ToString());
            LOG_IF_ERROR(workerDevOcManager_->ProcessGetP2PMetaRequest(req, serverApi), "Process GetP2PMeta failed");
            LOG(INFO) << "Process GetP2PMeta done";
        }
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::SendRootInfo(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.dst_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, GetP2P meta failed");
    SendRootInfoReqPb reqCopy = req;
    return workerMasterApi->SendRootInfo(reqCopy, resp);
}

Status WorkerOCServiceImpl::RecvRootInfo(
    std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi)
{
    ReadLock noRecon;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noRecon, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    RecvRootInfoReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.src_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    Timer timer;
    std::string traceID = Trace::Instance().GetTraceID();
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    devThreadPool_->Execute([=]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Worker processes RecvRootInfo from srcClientId: " << req.src_device_id()
                  << ", src_device_id: " << req.src_device_id()
                  << ", threads Statistics: " << devThreadPool_->GetStatistics();
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "RecvRootInfo RPC timeout. time elapsed " << elapsed << ", subTimeout:" << timeout
                       << ", threads Statistics: " << devThreadPool_->GetStatistics();
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
        } else {
            reqTimeoutDuration.Init(timeout - elapsed);
            LOG_IF_ERROR(workerDevOcManager_->ProcessRecvRootInfoRequest(req, serverApi),
                         "Process RecvRootInfo failed");
            LOG(INFO) << "Process RecvRootInfo done";
        }
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.dst_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, GetP2P meta failed");
    AckRecvFinishReqPb reqCopy = req;
    return workerMasterApi->AckRecvFinish(reqCopy, resp);
}

Status WorkerOCServiceImpl::RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, GetP2P meta failed");
    RemoveP2PLocationReqPb reqCopy = req;
    return workerMasterApi->RemoveP2PLocation(reqCopy, resp);
}

Status WorkerOCServiceImpl::GetDataInfo(
    std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> serverApi)
{
    ReadLock noReconciliation;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noReconciliation, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    GetDataInfoReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    auto objectKey = req.object_key();
    int64_t subTimeout = req.sub_timeout();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsInNonNegativeInt32(subTimeout), K_RUNTIME_ERROR,
                                         "SubTimeout is out of range.");
    Timer timer;
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    std::string traceID = Trace::Instance().GetTraceID();
    devThreadPool_->Execute([=]() mutable {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Worker processes GetDataInfo from object: " << objectKey
                  << ", threads Statistics: " << devThreadPool_->GetStatistics();
        int64_t elapsed = timer.ElapsedMilliSecond();
        if (elapsed >= timeout) {
            LOG(ERROR) << "GetDataInfo RPC timeout. time elapsed " << elapsed << ", subTimeout:" << timeout;
            LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
        } else {
            auto realRpcTimeout = std::max<int64_t>(timeout - elapsed, subTimeout);
            reqTimeoutDuration.Init(realRpcTimeout);
            LOG_IF_ERROR(workerDevOcManager_->ProcessGetDataInfoRequest(req, serverApi, subTimeout),
                         "Process GetDataInfo failed");
            LOG(INFO) << "Process GetDataInfo done";
        }
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::WaitInit()
{
    return initOkFuture_.get();
}

Status WorkerOCServiceImpl::GetObjMetaInfo(const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp)
{
    ReadLock noReconciliation;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noReconciliation, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return getProc_->GetObjMetaInfo(req, resp);
}

Status WorkerOCServiceImpl::QuerySize(const QuerySizeReqPb &req, QuerySizeRspPb &rsp)
{
    ReadLock noReconciliation;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noReconciliation, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return getProc_->QuerySize(req, rsp);
}

Status WorkerOCServiceImpl::Exist(const ExistReqPb &req, ExistRspPb &rsp)
{
    ReadLock noReconciliation;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noReconciliation, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    PerfPoint perfPoint(PerfKey::WORKER_EXIST);
    return getProc_->Exist(req, rsp);
}

Status WorkerOCServiceImpl::Expire(const ExpireReqPb &req, ExpireRspPb &rsp)
{
    ReadLock noReconciliation;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noReconciliation, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return expireProc_->Expire(req, rsp);
}

Status WorkerOCServiceImpl::GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    ReadLock noReconciliation;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ValidateWorkerState(noReconciliation, reqTimeoutDuration.CalcRemainingTime()),
                                     "validate worker state failed");
    return getProc_->GetMetaInfo(req, rsp);
};

Status WorkerOCServiceImpl::DeleteDevObjects(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp)
{
    LOG(INFO) << "Worker delete device objects: " << VectorToString(req.object_keys());
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, GetP2P meta failed");
    DeleteAllCopyMetaReqPb reqCopy;
    DeleteAllCopyMetaRspPb respFromMaster;
    reqCopy.set_address(req.client_id());
    reqCopy.set_are_device_objects(true);
    *reqCopy.mutable_object_keys() = { req.object_keys().begin(), req.object_keys().end() };
    PerfPoint point(PerfKey::WORKER_DELETE_OBJECT_TO_MASTER);
    auto rc = workerMasterApi->DeleteAllCopyMeta(reqCopy, respFromMaster);
    *resp.mutable_fail_object_keys() = { respFromMaster.failed_object_keys().begin(),
                                         respFromMaster.failed_object_keys().end() };
    resp.mutable_last_rc()->set_error_code(respFromMaster.last_rc().error_code());
    resp.mutable_last_rc()->set_error_msg(respFromMaster.last_rc().error_msg());
    return rc;
}
}  // namespace object_cache
}  // namespace datasystem
