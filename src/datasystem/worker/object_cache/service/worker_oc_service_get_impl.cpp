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
 * Description: Defines the worker service Get process.
 */
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"

#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <tuple>
#include <utility>

#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/l2_storage.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/common/rdma/npu/remote_h2d_manager.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/master/meta_addr_info.h"
#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

#include "datasystem/common/os_transport_pipeline/os_transport_pipeline_worker_api.h"
#include "datasystem/common/shared_memory/allocator.h"

DS_DECLARE_string(other_cluster_names);
DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(cross_cluster_get_data_from_worker);
DS_DECLARE_bool(cross_cluster_get_meta_from_worker);
DS_DECLARE_bool(oc_io_from_l2cache_need_metadata);
DS_DECLARE_bool(authorization_enable);
DS_DECLARE_bool(enable_data_replication);
DS_DEFINE_bool(enable_l2_cache_fallback, true, "Control whether enable fallback to L2 cache when worker failed.");
using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

static constexpr int DEBUG_LOG_LEVEL = 2;
static constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;

WorkerOcServiceGetImpl::WorkerOcServiceGetImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                                               EtcdStore *etcdStore, std::shared_ptr<ThreadPool> memCpyThreadPool,
                                               std::shared_ptr<ThreadPool> threadPool,
                                               std::shared_ptr<AkSkManager> akSkManager, HostPort localAddress)
    : WorkerOcServiceCrudCommonApi(initParam),
      etcdCM_(etcdCM),
      etcdStore_(etcdStore),
      memCpyThreadPool_(std::move(memCpyThreadPool)),
      threadPool_(std::move(threadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(std::move(localAddress))
{
    remoteGetThreadPool_ = std::make_unique<ThreadPool>(1, FLAGS_rpc_thread_num, "RemoteGetThreadPool");
    if (HaveOtherAZ()) {
        for (const auto &azName : Split(FLAGS_other_cluster_names, ",")) {
            if (azName != FLAGS_cluster_name) {
                otherAZNames_.emplace_back(azName);
            }
        }
    }
    workerBatchQueryMetaThreadPool_ = std::make_unique<ThreadPool>(1, FLAGS_rpc_thread_num, "BatchQureyMeta");
    if (FLAGS_enable_worker_worker_batch_get) {
        workerBatchRemoteGetThreadPool_ = std::make_unique<ThreadPool>(1, FLAGS_rpc_thread_num, "BatchRemoteGet");
    }
}

Status WorkerOcServiceGetImpl::Get(std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi)
{
    PerfPoint point(PerfKey::WORKER_GET_OBJECT);
    workerOperationTimeCost.Clear();
    Timer timer;
    auto request = std::make_shared<GetRequest>(AccessRecorderKey::DS_POSIX_GET);
    INJECT_POINT("WorkerOCServiceImpl.Get.Retry",
                 [&serverApi]() { return serverApi->SendStatus(Status(K_TRY_AGAIN, "test get retry")); });
    GetReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    auto clientId = ClientKey::Intern(req.client_id());
    LOG(INFO) << "Get start from client:" << clientId << " server api read elapsed ms: " << timer.ElapsedMilliSecond();
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");

    int64_t subTimeout = req.sub_timeout();
    int64_t timeout = reqTimeoutDuration.CalcRealRemainingTime();
    INJECT_POINT("WorkerOCServiceImpl.Get.Timeout", [&timeout](int changedTimeout) {
        timeout = changedTimeout;
        return Status::OK();
    });
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsInNonNegativeInt32(subTimeout), K_RUNTIME_ERROR,
                                         "SubTimeout is out of range.");

    PerfPoint p(PerfKey::WORKER_GET_REQUEST_INIT);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(request->Init(tenantId, req, memoryRefTable_, serverApi, threadPool_, clientId),
                                     "GetRequest Init failed");
    p.Record();

    timer.Reset();
    if (serverApi->EnableMsgQ()) {
        std::string traceID = Trace::Instance().GetTraceID();
        auto cost = workerOperationTimeCost;
        threadPool_->Execute([=]() mutable {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            workerOperationTimeCost = cost;
            auto elapsed = static_cast<int64_t>(timer.ElapsedMilliSecond());
            LOG(INFO) << "Process Get from client: " << clientId
                      << ", objects: " << VectorToString(request->GetRawObjectKeys())
                      << ", get threads Statistics: " << threadPool_->GetStatistics() << ", elapsed ms: " << elapsed
                      << ", remainingTime: " << timeout;
            if (elapsed >= timeout) {
                LOG(ERROR) << "RPC timeout. time elapsed " << elapsed << ", subTimeout:" << subTimeout
                           << ", get threads Statistics: " << threadPool_->GetStatistics();
                LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
            } else {
                reqTimeoutDuration.Init(timeout - elapsed);
                auto newSubTimeout = std::max<int64_t>(subTimeout - elapsed, 0);
                LOG_IF_ERROR(ProcessGetObjectRequest(newSubTimeout, request), "Process Get failed");
                workerOperationTimeCost.Append("ProcessGetObjectRequest",
                                               static_cast<int64_t>(timer.ElapsedMilliSecond()));
                LOG(INFO) << FormatString(
                    "Process Get done, clientId: %s, objectKeys: %s, subTimeout: %ld, get threads Statistics: %s."
                    "The operations of worker Get %s",
                    clientId, VectorToString(request->GetRawObjectKeys()), newSubTimeout, threadPool_->GetStatistics(),
                    workerOperationTimeCost.GetInfo());
            }
        });
    } else {
        LOG_IF_ERROR(ProcessGetObjectRequest(subTimeout, request), "Process Get failed");
        workerOperationTimeCost.Append("ProcessGetObjectRequest", timer.ElapsedMilliSecond());
        LOG(INFO) << FormatString("Process Get done, clientId: %s, objectKeys: %s. The operations of worker Get %s",
                                  clientId, VectorToString(request->GetRawObjectKeys()),
                                  workerOperationTimeCost.GetInfo());
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetObjectFromAnywhere(const ReadKey &readKey, const master::QueryMetaInfoPb &queryMeta,
                                                     std::vector<RpcMessage> &payloads)
{
    const auto &meta = queryMeta.meta();
    const auto &address = queryMeta.address();
    const auto &objectKey = meta.object_key();
    VLOG(1) << "Get object from remote, object meta is: " << LogHelper::IgnoreSensitive(meta) << ", addr: " << address;
    INJECT_POINT("worker.GetObjectFromRemote.AfterAttach");

    // Check whether object is already created by other client.
    std::shared_ptr<SafeObjType> entry;
    bool isInsert;
    RETURN_IF_NOT_OK(objectTable_->ReserveGetAndLock(objectKey, entry, isInsert));
    INJECT_POINT("worker.GetObjectFromAnywhere");
    Raii unlock([&entry]() { entry->WUnlock(); });

    if ((entry->Get() != nullptr) && entry->Get()->IsBinary() && !entry->Get()->stateInfo.IsCacheInvalid()) {
        // If a local publish or remote get finished between QueryMeta and ReserveGetAndLock,
        // we will get a valid object here.
        ReadObjectKV objectKV(readKey, *entry);
        if ((*entry)->GetShmUnit() == nullptr && (*entry)->IsSpilled()) {
            LOG(INFO) << FormatString("[ObjectKey %s] Exist on disk", objectKey);
            RETURN_IF_NOT_OK(LoadSpilledObjectToMemory(objectKV, evictionManager_));
        } else {
            evictionManager_->Add(objectKey);
        }
        return UpdateRequestForSuccess(objectKV, nullptr);
    }

    SetObjectEntryAccordingToMeta(meta, GetMetadataSize(), *entry);
    ReadKey readKeyAfterSet(readKey.objectKey, readKey.readOffset, readKey.readSize);
    ReadObjectKV objectKV(readKeyAfterSet, *entry);
    Status status = queryMeta.payload_indexs_size() == 0
                        ? GetObjectFromRemoteOnLock(meta, nullptr, address, queryMeta.single_copy(), objectKV)
                        : GetObjectFromQueryMetaResultOnLock(nullptr, queryMeta, payloads, objectKV);
    if (status.IsError()) {
        (void)RemoveLocation(objectKey, meta.version());
        if (entry->Get() != nullptr && entry->Get()->GetShmUnit() != nullptr) {
            entry->Get()->GetShmUnit()->SetHardFreeMemory();
        }
        if (isInsert) {
            (void)objectTable_->Erase(objectKey, *entry);
        } else if (entry->Get() != nullptr) {
            LOG_IF_ERROR(entry->Get()->FreeResources(), "free resources failed");
            entry->Get()->SetLifeState(ObjectLifeState::OBJECT_INVALID);
            entry->Get()->stateInfo.SetCacheInvalid(true);
        }
    }
    return status;
}

Status WorkerOcServiceGetImpl::GetDataFromL2CacheForPrimaryCopy(const std::string &objectKey, uint64_t version,
                                                                std::shared_ptr<SafeObjType> &safeEntry)
{
    std::vector<std::string> objectKeys{ objectKey };
    QueryMetadataFromMasterResult result;
    std::vector<master::QueryMetaInfoPb> &queryMetas = result.queryMetas;
    LOG_IF_ERROR(QueryMetadataFromMaster(objectKeys, 0, result), "");
    if (!queryMetas.empty()) {
        const auto &queryMeta = queryMetas.front();
        const auto &meta = queryMeta.meta();
        const auto &address = queryMeta.address();
        ObjectKV objectKV(objectKey, *safeEntry);
        SetObjectEntryAccordingToMeta(meta, GetMetadataSize(), *safeEntry);
        if (meta.primary_address() == localAddress_.ToString()) {
            VLOG(1) << FormatString("[ObjectKey %s] primary copy get data from L2 cache, meta is:%s", objectKey,
                                    LogHelper::IgnoreSensitive(meta));
            Status rc;
            Timer endToEndTimer;
            TryGetFromL2CacheWhenNotFoundInWorker(meta, address, true, objectKV, rc);
            LOG(INFO) << FormatString("object(%s) get from l2 finish, size:%zu, use %f millisecond.", objectKey,
                                      (*safeEntry)->GetDataSize(), endToEndTimer.ElapsedMilliSecond());
            if (rc.IsError() || (*safeEntry)->GetShmUnit() == nullptr) {
                SetEmptyObjectEntry(objectKey, *safeEntry);
                (void)RemoveLocation(objectKey, version);
                return Status(StatusCode::K_NOT_FOUND, "Object not found");
            }
            return rc;
        }
        SetEmptyObjectEntry(objectKey, *safeEntry);
    }
    (void)RemoveLocation(objectKey, version);
    return Status(StatusCode::K_NOT_FOUND, "Object not found");
}

Status WorkerOcServiceGetImpl::ProcessGetObjectRequest(int64_t subTimeout, std::shared_ptr<GetRequest> &request)
{
    PerfPoint all(PerfKey::WORKER_PROCESS_GET_OBJECT);
    INJECT_POINT("worker.Get.asyncGetStart", [](int timeout) {
        reqTimeoutDuration.Init(timeout);
        return Status::OK();
    });
    INJECT_POINT("worker.Get.delay");
    PerfPoint point(PerfKey::WORKER_PROCESS_GET_FROM_LOCAL);
    // Try get from local.
    std::set<ReadKey> remoteObjectKeys;
    RETURN_IF_NOT_OK(TryGetObjectFromLocal(request, remoteObjectKeys));
    RETURN_OK_IF_TRUE(request->AlreadyReturn());

    // Register request for subscribe
    if (subTimeout > 0) {
        point.RecordAndReset(PerfKey::WORKER_PROCESS_GET_FOR_REGISTER);
        request->Register(&workerRequestManager_);
    }

    point.RecordAndReset(PerfKey::WORKER_PROCESS_GET_FROM_REMOTE);
    // Try get from remote worker or L2 cache.
    RETURN_IF_NOT_OK(TryGetObjectFromRemote(subTimeout, request, std::move(remoteObjectKeys)));
    RETURN_OK_IF_TRUE(request->AlreadyReturn());
    int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
    if (request->GetNotReadyCount() == 0 || subTimeout == 0 || remainingTimeMs <= 0) {
        point.RecordAndReset(PerfKey::WORKER_PROCESS_GET_RETURN);
        return request->ReturnToClient();
    }

    point.RecordAndReset(PerfKey::WORKER_PROCESS_GET_ADD_TIMER);
    auto timer = std::make_unique<TimerQueue::TimerImpl>();
    auto traceID = Trace::Instance().GetTraceID();
    auto weakThis = weak_from_this();
    // For exclusive connections: inform parent that an async child is deployed
    request->GetServerApi()->SetRequestInProgress();
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        std::min<int64_t>(subTimeout, remainingTimeMs),
        [weakThis, subTimeout, request, traceID, remainingTimeMs]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
            LOG(ERROR) << "The get request times out, the sub timeout: " << subTimeout
                       << ", remainingTimeMs: " << remainingTimeMs << ", clientId: " << request->GetClientId()
                       << ", satisfied num: " << request->GetReadyCount()
                       << ", waiting num: " << request->GetNotReadyCount();
            auto impl = weakThis.lock();
            if (impl == nullptr) {
                return;
            }
            LOG_IF_ERROR(request->ReturnToClient(), "ReturnToClient failed");
        },
        *timer));

    request->SetTimer(std::move(timer));
    point.Record();
    return Status::OK();
}

static Status CheckAndResetStatus(const Status &status, std::set<StatusCode> &bypassCode)
{
    // If the error is RPC error, return them directly, other error would be covered up as RUNTIME_ERROR.
    return (bypassCode.count(status.GetCode()) > 0 || IsRpcTimeoutOrTryAgain(status))
               ? status
               : Status(K_RUNTIME_ERROR, status.GetMsg());
}

Status WorkerOcServiceGetImpl::TryGetObjectFromLocal(std::shared_ptr<GetRequest> &request,
                                                     std::set<ReadKey> &remoteObjectKeys)
{
    Status lastRc;
    auto &uniqueObjectMap = request->GetObjects();
    PerfPoint point(PerfKey::WORKER_PROCESS_GET_FROM_LOCAL_CHECK_ROLLBACK);
    asyncRollbackManager_->UpdateIsRollback(uniqueObjectMap);
    point.RecordAndReset(PerfKey::WORKER_PROCESS_GET_FROM_LOCAL_BATCH);
    std::vector<std::string> needEvictKeys;
    auto func = [this, &request](const std::string &objectKey, GetObjInfo &objectInfo,
                                 std::set<ReadKey> &remoteObjectKeys, std::vector<std::string> &needEvictKeys) {
        Status status;
        if (objectInfo.isRollBack) {
            objectInfo.rc = Status(K_NOT_FOUND, FormatString("ObjectKey %s in rollback", objectKey));
        } else {
            ReadKey readKey(objectKey, objectInfo.offsetInfo);
            status = PreProcessGetObject(readKey, objectInfo, remoteObjectKeys);
            if (status.IsError()) {
                objectInfo.rc = status;
                LOG(ERROR) << "PreProcessGetObject failed:" << status.GetMsg();
            }
            if (objectInfo.params != nullptr) {
                needEvictKeys.emplace_back(objectKey);
            }
            // submit local h2d io
            if (remoteObjectKeys.find(readKey) == remoteObjectKeys.end()) {
                RETURN_IF_NOT_OK(OsXprtPipln::TriggerLocalPipelineRH2D(
                    request->GetH2DChunkManager(), objectKey, objectInfo.params->shmUnit->GetPointer(),
                    objectInfo.params->metaSize, objectInfo.params->dataSize));
            }
        }
        return status;
    };
    const size_t parallelLimit = 128;
    const size_t objectKeyCount = uniqueObjectMap.size();
    if (objectKeyCount <= parallelLimit) {
        for (auto &[objectKey, objectInfo] : uniqueObjectMap) {
            auto rc = func(objectKey, objectInfo, remoteObjectKeys, needEvictKeys);
            lastRc = rc.IsError() ? rc : lastRc;
        }
    } else {
        const int parallism = 4;
        std::vector<std::pair<const std::string *, GetObjInfo *>> parallelTaskList;
        parallelTaskList.reserve(objectKeyCount);
        for (auto &[objectKey, objectInfo] : uniqueObjectMap) {
            parallelTaskList.emplace_back(std::make_pair(&objectKey, &objectInfo));
        }
        // protect remoteObjectKeys and lastRc
        std::mutex mutex;
        auto batchHandler = [&parallelTaskList, &func, &remoteObjectKeys, &mutex, &lastRc, &needEvictKeys](size_t start,
                                                                                                           size_t end) {
            Status status;
            std::set<ReadKey> remoteKeys;
            std::vector<std::string> evictKeys;
            for (size_t i = start; i < end; i++) {
                const auto &objectKey = *parallelTaskList[i].first;
                auto &objectInfo = *parallelTaskList[i].second;
                auto rc = func(objectKey, objectInfo, remoteKeys, evictKeys);
                status = rc.IsError() ? rc : status;
            }
            {
                std::lock_guard<std::mutex> locker(mutex);
                remoteObjectKeys.insert(remoteKeys.begin(), remoteKeys.end());
                needEvictKeys.insert(needEvictKeys.end(), evictKeys.begin(), evictKeys.end());
                lastRc = status.IsError() ? status : lastRc;
            }
        };
        LOG_IF_ERROR(Parallel::ParallelFor<size_t>(0, objectKeyCount, batchHandler, 0, parallism),
                     "ParallelFor local get failed");
    }
    if (lastRc.IsError()) {
        static std::set<StatusCode> bypassCode{ K_OUT_OF_MEMORY, K_OUT_OF_RANGE };
        lastRc = CheckAndResetStatus(lastRc, bypassCode);
    }
    SubmitAsyncAddEvictTask(std::move(needEvictKeys));
    point.RecordAndReset(PerfKey::WORKER_PROCESS_GET_FROM_LOCAL_UPDATE_REQ);
    return request->UpdateAfterLocalGet(std::move(lastRc), remoteObjectKeys.size());
}

void WorkerOcServiceGetImpl::SubmitAsyncAddEvictTask(std::vector<std::string> objectIds)
{
    if (objectIds.empty()) {
        return;
    }
    PerfPoint point(PerfKey::ASYNC_EVICT_TASK_ADD);
    static const size_t isAsyncThreshold = 64;
    if (objectIds.size() > isAsyncThreshold) {
        threadPool_->Execute([ids = std::move(objectIds), this]() {
            for (const auto &id : ids) {
                evictionManager_->Add(id);
            }
        });
    } else {
        for (const auto &id : objectIds) {
            evictionManager_->Add(id);
        }
    }
}

Status WorkerOcServiceGetImpl::TryGetObjectFromRemote(int64_t subTimeout, std::shared_ptr<GetRequest> &request,
                                                      std::set<ReadKey> remoteObjectKeys)
{
    RETURN_OK_IF_TRUE(remoteObjectKeys.empty());
    auto needRemoteGetIds = std::move(remoteObjectKeys);
    PerfPoint pointRemote(PerfKey::WORKER_PROCESS_GET_OBJECT_REMOTE);
    std::unordered_set<std::string> failedIds;
    Status status;

    do {
        std::set<ReadKey> needRetryIds;
        std::unordered_set<std::string> tmpFailedIds;
        status = ProcessObjectsNotExistInLocal(needRemoteGetIds, subTimeout, tmpFailedIds, needRetryIds, request);
        std::for_each(tmpFailedIds.begin(), tmpFailedIds.end(), [&](const std::string &id) {
            if (needRetryIds.count(ReadKey(id)) == 0) {
                (void)failedIds.emplace(id);
            }
        });
        int64_t remainTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
        const int64_t timeoutThresholdMs = 100;
        INJECT_POINT_NO_RETURN("TryGetObjectFromRemote.NoRetry", [&remainTimeMs] { remainTimeMs = 0; });
        // If we meets OOM, never try get again because there is no space for us to save the objects.
        if (status.GetCode() == K_OUT_OF_MEMORY || remainTimeMs <= timeoutThresholdMs) {
            std::for_each(needRetryIds.begin(), needRetryIds.end(),
                          [&](const ReadKey &key) { failedIds.emplace(key.objectKey); });
            break;
        }
        needRemoteGetIds.swap(needRetryIds);
        subTimeout = remainTimeMs >= subTimeout ? subTimeout : remainTimeMs > 0 ? subTimeout - remainTimeMs : 0;
    } while (!needRemoteGetIds.empty());
    if (!failedIds.empty()) {
        std::unordered_map<std::string, uint64_t> failedKeyVersions;
        failedKeyVersions.reserve(failedIds.size());
        // it's in sync flow, so we can use current time as the delete version.
        uint64_t version = static_cast<uint64_t>(GetSystemClockTimeStampUs());
        for (const auto &id : failedIds) {
            failedKeyVersions.emplace(id, version);
        }
        DeleteObjectsMetaUnacked(failedKeyVersions);
    }

    pointRemote.Record();
    if (status.GetCode() == K_OUT_OF_MEMORY) {
        LOG(INFO) << "TryGetObjectFromRemote failed, detail: " << status.ToString();
        return request->ReturnToClient(status);
    }

    Status lastRc;
    if (status.IsError()) {
        // K_NOT_FOUND_IN_L2CACHE: Metadata exists in etcd but data not exists in .
        // K_OUT_OF_RANGE: offset > szie.
        static std::set<StatusCode> bypassCodeRemoteGet{ K_OUT_OF_RANGE };
        if (status.GetCode() == K_NOT_FOUND_IN_L2CACHE) {
            LOG(ERROR) << "ProcessObjectsNotExistInLocal failed with status: " << status.ToString();
            auto msg = "Cannot get object from worker and l2 cache";
            lastRc = Status(K_NOT_FOUND, msg);
        } else {
            lastRc = CheckAndResetStatus(status, bypassCodeRemoteGet);
        }
        for (const auto &id : failedIds) {
            LOG_IF_ERROR(request->MarkFailed(id, lastRc), "MarkFailed failed");
        }
    }
    return Status::OK();
}

void WorkerOcServiceGetImpl::DeleteObjectsMetaUnacked(
    const std::unordered_map<std::string, uint64_t> &deleteKeyVersions)
{
    if (deleteKeyVersions.empty()) {
        return;
    }
    LOG(INFO) << "delete unacked object meta, size: " << deleteKeyVersions.size();
    const int32_t maxRetry = 3;
    int32_t retry = 0;
    std::vector<std::string> objKeys;
    std::vector<std::string> failedIds;
    std::vector<std::string> needMigrateIds;
    std::vector<std::string> needWaitIds;
    std::vector<std::string> noUseNeedL2DataIds;
    objKeys.reserve(deleteKeyVersions.size());
    for (const auto &kv : deleteKeyVersions) {
        objKeys.emplace_back(kv.first);
    }
    while (!objKeys.empty() && retry < maxRetry) {
        if (objKeys.size() == 1) {
            auto status = RemoveLocation(objKeys.front(), deleteKeyVersions.at(objKeys.front()));
            if (status.IsOk()) {
                objKeys.clear();
                break;
            }
            retry++;
            continue;
        }
        GroupAndRemoveMeta(objKeys, RemoveMetaReqPb_Cause_EVICTION, localAddress_.ToString(), deleteKeyVersions,
                           failedIds, needMigrateIds, needWaitIds, noUseNeedL2DataIds);
        objKeys.clear();
        if (!failedIds.empty() || !needMigrateIds.empty() || !needWaitIds.empty()) {
            objKeys.insert(objKeys.end(), std::make_move_iterator(failedIds.begin()),
                           std::make_move_iterator(failedIds.end()));
            objKeys.insert(objKeys.end(), std::make_move_iterator(needMigrateIds.begin()),
                           std::make_move_iterator(needMigrateIds.end()));
            objKeys.insert(objKeys.end(), std::make_move_iterator(needWaitIds.begin()),
                           std::make_move_iterator(needWaitIds.end()));
            failedIds.clear();
            needMigrateIds.clear();
            needWaitIds.clear();
        }
        retry++;
    }
    if (!objKeys.empty()) {
        LOG(WARNING) << "When delete unacked locations, failed to remove meta, objKeys are " << VectorToString(objKeys);
    }
}

Status WorkerOcServiceGetImpl::PreProcessGetObject(const ReadKey &readKey, GetObjInfo &info,
                                                   std::set<ReadKey> &remoteObjectKeys)
{
    INJECT_POINT("worker.PreProcessGetObject.begin");
    // use RLock instead of WLock try get from memory.
    bool objIsValidInMem = true;
    auto preSize = remoteObjectKeys.size();
    Status memGetRes = RLockGetObjectFromMem(readKey, info, remoteObjectKeys, objIsValidInMem);
    INJECT_POINT("set.objectIsInvalidInmem", [&objIsValidInMem]() {
        objIsValidInMem = false;
        return Status::OK();
    });
    if (objIsValidInMem) {
        // if not add readkey to remoteObjectKeys, mem hit
        if (remoteObjectKeys.size() == preSize) {
            CacheHitInfo::Instance().IncMemHit(1);
        }
        return memGetRes;
    }
    std::shared_ptr<SafeObjType> entry;
    // Fetch the object and lock it.
    // If the object is not found, add it to the GetRemote list and return.
    Status rc = objectTable_->Get(readKey.objectKey, entry);
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    if (rc.GetCode() == K_NOT_FOUND) {
        (void)remoteObjectKeys.emplace(readKey);
        return Status::OK();
    }
    ReadObjectKV objectKV(readKey, *entry);
    // If entry WLock is not found, it means the object is deleting locally.
    // Try to get object from remote.
    INJECT_POINT("local.get.sleep");
    rc = entry->WLock(true);
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    if (rc.GetCode() == K_NOT_FOUND) {
        (void)remoteObjectKeys.emplace(readKey);
        return Status::OK();
    }
    Raii unlock([&entry]() { entry->WUnlock(); });
    if ((*entry).Get() == nullptr) {
        (void)remoteObjectKeys.emplace(readKey);
        return Status::OK();
    }
    INJECT_POINT("set.objectIsInComplete", [&entry]() {
        entry->Get()->stateInfo.SetIncompleted(true);
        return Status::OK();
    });
    CHECK_FAIL_RETURN_STATUS((*entry)->IsBinary(), K_INVALID, "Not a Shm Unit");
    if ((*entry)->IsSealed() || (*entry)->IsPublished()) {
        if (!(*entry)->stateInfo.IsCacheInvalid()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectKV.CheckReadOffset(), "Read offset verify failed");
            // case 1: object exist in local node, and it is the latest version.
            auto res = KeepObjectDataInMemory(objectKV);
            if (res.IsError()) {
                // object not found from disk or l2 cache, try get from remote.
                (void)remoteObjectKeys.emplace(readKey);
                return Status::OK();
            }
            LOG(INFO) << FormatString("[ObjectKey %s] already load to memory", readKey);
        } else {
            // case 2: object exist in local node, but it is the expired version.
            Status status = TryGetObjectsFromPrimaryWorker((*entry)->GetAddress(), (*entry)->GetDataSize(), objectKV,
                                                           remoteObjectKeys);
            if (status.IsError()) {
                return Status::OK();
            }
        }
        info.params = GetObjEntryParams::Create(readKey.objectKey, *entry);
    } else {
        // case 3: object didn't exist in local node, is not published or sealed yet.
        (void)remoteObjectKeys.emplace(readKey);
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::RLockGetObjectFromMem(const ReadKey &readKey, GetObjInfo &info,
                                                     std::set<ReadKey> &remoteObjectKeys, bool &objIsValidInMem)
{
    std::shared_ptr<SafeObjType> entry;
    // Fetch the object and lock it.
    // If the object is not found, add it to the GetRemote list and return.
    Status rc = objectTable_->Get(readKey.objectKey, entry);
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    if (rc.GetCode() == K_NOT_FOUND) {
        (void)remoteObjectKeys.emplace(readKey);
        return Status::OK();
    }
    // If entry RLock is not found, it means the object is deleting locally.
    // Try to get object from remote.
    rc = entry->RLock(true);
    RETURN_IF_NOT_OK_EXCEPT(rc, K_NOT_FOUND);
    if (rc.GetCode() == K_NOT_FOUND) {
        (void)remoteObjectKeys.emplace(readKey);
        return Status::OK();
    }
    Raii unlock([&entry]() { entry->RUnlock(); });
    if ((*entry).Get() == nullptr) {
        (void)remoteObjectKeys.emplace(readKey);
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS((*entry)->IsBinary(), K_INVALID, "Not a Shm Unit");
    if ((*entry)->IsSealed() || (*entry)->IsPublished()) {
        if (!(*entry)->stateInfo.IsCacheInvalid()) {
            ReadObjectKV objectKV(readKey, *entry);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectKV.CheckReadOffset(), "Read offset verify failed");
            // case 1: object exist in local node and complete, and it is the latest version.
            if (!(*entry)->IsShmUnitExistsAndComplete()) {
                objIsValidInMem = false;
                RETURN_STATUS(K_NOT_FOUND, FormatString("[ObjectKey %s] not exist in memory.", readKey));
            }
            info.params = GetObjEntryParams::Create(readKey.objectKey, *entry);
            return Status::OK();
        }
        // case 2: object exist in local node,and it is the expired version.
        objIsValidInMem = false;
        RETURN_STATUS(K_NOT_FOUND, FormatString("[ObjectKey %s] exists locally but expired.", readKey));
    } else {
        // object didn't exist in local node, is not published or sealed yet.
        (void)remoteObjectKeys.emplace(readKey);
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::ProcessObjectsNotExistInLocal(const std::set<ReadKey> &objectsNeedGetRemote,
                                                             int64_t subTimeout,
                                                             std::unordered_set<std::string> &failedIds,
                                                             std::set<ReadKey> &needRetryIds,
                                                             const std::shared_ptr<GetRequest> &request)
{
    PerfPoint all(PerfKey::WORKER_PROCESS_NOT_EXISTS_ALL);
    PerfPoint point(PerfKey::WORKER_PROCESS_NOT_EXISTS_ADD_IN_REMOTE_GET);
    VLOG(1) << "Begin to process " << objectsNeedGetRemote.size() << " objects that doesn't exist in local: ["
            << VectorToString(objectsNeedGetRemote) << "]";

    INJECT_POINT("worker.after_add_remote_get_objects");

    // Batch lock objects that need to query from master because location would be published to master in QueryMeta
    // request, to ensure the concurrent timing of the location (e.g. remove location when object was deleted by spill
    // manager), we need to lock here.
    point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_BATCH_LOCK);
    Status lastRc;
    std::map<ReadKey, LockedEntity> lockedEntries;
    lastRc = BatchLockForGet(objectsNeedGetRemote, lockedEntries, failedIds);
    Raii unlockRaii([this, &failedIds, &lockedEntries]() {
        PerfPoint point(PerfKey::WORKER_PROCESS_NOT_EXISTS_BATCH_UNLOCK);
        BatchUnlockForGet(failedIds, lockedEntries);
    });

    // If a local publish or remote get finished before we lock the object, we will get a valid object here.
    point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_ATTEMPTE_LOCAL_GET);
    AttemptGetObjectsLocally(request, lockedEntries);

    point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_COPY_IDS);
    std::vector<std::string> needRemoteGetObjects;
    std::transform(lockedEntries.begin(), lockedEntries.end(), std::back_inserter(needRemoteGetObjects),
                   [](const auto &kv) { return kv.first.objectKey; });

    point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_QUERY_META);
    QueryMetadataFromMasterResult queryMetaResult;
    std::vector<master::QueryMetaInfoPb> &queryMetas = queryMetaResult.queryMetas;
    std::vector<RpcMessage> &payloads = queryMetaResult.payloads;
    const auto &absentObjectKeys = queryMetaResult.absentObjectKeysWithVersion;
    Status result =
        QueryMetadataFromMaster(needRemoteGetObjects, subTimeout, queryMetaResult, !request->NoQueryL2Cache());
    if (result.IsError()) {
        lastRc = result;
        // If we query meta from master meets RPC error, do not add these objects to failedIds,
        // otherwise other concurrent get operations would failed, so we just notify ourselves.
        if (IsRpcTimeoutOrTryAgain(result)) {
            for (const auto &objectKey : needRemoteGetObjects) {
                LOG_IF_ERROR(request->MarkFailed(objectKey, result), "MarkFailed failed");
            }
        } else {
            failedIds.insert(needRemoteGetObjects.begin(), needRemoteGetObjects.end());
        }
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Query from master failed : %s", result.ToString()));
    }
    INJECT_POINT("worker.after_query_meta");

    if (FLAGS_oc_io_from_l2cache_need_metadata) {
        point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_NEEDMETA_UNLOCK);
        // Unlock the not found objects as soon as possible.
        if (queryMetas.empty()) {
            // In the scenario where keys that do not exist are obtained, these keys need to be deleted from
            // objectTable.
            failedIds.insert(needRemoteGetObjects.begin(), needRemoteGetObjects.end());
        }
        BatchUnlockForGet(absentObjectKeys, lockedEntries);
    }
    point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_FROM_ANY_WHERE);
    VLOG(1) << FormatString("Query meta success: target num %d, success num %d", objectsNeedGetRemote.size(),
                            queryMetas.size());
    auto getRet = GetObjectsFromAnywhere(queryMetas, request, payloads, lockedEntries, failedIds, needRetryIds);
    lastRc = getRet.IsError() ? getRet : lastRc;
    // If Get() is allowed to receive objects without meta, do it at last so that valid objects with meta can have
    // a fair change to complete within the given timeout.
    if (!FLAGS_oc_io_from_l2cache_need_metadata) {
        point.RecordAndReset(PerfKey::WORKER_PROCESS_NOT_EXISTS_GET_WITHOUT_META);
        Status rc = GetObjectsWithoutMeta(absentObjectKeys, lockedEntries, failedIds);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    point.Record();

    VLOG(1) << "Get object data from remote node finish, lastRc:" << lastRc.ToString();
    return lastRc;
}

Status WorkerOcServiceGetImpl::GetObjectsWithoutMeta(const std::map<std::string, uint64_t> &objectKeys,
                                                     std::map<ReadKey, LockedEntity> &lockedEntries,
                                                     std::unordered_set<std::string> &failedIds)
{
    Status lastRc = Status::OK();
    std::vector<std::string> successIds;
    for (const auto &kv : objectKeys) {
        const auto &objectKey = kv.first;
        auto it = lockedEntries.find(ReadKey(objectKey));
        if (it == lockedEntries.end()) {
            lastRc = Status(
                K_RUNTIME_ERROR,
                FormatString("could not find safe object entry in the locked list for objectKey: %s", objectKey));
            LOG(ERROR) << lastRc.GetMsg();
            continue;
        }
        // full read.
        ReadKey readKey(objectKey);
        ReadObjectKV objectKV(readKey, *it->second.safeObj);
        objectKV.GetObjEntry()->SetMetadataSize(metadataSize_);
        Status rc = GetObjectsWithoutMetaFromL2Cache(objectKV, kv.second);
        if (rc.IsOk()) {
            rc = UpdateRequestForSuccess(objectKV);
        }
        if (rc.IsError()) {
            LOG(ERROR) << rc.GetMsg();
            failedIds.emplace(objectKey);
            lastRc = rc;
        }
    }
    return lastRc;
}

Status WorkerOcServiceGetImpl::GetObjectsWithoutMetaFromL2Cache(ObjectKV &objectKV, uint64_t minVersion)
{
    if (IsSupportL2Storage(supportL2Storage_) && FLAGS_enable_l2_cache_fallback) {
        return GetObjectFromPersistenceAndDumpWithoutCopyMeta(objectKV, true, true, minVersion);
    }
    RETURN_STATUS(K_INVALID, FormatString("The L2 Storage type is invalid for objectKey %s", objectKV.GetObjKey()));
}

Status WorkerOcServiceGetImpl::TryGetObjectsFromPrimaryWorker(const std::string &primaryAddress, uint64_t dataSize,
                                                              ReadObjectKV &objectKV,
                                                              std::set<ReadKey> &objectsNeedGetRemote)
{
    const auto &objectKey = objectKV.GetObjKey();
    LOG(INFO) << FormatString("[ObjectKey %s] exist in local node but expired, remote worker: %s, local worker: %s",
                              objectKey, primaryAddress, localAddress_.ToString());
    Status status = Status(StatusCode::K_RUNTIME_ERROR, "Try to get object from primary worker failed");
    if (!primaryAddress.empty() && (primaryAddress != localAddress_.ToString())) {
        status = GetObjectFromRemoteWorkerAndDump(primaryAddress, "", dataSize, objectKV, false);
        if (status.IsError()) {
            LOG(INFO) << "Try to Pull from primary worker failed. The system will obtain from other worker or "
                         "l2 cache again. Detail: "
                      << status.ToString();
            objectsNeedGetRemote.emplace(objectKV.ConstructReadKey());
            return status;
        }
        CacheHitInfo::Instance().IncRemoteHit(1);
        RETURN_OK_IF_TRUE(status.IsOk());
    }
    objectsNeedGetRemote.emplace(objectKV.ConstructReadKey());
    return status;
}

Status WorkerOcServiceGetImpl::GetObjectFromRemoteWorkerWithoutDump(const std::string &address,
                                                                    const std::string &primaryAddress,
                                                                    uint64_t dataSize, ReadObjectKV &objectKV)
{
    // Pull object from remote worker.
    Status status = PullObjectDataFromRemoteWorker(address, dataSize, objectKV);
    // If we meets the error when pull data from other copy via address, we have another chance to pull data from
    // primary copy unless the primary copy address is the same as address (we have try it) or it is ourselves (it's
    // obvious we're empty, we can't get anything)
    if (status.IsError() && !primaryAddress.empty() && primaryAddress != address
        && primaryAddress != localAddress_.ToString()) {
        // Remote get may fail if provider can't acquire read latch too many times.
        LOG(INFO) << FormatString("[ObjectKey %s] Object may not exist in %s, try to get from primary copy %s, %s",
                                  objectKV.GetObjKey(), address, primaryAddress, status.ToString());
        status = PullObjectDataFromRemoteWorker(primaryAddress, dataSize, objectKV);
    }
    RETURN_IF_NOT_OK(status);
    evictionManager_->Add(objectKV.GetObjKey());
    objectKV.GetObjEntry()->stateInfo.SetNeedToDelete(true);
    return Status::OK();
}

Status WorkerOcServiceGetImpl::ProcessObjectEntryAndSyncMetadata(ReadObjectKV &objectKV, bool isAsyncBatchGet)
{
    auto &entry = objectKV.GetObjEntry();
    uint64_t version = entry->GetCreateTime();
    if (entry->stateInfo.IsIncomplete()) {
        return Status::OK();
    }
    entry->stateInfo.SetNeedToDelete(false);
    const auto &objectKey = objectKV.GetObjKey();
    if (isAsyncBatchGet) {
        VLOG(1) << FormatString("[ObjectKey %s] Will be updateLocation in batch later.", objectKey);
        return Status::OK();
    }
    HostPort masterHostAddress;
    // If we can't connect to the master (could be multiple reasons like cross_az_get_meta_from_worker=false or the
    // master is indeed faulty), then we can't update the location to the master so just set the delete flag and
    // return
    if (GetMetaAddress(objectKey, masterHostAddress).IsError()) {
        LOG(WARNING) << "Can't connect with master " << masterHostAddress.ToString()
                     << ". Data will be automatically deleted after it is returned.";
        entry->stateInfo.SetNeedToDelete(true);
        return Status::OK();
    }
    VLOG(1) << "Sync meta data to master as an object data copy provider.";
    UpdateLocationParam param = { objectKey, version, static_cast<uint32_t>(entry->stateInfo.GetDataFormat()) };
    UpdateLocationTask task = UpdateLocationTask(param);
    RETURN_IF_NOT_OK(asyncUpdateLocationManager_->AddTask(std::move(task)));
    INJECT_POINT("create_copy_meta");
    VLOG(1) << FormatString("[ObjectKey %s] Remote get object data success.", objectKey);
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetObjectFromRemoteWorkerAndDump(const std::string &address,
                                                                const std::string &primaryAddress, uint64_t dataSize,
                                                                ReadObjectKV &objectKV, bool isAsyncBatchGet)
{
    PerfPoint point(PerfKey::WORKER_PULL_REMOTE_DATA_FROM_WORKER);
    RETURN_IF_NOT_OK(GetObjectFromRemoteWorkerWithoutDump(address, primaryAddress, dataSize, objectKV));
    if (FLAGS_enable_data_replication) {
        RETURN_IF_NOT_OK(ProcessObjectEntryAndSyncMetadata(objectKV, isAsyncBatchGet));
    }
    return Status::OK();
}

template <typename Req>
Status WorkerOcServiceGetImpl::PrepareGetRequestHelper(const std::string &srcIpAddr, uint64_t dataSize,
                                                       ReadObjectKV &objectKV, Req &reqPb, bool &shmUnitAllocated,
                                                       std::shared_ptr<ShmOwner> shmOwner)
{
    // If fast transport is enabled, or if shmOwner is not nullptr, memory distribution/allocation needs to be
    // processed.
    if (!IsFastTransportEnabled() && shmOwner == nullptr) {
        return Status::OK();
    }
    reqPb.set_data_size(dataSize);
    INJECT_POINT("WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize", [&reqPb](uint64_t testDataSize) {
        reqPb.set_data_size(testDataSize);
        return Status::OK();
    });
    // Allocate the memory for the remote worker to urma_write.
    // Or early distribute memory for general code path.
    const auto &objectKey = objectKV.GetObjKey();
    auto &entry = objectKV.GetObjEntry();
    auto metaSz = entry->GetMetadataSize();
    auto shmUnit = entry->GetShmUnit();
    uint64_t cap = dataSize + metaSz;
    bool szChanged = (shmUnit == nullptr) || (shmUnit->size != cap);
    // Only create new shm if size changed or not exist.
    if (szChanged) {
        shmUnit = std::make_shared<ShmUnit>();
        bool populate = false;
        if (shmOwner) {
            RETURN_IF_NOT_OK(DistributeMemoryForObject(objectKey, dataSize, metaSz, populate, shmOwner, *shmUnit));
        } else {
            RETURN_IF_NOT_OK(
                AllocateMemoryForObject(objectKey, dataSize, metaSz, populate, evictionManager_, *shmUnit));
        }
        shmUnit->id = ShmKey::Intern(GetStringUuid());
        entry->SetShmUnit(shmUnit);
        shmUnitAllocated = true;
    }
    // Fill in urma info and ucp info
    if (IsUrmaEnabled()) {
        RETURN_IF_NOT_OK(
            FillRequestUrmaInfo(localAddress_, shmUnit->GetPointer(), shmUnit->GetOffset(), metaSz, reqPb,
                                shmUnit->GetNumaId()));
    } else if (IsUcpEnabled()) {
        RETURN_IF_NOT_OK(FillRequestUcpInfo(localAddress_, srcIpAddr, shmUnit, metaSz, reqPb));
    }

    return Status::OK();
}

Status WorkerOcServiceGetImpl::TryReconnectRemoteWorker(const std::string &endPoint, Status &lastResult)
{
    if (lastResult.IsOk() || lastResult.GetCode() != K_URMA_NEED_CONNECT) {
        return lastResult;
    }

    std::string remoteWorkerId = "UNKNOWN";
    if (etcdCM_ != nullptr) {
        auto workerId = etcdCM_->GetWorkerIdByWorkerAddr(endPoint);
        if (!workerId.empty()) {
            remoteWorkerId = workerId;
        }
    }
    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_NEED_CONNECT] TryReconnectRemoteWorker triggered, remoteAddress=" << endPoint
        << ", remoteWorkerId=" << remoteWorkerId << ", lastResult=" << lastResult.ToString();

    HostPort hostAddress;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hostAddress.ParseString(endPoint), "ParseString failed");

    TbbTransportStubTable::const_accessor constAccApi;
    while (!tarnsportApiTable_.find(constAccApi, endPoint)) {
        TbbTransportStubTable::accessor acc;
        if (tarnsportApiTable_.insert(acc, endPoint)) {
            std::shared_ptr<WorkerRemoteWorkerTransApi> transportApi =
                std::make_shared<WorkerRemoteWorkerTransApi>(hostAddress);
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(transportApi->Init(), "Create transport api faild.");
            acc->second = std::move(transportApi);
        }
    }

    UrmaHandshakeRspPb dummyRsp;
    RETURN_IF_NOT_OK(constAccApi->second->ExecOnceParrallelExchange(dummyRsp));
    RETURN_STATUS(K_TRY_AGAIN, "Reconnect success");
}

Status WorkerOcServiceGetImpl::ConstructBatchGetRequest(const std::string &address, std::list<GetObjectInfo> &infos,
                                                        const std::shared_ptr<GetRequest> &request,
                                                        std::vector<std::string> &successIds,
                                                        std::vector<ReadKey> &needRetryIds,
                                                        std::unordered_set<std::string> &failedIds,
                                                        BatchGetObjectRemoteReqPb &reqPb)
{
    PerfPoint point(PerfKey::WORKER_CONSTRUCT_BATCH_GET_REQ);
    // The function is placed together with PrepareGetRequestHelper,
    // as the template definition does not suit in header file.
    Status lastRc = Status::OK();
    // Pre-allocate an aggregated chunk of shared memory as ShmOwner, to reduce the number of allocation calls.
    std::vector<std::shared_ptr<ShmOwner>> shmOwners;
    std::vector<uint32_t> shmIndexMapping(infos.size(), std::numeric_limits<uint32_t>::max());
    // Skip early allocate if the request is both RH2D enabled and supported.
    if (!IsRemoteH2DEnabled() || request->GetClientCommUuid().empty()) {
        RETURN_IF_NOT_OK(AggregateAllocateHelper(infos, shmOwners, shmIndexMapping));
    } else {
        // Assume the client works with only one device id, so only one connection is needed.
        (*reqPb.mutable_comm_id()) = request->GetClientCommUuid();
    }

    std::string transportInstanceId;
    if (GetLocalTransportInstanceId(transportInstanceId).IsOk()) {
        (*reqPb.mutable_urma_instance_id()) = transportInstanceId;
    }
    bool requestReady = false;
    uint32_t objectIndex = 0;
    for (auto infoIter = infos.begin(); infoIter != infos.end(); objectIndex++) {
        const auto &meta = infoIter->queryMeta->meta();
        const auto &objectKey = meta.object_key();
        auto lockEntry = infoIter->entry;
        if (lockEntry == nullptr) {
            LOG(WARNING) << "ObjectKey " << objectKey << " not exsits in lockedEntries";
            continue;
        }
        // Checked availability when metas are grouped, so it should be safe to just access the entry here.
        auto &entry = lockEntry->safeObj;
        // Re-set object entry in the case of looped for data size change.
        SetObjectEntryAccordingToMeta(meta, GetMetadataSize(), *entry);
        const auto &readKey = infoIter->readKey;

        // Function to handle individual status
        auto handleIndividualStatus = [&](Status &status) {
            BatchGetObjectHandleIndividualStatus(status, *readKey, successIds, needRetryIds, failedIds);
            infoIter = infos.erase(infoIter);
            lastRc = status;
        };

        ReadObjectKV objectKV(*readKey, *entry);
        Status status = objectKV.CheckReadOffset();
        if (status.IsError()) {
            handleIndividualStatus(status);
            continue;
        }
        GetObjectRemoteReqPb subReq;
        subReq.set_object_key(objectKey);
        subReq.set_version((*entry)->GetCreateTime());
        subReq.set_read_offset(objectKV.GetReadOffset());
        subReq.set_read_size(objectKV.GetReadSize());
        subReq.set_data_size(meta.data_size());
        // Prepare the protobuf with urma/ucp info for data transfer if applicable.
        // BatchGetObjectHandleIndividualStatus will free ShmUnit upon error, so no need to actually record it here.
        bool shmUnitAllocated = false;
        std::shared_ptr<ShmOwner> shmOwner = nullptr;
        if (shmIndexMapping.size() > objectIndex && shmOwners.size() > shmIndexMapping[objectIndex]) {
            shmOwner = shmOwners[shmIndexMapping[objectIndex]];
        }
        status = PrepareGetRequestHelper(address, meta.data_size(), objectKV, subReq, shmUnitAllocated, shmOwner);
        if (status.IsError()) {
            handleIndividualStatus(status);
            continue;
        }

        // start pipeline receiver and prepare pipeline h2d tag in request
        OsXprtPipln::TriggerRemotePipelineRH2D(request->GetH2DChunkManager(), objectKV.GetObjKey(),
            objectKV.GetReadOffset() + objectKV.GetObjEntry()->GetMetadataSize(),
            objectKV.GetReadSize(), objectKV.GetObjEntry()->GetShmUnit(), address, subReq);

        reqPb.mutable_requests()->Add(std::move(subReq));
        if (objectKV.IsOffsetRead()) {
            objectKV.GetObjEntry()->stateInfo.SetIncompleted(true);
        }
        requestReady = true;
        infoIter++;
    }
    CHECK_FAIL_RETURN_STATUS(
        requestReady, lastRc.GetCode(),
        FormatString("Request not ready for the remote get request to addr: %s, due to %s", address, lastRc.GetMsg()));
    return lastRc;
}

Status WorkerOcServiceGetImpl::PullObjectDataFromRemoteWorker(const std::string &address, uint64_t dataSize,
                                                              ReadObjectKV &objectKV)
{
    CHECK_FAIL_RETURN_STATUS(address != localAddress_.ToString(), StatusCode::K_RUNTIME_ERROR,
                             "Remote getting from self address is invalid");
    auto version = objectKV.GetObjEntry()->GetCreateTime();
    const std::string requestId = GetStringUuid();
    LOG(INFO) << FormatString("Remote get request:[%s] --(%s)--> object:[%s], offset[%lld] size[%lld]", requestId,
                              address, objectKV.GetObjKey(), objectKV.GetReadOffset(), objectKV.GetReadSize());
    INJECT_POINT("worker.remote_get_failed");
    std::shared_ptr<WorkerRemoteWorkerOCApi> workerStub;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(address, akSkManager_, workerStub),
                                     "Create remote worker api failed.");
    PerfPoint rpcPoint(PerfKey::WORKER_PULL_FROM_REMOTE);
    GetObjectRemoteReqPb reqPb;
    GetObjectRemoteRspPb rspPb;
    reqPb.set_object_key(objectKV.GetObjKey());
    reqPb.set_request_id(requestId);
    reqPb.set_version(version);
    reqPb.set_read_offset(objectKV.GetReadOffset());
    reqPb.set_read_size(objectKV.GetReadSize());
    HostPort hostAddr;
    hostAddr.ParseString(address);
    if (objectKV.IsOffsetRead()) {
        objectKV.GetObjEntry()->stateInfo.SetIncompleted(true);
    }
    if (objectKV.commId_) {
        (*reqPb.mutable_comm_id()) = (*objectKV.commId_);
    }
    std::string transportInstanceId;
    if (GetLocalTransportInstanceId(transportInstanceId).IsOk()) {
        reqPb.set_urma_instance_id(transportInstanceId);
    }

    bool dataSizeChange;
    const int32_t minRetryOnceRpcMs = 1; // The 1st level of retryIntervalsMs
    std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> clientApi;
    do {
        dataSizeChange = false;
        bool shmUnitAllocated = false;
        // Prepare the protobuf with urma/ucp info for data transfer if applicable.
        RETURN_IF_NOT_OK(PrepareGetRequestHelper(address, dataSize, objectKV, reqPb, shmUnitAllocated));
        // If getting data from other AZ, then we leave 3/4 remain time to query from L2 cache in case getting data
        // failed.
        int64_t timeoutMs =
            reqTimeoutDuration.CalcRealRemainingTime() / (etcdCM_->CheckIfOtherAzNodeConnected(hostAddr) ? 4 : 1);
        INJECT_POINT("worker_oc_service_get_impl.pull_object_data_from_remote_worker.before_get_from_remote");
        Status rc = RetryOnErrorRepent(
            timeoutMs,
            [&workerStub, &reqPb, &rspPb, &clientApi, &address, this](int32_t) {
                RETURN_IF_NOT_OK(workerStub->GetObjectRemote(&clientApi));
                RETURN_IF_NOT_OK(workerStub->GetObjectRemoteWrite(clientApi, reqPb));

                auto rc = clientApi->Read(rspPb);
                RETURN_IF_NOT_OK(TryReconnectRemoteWorker(address, rc));
                return Status::OK();
            },
            []() { return Status::OK(); },
            { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
              StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_URMA_WAIT_TIMEOUT }, minRetryOnceRpcMs);
        // In case of changed size, error will be returned as part of response PB and urma wont be written any data
        if (rspPb.error().error_code() == K_OC_REMOTE_GET_NOT_ENOUGH) {
            // If this error happens, remote worker should also sent the changed data size
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rspPb.data_size() != 0, K_INVALID,
                                                 "object size should be greater than 0");
            // Update the data size for the next round.
            dataSize = static_cast<uint64_t>(rspPb.data_size());
            dataSizeChange = true;
        } else {
            if (IsFastTransportEnabled() && rc.IsError() && shmUnitAllocated) {
                // memory is allocated but request failed
                // deallocate the memory here
                objectKV.GetObjEntry()->SetShmUnit(nullptr);
            }
            RETURN_IF_NOT_OK(rc);
        }
    } while (dataSizeChange);
    // At this point, we haven't materialized the payload which is still sitting in the tcp/ip buffers.
    // We either receive payload directly into shared memory or fall back to the old behavior to save
    // the payload in ZMQ private memory
    if (rspPb.data_source() == DataTransferSource::DATA_IN_PAYLOAD) {
        PerfPoint retrieveRemotePayloadPoint(PerfKey::WORKER_RETRIEVE_REMOTE_PAYLOAD);
        RETURN_IF_NOT_OK(RetrieveRemotePayload(objectKV, clientApi, rspPb));
    }

    if (IsRemoteH2DEnabled() && (rspPb.data_source() == DataTransferSource::DATA_DELAY_TRANSFER)
        && rspPb.has_host_info()) {
        // Return the remote host segment info back to client, so client can import the segment.
        // Also the root info for communicator connection, and the offsets for get scattered.
        auto hostInfo = std::make_shared<RemoteH2DHostInfoPb>();
        *hostInfo = std::move(rspPb.host_info());
#ifdef BUILD_HETERO
        objectKV.GetObjEntry()->SetRemoteHostInfo(*objectKV.commId_, hostInfo);
#endif
    }

    VLOG(1) << FormatString("Get object from remote worker end:[%s] --(%s)--> object:[%s]", requestId, address,
                            objectKV.GetObjKey());
    rpcPoint.Record();
    VLOG(1) << "Remote get success";
    return Status::OK();
}

Status WorkerOcServiceGetImpl::RetrieveRemotePayload(
    ReadObjectKV &objectKV,
    std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> &clientApi,
    GetObjectRemoteRspPb &rspPb)
{
    Status status;
    const auto &objectKey = objectKV.GetObjKey();
    auto &entry = objectKV.GetObjEntry();
    uint64_t offset = objectKV.GetReadOffset();
    // Allocate the memory earlier while the payload is still in flight in the background.
    // This will lower the latency.
    auto completeDataSize = static_cast<size_t>(rspPb.data_size());
    auto needReceiveSz = objectKV.GetReadSize();
    auto metaSz = entry->GetMetadataSize();
    uint64_t cap = completeDataSize + metaSz;
    bool szChanged = (entry->GetShmUnit() == nullptr) || (entry->GetShmUnit()->size != cap);
    // Only create new shm if size changed or not exist.
    if (szChanged) {
        auto shmUnit = std::make_shared<ShmUnit>();
        RETURN_IF_NOT_OK(AllocateMemoryForObject(objectKey, completeDataSize, metaSz, false, evictionManager_, *shmUnit,
                                                 entry->modeInfo.GetCacheType()));
        shmUnit->id = ShmKey::Intern(GetStringUuid());
        entry->SetShmUnit(shmUnit);
    }

    void *dest = reinterpret_cast<uint8_t *>(entry->GetShmUnit()->GetPointer()) + metaSz + objectKV.GetReadOffset();
    // Newer version of worker can connect to us using direct tcp connection
    // and is able to write directly into shared memory.
    if (clientApi->IsV2Client()) {
        status = clientApi->ReceivePayload(dest, needReceiveSz);
    } else {
        // Downlevel client.
        auto f = [this, &clientApi, &entry, &needReceiveSz, &metaSz, offset]() {
            std::vector<RpcMessage> payloads;
            RETURN_IF_NOT_OK(clientApi->ReceivePayload(payloads));
            PerfPoint copyPoint(PerfKey::WORKER_MEMORY_COPY);
            size_t payloadLen = 0;
            std::vector<std::pair<const uint8_t *, uint64_t>> payloadData;
            for (const auto &msg : payloads) {
                payloadLen += msg.Size();
                payloadData.emplace_back(reinterpret_cast<const uint8_t *>(msg.Data()), msg.Size());
            }
            CHECK_FAIL_RETURN_STATUS(!(payloads.empty() || payloadLen == 0), K_INVALID,
                                     "Payload is null or no bytes to write.");
            CHECK_FAIL_RETURN_STATUS(needReceiveSz == payloadLen, K_RUNTIME_ERROR, "Data size does not match.");
            RETURN_IF_NOT_OK(entry->GetShmUnit()->MemoryCopy(payloadData, memCpyThreadPool_, metaSz + offset));
            copyPoint.Record();
            return Status::OK();
        };
        status = f();
    }
    // Clean up on error
    if (status.IsError()) {
        entry->GetShmUnit()->SetHardFreeMemory();
        entry->GetShmUnit()->FreeMemory();
        LOG(ERROR) << "Fail to operate entry memory copy because of " << status.ToString();
        if (clientApi->IsV2Client()) {
            clientApi->CleanupOnError(status);
        }
        return status;
    }
    entry->SetDataSize(completeDataSize);
    return Status::OK();
}

void WorkerOcServiceGetImpl::AttemptGetObjectsLocally(const std::shared_ptr<GetRequest> &request,
                                                      std::map<ReadKey, LockedEntity> &lockedEntries)
{
    auto localGet = [this, request](const ReadKey &readKey, std::shared_ptr<SafeObjType> &entry) {
        if ((entry->Get() != nullptr) && entry->Get()->IsBinary() && !entry->Get()->stateInfo.IsCacheInvalid()
            && entry->Get()->IsGetDataEnablelFromLocal()) {
            ReadObjectKV objectKV(readKey, *entry);
            RETURN_IF_NOT_OK(KeepObjectDataInMemory(objectKV));
            RETURN_IF_NOT_OK(UpdateRequestForSuccess(objectKV, request));
            entry->WUnlock();
            RETURN_IF_NOT_OK(OsXprtPipln::TriggerLocalPipelineRH2D(request->GetH2DChunkManager(),
                readKey.objectKey,
                reinterpret_cast<char*>(entry->Get()->GetShmUnit()->GetPointer()),
                entry->Get()->GetMetadataSize(),
                entry->Get()->GetDataSize()));
            return Status::OK();
        }
        return Status(K_NOT_FOUND, "");
    };

    auto it = lockedEntries.begin();
    while (it != lockedEntries.end()) {
        auto &entry = it->second.safeObj;
        const auto &readKey = it->first;
        if (localGet(readKey, entry).IsOk()) {
            lockedEntries.erase(it++);
        } else {
            it++;
        }
    }
}

Status WorkerOcServiceGetImpl::QueryMetaDataFromMasterImpl(const HostPort &destMasterHostPort, uint64_t subTimeout,
                                                           const std::vector<std::string> &objKeysToQuery,
                                                           bool isFromOtherAz, datasystem::master::QueryMetaRspPb &rsp,
                                                           std::vector<RpcMessage> &payloads)
{
    datasystem::master::QueryMetaReqPb req;
    SetQueryMetaInfo(req, objKeysToQuery, destMasterHostPort.ToString(), true, isFromOtherAz);
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(destMasterHostPort);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "Get masterApi failed, cannot queryMeta");
    std::function<Status(QueryMetaReqPb &, QueryMetaRspPb &, std::vector<RpcMessage> &)> func =
        [&workerMasterApi, &subTimeout](QueryMetaReqPb &req, QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads) {
            Status s = workerMasterApi->QueryMeta(req, subTimeout, rsp, payloads);
            if (s.IsError()) {
                payloads.clear();
            }
            return s;
        };
    std::vector<RpcMessage> tmpPayloads;
    RETURN_IF_NOT_OK(RedirectRetryWhenMetasMoving(req, rsp, tmpPayloads, func));
    RETURN_IF_NOT_OK(CorrectQueryMetaResponse(tmpPayloads, rsp, payloads));
    RETURN_IF_NOT_OK(QueryMetadataFromRedirectMaster(rsp, subTimeout, isFromOtherAz, payloads));
    return Status::OK();
}

void WorkerOcServiceGetImpl::ProcessQueryMetaFailedObjsWhenMetaStoredInEtcd(
    const std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster,
    std::unordered_set<std::string> &&objectKeysNotExist, const std::unordered_set<std::string> &objectKeysPuzzled,
    const std::unordered_set<std::string> &objectKeysMayInOtherAz, std::vector<master::QueryMetaInfoPb> &queryMetas,
    std::vector<std::string> &absentObjectKeys)
{
    std::unordered_set<std::string> objectKeysNotExistNeedQueryInEtcd;
    if (HaveOtherAZ() && !FLAGS_cross_cluster_get_meta_from_worker) {
        for (auto it = objectKeysNotExist.begin(); it != objectKeysNotExist.end();) {
            if (!HasWorkerId(*it)) {
                objectKeysNotExistNeedQueryInEtcd.insert(std::move(*it));
                it = objectKeysNotExist.erase(it);
            } else {
                ++it;
            }
        }
    }
    absentObjectKeys.insert(absentObjectKeys.end(), objectKeysNotExist.begin(), objectKeysNotExist.end());

    if (objKeysUndecidedMaster.empty() && objectKeysNotExistNeedQueryInEtcd.empty() && objectKeysPuzzled.empty()
        && objectKeysMayInOtherAz.empty()) {
        return;
    }

    std::stringstream msg;
    msg << "Try get some miss objs from etcd:" << VectorToString(objectKeysMayInOtherAz)
        << VectorToString(objectKeysPuzzled) << VectorToString(objectKeysNotExistNeedQueryInEtcd);
    for (const auto &iter : objKeysUndecidedMaster) {
        msg << VectorToString(iter.second);
    }
    LOG(INFO) << msg.str();

    std::unordered_map<std::string, std::unordered_set<std::string>> groupedObjectKeysQueryMetaFailed;
    for (const auto &objKey : objectKeysPuzzled) {
        std::string workerId;
        (void)TrySplitWorkerIdFromObjecId(objKey, workerId);
        auto iter = groupedObjectKeysQueryMetaFailed.find(workerId);
        if (iter == std::end(groupedObjectKeysQueryMetaFailed)) {
            std::unordered_set<std::string> objectKeyList({ objKey });
            groupedObjectKeysQueryMetaFailed.insert(std::make_pair(workerId, std::move(objectKeyList)));
        } else {
            iter->second.emplace(objKey);
        }
    }

    for (const auto &kv : groupedObjectKeysQueryMetaFailed) {
        // If workerId not in hash ring, try to find meta data in local and other's AZ
        LOG_IF_ERROR(QueryMetaDataFromEtcd(kv.second, kv.first, true, queryMetas, absentObjectKeys),
                     "Query metadata from etcd by worker id failed.");
    }
    LOG_IF_ERROR(QueryMetaDataFromEtcd(objectKeysNotExistNeedQueryInEtcd, "", true, queryMetas, absentObjectKeys),
                 "Query metadata from etcd by hash failed.");
    LOG_IF_ERROR(QueryMetaDataFromEtcd(objectKeysMayInOtherAz, "", true, queryMetas, absentObjectKeys),
                 "Query metadata from etcd by hash failed.");
    for (const auto &kv : objKeysUndecidedMaster) {
        // If workerId not in hash ring, try to find meta data in local and other's AZ
        LOG_IF_ERROR(QueryMetaDataFromEtcd(kv.second, kv.first, true, queryMetas, absentObjectKeys),
                     "Query metadata from etcd by worker id failed.");
    }
}

Status WorkerOcServiceGetImpl::ProcessQueryMetaFailedObjsIfAllowCrossAzGetMeta(
    uint64_t subTimeout, std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster,
    ObjectKeysQueryMetaFailed &objectKeysQueryMetaFailed, std::unordered_set<std::string> &objectKeysMayInOtherAz,
    std::vector<master::QueryMetaInfoPb> &queryMetas, std::vector<RpcMessage> &payloads)
{
    auto iter = objKeysUndecidedMaster.find("");
    if (iter != objKeysUndecidedMaster.end()) {
        objectKeysMayInOtherAz = std::move(iter->second);
        objKeysUndecidedMaster.erase(iter);
    }

    auto extractObjectsMayExistInOtherAz = [&objectKeysMayInOtherAz](auto &set) {
        for (auto iter = set.begin(); iter != set.end();) {
            if (!HasWorkerId(*iter)) {
                objectKeysMayInOtherAz.insert(std::move(*iter));
                iter = set.erase(iter);
            } else {
                ++iter;
            }
        }
    };
    std::apply([&](auto &...sets) { (extractObjectsMayExistInOtherAz(sets), ...); }, objectKeysQueryMetaFailed);

    RETURN_OK_IF_TRUE(objectKeysMayInOtherAz.empty());
    LOG(INFO) << "Try get some miss objs from other az: " << VectorToString(objectKeysMayInOtherAz);
    for (const auto &otherAZName : otherAZNames_) {
        for (auto it = objectKeysMayInOtherAz.begin(); it != objectKeysMayInOtherAz.end();) {
            const auto &objectKey = *it;
            MetaAddrInfo metaAddrInfo;
            auto rc = etcdCM_->QueryMasterAddrInOtherAz(otherAZName, objectKey, metaAddrInfo);
            if (rc.IsError()) {
                LOG(WARNING) << "QueryMasterAddrInOtherAz failed, msg: " << rc.ToString();
                ++it;
                continue;
            }
            auto masterHostPort = metaAddrInfo.GetAddressAndSaveDbName();
            datasystem::master::QueryMetaRspPb rsp;
            bool isFromOtherAz = metaAddrInfo.IsFromOtherAz();
            rc = QueryMetaDataFromMasterImpl(masterHostPort, subTimeout, { objectKey }, isFromOtherAz, rsp, payloads);
            if (rc.IsError()) {
                LOG(WARNING) << "Query meta from master[" << masterHostPort.ToString()
                             << "] failed, msg: " << rc.ToString();
            }
            if (rc.IsError() || rsp.query_metas_size() == 0) {
                ++it;
                continue;
            }
            for (auto &meta : *rsp.mutable_query_metas()) {
                meta.set_is_from_other_az(isFromOtherAz);
            }
            (void)queryMetas.insert(queryMetas.end(), rsp.mutable_query_metas()->begin(),
                                    rsp.mutable_query_metas()->end());
            it = objectKeysMayInOtherAz.erase(it);
        }
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::QueryMetadataFromMaster(const std::vector<std::string> &objectKeys, uint64_t subTimeout,
                                                       QueryMetadataFromMasterResult &result, bool queryEtcdMeta)
{
    PerfPoint point(PerfKey::WORKER_QUERY_META_PRE);
    Status lastRc;
    std::vector<master::QueryMetaInfoPb> &queryMetas = result.queryMetas;
    std::vector<RpcMessage> &payloads = result.payloads;
    std::map<std::string, uint64_t> &absentObjectKeysWithVersion = result.absentObjectKeysWithVersion;
    INJECT_POINT("worker.before_query_meta");
    // 1. Get map of objectKeys grouped by master
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMaster;
    std::unordered_map<std::string, std::unordered_set<std::string>> objKeysUndecidedMaster;
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_ROUTER);
    etcdCM_->GroupObjKeysByMasterHostPort(objectKeys, objKeysGrpByMaster, objKeysUndecidedMaster);
    // 2. Send requests for each master
    std::vector<std::future<Status>> futures;
    std::string traceID = Trace::Instance().GetTraceID();
    Timer timer;
    int64_t realTimeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    std::vector<BatchQueryMetaResult> batchQueryResults;
    batchQueryResults.resize(objKeysGrpByMaster.size());
    size_t idx = 0;
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_BATCH_BY_ADDR);
    for (auto &item : objKeysGrpByMaster) {
        BatchQueryMetaResult &res = batchQueryResults[idx++];
        auto func = [&res, realTimeoutMs, subTimeout, item, &traceID, &timer, this]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID, true);
            int64_t elapsed = timer.ElapsedMilliSecond();
            reqTimeoutDuration.Init(realTimeoutMs - elapsed);
            HostPort masterAddr = item.first.GetAddressAndSaveDbName();
            const std::vector<std::string> &currentIds = item.second;
            bool isFromOtherAz = item.first.IsFromOtherAz();
            datasystem::master::QueryMetaRspPb &rsp = res.rsp;
            auto rc = QueryMetaDataFromMasterImpl(masterAddr, subTimeout, currentIds, isFromOtherAz, rsp, res.payloads);
            if (rc.IsError()) {
                LOG(ERROR) << FormatString("Query metadata from master[%s]: %s", masterAddr.ToString(), rc.ToString());
                res.failedKeys.insert(currentIds.begin(), currentIds.end());
                return rc;
            }
            for (auto &meta : *rsp.mutable_query_metas()) {
                meta.set_is_from_other_az(isFromOtherAz);
            }
            return Status::OK();
        };
        if (idx == objKeysGrpByMaster.size()) {
            // using current thread handle the last task.
            auto rc = func();
            lastRc = rc.IsError() ? rc : lastRc;
        } else {
            futures.emplace_back(workerBatchQueryMetaThreadPool_->Submit(std::move(func)));
        }
    }
    for (auto &f : futures) {
        auto rc = f.get();
        lastRc = rc.IsError() ? rc : lastRc;
    }
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_HANDLE_RESULT);
    // 3. Statistics the metadata results just queried.
    ObjectKeysQueryMetaFailed objectKeysQueryMetaFailed;
    auto &objectKeysNotExist = std::get<OBJECTS_NOT_EXIST_IDX>(objectKeysQueryMetaFailed);
    auto &objectKeysPuzzled = std::get<OBJECTS_PUZZLED_IDX>(objectKeysQueryMetaFailed);
    std::vector<std::string> absentObjectKeys;
    std::map<std::string, uint64_t> deletingObjectsWithVersion;
    for (auto &res : batchQueryResults) {
        if (!res.failedKeys.empty()) {
            objectKeysPuzzled.insert(res.failedKeys.begin(), res.failedKeys.end());
            continue;
        }
        auto &rsp = res.rsp;
        RETURN_IF_NOT_OK(CorrectQueryMetaResponse(res.payloads, rsp, payloads));
        queryMetas.insert(queryMetas.end(), rsp.mutable_query_metas()->begin(), rsp.mutable_query_metas()->end());
        objectKeysNotExist.insert(rsp.not_exist_ids().begin(), rsp.not_exist_ids().end());
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            rsp.not_exist_ids_size() == rsp.deleting_versions_size(), K_RUNTIME_ERROR,
            FormatString("The size of not_exist_ids %d and deleting_versions %d not match.", rsp.not_exist_ids_size(),
                         rsp.deleting_versions_size()));
        for (int index = 0; index < rsp.not_exist_ids_size(); index++) {
            deletingObjectsWithVersion.emplace(rsp.not_exist_ids(index), rsp.deleting_versions(index));
        }
    }

    INJECT_POINT("worker.get_no_metadata", [&queryMetas, &payloads, &objectKeys, &absentObjectKeysWithVersion]() {
        queryMetas.clear();
        payloads.clear();
        for (const auto &id : objectKeys) {
            (void)absentObjectKeysWithVersion.emplace(id, 0);
        }
        return Status::OK();
    });

    // 4. Handle objKeys needed to try to get metadata in other AZ
    std::unordered_set<std::string> objectKeysMayInOtherAz;
    if (FLAGS_cross_cluster_get_meta_from_worker) {
        point.RecordAndReset(PerfKey::WORKER_QUERY_META_HANDLE_CROSS_AZ);
        LOG_IF_ERROR(ProcessQueryMetaFailedObjsIfAllowCrossAzGetMeta(subTimeout, objKeysUndecidedMaster,
                                                                     objectKeysQueryMetaFailed, objectKeysMayInOtherAz,
                                                                     queryMetas, payloads),
                     "Handle objKeys needed to try to get metadata in other AZ failed");
    }
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_HANDLE_NOT_FOUND);
    // 5. If etcd is used as L2cache for metadata, try to get miss meta from etcd.
    bool multiReplicaEnabled = etcdCM_->MultiReplicaEnabled();
    bool metaStoredInEtcd = FLAGS_oc_io_from_l2cache_need_metadata && !multiReplicaEnabled;
    if (metaStoredInEtcd && queryEtcdMeta) {
        ProcessQueryMetaFailedObjsWhenMetaStoredInEtcd(objKeysUndecidedMaster, std::move(objectKeysNotExist),
                                                       objectKeysPuzzled, objectKeysMayInOtherAz, queryMetas,
                                                       absentObjectKeys);
    } else {
        absentObjectKeys.insert(absentObjectKeys.end(), objectKeysNotExist.begin(), objectKeysNotExist.end());
        absentObjectKeys.insert(absentObjectKeys.end(), objectKeysPuzzled.begin(), objectKeysPuzzled.end());
        absentObjectKeys.insert(absentObjectKeys.end(), objectKeysMayInOtherAz.begin(), objectKeysMayInOtherAz.end());
        for (const auto &kv : objKeysUndecidedMaster) {
            absentObjectKeys.insert(absentObjectKeys.end(), kv.second.begin(), kv.second.end());
        }
    }
    for (const auto &id : absentObjectKeys) {
        auto it = deletingObjectsWithVersion.find(id);
        uint64_t deletingVersion = it != deletingObjectsWithVersion.end() ? it->second : 0;
        absentObjectKeysWithVersion.emplace(id, deletingVersion);
    }
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_OTHER);
    return lastRc;
}

Status WorkerOcServiceGetImpl::QueryMetadataFromRedirectMaster(master::QueryMetaRspPb &rsp, uint64_t subTimeout,
                                                               bool isFromOtherAz, std::vector<RpcMessage> &payloads)
{
    for (const auto &redirectInfo : rsp.info()) {
        std::vector<RpcMessage> redirectPayloads;
        master::QueryMetaReqPb redirectQueryReq;
        master::QueryMetaRspPb redirectQueryRsp;
        std::vector<std::string> redirectIds = { redirectInfo.change_meta_ids().begin(),
                                                 redirectInfo.change_meta_ids().end() };
        HostPort redirectMasterAddr;
        RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr));
        SetQueryMetaInfo(redirectQueryReq, redirectIds, redirectMasterAddr.ToString(), false, isFromOtherAz);
        std::shared_ptr<WorkerMasterOCApi> redirectWorkerMasterApi =
            workerMasterApiManager_->GetWorkerMasterApi(redirectMasterAddr);
        CHECK_FAIL_RETURN_STATUS(redirectWorkerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "hash master get failed, QueryMetadataFromMaster failed");
        RETURN_IF_NOT_OK(
            redirectWorkerMasterApi->QueryMeta(redirectQueryReq, subTimeout, redirectQueryRsp, redirectPayloads));
        // save the result to rsp and payload
        RETURN_IF_NOT_OK(CorrectQueryMetaResponse(redirectPayloads, redirectQueryRsp, payloads));
        std::copy(redirectQueryRsp.mutable_query_metas()->begin(), redirectQueryRsp.mutable_query_metas()->end(),
                  RepeatedFieldBackInserter(rsp.mutable_query_metas()));
        std::copy(redirectQueryRsp.mutable_not_exist_ids()->begin(), redirectQueryRsp.mutable_not_exist_ids()->end(),
                  RepeatedFieldBackInserter(rsp.mutable_not_exist_ids()));
        std::copy(redirectQueryRsp.mutable_deleting_versions()->begin(),
                  redirectQueryRsp.mutable_deleting_versions()->end(),
                  RepeatedFieldBackInserter(rsp.mutable_deleting_versions()));
    }
    return Status::OK();
}

/*
 * There are 4 scenarios should query meata from ECTD
 * 1. Normal ObjectKey: we can certainly get a Worker by ObjectKey no matter whether it's belong to local AZ
 *   (1) The Worker's status is ACTIVE, and if failed to get Meta Data, then try to get from other AZ's ETCD;
 *   (2) The Worker's status is FAILED or TIMEOUT, then try to get from both local and other AZ's ETCD;
 * 2. ObjectKey with WorkerId: we can be definitely sure whether the WorkerId belong to local AZ
 *   (1) The Worker belong to local AZ but the status is NOT ACTIVE, then try to get from local ETCD;
 *   (2) The Worker doesn't belong to local AZ, then try to get from other AZ's ETCD;
 */
Status WorkerOcServiceGetImpl::QueryMetaDataFromEtcd(const std::unordered_set<std::string> &objectKeys,
                                                     const std::string &workerId, bool getLocalAz,
                                                     std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                     std::vector<std::string> &absentObjectKeys)
{
    INJECT_POINT("worker.QueryMetaDataFromEtcd_failure");
    if (!getLocalAz && !HaveOtherAZ()) {
        VLOG(DEBUG_LOG_LEVEL) << "No need to query local: flag " << getLocalAz << ", other AZ flag " << HaveOtherAZ();
        (void)absentObjectKeys.insert(absentObjectKeys.end(), objectKeys.begin(), objectKeys.end());
        return Status::OK();
    }
    Status rc;
    for (const std::string &objKey : objectKeys) {
        if (getLocalAz) {
            rc = ConstructKeyAndQueryMetaFromEtcd(FLAGS_cluster_name, objKey, workerId, queryMetas);
            if (rc.IsOk()) {
                continue;
            }
        }
        if (!HaveOtherAZ()) {
            LOG(ERROR) << "Can not get meta: " << rc.ToString();
            continue;
        }
        std::stringstream errLog;
        for (const auto &azName : otherAZNames_) {
            rc = ConstructKeyAndQueryMetaFromEtcd(azName, objKey, workerId, queryMetas);
            if (rc.IsOk()) {
                break;
            }
            errLog << FormatString("(%s : %s), ", azName, rc.ToString());
        }
        if (rc.IsError()) {
            LOG(ERROR) << "Can not get meta from local or other az. " << errLog.str();
            absentObjectKeys.emplace_back(objKey);
        }
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::ConstructKeyAndQueryMetaFromEtcd(const std::string &azName, const std::string &objKey,
                                                                const std::string &workerId,
                                                                std::vector<master::QueryMetaInfoPb> &queryMetas)
{
    std::string etcdTableName = std::string(ETCD_META_TABLE_PREFIX);
    std::string hashValue;
    if (workerId.empty()) {
        // /azName/ETCD_META_HASH_TABLE/key_hash/key
        etcdTableName.append(ETCD_HASH_SUFFIX);
        hashValue = Hash2Str(MurmurHash3_32(objKey));
    } else {
        // /azName/ETCD_META_WORKER_TABLE/worker_id_hash/objKey
        etcdTableName.append(ETCD_WORKER_SUFFIX);
        hashValue = Hash2Str(MurmurHash3_32(workerId));
    }
    std::string tablePrefix;
    if (!FLAGS_cluster_name.empty()) {
        tablePrefix = FormatString("/%s", azName);
    }
    std::string etcdKey = tablePrefix + FormatString("%s/%zu/%s", etcdTableName, hashValue, objKey);
    LOG(INFO) << "Query objKey: " << objKey << ", workerId: " << workerId << ", AZ name: " << azName
              << ", query ETCD key: " << etcdKey;

    auto metaPb = std::make_unique<ObjectMetaPb>();
    CHECK_FAIL_RETURN_STATUS(!etcdStore_->IsKeepAliveTimeout(), K_RPC_UNAVAILABLE, "etcd is unavailable");
    RangeSearchResult res;
    RETURN_IF_NOT_OK(etcdStore_->RawGet(etcdKey, res, 0, reqTimeoutDuration.CalcRemainingTime()));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(metaPb->ParseFromString(res.value), StatusCode::K_RUNTIME_ERROR,
                                         FormatString("Parse string to ObjectMetaPb failed. String is: %s", res.value));
    master::QueryMetaInfoPb queryMeta;
    VLOG(DEBUG_LOG_LEVEL) << "Success to get ObjectKey " << objKey << ", metadata primary addr "
                          << queryMeta.meta().primary_address() << " loadbalance addr " << queryMeta.address()
                          << " from ETCD";
    queryMeta.set_allocated_meta(metaPb.release());
    // Using the primary address to indicate the worker that holds the Object, and the address will be checked
    // before getting Object from the worker.
    queryMeta.set_address(queryMeta.meta().primary_address());
    queryMetas.emplace_back(std::move(queryMeta));
    return Status::OK();
}

void WorkerOcServiceGetImpl::SetQueryMetaInfo(master::QueryMetaReqPb &req, const std::vector<std::string> &objectKeys,
                                              const std::string &masterAddr, bool redirect, bool isFromOtherAz)
{
    // Get master addr successfully, query metadata by WorkerMastrOCApi.
    const std::string queryReqId = GetStringUuid();
    LOG(INFO) << "Query metadata from master: " << masterAddr << ", objects: " << VectorToString(objectKeys)
            << ", request id: " << queryReqId;
    *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
    req.set_request_id(queryReqId);
    req.set_redirect(redirect);
    req.set_address(localAddress_.ToString());
    req.set_is_from_other_az(isFromOtherAz);
}

Status WorkerOcServiceGetImpl::CorrectQueryMetaResponse(std::vector<RpcMessage> &tmpPayloads,
                                                        master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads)
{
    if (tmpPayloads.empty()) {
        return Status::OK();
    }
    auto payloadSize = payloads.size();
    auto realPayloadSize = static_cast<uint32_t>(payloadSize);
    if (payloadSize != realPayloadSize) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "overflow happen");
    }
    bool overflow = false;
    for (auto iter = rsp.mutable_query_metas()->begin(); iter != rsp.mutable_query_metas()->end(); ++iter) {
        auto &queryMeta = *iter;
        std::for_each(queryMeta.mutable_payload_indexs()->begin(), queryMeta.mutable_payload_indexs()->end(),
                      [realPayloadSize, &overflow](uint32_t &idx) {
                          overflow |= (idx > UINT32_MAX - realPayloadSize);
                          idx += realPayloadSize;
                      });
    }
    payloads.insert(payloads.end(), std::make_move_iterator(tmpPayloads.begin()),
                    std::make_move_iterator(tmpPayloads.end()));
    if (overflow) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "overflow happen");
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetObjectsFromAnywhere(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                      const std::shared_ptr<GetRequest> &request,
                                                      std::vector<RpcMessage> &payloads,
                                                      std::map<ReadKey, LockedEntity> &lockedEntries,
                                                      std::unordered_set<std::string> &failedIds,
                                                      std::set<ReadKey> &needRetryIds)
{
    if (FLAGS_enable_worker_worker_batch_get) {
        return GetObjectsFromAnywhereBatched(queryMetas, request, payloads, lockedEntries, failedIds, needRetryIds);
    }
    return GetObjectsFromAnywhereParallelly(queryMetas, request, payloads, lockedEntries, failedIds, needRetryIds);
}

Status WorkerOcServiceGetImpl::GetObjectsFromAnywhereParallelly(const std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                                const std::shared_ptr<GetRequest> &request,
                                                                std::vector<RpcMessage> &payloads,
                                                                std::map<ReadKey, LockedEntity> &lockedEntries,
                                                                std::unordered_set<std::string> &failedIds,
                                                                std::set<ReadKey> &needRetryIds)
{
    const size_t kMinParallelRequests = 2;
    if (queryMetas.size() < kMinParallelRequests) {
        return GetObjectsFromAnywhereSerially(queryMetas, request, payloads, lockedEntries, failedIds, needRetryIds);
    }
    Status lastRc = Status::OK();
    std::vector<std::string> successIds;
    successIds.reserve(queryMetas.size());

    std::vector<std::future<Status>> futures;
    std::atomic<bool> abortAllTasks{ false };
    std::mutex commonMutex;
    auto traceId = Trace::Instance().GetTraceID();
    for (size_t i = 0; i < queryMetas.size(); ++i) {
        if (abortAllTasks.load()) {
            break;
        }

        const auto &queryMeta = queryMetas[i];
        const auto &meta = queryMeta.meta();

        const auto dataFormat = static_cast<DataFormat>(queryMeta.meta().config().data_format());
        if (dataFormat != DataFormat::BINARY && dataFormat != DataFormat::HETERO) {
            lastRc = Status(K_INVALID, "object data format not match.");
            failedIds.emplace(meta.object_key());
            LOG(ERROR) << lastRc;
            continue;
        }

        Timer timer;
        int64_t realTimeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
        futures.emplace_back(remoteGetThreadPool_->Submit([=, &queryMetas, &lockedEntries, &commonMutex, &abortAllTasks,
                                                           &request, &payloads, &lastRc, &successIds, &needRetryIds,
                                                           &failedIds, &traceId]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            int64_t elapsed = timer.ElapsedMilliSecond();
            reqTimeoutDuration.Init(realTimeoutMs - elapsed);
            if (abortAllTasks.load()) {
                return Status::OK();
            }
            const auto &queryMeta = queryMetas[i];
            const auto &meta = queryMeta.meta();
            const auto &objectKey = meta.object_key();
            auto subIter = lockedEntries.find(ReadKey(objectKey));
            if (subIter == lockedEntries.end()) {
                std::lock_guard<std::mutex> lock(commonMutex);
                LOG(INFO) << FormatString("[ObjectKey %s] Object not found in locked entries", objectKey);
                lastRc =
                    Status(K_NOT_FOUND, FormatString("[ObjectKey %s] Object not found in locked entries", objectKey));
                return lastRc;
            }
            const auto &readKey = subIter->first;
            std::shared_ptr<SafeObjType> &subEntry = subIter->second.safeObj;
            bool isInsert = subIter->second.insert;
            Status status = GetObjectFromAnywhereWithLock(readKey, request, subEntry, isInsert, queryMeta, payloads);

            // Protects access to successIds, needRetryIds, failedIds, and lastRc
            std::lock_guard<std::mutex> lock(commonMutex);

            if (status.IsOk()) {
                LOG(INFO) << FormatString("[ObjectKey %s] Get from remote success.", objectKey);
                successIds.push_back(objectKey);
                return status;
            }
            failedIds.emplace(objectKey);
            if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
                LOG(INFO) << FormatString("[ObjectKey %s] Object not found in remote worker.", objectKey);
                (void)needRetryIds.emplace(readKey);
            } else if (status.GetCode() == K_OUT_OF_MEMORY) {
                LOG(INFO) << FormatString("[ObjectKey %s] Out of memory, get remote abort.", objectKey);
                lastRc = status;
                abortAllTasks.store(true);
            } else {
                LOG(ERROR) << FormatString("[ObjectKey %s] Get from remote failed: %s.", objectKey, status.ToString());
                lastRc = status;
            }
            return status;
        }));
    }

    for (auto &f : futures) {
        f.wait();
    }

    if (successIds.size() != queryMetas.size()) {
        LOG(ERROR) << "Failed to get object data from remote. " << successIds.size() << " objects pulled success: ["
                   << VectorToString(successIds) << "], meta data num: " << queryMetas.size()
                   << " lastRc: " << lastRc.ToString();
    }
    return lastRc;
}

Status WorkerOcServiceGetImpl::GetObjectsFromAnywhereSerially(const std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                              const std::shared_ptr<GetRequest> &request,
                                                              std::vector<RpcMessage> &payloads,
                                                              std::map<ReadKey, LockedEntity> &lockedEntries,
                                                              std::unordered_set<std::string> &failedIds,
                                                              std::set<ReadKey> &needRetryIds)
{
    Status lastRc;
    std::vector<std::string> successIds;
    successIds.reserve(queryMetas.size());
    for (auto queryIt = queryMetas.begin(); queryIt != queryMetas.end(); ++queryIt) {
        const auto &queryMeta = *queryIt;
        const auto &meta = queryMeta.meta();
        const auto dataFormat = static_cast<DataFormat>(queryMeta.meta().config().data_format());
        const auto &objectKey = meta.object_key();
        if (dataFormat != DataFormat::BINARY && dataFormat != DataFormat::HETERO) {
            lastRc = Status(K_INVALID, "object data format not match.");
            failedIds.emplace(objectKey);
            LOG(ERROR) << lastRc;
            continue;
        }
        auto iter = lockedEntries.find(ReadKey(objectKey));
        if (iter == lockedEntries.end()) {
            LOG(ERROR) << FormatString("[ObjectKey %s] QueryMeta exist but lock entry absent, should not happen",
                                       objectKey);
            lastRc = Status(K_UNKNOWN_ERROR, "QueryMeta exist but lock entry absent, should not happen");
            continue;
        }
        const auto &readKey = iter->first;
        auto status = GetObjectFromAnywhereWithLock(readKey, request, iter->second.safeObj, iter->second.insert,
                                                    queryMeta, payloads);
        if (status.IsOk()) {
            LOG(INFO) << FormatString("[ObjectKey %s] Get from remote success.", objectKey);
            successIds.push_back(objectKey);
            continue;
        }
        failedIds.emplace(objectKey);
        if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
            LOG(INFO) << FormatString("[ObjectKey %s] Object not found in remote worker.", objectKey);
            lastRc = Status::OK();
            (void)needRetryIds.emplace(readKey);
        } else if (status.GetCode() == K_OUT_OF_MEMORY) {
            LOG(INFO) << FormatString("[ObjectKey %s] Out of memory, get remote abort.", objectKey);
            lastRc = status;
            break;
        } else {
            LOG(ERROR) << FormatString("[ObjectKey %s] Get from remote failed: %s.", objectKey, status.ToString());
            lastRc = status;
        }
    }

    if (successIds.size() != queryMetas.size()) {
        LOG(ERROR) << "Failed to get object data from remote. " << successIds.size() << " objects pulled success: ["
                   << VectorToString(successIds) << "], meta data num: " << queryMetas.size()
                   << " lastRc: " << lastRc.ToString();
    }
    return lastRc;
}

Status WorkerOcServiceGetImpl::GetObjectFromAnywhereWithLock(const ReadKey &readKey,
                                                             const std::shared_ptr<GetRequest> &request,
                                                             std::shared_ptr<SafeObjType> &entry, bool isInsert,
                                                             const master::QueryMetaInfoPb &queryMeta,
                                                             std::vector<RpcMessage> &payloads)
{
    const auto &meta = queryMeta.meta();
    const auto &address = queryMeta.address();
    const auto objectKey = meta.object_key();
    const auto &isFromOtherAz = queryMeta.is_from_other_az();
    VLOG(1) << "Get object from remote, object meta is: " << LogHelper::IgnoreSensitive(meta) << ", payload_index_size"
            << queryMeta.payload_indexs_size() << ", addr: " << address << ", is_from_other_az: " << isFromOtherAz
            << ", cache_type: " << queryMeta.meta().config().cache_type();
    INJECT_POINT("worker.GetObjectFromAnywhere");

    if ((entry->Get() != nullptr) && entry->Get()->IsBinary() && !entry->Get()->stateInfo.IsCacheInvalid()
        && entry->Get()->IsGetDataEnablelFromLocal()) {
        // If a local publish or remote get finished between QueryMeta and ReserveGetAndLock,
        // we will get a valid object here.
        ReadObjectKV objectKV(readKey, *entry);
        Status rc = KeepObjectDataInMemory(objectKV);
        if (rc.IsOk()) {
            RETURN_IF_NOT_OK(UpdateRequestForSuccess(objectKV, request));
            return Status::OK();
        } else if (!IsRemoteH2DEnabled()) {
            return rc;
        }
        // For RemoteH2D scenario, data never reaches shared memory, so need to re-do get entirely.
    }
    SetObjectEntryAccordingToMeta(meta, GetMetadataSize(), *entry);
    // Assume the client work with only one device id.
    std::shared_ptr<std::string> commId = nullptr;
    if (IsRemoteH2DEnabled()) {
        commId = std::make_shared<std::string>(request->GetClientCommUuid());
    }
    ReadObjectKV objectKV(readKey, *entry, commId);
    Status status = queryMeta.payload_indexs_size() == 0
                        ? GetObjectFromRemoteOnLock(meta, request, address, queryMeta.single_copy(), objectKV)
                        : GetObjectFromQueryMetaResultOnLock(request, queryMeta, payloads, objectKV);
    if (status.IsError()) {
        HandleGetFailureHelper(objectKey, meta.version(), entry, isInsert);
    }

    return status;
}

void WorkerOcServiceGetImpl::CheckAndReturnPullNotFoundForRetry(const ObjectMetaPb &meta, const std::string &address,
                                                                SafeObjType &entry, Status &checkConnectStatus,
                                                                Status &status)
{
    // If we cannot get data from L2 cache and address is empty, it means that the object may be
    // being deleting or is being cache invalid (so it's location list is empty), so let's return
    // K_WORKER_PULL_OBJECT_NOT_FOUND to let it try get again.
    if (entry->GetShmUnit() == nullptr) {
        if ((address.empty() && !IsNearDeathObject(address, meta)) || checkConnectStatus.GetCode() == K_NOT_FOUND) {
            status = Status(K_WORKER_PULL_OBJECT_NOT_FOUND, "not found");
        }
    }
}

Status WorkerOcServiceGetImpl::GetObjectFromRemoteOnLock(const ObjectMetaPb &meta,
                                                         const std::shared_ptr<GetRequest> &request,
                                                         const std::string &address, bool singleCopy,
                                                         ReadObjectKV &objectKV)
{
    PerfPoint point(PerfKey::WORKER_PULL_REMOTE_DATA);
    SafeObjType &entry = objectKV.GetObjEntry();
    const std::string &objKey = meta.object_key();

    /*
     * 1. If we can't connect with the remote worker: The meta must be gotten from ETCD, and the worker may belong to
     *    local or others' AZ, then we just get Object from storage and no need to keep the copy.
     * 2. If we connect with the remote worker: In this situation, we can't judge whether the meta is gotten from the
     *    master or ETCD. So before send copy to master, check the connection between worker and master, if it's
     *    connected, then it's gotten from master, otherwise from ETCD.
     */
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectKV.CheckReadOffset(), "Read offset verify failed");
    Status status(K_RUNTIME_ERROR, FormatString("Fail to get object %s from remote worker, addr: %s", objKey, address));
    Timer endToEndTimer;
    Status checkConnectStatus;
    bool ifWorkerConnected = false;
    if (!address.empty()) {
        HostPort hostAddr;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hostAddr.ParseString(address),
                                         FormatString("Parse object %s address %s failed", objKey, address));
        // Step1: Try to get data from local AZ's worker
        checkConnectStatus =
            etcdCM_->CheckConnection(hostAddr, false, ToleranceNotExistNode(singleCopy, meta.config().write_mode()));
        if (checkConnectStatus.IsOk()) {
            ifWorkerConnected = true;
            INJECT_POINT("worker.before_GetObjectFromRemoteWorkerAndDump");
            status =
                GetObjectFromRemoteWorkerAndDump(address, meta.primary_address(), meta.data_size(), objectKV, false);
            if (status.GetCode() == K_OUT_OF_MEMORY || IsRpcTimeoutOrTryAgain(status)) {
                return status;
            }
            if (entry.Get() == nullptr) {
                // Return error if CreateCopyMeta failed.
                RETURN_STATUS(K_NOT_FOUND, FormatString("Get from remote worker failed, object(%s) not exist in "
                                                        "worker, maybe the object has been deleted.",
                                                        objKey));
            }
        }
        TryGetObjectFromOtherAZ(meta, hostAddr, objectKV, status, false);
    } else {
        status = Status(K_RUNTIME_ERROR,
                        FormatString("Fail to get object %s from remote worker, no object copy exists.", objKey));
    }
    bool isFromL2 = false;
    // Step3: Try to get data from
    if (status.IsError()) {
        Timer timer;
        TryGetFromL2CacheWhenNotFoundInWorker(meta, address, ifWorkerConnected, objectKV, status);
        LOG(INFO) << "Query from L2 cache use " << timer.ElapsedMilliSecond() << " millisecond, address: " << address
                  << ", ifWorkerConnected: " << ifWorkerConnected;
        CheckAndReturnPullNotFoundForRetry(meta, address, entry, checkConnectStatus, status);
        isFromL2 = true;
    }
    RETURN_IF_NOT_OK(status);

    // Either get from worker fail or address is null, we roll back and remove objectId from object table.
    if (entry.Get() == nullptr) {
        RETURN_STATUS(K_NOT_FOUND, FormatString("GetFromRemote failed, object(%s) not exist in worker.", objKey));
    }

    LOG(INFO) << FormatString("object(%s) get from remote finish, size:%zu, use %f millisecond.", objKey,
                              entry->GetDataSize(), endToEndTimer.ElapsedMilliSecond());
    point.Record();

    isFromL2 ? CacheHitInfo::Instance().IncL2Hit(1) : CacheHitInfo::Instance().IncRemoteHit(1);
    return UpdateRequestForSuccess(objectKV, request);
}

void WorkerOcServiceGetImpl::TryGetObjectFromOtherAZ(const ObjectMetaPb &meta, const HostPort &hostAddr,
                                                     ReadObjectKV &objectKV, Status &status, bool isBatchGet)
{
    if (!FLAGS_cross_cluster_get_data_from_worker || !etcdCM_->CheckIfOtherAzNodeConnected(hostAddr)) {
        return;
    }
    const std::string &objKey = meta.object_key();
    const std::string &address = hostAddr.ToString();
    LOG(INFO) << FormatString("Try get object[%s] from other az worker[%s].", objKey, address);
    // If the hash type keys of other clusters are also cached locally, we cannot distinguish whether the key belongs to
    // this cluster or other clusters when deleting it. It may be a feasible method to store the keys of each cluster in
    // separate tables, but unfortunately it is not currently implemented, so an additional judgment is needed here.
    if (HasWorkerId(objKey)) {
        status =
            GetObjectFromRemoteWorkerAndDump(address, meta.primary_address(), meta.data_size(), objectKV, isBatchGet);
    } else {
        Timer timer;
        status = GetObjectFromRemoteWorkerWithoutDump(address, meta.primary_address(), meta.data_size(), objectKV);
        LOG(INFO) << "Query from other AZ node use " << timer.ElapsedMilliSecond() << " millisecond.";
    }
}

void WorkerOcServiceGetImpl::TryGetFromL2CacheWhenNotFoundInWorker(const ObjectMetaPb &meta, const std::string &address,
                                                                   bool ifWorkerConnected, ObjectKV &objectKV,
                                                                   Status &status)
{
    if (!FLAGS_enable_l2_cache_fallback) {
        status = Status(K_NOT_FOUND_IN_L2CACHE, status.GetMsg());
        return;
    }
    const ConfigPb &configPb = meta.config();
    bool writeToL2Storage = WriteMode(configPb.write_mode()) != WriteMode::NONE_L2_CACHE
                            && WriteMode(configPb.write_mode()) != WriteMode::NONE_L2_CACHE_EVICT;
    // If a copy exists and the worker where the copy is located is disconnected, the data will not be cached locally
    // (the data obtained from L2 cache may be inconsistent with the copy, avoiding consistency issues).
    bool isQueryWithoutCopy = !address.empty() && !ifWorkerConnected;
    // No worker holds the object or got error when pulling from remote worker, pull object from persistence api.
    if (writeToL2Storage && IsSupportL2Storage(supportL2Storage_)) {
        if (isQueryWithoutCopy) {
            status = GetObjectFromPersistenceAndDumpWithoutCopyMeta(objectKV);
        } else {
            status = GetObjectFromPersistenceAndDump(objectKV);
        }
    }
}

Status WorkerOcServiceGetImpl::GetObjectFromPersistenceAndDumpWithoutCopyMeta(ObjectKV &objectKV,
                                                                              bool noVersionAvailable, bool needDelete,
                                                                              uint64_t minVersion)
{
    const auto &objectKey = objectKV.GetObjKey();
    SafeObjType &entry = objectKV.GetObjEntry();
    LOG(INFO) << FormatString("Get object from L2 storage, object key is : %s", objectKey);

    int64_t remainingTime = reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%ld ms).", -remainingTime));
    CHECK_FAIL_RETURN_STATUS(persistenceApi_ != nullptr, K_RUNTIME_ERROR, "persistenceApi is nullptr");

    std::shared_ptr<std::stringstream> buffer = std::make_shared<std::stringstream>();

    PerfPoint point(PerfKey::WORKER_GET_L2_CACHE);
    Status res;
    if (!noVersionAvailable) {
        res = persistenceApi_->Get(objectKey, entry->GetCreateTime(), remainingTime, buffer);
    } else {
        res = persistenceApi_->GetWithoutVersion(objectKey, remainingTime, minVersion, buffer);
    }
    point.Record();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(res,
                                     FormatString("Call get object from L2 storage failed. objectKey:%s", objectKey));

    PerfPoint saveLocal(PerfKey::WORKER_L2_CACHE_DATA_SAVE_LOCAL);
    std::string bufferStr = buffer->str();

    std::vector<RpcMessage> payloads;
    payloads.emplace_back();
    auto &ele = payloads.back();
    RETURN_IF_NOT_OK(ele.ZeroCopyBuffer((void *)bufferStr.data(), bufferStr.size()));
    RETURN_IF_NOT_OK(SaveBinaryObjectToMemory(objectKV, payloads, evictionManager_, memCpyThreadPool_));
    evictionManager_->Add(objectKey);
    entry->stateInfo.SetNeedToDelete(needDelete);
    saveLocal.Record();
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetObjectFromPersistenceAndDump(ObjectKV &objectKV)
{
    return GetObjectFromPersistenceAndDumpWithoutCopyMeta(objectKV);
}

Status WorkerOcServiceGetImpl::GetObjectFromQueryMetaResultOnLock(const std::shared_ptr<GetRequest> &request,
                                                                  const master::QueryMetaInfoPb &queryMeta,
                                                                  std::vector<RpcMessage> &payloads,
                                                                  ReadObjectKV &objectKV)
{
    PerfPoint point(PerfKey::WORKER_PULL_QUERY_DATA);
    const auto &idxs = queryMeta.payload_indexs();
    const auto &objectKey = objectKV.GetObjKey();
    VLOG(1) << FormatString("[ObjectKey %s] Get from query result", objectKey);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(objectKV.CheckReadOffset(), "Read offset verify failed");
    CHECK_FAIL_RETURN_STATUS(
        *idxs.rbegin() < payloads.size(), StatusCode::K_RUNTIME_ERROR,
        FormatString("payload index[%ld] large equal than payloads size[%ld]", *idxs.rbegin(), payloads.size()));
    std::vector<RpcMessage> objDatas;
    objDatas.resize(queryMeta.payload_indexs_size());
    uint32_t i = 0;
    for (auto idx : idxs) {
        CHECK_FAIL_RETURN_STATUS(
            idx < payloads.size(), StatusCode::K_RUNTIME_ERROR,
            FormatString("payload index[%ld] large equal than payloads size[%ld]", idx, payloads.size()));
        objDatas[i++] = std::move(payloads[idx]);
    }
    RETURN_IF_NOT_OK(SaveBinaryObjectToMemory(objectKV, objDatas, evictionManager_, memCpyThreadPool_));
    const auto &meta = queryMeta.meta();
    if (queryMeta.is_from_other_az() && !HasWorkerId(meta.object_key())) {
        objectKV.GetObjEntry()->stateInfo.SetNeedToDelete(true);
    } else {
        if (FLAGS_enable_data_replication) {
            UpdateLocationParam param = { objectKey, objectKV.GetObjEntry()->GetCreateTime(),
                                          static_cast<uint32_t>(objectKV.GetObjEntry()->stateInfo.GetDataFormat()) };
            UpdateLocationTask task = UpdateLocationTask(param);
            RETURN_IF_NOT_OK(asyncUpdateLocationManager_->AddTask(std::move(task)));
            evictionManager_->Add(objectKey);
        } else {
            objectKV.GetObjEntry()->stateInfo.SetNeedToDelete(true);
        }
    }
    point.Record();
    CacheHitInfo::Instance().IncRemoteHit(1);
    return UpdateRequestForSuccess(objectKV, request);
}

void WorkerOcServiceGetImpl::AsyncUpdateLocationFunc(UpdateLocationTask &&task)
{
    VLOG(1) << "AsyncUpdateLocationFunc, the size is " << task.GetParams().size();
    if (task.GetParams().empty()) {
        return;
    }
    PerfPoint point(PerfKey::WORKER_ASYNC_UPDATE_LOCATION_FUNCTION);
    if (task.GetParams().size() == 1) {
        return AsyncUpdateSingleLocationFunc(std::move(task));
    }
    return AsyncBatchUpdateLocationFunc(std::move(task));
}

void WorkerOcServiceGetImpl::AsyncUpdateSingleLocationFunc(UpdateLocationTask &&task)
{
    master::CreateCopyMetaReqPb req;
    req.set_object_key(task.GetParams().front().objectKey);
    req.set_address(localAddress_.ToString());
    req.set_data_format(task.GetParams().front().data_format);
    req.set_redirect(true);
    req.set_version(task.GetParams().front().version);
    VLOG(1) << FormatString("Send copy metadata to master req: %s", LogHelper::IgnoreSensitive(req));
    master::CreateCopyMetaRspPb rsp;
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(task.GetParams().front().objectKey, etcdCM_);
    if (workerMasterApi == nullptr) {
        LOG(ERROR) << "GetWorkerMasterApi failed, objectKey: " << task.GetParams().front().objectKey;
        DeleteObjectsMetaUnacked({ { task.GetParams().front().objectKey, task.GetParams().front().version } });
        ClearObjectsByObjectKeys({ { task.GetParams().front().objectKey, task.GetParams().front().version } });
        return;
    }
    std::function<Status(CreateCopyMetaReqPb &, CreateCopyMetaRspPb &)> func =
        [&workerMasterApi](CreateCopyMetaReqPb &req, CreateCopyMetaRspPb &rsp) {
            INJECT_POINT("CreateCopyMeta.skip", []() { return Status::OK(); });
            return workerMasterApi->CreateCopyMeta(req, rsp);
        };
    Status rc = RedirectRetryWhenMetaMoving(req, rsp, workerMasterApi, func);
    if (IsRpcTimeout(rc)) {
        Status status = asyncUpdateLocationManager_->AddTask(std::move(task));
        if (status.IsError()) {
            LOG(WARNING) << FormatString("Add retry task failed: %s", rc.ToString());
            DeleteObjectsMetaUnacked({ { task.GetParams().front().objectKey, task.GetParams().front().version } });
            ClearObjectsByObjectKeys({ { task.GetParams().front().objectKey, task.GetParams().front().version } });
        }
    } else if (rc.IsError()) {
        LOG(INFO) << "Update location info failed, the object key is " << task.GetParams().front().objectKey;
        DeleteObjectsMetaUnacked({ { task.GetParams().front().objectKey, task.GetParams().front().version } });
        ClearObjectsByObjectKeys({ { task.GetParams().front().objectKey, task.GetParams().front().version } });
    }
}

void WorkerOcServiceGetImpl::AsyncBatchUpdateLocationFunc(UpdateLocationTask &&task)
{
    std::vector<UpdateLocationParam> retryParams;
    std::unordered_map<std::string, uint64_t> clearMetaKeys;
    std::unordered_map<std::string, uint64_t> clearObjectKeys;
    std::vector<std::string> objectKeys;
    for (const auto &param : task.GetParams()) {
        objectKeys.emplace_back(param.objectKey);
    }
    // grouped by master address
    auto objKeysGrpByMaster = etcdCM_->GroupObjKeysByMasterHostPort(objectKeys);
    std::unordered_map<MetaAddrInfo, std::vector<UpdateLocationParam>> paramsGroupByAddress;
    paramsGroupByAddress.reserve(objKeysGrpByMaster.size());
    std::vector<UpdateLocationParam> params = task.ExtractParams();
    bool found;
    for (auto param : params) {
        found = false;
        for (auto groupInfo : objKeysGrpByMaster) {
            auto it = std::find(groupInfo.second.begin(), groupInfo.second.end(), param.objectKey);
            if (it != groupInfo.second.end()) {
                paramsGroupByAddress[groupInfo.first].reserve(groupInfo.second.size());
                paramsGroupByAddress[groupInfo.first].emplace_back(param);
                found = true;
                break;
            }
        }
        if (!found) {
            LOG(INFO) << "AsyncUpdateLocation not found the object key " << param.objectKey;
            retryParams.emplace_back(param);
        }
    }
    GroupSendCreateMultiCopyMeta(paramsGroupByAddress, params, retryParams, clearMetaKeys, clearObjectKeys);
    if (!retryParams.empty()) {
        UpdateLocationTask retryTask = UpdateLocationTask(retryParams);
        Status rc = asyncUpdateLocationManager_->AddTask(std::move(retryTask));
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("Add retry task failed: %s", rc.ToString());
            for (const auto &param : retryParams) {
                clearMetaKeys.insert({ param.objectKey, param.version });
                clearObjectKeys.insert({ param.objectKey, param.version });
            }
        }
    }
    DeleteObjectsMetaUnacked(clearMetaKeys);
    ClearObjectsByObjectKeys(clearObjectKeys);
}

void WorkerOcServiceGetImpl::GroupSendCreateMultiCopyMeta(
    std::unordered_map<MetaAddrInfo, std::vector<UpdateLocationParam>> &paramsGroupByAddress,
    std::vector<UpdateLocationParam> &params, std::vector<UpdateLocationParam> &retryParams,
    std::unordered_map<std::string, uint64_t> &clearMetaKeys,
    std::unordered_map<std::string, uint64_t> &clearObjectKeys)
{
    // grouped send batch request
    for (const auto &group : paramsGroupByAddress) {
        if (group.second.empty()) {
            continue;
        }
        std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
        workerMasterApiManager_->GetWorkerMasterApi(group.first.GetAddress(), workerMasterApi);
        if (workerMasterApi == nullptr) {
            retryParams.insert(retryParams.end(), group.second.begin(), group.second.end());
            continue;
        }
        master::CreateMultiCopyMetaReqPb req;
        master::CreateMultiCopyMetaRspPb rsp;
        req.set_address(localAddress_.ToString());
        req.set_redirect(true);
        for (const auto &elem : group.second) {
            auto reqElem = req.add_multi_copy_meta_req_elems();
            reqElem->set_object_key(elem.objectKey);
            reqElem->set_data_format(elem.data_format);
            reqElem->set_version(elem.version);
        }
        std::function<Status(CreateMultiCopyMetaReqPb &, CreateMultiCopyMetaRspPb &)> func =
            [&workerMasterApi](CreateMultiCopyMetaReqPb &multiCopyReq, CreateMultiCopyMetaRspPb &multiCopyRsp) {
                INJECT_POINT("CreateMultiCopyMeta.skip", []() { return Status::OK(); });
                return workerMasterApi->CreateMultiCopyMeta(multiCopyReq, multiCopyRsp);
            };
        Status status = RedirectRetryWhenMetasMoving(req, rsp, func);
        if (status.IsError()) {
            VLOG(1) << "CreateMultiCopyMeta failed, status: " << status.ToString();
            if (IsRpcTimeout(status)) {
                retryParams.insert(retryParams.end(), group.second.begin(), group.second.end());
            } else {
                for (const auto &param : group.second) {
                    clearMetaKeys.insert({ param.objectKey, param.version });
                    clearObjectKeys.insert({ param.objectKey, param.version });
                }
            }
            continue;
        }
        UpdateLocationRedirectKeysToRetry(rsp.info(), group.second, retryParams);
        // if success, deal the failed_object_keys returned by res
        if (rsp.failed_object_keys().empty()) {
            continue;
        }
        for (const auto &failedKey : rsp.failed_object_keys()) {
            const auto &it = std::find_if(params.begin(), params.end(), [&](const UpdateLocationParam &param) {
                return param.objectKey == failedKey;
            });
            clearMetaKeys.insert({ it->objectKey, it->version });
            clearObjectKeys.insert({ it->objectKey, it->version });
        }
    }
}

void WorkerOcServiceGetImpl::UpdateLocationRedirectKeysToRetry(
    const google::protobuf::RepeatedPtrField<RedirectMetaInfo> &infos, const std::vector<UpdateLocationParam> &params,
    std::vector<UpdateLocationParam> &retryParams)
{
    if (infos.empty()) {
        return;
    }
    std::unordered_set<std::string> allObjKeySet;
    for (const auto &redirectInfo : infos) {
        const auto &redirectObjKeys = redirectInfo.change_meta_ids();
        allObjKeySet.insert(redirectObjKeys.begin(), redirectObjKeys.end());
    }
    for (const auto &param : params) {
        if (allObjKeySet.find(param.objectKey) != allObjKeySet.end()) {
            retryParams.emplace_back(param);
        }
    }
}

void WorkerOcServiceGetImpl::ClearObjectsByObjectKeys(const std::unordered_map<std::string, uint64_t> &clearKeyVersions)
{
    LOG(INFO) << "clear objects by update location params";
    for (auto &keyVersion : clearKeyVersions) {
        std::shared_ptr<SafeObjType> entry;
        auto status = objectTable_->Get(keyVersion.first, entry);
        if (status.IsError()) {
            if (status.GetCode() == StatusCode::K_NOT_FOUND) {
                continue;
            }
            LOG(WARNING) << FormatString("objectKey: %s get failed, status: %s", keyVersion.first, status.ToString());
            continue;
        }
        Status rc = TryLockWithRetry(keyVersion.first, entry);
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("objectKey: %s lock failed, status: %s", keyVersion.first, rc.ToString());
            continue;
        }
        Raii unlock([&entry]() { entry->WUnlock(); });
        if ((*entry)->GetCreateTime() == keyVersion.second) {
            ObjectKV objectKV(keyVersion.first, *entry);
            LOG_IF_ERROR(ClearObject(objectKV),
                         FormatString("Failed to erase object %s from object table", keyVersion.first));
        }
    }
}

Status WorkerOcServiceGetImpl::RemoveLocation(const std::string &objectKey, uint64_t version)
{
    INJECT_POINT("worker.remove_location");
    if (etcdCM_ == nullptr) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    auto api = workerMasterApiManager_->GetWorkerMasterApi(objectKey, etcdCM_);
    CHECK_FAIL_RETURN_STATUS(api != nullptr, StatusCode::K_INVALID,
                             "Getting master api failed. object key: " + objectKey);
    RemoveMetaReqPb req;
    RemoveMetaRspPb rsp;
    req.add_ids(objectKey);
    req.set_address(localAddress_.ToString());
    req.set_cause(master::RemoveMetaReqPb::EVICTION);
    req.set_version(version);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(api->RemoveMeta(req, rsp),
                                     FormatString("[ObjectKey %s] Remove location failed.", objectKey));
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetMetaAddress(const std::string &objKey, HostPort &masterAddr) const
{
    CHECK_FAIL_RETURN_STATUS(etcdCM_ != nullptr, StatusCode::K_NOT_READY, "ETCD cluster manager is not provided.");
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK(etcdCM_->GetMetaAddress(objKey, metaAddrInfo));
    masterAddr = metaAddrInfo.GetAddressAndSaveDbName();
    return Status::OK();
}

bool WorkerOcServiceGetImpl::IsNearDeathObject(const std::string &location, const ObjectMetaPb &meta)
{
    // 1. The location is get from master, it is empty means the object's location list is empty.
    // 2. The primary address is empty means the object's primary node has been restart or dead.
    // If 1 and 2 are both true, it means that the object can never be get from datasystem because
    // of the node dead/restart, in this scenario, we say the object is near-death.
    return location.empty() && meta.primary_address().empty();
}

bool WorkerOcServiceGetImpl::IsGetFromL2Storage(bool canNotFindInWorker, bool writeToL2Storage,
                                                datasystem::L2StorageType storageType)
{
    return canNotFindInWorker && writeToL2Storage && TESTFLAG(supportL2Storage_, storageType);
}

bool WorkerOcServiceGetImpl::HaveOtherAZ()
{
    return !FLAGS_other_cluster_names.empty();
}

Status WorkerOcServiceGetImpl::BatchLockForGet(const std::set<ReadKey> &readKeys,
                                               std::map<ReadKey, LockedEntity> &lockedEntries,
                                               std::unordered_set<std::string> &failObjects)
{
    Status lastRc;
    lockedEntries.clear();
    for (const auto &readKey : readKeys) {
        const auto &objectKey = readKey.objectKey;
        std::shared_ptr<SafeObjType> entry;
        bool isInsert;
        Status rc = objectTable_->ReserveGetAndLock(objectKey, entry, isInsert);
        if (rc.IsOk()) {
            if (isInsert) {
                SetEmptyObjectEntry(objectKey, *entry);
            }
            (void)lockedEntries.emplace(readKey, LockedEntity{ .safeObj = std::move(entry), .insert = isInsert });
        } else {
            LOG(ERROR) << FormatString("[ObjectKey %s] GetObjectFromRemote failed: %s.", objectKey, rc.ToString());
            failObjects.emplace(objectKey);
            lastRc = std::move(rc);
        }
    }
    return lastRc;
}

void WorkerOcServiceGetImpl::BatchUnlockForGet(const std::unordered_set<std::string> &failedObjectKeys,
                                               std::map<ReadKey, LockedEntity> &lockedEntries)
{
    for (auto &entry : lockedEntries) {
        const auto &objectKey = entry.first.objectKey;
        auto &safeObj = entry.second.safeObj;
        if (!safeObj->IsWLockedByCurrentThread()) {
            continue;
        }
        if (failedObjectKeys.find(objectKey) != failedObjectKeys.end() && entry.second.insert) {
            (void)objectTable_->Erase(objectKey, *safeObj);
        }
        safeObj->WUnlock();
    }
}

void WorkerOcServiceGetImpl::BatchUnlockForGet(const std::map<std::string, uint64_t> &objectKeys,
                                               std::map<ReadKey, LockedEntity> &lockedEntries)
{
    for (const auto &kv : objectKeys) {
        auto iter = lockedEntries.find(ReadKey(kv.first));
        if (iter == lockedEntries.end()) {
            continue;
        }
        auto &safeObj = iter->second.safeObj;
        // Not held by the current thread means that the previous process has been cleaned up.
        if (!safeObj->IsWLockedByCurrentThread()) {
            continue;
        }
        if (iter->second.insert) {
            (void)objectTable_->Erase(kv.first, *safeObj);
        }
        safeObj->WUnlock();
        (void)lockedEntries.erase(iter);
    }
}

void WorkerOcServiceGetImpl::FillGetObjMetaInfoRspPb(
    const std::vector<std::string> &objectKeys,
    const std::unordered_map<std::string, master::ObjectLocationInfoPb> &result, GetObjMetaInfoRspPb &resp)
{
    for (const auto &objectKey : objectKeys) {
        auto meta = resp.add_objs_meta_info();
        auto it = result.find(objectKey);
        if (it == result.end()) {
            continue;
        }
        meta->set_obj_size(it->second.object_size());
        for (auto &loc : it->second.object_locations()) {
            meta->add_location_ids(etcdCM_->GetWorkerIdByWorkerAddr(loc));
        }
    }
}

Status AuthenticateGetMetaUser(AkSkManager *akSkManager, const GetObjMetaInfoReqPb &req)
{
    if (akSkManager->SystemAuthEnabled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager->VerifySignatureAndTimestamp(req),
                                         "AK/SK failed. Only the ak/sk client can check the meta info of objects.");
    } else {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            !FLAGS_authorization_enable, K_NOT_AUTHORIZED,
            "Only the ak/sk client can check the meta info of objects if tenant authorization is enabled.");
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetMapOfObjectKeys(const std::vector<std::basic_string<char>> &objectKeys,
                                                  std::unordered_map<std::string, ObjectLocationInfoPb> &result,
                                                  Status &lastRc)
{
    std::unordered_map<MetaAddrInfo, std::vector<std::string>> objKeysGrpByMaster;
    std::unordered_map<std::string, std::unordered_set<std::string>> objKeysNotInHashRing;
    etcdCM_->GroupObjKeysByMasterHostPort(objectKeys, objKeysGrpByMaster, objKeysNotInHashRing);
    for (auto &[master, objs] : objKeysGrpByMaster) {
        HostPort workerAddr = master.GetAddressAndSaveDbName();
        master::GetObjectLocationsReqPb masterReq;
        master::GetObjectLocationsRspPb masterRsp;
        *masterReq.mutable_object_keys() = { objs.begin(), objs.end() };
        // redirect
        auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(workerAddr);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "hash master get failed, GetObjMetaInfo failed");
        auto status = workerMasterApi->GetObjectLocations(masterReq, masterRsp);
        if (status.IsError()) {
            LOG(ERROR) << FormatString("Query locations from %s of %s failed. %s", workerAddr.ToString(),
                                       VectorToString(objs), status.ToString());
            lastRc = status;
            continue;
        }
        for (auto &item : *masterRsp.mutable_location_infos()) {
            result.emplace(item.object_key(), std::move(item));
        }
    }
    if (result.empty()) {
        return lastRc;
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetObjMetaInfo(const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp)
{
    workerOperationTimeCost.Clear();
    RETURN_IF_NOT_OK(AuthenticateGetMetaUser(akSkManager_.get(), req));
    // construct objectKey by the input tenantId
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(req.tenantid(), req.object_keys());
    std::unordered_map<std::string, ObjectLocationInfoPb> result;
    Status lastRc;
    RETURN_IF_NOT_OK(GetMapOfObjectKeys(objectKeys, result, lastRc));
    FillGetObjMetaInfoRspPb(objectKeys, result, resp);
    return Status::OK();
}

Status WorkerOcServiceGetImpl::QuerySize(const QuerySizeReqPb &req, QuerySizeRspPb &rsp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    auto clientId = ClientKey::Intern(req.client_id());
    LOG(INFO) << "QuerySize start from client:" << clientId;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    QueryMetadataFromMasterResult queryMetaResult;
    std::vector<master::QueryMetaInfoPb> &queryMetas = queryMetaResult.queryMetas;
    int64_t subTimeout = 0;
    Status lastRc = QueryMetadataFromMaster(objectKeys, subTimeout, queryMetaResult);
    std::unordered_map<std::string, uint64_t> result;
    result.reserve(queryMetas.size());
    for (auto queryIt = queryMetas.begin(); queryIt != queryMetas.end(); ++queryIt) {
        const auto &queryMeta = *queryIt;
        const auto &meta = queryMeta.meta();
        const auto dataSize = meta.data_size();
        const auto objectKey = meta.object_key();
        result.emplace(objectKey, dataSize);
    }

    for (const auto &objectKey : objectKeys) {
        auto it = result.find(objectKey);
        if (it != result.end()) {
            rsp.add_sizes(it->second);
        } else {
            rsp.add_sizes(0);
        }
    }

    rsp.mutable_last_rc()->set_error_code(lastRc.GetCode());
    rsp.mutable_last_rc()->set_error_msg(lastRc.GetMsg());
    return Status::OK();
}

bool WorkerOcServiceGetImpl::IsLocalObject(const std::string &key)
{
    std::shared_ptr<SafeObjType> entry;
    if (objectTable_->Get(key, entry).IsOk() && entry->RLock(false).IsOk()) {
        Raii unlock([&entry]() { entry->RUnlock(); });
        return (*entry)->IsBinary() && !(*entry)->IsInvalid();
    }
    return false;
}

Status WorkerOcServiceGetImpl::Exist(const ExistReqPb &req, ExistRspPb &rsp)
{
    workerOperationTimeCost.Clear();
    Timer timer;
    AccessRecorder posixPoint(AccessRecorderKey::DS_POSIX_EXIST);
    INJECT_POINT("Exist.Sleep");
    auto clientId = ClientKey::Intern(req.client_id());
    LOG(INFO) << "Exist start from client:" << clientId;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsBatchSizeUnderLimit(req.object_keys_size()),
                                         StatusCode::K_INVALID, "invalid object size");
    auto keys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    std::unordered_set<std::string> existKeys;
    std::vector<std::string> nonLocalKeys;
    for (const auto &key : keys) {
        if (!IsLocalObject(key)) {
            nonLocalKeys.emplace_back(key);
            continue;
        }
        evictionManager_->Add(key);
        existKeys.emplace(key);
    }

    Status rc;
    if (!req.is_local() && !nonLocalKeys.empty()) {
        QueryMetadataFromMasterResult queryResult;
        std::vector<master::QueryMetaInfoPb> &queryMetas = queryResult.queryMetas;
        std::vector<RpcMessage> payloads;
        std::map<std::string, uint64_t> absentObjectKeys;
        rc = QueryMetadataFromMaster(keys, 0, queryResult, req.query_l2cache());
        for (const auto &meta : queryMetas) {
            existKeys.emplace(meta.meta().object_key());
        }
    }

    for (const auto &key : keys) {
        rsp.add_exists(existKeys.count(key));
    }

    RequestParam reqParam;
    reqParam.objectKey = ObjectKeysToAbbrStr(req.object_keys());
    posixPoint.Record(rc.GetCode(), "0", reqParam, rc.GetMsg());
    workerOperationTimeCost.Append("Total Exist", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("The operations of Exist %s", workerOperationTimeCost.GetInfo());
    return rc;
}

Status WorkerOcServiceGetImpl::GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    Timer timer;
    auto clientId = ClientKey::Intern(req.client_id());
    LOG(INFO) << "GetMetaInfo start from client:" << clientId;
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    auto objectKeys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());
    if (!req.is_dev_key()) {
        QueryMetadataFromMasterResult queryResult;
        auto rc = QueryMetadataFromMaster(objectKeys, 0, queryResult, false);
        std::vector<master::QueryMetaInfoPb> &queryMetas = queryResult.queryMetas;
        std::map<std::string, const ::google::protobuf::RepeatedField<::uint64_t>> mp;
        for (const auto &queryMeta : queryMetas) {
            auto &meta = queryMeta.meta();
            mp.insert({ meta.object_key(), std::move(meta.device_info().blob_sizes()) });
        }
        for (const auto &key : objectKeys) {
            auto devMetaInfos = rsp.add_dev_meta_infos();
            if (mp.find(key) != mp.end()) {
                devMetaInfos->mutable_blob_sizes()->Add(mp[key].begin(), mp[key].end());
            } else {
                *(devMetaInfos->mutable_blob_sizes()) = {};
            }
        }
        return Status::OK();
    }
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi =
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, etcdCM_);
    auto reqCopy = req;
    return workerMasterApi->GetMetaInfo(reqCopy, rsp);
}
Status WorkerOcServiceGetImpl::KeepObjectDataInMemory(ReadObjectKV &objectKV)
{
    const auto &objectKey = objectKV.GetObjKey();
    auto &entry = objectKV.GetObjEntry();
    if (entry->IsShmUnitExistsAndComplete()) {
        evictionManager_->Add(objectKey);
        CacheHitInfo::Instance().IncMemHit(1);
    } else if (entry->IsSpilled()) {
        RETURN_IF_NOT_OK(LoadSpilledObjectToMemory(objectKV, evictionManager_));
        CacheHitInfo::Instance().IncDiskHit(1);
    } else if (entry->HasL2Cache() && FLAGS_enable_l2_cache_fallback) {
        RETURN_IF_NOT_OK(GetObjectFromPersistenceAndDumpWithoutCopyMeta(objectKV, false, false));
        CacheHitInfo::Instance().IncL2Hit(1);
    } else {
        return Status(K_RUNTIME_ERROR, "object not found in local");
    }
    return Status::OK();
}

bool WorkerOcServiceGetImpl::ToleranceNotExistNode(bool singleCopy, uint32_t writeMode)
{
    bool writeToL2Storage =
        WriteMode(writeMode) != WriteMode::NONE_L2_CACHE && WriteMode(writeMode) != WriteMode::NONE_L2_CACHE_EVICT;
    return singleCopy && !writeToL2Storage;
}

std::string WorkerOcServiceGetImpl::GetHitInfo()
{
    return CacheHitInfo::Instance().GetHitInfo();
}

void WorkerOcServiceGetImpl::PostProcessRemoteGetInNotificationImpl(
    std::map<ReadKey, LockedEntity> &lockedEntries,
    const std::unordered_map<std::string, std::list<std::pair<std::list<GetObjectInfo>, uint64_t>>> &groupedQueryMetas,
    std::vector<std::vector<std::string>> &tempSuccessIds, std::vector<std::vector<ReadKey>> &tempNeedRetryIds,
    std::vector<std::unordered_set<std::string>> &tempFailedIds, std::set<ReadKey> &objectsNeedGetRemote,
    Status &lastRc, NotifyRemoteGetRspPb &rsp, const QueryMetaMap &queryMetas)
{
    std::vector<std::string> successIds;
    successIds.reserve(lockedEntries.size());
    std::set<ReadKey> needRetryIds;
    for (uint64_t i = 0; i < groupedQueryMetas.size(); ++i) {
        if (!tempSuccessIds[i].empty()) {
            successIds.insert(successIds.end(), std::make_move_iterator(tempSuccessIds[i].begin()),
                              std::make_move_iterator(tempSuccessIds[i].end()));
        }
        if (!tempNeedRetryIds[i].empty()) {
            needRetryIds.insert(std::make_move_iterator(tempNeedRetryIds[i].begin()),
                                std::make_move_iterator(tempNeedRetryIds[i].end()));
        }
        if (!tempFailedIds[i].empty()) {
            rsp.mutable_failed_object_keys()->Add(tempFailedIds[i].begin(), tempFailedIds[i].end());
        }
    }
    objectsNeedGetRemote.swap(needRetryIds);
    if (successIds.size() != lockedEntries.size()) {
        LOG(ERROR) << "Failed to get object data from remote. " << successIds.size() << " objects pulled success: ["
                   << VectorToString(successIds) << "], meta data num: " << lockedEntries.size()
                   << " lastRc: " << lastRc.ToString();
    }
    BatchUpdateLocationHelper(successIds, queryMetas, lockedEntries);
}

Status WorkerOcServiceGetImpl::ProcessRemoteGetInNotificationImpl(
    std::unordered_map<std::string, std::list<std::pair<std::list<GetObjectInfo>, uint64_t>>> &groupedQueryMetas,
    std::map<ReadKey, LockedEntity> &lockedEntries, NotifyRemoteGetRspPb &rsp, std::set<ReadKey> &objectsNeedGetRemote,
    const QueryMetaMap &queryMetas)
{
    Status lastRc;
    std::vector<std::future<Status>> futures;
    std::list<GetObjectInfo> failedMetas;
    std::vector<std::list<GetObjectInfo>> tempFailedMetas(groupedQueryMetas.size());
    std::vector<std::vector<std::string>> tempSuccessIds(groupedQueryMetas.size());
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(groupedQueryMetas.size());
    std::vector<std::unordered_set<std::string>> tempFailedIds(groupedQueryMetas.size());
    size_t index = 0;
    auto traceId = Trace::Instance().GetTraceID();
    Timer timer;
    int64_t realTimeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    std::shared_ptr<GetRequest> fakeRequest = nullptr;
    for (auto queryMeta = groupedQueryMetas.begin(); queryMeta != groupedQueryMetas.end(); ++queryMeta, ++index) {
        auto &address = queryMeta->first;
        auto &infoList = queryMeta->second;
        auto func = [this, address, &infoList, &fakeRequest, &tempSuccessIds, &tempNeedRetryIds, &tempFailedIds,
                     &tempFailedMetas, index, traceId, realTimeoutMs, &timer] {
            int64_t elapsed = static_cast<int64_t>(timer.ElapsedMilliSecond());
            reqTimeoutDuration.Init(realTimeoutMs - elapsed);
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId, true);
            Status lastRc;
            for (auto &infoPair : infoList) {
                auto &infos = infoPair.first;
                auto rc = BatchGetObjectFromRemoteOnLock(address, infos, fakeRequest, tempSuccessIds[index],
                                                         tempNeedRetryIds[index], tempFailedIds[index],
                                                         tempFailedMetas[index]);
                if (rc.IsError()) {
                    LOG(WARNING) << "BatchGetObjectFromRemoteOnLock failed, rc: " << rc.ToString();
                    lastRc = std::move(rc);
                }
            }
            return lastRc;
        };
        if (index + 1 == groupedQueryMetas.size()) {
            auto rc = func();
            if (rc.IsError()) {
                LOG(WARNING) << "BatchGetObjectFromRemoteOnLock failed, rc: " << rc.ToString();
                lastRc = std::move(rc);
            }
        } else {
            futures.emplace_back(workerBatchRemoteGetThreadPool_->Submit(std::move(func)));
        }
    }
    for (auto &fut : futures) {
        auto rc = fut.get();
        if (rc.IsError()) {
            LOG(WARNING) << "BatchGetObjectFromRemoteOnLock failed, rc: " << rc.ToString();
            lastRc = std::move(rc);
        }
    }
    PostProcessRemoteGetInNotificationImpl(lockedEntries, groupedQueryMetas, tempSuccessIds, tempNeedRetryIds,
                                           tempFailedIds, objectsNeedGetRemote, lastRc, rsp, queryMetas);
    return lastRc;
}

Status WorkerOcServiceGetImpl::ProcessRemoteGetInNotification(const NotifyRemoteGetReqPb &req,
                                                              std::set<ReadKey> objectsNeedGetRemote,
                                                              QueryMetaMap &queryMetas, NotifyRemoteGetRspPb &rsp)
{
    Status lastRc;
    do {
        std::unordered_set<std::string> lockfailedIds;
        std::map<ReadKey, LockedEntity> lockedEntries;
        auto lockRc = BatchLockForGet(objectsNeedGetRemote, lockedEntries, lockfailedIds);
        if (lockRc.IsError()) {
            lastRc = std::move(lockRc);
        }
        rsp.mutable_failed_object_keys()->Add(lockfailedIds.begin(), lockfailedIds.end());
        Raii unlockRaii([this, &lockfailedIds, &lockedEntries]() { BatchUnlockForGet(lockfailedIds, lockedEntries); });

        std::unordered_map<std::string, std::list<std::pair<std::list<GetObjectInfo>, uint64_t>>> groupedQueryMetas;
        for (const auto &obj : objectsNeedGetRemote) {
            auto iter = lockedEntries.find(obj);
            if (iter == lockedEntries.end()) {
                continue;
            }
            // Override the address to req.addr() (the leaving worker) so that BatchGetObjectFromRemoteOnLock pulls data
            // from the leaving worker.
            auto &queryMeta = queryMetas[obj.objectKey];
            queryMeta.set_address(req.addr());
            GetObjectInfo info;
            info.entry = &(iter->second);
            info.readKey = &(iter->first);
            info.queryMeta = &(queryMeta);
            GroupQueryMeta(info, groupedQueryMetas);
            SetObjectEntryAccordingToMeta(queryMeta.meta(), GetMetadataSize(), *iter->second.safeObj);
        }
        auto remoteGetRc =
            ProcessRemoteGetInNotificationImpl(groupedQueryMetas, lockedEntries, rsp, objectsNeedGetRemote, queryMetas);
        if (remoteGetRc.IsError()) {
            LOG(WARNING) << "ProcessRemoteGetInNotificationImpl failed, rc: " << remoteGetRc.ToString();
            lastRc = std::move(remoteGetRc);
        }

    } while (!objectsNeedGetRemote.empty() && reqTimeoutDuration.CalcRealRemainingTime() > 0);
    for (const auto &obj : objectsNeedGetRemote) {
        rsp.add_failed_object_keys(obj.objectKey);
    }
    return lastRc;
}

Status WorkerOcServiceGetImpl::NotifyRemoteGet(const NotifyRemoteGetReqPb &req, QueryMetaMap queryMetas,
                                               NotifyRemoteGetRspPb &rsp)
{
    std::unordered_map<std::string, uint64_t> object2VersionInLeavingWorker;
    for (int i = 0; i < req.object_keys_size(); ++i) {
        object2VersionInLeavingWorker[req.object_keys(i)] = req.versions(i);
    }

    // Check version consistency and build ReadKey set.
    // If Master's version differs from the leaving worker's version, the object has been
    // modified and the leaving worker's data is stale. Mark it as success (no need to migrate).
    std::set<ReadKey> objectsNeedGetRemote;
    for (const auto &kv : queryMetas) {
        const auto &objectKey = kv.first;
        const auto &meta = kv.second;
        auto versionIt = object2VersionInLeavingWorker.find(objectKey);
        // Fixme: add verification of location info.
        if (versionIt != object2VersionInLeavingWorker.end() && meta.meta().version() != versionIt->second) {
            LOG(INFO) << "[NotifyRemoteGet] Object " << objectKey
                      << " version mismatch, master version: " << meta.meta().version()
                      << ", leaving worker version: " << versionIt->second << ", skip migration";
            continue;
        }
        ReadKey readKey(objectKey, 0, meta.meta().data_size());
        objectsNeedGetRemote.insert(readKey);
    }
    return ProcessRemoteGetInNotification(req, std::move(objectsNeedGetRemote), queryMetas, rsp);
}
}  // namespace object_cache
}  // namespace datasystem
