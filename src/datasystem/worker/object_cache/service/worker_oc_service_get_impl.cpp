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
#include <chrono>
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
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/metrics.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
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

DS_DECLARE_string(cluster_name);
DS_DECLARE_bool(oc_io_from_l2cache_need_metadata);
DS_DECLARE_bool(authorization_enable);
DS_DECLARE_bool(enable_data_replication);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);
DS_DEFINE_bool(enable_l2_cache_fallback, true, "Control whether enable fallback to L2 cache when worker failed.");
using namespace datasystem::worker;
using namespace datasystem::master;
namespace datasystem {
namespace object_cache {

static constexpr int DEBUG_LOG_LEVEL = 2;
static constexpr uint32_t K_URMA_WARNING_LOG_EVERY_N = 100;
static constexpr double EXIST_LOCAL_CHECK_TIMEOUT_US = 50.0;

static constexpr double US_PER_MS = 1000.0;

WorkerOcServiceGetImpl::WorkerOcServiceGetImpl(WorkerOcServiceCrudParam &initParam, ClusterManager *clusterManager,
                                               EtcdStore *etcdStore, std::shared_ptr<ThreadPool> memCpyThreadPool,
                                               std::shared_ptr<ThreadPool> threadPool,
                                               std::shared_ptr<AkSkManager> akSkManager, HostPort localAddress,
                                               std::shared_ptr<MigrateDataRateController> rateController)
    : WorkerOcServiceCrudCommonApi(initParam),
      clusterManager_(clusterManager),
      etcdStore_(etcdStore),
      memCpyThreadPool_(std::move(memCpyThreadPool)),
      threadPool_(std::move(threadPool)),
      akSkManager_(std::move(akSkManager)),
      localAddress_(std::move(localAddress)),
      rateController_(std::move(rateController))
{
    remoteGetThreadPool_ = std::make_unique<ThreadPool>(1, FLAGS_rpc_thread_num, "RemoteGetThreadPool");
    workerBatchQueryMetaThreadPool_ = std::make_unique<ThreadPool>(1, FLAGS_rpc_thread_num, "BatchQureyMeta");
    if (FLAGS_enable_worker_worker_batch_get) {
        workerBatchRemoteGetThreadPool_ = std::make_unique<ThreadPool>(1, FLAGS_rpc_thread_num, "BatchRemoteGet");
    }
}

Status WorkerOcServiceGetImpl::Get(std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi)
{
    PerfPoint point(PerfKey::WORKER_GET_OBJECT);
    ScopedRequestContext ctx;
    Timer timer;
    const bool traceEnabled = ShouldCollectLatencyTrace(GetServerLatencyTraceConfig());
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_GET_START);
    }
    auto request = std::make_shared<GetRequest>(AccessRecorderKey::DS_POSIX_GET);
    INJECT_POINT("WorkerOCServiceImpl.Get.Retry",
                 [&serverApi]() { return serverApi->SendStatus(Status(K_TRY_AGAIN, "test get retry")); });
    GetReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    auto clientId = ClientKey::Intern(req.client_id());
    auto inflightGauge =
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_INFLIGHT_REMOTE_GET_REQUEST));
    VLOG(1) << FormatString("[Get] Receive, clientId: %s, serverApiReadCost: %.3fms, inflightRemoteGet: %d", clientId,
                            timer.ElapsedMilliSecond(), inflightGauge.Get());
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
        const std::chrono::steady_clock::time_point submitToPool = std::chrono::steady_clock::now();
        auto traceContext = Trace::Instance().GetContext();
        auto cost = GetWorkerTimeCost();
        threadPool_->Execute([=]() mutable {
            ScopedRequestContext ctx;
            GetWorkerTimeCost() = cost;
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
            const std::chrono::steady_clock::time_point poolThreadStart = std::chrono::steady_clock::now();
            {
                const uint64_t qUs = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(poolThreadStart - submitToPool).count());
                metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_GET_THREADPOOL_QUEUE_LATENCY))
                    .Observe(qUs);
            }
            auto elapsed = static_cast<int64_t>(timer.ElapsedMilliSecond());
            auto vlogLevel = elapsed > 1 ? 0 : 1;
            VLOG(vlogLevel) << FormatString(
                "[Get] Receive, clientId: %s, objects: %s, "
                "threadPool: %s, elapsed: %.3fms, remainingTime: %.3fms",
                clientId, VectorToString(request->GetRawObjectKeys()), threadPool_->GetStatistics(),
                static_cast<double>(elapsed), static_cast<double>(timeout));
            if (elapsed >= timeout) {
                LOG(ERROR) << "RPC timeout. time elapsed " << elapsed << ", subTimeout:" << subTimeout
                           << ", get threads Statistics: " << threadPool_->GetStatistics();
                LOG_IF_ERROR(serverApi->SendStatus(Status(K_RUNTIME_ERROR, "Rpc timeout")), "Send status failed");
            } else {
                reqTimeoutDuration.Init(timeout - elapsed);
                auto newSubTimeout = std::max<int64_t>(subTimeout - elapsed, 0);
                const std::chrono::steady_clock::time_point processStart = std::chrono::steady_clock::now();
                LOG_IF_ERROR(ProcessGetObjectRequest(newSubTimeout, request), "Process Get failed");
                {
                    const uint64_t execUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                      std::chrono::steady_clock::now() - processStart)
                                                                      .count());
                    const uint64_t e2EUs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                     std::chrono::steady_clock::now() - submitToPool)
                                                                     .count());
                    metrics::GetHistogram(
                        static_cast<uint16_t>(metrics::KvMetricId::WORKER_GET_THREADPOOL_EXEC_LATENCY))
                        .Observe(execUs);
                    metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_PROCESS_GET_LATENCY))
                        .Observe(e2EUs);
                }
                auto getElapsedMs = timer.ElapsedMilliSecond();
                GetWorkerTimeCost().Append("ProcessGetObjectRequest", getElapsedMs);
                auto inflightGaugeGet =
                    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_INFLIGHT_REMOTE_GET_REQUEST));
                LOG(INFO) << FormatString(
                    "[Get] Done, clientId: %s, objects: %zu, transferPath: %s, totalCost: %.3fms, inflightRemoteGet: "
                    "%d %s",
                    clientId, request->GetRawObjectKeys().size(),
                    IsUrmaEnabled() ? "UB" : (IsUcpEnabled() ? "RDMA" : "TCP"), getElapsedMs, inflightGauge.Get(),
                    GetWorkerTimeCost().GetInfo());
            }
        });
    } else {
        {
            const std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
            LOG_IF_ERROR(ProcessGetObjectRequest(subTimeout, request), "Process Get failed");
            {
                const uint64_t e2EUs = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0)
                        .count());
                metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_GET_THREADPOOL_EXEC_LATENCY))
                    .Observe(e2EUs);
                metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_PROCESS_GET_LATENCY))
                    .Observe(e2EUs);
            }
        }
        auto getElapsedMs = timer.ElapsedMilliSecond();
        GetWorkerTimeCost().Append("ProcessGetObjectRequest", getElapsedMs);
        auto inflightGaugeGet =
            metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_INFLIGHT_REMOTE_GET_REQUEST));
        LOG(INFO) << FormatString(
            "[Get] Done, clientId: %s, objects: %zu, transferPath: %s, totalCost: %.3fms, inflightRemoteGet: "
            "%d %s",
            clientId, request->GetRawObjectKeys().size(), IsUrmaEnabled() ? "UB" : (IsUcpEnabled() ? "RDMA" : "TCP"),
            getElapsedMs, inflightGauge.Get(), GetWorkerTimeCost().GetInfo());
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
            auto config = GetServerLatencyTraceConfig();
            const bool traceEnabled = ShouldCollectLatencyTrace(config);
            TryGetFromL2CacheWhenNotFoundInWorker(meta, address, true, objectKV, rc, traceEnabled);
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
    auto config = GetServerLatencyTraceConfig();
    INJECT_POINT("worker.Get.asyncGetStart", [](int timeout) {
        reqTimeoutDuration.Init(timeout);
        return Status::OK();
    });
    INJECT_POINT("worker.Get.delay");
    PerfPoint point(PerfKey::WORKER_PROCESS_GET_FROM_LOCAL);
    // Try get from local.
    std::set<ReadKey> remoteObjectKeys;
    Timer localProcessingTimer;
    Status localRc = TryGetObjectFromLocal(request, remoteObjectKeys);
    const auto localProcessingUs = static_cast<uint64_t>(localProcessingTimer.ElapsedMicroSecond());
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && localProcessingUs >= config.processSlowerThanUs, 1,
                        FormatString("[Get] Local processing done, clientId: %s, objects: %zu, remoteObjects: %zu, "
                                     "costUs: %zu, rc: %s",
                                     request->GetClientId(), request->GetRawObjectKeys().size(),
                                     remoteObjectKeys.size(), localProcessingUs, localRc.ToString()));
    RETURN_IF_NOT_OK(localRc);
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
    auto traceContext = Trace::Instance().GetContext();
    auto weakThis = weak_from_this();
    // For exclusive connections: inform parent that an async child is deployed
    request->GetServerApi()->SetRequestInProgress();
    RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
        std::min<int64_t>(subTimeout, remainingTimeMs),
        [weakThis, subTimeout, request, traceContext, remainingTimeMs]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
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
    PerfPoint point(PerfKey::WORKER_PROCESS_GET_FROM_LOCAL_BATCH);
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
                // submit local pipeline rh2d step2
                if (remoteObjectKeys.find(readKey) == remoteObjectKeys.end()) {
                    RETURN_IF_NOT_OK(OsXprtPipln::TriggerLocalPipelineRH2D(
                        request->GetH2DChunkManager(), objectKey, objectInfo.params->shmUnit,
                        readKey.readOffset + objectInfo.params->metaSize, objectInfo.params->dataSize));
                }
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
        auto traceContext = Trace::Instance().GetContext();
        threadPool_->Execute([ids = std::move(objectIds), traceContext, this]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
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
        if (reqTimeoutDuration.CalcRealRemainingTime() <= 0) {
            LOG(WARNING) << "Stop retry delete unacked object meta due to exhausted timeout, remaining size: "
                         << objKeys.size();
            break;
        }
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
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
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
    Timer queryMetaTimer;
    QueryMetadataFromMasterResult queryMetaResult;
    std::vector<master::QueryMetaInfoPb> &queryMetas = queryMetaResult.queryMetas;
    std::vector<RpcMessage> &payloads = queryMetaResult.payloads;
    const auto &absentObjectKeys = queryMetaResult.absentObjectKeysWithVersion;
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_QUERYMETA_START);
    }
    Status result =
        QueryMetadataFromMaster(needRemoteGetObjects, subTimeout, queryMetaResult, !request->NoQueryL2Cache());
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_QUERYMETA_END);
    }
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
    Timer postQueryMetaPhase;

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
    const auto queryMetaUs = static_cast<uint64_t>(queryMetaTimer.ElapsedMicroSecond());
    const double queryMetaMs = static_cast<double>(queryMetaUs) / US_PER_MS;
    SLOW_LOG_IF_OR_VLOG(INFO, config.rpcSlowerThanUs > 0 && queryMetaUs >= config.rpcSlowerThanUs, 1,
                        FormatString("[Get] Master query done, targets: %d, hits: %d, cost: %.3fms",
                                     objectsNeedGetRemote.size(), queryMetas.size(), queryMetaMs));
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
    {
        const uint64_t us = static_cast<uint64_t>(postQueryMetaPhase.ElapsedMicroSecond());
        metrics::GetHistogram(static_cast<uint16_t>(metrics::KvMetricId::WORKER_GET_POST_QUERY_META_PHASE_LATENCY))
            .Observe(us);
    }

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

Status WorkerOcServiceGetImpl::WarmupGetObjectFromRemoteWorker(const std::string &address, const std::string &objectKey,
                                                               uint64_t dataSize)
{
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_INVALID, "URMA warmup remote address is empty.");
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "URMA warmup object key is empty.");
    if (address == localAddress_.ToString()) {
        return Status::OK();
    }
    INJECT_POINT("WorkerOcServiceGetImpl.WarmupGetObjectFromRemoteWorker.begin");
    std::shared_ptr<SafeObjType> entry;
    bool isInsert = false;
    RETURN_IF_NOT_OK(RetryWhenDeadlock([this, &objectKey, &entry, &isInsert] {
        return objectTable_->ReserveGetAndLock(objectKey, entry, isInsert, false, true);
    }));
    (void)isInsert;
    Raii unlock([&entry]() { entry->WUnlock(); });
    Raii cleanup([this, &entry, &objectKey]() {
        (void)evictionManager_->Erase(objectKey);
        LOG_IF_ERROR(objectTable_->Erase(objectKey, *entry),
                     FormatString("[URMA_WARMUP] erase local temp warmup object failed, objectKey=%s", objectKey));
    });
    SetNewObjectEntry(objectKey, ConsistencyType::CAUSAL, WriteMode::NONE_L2_CACHE, CacheType::MEMORY, dataSize,
                      GetMetadataSize(), *entry);
    ReadKey readKey(objectKey, 0, dataSize);
    ReadObjectKV objectKV(readKey, *entry);
    auto rc = GetObjectFromRemoteWorkerWithoutDump(address, "", dataSize, objectKV);
    return rc;
}

Status WorkerOcServiceGetImpl::ProcessObjectEntryAndSyncMetadata(ReadObjectKV &objectKV, bool isAsyncBatchGet)
{
    auto &entry = objectKV.GetObjEntry();
    uint64_t version = entry->GetCreateTime();
    if (entry->stateInfo.IsIncomplete()) {
        return Status::OK();
    }
    // RH2D keeps remote host info only, without local shm data this worker cannot serve as a future copy provider.
    if (entry->GetShmUnit() == nullptr) {
        VLOG(1) << FormatString("[ObjectKey %s] Skip sync copy metadata because local shm data is non-existent.",
                                objectKV.GetObjKey());
        return Status::OK();
    }
    entry->stateInfo.SetNeedToDelete(false);
    const auto &objectKey = objectKV.GetObjKey();
    if (isAsyncBatchGet) {
        VLOG(1) << FormatString("[ObjectKey %s] Will be updateLocation in batch later.", objectKey);
        return Status::OK();
    }
    HostPort masterHostAddress;
    // If we can't connect to the master, then we can't update the location to the master so just set the delete flag
    // and return.
    if (GetMetaAddress(objectKey, masterHostAddress).IsError()) {
        LOG(WARNING) << "Can't connect with master " << masterHostAddress.ToString()
                     << ". Data will be automatically deleted after it is returned.";
        entry->stateInfo.SetNeedToDelete(true);
        return Status::OK();
    }
    VLOG(1) << "Sync meta data to master as an object data copy provider.";
    UpdateLocationParam param = { objectKey, version, static_cast<uint32_t>(entry->stateInfo.GetDataFormat()),
                                  Trace::Instance().GetTraceID() };
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
        RETURN_IF_NOT_OK(FillRequestUrmaInfo(localAddress_, shmUnit->GetPointer(), shmUnit->GetOffset(), metaSz, reqPb,
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
    if (clusterManager_ != nullptr) {
        auto workerId = clusterManager_->GetWorkerIdByWorkerAddr(endPoint);
        if (!workerId.empty()) {
            remoteWorkerId = workerId;
        }
    }
    LOG_FIRST_AND_EVERY_N(WARNING, K_URMA_WARNING_LOG_EVERY_N)
        << "[URMA_NEED_CONNECT] TryReconnectRemoteWorker triggered, remoteAddress=" << endPoint
        << ", remoteWorkerId=" << remoteWorkerId
        << ", realRemainingTimeMs=" << reqTimeoutDuration.CalcRealRemainingTime()
        << ", lastResult=" << lastResult.ToString();

    HostPort hostAddress;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(hostAddress.ParseString(endPoint), "ParseString failed");
    Timer reconnectTimer;

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
    auto rc = constAccApi->second->ExecOnceParrallelExchange(dummyRsp);
    auto elapsedMs = reconnectTimer.ElapsedMilliSecond();
    static const int logThresholdMs = 2'000;
    LOG_IF(INFO, elapsedMs > logThresholdMs)
        << "[URMA_NEED_CONNECT] TryReconnectRemoteWorker finished, remoteAddress=" << endPoint
        << ", remoteWorkerId=" << remoteWorkerId << ", elapsed ms: " << elapsedMs
        << ", realRemainingTimeMs=" << reqTimeoutDuration.CalcRealRemainingTime() << ", status=" << rc.ToString();
    RETURN_IF_NOT_OK(rc);
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
    if (!IsRemoteH2DEnabled() || request == nullptr || request->GetClientCommUuid().empty()) {
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

        // start pipeline receiver and prepare pipeline h2d tag
        if (request) {
            OsXprtPipln::TriggerRemotePipelineRH2D(request->GetH2DChunkManager(), objectKV.GetObjKey(),
                                                   objectKV.GetReadOffset() + objectKV.GetObjEntry()->GetMetadataSize(),
                                                   objectKV.GetReadSize(), objectKV.GetObjEntry()->GetShmUnit(),
                                                   address, subReq);
        }

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
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_REMOTEGET_START);
    }
    CHECK_FAIL_RETURN_STATUS(address != localAddress_.ToString(), StatusCode::K_RUNTIME_ERROR,
                             "Remote getting from self address is invalid");
    auto version = objectKV.GetObjEntry()->GetCreateTime();
    const std::string requestId = GetStringUuid();
    LOG(INFO) << AppendSrcDstForLog(
        FormatString("Remote get request:[%s] object:[%s], offset[%lld] size[%lld]", requestId, objectKV.GetObjKey(),
                     objectKV.GetReadOffset(), objectKV.GetReadSize()),
        localAddress_.ToString(), address);
    INJECT_POINT("worker.remote_get_failed");
    std::shared_ptr<WorkerRemoteWorkerOCApi> workerStub;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateRemoteWorkerApi(address, localAddress_, akSkManager_, workerStub),
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
    Timer remoteGetTimer;
    const int32_t minRetryOnceRpcMs = 1;  // The 1st level of retryIntervalsMs
    std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> clientApi;
    do {
        dataSizeChange = false;
        bool shmUnitAllocated = false;
        // Prepare the protobuf with urma/ucp info for data transfer if applicable.
        RETURN_IF_NOT_OK(PrepareGetRequestHelper(address, dataSize, objectKV, reqPb, shmUnitAllocated));
        int64_t timeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
        INJECT_POINT("worker_oc_service_get_impl.pull_object_data_from_remote_worker.before_get_from_remote");
        Status rc = RetryOnErrorRepent(
            timeoutMs,
            [&workerStub, &reqPb, &rspPb, &clientApi, &address, this](int32_t) {
                RETURN_IF_NOT_OK(workerStub->GetObjectRemote(&clientApi));
                RETURN_IF_NOT_OK(workerStub->GetObjectRemoteWrite(clientApi, reqPb));

                auto rc = clientApi->Read(rspPb);
                if (rc.IsError()) {
                    rc = WithRpcDiag(rc, "GetObjectRemote", localAddress_, address);
                }
                RETURN_IF_NOT_OK(TryReconnectRemoteWorker(address, rc));
                return Status::OK();
            },
            []() { return Status::OK(); },
            { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
              StatusCode::K_RPC_UNAVAILABLE, StatusCode::K_URMA_WAIT_TIMEOUT },
            minRetryOnceRpcMs);
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
        RETURN_IF_NOT_OK(RetrieveRemotePayload(address, objectKV, clientApi, rspPb));
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
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_REMOTEGET_END);
    }
    if (traceEnabled && rspPb.latency_phase_us_size() > 0) {
        std::vector<uint32_t> phases(rspPb.latency_phase_us().begin(), rspPb.latency_phase_us().end());
        MergeDecodedPhasesToTrace(phases, rspPb.latency_tick_dropped_count());
    }
    rpcPoint.Record();
    const auto remoteGetUs = static_cast<uint64_t>(remoteGetTimer.ElapsedMicroSecond());
    const double remoteGetMs = static_cast<double>(remoteGetUs) / US_PER_MS;
    SLOW_LOG_IF_OR_VLOG(
        INFO, config.rpcSlowerThanUs > 0 && remoteGetUs >= config.rpcSlowerThanUs, 1,
        AppendSrcDstForLog(
            FormatString("Remote get success, objectKey: %s, path: %s, cost: %.3fms", objectKV.GetObjKey(),
                         IsUrmaEnabled() ? "UB" : (IsUcpEnabled() ? "RDMA" : "TCP"), remoteGetMs),
            localAddress_.ToString(), address));
    return Status::OK();
}

Status WorkerOcServiceGetImpl::RetrieveRemotePayload(
    const std::string &address, ReadObjectKV &objectKV,
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
        if (status.IsError()) {
            status = WithRpcDiag(status, "GetObjectRemote", localAddress_, address);
        }
    } else {
        // Downlevel client.
        auto f = [this, &address, &clientApi, &entry, &needReceiveSz, &metaSz, offset]() {
            std::vector<RpcMessage> payloads;
            auto rc = clientApi->ReceivePayload(payloads);
            if (rc.IsError()) {
                rc = WithRpcDiag(rc, "GetObjectRemote", localAddress_, address);
            }
            RETURN_IF_NOT_OK(rc);
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
                                                           datasystem::master::QueryMetaRspPb &rsp,
                                                           std::vector<RpcMessage> &payloads)
{
    PerfPoint point(PerfKey::WORKER_QUERY_META_IMPL);
    datasystem::master::QueryMetaReqPb req;
    SetQueryMetaInfo(req, objKeysToQuery, destMasterHostPort.ToString(), true);
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
    RETURN_IF_NOT_OK(QueryMetadataFromRedirectMaster(rsp, subTimeout, payloads));
    return Status::OK();
}

void WorkerOcServiceGetImpl::ProcessQueryMetaFailedObjsWhenMetaStoredInEtcd(
    const std::unordered_set<std::string> &routeFailedObjectKeys, std::unordered_set<std::string> &&objectKeysNotExist,
    const std::unordered_set<std::string> &objectKeysPuzzled, std::vector<master::QueryMetaInfoPb> &queryMetas,
    std::vector<std::string> &absentObjectKeys)
{
    absentObjectKeys.insert(absentObjectKeys.end(), objectKeysNotExist.begin(), objectKeysNotExist.end());
    if (routeFailedObjectKeys.empty() && objectKeysPuzzled.empty()) {
        return;
    }

    std::stringstream msg;
    msg << "Try get some miss objs from etcd:" << VectorToString(objectKeysPuzzled);
    msg << ", route failed objs:";
    msg << VectorToString(routeFailedObjectKeys);
    LOG(INFO) << msg.str();

    if (!objectKeysPuzzled.empty()) {
        LOG_IF_ERROR(QueryMetaDataFromEtcd(objectKeysPuzzled, queryMetas, absentObjectKeys),
                     "Query metadata from etcd for puzzled keys failed.");
    }
    if (!routeFailedObjectKeys.empty()) {
        LOG_IF_ERROR(QueryMetaDataFromEtcd(routeFailedObjectKeys, queryMetas, absentObjectKeys),
                     "Query metadata from etcd for route failed keys failed.");
    }
}

Status WorkerOcServiceGetImpl::QueryMetadataFromMaster(const std::vector<std::string> &objectKeys, uint64_t subTimeout,
                                                       QueryMetadataFromMasterResult &result, bool queryEtcdMeta)
{
    PerfPoint point(PerfKey::WORKER_QUERY_META_PRE);
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    Status lastRc;
    std::vector<master::QueryMetaInfoPb> &queryMetas = result.queryMetas;
    std::vector<RpcMessage> &payloads = result.payloads;
    std::map<std::string, uint64_t> &absentObjectKeysWithVersion = result.absentObjectKeysWithVersion;
    INJECT_POINT("worker.before_query_meta");
    // 1. Get map of objectKeys grouped by master
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_ROUTER);
    auto grouped = clusterManager_->GroupKeysByMetaOwner(objectKeys);
    auto &objKeysGrpByMaster = grouped.groups;
    std::unordered_set<std::string> routeFailedObjectKeys;
    routeFailedObjectKeys.reserve(grouped.failures.size());
    for (const auto &failure : grouped.failures) {
        routeFailedObjectKeys.emplace(failure.first);
    }
    // 2. Send requests for each master
    std::vector<std::future<Status>> futures;
    futures.reserve(objKeysGrpByMaster.size());
    std::string traceID = Trace::Instance().GetTraceID();
    Timer timer;
    int64_t realTimeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    std::vector<BatchQueryMetaResult> batchQueryResults;
    batchQueryResults.resize(objKeysGrpByMaster.size());
    size_t idx = 0;
    point.RecordAndReset(PerfKey::WORKER_QUERY_META_BATCH_BY_ADDR);
    queryMetas.reserve(queryMetas.size() + objectKeys.size());
    for (auto &item : objKeysGrpByMaster) {
        BatchQueryMetaResult &res = batchQueryResults[idx++];
        auto *itemPtr = &item;
        auto func = [&res, realTimeoutMs, subTimeout, itemPtr, &traceID, &timer, this]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID, true);
            int64_t elapsed = timer.ElapsedMilliSecond();
            reqTimeoutDuration.Init(realTimeoutMs - elapsed);
            HostPort masterAddr = itemPtr->first.GetAddress();
            const std::vector<std::string> &currentIds = itemPtr->second;
            datasystem::master::QueryMetaRspPb &rsp = res.rsp;
            Timer queryMetaTimer;
            auto rc = QueryMetaDataFromMasterImpl(masterAddr, subTimeout, currentIds, rsp, res.payloads);
            if (rc.IsError()) {
                LOG(ERROR) << FormatString("Query metadata from master[%s]: %s, elapsed %.3f ms", masterAddr.ToString(),
                                           rc.ToString(), queryMetaTimer.ElapsedMilliSecond());
                res.failedKeys.insert(currentIds.begin(), currentIds.end());
                return rc;
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
    absentObjectKeys.reserve(objectKeys.size());
    std::map<std::string, uint64_t> deletingObjectsWithVersion;
    for (auto &res : batchQueryResults) {
        if (!res.failedKeys.empty()) {
            objectKeysPuzzled.insert(res.failedKeys.begin(), res.failedKeys.end());
            continue;
        }
        auto &rsp = res.rsp;
        if (traceEnabled && rsp.latency_phase_us_size() > 0) {
            std::vector<uint32_t> phases(rsp.latency_phase_us().begin(), rsp.latency_phase_us().end());
            MergeDecodedPhasesToTrace(phases, rsp.latency_tick_dropped_count());
        }
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

    point.RecordAndReset(PerfKey::WORKER_QUERY_META_HANDLE_NOT_FOUND);
    // 4. If etcd is used as L2cache for metadata, try to get miss meta from etcd.
    bool metaStoredInEtcd = FLAGS_oc_io_from_l2cache_need_metadata;
    // If l2cache is disabled, there is no need to query meta in etcd.
    bool isL2CacheDisable = FLAGS_l2_cache_type == "none" || FLAGS_l2_cache_type == "distributed_disk";
    if (metaStoredInEtcd && queryEtcdMeta && !isL2CacheDisable) {
        ProcessQueryMetaFailedObjsWhenMetaStoredInEtcd(routeFailedObjectKeys, std::move(objectKeysNotExist),
                                                       objectKeysPuzzled, queryMetas, absentObjectKeys);
    } else {
        absentObjectKeys.insert(absentObjectKeys.end(), objectKeysNotExist.begin(), objectKeysNotExist.end());
        absentObjectKeys.insert(absentObjectKeys.end(), objectKeysPuzzled.begin(), objectKeysPuzzled.end());
        absentObjectKeys.insert(absentObjectKeys.end(), routeFailedObjectKeys.begin(), routeFailedObjectKeys.end());
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
                                                               std::vector<RpcMessage> &payloads)
{
    for (const auto &redirectInfo : rsp.info()) {
        std::vector<RpcMessage> redirectPayloads;
        master::QueryMetaReqPb redirectQueryReq;
        master::QueryMetaRspPb redirectQueryRsp;
        std::vector<std::string> redirectIds = { redirectInfo.change_meta_ids().begin(),
                                                 redirectInfo.change_meta_ids().end() };
        HostPort redirectMasterAddr;
        RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr));
        SetQueryMetaInfo(redirectQueryReq, redirectIds, redirectMasterAddr.ToString(), false);
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
 * Query missing metadata from ETCD by complete object-key hash.
 */
Status WorkerOcServiceGetImpl::QueryMetaDataFromEtcd(const std::unordered_set<std::string> &objectKeys,
                                                     std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                     std::vector<std::string> &absentObjectKeys)
{
    INJECT_POINT("worker.QueryMetaDataFromEtcd_failure");
    for (const std::string &objKey : objectKeys) {
        std::string etcdTableName = std::string(ETCD_META_TABLE_PREFIX) + ETCD_HASH_SUFFIX;
        std::string hashValue = Hash2Str(MurmurHash3_32(objKey));
        std::string tablePrefix;
        if (!FLAGS_cluster_name.empty()) {
            tablePrefix = FormatString("/%s", FLAGS_cluster_name);
        }
        std::string etcdKey = tablePrefix + FormatString("%s/%zu/%s", etcdTableName, hashValue, objKey);
        LOG(INFO) << "Query objKey: " << objKey << ", AZ name: " << FLAGS_cluster_name
                  << ", query ETCD key: " << etcdKey;

        auto metaPb = std::make_unique<ObjectMetaPb>();
        CHECK_FAIL_RETURN_STATUS(!etcdStore_->IsKeepAliveTimeout(), K_RPC_UNAVAILABLE, "etcd is unavailable");
        RangeSearchResult res;
        Status rc = etcdStore_->RawGet(etcdKey, res, 0, reqTimeoutDuration.CalcRemainingTime());
        if (rc.IsError()) {
            LOG(ERROR) << "Can not get meta: " << rc.ToString();
            absentObjectKeys.emplace_back(objKey);
            continue;
        }
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            metaPb->ParseFromString(res.value), StatusCode::K_RUNTIME_ERROR,
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
    }
    return Status::OK();
}

void WorkerOcServiceGetImpl::SetQueryMetaInfo(master::QueryMetaReqPb &req, const std::vector<std::string> &objectKeys,
                                              const std::string &masterAddr, bool redirect)
{
    // Get master addr successfully, query metadata by WorkerMastrOCApi.
    const std::string queryReqId = GetStringUuid();
    int64_t remainingTime = reqTimeoutDuration.CalcRealRemainingTime();
    LOG(INFO) << "Query metadata from master: " << masterAddr << ", object_count: " << objectKeys.size()
              << ", request id: " << queryReqId << ", remainingTime: " << remainingTime;
    *req.mutable_ids() = { objectKeys.begin(), objectKeys.end() };
    req.set_request_id(queryReqId);
    req.set_redirect(redirect);
    req.set_address(localAddress_.ToString());
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
    PerfPoint totalPoint(PerfKey::WORKER_GET_OBJECTS_FROM_ANYWHERE);
    if (FLAGS_enable_worker_worker_batch_get) {
        PerfPoint stagePoint(PerfKey::WORKER_GET_OBJECTS_FROM_ANYWHERE_BATCH);
        return GetObjectsFromAnywhereBatched(queryMetas, request, payloads, lockedEntries, failedIds, needRetryIds);
    }
    PerfPoint stagePoint(PerfKey::WORKER_GET_OBJECTS_FROM_ANYWHERE_PARALLEL);
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
    auto traceContext = Trace::Instance().GetContext();
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
                                                           &failedIds]() {
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext);
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
            Timer getFromRemoteTimer;
            Status status = GetObjectFromAnywhereWithLock(readKey, request, subEntry, isInsert, queryMeta, payloads);
            double remoteElapsed = getFromRemoteTimer.ElapsedMilliSecond();

            // Protects access to successIds, needRetryIds, failedIds, and lastRc
            std::lock_guard<std::mutex> lock(commonMutex);

            if (status.IsOk()) {
                LOG(INFO) << FormatString("[ObjectKey %s] Get from remote success, elapsed %.3f ms.", objectKey,
                                          remoteElapsed);
                successIds.push_back(objectKey);
                return status;
            }
            failedIds.emplace(objectKey);
            if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
                LOG(INFO) << FormatString("[ObjectKey %s] Object not found in remote worker, elapsed %.3f ms.",
                                          objectKey, remoteElapsed);
                (void)needRetryIds.emplace(readKey);
            } else if (status.GetCode() == K_OUT_OF_MEMORY) {
                LOG(INFO) << FormatString("[ObjectKey %s] Out of memory, get remote abort, elapsed %.3f ms.", objectKey,
                                          remoteElapsed);
                lastRc = status;
                abortAllTasks.store(true);
            } else {
                LOG(ERROR) << FormatString("[ObjectKey %s] Get from remote failed: %s, elapsed %.3f ms.", objectKey,
                                           status.ToString(), remoteElapsed);
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
        Timer getFromRemoteTimer;
        auto status = GetObjectFromAnywhereWithLock(readKey, request, iter->second.safeObj, iter->second.insert,
                                                    queryMeta, payloads);
        double remoteElapsed = getFromRemoteTimer.ElapsedMilliSecond();
        if (status.IsOk()) {
            LOG(INFO) << FormatString("[ObjectKey %s] Get from remote success, elapsed %.3f ms.", objectKey,
                                      remoteElapsed);
            successIds.push_back(objectKey);
            continue;
        }
        failedIds.emplace(objectKey);
        if (status.GetCode() == K_WORKER_PULL_OBJECT_NOT_FOUND) {
            LOG(INFO) << FormatString("[ObjectKey %s] Object not found in remote worker, elapsed %.3f ms.", objectKey,
                                      remoteElapsed);
            lastRc = Status::OK();
            (void)needRetryIds.emplace(readKey);
        } else if (status.GetCode() == K_OUT_OF_MEMORY) {
            LOG(INFO) << FormatString("[ObjectKey %s] Out of memory, get remote abort, elapsed %.3f ms.", objectKey,
                                      remoteElapsed);
            lastRc = status;
            break;
        } else {
            LOG(ERROR) << FormatString("[ObjectKey %s] Get from remote failed: %s, elapsed %.3f ms.", objectKey,
                                       status.ToString(), remoteElapsed);
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
    VLOG(1) << "Get object from remote, object meta is: " << LogHelper::IgnoreSensitive(meta) << ", payload_index_size"
            << queryMeta.payload_indexs_size() << ", addr: " << address
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
    const bool traceEnabled = ShouldCollectLatencyTrace(GetServerLatencyTraceConfig());
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
            clusterManager_->CheckConnection(hostAddr, ToleranceNotExistNode(singleCopy, meta.config().write_mode()));
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
    } else {
        status = Status(K_RUNTIME_ERROR,
                        FormatString("Fail to get object %s from remote worker, no object copy exists.", objKey));
    }
    bool isFromL2 = false;
    // Step3: Try to get data from
    if (status.IsError()) {
        Timer timer;
        TryGetFromL2CacheWhenNotFoundInWorker(meta, address, ifWorkerConnected, objectKV, status, traceEnabled);
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

void WorkerOcServiceGetImpl::TryGetFromL2CacheWhenNotFoundInWorker(const ObjectMetaPb &meta, const std::string &address,
                                                                   bool ifWorkerConnected, ObjectKV &objectKV,
                                                                   Status &status, bool traceEnabled)
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
        if (traceEnabled) {
            Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_L2CACHE_READ_START);
        }
        if (isQueryWithoutCopy) {
            status = GetObjectFromPersistenceAndDumpWithoutCopyMeta(objectKV);
        } else {
            status = GetObjectFromPersistenceAndDump(objectKV);
        }
        if (traceEnabled) {
            Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_L2CACHE_READ_END);
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
    if (FLAGS_enable_data_replication) {
        UpdateLocationParam param = { objectKey, objectKV.GetObjEntry()->GetCreateTime(),
                                      static_cast<uint32_t>(objectKV.GetObjEntry()->stateInfo.GetDataFormat()),
                                      Trace::Instance().GetTraceID() };
        UpdateLocationTask task = UpdateLocationTask(param);
        RETURN_IF_NOT_OK(asyncUpdateLocationManager_->AddTask(std::move(task)));
        evictionManager_->Add(objectKey);
    } else {
        objectKV.GetObjEntry()->stateInfo.SetNeedToDelete(true);
    }
    point.Record();
    CacheHitInfo::Instance().IncRemoteHit(1);
    return UpdateRequestForSuccess(objectKV, request);
}

void WorkerOcServiceGetImpl::AsyncUpdateLocationFunc(UpdateLocationTask &&task)
{
    // The async update location worker thread does not inherit the trace ID from
    // the handler that dispatched the task. Re-establish it from the first param
    // (captured at AddTask time on the handler thread) so this VLOG and the
    // downstream CreateCopyMeta RPC carry the original request's trace ID for
    // correlation. Aggregated/batch tasks may merge params with different trace
    // IDs; we pick the first for the entry log, single-param path restores it
    // before the RPC. Use keep=true so the scope guard does not clear the trace
    // for sibling work on this thread.
    const auto &params = task.GetParams();
    if (!params.empty() && !params.front().traceID.empty()) {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(params.front().traceID, true);
        (void)traceGuard;
    }
    VLOG(1) << "AsyncUpdateLocationFunc, the size is " << params.size();
    if (params.empty()) {
        return;
    }
    const int64_t updateLocationTimeoutMs = 3000;
    reqTimeoutDuration.Init(updateLocationTimeoutMs);
    PerfPoint point(PerfKey::WORKER_ASYNC_UPDATE_LOCATION_FUNCTION);
    if (params.size() == 1) {
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
        workerMasterApiManager_->GetWorkerMasterApi(task.GetParams().front().objectKey, clusterManager_);
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
            LOG(WARNING) << FormatString("Add retry task failed: %s", status.ToString());
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
    auto grouped = clusterManager_->GroupKeysByMetaOwner(objectKeys);
    grouped.AppendFailuresToGroup();
    auto &objKeysGrpByMaster = grouped.groups;
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
    int clearCount = 0;
    int failCount = 0;
    for (auto &keyVersion : clearKeyVersions) {
        std::shared_ptr<SafeObjType> entry;
        auto status = objectTable_->Get(keyVersion.first, entry);
        if (status.IsError()) {
            if (status.GetCode() == StatusCode::K_NOT_FOUND) {
                continue;
            }
            failCount++;
            continue;
        }
        Status rc = TryLockWithRetry(keyVersion.first, entry);
        if (rc.IsError()) {
            failCount++;
            continue;
        }
        Raii unlock([&entry]() { entry->WUnlock(); });
        if ((*entry)->GetCreateTime() == keyVersion.second) {
            ObjectKV objectKV(keyVersion.first, *entry);
            auto clearRc = ClearObject(objectKV);
            if (clearRc.IsOk()) {
                clearCount++;
            } else {
                LOG(ERROR) << FormatString("Failed to erase object %s from object table, %s", keyVersion.first,
                                           clearRc.ToString());
            }
        }
    }
    if (failCount > 0) {
        LOG(WARNING) << FormatString("Clear objects by update location: total %d, cleared %d, failed %d",
                                     static_cast<int>(clearKeyVersions.size()), clearCount, failCount);
    } else if (clearCount > 0) {
        LOG_EVERY_N(INFO, 100) << FormatString("Clear objects by update location: total %d, cleared %d",
                                               static_cast<int>(clearKeyVersions.size()), clearCount);
    }
}

Status WorkerOcServiceGetImpl::RemoveLocation(const std::string &objectKey, uint64_t version)
{
    INJECT_POINT("worker.remove_location");
    if (clusterManager_ == nullptr) {
        RETURN_STATUS(StatusCode::K_NOT_FOUND, "ETCD cluster manager is not provided");
    }
    auto api = workerMasterApiManager_->GetWorkerMasterApi(objectKey, clusterManager_);
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
    CHECK_FAIL_RETURN_STATUS(clusterManager_ != nullptr, StatusCode::K_NOT_READY,
                             "ETCD cluster manager is not provided.");
    MetaAddrInfo metaAddrInfo;
    RETURN_IF_NOT_OK(clusterManager_->LocateMetaOwner(objKey, true, metaAddrInfo));
    masterAddr = metaAddrInfo.GetAddress();
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
            meta->add_location_ids(clusterManager_->GetWorkerIdByWorkerAddr(loc));
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

static void MergeObjectLocations(master::GetObjectLocationsRspPb &masterRsp,
                                 std::unordered_map<std::string, ObjectLocationInfoPb> &result)
{
    for (auto &item : *masterRsp.mutable_location_infos()) {
        // Use operator[] to ensure redirect data (authoritative source) overwrites main query result
        result[item.object_key()] = std::move(item);
    }
}

Status WorkerOcServiceGetImpl::QueryObjectLocationsFromRedirectMaster(
    const google::protobuf::RepeatedPtrField<RedirectMetaInfo> &infos,
    std::unordered_map<std::string, ObjectLocationInfoPb> &result, Status &lastRc)
{
    for (const auto &redirectInfo : infos) {
        HostPort redirectMasterAddr;
        Status rc = GetPrimaryReplicaAddr(redirectInfo.redirect_meta_address(), redirectMasterAddr);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Get redirect master address failed: %s", rc.ToString());
            lastRc = rc;
            continue;
        }
        auto redirectWorkerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(redirectMasterAddr);
        if (redirectWorkerMasterApi == nullptr) {
            LOG(ERROR) << FormatString("Get redirect master api failed, address: %s", redirectMasterAddr.ToString());
            lastRc = Status(K_RUNTIME_ERROR, "redirect master get failed");
            continue;
        }
        master::GetObjectLocationsReqPb redirectReq;
        master::GetObjectLocationsRspPb redirectRsp;
        *redirectReq.mutable_object_keys() = { redirectInfo.change_meta_ids().begin(),
                                               redirectInfo.change_meta_ids().end() };
        redirectReq.set_redirect(false);
        rc = redirectWorkerMasterApi->GetObjectLocations(redirectReq, redirectRsp);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Query locations from redirect master %s failed: %s",
                                       redirectMasterAddr.ToString(), rc.ToString());
            lastRc = rc;
            continue;
        }
        MergeObjectLocations(redirectRsp, result);
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetMapOfObjectKeys(const std::vector<std::basic_string<char>> &objectKeys,
                                                  std::unordered_map<std::string, ObjectLocationInfoPb> &result,
                                                  Status &lastRc)
{
    auto grouped = clusterManager_->GroupKeysByMetaOwner(objectKeys);
    auto &objKeysGrpByMaster = grouped.groups;
    for (auto &[master, objs] : objKeysGrpByMaster) {
        HostPort workerAddr = master.GetAddress();
        master::GetObjectLocationsReqPb masterReq;
        master::GetObjectLocationsRspPb masterRsp;
        *masterReq.mutable_object_keys() = { objs.begin(), objs.end() };
        masterReq.set_redirect(true);
        auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(workerAddr);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "hash master get failed, GetObjMetaInfo failed");
        std::function<Status(master::GetObjectLocationsReqPb &, master::GetObjectLocationsRspPb &)> func =
            [&workerMasterApi](master::GetObjectLocationsReqPb &req, master::GetObjectLocationsRspPb &rsp) {
                return workerMasterApi->GetObjectLocations(req, rsp);
            };
        auto status = RedirectRetryWhenMetasMoving(masterReq, masterRsp, func);
        if (status.IsError()) {
            LOG(ERROR) << FormatString("Query locations from %s of %s failed. %s", workerAddr.ToString(),
                                       VectorToString(objs), status.ToString());
            lastRc = status;
            continue;
        }
        MergeObjectLocations(masterRsp, result);
        QueryObjectLocationsFromRedirectMaster(masterRsp.info(), result, lastRc);
    }
    if (result.empty()) {
        return lastRc;
    }
    return Status::OK();
}

Status WorkerOcServiceGetImpl::GetObjMetaInfo(const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp)
{
    ScopedRequestContext ctx;
    INJECT_POINT("worker.GetObjMetaInfo");
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
    ScopedRequestContext ctx;
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
    if (objectTable_->Get(key, entry).IsOk() && entry->TryRLock(false).IsOk()) {
        Raii unlock([&entry]() { entry->RUnlock(); });
        return (*entry)->IsBinary() && !(*entry)->IsInvalid();
    }
    return false;
}

Status WorkerOcServiceGetImpl::Exist(const ExistReqPb &req, ExistRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    PerfPoint point(PerfKey::WORKER_EXIST_PRE_PROCESS);
    auto config = GetServerLatencyTraceConfig();
    const bool traceEnabled = ShouldCollectLatencyTrace(config);
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_EXIST_START);
    }
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_EXIST);
    access.ObjectKeysRef(req.object_keys());
    INJECT_POINT("Exist.Sleep");
    auto clientId = ClientKey::Intern(req.client_id());
    VLOG(1) << "Exist start from client:" << clientId;
    std::string tenantId;
    Status authRc = worker::Authenticate(akSkManager_, req, tenantId);
    if (authRc.IsError()) {
        LOG(ERROR) << "Authenticate failed. Detail: " << authRc.ToString();
        access.Result(authRc).Record();
        return authRc;
    }
    if (!Validator::IsBatchSizeUnderLimit(req.object_keys_size())) {
        LOG(ERROR) << "invalid object size";
        Status rc(StatusCode::K_INVALID, __LINE__, __FILE__, "invalid object size");
        access.Result(rc).Record();
        return rc;
    }
    auto keys = TenantAuthManager::ConstructNamespaceUriWithTenantId(tenantId, req.object_keys());

    point.RecordAndReset(PerfKey::WORKER_EXIST_LOCAL_CHECK);
    std::unordered_set<std::string> existKeys;
    std::vector<std::string> nonLocalKeys;
    existKeys.reserve(keys.size());
    nonLocalKeys.reserve(keys.size());
    Timer localCheckTimer;
    for (auto it = keys.begin(); it != keys.end(); ++it) {
        if (localCheckTimer.ElapsedMicroSecond() >= EXIST_LOCAL_CHECK_TIMEOUT_US) {
            nonLocalKeys.insert(nonLocalKeys.end(), it, keys.end());
            break;
        }
        const auto &key = *it;
        if (!IsLocalObject(key)) {
            nonLocalKeys.emplace_back(key);
            continue;
        }
        evictionManager_->Add(key);
        existKeys.emplace(key);
        INJECT_POINT("worker.Exist.after_local_check");
    }

    Status rc;
    if (!req.is_local() && !nonLocalKeys.empty()) {
        point.RecordAndReset(PerfKey::WORKER_EXIST_QUERY_MASTER);
        if (traceEnabled) {
            Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_EXIST_QUERYMETA_START);
        }
        QueryMetadataFromMasterResult queryResult;
        std::vector<master::QueryMetaInfoPb> &queryMetas = queryResult.queryMetas;
        std::vector<RpcMessage> payloads;
        std::map<std::string, uint64_t> absentObjectKeys;
        rc = QueryMetadataFromMaster(nonLocalKeys, 0, queryResult, req.query_l2cache());
        if (traceEnabled) {
            Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_EXIST_QUERYMETA_END);
        }
        const auto localAddress = localAddress_.ToString();
        for (const auto &meta : queryMetas) {
            // Master skips returning the source worker as a remote address; a single local primary copy still exists.
            const bool isLocalPrimaryCopy = meta.single_copy() && meta.meta().primary_address() == localAddress;
            if (!meta.address().empty() || isLocalPrimaryCopy) {
                existKeys.emplace(meta.meta().object_key());
            }
        }
    }

    point.RecordAndReset(PerfKey::WORKER_EXIST_BUILD_RSP);
    for (const auto &key : keys) {
        rsp.add_exists(existKeys.count(key));
    }

    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(LatencyTickKey::WORKER_EXIST_END);
    }
    const auto totalExistUs = static_cast<uint64_t>(timer.ElapsedMicroSecond());
    if (ShouldPrintLatencySummary(totalExistUs, config)) {
        auto result =
            ComputePhaseDurations(Trace::Instance().GetLatencyTicks(), Trace::Instance().GetLatencyTickCount(),
                                  Trace::Instance().GetLatencyTickDroppedCount());
        auto &downstream = Trace::Instance().GetDownstreamPhases();
        bool hasDownstream = downstream.count > 0;
        if (hasDownstream) {
            for (uint16_t di = 0; di < downstream.count; ++di) {
                result.Add(downstream.entries[di].phase, downstream.entries[di].durationUs);
            }
            result.tickDroppedCount += downstream.tickDroppedCount;
            result.phaseDroppedCount += downstream.phaseDroppedCount;
        }
        bool gateHit = CheckPhaseGate(result, config);
        if (gateHit) {
            Trace::Instance().SetLatencySummary(FormatLatencySummary(result));
        }
        bool shouldEncodeProto = gateHit || hasDownstream;
        if (shouldEncodeProto) {
            auto *phases = rsp.mutable_latency_phase_us();
            for (uint16_t i = 0; i < result.count; ++i) {
                phases->Add(static_cast<unsigned int>(result.entries[i].phase));
                phases->Add(static_cast<unsigned int>(result.entries[i].durationUs));
            }
            if (result.droppedCount() > 0) {
                rsp.set_latency_tick_dropped_count(static_cast<uint32_t>(result.droppedCount()));
            }
        }
    }
    access.Result(rc).Record();
    const double totalExistMs = static_cast<double>(totalExistUs) / US_PER_MS;
    GetWorkerTimeCost().Append("Total Exist", totalExistMs);
    SLOW_LOG_IF_OR_VLOG(INFO, config.processSlowerThanUs > 0 && totalExistUs >= config.processSlowerThanUs, 1,
                        FormatString("Exist done, cost: %.3fms, %s", totalExistMs, GetWorkerTimeCost().GetInfo()));
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
        workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, clusterManager_);
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
    Status &lastRc, NotifyRemoteGetRspPb &rsp, const QueryMetaMap &queryMetas, uint64_t &migratedBytes)
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
    ClearNeedDeleteForMigratedObjects(successIds, lockedEntries);
    BatchUpdateLocationHelper(successIds, queryMetas, lockedEntries);
    for (const auto &objectKey : successIds) {
        auto metaIt = queryMetas.find(objectKey);
        if (metaIt != queryMetas.end()) {
            migratedBytes += metaIt->second.meta().data_size();
        }
    }
}

void WorkerOcServiceGetImpl::ClearNeedDeleteForMigratedObjects(const std::vector<std::string> &successIds,
                                                               std::map<ReadKey, LockedEntity> &lockedEntries)
{
    if (!successIds.empty() && !FLAGS_enable_data_replication) {
        VLOG(1) << FormatString("[NotifyRemoteGet] clear needDelete for %zu migrated objects", successIds.size());
        for (const auto &objectKey : successIds) {
            auto it = lockedEntries.find(ReadKey(objectKey));
            if (it == lockedEntries.end()) {
                continue;
            }
            VLOG(1) << FormatString("[NotifyRemoteGet] clear needDelete for object %s", objectKey);
            it->second.safeObj->Get()->stateInfo.SetNeedToDelete(false);
        }
    }
}

Status WorkerOcServiceGetImpl::ProcessRemoteGetInNotificationImpl(
    std::unordered_map<std::string, std::list<std::pair<std::list<GetObjectInfo>, uint64_t>>> &groupedQueryMetas,
    std::map<ReadKey, LockedEntity> &lockedEntries, NotifyRemoteGetRspPb &rsp, std::set<ReadKey> &objectsNeedGetRemote,
    const QueryMetaMap &queryMetas, uint64_t &migratedBytes)
{
    Status lastRc;
    std::vector<std::future<Status>> futures;
    std::list<GetObjectInfo> failedMetas;
    std::vector<std::list<GetObjectInfo>> tempFailedMetas(groupedQueryMetas.size());
    std::vector<std::vector<std::string>> tempSuccessIds(groupedQueryMetas.size());
    std::vector<std::vector<ReadKey>> tempNeedRetryIds(groupedQueryMetas.size());
    std::vector<std::unordered_set<std::string>> tempFailedIds(groupedQueryMetas.size());
    size_t index = 0;
    auto traceContext = Trace::Instance().GetContext();
    Timer timer;
    int64_t realTimeoutMs = reqTimeoutDuration.CalcRealRemainingTime();
    std::shared_ptr<GetRequest> fakeRequest = nullptr;
    for (auto queryMeta = groupedQueryMetas.begin(); queryMeta != groupedQueryMetas.end(); ++queryMeta, ++index) {
        auto &address = queryMeta->first;
        auto &infoList = queryMeta->second;
        auto func = [this, address, &infoList, &fakeRequest, &tempSuccessIds, &tempNeedRetryIds, &tempFailedIds,
                     &tempFailedMetas, index, traceContext, realTimeoutMs, &timer] {
            int64_t elapsed = static_cast<int64_t>(timer.ElapsedMilliSecond());
            reqTimeoutDuration.Init(realTimeoutMs - elapsed);
            TraceGuard traceGuard = Trace::Instance().SetTraceContext(traceContext, true);
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
                                           tempFailedIds, objectsNeedGetRemote, lastRc, rsp, queryMetas, migratedBytes);
    return lastRc;
}

Status WorkerOcServiceGetImpl::ProcessRemoteGetInNotification(const NotifyRemoteGetReqPb &req,
                                                              std::set<ReadKey> objectsNeedGetRemote,
                                                              QueryMetaMap &queryMetas, NotifyRemoteGetRspPb &rsp,
                                                              uint64_t &migratedBytes)
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
        auto remoteGetRc = ProcessRemoteGetInNotificationImpl(groupedQueryMetas, lockedEntries, rsp,
                                                              objectsNeedGetRemote, queryMetas, migratedBytes);
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

void WorkerOcServiceGetImpl::UpdateNotifyRemoteGetRateLimit(const std::string &workerAddr, uint64_t migratedBytes,
                                                            NotifyRemoteGetRspPb &rsp)
{
    rateController_->SlidingWindowUpdateRate(migratedBytes);
    rsp.set_limit_rate(rateController_->CalculateNewRate(workerAddr));
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
    const int maxBatchSize = 32;
    std::set<ReadKey> batchObjects;
    Status lastRc;
    uint64_t migratedBytes = 0;
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
        batchObjects.insert(readKey);

        if (batchObjects.size() >= maxBatchSize) {
            Status rc = ProcessRemoteGetInNotification(req, std::move(batchObjects), queryMetas, rsp, migratedBytes);
            if (rc.IsError()) {
                lastRc = rc;
            }
            batchObjects.clear();
        }
    }

    // Process remaining objects in the last batch
    if (!batchObjects.empty()) {
        Status rc = ProcessRemoteGetInNotification(req, std::move(batchObjects), queryMetas, rsp, migratedBytes);
        if (rc.IsError()) {
            lastRc = rc;
        }
    }
    UpdateNotifyRemoteGetRateLimit(req.addr(), migratedBytes, rsp);
    return lastRc;
}
}  // namespace object_cache
}  // namespace datasystem
