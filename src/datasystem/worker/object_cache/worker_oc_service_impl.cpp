/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/cluster/executor/key_filter.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/parallel/parallel_for.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/deadlock_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/worker/object_cache/data_migrator/data_migrator.h"
#include "datasystem/worker/object_cache/data_migrator/handler/migrate_data_handler.h"
#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/protos/cluster_topology.pb.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/async_update_location_manager.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/scale_down_node_selector.h"
#include "datasystem/worker/object_cache/device/worker_device_oc_manager.h"
#include "datasystem/worker/object_cache/get_hash_ring_response.h"
#include "datasystem/worker/object_cache/kv_event/kv_event_publisher.h"
#include "datasystem/worker/object_cache/metadata_recovery_selector.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/recovery/object_cache_ownership_reconciliation.h"
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_startup.h"
#include "datasystem/worker/object_cache/recovery/object_cache_recovery_state.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/object_cache/verify_leaving_state.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/runtime/worker_topology_runtime.h"
#include "datasystem/worker/worker_health_check.h"

DS_DEFINE_int32(oc_thread_num, 32, "Thread number of worker service");
DS_DEFINE_validator(oc_thread_num, &Validator::ValidateThreadNum);
DS_DEFINE_uint32_dynamic(client_reconnect_wait_s, 10, "Client reconnect wait seconds, default is 10.");
DS_DEFINE_validator(client_reconnect_wait_s, &Validator::ValidateUint32);
DS_DEFINE_bool(enable_reconciliation, true, "Whether to enable reconciliation, default is true");
DS_DEFINE_bool(oc_io_from_l2cache_need_metadata, true,
               "Whether data read and write from the L2 cache daemon depend on metadata. Note: If set to false, it "
               "indicates that the metadata is not stored in etcd.");
DS_DECLARE_string(l2_cache_type);
DS_DEFINE_bool(oc_metadata_header, true,
               "Whether to allocate metadata header for object cache shared memory. Set to false (0) to disable the "
               "metadata header, which also disables shm-based latch/visibility.");
DS_DEFINE_uint32(data_migrate_rate_limit_mb, 40, "Data migrate rate limit for every node when scale down happen");
DS_DEFINE_validator(data_migrate_rate_limit_mb, [](const char *flagName, uint32_t value) {
    (void)flagName;
    return value > 0;
});
DS_DEFINE_string(data_migrate_urma_transport_mode, "write",
                 "URMA transport mode for background data migration, valid values are read and write.");
DS_DEFINE_validator(data_migrate_urma_transport_mode, [](const char *flagName, const std::string &value) {
    (void)flagName;
    return value == "read" || value == "write";
});

DS_DECLARE_uint32(max_client_num);
DS_DECLARE_string(worker_address);
DS_DECLARE_string(master_address);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(etcd_address);
DS_DECLARE_bool(enable_distributed_master);
DS_DECLARE_uint32(memory_alignment);
DS_DECLARE_bool(enable_metadata_recovery);
DS_DECLARE_string(kv_events_config);

using namespace datasystem::master;
using namespace datasystem::worker;
namespace datasystem {
namespace object_cache {
namespace {
using RuntimeAdmissionGuard = datasystem::worker::WorkerRuntimeFacade::AdmissionGuard;

constexpr char CLUSTER_TOPOLOGY_SCHEMA_VERSION[] = "1";
constexpr char TOPOLOGY_READINESS_PROBE_KEY[] = "topology-readiness-probe";

std::string JoinP2PMetaObjectKeys(const GetP2PMetaReqPb &req)
{
    std::stringstream allKeys;
    bool first = true;
    for (const auto &devObjMeta : req.dev_obj_meta()) {
        if (!first) {
            allKeys << ", ";
        }
        allKeys << devObjMeta.object_key();
        first = false;
    }
    return allKeys.str();
}

void RewriteP2PMetaWorkerAddress(GetP2PMetaReqPb &req, const HostPort &localAddress)
{
    for (auto &devObjMeta : *req.mutable_dev_obj_meta()) {
        for (auto &location : *devObjMeta.mutable_locations()) {
            location.set_worker_ip(localAddress.Host());
        }
    }
    req.set_worker_address(localAddress.ToString());
}

Status ToTopologyChangeTypePb(cluster::TopologyChangeType type, ::datasystem::TypePb &typePb)
{
    switch (type) {
        case cluster::TopologyChangeType::SCALE_OUT:
            typePb = ::datasystem::SCALE_OUT;
            return Status::OK();
        case cluster::TopologyChangeType::SCALE_IN:
            typePb = ::datasystem::SCALE_IN;
            return Status::OK();
        case cluster::TopologyChangeType::FAILURE:
            typePb = ::datasystem::FAILURE;
            return Status::OK();
    }
    RETURN_STATUS(K_INVALID, "Invalid cluster topology change type");
}

Status BuildClusterTopologyPb(const cluster::TopologySnapshot &snapshot, ::datasystem::ClusterTopologyPb &topologyPb)
{
    topologyPb.set_cluster_has_init(snapshot.ClusterHasInit());
    topologyPb.set_version(snapshot.Version());
    topologyPb.set_schema_version(CLUSTER_TOPOLOGY_SCHEMA_VERSION);
    for (const auto &member : snapshot.Members()) {
        auto &memberPb = (*topologyPb.mutable_members())[member.identity.address];
        memberPb.set_id(member.identity.id);
        memberPb.set_state(static_cast<::datasystem::MembershipPb::StatePb>(member.state));
        for (uint32_t token : member.tokens) {
            memberPb.add_tokens(token);
        }
    }
    if (snapshot.GetActiveBatch().has_value()) {
        ::datasystem::TypePb typePb = ::datasystem::SCALE_OUT;
        RETURN_IF_NOT_OK(ToTopologyChangeTypePb(snapshot.GetActiveBatch()->type, typePb));
        topologyPb.mutable_active_batch()->set_type(typePb);
        topologyPb.mutable_active_batch()->set_epoch(snapshot.GetActiveBatch()->epoch);
    }
    return Status::OK();
}

}  // namespace

Status BuildGetHashRingResponse(const cluster::TopologySnapshot &snapshot, uint64_t requestedVersion,
                                const std::string &masterAddress, const RoutingHostIdLoader &loadHostIds,
                                GetHashRingRspPb &rsp)
{
    rsp.Clear();
    rsp.set_version(snapshot.Version());
    rsp.set_master_address(masterAddress);
    if (requestedVersion != 0 && requestedVersion == snapshot.Version()) {
        rsp.set_hash_ring_changed(false);
        return Status::OK();
    }

    rsp.set_hash_ring_changed(true);
    RETURN_IF_NOT_OK(BuildClusterTopologyPb(snapshot, *rsp.mutable_hash_ring()));
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(loadHostIds), K_NOT_READY,
                             "Host ID loader is unavailable for hash ring refresh");
    return loadHostIds(*rsp.mutable_host_id_map());
}

static constexpr int DEBUG_LOG_LEVEL = 2;
static constexpr uint64_t URMA_WARMUP_OBJECT_SIZE = 1;
static constexpr int64_t URMA_WARMUP_REQUEST_TIMEOUT_MS = 5'000;
static constexpr int64_t TOPOLOGY_READY_WAIT_TIMEOUT_S = 60;
static constexpr size_t MAX_TOPOLOGY_SCALE_IN_CLEANUP_STATES = 1'024;

uint64_t PayloadBytes(const std::vector<RpcMessage> &payloads)
{
    uint64_t bytes = 0;
    for (const auto &payload : payloads) {
        bytes += payload.Size();
    }
    return bytes;
}

void UpdateWorkerObjectGauge(const std::shared_ptr<ObjectTable> &objectTable)
{
    if (objectTable != nullptr) {
        metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_OBJECT_COUNT)).Set(objectTable->GetSize());
    }
    memory::ShmMemStat stat;
    memory::Allocator::Instance()->GetMemStat(stat);
    metrics::GetGauge(static_cast<uint16_t>(metrics::KvMetricId::WORKER_ALLOCATED_MEMORY_SIZE))
        .Set(stat.objectMemoryUsage);
}

void RecordMultiPublishTransportMetrics(const MultiPublishReqPb &req, uint64_t payloadBytes, bool clientShmEnabled)
{
    const bool shmEnabled = WorkerOcServiceCrudCommonApi::ShmEnable();
    uint64_t shmBytes = 0;
    uint64_t urmaBytes = 0;
    bool hasNonShmObject = false;
    for (const auto &info : req.object_info()) {
        if (info.shm_id().empty()) {
            hasNonShmObject = true;
            continue;
        }
        if (shmEnabled && clientShmEnabled) {
            shmBytes += static_cast<uint64_t>(info.data_size());
        } else {
            urmaBytes += static_cast<uint64_t>(info.data_size());
        }
    }
    if (hasNonShmObject && payloadBytes > 0) {
        const auto metricId = clientShmEnabled ? metrics::KvMetricId::WORKER_FROM_CLIENT_LOCAL_TOTAL_BYTES
                                               : metrics::KvMetricId::WORKER_FROM_CLIENT_TCP_TOTAL_BYTES;
        METRIC_ADD(metricId, payloadBytes);
    }
    METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_SHM_TOTAL_BYTES, shmBytes);
    METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_URMA_TOTAL_BYTES, urmaBytes);
}

Status AcquireObjectCacheAdmissionGuard(const worker::WorkerRuntimeFacade *runtime, worker::WorkerAdmissionKind kind,
                                        std::optional<RuntimeAdmissionGuard> &guard)
{
    if (runtime == nullptr) {
        return Status::OK();
    }
    RuntimeAdmissionGuard candidate;
    RETURN_IF_NOT_OK(runtime->AcquireAdmissionGuard(kind, "ObjectCacheService", candidate));
    guard.emplace(std::move(candidate));
    return Status::OK();
}

Status AcquireObjectCacheAsyncAdmissionGuard(const worker::WorkerRuntimeFacade *runtime,
                                             worker::WorkerAdmissionKind kind,
                                             std::shared_ptr<std::optional<RuntimeAdmissionGuard>> &guard)
{
    guard = std::make_shared<std::optional<RuntimeAdmissionGuard>>();
    return AcquireObjectCacheAdmissionGuard(runtime, kind, *guard);
}

static constexpr int OLD_VERSION_DEL_THREAD_MIN_NUM = 0;
static constexpr int OLD_VERSION_DEL_THREAD_MAX_NUM = 1;
static constexpr uint32_t SHM_QUEUE_SLOT_NUM = 32;
static const std::string WORKER_OC_SERVICE_IMPL = "WorkerOCServiceImpl";
constexpr size_t GET_MATCH_OBJECT_BATCH = 500;  // batch number is 500.

WorkerOCServiceImpl::WorkerOCServiceImpl(
    HostPort serverAddr, HostPort masterAddr, std::shared_ptr<ObjectTable> objectTable,
    std::shared_ptr<AkSkManager> manager, std::shared_ptr<WorkerOcEvictionManager> evictionManager,
    std::shared_ptr<PersistenceApi> persistApi, ObjectCacheRecoveryDependencies recoveryDependencies,
    MasterOCServiceImpl *masterOCService, worker::IWorkerTopologyRuntime *topologyRuntime,
    const worker::MetadataRouteResolver &metadataRoute, const cluster::MembershipEndpointView &membership,
    const std::atomic<bool> *exitRequested, bool isRestart, bool controlBackendAvailableAtStartup)
    : WorkerOCService(std::move(serverAddr)),
      localMasterAddress_(std::move(masterAddr)),
      persistenceApi_(persistApi),
      objectTable_(std::move(objectTable)),
      evictionManager_(std::move(evictionManager)),
      recoveryDependencies_(std::move(recoveryDependencies)),
      topologyRuntime_(topologyRuntime),
      metadataRoute_(metadataRoute),
      membership_(membership),
      endpointPolicy_(metadataRoute, membership),
      exitRequested_(exitRequested),
      isRestart_(isRestart),
      centralizedMetadata_(!FLAGS_enable_distributed_master),
      controlBackendAvailableAtStartup_(controlBackendAvailableAtStartup),
      akSkManager_(std::move(manager))
{
    initOkFuture_ = initOk_.get_future();
    workerMasterApiManager_ =
        std::make_shared<WorkerMasterOcApiManager>(localAddress_, akSkManager_, metadataRoute_, masterOCService);
    memoryRefTable_ = std::make_shared<SharedMemoryRefTable>();
    globalRefTable_ = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    asyncSendManager_ = std::make_shared<AsyncSendManager>(persistApi, evictionManager_);
    slotRecoveryManager_ = std::make_shared<SlotRecoveryManager>();
    recoveryState_ = std::make_unique<ObjectCacheRecoveryState>();
    exitFlag_ = std::make_shared<std::atomic_bool>(false);

    // Set async send manager and persistence api to eviction manager
    evictionManager_->SetAsyncSendManager(asyncSendManager_);
}

WorkerOCServiceImpl::~WorkerOCServiceImpl()
{
    LOG(INFO) << "WorkerOCServiceImpl exit";
    // Ensure that initOk_.set_value() is called to avoid suspension when MasterLocalWorkerOCApi inits.
    if (!setValue_) {
        initOk_.set_value(Status(K_RUNTIME_ERROR, "WorkerOCServiceImpl init fail"));
        setValue_ = true;
    }
    clearDataFlow_.reset();
    AddLocalFailedNodeEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    NodeRestartEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    EraseFailedNodeApiEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    StartNodeCheckEvent::GetInstance().RemoveSubscriber(WORKER_OC_SERVICE_IMPL);
    AsyncResourceReleaser::Instance().Shutdown();
    NodeSelector::Instance().Shutdown();
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

void WorkerOCServiceImpl::SetRuntimeFacade(worker::WorkerRuntimeFacade *runtime)
{
    runtime_ = runtime;
    if (gMigrateProc_ != nullptr) {
        gMigrateProc_->SetRuntimeFacade(runtime_);
    }
    MarkRestartReconciliationPending(runtime_, recoveryState_.get(), isRestart_, controlBackendAvailableAtStartup_,
                                     FLAGS_enable_reconciliation);
}

worker::WorkerRecoveryEvidenceReport WorkerOCServiceImpl::GetLastMetadataRecoveryEvidenceReport() const
{
    return recoveryState_->GetLastMetadataRecoveryEvidenceReport();
}

worker::WorkerRecoveryEvidenceReport WorkerOCServiceImpl::BuildObjectCacheRecoveryEvidenceReport(
    uint64_t *resourceRecoveryGeneration) const
{
    return recoveryState_->BuildObjectCacheRecoveryEvidenceReport(
        [this] {
            worker::WorkerRecoveryEvidenceBuilder builder;
            return slotRecoveryManager_ == nullptr ? builder.BuildReport("slot_manager_unavailable")
                                                   : slotRecoveryManager_->BuildSlotRecoveryEvidenceReportFromStore();
        },
        [this] { return recoveryState_->GetLastOwnershipRecoveryEvidenceReport(); },
        [this](CacheType cacheType) {
            if (cacheType == CacheType::DISK && !memory::Allocator::Instance()->IsDiskAvailable()) {
                return false;
            }
            return evictionManager_ != nullptr && evictionManager_->IsResourceRecovered(cacheType);
        },
        resourceRecoveryGeneration);
}

worker::WorkerRecoveryGeneration WorkerOCServiceImpl::BeginRecoveryEvidenceGeneration(std::string detail)
{
    return recoveryState_->BeginRecoveryEvidenceGeneration(std::move(detail));
}

worker::WorkerRecoveryEvidenceReport WorkerOCServiceImpl::BuildObjectCacheRecoveryEvidenceReport(
    worker::WorkerRecoveryGeneration generation) const
{
    return recoveryState_->TrackEvidenceForGeneration(generation, BuildObjectCacheRecoveryEvidenceReport());
}

bool WorkerOCServiceImpl::PublishResourceRecoveryIfCurrent(uint64_t resourceRecoveryGeneration,
                                                           const std::function<bool()> &publish)
{
    return recoveryState_->PublishResourceRecoveryIfCurrent(resourceRecoveryGeneration, publish);
}

void WorkerOCServiceImpl::RegisterRecoveryEvidenceReadyHandler(std::function<void()> handler)
{
    recoveryState_->RegisterRecoveryEvidenceReadyHandler(std::move(handler));
}

void WorkerOCServiceImpl::MarkRestartReconciliationEvidenceReady(const std::string &detail)
{
    MarkReconciliationEvidenceReady(detail);
}

void WorkerOCServiceImpl::MarkReconciliationEvidenceReady(const std::string &detail)
{
    recoveryState_->MarkOwnershipReconciliationReady(detail);
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
        .kvEventPublisher = kvEventPublisher_,
        .metadataSize = metadataSize_,
        .persistenceApi = persistenceApi_,
        .metadataRouteResolver = &metadataRoute_,
        .endpointPolicy = &endpointPolicy_,
        .exitRequested = exitRequested_,
        .allowDirectoryLag = centralizedMetadata_,
    };
    createProc_ = std::make_shared<WorkerOcServiceCreateImpl>(param, akSkManager_, localAddress_);

    publishProc_ = std::make_shared<WorkerOcServicePublishImpl>(param, memCpyThreadPool_, akSkManager_, localAddress_);

    multiPublishProc_ = std::make_shared<WorkerOcServiceMultiPublishImpl>(param, memCpyThreadPool_, threadPool_,
                                                                          akSkManager_, localAddress_);

    migrateRateController_ =
        std::make_shared<MigrateDataRateController>(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul);
    getProc_ =
        std::make_shared<WorkerOcServiceGetImpl>(param, recoveryDependencies_.metadataReader, memCpyThreadPool_,
                                                 threadPool_, akSkManager_, localAddress_, migrateRateController_);

    deleteProc_ = std::make_shared<WorkerOcServiceDeleteImpl>(param, akSkManager_, localAddress_, getProc_);

    gRefProc_ =
        std::make_shared<WorkerOcServiceGlobalReferenceImpl>(param, globalRefTable_, akSkManager_, localAddress_);

    gMigrateProc_ = std::make_shared<WorkerOcServiceMigrateImpl>(param, memCpyThreadPool_, akSkManager_,
                                                                 GetLocalAddr().ToString(), migrateRateController_);
    gMigrateProc_->SetRuntimeFacade(runtime_);
    gMigrateProc_->SetOutOfMemoryHandler(
        [this](const Status &rc, const std::string &operation, memory::CacheType cacheType) {
            MarkOutOfMemoryIfNeeded(rc, operation, cacheType);
        });

    expireProc_ = std::make_shared<WorkerOcServiceExpireImpl>(param, akSkManager_);
    initOk_.set_value(Status::OK());
    setValue_ = true;
}

Status WorkerOCServiceImpl::Init()
{
    InitMetaSize();
    RETURN_IF_NOT_OK(ResetHealthProbe());
    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(localMasterAddress_);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "get worker master api failed, Init failed");
    RETURN_IF_NOT_OK(InitThreadResources());
    RETURN_IF_NOT_OK(evictionManager_->Init(globalRefTable_, akSkManager_));
    RETURN_IF_NOT_OK(InitL2Cache());
    WorkerRequestManager::SetDeleteObjectsFunc(
        [this](const std::string &objectKey, uint64_t version) -> Status { return DeleteObject(objectKey, version); });
    workerDevOcManager_ = std::make_shared<WorkerDeviceOcManager>(this);
    lastReconTime_ = GetSteadyClockTimeStampMs();  // Record current timestamp in case we need reconciliation.
    RETURN_IF_NOT_OK(StartDecreaseReferenceProcess());
    auto kvEventConfig = BuildKvEventConfigFromJsonString(FLAGS_kv_events_config);
    if (kvEventConfig.enabled) {
        kvEventPublisher_ = std::make_shared<KvEventPublisher>(std::move(kvEventConfig));
        if (!kvEventPublisher_->Enabled()) {
            kvEventPublisher_.reset();
        }
    }
    evictionManager_->SetKvEventPublisher(kvEventPublisher_);
    RETURN_IF_NOT_OK(InitRecoveryServices());
    return Status::OK();
}

Status WorkerOCServiceImpl::InitThreadResources()
{
    RETURN_IF_EXCEPTION_OCCURS(threadPool_ = std::make_shared<ThreadPool>(FLAGS_oc_thread_num, 0, "OcGetThread"));
    RETURN_IF_EXCEPTION_OCCURS(memCpyThreadPool_ = std::make_shared<ThreadPool>(MEMCOPY_THREAD_NUM));
    datasystem::Parallel::InitParallelThreadPool(PARALLEL_THREAD_NUM, FLAGS_oc_thread_num);
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
    return Status::OK();
}

Status WorkerOCServiceImpl::InitRecoveryServices()
{
    MetaDataRecoveryManager::ClusterAccess clusterAccess;
    clusterAccess.checkConnection = [this](const HostPort &addr) { return endpointPolicy_.CheckEndpoint(addr, false); };
    metadataRecoveryManager_ = std::make_unique<MetaDataRecoveryManager>(
        localAddress_, objectTable_, std::move(clusterAccess), workerMasterApiManager_, metadataRoute_, metadataSize_,
        evictionManager_, memCpyThreadPool_);
    AsyncResourceReleaser::Instance().Init(objectTable_);
    InitServiceImpl();
    NodeSelector::Instance().Init(localAddress_.ToString(), membership_, exitRequested_, workerMasterApiManager_);
    getProc_->Init();
    RETURN_IF_NOT_OK(slotRecoveryManager_->Init(localAddress_, membership_, persistenceApi_, workerMasterApiManager_,
                                                recoveryDependencies_.slotRecoveryStore,
                                                metadataRecoveryManager_.get()));
    clearDataFlow_ = std::make_unique<WorkerOcServiceClearDataFlow>(
        objectTable_, globalRefTable_, workerMasterApiManager_, gRefProc_, deleteProc_, metadataRecoveryManager_.get(),
        metadataRoute_, endpointPolicy_, localAddress_.ToString());
    AddLocalFailedNodeEvent::GetInstance().AddSubscriber(
        WORKER_OC_SERVICE_IMPL, [this](const HostPort &node) { return PushMetadataToMaster(node); });
    NodeRestartEvent::GetInstance().AddSubscriber(
        WORKER_OC_SERVICE_IMPL,
        [this](const std::string &workerAddr, int64_t, bool) { return HandleNodeRestartEvent(workerAddr); });
    EraseFailedNodeApiEvent::GetInstance().AddSubscriber(WORKER_OC_SERVICE_IMPL,
                                                         [this](HostPort &node) { EraseFailedWorkerMasterApi(node); });
    StartNodeCheckEvent::GetInstance().AddSubscriber(WORKER_OC_SERVICE_IMPL, [this] { return GiveUpReconciliation(); });
    return Status::OK();
}

Status WorkerOCServiceImpl::HealthCheck(const HealthCheckRequestPb &req, HealthCheckReplyPb &resp)
{
    INJECT_POINT("worker.HealthCheck.begin");
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::DIAGNOSTIC_RPC, admissionGuard));
    auto rc = ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                                  worker::WorkerAdmissionKind::DIAGNOSTIC_RPC);
    if (rc.IsError()) {
        LOG(WARNING) << rc;
        return rc;
    }
    if (not req.client_id().empty()) {
        std::string tenantId;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    }
    if (runtime_ != nullptr) {
        const auto snapshot = runtime_->GetSnapshot();
        resp.set_worker_service_mode(worker::ToString(snapshot.mode));
        resp.set_worker_service_reason(worker::ToString(snapshot.reason));
        resp.set_worker_recovery_phase(worker::ToString(snapshot.recoveryPhase));
        resp.set_recovery_evidence_mask(worker::RecoveryEvidenceMask(snapshot.evidence));
    }
    if (exitRequested_ != nullptr && exitRequested_->load()) {
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

std::string WorkerOCServiceImpl::GetHitInfo() const
{
    return getProc_->GetHitInfo();
}

Status WorkerOCServiceImpl::Publish(const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> payloads)
{
    ScopedRequestContext ctx;
    METRIC_TIMER(metrics::KvMetricId::WORKER_PROCESS_PUBLISH_LATENCY);
    uint64_t payloadBytes = PayloadBytes(payloads);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::VerifyLeavingState(exitRequested_, FLAGS_enable_leaving_intercept),
                                     "verify leaving state failed");
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    Status rc = publishProc_->Publish(req, resp, payloads);
    if (rc.IsOk()) {
        auto clientId = ClientKey::Intern(req.client_id());
        const bool clientShmEnabled = WorkerOcServiceCrudCommonApi::ClientShmEnabled(clientId);
        if (!req.shm_id().empty()) {
            if (WorkerOcServiceCrudCommonApi::ShmEnable() && clientShmEnabled) {
                METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_SHM_TOTAL_BYTES,
                           static_cast<uint64_t>(req.data_size()));
            } else {
                METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_URMA_TOTAL_BYTES,
                           static_cast<uint64_t>(req.data_size()));
            }
        } else if (payloadBytes > 0) {
            if (clientShmEnabled) {
                METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_LOCAL_TOTAL_BYTES, payloadBytes);
            } else {
                METRIC_ADD(metrics::KvMetricId::WORKER_FROM_CLIENT_TCP_TOTAL_BYTES, payloadBytes);
            }
        }
        UpdateWorkerObjectGauge(objectTable_);
    }
    return rc;
}

Status WorkerOCServiceImpl::MultiPublish(const MultiPublishReqPb &req, MultiPublishRspPb &resp,
                                         std::vector<RpcMessage> payloads)
{
    ScopedRequestContext ctx;
    METRIC_TIMER(metrics::KvMetricId::WORKER_PROCESS_PUBLISH_LATENCY);
    uint64_t payloadBytes = PayloadBytes(payloads);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::VerifyLeavingState(exitRequested_, FLAGS_enable_leaving_intercept),
                                     "verify leaving state failed");
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    auto clientId = ClientKey::Intern(req.client_id());
    RETURN_IF_NOT_OK(multiPublishProc_->MultiPublish(req, resp, payloads, clientId));
    const bool clientShmEnabled = WorkerOcServiceCrudCommonApi::ClientShmEnabled(clientId);
    RecordMultiPublishTransportMetrics(req, payloadBytes, clientShmEnabled);
    UpdateWorkerObjectGauge(objectTable_);
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
                                          std::vector<std::string> &objKeys, bool includeL2CacheIds)
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
    std::function batchFun = [this, matchFunc, &objKeys, includeL2CacheIds](std::vector<std::string> &objects) {
        for (const auto &id : objects)
            if (matchFunc(id)) {
                if (includeL2CacheIds) {
                    objKeys.emplace_back(id);
                    continue;
                }
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
    LOG(INFO) << "GetObjectsMatch finish, objectKeys size: " << objKeys.size()
              << " , include l2cache ids: " << includeL2CacheIds
              << " ElapsedMilliSecond: " << timer.ElapsedMilliSecond();
}

Status WorkerOCServiceImpl::GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr)
{
    return destAddr.ParseString(srcAddr);
}

void WorkerOCServiceImpl::RegisterAsyncTasksDoneChecker(AsyncTasksDoneChecker checker)
{
    asyncTasksDoneChecker_ = std::move(checker);
}

Status WorkerOCServiceImpl::SelectTopologyScaleInObjects(std::vector<std::string> &copies,
                                                         std::vector<std::string> &primaries) const
{
    for (const auto &entryPair : *objectTable_) {
        const auto &entry = entryPair.second;
        RETURN_IF_NOT_OK_APPEND_MSG(entry->TryRLock(), "member-wide object selection needs retry");
        Raii unlock([&entry]() { entry->RUnlock(); });
        auto &destination = (*entry)->stateInfo.IsPrimaryCopy() ? primaries : copies;
        destination.emplace_back(entryPair.first);
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::PrepareTopologyScaleInData(const std::vector<std::string> &copies,
                                                       const std::vector<std::string> &primaries,
                                                       std::vector<std::string> &migrateIds,
                                                       std::vector<std::string> &waitIds,
                                                       std::vector<std::string> &l2Ids,
                                                       const std::string &businessOperationId)
{
    std::vector<std::string> copyFailures;
    GroupAndRemoveMeta(copies, master::RemoveMetaReqPb::NORMAL, copyFailures, migrateIds, waitIds, l2Ids,
                       businessOperationId);
    std::vector<std::string> primaryFailures;
    GroupAndRemoveMeta(primaries, master::RemoveMetaReqPb::GIVEUP_PRIMARY, primaryFailures, migrateIds, waitIds, l2Ids,
                       businessOperationId);
    CHECK_FAIL_RETURN_STATUS(copyFailures.empty() && primaryFailures.empty(), K_TRY_AGAIN,
                             "member-wide metadata removal needs retry");
    return Status::OK();
}

Status WorkerOCServiceImpl::DrainTopologyScaleInData(const cluster::TopologyPhaseAction &action,
                                                     const std::string &businessOperationId,
                                                     std::chrono::steady_clock::time_point deadline,
                                                     const cluster::CancellationToken &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(!action.taskId.empty(), K_INVALID, "ScaleIn callback lacks task id");
    CHECK_FAIL_RETURN_STATUS(!businessOperationId.empty(), K_INVALID, "empty topology business operation id");
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology ScaleIn cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology ScaleIn deadline exceeded");
    std::vector<std::string> copies;
    std::vector<std::string> primaries;
    std::vector<std::string> migrateIds;
    std::vector<std::string> waitIds;
    std::vector<std::string> l2Ids;
    RETURN_IF_NOT_OK(SelectTopologyScaleInObjects(copies, primaries));
    RETURN_IF_NOT_OK(PrepareTopologyScaleInData(copies, primaries, migrateIds, waitIds, l2Ids, businessOperationId));
    RETURN_IF_NOT_OK(MigrateData(migrateIds, action.taskId, deadline, cancellation));
    std::vector<std::string> failures;
    GroupAndRemoveMeta(migrateIds, master::RemoveMetaReqPb::NORMAL, failures, migrateIds, waitIds, l2Ids,
                       businessOperationId);
    CHECK_FAIL_RETURN_STATUS(failures.empty(), K_TRY_AGAIN, "migrated metadata removal needs retry");
    {
        std::lock_guard<std::shared_timed_mutex> lock(clearIdsMutex_);
        voluntaryScaleDownClearIds_ = std::move(waitIds);
    }
    if (asyncTasksDoneChecker_ != nullptr) {
        RETURN_IF_NOT_OK(asyncTasksDoneChecker_(action.taskId, deadline, cancellation));
    }
    RETURN_IF_NOT_OK(MigrateL2CacheData(l2Ids, action.taskId, deadline, cancellation));
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology ScaleIn cancelled");
    return Status::OK();
}

Status WorkerOCServiceImpl::GetOrCreateTopologyScaleInCleanupState(const cluster::IKeyFilter &filter,
                                                                   const std::string &businessOperationId,
                                                                   std::shared_ptr<PreparedScaleInCleanupState> &state)
{
    {
        std::lock_guard<std::mutex> lock(topologyScaleInCleanupMutex_);
        const auto iter = topologyScaleInCleanupByOperation_.find(businessOperationId);
        if (iter != topologyScaleInCleanupByOperation_.end()) {
            state = iter->second.lock();
            if (state != nullptr) {
                return Status::OK();
            }
        }
    }
    auto candidate = std::make_shared<PreparedScaleInCleanupState>();
    for (const auto &entry : *objectTable_) {
        if (filter.Contains(entry.first)) {
            candidate->objectIds.emplace_back(entry.first);
        }
    }
    std::lock_guard<std::mutex> lock(topologyScaleInCleanupMutex_);
    const auto existing = topologyScaleInCleanupByOperation_.find(businessOperationId);
    if (existing != topologyScaleInCleanupByOperation_.end()) {
        state = existing->second.lock();
        if (state != nullptr) {
            return Status::OK();
        }
    }
    for (auto iter = topologyScaleInCleanupByOperation_.begin(); iter != topologyScaleInCleanupByOperation_.end();) {
        iter = iter->second.expired() ? topologyScaleInCleanupByOperation_.erase(iter) : std::next(iter);
    }
    CHECK_FAIL_RETURN_STATUS(topologyScaleInCleanupByOperation_.size() < MAX_TOPOLOGY_SCALE_IN_CLEANUP_STATES,
                             K_TRY_AGAIN, "topology ScaleIn cleanup state capacity reached");
    state = std::move(candidate);
    topologyScaleInCleanupByOperation_.emplace(businessOperationId, state);
    return Status::OK();
}

Status WorkerOCServiceImpl::AuthorizeTopologyScaleInCleanup(const std::shared_ptr<PreparedScaleInCleanupState> &state)
{
    std::lock_guard<std::mutex> lock(topologyScaleInCleanupMutex_);
    state->authorized = true;
    return Status::OK();
}

Status WorkerOCServiceImpl::ApplyTopologyCleanupEffects(const std::vector<std::string> &objectIds,
                                                        const std::string &businessOperationId)
{
    std::vector<std::string> failures;
    std::vector<std::string> migrateIds;
    std::vector<std::string> waitIds;
    std::vector<std::string> l2Ids;
    GroupAndRemoveMeta(objectIds, master::RemoveMetaReqPb::NORMAL, failures, migrateIds, waitIds, l2Ids,
                       businessOperationId);
    CHECK_FAIL_RETURN_STATUS(failures.empty() && migrateIds.empty() && waitIds.empty() && l2Ids.empty(), K_TRY_AGAIN,
                             "task-scoped source cleanup is not ready");
    return Status::OK();
}

Status WorkerOCServiceImpl::ApplyTopologyScaleInCleanup(const std::string &businessOperationId,
                                                        const std::shared_ptr<PreparedScaleInCleanupState> &state,
                                                        std::chrono::steady_clock::time_point deadline,
                                                        const cluster::CancellationToken &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology cleanup cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology cleanup deadline exceeded");
    {
        std::lock_guard<std::mutex> lock(topologyScaleInCleanupMutex_);
        CHECK_FAIL_RETURN_STATUS(state->authorized, K_INVALID, "topology ScaleIn cleanup is not authorized");
        if (state->applied) {
            return Status::OK();
        }
    }
    const auto remainingUs =
        std::chrono::duration_cast<std::chrono::microseconds>(deadline - std::chrono::steady_clock::now()).count();
    ApiDeadlineGuard deadlineGuard(remainingUs, InUs{});
    RETURN_IF_NOT_OK(ApplyTopologyCleanupEffects(state->objectIds, businessOperationId));
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology cleanup cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology cleanup deadline exceeded");
    std::lock_guard<std::mutex> lock(topologyScaleInCleanupMutex_);
    state->applied = true;
    topologyScaleInCleanupByOperation_.erase(businessOperationId);
    return Status::OK();
}

Status WorkerOCServiceImpl::PrepareTopologyScaleInCleanup(const cluster::TopologyPhaseAction &action,
                                                          const cluster::IKeyFilter &filter,
                                                          const std::string &businessOperationId,
                                                          std::chrono::steady_clock::time_point deadline,
                                                          const cluster::CancellationToken &cancellation,
                                                          std::function<Status()> &authorize,
                                                          cluster::TopologyCleanupEffect &apply)
{
    CHECK_FAIL_RETURN_STATUS(!action.taskId.empty(), K_INVALID, "ScaleIn cleanup lacks task id");
    CHECK_FAIL_RETURN_STATUS(!businessOperationId.empty(), K_INVALID, "empty topology cleanup operation id");
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology cleanup cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology cleanup deadline exceeded");
    std::shared_ptr<PreparedScaleInCleanupState> state;
    RETURN_IF_NOT_OK(GetOrCreateTopologyScaleInCleanupState(filter, businessOperationId, state));
    CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology cleanup cancelled");
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "topology cleanup deadline exceeded");
    authorize = [this, state] { return AuthorizeTopologyScaleInCleanup(state); };
    apply = [this, businessOperationId, state](std::chrono::steady_clock::time_point applyDeadline,
                                               const cluster::CancellationToken &applyCancellation) {
        return ApplyTopologyScaleInCleanup(businessOperationId, state, applyDeadline, applyCancellation);
    };
    return Status::OK();
}

Status WorkerOCServiceImpl::SubmitTopologyFailureCleanup(const cluster::TopologyPhaseAction &action,
                                                         const cluster::IKeyFilter &filter,
                                                         const std::string &businessOperationId,
                                                         std::chrono::steady_clock::time_point deadline,
                                                         const cluster::CancellationToken &cancellation)
{
    CHECK_FAIL_RETURN_STATUS(clearDataFlow_ != nullptr, K_NOT_READY, "clear-data flow is not initialized");
    return clearDataFlow_->SubmitTopologyFailureCleanup(action, filter, businessOperationId, deadline, cancellation);
}

Status WorkerOCServiceImpl::RemoveWriteBackIdsLocation()
{
    {
        std::shared_lock<std::shared_timed_mutex> l(clearIdsMutex_);
        if (voluntaryScaleDownClearIds_.empty()) {
            LOG(INFO) << "RemoveWriteBackIdsLocation: voluntaryScaleDownClearIds_ is empty, skip";
            return Status::OK();
        }
    }

    LOG(INFO) << "RemoveWriteBackIdsLocation begin, WriteBackIds size: " << voluntaryScaleDownClearIds_.size();
    std::vector<std::string> clearIds;
    {
        std::lock_guard<std::shared_timed_mutex> l(clearIdsMutex_);
        clearIds = std::move(voluntaryScaleDownClearIds_);
    }
    std::vector<std::string> removeFailedIds;
    std::vector<std::string> needMigrateDataIds;
    std::vector<std::string> needWaitIds;
    std::vector<std::string> unUseNeedMigrateL2DataIds;
    GroupAndRemoveMeta(clearIds, master::RemoveMetaReqPb::NORMAL, removeFailedIds, needMigrateDataIds, needWaitIds,
                       unUseNeedMigrateL2DataIds);

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
                               needWaitIds, unUseNeedMigrateL2DataIds);
        }
    }
    LOG(INFO) << "RemoveWriteBackIdsLocation finished, removeFailedIds :" << VectorToString(removeFailedIds);
    CHECK_FAIL_RETURN_STATUS(removeFailedIds.empty(), K_RUNTIME_ERROR, "RemoveWriteBackIdsLocation failed");
    return Status::OK();
}

Status WorkerOCServiceImpl::MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                                        std::vector<RpcMessage> payloads)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    return gMigrateProc_->MigrateData(req, rsp, std::move(payloads));
}

Status WorkerOCServiceImpl::MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    return gMigrateProc_->MigrateDataDirect(req, rsp);
}

Status WorkerOCServiceImpl::CloseIncomingMigrationAdmissionAndWait(std::chrono::steady_clock::time_point deadline)
{
    RETURN_OK_IF_TRUE(gMigrateProc_ == nullptr);
    return gMigrateProc_->CloseIncomingMigrationAdmissionAndWait(deadline);
}

Status WorkerOCServiceImpl::MigrateData(const std::vector<std::string> &objectKeys, const std::string &taskId)
{
    DataMigrator migrator(MigrateType::SCALE_DOWN, metadataRoute_, membership_, endpointPolicy_, exitRequested_,
                          localAddress_, akSkManager_, objectTable_, taskId);
    migrator.Init();
    return migrator.Migrate(objectKeys, {});
}

Status WorkerOCServiceImpl::MigrateData(const std::vector<std::string> &objectKeys, const std::string &taskId,
                                        std::chrono::steady_clock::time_point deadline,
                                        const cluster::CancellationToken &cancellation)
{
    DataMigrator migrator(MigrateType::SCALE_DOWN, metadataRoute_, membership_, endpointPolicy_, exitRequested_,
                          localAddress_, akSkManager_, objectTable_, taskId, DataMigrator::UNLIMITED_RETRY_COUNT,
                          deadline, &cancellation);
    migrator.Init();
    return migrator.Migrate(objectKeys, {});
}

Status WorkerOCServiceImpl::MigrateL2CacheData(const std::vector<std::string> &needMigrateL2CacheIds,
                                               const std::string &taskId)
{
    DataMigrator migrator(MigrateType::SCALE_DOWN, metadataRoute_, membership_, endpointPolicy_, exitRequested_,
                          localAddress_, akSkManager_, objectTable_, taskId);
    migrator.Init();
    return migrator.MigrateL2CacheBySlot(needMigrateL2CacheIds);
}

Status WorkerOCServiceImpl::MigrateL2CacheData(const std::vector<std::string> &needMigrateL2CacheIds,
                                               const std::string &taskId,
                                               std::chrono::steady_clock::time_point deadline,
                                               const cluster::CancellationToken &cancellation)
{
    DataMigrator migrator(MigrateType::SCALE_DOWN, metadataRoute_, membership_, endpointPolicy_, exitRequested_,
                          localAddress_, akSkManager_, objectTable_, taskId, DataMigrator::UNLIMITED_RETRY_COUNT,
                          deadline, &cancellation);
    migrator.Init();
    return migrator.MigrateL2CacheBySlot(needMigrateL2CacheIds);
}

void WorkerOCServiceImpl::FindObjectKeyNotInRsp(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                                std::vector<std::string> &currentIds,
                                                std::vector<std::string> &objectKeysNotInRsp)
{
    std::set<std::string> objectKeysReceivedMeta;
    for (auto &queryMeta : queryMetas) {
        (void)objectKeysReceivedMeta.emplace(queryMeta.meta().object_key());
    }
    for (auto &objKey : currentIds) {
        if (objectKeysReceivedMeta.find(objKey) == objectKeysReceivedMeta.end()) {
            VLOG(1) << "objKey " << objKey << " not in rsp";
            objectKeysNotInRsp.emplace_back(std::move(objKey));
        }
    }
}

Status WorkerOCServiceImpl::FillRequestMetaByMaster(const RequestMetaFromWorkerReqPb &req,
                                                    RequestMetaFromWorkerRspPb &rsp)
{
    const auto &masterAddress = req.address();
    HostPort masterAddr;
    RETURN_IF_NOT_OK(masterAddr.ParseString(masterAddress));
    std::vector<std::string> objectKeys;
    GetAllObjectKeys(objectKeys);
    rsp.set_address(localAddress_.ToString());
    ObjectMetaPb *metadata = rsp.add_metas();
    bool isFill = false;
    for (const auto &objectKey : objectKeys) {
        FillMetadata(objectKey, masterAddr, metadata, isFill);
        if (isFill) {
            metadata = rsp.add_metas();
            isFill = false;
        }
    }
    if (!isFill) {
        rsp.mutable_metas()->RemoveLast();
    }

    std::vector<std::string> objKeys;
    FillRefData(masterAddr, objKeys);
    *rsp.mutable_gref_object_keys() = { objKeys.begin(), objKeys.end() };
    return Status::OK();
}

Status WorkerOCServiceImpl::PushMetadataToMaster(const HostPort &masterAddr)
{
    master::PushMetaToMasterReqPb req;
    master::PushMetaToMasterRspPb rsp;
    RETURN_IF_NOT_OK(FillObjData(req, masterAddr));

    std::vector<std::string> objectKeys;
    FillRefData(masterAddr, objectKeys);
    *req.mutable_gref_object_keys() = { objectKeys.begin(), objectKeys.end() };

    VLOG(1) << "PushMetadataToMaster: " << LogHelper::IgnoreSensitive(req);
    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "hash master get failed, PushMeta failed");
    RETURN_IF_NOT_OK(workerMasterApi->PushMetadataToMaster(req, rsp));
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

Status WorkerOCServiceImpl::GetMetaAddressNotCheckConnection(const std::string &objKey, HostPort &masterAddr) const
{
    return metadataRoute_.ResolveOwner(objKey, masterAddr);
}

void WorkerOCServiceImpl::FillMetadata(const std::string &objectKey, const HostPort &targetMasterAddr,
                                       ObjectMetaPb *metadata, bool &isFill)
{
    HostPort masterAddr;
    Status status = metadataRoute_.ResolveOwner(objectKey, masterAddr);
    if (status.IsError()) {
        LOG(ERROR) << FormatString("[ObjectKey %s] FillMetadata may failed, status: %s", objectKey, status.ToString());
        return;
    }
    if (masterAddr != targetMasterAddr) {
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
    configPb->set_write_mode(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetWriteMode()));
    configPb->set_data_format(static_cast<uint32_t>((*currSafeObj)->stateInfo.GetDataFormat()));
    configPb->set_consistency_type(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetConsistencyType()));
    configPb->set_cache_type(static_cast<uint32_t>((*currSafeObj)->modeInfo.GetCacheType()));
    configPb->set_is_replica(!(*currSafeObj)->stateInfo.IsPrimaryCopy());
    isFill = true;
}

Status WorkerOCServiceImpl::FillObjData(master::PushMetaToMasterReqPb &req, const HostPort &masterAddr)
{
    INJECT_POINT("worker.FillObjData.Start");
    req.set_address(localAddress_.ToString());
    std::vector<std::string> objectKeys;
    GetAllObjectKeys(objectKeys);
    ObjectMetaPb *metadata = req.add_metas();
    bool isFill = false;
    for (const auto &objectKey : objectKeys) {
        FillMetadata(objectKey, masterAddr, metadata, isFill);
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

void WorkerOCServiceImpl::FillRefData(const HostPort &targetMasterAddr, std::vector<std::string> &objectKeys)
{
    LOG(INFO) << "Filling Ref data for master address:" << targetMasterAddr.ToString();
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    globalRefTable_->GetAllRef(refTable);
    if (!refTable.empty()) {
        std::vector<std::string> ids;
        std::transform(refTable.begin(), refTable.end(), std::back_inserter(ids), [](auto &kv) { return kv.first; });

        auto filter = [targetMasterAddr, this](const std::string &objectKey) {
            HostPort masterAddr;
            Status status = metadataRoute_.ResolveOwner(objectKey, masterAddr);
            if (status.IsError()) {
                LOG(ERROR) << FormatString("[ObjectKey %s] Get meta address failed: %s", objectKey, status.ToString());
                return true;
            }
            return targetMasterAddr != masterAddr;
        };
        // we only need objectKeys with masterAddr as master
        (void)ids.erase(std::remove_if(ids.begin(), ids.end(), filter), ids.end());

        objectKeys = std::move(ids);
    }
}

Status WorkerOCServiceImpl::RecoverMetadataOfData(const std::vector<std::string> &objectKeys,
                                                  std::vector<std::string> &failedIds, std::string standbyWorker)
{
    if (metadataRecoveryManager_ == nullptr) {
        failedIds = objectKeys;
        MetaDataRecoveryManager::RecoverySummary summary;
        summary.status = Status(K_RUNTIME_ERROR, "metadataRecoveryManager is null");
        summary.requestedCount = objectKeys.size();
        summary.failedIds = objectKeys;
        recoveryState_->SetMetadataRecoverySummary(summary);
        return Status(K_RUNTIME_ERROR, "metadataRecoveryManager is null");
    }
    auto summary = metadataRecoveryManager_->RecoverMetadataWithSummary(objectKeys, standbyWorker);
    failedIds = std::move(summary.failedIds);
    recoveryState_->SetMetadataRecoverySummary(summary);
    return summary.status;
}

Status WorkerOCServiceImpl::FinishRestartMetadataRecovery(const std::string &workerAddr,
                                                          const std::vector<std::string> &matchObjIds,
                                                          const std::vector<std::string> &failedIds)
{
    CHECK_FAIL_RETURN_STATUS(clearDataFlow_ != nullptr, K_NOT_READY, "clear-data flow is not initialized");

    const std::unordered_set<std::string> matchedIds(matchObjIds.begin(), matchObjIds.end());
    std::unordered_set<std::string> seenFailedIds;
    std::vector<std::string> uniqueFailedIds;
    uniqueFailedIds.reserve(failedIds.size());
    for (const auto &objectKey : failedIds) {
        CHECK_FAIL_RETURN_STATUS(matchedIds.count(objectKey) != 0, K_RUNTIME_ERROR,
                                 "metadata recovery returned an unknown failed object");
        if (seenFailedIds.emplace(objectKey).second) {
            uniqueFailedIds.emplace_back(objectKey);
        }
    }

    const size_t initiallyRecovered = matchObjIds.size() - uniqueFailedIds.size();
    auto result = clearDataFlow_->RetryFailedMetadataRecoveryAndClearUnrecoverable(uniqueFailedIds);
    const size_t resolvedCount = initiallyRecovered + result.recoveredCount + result.clearedCount;
    std::ostringstream detail;
    detail << "metadata_recovered=" << initiallyRecovered + result.recoveredCount << "/" << matchObjIds.size()
           << "; cleared_orphan_local=" << result.clearedCount << "; unresolved=" << result.unresolvedCount;
    LOG(INFO) << "Restart metadata ownership reconciliation finished, restart worker: " << workerAddr << ", "
              << detail.str();
    if (result.unresolvedCount == 0 && result.status.IsOk() && resolvedCount == matchObjIds.size()) {
        MarkReconciliationEvidenceReady(detail.str());
        return Status::OK();
    }
    if (result.unresolvedCount == 0) {
        return Status(K_RUNTIME_ERROR, "restart metadata ownership reconciliation result is inconsistent");
    }
    return result.status.IsError() ? result.status
                                   : Status(K_RUNTIME_ERROR, "restart metadata ownership reconciliation unresolved");
}

Status WorkerOCServiceImpl::RecoverMetadataOfRestartedWorker(const std::string &workerAddr)
{
    LOG(INFO) << "Begin to recover metadata of restarted worker: " << workerAddr
              << ", local worker: " << localAddress_.ToString();
    CHECK_FAIL_RETURN_STATUS(topologyRuntime_ != nullptr, K_RUNTIME_ERROR, "topology runtime is null");
    CHECK_FAIL_RETURN_STATUS(metadataRecoveryManager_ != nullptr, K_RUNTIME_ERROR, "metadataRecoveryManager is null");
    HostPort restartedAddress;
    RETURN_IF_NOT_OK(restartedAddress.ParseString(workerAddr));

    INJECT_POINT_NO_RETURN("WorkerOCServiceImpl.BeforeRestartMetadataSelection");
    MetadataRecoverySelector selector(objectTable_);
    std::vector<std::string> matchObjIds;
    selector.Select(
        [this, &restartedAddress](const std::string &objectKey) {
            HostPort metadataOwner;
            return metadataRoute_.ResolveOwner(objectKey, metadataOwner).IsOk() && metadataOwner == restartedAddress;
        },
        true, matchObjIds);
    RETURN_OK_IF_TRUE(matchObjIds.empty());

    std::vector<std::string> failedIds;
    std::string standbyWorker;
    auto rc = RecoverMetadataOfData(matchObjIds, failedIds, standbyWorker);
    return failedIds.empty() ? rc : FinishRestartMetadataRecovery(workerAddr, matchObjIds, failedIds);
}

Status WorkerOCServiceImpl::ClearObject(const ClearDataReqPb &req)
{
    clearDataFlow_->SubmitClearDataAsync(req);
    return Status::OK();
}

Status WorkerOCServiceImpl::HandleNodeRestartEvent(const std::string &workerAddr)
{
    // Restart recovery is required even when failure-time metadata recovery is disabled. A restarted metadata owner
    // cannot rely on persisted metadata in the target architecture, so surviving workers always rebuild it best-effort.
    RETURN_OK_IF_TRUE(workerAddr.empty() || workerAddr == localAddress_.ToString());
    if (threadPool_ == nullptr) {
        return RecoverMetadataOfRestartedWorker(workerAddr);
    }
    auto restartTraceID = Trace::Instance().GetTraceID();
    threadPool_->Execute([this, workerAddr, restartTraceID]() {
        TraceGuard traceGuard = restartTraceID.empty() ? Trace::Instance().SetTraceUUID()
                                                       : Trace::Instance().SetTraceNewID(restartTraceID, true);
        LOG_IF_ERROR(RecoverMetadataOfRestartedWorker(workerAddr),
                     "RecoverMetadataOfRestartedWorker failed after NodeRestartEvent");
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::ValidateWorkerState(ReadLock &noRecon, int reqTimeoutMs, worker::WorkerAdmissionKind kind)
{
    (void)kind;
    Timer timer;
    if (!IsHealthy()) {
        const bool runtimeAdmissionOverridesLegacyGate =
            runtime_ != nullptr && runtime_->GetSnapshot().mode != worker::WorkerServiceMode::RUNNING;
        if (!runtimeAdmissionOverridesLegacyGate) {
            RETURN_STATUS(K_NOT_READY, "Worker not ready");
        }
    }
    using namespace std::chrono;
    static const int SEC_TO_MS = 1000;
    const int totalWaitTimeMs = std::min(30 * SEC_TO_MS, reqTimeoutMs);
    static const int INTERVAL_MS = 10;
    auto start = GetSteadyClockTimeStampMs();

    noRecon.Assign(&reconFlag_);
    bool rc = noRecon.TryLockIfUnlocked();
    bool hasLoggedBeforeWait = false;
    while (!rc && GetSteadyClockTimeStampMs() - start < milliseconds(totalWaitTimeMs).count()) {
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
    if (hasLoggedBeforeWait) {
        LOG(INFO) << "Finished waiting for the reconFlag, elapsed ms: " << timer.ElapsedMilliSecond()
                  << ", setHealthFile: " << setHealthFile_.load() << ", numRecon: " << numRecon_;
    }
    GetWorkerTimeCost().Append("ValidateWorkerState", timer.ElapsedMilliSecond());
    return Status::OK();
}

void WorkerOCServiceImpl::MarkOutOfMemoryIfNeeded(const Status &rc, const std::string &operation,
                                                  memory::CacheType cacheType)
{
    if (runtime_ == nullptr || rc.GetCode() != StatusCode::K_OUT_OF_MEMORY) {
        return;
    }
    if (rc.GetMsg().find("Status inject by") != std::string::npos) {
        return;
    }
    recoveryState_->MarkResourceRecoveryRequired(cacheType);
    runtime_->MarkOutOfMemory(FormatString("%s returned K_OUT_OF_MEMORY: %s", operation, rc.GetMsg()));
}

Status WorkerOCServiceImpl::Create(const CreateReqPb &req, CreateRspPb &resp)
{
    ScopedRequestContext ctx;
    METRIC_TIMER(metrics::KvMetricId::WORKER_PROCESS_CREATE_LATENCY);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::VerifyLeavingState(exitRequested_, FLAGS_enable_leaving_intercept),
                                     "verify leaving state failed");
    ReadLock noRecon;
    INJECT_POINT("worker.Create.beforeValidate");
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    Status rc = createProc_->Create(req, resp);
    if (rc.GetCode() == StatusCode::K_OUT_OF_MEMORY) {
        admissionGuard.reset();
    }
    MarkOutOfMemoryIfNeeded(rc, "Create", static_cast<memory::CacheType>(req.cache_type()));
    if (rc.IsOk()) {
        UpdateWorkerObjectGauge(objectTable_);
        METRIC_ADD(metrics::KvMetricId::WORKER_CREATE_ALLOCATED_BYTES,
                   static_cast<uint64_t>(req.data_size()) + resp.metadata_size());
    }
    return rc;
}

Status WorkerOCServiceImpl::MultiCreate(const MultiCreateReqPb &req, MultiCreateRspPb &resp)
{
    ScopedRequestContext ctx;
    METRIC_TIMER(metrics::KvMetricId::WORKER_PROCESS_CREATE_LATENCY);
    Status returnStatus;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_POSIX_MULTI_CREATE);
    access.ObjectKeysRef(req.object_key());
    Raii raii([&returnStatus, &access]() { access.Result(returnStatus).Record(); });
    returnStatus = worker::VerifyLeavingState(exitRequested_, FLAGS_enable_leaving_intercept);
    if (returnStatus.IsError()) {
        LOG(ERROR) << "verify leaving state failed:" << returnStatus.ToString();
        return returnStatus;
    }
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    returnStatus =
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard);
    if (returnStatus.IsError()) {
        LOG(ERROR) << "acquire admission guard failed:" << returnStatus.ToString();
        return returnStatus;
    }
    returnStatus = ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime());
    if (returnStatus.IsError()) {
        LOG(ERROR) << "validate worker state failed:" << returnStatus.ToString();
        return returnStatus;
    }
    returnStatus = createProc_->MultiCreate(req, resp);
    MarkOutOfMemoryIfNeeded(returnStatus, "MultiCreate");
    if (returnStatus.IsOk()) {
        UpdateWorkerObjectGauge(objectTable_);
        uint64_t totalBytes = 0;
        const int resultCount = resp.results_size();
        for (int i = 0; i < resultCount; ++i) {
            const auto &result = resp.results(i);
            if (result.shm_id().empty()) {
                // existence/NX skip: no memory allocated for this object
                continue;
            }
            const uint64_t dataSize = (i < req.data_size_size()) ? static_cast<uint64_t>(req.data_size(i)) : 0;
            totalBytes += dataSize + static_cast<uint64_t>(result.metadata_size());
        }
        METRIC_ADD(metrics::KvMetricId::WORKER_CREATE_ALLOCATED_BYTES, totalBytes);
    }
    return returnStatus;
}

Status WorkerOCServiceImpl::PrepareRestartReconciliation(const PushMetaToWorkerReqPb &req)
{
    RETURN_OK_IF_TRUE(!req.is_restart());
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
    ClearDisconnectedClientRefsForReconciliation();
    return Status::OK();
}

Status WorkerOCServiceImpl::Reconciliation(const PushMetaToWorkerReqPb &req)
{
    ScopedRequestContext ctx;
    INJECT_POINT("Reconciliation.before");
    GetRequestContext()->reqTimeoutDuration.Init(RPC_TIMEOUT);
    ApiDeadline::Instance().Reset();
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
        reconciledMasters_.clear();
        timestamp_ = req.event_timestamp();
    } else if (req.event_timestamp() < timestamp_) {
        LOG(WARNING) << "The request is out of date. Reconciling for later event. Timestamp: " << timestamp_;
        return Status::OK();
    }
    RETURN_IF_NOT_OK(PrepareRestartReconciliation(req));
    // reconciliation global references with master.
    std::unordered_map<std::string, std::unordered_set<ClientKey>> refTable;
    std::vector<std::string> needDelGrefIds;
    globalRefTable_->GetAllRef(refTable);
    for (const auto &id : req.gref_object_keys()) {
        if (refTable.find(id) == refTable.end()) {
            needDelGrefIds.emplace_back(id);
        }
    }
    if (FLAGS_enable_reconciliation && !req.source_address().empty()) {
        RETURN_IF_NOT_OK(ReconcileGlobalRefsWithSourceMaster(req, refTable, needDelGrefIds));
    } else if (FLAGS_enable_reconciliation && (req.is_restart() || !req.gref_object_keys().empty())) {
        RETURN_STATUS(K_INVALID, "Reconciliation request missing source master address.");
    }
    if (!req.source_address().empty() && reconciledMasters_.insert(req.source_address()).second) {
        ++numRecon_;
    }
    LOG(INFO) << "Reconciliation with master " << req.source_address() << " is done.";
    RETURN_IF_NOT_OK(GetReadyToWork(req));

    return Status::OK();
}

Status WorkerOCServiceImpl::GetReadyToWork(const PushMetaToWorkerReqPb &req)
{
    ScopedRequestContext ctx;
    int hashWorkerNum = 0;
    RETURN_IF_NOT_OK(GetExpectedReconciliationCount(hashWorkerNum));
    const bool hasReconciledWithSource = !req.source_address().empty() && numRecon_ >= 1;
    const bool normalTopologyReconciled =
        (hashWorkerNum > 0 && numRecon_ >= hashWorkerNum) || (hashWorkerNum < 0 && numRecon_ >= 1);
    const bool emptyTopologyRecoveryReconciled = hashWorkerNum == 0 && hasReconciledWithSource;
    if (normalTopologyReconciled || emptyTopologyRecoveryReconciled) {
        LOG(INFO) << "Reconciliation with all masters is done.";
        if (!emptyTopologyRecoveryReconciled) {
            RETURN_IF_NOT_OK(CheckWaitTopologyReady());
        }
        if (req.is_restart()) {
            LOG(INFO) << "Restart finish. Set health file.";
            if (!topologyRuntime_->HasEstablishedMemberLease() && controlBackendAvailableAtStartup_) {
                RETURN_STATUS(K_NOT_READY,
                              "Setting the health file is not allowed before the first lease is successfully created");
            }
            setHealthFile_.store(true);
            RETURN_IF_NOT_OK(SetHealthProbe());
            MarkRestartReconciliationEvidenceReady("restart_reconciliation metadata owners completed");
        }
        if (exitRequested_ != nullptr && exitRequested_->load()) {
            INJECT_POINT("recover.toexiting.delay");
            RETURN_IF_NOT_OK(topologyRuntime_->MarkExiting());
        } else {
            RETURN_IF_NOT_OK(topologyRuntime_->NotifyReconciliationDone());
            if (!req.is_restart()) {
                MarkReconciliationEvidenceReady("membership_reconciliation metadata owners completed");
            }
        }
    } else {
        LOG(INFO) << "Has finished reconciliation master num: " << numRecon_ << ", total expect num: " << hashWorkerNum;
    }
    INJECT_POINT("WorkerOCServiceImpl.Reconciliation.expectedReconNum", [this](int expectedReconNum) {
        if (!setHealthFile_.load() && expectedReconNum > 0 && numRecon_ >= expectedReconNum) {
            setHealthFile_.store(true);
            RETURN_IF_NOT_OK(SetHealthProbe());
            RETURN_IF_NOT_OK(UpdateLocalNodeReady());
        }
        return Status::OK();
    });

    return Status::OK();
}

bool WorkerOCServiceImpl::HasCompleteReconciliationSet(const std::set<std::string> &expected,
                                                       const std::unordered_set<std::string> &completed)
{
    return std::all_of(expected.begin(), expected.end(),
                       [&completed](const std::string &master) { return completed.count(master) != 0; });
}

Status WorkerOCServiceImpl::ReconciliationDecrRef(const HostPort &sourceMasterAddr,
                                                  const std::vector<std::string> &objectKeys)
{
    auto func = [this, &objectKeys, &sourceMasterAddr](int32_t) {
        std::unordered_set<std::string> unAliveIds;
        std::vector<std::string> failDecIds;
        auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(sourceMasterAddr);
        CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                 "hash master get failed, Reconciliation failed");
        return workerMasterApi->GDecreaseMasterRef(objectKeys, unAliveIds, failDecIds);
    };
    constexpr int32_t timeoutMs = 1000 * 60;
    Status rc = RetryOnError(
        timeoutMs, func, []() { return Status::OK(); },
        { StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED, StatusCode::K_RPC_UNAVAILABLE });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Push need deleted metadata to master failed: " + rc.ToString());
    return Status::OK();
}

Status WorkerOCServiceImpl::ReconciliationIncrRef(const HostPort &sourceMasterAddr,
                                                  const std::vector<std::string> &objectKeys)
{
    RETURN_OK_IF_TRUE(objectKeys.empty());
    std::vector<std::string> failedIds;
    auto func = [this, &sourceMasterAddr, &objectKeys, &failedIds](int32_t) {
        failedIds.clear();
        return gRefProc_->GIncreaseMasterRefWithLock(sourceMasterAddr, objectKeys, failedIds);
    };
    constexpr int32_t timeoutMs = 1000 * 60;
    Status rc = RetryOnError(
        timeoutMs, func, []() { return Status::OK(); },
        { StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED, StatusCode::K_RPC_UNAVAILABLE });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rc, "Restore missing gref in master failed: " + rc.ToString());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        failedIds.empty(), K_RUNTIME_ERROR,
        "Restore missing gref in master failed object size: " + std::to_string(failedIds.size()));
    return Status::OK();
}

Status WorkerOCServiceImpl::ReconcileGlobalRefsWithSourceMaster(
    const PushMetaToWorkerReqPb &req, const std::unordered_map<std::string, std::unordered_set<ClientKey>> &refTable,
    const std::vector<std::string> &needDelGrefIds)
{
    HostPort sourceMasterAddr;
    RETURN_IF_NOT_OK(sourceMasterAddr.ParseString(req.source_address()));
    std::unordered_set<std::string> sourceMasterRefIds(req.gref_object_keys().begin(), req.gref_object_keys().end());
    auto needIncGrefIds = CollectMissingSourceMasterRefs(sourceMasterAddr, refTable, sourceMasterRefIds);
    if (!needDelGrefIds.empty()) {
        Status result = ReconciliationDecrRef(sourceMasterAddr, needDelGrefIds);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(result, "Decrease gref in master failed. Error: " + result.ToString());
    }
    if (!needIncGrefIds.empty()) {
        Status result = ReconciliationIncrRef(sourceMasterAddr, needIncGrefIds);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(result, "Increase gref in master failed. Error: " + result.ToString());
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::Get(std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetRspPb, GetReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_READ, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::NORMAL_READ),
        "validate worker state failed");
    return getProc_->Get(serverApi);
}

Status WorkerOCServiceImpl::RefreshMeta(const ClientKey &clientId)
{
    LOG(INFO) << "[RefreshMeta] clear memory reference count for client:" << clientId;
    std::shared_ptr<ClientInfo> clientInfo;
    clientInfo = ClientManager::Instance().GetClientInfo(clientId);
    if (clientInfo == nullptr) {
        LOG(INFO) << FormatString("client id: %s not exist", clientId);
        return Status::OK();
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
            if (metadataSize_ > 0) {
                TryUnlatch(shmUnit->pointer, lockId);
            }
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

#ifdef USE_URMA
    // 4th: Clean up client urma info, assuming there is only one client in the process.
    LOG_IF_ERROR_EXCEPT(RemoveRemoteFastTransportClient(clientId), "", K_NOT_FOUND);
#endif
    return rc;
}

Status WorkerOCServiceImpl::ClearDeviceMetaData(const ClientKey &clientId)
{
    if (gcThreadPool_ != nullptr) {
        auto traceId = Trace::Instance().GetTraceID();
        gcThreadPool_->Execute([this, clientId, traceId] {
            auto traceGuard = Trace::Instance().SetTraceNewID(traceId);
            std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
            auto rc = workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerMasterApi);
            if (rc.IsError()) {
                LOG(ERROR) << "hash master get failed, GetP2P meta failed: " << rc;
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
        GetRequestContext()->reqTimeoutDuration.Init();
        ApiDeadline::Instance().Reset();
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

std::vector<ClientKey> WorkerOCServiceImpl::CollectDisconnectedClientRefIds() const
{
    std::unordered_map<ClientKey, std::vector<std::string>> refTable;
    globalRefTable_->GetAllClientRef(refTable);

    std::vector<ClientKey> disconnectedClientIds;
    for (const auto &clientRefs : refTable) {
        const auto &clientId = clientRefs.first;
        if (ClientManager::Instance().GetClientInfo(clientId) == nullptr) {
            disconnectedClientIds.emplace_back(clientId);
        }
    }
    return disconnectedClientIds;
}

void WorkerOCServiceImpl::ClearDisconnectedClientRefsForReconciliation()
{
    auto disconnectedClientIds = CollectDisconnectedClientRefIds();
    if (disconnectedClientIds.empty()) {
        return;
    }
    LOG(INFO) << "[Ref] Clear disconnected client refs during restart reconciliation, client count: "
              << disconnectedClientIds.size();
    for (const auto &clientId : disconnectedClientIds) {
        AsyncClearClientRef(clientId);
    }
}

std::vector<std::string> WorkerOCServiceImpl::CollectMissingSourceMasterRefs(
    const HostPort &sourceMasterAddr,
    const std::unordered_map<std::string, std::unordered_set<ClientKey>> &localRefTable,
    const std::unordered_set<std::string> &sourceMasterRefIds) const
{
    std::vector<std::string> missingIds;
    for (const auto &localRef : localRefTable) {
        const auto &objectKey = localRef.first;
        if (sourceMasterRefIds.count(objectKey) != 0) {
            continue;
        }
        HostPort masterAddr;
        Status rc = metadataRoute_.ResolveOwner(objectKey, masterAddr);
        if (rc.IsError()) {
            LOG(WARNING) << "[Ref] Skip restoring missing source master ref for " << objectKey
                         << ", failed to locate meta owner: " << rc.ToString();
            continue;
        }
        if (masterAddr == sourceMasterAddr) {
            missingIds.emplace_back(objectKey);
        }
    }
    if (!missingIds.empty()) {
        LOG(INFO) << "[Ref] Restore missing source master refs during reconciliation, source master: "
                  << sourceMasterAddr.ToString() << ", object count: " << missingIds.size();
    }
    return missingIds;
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

Status WorkerOCServiceImpl::WarmupWorkerMasterRpc(const HostPort &masterAddr)
{
    static constexpr int64_t MASTER_RPC_WARMUP_TIMEOUT_MS = 500;
    CHECK_FAIL_RETURN_STATUS(!masterAddr.Empty(), K_INVALID, "Master address is empty.");
    Timer timer;
    auto workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(masterAddr);
    CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR, "GetWorkerMasterApi failed.");

    master::GetObjectLocationsReqPb req;
    master::GetObjectLocationsRspPb rsp;
    auto rc = workerMasterApi->GetObjectLocations(req, rsp, MASTER_RPC_WARMUP_TIMEOUT_MS);
    auto elapsedMs = timer.ElapsedMilliSecond();
    if (rc.IsOk()) {
        LOG(INFO) << FormatString("[MASTER_RPC_WARMUP] success, master=%s, elapsed=%.3f ms", masterAddr.ToString(),
                                  elapsedMs);
    } else {
        LOG(WARNING) << FormatString("[MASTER_RPC_WARMUP] failed, master=%s, elapsed=%.3f ms, status=%s",
                                     masterAddr.ToString(), elapsedMs, rc.ToString());
    }
    return rc;
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
    decreaseRPCQ->Clear();
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
    auto decTraceID = Trace::Instance().GetTraceID();
    decThreadPool_->Execute([this, &dfxTestFunc, decreaseRPCQ, decTraceID]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(decTraceID);
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
    ScopedRequestContext ctx;
    Timer timer;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::CLEANUP_RPC, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::CLEANUP_RPC),
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
    ScopedRequestContext ctx;
    Timer timer;
    std::string tenantId;
    Status authRc = req.is_routed() ? worker::AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId)
                                    : worker::Authenticate(akSkManager_, req, tenantId);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(authRc, "Authenticate failed.");
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
    GetWorkerTimeCost().Append("DecreaseReference", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("[Ref] DecreaseReference finish. The operations of worker DecreaseReference %s",
                              GetWorkerTimeCost().GetInfo());
    return Status::OK();
}

Status WorkerOCServiceImpl::ReconcileShmRef(const ReconcileShmRefReqPb &req, ReconcileShmRefRspPb &resp)
{
    ScopedRequestContext ctx;
    Timer timer;
    std::string tenantId;
    bool exist;
    auto clientId = ClientKey::Intern(req.client_id());
    std::string authTenantId = worker::ClientManager::Instance().GetAuthTenantIdByClientId(clientId, exist);
    CHECK_FAIL_RETURN_STATUS(exist, K_INVALID, FormatString("Client %s not found", req.client_id()));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(AuthenticateRequest(akSkManager_, req, authTenantId, tenantId),
                                     "Authenticate failed");
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::CLEANUP_RPC, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::CLEANUP_RPC),
        "validate worker state failed");

    std::vector<ShmKey> confirmedExpiredShmIds;
    confirmedExpiredShmIds.reserve(req.confirmed_expired_shm_ids_size());
    std::transform(req.confirmed_expired_shm_ids().begin(), req.confirmed_expired_shm_ids().end(),
                   std::back_inserter(confirmedExpiredShmIds), [](const auto &shmId) { return ShmKey::Intern(shmId); });
    std::vector<ShmKey> maybeExpiredShmIds;
    memoryRefTable_->ReconcileClientShmRefs(clientId, confirmedExpiredShmIds, maybeExpiredShmIds);
    for (const auto &shmId : maybeExpiredShmIds) {
        resp.add_maybe_expired_shm_ids(shmId);
    }
    GetWorkerTimeCost().Append("ReconcileShmRef", timer.ElapsedMilliSecond());
    return Status::OK();
}

Status WorkerOCServiceImpl::ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp)
{
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::CLEANUP_RPC, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::CLEANUP_RPC),
        "validate worker state failed");

    return gRefProc_->ReleaseGRefs(req, resp);
}

Status WorkerOCServiceImpl::GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp)
{
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");

    return gRefProc_->GIncreaseRef(req, resp);
}

Status WorkerOCServiceImpl::GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp)
{
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::CLEANUP_RPC, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::CLEANUP_RPC),
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
    std::vector<std::string> objectKeys(objectKeysRemove.begin(), objectKeysRemove.end());
    auto grouped = metadataRoute_.GroupOwners(objectKeys);
    AppendRouteFailures(grouped);
    auto &objKeysGrpByMaster = grouped.groups;

    // Send requests for each master
    for (auto &item : objKeysGrpByMaster) {
        const HostPort &masterAddr = item.first;
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
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    PerfPoint point(PerfKey::WORKER_DELETE_OBJECT);
    if (req.are_device_objects()) {
        return DeleteDevObjects(req, resp);
    }
    return deleteProc_->DeleteAllCopy(req, resp);
}

Status WorkerOCServiceImpl::DeleteCopyNotification(const DeleteObjectReqPb &req, DeleteObjectRspPb &rsp)
{
    ScopedRequestContext ctx;
    return deleteProc_->DeleteCopyNotification(req, rsp);
}

Status WorkerOCServiceImpl::DeletePersistenceObject(const DeletePersistenceObjectReqPb &req,
                                                    DeletePersistenceObjectRspPb &rsp)
{
    ScopedRequestContext ctx;
    return deleteProc_->DeletePersistenceObject(req, rsp);
}

Status WorkerOCServiceImpl::InvalidateBuffer(const InvalidateBufferReqPb &req, InvalidateBufferRspPb &rsp)
{
    ScopedRequestContext ctx;
    Timer timer;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
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
    GetWorkerTimeCost().Append("Total InvalidateBuffer", timer.ElapsedMilliSecond());
    LOG(INFO) << FormatString("InvalidateBuffer end, clientId: %s. The operations of worker InvalidateBuffer %s",
                              req.client_id(), GetWorkerTimeCost().GetInfo());
    return Status::OK();
}

Status WorkerOCServiceImpl::QueryGlobalRefNum(const QueryGlobalRefNumReqPb &req, QueryGlobalRefNumRspCollectionPb &rsp)
{
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
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
    if (!FLAGS_oc_metadata_header) {
        metadataSize_ = 0;
        LOG(INFO) << "Object cache metadata header disabled: shm latch/visibility are unavailable.";
        return;
    }
    constexpr int alignment = 0x8;
    // Worker set lockId_ = 0(shm_guard), so we need client_nums + 1 bits slot.
    metadataSize_ = FLAGS_max_client_num == 0 ? 0 : FLAGS_max_client_num / alignment + 1;
    metadataSize_ += sizeof(uint32_t) + sizeof(char);
    auto alignCeiling = [](uintptr_t addr, uintptr_t alignment) { return (addr + alignment - 1) & ~(alignment - 1); };
    metadataSize_ = alignCeiling(metadataSize_, FLAGS_memory_alignment);
}

size_t WorkerOCServiceImpl::GetMetadataSize() const
{
    return metadataSize_;
}

Status WorkerOCServiceImpl::GetCommittedMemberAddresses(std::vector<std::string> &addresses) const
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership_.GetSnapshot(snapshot));
    std::vector<std::string> candidate;
    candidate.reserve(snapshot->CommittedMembers().size());
    for (const auto *member : snapshot->CommittedMembers()) {
        candidate.emplace_back(member->identity.address);
    }
    addresses = std::move(candidate);
    return Status::OK();
}

Status WorkerOCServiceImpl::GetExpectedReconciliationCount(int &expectedCount) const
{
    if (centralizedMetadata_) {
        expectedCount = -1;
        return Status::OK();
    }
    std::vector<std::string> addresses;
    RETURN_IF_NOT_OK(GetCommittedMemberAddresses(addresses));
    expectedCount = static_cast<int>(addresses.size());
    return Status::OK();
}

Status WorkerOCServiceImpl::CheckTopologyServingReady() const
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership_.GetSnapshot(snapshot));
    const cluster::Member *local = nullptr;
    auto rc = snapshot->FindMemberByAddress(localAddress_.ToString(), local);
    if (rc.GetCode() == K_NOT_FOUND) {
        RETURN_STATUS(K_NOT_READY, "local topology member is not admitted");
    }
    RETURN_IF_NOT_OK(rc);
    const auto state = local->state;
    const bool committed = state == cluster::MemberState::ACTIVE || state == cluster::MemberState::PRE_LEAVING
                           || state == cluster::MemberState::LEAVING;
    CHECK_FAIL_RETURN_STATUS(committed, K_NOT_READY, "local topology member is not committed");
    HostPort owner;
    return metadataRoute_.ResolveOwner(TOPOLOGY_READINESS_PROBE_KEY, owner);
}

Status WorkerOCServiceImpl::ReconcileMembershipChange()
{
    std::vector<std::string> committedAddresses;
    if (!centralizedMetadata_) {
        RETURN_IF_NOT_OK(GetCommittedMemberAddresses(committedAddresses));
    }
    ObjectCacheOwnershipReconciliationPlan plan;
    ObjectCacheOwnershipReconciliationRequest request{ centralizedMetadata_, localMasterAddress_.ToString(),
                                                       localAddress_.ToString(), std::move(committedAddresses),
                                                       OwnershipReconciliationKind::RESTART };
    RETURN_IF_NOT_OK(BuildOwnershipReconciliationPlan(request, plan));
    const int64_t eventTimestamp = std::chrono::system_clock::now().time_since_epoch().count();
    for (const auto &masterAddress : plan.metadataOwners) {
        LOG_IF_ERROR(ScheduleReconciliationRequest(masterAddress, eventTimestamp, ReconciliationQueryPb::RESTART),
                     "Failed to schedule restart reconciliation with metadata owner " + masterAddress);
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::ReconcileLocalIsolationOwnership()
{
    std::vector<std::string> committedAddresses;
    if (!centralizedMetadata_) {
        RETURN_IF_NOT_OK(GetCommittedMemberAddresses(committedAddresses));
    }
    ObjectCacheOwnershipReconciliationPlan plan;
    ObjectCacheOwnershipReconciliationRequest request{ centralizedMetadata_, localMasterAddress_.ToString(),
                                                       localAddress_.ToString(), std::move(committedAddresses),
                                                       OwnershipReconciliationKind::LOCAL_ISOLATION };
    RETURN_IF_NOT_OK(BuildOwnershipReconciliationPlan(request, plan));
    (void)BeginRecoveryEvidenceGeneration(plan.pendingEvidenceDetail);
    const int64_t eventTimestamp = std::chrono::system_clock::now().time_since_epoch().count();
    for (const auto &masterAddress : plan.metadataOwners) {
        LOG_IF_ERROR(
            ScheduleReconciliationRequest(masterAddress, eventTimestamp, ReconciliationQueryPb::LOCAL_ISOLATION),
            "Failed to schedule local-isolation ownership handoff with metadata owner " + masterAddress);
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::ReconcileNetworkRecoveryOwnership()
{
    std::vector<std::string> committedAddresses;
    if (!centralizedMetadata_) {
        RETURN_IF_NOT_OK(GetCommittedMemberAddresses(committedAddresses));
    }
    ObjectCacheOwnershipReconciliationPlan plan;
    ObjectCacheOwnershipReconciliationRequest request{ centralizedMetadata_, localMasterAddress_.ToString(),
                                                       localAddress_.ToString(), std::move(committedAddresses),
                                                       OwnershipReconciliationKind::NETWORK_RECOVERY };
    RETURN_IF_NOT_OK(BuildOwnershipReconciliationPlan(request, plan));
    (void)BeginRecoveryEvidenceGeneration(plan.pendingEvidenceDetail);
    const int64_t eventTimestamp = std::chrono::system_clock::now().time_since_epoch().count();
    for (const auto &masterAddress : plan.metadataOwners) {
        LOG_IF_ERROR(
            ScheduleReconciliationRequest(masterAddress, eventTimestamp, ReconciliationQueryPb::NETWORK_RECOVERY),
            "Failed to schedule network recovery with metadata owner " + masterAddress);
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::ScheduleReconciliationRequest(const std::string &masterAddress, int64_t eventTimestamp,
                                                          ReconciliationQueryPb::EventType eventType)
{
    CHECK_FAIL_RETURN_STATUS(threadPool_ != nullptr && workerMasterApiManager_ != nullptr, K_NOT_READY,
                             "Reconciliation runtime is unavailable");
    HostPort address;
    RETURN_IF_NOT_OK(address.ParseString(masterAddress));
    auto api = workerMasterApiManager_->GetWorkerMasterApi(address);
    CHECK_FAIL_RETURN_STATUS(api != nullptr, K_NOT_READY, "Getting metadata owner API failed: " + masterAddress);
    auto traceId = Trace::Instance().GetTraceID();
    RETURN_IF_EXCEPTION_OCCURS(
        threadPool_->Execute([api = std::move(api), eventTimestamp, eventType, traceId, masterAddress] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            ReconciliationQueryPb req;
            ReconciliationRspPb rsp;
            req.set_event_timestamp(eventTimestamp);
            req.set_event_type(eventType);
            LOG_IF_ERROR(api->ReconcileMembershipChange(req, rsp),
                         "Reconciliation request failed for metadata owner " + masterAddress);
        }));
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
    LOG(INFO) << "Determining startup path for restart: reconciliation and local slot recovery.";
    const bool isRestart = isRestart_;
    if (!isRestart || !controlBackendAvailableAtStartup_ || !FLAGS_enable_reconciliation) {
        RETURN_IF_NOT_OK(CheckWaitTopologyReady());
        LOG(INFO) << "Did not restart so no need to reconcile. Set health file.";
        setHealthFile_.store(true);
        RETURN_IF_NOT_OK(SetHealthProbe());
        if (controlBackendAvailableAtStartup_) {
            RETURN_IF_NOT_OK(UpdateLocalNodeReady());
        }
    } else {
        LOG(INFO) << "Local node restarted. Need reconciliation.";
        MarkRestartReconciliationPending(runtime_, recoveryState_.get(), isRestart_, controlBackendAvailableAtStartup_,
                                         FLAGS_enable_reconciliation);
        RETURN_IF_NOT_OK(ReconcileMembershipChange());
    }
    LOG_IF_ERROR(slotRecoveryManager_->ScheduleLocalPendingTasksFromStore(), "Recover slot failed");
    if (isRestart) {
        LOG_IF_ERROR(slotRecoveryManager_->HandleLocalRestart(), "Recover slot failed");
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::PrepareUrmaWarmupObject(const std::string &objectKey)
{
    CHECK_FAIL_RETURN_STATUS(!objectKey.empty(), K_INVALID, "URMA warmup object key is empty.");
    std::shared_ptr<SafeObjType> entry;
    bool isInsert = false;
    RETURN_IF_NOT_OK(RetryWhenDeadlock([this, &objectKey, &entry, &isInsert] {
        return objectTable_->ReserveGetAndLock(objectKey, entry, isInsert, false, true);
    }));
    (void)isInsert;
    Raii unlock([&entry]() { entry->WUnlock(); });
    if (entry->Get() != nullptr && (*entry)->IsBinary() && (*entry)->IsPublished()
        && !(*entry)->stateInfo.IsCacheInvalid()) {
        LOG(INFO) << FormatString("[URMA_WARMUP] local warmup object already exists, objectKey=%s", objectKey);
        return Status::OK();
    }

    SetNewObjectEntry(objectKey, ConsistencyType::CAUSAL, WriteMode::NONE_L2_CACHE, CacheType::MEMORY,
                      URMA_WARMUP_OBJECT_SIZE, GetMetadataSize(), *entry);
    ObjectKV objectKV(objectKey, *entry);
    char warmupValue = 0;
    RpcMessage payload;
    auto rc = payload.CopyBuffer(&warmupValue, sizeof(warmupValue));
    if (rc.IsError()) {
        (void)objectTable_->Erase(objectKey, *entry);
        return rc;
    }
    std::vector<RpcMessage> payloads;
    payloads.emplace_back(std::move(payload));
    rc = SaveBinaryObjectToMemory(objectKV, payloads, evictionManager_, memCpyThreadPool_);
    if (rc.IsError()) {
        (void)objectTable_->Erase(objectKey, *entry);
        return rc;
    }
    (*entry)->stateInfo.SetNeedToDelete(false);
    (*entry)->SetLifeState(ObjectLifeState::OBJECT_PUBLISHED);
    (*entry)->stateInfo.SetPrimaryCopy(true);
    (*entry)->stateInfo.SetCacheInvalid(false);
    (*entry)->stateInfo.SetIncompleted(false);
    (*entry)->SetCreateTime(0);
    (*entry)->SetTtlSecond(0);
    LOG(INFO) << FormatString("[URMA_WARMUP] local warmup object prepared, objectKey=%s", objectKey);
    INJECT_POINT("WorkerOCServiceImpl.PrepareUrmaWarmupObject.done");
    return Status::OK();
}

Status WorkerOCServiceImpl::WarmupUrmaConnectionToPeer(const std::string &peerAddr, const std::string &peerKey)
{
    CHECK_FAIL_RETURN_STATUS(!peerAddr.empty(), K_INVALID, "URMA warmup peer address is empty.");
    CHECK_FAIL_RETURN_STATUS(!peerKey.empty(), K_INVALID, "URMA warmup peer key is empty.");
    CHECK_FAIL_RETURN_STATUS(getProc_ != nullptr, K_NOT_READY, "Object cache get service is not ready.");
    GetRequestContext()->reqTimeoutDuration.Init(URMA_WARMUP_REQUEST_TIMEOUT_MS);
    ApiDeadline::Instance().Reset();
    return getProc_->WarmupGetObjectFromRemoteWorker(peerAddr, peerKey, URMA_WARMUP_OBJECT_SIZE);
}

Status WorkerOCServiceImpl::GiveUpReconciliation()
{
    RETURN_OK_IF_TRUE(setHealthFile_.load());
    // In case of centralized master, reconciliation is triggered by starting worker. No need to wait for
    // reconciliation requests from master.
    const bool isRestart = isRestart_;
    if (centralizedMetadata_ || !isRestart || !controlBackendAvailableAtStartup_) {
        return Status::OK();
    }
    if (!FLAGS_enable_reconciliation) {
        LOG_FIRST_N(INFO, 1) << "enable_reconciliation is false, set worker healthy";
        return Status::OK();
    }

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
    Timer holdReconFlagTimer;
    std::string finishReason = "unknown";
    Raii logReconFlagCost([&holdReconFlagTimer, &finishReason, this]() {
        constexpr int logPerCount = 10;
        LOG_FIRST_AND_EVERY_N(INFO, logPerCount)
            << "GiveUpReconciliation held reconFlag, elapsed ms: " << holdReconFlagTimer.ElapsedMilliSecond()
            << ", reason: " << finishReason << ", numRecon: " << numRecon_;
    });
    bool shouldSetReady = false;
    RETURN_IF_NOT_OK(CheckGiveUpReconciliationAfterLock(waitMs, finishReason, shouldSetReady));
    if (!shouldSetReady) {
        return Status::OK();
    }

    setHealthFile_.store(true);
    RETURN_IF_NOT_OK(SetHealthProbe());
    RETURN_IF_NOT_OK(UpdateLocalNodeReady());
    return Status::OK();
}

Status WorkerOCServiceImpl::UpdateLocalNodeReady()
{
    return topologyRuntime_->MarkReady();
}

Status WorkerOCServiceImpl::CheckGiveUpReconciliationAfterLock(int64_t waitMs, std::string &finishReason,
                                                               bool &shouldSetReady)
{
    static const int64_t MAX_WAIT_TIME_SEC = 60;  // 60s
    int hashWorkerNum = 0;
    RETURN_IF_NOT_OK(GetExpectedReconciliationCount(hashWorkerNum));
    // no need to set health or not ready to set health
    if (setHealthFile_) {
        finishReason = "health file has already been set";
        return Status::OK();
    }
    if (numRecon_ < hashWorkerNum && GetSteadyClockTimeStampMs() - lastReconTime_ <= waitMs) {
        finishReason = FormatString("waiting for reconciliation, expected: %d", hashWorkerNum);
        return Status::OK();
    }
    if (!topologyRuntime_->HasEstablishedMemberLease()) {
        finishReason = "first keepalive is not sent";
        return Status::OK();
    }
    shouldSetReady = true;
    if (numRecon_ < hashWorkerNum) {
        finishReason = FormatString("give up waiting, expected: %d", hashWorkerNum);
        LOG(ERROR) << "Did not finish reconciling with all masters within " << MAX_WAIT_TIME_SEC << " seconds. Give up."
                   << " Plan to reconciliate with: " << hashWorkerNum << ", had reconciliated with: " << numRecon_;
        LOG(WARNING) << "Topology reconciliation timed out before all expected masters replied.";
    } else {
        finishReason = FormatString("all reconciliations received, expected: %d", hashWorkerNum);
    }
    return Status::OK();
}

Status WorkerOCServiceImpl::CheckWaitTopologyReady()
{
    constexpr int64_t waitIntervalMs = 100;
    constexpr int logPerCount = 10;
    const int64_t timeoutMs =
        std::max<int64_t>(TOPOLOGY_READY_WAIT_TIMEOUT_S, FLAGS_node_timeout_s) * static_cast<int64_t>(SECS_TO_MS);
    Timer timer;
    auto rc = CheckTopologyServingReady();
    while (rc.GetCode() == K_NOT_READY && timer.ElapsedMilliSecond() < static_cast<double>(timeoutMs)
           && !IsTermSignalReceived()) {
        LOG_FIRST_AND_EVERY_N(INFO, logPerCount)
            << "Waiting topology ready before setting worker health, elapsed ms: " << timer.ElapsedMilliSecond()
            << ", status: " << rc.ToString();
        std::this_thread::sleep_for(std::chrono::milliseconds(waitIntervalMs));
        rc = CheckTopologyServingReady();
    }
    return rc;
}

Status WorkerOCServiceImpl::PublishDeviceObject(const PublishDeviceObjectReqPb &req, PublishDeviceObjectRspPb &resp,
                                                std::vector<RpcMessage> payloads)
{
    ScopedRequestContext ctx;
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    std::vector<ShmKey> shmUnits = { ShmKey::Intern(req.shm_id()) };
    RETURN_IF_NOT_OK(
        WorkerOcServiceCrudCommonApi::CheckShmUnitByTenantId(tenantId, clientId, shmUnits, memoryRefTable_));
    PerfPoint point(PerfKey::WORKER_SEAL_OBJECT);
    ReadLock noRecon;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
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
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::shared_ptr<std::optional<RuntimeAdmissionGuard>> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAsyncAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
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

    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
        FormatString("RPC deadline exceeded before dispatch, remaining %ld us.", remainingUs));
    std::string traceID = Trace::Instance().GetTraceID();
    auto dispatchTime = std::chrono::steady_clock::now();
    threadPool_->Execute([objectKeys, serverApi, subTimeout, clientId, remainingUs, dispatchTime, this, traceID,
                          admissionGuard]() mutable {
        (void)admissionGuard;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Processing GetDeviceObject, threads Statistics: " << threadPool_->GetStatistics();
        auto initRc = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
        if (initRc.IsError()) {
            LOG(ERROR) << initRc.GetMsg();
            LOG_IF_ERROR(serverApi->SendStatus(initRc), "Send status failed");
            return;
        }
        workerDevOcManager_->ProcessGetDeviceObjectRequest(objectKeys, serverApi, subTimeout, clientId);
        LOG(INFO) << "Process GetDeviceObject done, threads Statistics: " << threadPool_->GetStatistics();
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::PutP2PMeta(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp)
{
    ScopedRequestContext ctx;
    PerfPoint point(PerfKey::WORKER_PUT_P2PMETA);
    std::string tenantId;
    CHECK_FAIL_RETURN_STATUS(req.dev_obj_meta_size() > 0 && req.dev_obj_meta(0).locations_size() > 0, K_INVALID,
                             "The device meta and locations cannot be empty.");
    const auto clientId = ClientKey::Intern(req.dev_obj_meta(0).locations().begin()->client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerMasterApi));
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
    ScopedRequestContext ctx;
    PerfPoint point(PerfKey::WORKER_SUBSCRIBE_EVENT);
    ReadLock noRecon;
    std::shared_ptr<std::optional<RuntimeAdmissionGuard>> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAsyncAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    SubscribeReceiveEventReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.src_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
        FormatString("RPC deadline exceeded before dispatch, remaining %ld us.", remainingUs));
    std::string traceID = Trace::Instance().GetTraceID();
    auto dispatchTime = std::chrono::steady_clock::now();
    devThreadPool_->Execute([=]() mutable {
        (void)admissionGuard;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Worker processes SubscribeReceiveEvent from srcClientId: " << req.src_client_id()
                  << ", src_device_id: " << req.src_device_id()
                  << ", threads Statistics: " << devThreadPool_->GetStatistics();
        auto initRc = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
        if (initRc.IsError()) {
            LOG(ERROR) << initRc.GetMsg();
            LOG_IF_ERROR(serverApi->SendStatus(initRc), "Send status failed");
            return;
        }
        req.set_worker_ip(localAddress_.Host());
        LOG_IF_ERROR(workerDevOcManager_->ProcessSubscribeReceiveEventRequest(req, serverApi),
                     "Process SubscribeReceiveEvent failed");
        LOG(INFO) << "Process SubscribeReceiveEvent done";
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::GetP2PMeta(
    std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    PerfPoint point(PerfKey::WORKER_GET_P2PMEATA);
    ReadLock noRecon;
    std::shared_ptr<std::optional<RuntimeAdmissionGuard>> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAsyncAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    GetP2PMetaReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    CHECK_FAIL_RETURN_STATUS(req.dev_obj_meta_size() > 0 && req.dev_obj_meta(0).locations_size() > 0, K_INVALID,
                             "The device meta and locations cannot be empty.");
    const auto clientId = ClientKey::Intern(req.dev_obj_meta(0).locations().begin()->client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
        FormatString("RPC deadline exceeded before dispatch, remaining %ld us.", remainingUs));
    std::string traceID = Trace::Instance().GetTraceID();
    auto dispatchTime = std::chrono::steady_clock::now();
    devThreadPool_->Execute([=]() mutable {
        (void)admissionGuard;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << FormatString("Worker processes GetP2PMeta from client: %s, allKeys: [%s], threads Statistics: %s",
                                  clientId, JoinP2PMetaObjectKeys(req), devThreadPool_->GetStatistics());
        auto initRc = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
        if (initRc.IsError()) {
            LOG(ERROR) << initRc.GetMsg();
            LOG_IF_ERROR(serverApi->SendStatus(initRc), "Send status failed");
            return;
        }
        RewriteP2PMetaWorkerAddress(req, localAddress_);
        LOG_IF_ERROR(workerDevOcManager_->ProcessGetP2PMetaRequest(req, serverApi), "Process GetP2PMeta failed");
        LOG(INFO) << "Process GetP2PMeta done";
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::SendRootInfo(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp)
{
    ScopedRequestContext ctx;
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.dst_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerMasterApi));
    SendRootInfoReqPb reqCopy = req;
    return workerMasterApi->SendRootInfo(reqCopy, resp);
}

Status WorkerOCServiceImpl::RecvRootInfo(
    std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    ReadLock noRecon;
    std::shared_ptr<std::optional<RuntimeAdmissionGuard>> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAsyncAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noRecon, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    RecvRootInfoReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.src_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
        FormatString("RPC deadline exceeded before dispatch, remaining %ld us.", remainingUs));
    std::string traceID = Trace::Instance().GetTraceID();
    auto dispatchTime = std::chrono::steady_clock::now();
    devThreadPool_->Execute([=]() mutable {
        (void)admissionGuard;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Worker processes RecvRootInfo from srcClientId: " << req.src_device_id()
                  << ", src_device_id: " << req.src_device_id()
                  << ", threads Statistics: " << devThreadPool_->GetStatistics();
        auto initRc = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
        if (initRc.IsError()) {
            LOG(ERROR) << initRc.GetMsg();
            LOG_IF_ERROR(serverApi->SendStatus(initRc), "Send status failed");
            return;
        }
        LOG_IF_ERROR(workerDevOcManager_->ProcessRecvRootInfoRequest(req, serverApi), "Process RecvRootInfo failed");
        LOG(INFO) << "Process RecvRootInfo done";
    });
    return Status::OK();
}

Status WorkerOCServiceImpl::AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp)
{
    ScopedRequestContext ctx;
    std::string tenantId;
    auto clientId = ClientKey::Intern(req.dst_client_id());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId, clientId),
                                     "Authenticate failed.");
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerMasterApi));
    AckRecvFinishReqPb reqCopy = req;
    return workerMasterApi->AckRecvFinish(reqCopy, resp);
}

Status WorkerOCServiceImpl::RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp)
{
    ScopedRequestContext ctx;
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerMasterApi));
    RemoveP2PLocationReqPb reqCopy = req;
    return workerMasterApi->RemoveP2PLocation(reqCopy, resp);
}

Status WorkerOCServiceImpl::GetDataInfo(
    std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> serverApi)
{
    ScopedRequestContext ctx;
    ReadLock noReconciliation;
    std::shared_ptr<std::optional<RuntimeAdmissionGuard>> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAsyncAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noReconciliation, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    GetDataInfoReqPb req;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(serverApi->Read(req), "serverApi read request failed");
    std::string tenantId;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker::Authenticate(akSkManager_, req, tenantId), "Authenticate failed.");
    auto objectKey = req.object_key();
    int64_t subTimeout = req.sub_timeout();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsInNonNegativeInt32(subTimeout), K_RUNTIME_ERROR,
                                         "SubTimeout is out of range.");
    int64_t remainingUs = GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTimeUs();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        remainingUs > 0, K_RPC_DEADLINE_EXCEEDED,
        FormatString("RPC deadline exceeded before dispatch, remaining %ld us.", remainingUs));
    std::string traceID = Trace::Instance().GetTraceID();
    auto dispatchTime = std::chrono::steady_clock::now();
    devThreadPool_->Execute([=]() mutable {
        (void)admissionGuard;
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
        LOG(INFO) << "Worker processes GetDataInfo from object: " << objectKey
                  << ", threads Statistics: " << devThreadPool_->GetStatistics();
        auto initRc = InitTimeoutsFromDispatch(remainingUs, dispatchTime);
        if (initRc.IsError()) {
            LOG(ERROR) << initRc.GetMsg();
            LOG_IF_ERROR(serverApi->SendStatus(initRc), "Send status failed");
            return;
        }
        LOG_IF_ERROR(workerDevOcManager_->ProcessGetDataInfoRequest(req, serverApi, subTimeout),
                     "Process GetDataInfo failed");
        LOG(INFO) << "Process GetDataInfo done";
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
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_READ, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noReconciliation, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::NORMAL_READ),
        "validate worker state failed");
    return getProc_->GetObjMetaInfo(req, resp);
}

Status WorkerOCServiceImpl::QuerySize(const QuerySizeReqPb &req, QuerySizeRspPb &rsp)
{
    ScopedRequestContext ctx;
    ReadLock noReconciliation;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noReconciliation, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    return getProc_->QuerySize(req, rsp);
}

Status WorkerOCServiceImpl::Exist(const ExistReqPb &req, ExistRspPb &rsp)
{
    ScopedRequestContext ctx;
    ReadLock noReconciliation;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_READ, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noReconciliation, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime(),
                            worker::WorkerAdmissionKind::NORMAL_READ),
        "validate worker state failed");
    PerfPoint perfPoint(PerfKey::WORKER_EXIST);
    return getProc_->Exist(req, rsp);
}

Status WorkerOCServiceImpl::Expire(const ExpireReqPb &req, ExpireRspPb &rsp)
{
    ScopedRequestContext ctx;
    ReadLock noReconciliation;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noReconciliation, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    return expireProc_->Expire(req, rsp);
}

Status WorkerOCServiceImpl::GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp)
{
    ReadLock noReconciliation;
    std::optional<RuntimeAdmissionGuard> admissionGuard;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        AcquireObjectCacheAdmissionGuard(runtime_, worker::WorkerAdmissionKind::NORMAL_WRITE, admissionGuard),
        "acquire admission guard failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        ValidateWorkerState(noReconciliation, GetRequestContext()->reqTimeoutDuration.CalcRemainingTime()),
        "validate worker state failed");
    return getProc_->GetMetaInfo(req, rsp);
};

Status WorkerOCServiceImpl::GetHashRing(const GetHashRingReqPb &req, GetHashRingRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_RUNTIME_ERROR_IF_NULL(akSkManager_);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    RETURN_RUNTIME_ERROR_IF_NULL(topologyRuntime_);
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(membership_.GetSnapshot(snapshot));
    auto loadHostIds = [this](RoutingHostIdMap &hostIdMap) {
        std::unordered_map<std::string, std::string> hostIds;
        RETURN_IF_NOT_OK(topologyRuntime_->GetRoutingHostIds(hostIds));
        hostIdMap.insert(hostIds.begin(), hostIds.end());
        return Status::OK();
    };
    return BuildGetHashRingResponse(*snapshot, req.version(), FLAGS_master_address, loadHostIds, rsp);
}

Status WorkerOCServiceImpl::DeleteDevObjects(const DeleteAllCopyReqPb &req, DeleteAllCopyRspPb &resp)
{
    LOG(INFO) << "Worker delete device objects: " << VectorToString(req.object_keys());
    std::shared_ptr<WorkerMasterOCApi> workerMasterApi;
    RETURN_IF_NOT_OK(workerMasterApiManager_->GetWorkerMasterApi(P2P_DEFAULT_MASTER, workerMasterApi));
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

void WorkerOCServiceImpl::InitShmRefForClient(const ClientKey &clientId, bool supportMultiShmRefCount)
{
    memoryRefTable_->InitShmRefForClient(clientId, supportMultiShmRefCount);
}

Status WorkerOCServiceImpl::NotifyRemoteGet(const NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp)
{
    ScopedRequestContext ctx;
    RETURN_IF_NOT_OK(gMigrateProc_->AcquireIncomingMigrationAdmission());
    Raii admission([this] { gMigrateProc_->ReleaseIncomingMigrationAdmission(); });
    INJECT_POINT_NO_RETURN("WorkerOCServiceImpl.NotifyRemoteGet.afterAdmission");
    if (gMigrateProc_->IsIncomingMigrationAdmissionClosed()) {
        return Status(StatusCode::K_NOT_READY, "admission closed before data processing");
    }
    std::unordered_set<std::string> objectKeySet(req.object_keys().begin(), req.object_keys().end());
    QueryMetaMap queryMetas;
    std::unordered_set<std::string> failedIds;
    RETURN_IF_NOT_OK(gMigrateProc_->QueryMasterMetadata(objectKeySet, queryMetas, failedIds));

    // Add query-failed objects to response
    rsp.mutable_failed_object_keys()->Add(failedIds.begin(), failedIds.end());

    Status rc = getProc_->NotifyRemoteGet(req, std::move(queryMetas), rsp);
    if (rc.IsOk() && gMigrateProc_->IsIncomingMigrationDrainTimedOut()) {
        // Data may already be written to target; intentionally return failure so Source
        // keeps its local copy, accepting a transient double-write rather than data loss.
        LOG(WARNING) << "[NotifyRemoteGet] Drain timed out during processing; "
                     << "returning K_NOT_READY so Source keeps its local copy "
                     << "(target may already have the data; transient double-write is expected)";
        return Status(StatusCode::K_NOT_READY, "admission drain timed out during processing");
    }

    // Set remain_bytes based on current available memory
    // If any object was successfully processed, set remain_bytes even if there were some failures
    // This allows the client to continue sending more data based on the remaining memory
    uint64_t originalFailedCount = rsp.failed_object_keys_size();
    bool hasSuccess = false;
    if (originalFailedCount < static_cast<uint64_t>(req.object_keys_size())) {
        hasSuccess = true;
    }

    if (hasSuccess || rc.GetCode() != StatusCode::K_OUT_OF_MEMORY) {
        rsp.set_remain_bytes(memory::Allocator::Instance()->GetMemoryAvailToHighWater());
    } else {
        rsp.set_remain_bytes(0);
    }

    // Return the last error code encountered
    // If some objects succeeded but later ones failed due to OOM, we still want to return OK
    // so the caller knows which objects succeeded (they're not in failed_object_keys)
    // But if all objects failed due to OOM, return K_OUT_OF_MEMORY
    if (!hasSuccess || rc.GetCode() == StatusCode::K_OUT_OF_MEMORY) {
        return rc;
    }
    return Status::OK();
}
}  // namespace object_cache
}  // namespace datasystem
