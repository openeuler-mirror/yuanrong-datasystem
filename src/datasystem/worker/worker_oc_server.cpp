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
 * Description: Implementation of worker server.
 */
#include "datasystem/worker/worker_oc_server.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <unistd.h>
#include <fcntl.h>

#include "datasystem/cluster/coordination_backend/ds_coordination_backend.h"
#include "datasystem/cluster/coordination_backend/etcd_coordination_backend.h"
#include "datasystem/cluster/membership/membership_value_codec.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/flags/dynamic_config_updater.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/l2cache/slot_client/slot_file_util.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_health.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/string_intern/string_pool.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/brpc_stream_close_helper.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/protobuf_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/master/object_cache/oc_migrate_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/protos/worker_object.service.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_event_type.h"
#include "datasystem/worker/object_cache/central_metadata_address_resolver.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/worker_topology_object_cache_actions.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"
#include "datasystem/worker/object_cache/worker_worker_peer_state_codec.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "datasystem/worker/worker_health_check.h"
#include "datasystem/worker/worker_liveness_check.h"
#include "datasystem/worker/runtime/worker_topology_availability_admission.h"
#include "datasystem/worker/runtime/worker_control_backend_probe.h"
#include "datasystem/worker/rebalance_executor.h"
#include "datasystem/worker/runtime/worker_topology_phase_callbacks.h"

DS_DECLARE_bool(use_brpc);
DS_DECLARE_string(coordinator_address);
DS_DEFINE_string(master_address, "", "Address of ds master and the value cannot be empty.");
DS_DEFINE_bool(enable_distributed_master, true, "Whether to support distributed master, default is true.");
DS_DEFINE_uint32_dynamic(add_node_wait_time_s, 60, "Wait time (s) for the first node joining a working hash ring.");
DS_DECLARE_bool(auto_del_dead_node);
DS_DEFINE_bool(enable_p2p_transfer, false, "Heterogeneous object transfer protocol Enables p2ptransfer");
#ifdef WITH_TESTS
DS_DEFINE_uint64(shared_memory_size_mb, 64,
                 "Upper limit of the shared memory, the unit is mb, must be greater than 0.");
#else
DS_DEFINE_uint64(shared_memory_size_mb, 1024,
                 "Upper limit of the shared memory, the unit is mb, must be greater than 0.");
#endif
DS_DEFINE_uint64(shared_disk_size_mb, 0, "Upper limit of the shared disk, the unit is mb.");

#ifdef WITH_TESTS
DS_DEFINE_uint64(sc_local_cache_memory_size_mb, 128,
                 "Upper limit of the shared memory, the unit is mb, must be greater than 0.");
#else
DS_DEFINE_uint64(sc_local_cache_memory_size_mb, 1024,
                 "Upper limit of the SC local cache, the unit is mb, must be greater than 0.");
#endif

DS_DEFINE_uint32(oc_shm_threshold_percentage, 100,
                 "Upper limit of the shared memory in percentage can be used by OC, must be within (0, 100]");
DS_DEFINE_uint32(sc_shm_threshold_percentage, 100,
                 "Upper limit of the shared memory in percentage can be used by SC, must be within (0, 100].");
DS_DEFINE_uint32(page_size, 1024 * 1024,
                 "Size of the page used for caching worker files. The valid range is 4096-1073741824.");
DS_DEFINE_bool(ipc_through_shared_memory, true, "Using shared memory to exchange data between client and worker.");
DS_DECLARE_bool(authorization_enable);
DS_DEFINE_string(ready_check_path, "",
                 "This file will be created after the worker service is started successfully "
                 "and a connection can be established using the stub. It is used to "
                 "detect whether the container is ready in the k8s scenario. In default, \"ready\" is filename"
                 "The path length should less than 4095 characters.");
DS_DEFINE_string(system_access_key, "", "The access key for system component AK/SK authentication.");
DS_DEFINE_string(system_secret_key, "", "The secret key for system component AK/SK authentication.");
DS_DEFINE_string(system_data_key, "", "The data key for system encrypte and decrypt secert key.");
DS_DEFINE_string(tenant_access_key, "", "The access key for tenant AK/SK authentication.");
DS_DEFINE_string(tenant_secret_key, "", "The secret key for tenant AK/SK authentication.");
DS_DEFINE_uint32(request_expire_time_s, 300,
                 "When AK/SK authentication is used, if the duration from the client to the server is longer than the "
                 "value of this parameter, the authentication fails and the service is denied.");
DS_DEFINE_validator(ready_check_path, &Validator::ValidatePathString);
DS_DEFINE_validator(shared_memory_size_mb, &Validator::ValidateSharedMemSize);
DS_DEFINE_validator(shared_disk_size_mb, &Validator::ValidateSharedDiskSize);
DS_DEFINE_validator(sc_local_cache_memory_size_mb, &Validator::ValidateLocalCacheMemSize);
DS_DEFINE_validator(page_size, &Validator::ValidatePageSize);
DS_DECLARE_int32(sc_regular_socket_num);
DS_DECLARE_int32(sc_stream_socket_num);
DS_DECLARE_string(unix_domain_socket_dir);
DS_DECLARE_string(etcd_address);
DS_DECLARE_string(monitor_config_file);
DS_DECLARE_string(worker_address);
DS_DECLARE_string(master_address);
DS_DECLARE_string(metastore_address);
DS_DEFINE_bool(start_metastore_service, false,
               "Start metastore service on master worker to replace external etcd server for cluster worker "
               "metadata storage, default is false.");
DS_DEFINE_bool_dynamic(async_delete, false, "Master notify workers to delete objects asynchronously.");
DS_DEFINE_uint32(memory_reclamation_time_second, 600, "The memory reclamation time after free.");
DS_DECLARE_uint32(node_timeout_s);
DS_DEFINE_bool(cross_cluster_get_data_from_worker, true,
               "[DEPRECATED] Cross-cluster data access from workers has been removed. This flag is kept for "
               "compatibility and is ignored.");
DS_DEFINE_string(liveness_check_path, "",
                 "File will create after the worker check liveness successfully"
                 "It is used to detect whether the container is liveness in the k8s scenario."
                 "The path length must less than 4095 characters.");
DS_DEFINE_validator(liveness_check_path, &Validator::ValidatePathString);
DS_DEFINE_uint32(liveness_probe_timeout_s, 150, "Liveness probe timeout in seconds.");
DS_DEFINE_uint32(check_async_queue_empty_time_s, 1,
                 "The async queue needs to be empty for a certain period of time before worker can exist.");
DS_DECLARE_string(rocksdb_store_dir);
DS_DEFINE_string(sc_encrypt_secret_key, "",
                 "The encrypted secret key for stream cache. The key length is up to 1024 bytes and must be 32 bytes "
                 "after decryption.");
DS_DEFINE_validator(sc_encrypt_secret_key, &Validator::ValidateScEncryptSecretKey);
DS_DEFINE_int32(max_rpc_session_num, 2048,
                "Maximum number of sessions that can be cached, must be within [512, 10'000]");
DS_DEFINE_validator(max_rpc_session_num, &Validator::ValidateMaxRpcSessionNum);
DS_DEFINE_bool_dynamic(
    enable_lossless_data_exit_mode, false,
    "Decide whether to migrate data to other nodes or not when current node exits, default is false.");
DS_DEFINE_bool(shared_memory_populate, false,
               "Avoiding page faults during copying improves runtime performance but may result in longer worker "
               "startup times (depending on shared_memory_size_mb).");

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(sfs_path);
DS_DECLARE_string(distributed_disk_path);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(log_dir);

DS_DECLARE_string(etcd_username);
DS_DECLARE_string(etcd_password);
DS_DECLARE_uint32(etcd_token_refresh_interval_s);

namespace datasystem {
namespace worker {
constexpr int32_t LIGHTWEIGHT_SERVICE_THREAD_NUM = 4;  // Smaller number of service threads.
constexpr int32_t DEFAULT_STREAM_SOCKET_NUM = 8;       // The default stream socket number.
constexpr double DFT_TIMEOUT_MULT = 1.0;          // A default timeout multiplier iartWorkerf file cache HA is used.
constexpr int32_t THREAD_POOL_SIZE_LIMIT = 4096;  // size limit of the thread pool
constexpr int32_t CHECK_ASYNC_SLEEP_TIME_S = 1;   // Check async task time interval.
constexpr int32_t WARMUP_THREAD_NUM = 4;
constexpr int64_t WARMUP_SCAN_INTERVAL_MS = 1'000;
constexpr int64_t WARMUP_MIN_SCAN_MS = 30'000;
constexpr int64_t WARMUP_MAX_SCAN_MS = 110'000;
constexpr uint32_t WARMUP_STABLE_ROUNDS = 3;
constexpr int32_t MASTER_RPC_WARMUP_THREAD_NUM = 4;
constexpr auto TOPOLOGY_CALLBACK_POLL_INTERVAL = std::chrono::milliseconds(100);
constexpr auto TOPOLOGY_MEMBERSHIP_POLL_INTERVAL = std::chrono::milliseconds(100);
constexpr auto TOPOLOGY_STOP_GRACE = std::chrono::seconds(10);
static const std::string WORKER_OC_SERVER = "WorkerOcServer";
static const std::string URMA_WARMUP_KEY_PREFIX = "_urma_";
constexpr char TOPOLOGY_READINESS_PROBE_KEY[] = "topology-readiness-probe";

namespace {
/**
 * @brief Verify local committed membership and placement readiness using one immutable Snapshot.
 * @param[in] engine Running topology Engine that owns the immutable Snapshot.
 * @param[in] localAddress Canonical local Worker address.
 * @return K_OK when the local member and placement are ready; K_NOT_READY or placement status otherwise.
 */
Status CheckLocalTopologyServingReady(cluster::TopologyEngine &engine, const std::string &localAddress)
{
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    RETURN_IF_NOT_OK(engine.GetSnapshot(snapshot));
    const cluster::Member *local = nullptr;
    auto rc = snapshot->FindMemberByAddress(localAddress, local);
    if (rc.GetCode() == K_NOT_FOUND) {
        RETURN_STATUS(K_NOT_READY, "local topology member is not admitted");
    }
    RETURN_IF_NOT_OK(rc);
    const bool committed = local->state == cluster::MemberState::ACTIVE
                           || local->state == cluster::MemberState::PRE_LEAVING
                           || local->state == cluster::MemberState::LEAVING;
    CHECK_FAIL_RETURN_STATUS(committed, K_NOT_READY, "local topology member is not committed");
    cluster::PlacementDecision decision;
    return engine.Placement().Locate(TOPOLOGY_READINESS_PROBE_KEY, decision);
}

bool IsWorkerScopedSlotStoreEnabled()
{
    return FLAGS_l2_cache_type == "distributed_disk";
}

bool EnableOCService()
{
    return FLAGS_rpc_thread_num > 0;
}

bool EnableSCService()
{
    return FLAGS_sc_regular_socket_num > 0 && FLAGS_sc_stream_socket_num > 0;
}

bool IsWarmupKeyChar(unsigned char c)
{
    if (std::isalnum(c) != 0) {
        return true;
    }
    static constexpr char allowedSymbols[] = "-_!@#%^*()+=:;";
    for (char symbol : allowedSymbols) {
        if (symbol == '\0') {
            break;
        }
        if (static_cast<unsigned char>(symbol) == c) {
            return true;
        }
    }
    return false;
}

std::string BuildWarmupKey(const std::string &workerAddr)
{
    std::string encoded = workerAddr;
    std::replace_if(encoded.begin(), encoded.end(), [](unsigned char c) { return !IsWarmupKeyChar(c); }, '_');
    return URMA_WARMUP_KEY_PREFIX + encoded;
}

class WorkerRpcControlBackendProbeClient : public IControlBackendPeerProbeClient {
public:
    explicit WorkerRpcControlBackendProbeClient(std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi> api)
        : api_(std::move(api))
    {
    }

    Status Start(int32_t timeoutMs, int64_t &tag) override
    {
        GetClusterStateReqPb request;
        return api_->GetClusterStateAsyncWrite(request, timeoutMs, tag);
    }

    Status Finish(const cluster::MemberIdentity &peer, int64_t tag,
                  cluster::ControlBackendObservation &observation) override
    {
        GetClusterStateRspPb response;
        RETURN_IF_NOT_OK(api_->GetClusterStateAsyncRead(tag, response));
        return object_cache::FillControlBackendObservationFromGetClusterStateRspPb(peer.address, response, observation);
    }

private:
    std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi> api_;
};

void SleepForWarmupScanInterval(const std::atomic<bool> &exitFlag, std::condition_variable &exitCv,
                                std::mutex &exitMutex)
{
    std::unique_lock<std::mutex> lock(exitMutex);
    exitCv.wait_for(lock, std::chrono::milliseconds(WARMUP_SCAN_INTERVAL_MS),
                    [&exitFlag]() { return exitFlag.load(); });
}
}  // namespace

static bool ValidateEtcdOrMetastoreAddress()
{
    if (FLAGS_metastore_address.empty() && FLAGS_etcd_address.empty()) {
        LOG(ERROR) << "At least one of etcd_address or metastore_address must be specified";
        return false;
    }
    if (!FLAGS_metastore_address.empty() && !FLAGS_etcd_address.empty()) {
        LOG(ERROR) << "Only one of etcd_address or metastore_address can be specified, not both";
        return false;
    }
    return true;
}

WorkerOCServer::WorkerOCServer(HostPort workerAddr, HostPort bindAddr, HostPort masterAddr,
                               std::shared_ptr<ICoordinatorDiscovery> coordinatorDiscovery)
    : CommonServer(std::move(workerAddr), std::move(bindAddr)),
      coordinatorDiscovery_(std::move(coordinatorDiscovery)),
      masterAddr_(std::move(masterAddr))
{
}

WorkerOCServer::~WorkerOCServer()
{
    StopConnectionWarmup();
    StopWorkerMasterRpcWarmup();
    StopRebalanceExecutor();
    StopLivenessCheck();
    object_cache::NodeSelector::Instance().Shutdown();
    if (rpcServer_) {
        rpcServer_->Shutdown();
    }
    checkThreadRunning_ = false;
    if (checkAsyncTasksThread_ != nullptr) {
        checkAsyncTasksThread_->join();
        checkAsyncTasksThread_.reset();
    }
    if (clientsExitChecker_ != nullptr) {
        clientsExitChecker_->join();
        clientsExitChecker_.reset();
    }
    const auto topologyStopDeadline = std::chrono::steady_clock::now() + TOPOLOGY_STOP_GRACE;
    const auto topologyStopStatus = StopTopologyRuntime(topologyStopDeadline);
    if (topologyStopStatus.IsError()) {
        LOG(WARNING) << "CLUSTER_LIFECYCLE role=worker state=bounded_shutdown_failed action=final_safe_join status="
                     << topologyStopStatus.ToString();
        LOG_IF_ERROR(StopTopologyRuntime(std::chrono::steady_clock::time_point::max()),
                     "CLUSTER_LIFECYCLE role=worker state=final_safe_join_failed");
    }
    if (metadataManagerHolder_ != nullptr) {
        metadataManagerHolder_->Shutdown();
    }

    // Stop brpc server and drain bthread handlers BEFORE destroying adapters.
    // C++ destroys derived-class members before running the base-class destructor,
    // so without this explicit call, brpc adapters (unique_ptr members of
    // WorkerOCServer) would be freed while bthread handlers may still reference
    // them via brpc service dispatch — causing use-after-free in
    // TenantAuthManager, GetImpl, Stream, etc.
    // The normal Shutdown() path calls CommonServer::Shutdown() → RpcServer::Shutdown()
    // → StopBrpcServer() before adapter destruction. The destructor path (abnormal
    // exit, exception unwind) must replicate this ordering.
    // StopBrpcServer() is idempotent (resets brpcServer_ after Stop+Join), so calling
    // it here has no effect when ~RpcServer() later calls StopBrpcServer() again.
    builder_ = RpcServer::Builder();
    objCacheMasterSvc_.reset();
    objCacheWorkerWkSvc_.reset();
    objCacheWorkerMsSvc_.reset();
    brpcOcAdapter_.reset();
    brpcMasterAdapter_.reset();
    brpcWorkerAdapter_.reset();
    brpcWorkerWorkerOcAdapter_.reset();
    brpcWorkerWorkerTransAdapter_.reset();
    brpcMasterWorkerOcAdapter_.reset();
    brpcMasterOcAdapter_.reset();
    brpcMasterScAdapter_.reset();
    brpcClientWorkerScAdapter_.reset();
    brpcWorkerWorkerScAdapter_.reset();
    brpcMasterWorkerScAdapter_.reset();
    objCacheClientWorkerSvc_.reset();
    objCacheWorkerTransSvc_.reset();
    streamCacheWorkerWorkerSvc_.reset();
    streamCacheClientWorkerSvc_.reset();
    streamCacheMasterSvc_.reset();

    // Drain deferred cleanup queue AFTER all adapters/services have been
    // destroyed.  Adapter destructors may trigger streaming client cleanup
    // (StreamCloseAndDrain → EnqueueDeferredCleanup), which would restart
    // the reaper if Stop were called too early.
    StopDeferredCleanupReaper();
    objectEndpointPolicy_.reset();
    metadataRouteResolver_.reset();
    // ResourceManager owns the scheduler that borrows topologyEngine_->Membership(). Destroy and join it before the
    // topology engine so no scheduler access can outlive the borrowed membership view.
    resourceManager_.reset();
    // All Engine threads and business borrowers are stopped. Destroy Engine while Store/Proxy and callback owners are
    // still alive, and before the process allocator is shut down.
    topologyEngine_.reset();
    datasystem::memory::Allocator::Instance()->Shutdown();
}

void WorkerOCServer::InitSlotWorkerNamespace()
{
    if (!IsWorkerScopedSlotStoreEnabled()) {
        return;
    }
    SetSlotWorkerNamespace(SanitizeSlotWorkerNamespace(FLAGS_worker_address));
}

Status WorkerOCServer::InitSlotRecovery()
{
    RETURN_OK_IF_TRUE(!IsWorkerScopedSlotStoreEnabled());
    RETURN_OK_IF_TRUE(etcdStore_ == nullptr);
    slotRecoveryOrchestrator_ = std::make_unique<object_cache::SlotRecoveryOrchestrator>(FLAGS_distributed_disk_path);
    RETURN_IF_NOT_OK(slotRecoveryOrchestrator_->Init());
    return slotRecoveryOrchestrator_->RepairLocalSlots();
}

Status WorkerOCServer::InitWorkerOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheClientWorkerSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcOcAdapter_ = std::make_unique<WorkerOCServiceBrpcAdapter>(*objCacheClientWorkerSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcOcAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        cfg.udsEnabled_ = FLAGS_ipc_through_shared_memory;
        builder_.AddService(objCacheClientWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitWorkerWorkerOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheWorkerWkSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcWorkerWorkerOcAdapter_ = std::make_unique<WorkerWorkerOCServiceBrpcAdapter>(*objCacheWorkerWkSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcWorkerWorkerOcAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            Validator::ValidatePort("FLAGS_oc_worker_worker_direct_port", FLAGS_oc_worker_worker_direct_port),
            K_INVALID, FormatString("Invalid tcp/ip port value %d", FLAGS_oc_worker_worker_direct_port));
        cfg.tcpDirect_ = std::to_string(FLAGS_oc_worker_worker_direct_port);
        builder_.AddService(objCacheWorkerWkSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitWorkerWorkerTransportService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheWorkerTransSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcWorkerWorkerTransAdapter_ =
            std::make_unique<WorkerWorkerTransportServiceBrpcAdapter>(*objCacheWorkerTransSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcWorkerWorkerTransAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = LIGHTWEIGHT_SERVICE_THREAD_NUM;
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
        builder_.AddService(objCacheWorkerTransSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitMasterWorkerOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheWorkerMsSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcMasterWorkerOcAdapter_ = std::make_unique<MasterWorkerOCServiceBrpcAdapter>(*objCacheWorkerMsSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcMasterWorkerOcAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        builder_.AddService(objCacheWorkerMsSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::CheckScEncryptSecretKey()
{
    if (FLAGS_sc_encrypt_secret_key.empty() || !SecretManager().Instance()->IsRootKeyActive()) {
        return Status::OK();
    }
    std::unique_ptr<char[]> keyContent;
    int outSize;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        SecretManager::Instance()->Decrypt(FLAGS_sc_encrypt_secret_key, keyContent, outSize),
        "Sc encrypt secret key decrypt failed.");
    (void)memset_s(keyContent.get(), outSize, 0, outSize);
    const int AES_256_GCM_KEY_LEN = 32;
    CHECK_FAIL_RETURN_STATUS(outSize == AES_256_GCM_KEY_LEN, StatusCode::K_INVALID,
                             "The decrypted length is incorrect.");
    return Status::OK();
}

Status WorkerOCServer::InitClientWorkerSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheClientWorkerSvc_->Init());
    CHECK_FAIL_RETURN_STATUS(FLAGS_sc_stream_socket_num + FLAGS_sc_regular_socket_num <= THREAD_POOL_SIZE_LIMIT,
                             StatusCode::K_INVALID,
                             "The number of service threads exceeds the upper limit, please adjust it");
    RETURN_IF_NOT_OK(CheckScEncryptSecretKey());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcClientWorkerScAdapter_ = std::make_unique<ClientWorkerSCServiceBrpcAdapter>(*streamCacheClientWorkerSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcClientWorkerScAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = FLAGS_sc_regular_socket_num;
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        cfg.udsEnabled_ = FLAGS_ipc_through_shared_memory;
        builder_.AddService(streamCacheClientWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitWorkerWorkerSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheWorkerWorkerSvc_->Init());
    CHECK_FAIL_RETURN_STATUS(FLAGS_sc_stream_socket_num + FLAGS_sc_regular_socket_num <= THREAD_POOL_SIZE_LIMIT,
                             StatusCode::K_INVALID,
                             "The number of service threads exceeds the upper limit, please adjust it");
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcWorkerWorkerScAdapter_ = std::make_unique<WorkerWorkerSCServiceBrpcAdapter>(*streamCacheWorkerWorkerSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcWorkerWorkerScAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = FLAGS_sc_regular_socket_num;
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            Validator::ValidatePort("FLAGS_sc_worker_worker_direct_port", FLAGS_sc_worker_worker_direct_port),
            K_INVALID, FormatString("Invalid tcp/ip port value %d", FLAGS_sc_worker_worker_direct_port));
        cfg.tcpDirect_ = std::to_string(FLAGS_sc_worker_worker_direct_port);
        builder_.AddService(streamCacheWorkerWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitMasterWorkerSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheMasterWorkerSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcMasterWorkerScAdapter_ = std::make_unique<MasterWorkerSCServiceBrpcAdapter>(*streamCacheMasterWorkerSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcMasterWorkerScAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_sc_regular_socket_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = DEFAULT_STREAM_SOCKET_NUM;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        builder_.AddService(streamCacheMasterWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitWorkerService()
{
    RETURN_IF_NOT_OK(workerSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcWorkerAdapter_ = std::make_unique<WorkerServiceBrpcAdapter>(*workerSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcWorkerAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        cfg.udsEnabled_ = false;
        builder_.AddService(workerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitCoordinatorWatchService()
{
    RETURN_OK_IF_TRUE(coordinatorWatchSvc_ == nullptr);
    RETURN_IF_NOT_OK(coordinatorWatchSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcCoordinatorWatchAdapter_ =
            std::make_unique<coordinator::CoordinatorWatchServiceBrpcAdapter>(*coordinatorWatchSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcCoordinatorWatchAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = LIGHTWEIGHT_SERVICE_THREAD_NUM;
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
        cfg.udsEnabled_ = false;
        builder_.AddService(coordinatorWatchSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitMasterService()
{
    RETURN_IF_NOT_OK(commonSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcMasterAdapter_ = std::make_unique<master::MasterServiceBrpcAdapter>(*commonSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcMasterAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        builder_.AddService(commonSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitMasterOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheMasterSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcMasterOcAdapter_ = std::make_unique<master::MasterOCServiceBrpcAdapter>(*objCacheMasterSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcMasterOcAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        builder_.AddService(objCacheMasterSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerOCServer::InitMasterSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheMasterSvc_->Init());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        brpcMasterScAdapter_ = std::make_unique<master::MasterSCServiceBrpcAdapter>(*streamCacheMasterSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcMasterScAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        builder_.AddService(streamCacheMasterSvc_.get(), cfg);
    }
    return Status::OK();
}

#ifdef WITH_TESTS
Status WorkerOCServer::InitUtOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    if (rpcServer_ && rpcServer_->IsBrpc()) {
        // brpc mode: register the generated UtOCServiceBrpcAdapter so the test
        // UtOCService RPCs reach a brpc handler. StOCServiceImpl multi-inherits
        // IUtOCService for this. Mirrors the GenericService fix.
        brpcUtOcAdapter_ = std::make_unique<UtOCServiceBrpcAdapter>(*utSvc_);
        RETURN_IF_NOT_OK(rpcServer_->AddBrpcService(brpcUtOcAdapter_.get()));
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = LIGHTWEIGHT_SERVICE_THREAD_NUM;
        cfg.numStreamSockets_ = 1;  // test stream function, set thread num as 1.
        builder_.AddService(utSvc_.get(), cfg);
    }
    return Status::OK();
}
#endif

#ifdef ENABLE_PERF
Status WorkerOCServer::CreateAndInitPerfService()
{
    perfService_ = std::make_unique<PerfServiceImpl>(hostPort_, akSkManager_);
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = RPC_NUM_BACKEND;
    cfg.numStreamSockets_ = 0;
    builder_.AddService(perfService_.get(), cfg);
    return Status::OK();
}
#endif

void WorkerOCServer::EnableLocalBypass()
{
    if (EnableOCService()) {
        // Pass the receiving-side ptr of the WorkerMaster service so that the sending side can implement local
        objCacheMasterSvc_->AssignLocalWorker(objCacheWorkerMsSvc_.get());
    }

    if (EnableSCService()) {
        // MasterSCServiceImpl uses the RpcSessionManager singleton for managing the MasterWorkerSCApi. Provide the
        // session manager with the fields needed to enable local bypass.
        rpcSessionManager_->SetLocalArgs(hostPort_, streamCacheMasterWorkerSvc_);
    }
}

Status WorkerOCServer::InitAkSk()
{
    akSkManager_ = std::make_shared<AkSkManager>(FLAGS_request_expire_time_s);

    Raii raii([]() {
        ClearStr(FLAGS_system_secret_key);
        ClearStr(FLAGS_tenant_secret_key);
        ClearStr(FLAGS_system_data_key);
    });
    RETURN_IF_NOT_OK(akSkManager_->SetServerAkSk(AkSkType::SYSTEM, FLAGS_system_access_key, FLAGS_system_secret_key,
                                                 FLAGS_system_data_key));
    RETURN_IF_NOT_OK(akSkManager_->SetServerAkSk(AkSkType::TENANT, FLAGS_tenant_access_key, FLAGS_tenant_secret_key));
    RETURN_IF_NOT_OK(
        akSkManager_->SetClientAkSk(FLAGS_system_access_key, FLAGS_system_secret_key, FLAGS_system_data_key));
    return Status::OK();
}

void WorkerOCServer::CreateMasterServices()
{
    using namespace datasystem::master;
    LOG(INFO) << "Start create master services";
    // create MasterServiceImpl
    commonSvc_ = std::make_unique<MasterServiceImpl>(hostPort_, akSkManager_);
    if (EnableOCService()) {
        // create MasterOCServiceImpl
        resourceManager_->SetTopologyMembership(&topologyEngine_->Membership());
        objCacheMasterSvc_ = std::make_unique<MasterOCServiceImpl>(hostPort_, persistenceApi_, akSkManager_,
                                                                   metadataManagerHolder_.get(), resourceManager_.get(),
                                                                   topologyEngine_->Membership(), hostPort_.ToString());
    }
    if (EnableSCService()) {
        // create MasterSCServiceImpl
        rpcSessionManager_ = std::make_shared<RpcSessionManager>();
        streamCacheMasterSvc_ = std::make_unique<MasterSCServiceImpl>(
            hostPort_, akSkManager_, metadataManagerHolder_.get(), topologyEngine_->Membership(), hostPort_.ToString(),
            topologyEngine_->IsRestart(), true);
    }
}

void WorkerOCServer::CreateWorkerServices()
{
    using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;
    LOG(INFO) << "Start create worker services";

    // create WorkerServiceImpl
    workerSvc_ = std::make_unique<WorkerServiceImpl>(hostPort_, masterAddr_, DFT_TIMEOUT_MULT, this, akSkManager_,
                                                     hostPort_.ToString(), topologyEngine_->Membership(),
                                                     topologyExitRequested_);
    auto objectTable = std::make_shared<ObjectTable>();
    auto evictionManager = std::make_shared<object_cache::WorkerOcEvictionManager>(
        objectTable, hostPort_, masterAddr_, *metadataRouteResolver_, objCacheMasterSvc_.get());
    if (EnableOCService()) {
        CreateObjectCacheWorkerServices(objectTable, evictionManager);
    }
    if (EnableSCService()) {
        auto scAllocateManager = std::make_shared<stream_cache::WorkerSCAllocateMemory>(evictionManager);
        // create ClientWorkerSCService
        streamCacheClientWorkerSvc_ = std::make_shared<stream_cache::ClientWorkerSCServiceImpl>(
            hostPort_, masterAddr_, streamCacheMasterSvc_.get(), akSkManager_, scAllocateManager,
            *metadataRouteResolver_, topologyEngine_->Membership());
        streamCacheClientWorkerSvc_->SetRuntimeFacade(&workerRuntime_);
        // create MasterWorkerSCServiceImpl
        streamCacheMasterWorkerSvc_ = std::make_shared<stream_cache::MasterWorkerSCServiceImpl>(
            hostPort_, masterAddr_, streamCacheClientWorkerSvc_.get(), akSkManager_);
        // create WorkerWorkerSCService
        streamCacheWorkerWorkerSvc_ =
            std::make_unique<stream_cache::WorkerWorkerSCServiceImpl>(streamCacheClientWorkerSvc_.get(), akSkManager_);
    }
}

void WorkerOCServer::CreateObjectCacheWorkerServices(
    const std::shared_ptr<SafeTable<ImmutableString, ObjectInterface>> &objectTable,
    const std::shared_ptr<object_cache::WorkerOcEvictionManager> &evictionManager)
{
    // create WorkerOCServices
    objCacheClientWorkerSvc_ = std::make_shared<datasystem::object_cache::WorkerOCServiceImpl>(
        hostPort_, masterAddr_, objectTable, akSkManager_, evictionManager, persistenceApi_,
        metadataCoordinationBackend_.get(), objCacheMasterSvc_.get(), topologyEngine_.get(), *metadataRouteResolver_,
        topologyEngine_->Membership(), &topologyExitRequested_, topologyEngine_->IsRestart(), true);
    objCacheClientWorkerSvc_->SetRuntimeFacade(&workerRuntime_);
    object_cache::NodeSelector::Instance().SetRuntimeFacade(&workerRuntime_);
    CreateRebalanceExecutor(objectTable, evictionManager);
    objCacheClientWorkerSvc_->RegisterAsyncTasksDoneChecker([this](const std::string &,
                                                                   std::chrono::steady_clock::time_point deadline,
                                                                   const cluster::CancellationToken &cancellation) {
        while (!checkAsyncTasksDone_.load()) {
            CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology async-task drain cancelled");
            CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                                     "topology async-task drain deadline exceeded");
            cancellation.WaitUntil(
                std::min(deadline, std::chrono::steady_clock::now() + std::chrono::seconds(CHECK_ASYNC_SLEEP_TIME_S)));
        }
        return Status::OK();
    });
    // create WorkerWorkerOCService
    objCacheWorkerWkSvc_ = std::make_shared<datasystem::object_cache::WorkerWorkerOCServiceImpl>(
        objCacheClientWorkerSvc_, akSkManager_, topologyEngine_->Membership(),
        [this] { return topologyEngine_ != nullptr && !topologyEngine_->IsMemberLeaseTimedOut(); },
        [this] {
            return topologyEngine_ == nullptr ? cluster::ControlBackendObservation{}
                                              : topologyEngine_->GetControlBackendObservation();
        });
    // create MasterWorkerOCService
    objCacheWorkerMsSvc_ =
        std::make_shared<datasystem::object_cache::MasterWorkerOCServiceImpl>(objCacheClientWorkerSvc_, akSkManager_);
    // create WorkerWorkerTransportService
    objCacheWorkerTransSvc_ =
        std::make_shared<datasystem::object_cache::WorkerWorkerTransportServiceImpl>(objCacheClientWorkerSvc_);
}

void WorkerOCServer::CreateRebalanceExecutor(
    const std::shared_ptr<SafeTable<ImmutableString, ObjectInterface>> &objectTable,
    const std::shared_ptr<object_cache::WorkerOcEvictionManager> &evictionManager)
{
    RebalanceExecutorConfig rebalanceConfig{ hostPort_,
                                             metadataRouteResolver_.get(),
                                             &topologyEngine_->Membership(),
                                             objectEndpointPolicy_.get(),
                                             &topologyExitRequested_,
                                             akSkManager_,
                                             objectTable,
                                             evictionManager,
                                             objCacheClientWorkerSvc_->GetWorkerMasterApiManager() };
    rebalanceExecutor_ = std::make_unique<RebalanceExecutor>(std::move(rebalanceConfig));
    object_cache::NodeSelector::Instance().RegisterRebalanceTaskHandler(
        [this](const master::RebalanceTaskPb &task, const std::string &assignedMasterAddress) {
            std::lock_guard<std::mutex> lock(rebalanceExecutorMutex_);
            if (rebalanceExecutor_ != nullptr) {
                rebalanceExecutor_->Submit(task, assignedMasterAddress);
            }
        });
}

void WorkerOCServer::CreateAllServices()
{
    // In case of centralized master, create either master or worker services
    if (IsLocalMetadataMaster()) {
        CreateMasterServices();
        CreateWorkerServices();
    } else {
        CreateWorkerServices();
    }
#ifdef WITH_TESTS
    // create StOCServiceImpl
    utSvc_ = std::make_unique<st::StOCServiceImpl>(objCacheClientWorkerSvc_.get(), &topologyEngine_->Membership(),
                                                   metadataManagerHolder_.get(), akSkManager_);
#endif
}

Status WorkerOCServer::InitializeMasterServices()
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterService(), "InitMasterService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterOCService(), "InitMasterOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterSCService(), "InitMasterSCService failed");

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(metadataManagerHolder_->InitLocalMetadataForStart(topologyEngine_->IsRestart()),
                                     "InitLocalMetadataForStart failed");
    return Status::OK();
}

Status WorkerOCServer::InitializeWorkerServices()
{
    if (FLAGS_enable_lossless_data_exit_mode && !FLAGS_enable_distributed_master) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID, "enable_lossless_data_exit_mode can be set to true only when enable_distributed_master is true");
    }
    // Register GenericService (test control-plane: SetInjectAction / GcovFlush / ...)
    // as a brpc service. No-op in ZMQ mode or non-test builds.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitGenericBrpcService(), "InitGenericBrpcService failed");
    // Init the services and hook them up to the RPC server.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerOCService(), "InitWorkerOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerWorkerOCService(), "InitWorkerWorkerOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterWorkerOCService(), "InitMasterWorkerOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerWorkerTransportService(), "InitWorkerWorkerTransportService failed");
    // In some cases services have dependencies between each other where they access each other via pointers.
    // For example, the WorkerOCService takes a pointer to the MasterOCService to provide a local bypass optimization.
    // However, there exist cases that have a circular dependency:
    //    MasterWorkerOCService depends on WorkerOCService
    //    WorkerOCService depends on MasterOCService
    //    MasterOCService depends on MasterWorkerOCService
    // When it is circular like this, some services have not been created yet at the time when they are needed to
    // satisfy the dependencies during initial creation.
    // To resolve this, some of the dependencies need to be assigned after all the dependent services are created.
    // The following function must be called after the services have been created.
    // Must be called after MasterOCServiceImpl initialization.
    if (IsLocalMetadataMaster()) {
        EnableLocalBypass();
    }
    // Init the stream services and hook them up to the RPC server
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitClientWorkerSCService(), "InitClientWorkerSCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterWorkerSCService(), "InitMasterWorkerSCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerWorkerSCService(), "InitWorkerWorkerSCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerService(), "InitWorkerService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitCoordinatorWatchService(), "InitCoordinatorWatchService failed");
    return Status::OK();
}

Status WorkerOCServer::InitializeAllServices()
{
    // In case of centralized master, initialize either master or worker services
    if (IsLocalMetadataMaster()) {
        RETURN_IF_NOT_OK(InitializeMasterServices());
        RETURN_IF_NOT_OK(InitializeWorkerServices());
    } else {
        RETURN_IF_NOT_OK(InitializeWorkerServices());
    }
#ifdef WITH_TESTS
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitUtOCService(), "InitUtOCService failed");
#endif
#ifdef ENABLE_PERF
    RETURN_IF_NOT_OK(CreateAndInitPerfService());
#endif
    return Status::OK();
}

Status WorkerOCServer::InitCoordinationBackend()
{
    if (coordinatorDiscovery_ != nullptr) {
        if (FLAGS_use_brpc) {
            coordinatorServiceProxy_ = std::make_unique<CoordinatorServiceProxyBrpcImpl>(coordinatorDiscovery_);
        } else {
            coordinatorServiceProxy_ = std::make_unique<CoordinatorServiceProxyZmqImpl>(coordinatorDiscovery_);
        }
        RETURN_IF_NOT_OK(coordinatorServiceProxy_->Init());
        LOG(INFO) << "Using DataSystem Coordinator as cluster coordination backend, source=discovery";
        return Status::OK();
    }

    // EtcdStore is owned by WorkerOcServer. It is used by multiple services.
    CHECK_FAIL_RETURN_STATUS(ValidateEtcdOrMetastoreAddress(), K_RUNTIME_ERROR,
                             "Neither etcd_address nor metastore_address is specified");

    // Determine which backend to use for metadata storage.
    if (!FLAGS_metastore_address.empty()) {
        // Use metastore as the backend.
        etcdOrMetastoreAddress_ = FLAGS_metastore_address;
        LOG(INFO) << "Using metastore as etcd replacement: " << etcdOrMetastoreAddress_;
        if (FLAGS_start_metastore_service) {
            // Start metastore service on this node (head worker).
            RETURN_IF_NOT_OK(StartMetaStoreService());
        }
    } else {
        // Use external etcd as the backend.
        FLAGS_etcd_address = ShuffleStringWithDelimiter(FLAGS_etcd_address, ETCD_ADDR_PATTREN);
        etcdOrMetastoreAddress_ = FLAGS_etcd_address;
        LOG(INFO) << "Using external etcd: " << etcdOrMetastoreAddress_;
    }
    etcdStore_ = std::make_unique<EtcdStore>(etcdOrMetastoreAddress_);
    workerIsolationCoordinator_ = std::make_unique<WorkerIsolationCoordinator>(
        workerRuntime_,
        WorkerIsolationCoordinatorHooks{
            .setTopologyServingAdmission = [this](bool open) { SetTopologyServingAdmission(open); },
            .reconcileLocalIsolationOwnership =
                [this] {
                    RETURN_OK_IF_TRUE(objCacheClientWorkerSvc_ == nullptr);
                    return objCacheClientWorkerSvc_->ReconcileLocalIsolationOwnership();
                },
            .isTopologyRuntimeReady = [this] { return topologyEngine_ != nullptr; },
            .publishReadyMembership = [this] { return topologyEngine_->MarkReady(); },
            .reconcileNetworkRecoveryOwnership =
                [this] {
                    RETURN_OK_IF_TRUE(objCacheClientWorkerSvc_ == nullptr);
                    return objCacheClientWorkerSvc_->ReconcileNetworkRecoveryOwnership();
                },
            .requestRecoveryReconciliation =
                [this](std::function<void()> onReconciliationStarted) {
                    return topologyEngine_->RequestRecoveryReconciliation(std::move(onReconciliationStarted));
                },
        });
    RETURN_IF_NOT_OK(etcdStore_->Init());
    RETURN_IF_NOT_OK(
        etcdStore_->Authenticate(FLAGS_etcd_username, FLAGS_etcd_password, FLAGS_etcd_token_refresh_interval_s));
    cluster::EtcdCoordinationBackend backend(etcdStore_.get());
    object_cache::CentralMetadataAddressResolver resolver(backend);
    RETURN_IF_NOT_OK(resolver.EnsureTable());
    return Status::OK();
}


void WorkerOCServer::CleanupRpcStubsForFailedMembers(const cluster::TopologySnapshot &snapshot)
{
    for (const auto &member : snapshot.FailedMembers())
        knownFailedAddresses_.insert(member->identity.address);
    for (const auto &member : snapshot.ActiveMembers())
        knownFailedAddresses_.erase(member->identity.address);
    for (const auto &addrStr : knownFailedAddresses_) {
        HostPort addr;
        if (addr.ParseString(addrStr).IsError() || addr.Empty()) continue;
        for (auto type : { StubType::WORKER_WORKER_OC_SVC, StubType::WORKER_WORKER_SC_SVC, StubType::WORKER_WORKER_TRANS_SVC })
            RpcStubCacheMgr::Instance().Remove(addr, type);
    }
}

Status WorkerOCServer::ConstructTopologyCallbacks()
{
    const bool centralizedMetadata = !FLAGS_enable_distributed_master;
    const bool localMetadataMaster = !centralizedMetadata || masterAddr_ == hostPort_;
    auto readinessCheck = [this](std::chrono::steady_clock::time_point deadline,
                                 const cluster::CancellationToken &cancellation) {
        while (IsClientsExist() || IsAsyncTasksRunning()) {
            CHECK_FAIL_RETURN_STATUS(!cancellation.IsCancelled(), K_NOT_READY, "topology callback cancelled");
            CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                                     "topology callback readiness deadline exceeded");
            cancellation.WaitUntil(
                std::min(deadline, std::chrono::steady_clock::now() + TOPOLOGY_CALLBACK_POLL_INTERVAL));
        }
        return Status::OK();
    };
    auto objectCacheProvider = [this] { return objCacheClientWorkerSvc_.get(); };
    topologyTaskCallbacks_ = std::make_unique<WorkerTopologyPhaseCallbacks>(WorkerTopologyPhaseCallbackDependencies{
        centralizedMetadata, localMetadataMaster, EnableSCService(), *metadataManagerHolder_, std::move(readinessCheck),
        std::make_shared<object_cache::WorkerTopologyObjectCacheActions>(objectCacheProvider) });
    return Status::OK();
}

cluster::CoordinatorWatchIngress WorkerOCServer::BuildCoordinatorWatchIngress()
{
    cluster::CoordinatorWatchIngress ingress;
    ingress.bind = [this](cluster::CoordinatorWatchIngress::Handler handler) {
        CHECK_FAIL_RETURN_STATUS(coordinatorWatchSvc_ != nullptr, K_NOT_READY,
                                 "Coordinator watch service is not constructed");
        return coordinatorWatchSvc_->BindEventHandler(std::move(handler));
    };
    ingress.unbindAndDrain = [this](std::chrono::steady_clock::time_point deadline) {
        CHECK_FAIL_RETURN_STATUS(coordinatorWatchSvc_ != nullptr, K_NOT_READY,
                                 "Coordinator watch service is not constructed");
        return coordinatorWatchSvc_->UnbindEventHandlerAndWait(deadline);
    };
    return ingress;
}

void WorkerOCServer::ConfigureTopologyBuilder(cluster::TopologyEngine::Builder &builder)
{
    auto controlBackendProbe = [localAddress = hostPort_, akSkManager = akSkManager_](const auto &, const auto &peers,
                                                                                      auto deadline) {
        ControlBackendPeerProbeClientFactory clientFactory =
            [localAddress, akSkManager](const cluster::MemberIdentity &peer,
                                        std::unique_ptr<IControlBackendPeerProbeClient> &client) {
                std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi> api;
                RETURN_IF_NOT_OK(object_cache::CreateRemoteWorkerApi(peer.address, localAddress, akSkManager, api));
                client = std::make_unique<WorkerRpcControlBackendProbeClient>(std::move(api));
                return Status::OK();
            };
        return ProbeControlBackendPeers(peers, deadline, clientFactory);
    };
    auto availabilityHandler = [this](cluster::TopologyAvailabilityLevel level) {
        RefreshTopologyServingAdmission(level);
    };
    auto localIsolationHandler = [this](const Status &status) {
        workerIsolationCoordinator_->OnLocalIsolation(status);
    };
    builder.SetClusterName(FLAGS_cluster_name)
        .SetLocalAddress(hostPort_.ToString())
        .SetPhaseCallbacks(*topologyTaskCallbacks_)
        .SetNodeDeadTimeout(std::chrono::seconds(FLAGS_node_timeout_s))
        .SetMembershipRestartHandler([this](const std::string &address, int64_t timestamp) {
            return HandleMembershipRestart(address, timestamp);
        })
        .SetMembershipRecoveryHandler([this](const std::string &address, int64_t timestamp) {
            return HandleMembershipRecovery(address, timestamp);
        })
        .SetLocalIsolationHandler(std::move(localIsolationHandler))
        .SetLocalRecoveryHandler([this] { workerIsolationCoordinator_->OnLocalRecovery(); })
        .SetSnapshotPublishedHandler([this](std::shared_ptr<const cluster::TopologySnapshot> snapshot) {
            ScheduleTopologySnapshotWarmup(std::move(snapshot));
        })
        .SetControlBackendProbe(std::move(controlBackendProbe))
        .SetAvailabilityHandler(std::move(availabilityHandler));
}

Status WorkerOCServer::ConstructTopologyRuntime()
{
    RETURN_IF_NOT_OK(ConstructTopologyCallbacks());
    if (coordinatorDiscovery_ != nullptr) {
        coordinatorWatchSvc_ = std::make_unique<coordinator::CoordinatorWatchServiceImpl>(hostPort_);
    }
    cluster::TopologyEngine::Builder builder;
    ConfigureTopologyBuilder(builder);
    if (coordinatorServiceProxy_ != nullptr) {
        builder.UseCoordinator(*coordinatorServiceProxy_, BuildCoordinatorWatchIngress());
    } else {
        builder.UseEtcd(*etcdStore_);
    }
    RETURN_IF_NOT_OK(builder.Build(topologyEngine_));
    const bool isRestart = topologyEngine_->IsRestart();
    const bool centralizedMetadata = !FLAGS_enable_distributed_master;
    MetadataRouteOptions metadataRouteOptions;
    metadataRouteOptions.requireAvailableTarget = true;
    metadataRouteOptions.centralizedMode = centralizedMetadata;
    metadataRouteOptions.masterAddress = masterAddr_;
    metadataRouteResolver_ =
        std::make_unique<MetadataRouteResolver>(&topologyEngine_->Placement(), std::move(metadataRouteOptions));
    objectEndpointPolicy_ =
        std::make_unique<object_cache::ObjectEndpointPolicy>(*metadataRouteResolver_, topologyEngine_->Membership());
    LOG(INFO) << "CLUSTER_LIFECYCLE cluster=" << FLAGS_cluster_name
              << " role=worker state=constructed local_address=" << hostPort_.ToString() << " restart=" << isRestart;
    return Status::OK();
}

Status WorkerOCServer::HandleMembershipRestart(const std::string &address, int64_t timestamp)
{
    HostPort restartedMember;
    RETURN_IF_NOT_OK(restartedMember.ParseString(address));
    // Preserve the proven yrds/master restart order: clear Stream ownership before object metadata reconciliation.
    RETURN_IF_NOT_OK(ClearWorkerMeta::GetInstance().NotifyAll(restartedMember));
    return NodeRestartEvent::GetInstance().NotifyAll(address, timestamp, false);
}

Status WorkerOCServer::ResolveLocalMetadataAddress()
{
    if (!FLAGS_enable_distributed_master) {
        if (masterAddr_.Empty()) {
            RETURN_IF_NOT_OK(ResolveCentralMetadataAddress(FLAGS_master_address));
            RETURN_IF_NOT_OK(masterAddr_.ParseString(FLAGS_master_address));
            LOG(INFO) << "Use centralized metadata endpoint: " << masterAddr_.ToString();
        }
        CHECK_FAIL_RETURN_STATUS(!masterAddr_.Empty(), K_INVALID, "master_address is required in centralized mode");
        return Status::OK();
    }

    if (masterAddr_.Empty()) {
        masterAddr_ = hostPort_;
        LOG(INFO) << "Use local worker address as distributed metadata endpoint: " << masterAddr_.ToString();
    }
    return Status::OK();
}

bool WorkerOCServer::IsLocalMetadataMaster() const
{
    return FLAGS_enable_distributed_master || masterAddr_ == hostPort_;
}

Status WorkerOCServer::ResolveCentralMetadataAddress(std::string &address)
{
    const auto localAddress = hostPort_.ToString();
    if (coordinatorServiceProxy_ == nullptr) {
        CHECK_FAIL_RETURN_STATUS(etcdStore_ != nullptr, K_NOT_READY, "ETCD Store is not initialized");
        cluster::EtcdCoordinationBackend backend(etcdStore_.get());
        object_cache::CentralMetadataAddressResolver resolver(backend);
        return resolver.ClaimOrRead(localAddress, address);
    }
    cluster::DsCoordinationBackend backend(coordinatorServiceProxy_.get(), localAddress);
    object_cache::CentralMetadataAddressResolver resolver(backend);
    return resolver.ClaimOrRead(localAddress, address);
}

Status WorkerOCServer::StartTopologyRuntime()
{
    CHECK_FAIL_RETURN_STATUS(topologyEngine_ != nullptr, K_NOT_READY, "topology engine is not constructed");
    return topologyEngine_->Start();
}

Status WorkerOCServer::PublishReadyMembership()
{
    CHECK_FAIL_RETURN_STATUS(topologyEngine_ != nullptr, K_NOT_READY, "topology Engine is not constructed");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(FLAGS_node_timeout_s);
    Status lastStatus(K_NOT_READY, "first membership keepalive has not been sent");
    LOG(INFO) << "Wait for the first membership lease before publishing READY";
    while (std::chrono::steady_clock::now() < deadline) {
        if (topologyEngine_->HasEstablishedMemberLease()) {
            lastStatus = topologyEngine_->MarkReady();
            if (lastStatus.IsOk() || lastStatus.GetCode() != K_NOT_READY) {
                return lastStatus;
            }
        }
        std::this_thread::sleep_for(TOPOLOGY_MEMBERSHIP_POLL_INTERVAL);
    }
    RETURN_STATUS(K_RPC_DEADLINE_EXCEEDED,
                  "timed out publishing READY membership after first lease: " + lastStatus.ToString());
}

Status WorkerOCServer::WaitForTopologyReady()
{
    CHECK_FAIL_RETURN_STATUS(topologyEngine_ != nullptr, K_NOT_READY, "topology engine is not constructed");
    constexpr int32_t logEveryCount = 50;
    while (!IsTermSignalReceived()) {
        const auto availability = topologyEngine_->GetAvailability();
        if (availability == cluster::TopologyAvailabilityLevel::NORMAL
            || availability == cluster::TopologyAvailabilityLevel::CONTROL_DEGRADED) {
            auto rc = CheckLocalTopologyServingReady(*topologyEngine_, hostPort_.ToString());
            if (rc.IsOk()) {
                return rc;
            }
            LOG_FIRST_AND_EVERY_N(INFO, logEveryCount)
                << "External readiness remains closed while topology placement is unavailable: " << rc.ToString();
        } else {
            LOG_FIRST_AND_EVERY_N(INFO, logEveryCount)
                << "External readiness remains closed while topology availability is "
                << static_cast<uint32_t>(availability);
        }
        std::this_thread::sleep_for(TOPOLOGY_MEMBERSHIP_POLL_INTERVAL);
    }
    RETURN_STATUS(K_NOT_READY, "process termination interrupted topology readiness wait");
}

Status WorkerOCServer::StopTopologyRuntime(std::chrono::steady_clock::time_point deadline)
{
    return topologyEngine_ == nullptr ? Status::OK() : topologyEngine_->Shutdown(deadline);
}

Status WorkerOCServer::StartMetaStoreService()
{
    // Use FLAGS_metastore_address if FLAGS_start_metastore_service is enabled
    // to replace external etcd server for cluster worker metadata storage
    if (FLAGS_metastore_address.empty()) {
        LOG(ERROR) << "metastore_address is empty, cannot start metastore service";
        return Status(StatusCode::K_INVALID, "metastore_address is empty");
    }

    LOG(INFO) << "Starting metastore service on master worker at " << FLAGS_metastore_address;

    metaStoreServer_ = std::make_unique<MetaStoreServer>();
    RETURN_IF_NOT_OK(metaStoreServer_->Start(FLAGS_metastore_address));

    LOG(INFO) << "Metastore service started successfully to replace external etcd server";
    return Status::OK();
}

Status WorkerOCServer::StopMetaStoreService()
{
    if (metaStoreServer_) {
        LOG(INFO) << "Stopping metastore service on master worker";
        RETURN_IF_NOT_OK(metaStoreServer_->Stop());
        metaStoreServer_.reset();
        LOG(INFO) << "Metastore service stopped successfully";
    }
    return Status::OK();
}

Status WorkerOCServer::Init()
{
    ImmutableStringPool::Instance().Init();
    intern::StringPool::InitAll();
    RETURN_IF_NOT_OK(InitAkSk());
    // The static members are destructed in the reverse order of their construction,
    // Below call guarantees that the destructor of Env is behind other Rocksdb singletons.
    (void)rocksdb::Env::Default();
    RETURN_IF_NOT_OK(InitRpcAndMemoryRuntime());
    RETURN_IF_NOT_OK(InitClusterRuntimeAndServices());
    LOG(INFO) << "Worker init success.";
    return Status::OK();
}

Status WorkerOCServer::InitRpcAndMemoryRuntime()
{
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);

    // Configure HCCS worker IP before any allocator/mmap path can trigger RemoteH2DManager::Instance()
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SetRH2DLocalEndpointIp(hostPort_.Host()), "Failed to configure HCCS worker IP");

    if (FLAGS_use_brpc) {
        int brpcPort = bindHostPort_.Port() + kBrpcPortOffset;
        builder_.SetUseBrpc(true).SetBrpcAddr(bindHostPort_.Host(), brpcPort);
        // Exclusive mode: brpc uses the same port as ZMQ would. ZMQ server is
        // not started (see rpc_server.cpp BuildAndSTart: skips server->Run()
        // when useBrpc_=true). No dual-listen.
        LOG(INFO) << "brpc mode enabled, brpc listening on " << bindHostPort_.Host() << ":" << brpcPort
                  << " (exclusive mode, ZMQ not started)";
    }

    // Init shared memory
    uint64_t sharedMemoryBytes = FLAGS_shared_memory_size_mb * 1024ul * 1024ul;  // convert mb to bytes.
    uint64_t sharedDiskBytes = FLAGS_shared_disk_size_mb * 1024ul * 1024ul;      // convert mb to bytes.
    ssize_t decayMs = FLAGS_memory_reclamation_time_second * 1000;               // convert to ms.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(datasystem::memory::Allocator::Instance()->Init(
                                         sharedMemoryBytes, sharedDiskBytes, FLAGS_shared_memory_populate, true,
                                         decayMs, FLAGS_oc_shm_threshold_percentage, FLAGS_sc_shm_threshold_percentage),
                                     "Init allocator failed");
    // Call base class to init common service
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CommonServer::Init(), "CommonServer init failed");
    return Status::OK();
}

Status WorkerOCServer::InitClusterRuntimeAndServices()
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitializeFastTransportManager(hostPort_),
                                     "Fast transport (URMA/RDMA) init failed");
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_max_rpc_session_num, hostPort_));
    InitSlotWorkerNamespace();
    if (IsSupportL2Storage(GetCurrentStorageType())) {
        persistenceApi_ = PersistenceApi::CreateShared();
        RETURN_IF_NOT_OK(persistenceApi_->Init());
    }

    metadataManagerHolder_ = std::make_unique<MetadataManagerHolder>();
    resourceManager_ = std::make_unique<master::ResourceManager>();

    RETURN_IF_NOT_OK(InitCoordinationBackend());

    RETURN_IF_NOT_OK(ClientManager::Instance().Init());

    memory::Allocator::Instance()->SetCheckIfAllFdReleasedHandler(
        [](const std::vector<int> &workerFds) { return ClientManager::Instance().IsAllWorkerFdsReleased(workerFds); });

    if (masterAddr_.Empty() && !FLAGS_master_address.empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterAddr_.ParseString(FLAGS_master_address),
                                         "The master_address/etcd_address can not be empty at the same time.");
    }
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ResolveLocalMetadataAddress(), "Resolve local metadata address failed");
    RETURN_IF_NOT_OK(ConstructTopologyRuntime());

    CreateAllServices();

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMetadataManagerHolder(), "metadata manager holder init failed");

    // after setting ETCD cluster manager, it time to initialize all services
    RETURN_IF_NOT_OK(InitializeAllServices());
    // Start task execution only after every callback target is fully initialized.
    RETURN_IF_NOT_OK(StartTopologyRuntime());
    RETURN_IF_NOT_OK(StartBrpcIfEnabled());
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(PublishReadyMembership(), "Publish Worker topology READY membership failed");
    RETURN_IF_NOT_OK(InitSlotRecovery());
    return Status::OK();
}

Status WorkerOCServer::StartBrpcIfEnabled()
{
    // Start brpc server now that all brpc adapter services have been
    // registered via AddBrpcService() in InitializeAllServices().
    // BuildAndStart() in CommonServer::Init() skips brpc start because
    // services are added later via AddBrpcService() calls.
    if (FLAGS_use_brpc && rpcServer_) {
        int brpcPort = bindHostPort_.Port() + kBrpcPortOffset;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(rpcServer_->StartBrpcServer(bindHostPort_.Host(), brpcPort),
                                         "Failed to start brpc server");
    }
    return Status::OK();
}

Status WorkerOCServer::InitLivenessCheck()
{
    if (!FLAGS_liveness_check_path.empty()) {
        const int secToMs = 1000;
        // start liveiness check.
        livenessCheck_ = std::make_unique<WorkerLivenessCheck>(this, FLAGS_liveness_check_path,
                                                               FLAGS_liveness_probe_timeout_s * secToMs, bindHostPort_,
                                                               hostPort_.ToString(), akSkManager_);
        RETURN_IF_NOT_OK(livenessCheck_->Init());
    }
    return Status::OK();
}

Status WorkerOCServer::InitMetadataManagerHolder()
{
    MetadataManagerHolderParam param;
    param.dbRootPath = FLAGS_rocksdb_store_dir;
    param.currWorkerId = hostPort_.ToString();
    param.akSkManager = akSkManager_;
    param.etcdStore = etcdStore_.get();
    param.persistenceApi = persistenceApi_;
    param.masterAddress = hostPort_;
    param.placement = &topologyEngine_->Placement();
    param.membership = &topologyEngine_->Membership();
    param.centralizedMetadata = !FLAGS_enable_distributed_master;
    param.metadataAddress = masterAddr_;
    param.localAddress = hostPort_.ToString();
    param.isRestart = topologyEngine_->IsRestart();
    param.controlBackendAvailableAtStartup = true;
    param.exitRequested = &topologyExitRequested_;
    param.masterWorkerService = objCacheWorkerMsSvc_.get();
    param.workerWorkerService = objCacheWorkerWkSvc_.get();
    param.rpcSessionManager = rpcSessionManager_;
    param.isOcEnabled = EnableOCService();
    param.isScEnabled = EnableSCService();
    return metadataManagerHolder_->Init(param);
}

void WorkerOCServer::RegisteringAllResourceCollectionCallbackFunc()
{
    RegisteringWorkerCallbackFunc();
    RegisteringMasterCallbackFunc();
    RegisteringThirdComponentCallbackFunc();
}

void WorkerOCServer::RegisteringWorkerCallbackFunc()
{
    auto &instance = ResMetricCollector::Instance();
    // The usage of spill
    instance.RegisterCollectHandler(ResMetricName::SPILL_HARD_DISK,
                                    []() { return object_cache::WorkerOcSpill::Instance()->GetSpillUsage(); });
    // Spill IO statistics (cumulative + rolling one-hour delta)
    instance.RegisterCollectHandler(ResMetricName::SPILL_IO_STATS,
                                    []() { return object_cache::WorkerOcSpill::Instance()->GetSpillIoStats(); });
    // The usage of share memory
    instance.RegisterCollectHandler(ResMetricName::SHARED_MEMORY,
                                    []() { return memory::Allocator::Instance()->GetMemoryStatistics(); });
    // The usage of share disk
    instance.RegisterCollectHandler(ResMetricName::SHARED_DISK,
                                    []() { return memory::Allocator::Instance()->GetSharedDiskStatistics(); });
    // The usage of WorkerOCService
    instance.RegisterCollectHandler(ResMetricName::WORKER_OC_SERVICE_THREAD_POOL, [this]() {
        return GetRpcServicesUsage("WorkerOCService").ToString(FLAGS_log_monitor_interval_ms);
    });
    // The usage of WorkerWorkerOCService
    instance.RegisterCollectHandler(ResMetricName::WORKER_WORKER_OC_SERVICE_THREAD_POOL, [this]() {
        return GetRpcServicesUsage("WorkerWorkerOCService").ToString(FLAGS_log_monitor_interval_ms);
    });
    // The total number of clients
    instance.RegisterCollectHandler(ResMetricName::ACTIVE_CLIENT_COUNT,
                                    [] { return std::to_string(ClientManager::Instance().GetClientCount()); });

    // brpc stream close intentional leak counter
    instance.RegisterCollectHandler(ResMetricName::BRPC_STREAM_LEAK_COUNT,
                                    [] { return std::to_string(GetBrpcStreamLeakCount()); });

    // Deferred cleanup queue depth
    instance.RegisterCollectHandler(ResMetricName::DEFERRED_CLEANUP_QUEUE_SIZE,
                                    [] { return std::to_string(GetDeferredCleanupQueueSize()); });

    if (EnableOCService()) {
        // The total number of objects
        instance.RegisterCollectHandler(ResMetricName::OBJECT_COUNT, [this] {
            return std::to_string(objCacheClientWorkerSvc_->GetTotalObjectCount());
        });
        // The total size of objects
        instance.RegisterCollectHandler(ResMetricName::OBJECT_SIZE, [this] {
            return std::to_string(objCacheClientWorkerSvc_->GetTotalObjectSize());
        });
        instance.RegisterCollectHandler(ResMetricName::OC_HIT_NUM,
                                        [this] { return objCacheClientWorkerSvc_->GetHitInfo(); });
    }

    if (EnableSCService()) {
        instance.RegisterCollectHandler(ResMetricName::STREAM_COUNT,
                                        [this]() { return streamCacheClientWorkerSvc_->GetTotalStreamCount(); });

        // The usage of WorkerSCService
        instance.RegisterCollectHandler(ResMetricName::WORKER_SC_SERVICE_THREAD_POOL, [this]() {
            return GetRpcServicesUsage("ClientWorkerSCService").ToString(FLAGS_log_monitor_interval_ms);
        });

        // The usage of WorkerSCService
        instance.RegisterCollectHandler(ResMetricName::WORKER_WORKER_SC_SERVICE_THREAD_POOL, [this]() {
            return GetRpcServicesUsage("WorkerWorkerSCService").ToString(FLAGS_log_monitor_interval_ms);
        });

        instance.RegisterCollectHandler(ResMetricName::STREAM_REMOTE_SEND_SUCCESS_RATE,
                                        [this]() { return streamCacheClientWorkerSvc_->GetSCRemoteSendSuccessRate(); });

        instance.RegisterCollectHandler(ResMetricName::SC_LOCAL_CACHE, [this]() {
            return streamCacheWorkerWorkerSvc_->GetUsageMonitor().GetLocalMemoryUsed();
        });
    }
}

void WorkerOCServer::RegisteringMasterCallbackFunc()
{
    auto &instance = ResMetricCollector::Instance();
    // The usage of MasterWorkerOCService
    instance.RegisterCollectHandler(ResMetricName::MASTER_WORKER_OC_SERVICE_THREAD_POOL, [this]() {
        return GetRpcServicesUsage("MasterWorkerOCService").ToString(FLAGS_log_monitor_interval_ms);
    });
    // The usage of MasterOcService
    instance.RegisterCollectHandler(ResMetricName::MASTER_OC_SERVICE_THREAD_POOL, [this]() {
        return GetRpcServicesUsage("MasterOCService").ToString(FLAGS_log_monitor_interval_ms);
    });

    if (EnableOCService()) {
        if (IsLocalMetadataMaster()) {
            // The usage of etcd asynchronous write task queue
            instance.RegisterCollectHandler(ResMetricName::ETCD_QUEUE, [this]() {
                auto usage = objCacheMasterSvc_->GetETCDAsyncQueueUsage();
                return usage.empty() ? RES_ETCD_DEFAULT_USAGE : usage;
            });
            // The usage of master asyncPool_
            instance.RegisterCollectHandler(ResMetricName::MASTER_ASYNC_TASKS_THREAD_POOL, [this]() {
                auto usage = objCacheMasterSvc_->GetMasterAsyncPoolUsage(FLAGS_log_monitor_interval_ms);
                return usage.empty() ? RES_THREAD_POOL_DEFAULT_USAGE : usage;
            });
        } else {
            instance.RegisterCollectHandler(ResMetricName::ETCD_QUEUE, []() { return RES_ETCD_DEFAULT_USAGE; });
            instance.RegisterCollectHandler(ResMetricName::MASTER_ASYNC_TASKS_THREAD_POOL,
                                            []() { return RES_THREAD_POOL_DEFAULT_USAGE; });
        }
    }

    if (EnableSCService()) {
        // The usage of MasterWorkerOCService
        instance.RegisterCollectHandler(ResMetricName::MASTER_WORKER_SC_SERVICE_THREAD_POOL, [this]() {
            return GetRpcServicesUsage("MasterWorkerSCService").ToString(FLAGS_log_monitor_interval_ms);
        });
        // The usage of MasterOcService
        instance.RegisterCollectHandler(ResMetricName::MASTER_SC_SERVICE_THREAD_POOL, [this]() {
            return GetRpcServicesUsage("MasterSCService").ToString(FLAGS_log_monitor_interval_ms);
        });
    }
}

void WorkerOCServer::RegisteringThirdComponentCallbackFunc()
{
    auto &instance = ResMetricCollector::Instance();
    if (EnableOCService()) {
        // The success rate of etcd request
        instance.RegisterCollectHandler(ResMetricName::ETCD_REQUEST_SUCCESS_RATE, [this]() {
            return etcdStore_ == nullptr ? RES_ETCD_DEFAULT_USAGE : etcdStore_->GetEtcdRequestSuccessRate();
        });
    }
    if (TESTFLAG(GetCurrentStorageType(), L2StorageType::OBS)) {
        // The success rate of obs request
        instance.RegisterCollectHandler(ResMetricName::OBS_REQUEST_SUCCESS_RATE,
                                        [this]() { return persistenceApi_->GetL2CacheRequestSuccessRate(); });
    }
}

Status WorkerOCServer::WaitForServiceReady()
{
    // In brpc exclusive mode ZMQ port is not listened (kBrpcPortOffset=0), so ZMQ-based
    // HealthCheck RPC always fails. brpc exposes HTTP /status as the health interface;
    // skip ZMQ probe and let ReadinessProbe fall through to writing the ready file.
    if (FLAGS_use_brpc) {
        return Status::OK();
    }
    if (EnableOCService()) {
        RpcCredential cred;
        RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateCredentials(WORKER_SERVER_NAME, cred));
        auto channel = std::make_shared<RpcChannel>(hostPort_, cred);
        std::unique_ptr<WorkerOCService::Stub> stub = std::make_unique<WorkerOCService_Stub>(channel);
        HealthCheckRequestPb req;
        std::string msg = "hello";
        req.set_info(msg);
        HealthCheckReplyPb reply;
        Status ws;
        while (!IsTermSignalReceived()) {
            ws = stub->HealthCheck(req, reply);
            if (ws.IsOk()) {
                break;
            }
            LOG(INFO) << "Readiness probe retrying, detail: " << ws.ToString();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (IsTermSignalReceived()) {
            LOG(INFO) << "Meets SIGTERM and need to exit in advance, message: " << ws.ToString();
        }
    } else {
        LOG(INFO) << "Unable worker service, skip health check.";
    }
    return Status::OK();
}

Status WorkerOCServer::MaybeStartConnectionWarmup()
{
    if (!FLAGS_enable_urma) {
        LOG(INFO) << "[URMA_WARMUP] enable_urma=false, skip warmup.";
        return Status::OK();
    }
    if (!EnableOCService()) {
        LOG(WARNING) << "[URMA_WARMUP] object cache service is disabled, skip warmup.";
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(objCacheClientWorkerSvc_ != nullptr, K_NOT_READY, "Object cache service is not ready.");
    RETURN_IF_NOT_OK(objCacheClientWorkerSvc_->PrepareUrmaWarmupObject(BuildWarmupKey(hostPort_.ToString())));
    warmupExit_ = false;
    RETURN_IF_EXCEPTION_OCCURS(warmupThreadPool_ =
                                   std::make_shared<ThreadPool>(WARMUP_THREAD_NUM, WARMUP_THREAD_NUM, "UrmaWarmup"));
    RaiiPlus cleanupWarmupThreadPool([this]() { ReleaseWarmupThreadPool(); });
    RETURN_IF_EXCEPTION_OCCURS(warmupControllerThread_ =
                                   std::make_unique<Thread>([this]() { RunUrmaWarmupController(); }));
    cleanupWarmupThreadPool.ClearAllTask();
    warmupControllerThread_->set_name("UrmaWarmup");
    return Status::OK();
}

void WorkerOCServer::StopConnectionWarmup()
{
    warmupExit_ = true;
    warmupScanCv_.notify_all();
    if (warmupControllerThread_ != nullptr) {
        warmupControllerThread_->join();
        warmupControllerThread_.reset();
    }
    warmupThreadPool_.reset();
}

void WorkerOCServer::ReleaseWarmupThreadPool()
{
    warmupThreadPool_.reset();
}

void WorkerOCServer::ScheduleUrmaWarmupTasks(const std::vector<const cluster::Member *> &members,
                                             std::unordered_set<std::string> &scheduledPeers,
                                             std::vector<std::future<bool>> &futures)
{
    for (const auto *member : members) {
        if (warmupExit_) {
            break;
        }
        const auto &peerAddress = member->identity.address;
        if (peerAddress == hostPort_.ToString() || scheduledPeers.count(peerAddress) > 0) {
            continue;
        }
        scheduledPeers.emplace(peerAddress);
        try {
            futures.emplace_back(warmupThreadPool_->Submit([this, peerAddr = peerAddress]() {
                if (warmupExit_) {
                    return false;
                }
                TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                auto rc = objCacheClientWorkerSvc_->WarmupUrmaConnectionToPeer(peerAddr, BuildWarmupKey(peerAddr));
                if (rc.IsError()) {
                    LOG(WARNING) << FormatString("[URMA_WARMUP] peer warmup failed, peer=%s, status=%s", peerAddr,
                                                 rc.ToString());
                    return false;
                }
                return true;
            }));
        } catch (const std::exception &e) {
            LOG(WARNING) << FormatString("[URMA_WARMUP] submit peer warmup failed, peer=%s, error=%s", peerAddress,
                                         e.what());
            scheduledPeers.erase(peerAddress);
        }
    }
}

size_t WorkerOCServer::GetUrmaWarmupSuccessCount(std::vector<std::future<bool>> &futures) const
{
    size_t successCount = 0;
    for (auto &future : futures) {
        try {
            if (future.valid() && future.get()) {
                ++successCount;
            }
        } catch (const std::exception &e) {
            LOG(WARNING) << "[URMA_WARMUP] peer warmup task failed, error=" << e.what();
        }
    }
    return successCount;
}

bool WorkerOCServer::ShouldStopUrmaWarmup(int64_t elapsedMs, uint32_t stableRounds) const
{
    return elapsedMs >= WARMUP_MAX_SCAN_MS || (elapsedMs >= WARMUP_MIN_SCAN_MS && stableRounds >= WARMUP_STABLE_ROUNDS);
}

void WorkerOCServer::RunUrmaWarmupController()
{
    Raii releaseWarmupPool([this]() { ReleaseWarmupThreadPool(); });
    if (topologyEngine_ == nullptr) {
        LOG(WARNING) << "[URMA_WARMUP] topology runtime is unavailable, skip peer warmup.";
        return;
    }
    const auto start = std::chrono::steady_clock::now();
    std::unordered_set<std::string> scheduledPeers;
    std::vector<std::future<bool>> futures;
    for (uint32_t stableRounds = 0; !warmupExit_;) {
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        auto rc = topologyEngine_->GetSnapshot(snapshot);
        if (rc.IsError()) {
            VLOG(1) << "[URMA_WARMUP] topology Snapshot is not ready, status=" << rc.ToString();
        }
        auto oldPeerCount = scheduledPeers.size();
        if (snapshot != nullptr) {
            ScheduleUrmaWarmupTasks(snapshot->ActiveMembers(), scheduledPeers, futures);
        }

        const auto elapsedMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        stableRounds = scheduledPeers.size() == oldPeerCount ? stableRounds + 1 : 0;
        if (ShouldStopUrmaWarmup(elapsedMs, stableRounds)) {
            break;
        }
        SleepForWarmupScanInterval(warmupExit_, warmupScanCv_, warmupScanMutex_);
    }

    auto successCount = GetUrmaWarmupSuccessCount(futures);
    const auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    LOG(INFO) << FormatString(
        "[URMA_WARMUP] finished actual_success_count=%zu/total_count=%zu, elapsed_ms=%lld, "
        "discovered_peer_count=%zu",
        successCount, futures.size(), elapsedMs, scheduledPeers.size());
}

Status WorkerOCServer::MaybeStartWorkerMasterRpcWarmup()
{
    if (!EnableOCService()) {
        LOG(INFO) << "[MASTER_RPC_WARMUP] object cache service is disabled, skip warmup.";
        return Status::OK();
    }
    if (topologyEngine_ == nullptr) {
        LOG(WARNING) << "[MASTER_RPC_WARMUP] topology Engine is null, skip warmup.";
        return Status::OK();
    }
    if (objCacheClientWorkerSvc_ == nullptr) {
        LOG(WARNING) << "[MASTER_RPC_WARMUP] object cache service is not ready, skip warmup.";
        return Status::OK();
    }
    masterRpcWarmupExit_ = false;
    std::shared_ptr<ThreadPool> warmupThreadPool;
    // ThreadPool construction exposes std::thread resource failures only through standard exceptions.
    try {
        warmupThreadPool =
            std::make_shared<ThreadPool>(MASTER_RPC_WARMUP_THREAD_NUM, MASTER_RPC_WARMUP_THREAD_NUM, "MasterRpcWarmup");
    } catch (const std::exception &e) {
        LOG(WARNING) << "[MASTER_RPC_WARMUP] create thread pool failed, skip warmup, error=" << e.what();
        return Status::OK();
    }
    std::shared_ptr<ThreadPool> warmupThreadPoolForSubmit;
    {
        std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
        warmingMasterRpcAddrs_.clear();
        warmedMasterRpcMemberIds_.clear();
        pendingMasterRpcWarmupSnapshot_.reset();
        topologySnapshotWarmupScheduled_ = false;
        masterRpcWarmupThreadPool_ = warmupThreadPool;
        warmupThreadPoolForSubmit = masterRpcWarmupThreadPool_;
    }
    try {
        auto warmupTraceID = Trace::Instance().GetTraceID();
        (void)warmupThreadPoolForSubmit->Submit([this, warmupTraceID]() {
            TraceGuard traceGuard = warmupTraceID.empty() ? Trace::Instance().SetTraceUUID()
                                                          : Trace::Instance().SetTraceNewID(warmupTraceID, true);
            WarmupReadyWorkerMasterRpcOnStartup();
        });
    } catch (const std::exception &e) {
        LOG(WARNING) << "[MASTER_RPC_WARMUP] submit startup warmup failed, error=" << e.what();
    }
    return Status::OK();
}

void WorkerOCServer::StopWorkerMasterRpcWarmup()
{
    masterRpcWarmupExit_ = true;
    masterRpcWarmupScanCv_.notify_all();
    std::shared_ptr<ThreadPool> warmupThreadPool;
    {
        std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
        warmupThreadPool.swap(masterRpcWarmupThreadPool_);
        warmingMasterRpcAddrs_.clear();
        warmedMasterRpcMemberIds_.clear();
        pendingMasterRpcWarmupSnapshot_.reset();
        topologySnapshotWarmupScheduled_ = false;
    }
    warmupThreadPool.reset();
}

bool WorkerOCServer::SubmitWorkerMasterRpcWarmupTask(const std::string &masterAddr,
                                                     const std::function<void(const std::string &)> &onSuccess)
{
    if (masterAddr.empty() || masterAddr == hostPort_.ToString()) {
        return false;
    }
    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> objCacheClientWorkerSvc;
    std::shared_ptr<ThreadPool> warmupThreadPool;
    {
        std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
        if (masterRpcWarmupExit_ || masterRpcWarmupThreadPool_ == nullptr || objCacheClientWorkerSvc_ == nullptr) {
            return false;
        }
        if (!warmingMasterRpcAddrs_.emplace(masterAddr).second) {
            return false;
        }
        objCacheClientWorkerSvc = objCacheClientWorkerSvc_;
        warmupThreadPool = masterRpcWarmupThreadPool_;
    }
    auto warmupTask = [this, masterAddr, objCacheClientWorkerSvc, onSuccess]() {
        auto traceGuard =
            Trace::Instance().SetTraceNewID(GetStringUuid().substr(0, SHORT_TRACEID_SIZE) + "-wm-rpc-warmup");
        Raii eraseWarmupAddr([this, masterAddr]() {
            std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
            warmingMasterRpcAddrs_.erase(masterAddr);
        });
        if (masterRpcWarmupExit_) {
            return false;
        }
        HostPort hostPort;
        auto rc = hostPort.ParseString(masterAddr);
        if (rc.IsOk()) {
            rc = objCacheClientWorkerSvc->WarmupWorkerMasterRpc(hostPort);
        }
        if (rc.IsError()) {
            LOG(WARNING) << FormatString("[MASTER_RPC_WARMUP] task failed, master=%s, status=%s", masterAddr,
                                         rc.ToString());
            return false;
        }
        if (masterRpcWarmupExit_) {
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
            warmedMasterRpcMemberIds_.try_emplace(masterAddr);
        }
        if (onSuccess) {
            onSuccess(masterAddr);
        }
        return true;
    };
    try {
        (void)warmupThreadPool->Submit(std::move(warmupTask));
    } catch (const std::exception &e) {
        {
            std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
            warmingMasterRpcAddrs_.erase(masterAddr);
        }
        LOG(WARNING) << FormatString("[MASTER_RPC_WARMUP] submit failed, master=%s, error=%s", masterAddr, e.what());
        return false;
    }
    return true;
}

size_t WorkerOCServer::ScheduleWorkerMasterRpcWarmupTasks(const std::vector<const cluster::Member *> &members,
                                                          const std::unordered_set<std::string> *warmedAddrs,
                                                          const std::function<void(const std::string &)> &onSuccess)
{
    size_t scheduledCount = 0;
    for (const auto *member : members) {
        if (masterRpcWarmupExit_) {
            break;
        }
        const auto &address = member->identity.address;
        if (warmedAddrs != nullptr && warmedAddrs->count(address) > 0) {
            continue;
        }
        // In centralized mode, only warmup the actual master; non-master workers
        // do not register MasterOCServiceBrpcAdapter, leading to "Fail to find method"
        // errors in brpc mode (ZMQ mode silently fails on unknown services).
        if (!FLAGS_enable_distributed_master && address != masterAddr_.ToString()) {
            continue;
        }
        if (SubmitWorkerMasterRpcWarmupTask(address, onSuccess)) {
            ++scheduledCount;
        }
    }
    return scheduledCount;
}

void WorkerOCServer::WarmupReadyWorkerMasterRpcOnStartup()
{
    if (topologyEngine_ == nullptr) {
        return;
    }
    const auto start = std::chrono::steady_clock::now();
    auto warmedAddrs = std::make_shared<std::unordered_set<std::string>>();
    auto warmedAddrsMutex = std::make_shared<std::mutex>();
    auto markWarmupSucceeded = [warmedAddrs, warmedAddrsMutex](const std::string &masterAddr) {
        std::lock_guard<std::mutex> lock(*warmedAddrsMutex);
        warmedAddrs->emplace(masterAddr);
    };
    uint32_t stableRounds = 0;
    size_t totalScheduledCount = 0;
    size_t lastDiscoveredCount = 0;
    size_t lastWarmedCount = 0;
    for (; !masterRpcWarmupExit_;) {
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        auto rc = topologyEngine_->GetSnapshot(snapshot);
        if (rc.IsError()) {
            VLOG(1) << "[MASTER_RPC_WARMUP] topology Snapshot is not ready, status=" << rc.ToString();
        } else {
            size_t oldWarmedCount;
            std::unordered_set<std::string> warmedSnapshot;
            {
                std::lock_guard<std::mutex> lock(*warmedAddrsMutex);
                oldWarmedCount = warmedAddrs->size();
                warmedSnapshot = *warmedAddrs;
            }
            auto scheduledCount =
                ScheduleWorkerMasterRpcWarmupTasks(snapshot->ActiveMembers(), &warmedSnapshot, markWarmupSucceeded);
            totalScheduledCount += scheduledCount;
            {
                std::lock_guard<std::mutex> lock(*warmedAddrsMutex);
                lastWarmedCount = warmedAddrs->size();
            }
            size_t pendingCount;
            {
                std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
                pendingCount = warmingMasterRpcAddrs_.size();
            }
            stableRounds =
                (scheduledCount == 0 && pendingCount == 0 && lastWarmedCount == oldWarmedCount) ? stableRounds + 1 : 0;
            lastDiscoveredCount = snapshot->ActiveMembers().size();
        }

        const auto elapsedMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
        if (ShouldStopUrmaWarmup(elapsedMs, stableRounds)) {
            break;
        }
        SleepForWarmupScanInterval(masterRpcWarmupExit_, masterRpcWarmupScanCv_, masterRpcWarmupScanMutex_);
    }
    const auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    LOG(INFO) << FormatString(
        "[MASTER_RPC_WARMUP] startup finished, scheduled_count=%zu, warmup_target_count=%zu, "
        "discovered_worker_count=%zu, elapsed_ms=%lld",
        totalScheduledCount, lastWarmedCount, lastDiscoveredCount, elapsedMs);
}

void WorkerOCServer::ScheduleTopologySnapshotWarmup(std::shared_ptr<const cluster::TopologySnapshot> snapshot)
{
    if (!EnableOCService() || snapshot == nullptr || masterRpcWarmupExit_) {
        return;
    }
    std::shared_ptr<ThreadPool> warmupThreadPool;
    {
        std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
        if (masterRpcWarmupExit_ || masterRpcWarmupThreadPool_ == nullptr) {
            return;
        }
        if (pendingMasterRpcWarmupSnapshot_ == nullptr
            || snapshot->Version() > pendingMasterRpcWarmupSnapshot_->Version()) {
            pendingMasterRpcWarmupSnapshot_ = std::move(snapshot);
        }
        if (topologySnapshotWarmupScheduled_) {
            return;
        }
        (void)masterRpcWarmupThreadPool_->Submit([this] { DrainTopologySnapshotWarmup(); });
        topologySnapshotWarmupScheduled_ = true;
    }
}

void WorkerOCServer::DrainTopologySnapshotWarmup()
{
    while (!masterRpcWarmupExit_) {
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        {
            std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
            if (masterRpcWarmupExit_ || pendingMasterRpcWarmupSnapshot_ == nullptr) {
                topologySnapshotWarmupScheduled_ = false;
                return;
            }
            snapshot.swap(pendingMasterRpcWarmupSnapshot_);
        }
        WarmupTopologySnapshotMembers(*snapshot);
    }
    std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
    topologySnapshotWarmupScheduled_ = false;
}

void WorkerOCServer::WarmupTopologySnapshotMembers(const cluster::TopologySnapshot &snapshot)
{
    std::unordered_map<std::string, std::string> activeMembers;
    activeMembers.reserve(snapshot.ActiveMembers().size());
    const auto localAddress = hostPort_.ToString();
    const auto centralizedMasterAddress = masterAddr_.ToString();
    size_t successCount = 0;
    size_t failureCount = 0;
    for (const auto *member : snapshot.ActiveMembers()) {
        if (masterRpcWarmupExit_) {
            return;
        }
        const auto &address = member->identity.address;
        if (address == localAddress || (!FLAGS_enable_distributed_master && address != centralizedMasterAddress)) {
            continue;
        }
        activeMembers.emplace(address, member->identity.id);
        {
            std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
            auto iter = warmedMasterRpcMemberIds_.find(address);
            if (iter != warmedMasterRpcMemberIds_.end()
                && (iter->second.empty() || iter->second == member->identity.id)) {
                iter->second = member->identity.id;
                continue;
            }
            if (warmingMasterRpcAddrs_.count(address) > 0) {
                continue;
            }
        }
        HostPort hostPort;
        auto rc = hostPort.ParseString(address);
        if (rc.IsOk()) {
            rc = objCacheClientWorkerSvc_->WarmupWorkerMasterRpc(hostPort);
        }
        if (rc.IsError()) {
            ++failureCount;
            continue;
        }
        if (masterRpcWarmupExit_) {
            return;
        }
        ++successCount;
        std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
        warmedMasterRpcMemberIds_[address] = member->identity.id;
    }
    PruneWorkerMasterRpcWarmupCache(activeMembers);
    VLOG(1) << "[MASTER_RPC_WARMUP] Snapshot version=" << snapshot.Version() << ", success_count=" << successCount
            << ", failure_count=" << failureCount;
}

void WorkerOCServer::PruneWorkerMasterRpcWarmupCache(const std::unordered_map<std::string, std::string> &activeMembers)
{
    std::lock_guard<std::mutex> lock(masterRpcWarmupMutex_);
    for (auto iter = warmedMasterRpcMemberIds_.begin(); iter != warmedMasterRpcMemberIds_.end();) {
        if (activeMembers.count(iter->first) == 0) {
            iter = warmedMasterRpcMemberIds_.erase(iter);
        } else {
            ++iter;
        }
    }
}

Status WorkerOCServer::ReadinessProbe()
{
    // delete health check flag for worker service
    if (FLAGS_ready_check_path.empty()) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_ready_check_path, "~/.datasystem/probe/ready", ""));
    std::string fileDir = FLAGS_ready_check_path.substr(0, FLAGS_ready_check_path.find_last_of('/'));
    if (!FileExist(fileDir)) {
        // Change the permission of "~/.datasystem/probe" to 0700.
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateDir(fileDir, true, 0700), "Create ready check file failed!");
    }
    (void)DeleteFile(FLAGS_ready_check_path);  // Delete file if exist
    RETURN_IF_NOT_OK(WaitForServiceReady());
    RETURN_IF_NOT_OK(WaitForTopologyReady());
    std::ofstream outfile(FLAGS_ready_check_path);
    outfile << "health check success\n" << std::endl;
    outfile.close();
    const mode_t permission = 0600;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(chmod(FLAGS_ready_check_path.c_str(), permission) == 0, K_RUNTIME_ERROR,
                                         FormatString("Chmod of %s error.", FLAGS_ready_check_path));
    return Status::OK();
}

Status WorkerOCServer::Start()
{
    RETURN_IF_NOT_OK_APPEND_MSG(CommonServer::Start(), "\nWorker Start failed.");
    RETURN_IF_NOT_OK(InitLivenessCheck());
    // The task via uds accept fd is started here.
    clientWorkerCommonSvcStatus_ = loadFunctor(*workerSvc_);
    if (IsLocalMetadataMaster()) {
        if (EnableSCService()) {
            RETURN_IF_NOT_OK_APPEND_MSG(streamCacheMasterSvc_->StartCheckMetadata(), "\nmaster Start failed.");
        }
        if (EnableOCService()) {
            RETURN_IF_NOT_OK_APPEND_MSG(objCacheClientWorkerSvc_->WhetherNonRestart(), "\nWorker Start failed.");
        } else {
            RETURN_IF_NOT_OK(WaitForTopologyReady());
            RETURN_IF_NOT_OK(SetHealthProbe());
        }
    } else {
        if (EnableOCService()) {
            RETURN_IF_NOT_OK_APPEND_MSG(objCacheClientWorkerSvc_->WhetherNonRestart(), "\nWorker Start failed.");
        } else {
            RETURN_IF_NOT_OK(WaitForTopologyReady());
            RETURN_IF_NOT_OK(SetHealthProbe());
        }
    }
    RETURN_IF_NOT_OK_APPEND_MSG(metrics::InitKvMetrics(), "\nWorker Start failed: metrics init.");
    workerRuntime_.PublishMetrics();
    RETURN_IF_NOT_OK_APPEND_MSG(MaybeStartConnectionWarmup(), "\nWorker Start failed.");
    RETURN_IF_NOT_OK_APPEND_MSG(MaybeStartWorkerMasterRpcWarmup(), "\nWorker Start failed.");
    RETURN_IF_NOT_OK_APPEND_MSG(ReadinessProbe(), "\nWorker Start failed.");
    RETURN_IF_NOT_OK(ResMetricCollector::Instance().Init());
    RegisteringAllResourceCollectionCallbackFunc();
    ResMetricCollector::Instance().Start();
    return Status::OK();
}

bool WorkerOCServer::IsScaleIn()
{
    const std::string checkFileName = "worker-status";
    checkFilePath_ =
        FLAGS_log_dir.back() == '/' ? (FLAGS_log_dir + checkFileName) : (FLAGS_log_dir + "/" + checkFileName);

    if (FLAGS_enable_lossless_data_exit_mode) {
        return true;
    }

    std::ifstream ifs(checkFilePath_);
    if (!ifs.is_open()) {
        LOG(WARNING) << "Can not open worker status file in " << checkFilePath_
                     << ", voluntary scale in will not start, errno: " << errno;
    } else {
        std::stringstream buffer;
        buffer << ifs.rdbuf();
        ifs.close();
        if (buffer.str() == "voluntary scale in\n") {
            return true;
        }
    }
    return false;
}

bool WorkerOCServer::IsClientsExist()
{
    if (IsHealthy()) {
        return ClientManager::Instance().ExistClientsOnSameNode();
    }
    LOG_FIRST_N(WARNING, 1) << "[Graceful exit] worker is unhealthy, skip check client existence.";
    return false;
}

void WorkerOCServer::WaitClientsExit()
{
    (void)RetryUntilSuccessDuringGracefulExit([this] {
        if (checkThreadRunning_) {
            return IsClientsExist() ? Status(K_NOT_READY, "Still exists clients on the worker.") : Status::OK();
        } else {
            LOG(ERROR) << "Give up waiting for clients' exiting, the worker is unhealthy.";
            return Status::OK();
        }
    });
    LOG(INFO) << "[Graceful exit] All clients on this node have exited.";

    SetUnhealthy();
    allClientsExited_ = true;
    checkAsyncTasksDoneCv_.notify_all();
}

Status WorkerOCServer::PublishExitingMembership()
{
    return RetryUntilSuccessDuringGracefulExit([this] {
        CHECK_FAIL_RETURN_STATUS(topologyEngine_ != nullptr, K_RUNTIME_ERROR, "Topology Engine is null");
        return topologyEngine_->MarkExiting();
    });
}

Status WorkerOCServer::WaitForTopologyRemoval()
{
    return RetryUntilSuccessDuringGracefulExit([this] {
        std::shared_ptr<const cluster::TopologySnapshot> snapshot;
        RETURN_IF_NOT_OK(topologyEngine_->GetSnapshot(snapshot));
        const cluster::Member *local = nullptr;
        auto rc = snapshot->FindMemberByAddress(hostPort_.ToString(), local);
        if (rc.GetCode() == K_NOT_FOUND) {
            return Status::OK();
        }
        RETURN_IF_NOT_OK(rc);
        RETURN_STATUS(K_NOT_READY, "local member is still present in the authoritative topology");
    });
}

Status WorkerOCServer::PreShutDown()
{
    RETURN_OK_IF_TRUE(topologyEngine_ == nullptr);
    INJECT_POINT("worker.PreShutDown.skip");
    bool scaleIn = IsScaleIn();
    auto traceId = Trace::Instance().GetTraceID();
    RETURN_IF_NOT_OK(StartPreShutdownWorkers(scaleIn, traceId));
    WaitForPreShutdownTasks(scaleIn);
    if (scaleIn) {
        RETURN_IF_NOT_OK(PublishExitingMembership());
        RETURN_IF_NOT_OK(WaitForTopologyRemoval());
    }
    if (objCacheClientWorkerSvc_ != nullptr) {
        LOG_IF_ERROR(objCacheClientWorkerSvc_->RemoveWriteBackIdsLocation(), "RemoveWriteBackIdsLocation failed");
    }
    return Status::OK();
}

Status WorkerOCServer::StartPreShutdownWorkers(bool scaleIn, const std::string &traceId)
{
    if (scaleIn) {
        // Close local business and migration admission before starting the drain while membership remains READY.
        topologyExitRequested_.store(true);
        if (objCacheClientWorkerSvc_ != nullptr) {
            const auto deadline = std::chrono::steady_clock::now() + TOPOLOGY_STOP_GRACE;
            LOG_IF_ERROR(objCacheClientWorkerSvc_->CloseIncomingMigrationAdmissionAndWait(deadline),
                         "CloseIncomingMigrationAdmissionAndWait failed");
        }
    }
    if (EnableOCService() || EnableSCService()) {
        RETURN_IF_EXCEPTION_OCCURS(checkAsyncTasksThread_ = std::make_unique<Thread>([this, traceId]() {
                                       TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                                       CheckAsyncTasks();
                                   }));
        checkAsyncTasksThread_->set_name("CheckAsyncTask");
    } else {
        SetCheckAsyncTasksDone(true);
    }

    if (scaleIn) {
        RETURN_IF_EXCEPTION_OCCURS(clientsExitChecker_ = std::make_unique<Thread>([this, traceId]() {
                                       TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                                       WaitClientsExit();
                                   }));
        clientsExitChecker_->set_name("ClientsExitChecker");
        LOG(INFO) << "[Graceful exit] Begin to active reduction node.";
    }
    return Status::OK();
}

void WorkerOCServer::MigrateUnfinishedAsyncDataIfNeeded(bool scaleIn)
{
    if (objCacheClientWorkerSvc_ == nullptr || !scaleIn || checkAsyncTasksDone_
        || objCacheClientWorkerSvc_->AsyncTaskHealth()) {
        return;
    }
    auto traceId = Trace::Instance().GetTraceID();
    auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-migrate-data");
    LOG(INFO) << "[Graceful exit] Async L2 queue need to migrate data to another node";
    auto objectKeys = objCacheClientWorkerSvc_->StopAndGetAllUnfinishedObjects();
    objCacheClientWorkerSvc_->MigrateData(objectKeys, "");
    objCacheClientWorkerSvc_->RemoveAsyncTasks(objectKeys);
    LOG(INFO) << "[Graceful exit] Async L2 queue migrate data to another node finish";
    (void)Trace::Instance().SetTraceNewID(traceId, true);
}

void WorkerOCServer::WaitForPreShutdownTasks(bool scaleIn)
{
    std::unique_lock<std::mutex> lock(checkAsyncTasksDoneMutex_);
    auto shouldExit = [this, scaleIn]() {
        return !scaleIn ? checkAsyncTasksDone_.load() : checkAsyncTasksDone_.load() && allClientsExited_.load();
    };
    bool waitFlag = false;
    while (!waitFlag) {
        if (scaleIn) {
            constexpr int kShutdownProgressLogEvery = 5;
            waitFlag = checkAsyncTasksDone_ && allClientsExited_;
            LOG_EVERY_N(INFO, kShutdownProgressLogEvery)
                << "[Graceful exit] The progress of voluntary scaling down is as follows: "
                << "checkAsyncTasksDone_: " << checkAsyncTasksDone_ << ", allClientsExited_: " << allClientsExited_;
        } else {
            waitFlag = checkAsyncTasksDone_;
        }
        MigrateUnfinishedAsyncDataIfNeeded(scaleIn);
        if (!waitFlag) {
            (void)checkAsyncTasksDoneCv_.wait_for(lock, std::chrono::seconds(1), shouldExit);
        }
    }
}

void WorkerOCServer::StopLivenessCheck()
{
    if (livenessCheck_ != nullptr) {
        livenessCheck_->Stop();
    }
}

void WorkerOCServer::StopRebalanceExecutor()
{
    object_cache::NodeSelector::Instance().UnregisterRebalanceTaskHandler();
    std::lock_guard<std::mutex> lock(rebalanceExecutorMutex_);
    rebalanceExecutor_.reset();
}

Status WorkerOCServer::Shutdown()
{
    INJECT_POINT("worker.BeforeShutdown");
    LOG(INFO) << "Worker process executing a shutdown.";
    StopConnectionWarmup();
    StopWorkerMasterRpcWarmup();
    StopRebalanceExecutor();
    StopLivenessCheck();
    // Stop the background resource collector to prevent the background resource collector from invoking the background
    // resource collector when some objects of the worker exit.
    ResMetricCollector::Instance().Stop();
    // Stop NodeSelector background thread before CommonServer::Shutdown() shuts down
    // the brpc server. NodeSelector::WorkerThread periodically issues CollectClusterInfo
    // RPCs to embedded master; if those RPCs are in flight when brpcServer_->Stop(0)+Join()
    // runs, Join() blocks waiting for them while the RPC targets are already shutting
    // down → shutdown deadlock. Idempotent (also called from ~WorkerOCServiceImpl).
    object_cache::NodeSelector::Instance().Shutdown();
    // Interrupt the service and wait for its completion.
    if (workerSvc_ && IsCallable<datasystem::worker::WorkerServiceImpl>()) {
        InterruptService(*workerSvc_);
        if (clientWorkerCommonSvcStatus_.valid()) {
            clientWorkerCommonSvcStatus_.get();
        }
    }
    // Drain external RPC ingress before stopping the Engine. Rebalance and Worker common-service producers are
    // already stopped above, so no business owner can start a new topology-dependent operation during Engine drain.
    (void)CommonServer::Shutdown();
    const auto stopDeadline = std::chrono::steady_clock::now() + TOPOLOGY_STOP_GRACE;
    RETURN_IF_NOT_OK(StopTopologyRuntime(stopDeadline));
    if (objCacheMasterSvc_) {
        objCacheMasterSvc_->Shutdown();
    }
    if (streamCacheMasterSvc_) {
        streamCacheMasterSvc_->Shutdown();
    }
    // Stop metastore service if it was started
    RETURN_IF_NOT_OK(StopMetaStoreService());
    // Notify the pre-stop script to stop.
    errno = 0;
    std::ofstream ofs(checkFilePath_);
    if (ofs.is_open()) {
        ofs << "worker_stop_status:ready";
    } else {
        LOG(WARNING) << FormatString("Open file %s failed, errno: %d", checkFilePath_, errno);
    }
    ofs.close();
    LOG(INFO) << "Worker shutdown success.";
    return Status::OK();
}

Status WorkerOCServer::GetShmQueueUnit(uint32_t lockId, int &fd, uint64_t &mmapSize, ptrdiff_t &offset, ShmKey &id)
{
    if (!EnableOCService()) {
        fd = -1;
        return Status::OK();
    }
    return objCacheClientWorkerSvc_->GetShmQueueUnit(lockId, fd, mmapSize, offset, id);
}

Status WorkerOCServer::ProcessServerReboot(const ClientKey &clientId, const std::string &tenantId,
                                           const std::string &reqToken,
                                           const google::protobuf::RepeatedPtrField<google::protobuf::Any> &msg)
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    return objCacheClientWorkerSvc_->RecoveryClient(clientId, tenantId, reqToken, msg);
}

void WorkerOCServer::AfterClientLostHandler(const ClientKey &clientId)
{
    INJECT_POINT_NO_RETURN("worker.AfterClientLostHandler");
    auto traceId = Trace::Instance().GetTraceID();
    LOG(INFO) << FormatString("Client %s start exitting", clientId);
    if (objCacheClientWorkerSvc_ != nullptr) {
        LOG_IF_ERROR(objCacheClientWorkerSvc_->RefreshMeta(clientId),
                     FormatString("Failed to RefreshMeta for client:%s", clientId));
    }
    if (streamCacheClientWorkerSvc_ != nullptr) {
        // When a client is lost it uses forceMode true when closing the producers and consumers.
        // Any errors that occur from this close are ignored.
        LOG_IF_ERROR(streamCacheClientWorkerSvc_->ClosePubSubForClientLost(clientId),
                     FormatString("Failed to ClosePubSubForClient: %s ", clientId));
    }
    ClientManager::Instance().RemoveClient(clientId);
}

Status WorkerOCServer::AddClient(const ClientKey &clientId, bool shmEnabled, int32_t socketFd,
                                 const std::string &tenantId, bool enableCrossNode, const std::string &podName,
                                 bool supportMultiShmRefCount, std::string deviceId,
                                 const CompatibilityVersion &compatibilityVersion, uint32_t &lockId,
                                 uint32_t *pipelineQueueId, bool socketHeartbeat)
{
    RETURN_IF_NOT_OK(ClientManager::Instance().AddClient(clientId, shmEnabled, socketFd, tenantId, enableCrossNode,
                                                         podName, std::move(deviceId), compatibilityVersion, lockId,
                                                         pipelineQueueId));
    if (objCacheClientWorkerSvc_ != nullptr) {
        objCacheClientWorkerSvc_->InitShmRefForClient(clientId, supportMultiShmRefCount);
    }
    // SOCKET_HEARTBEAT requires a valid fd-passing socketFd. Fall back to RPC_HEARTBEAT when it is
    // invalid (embedded client, cross-host fallback, shm connection failure, or a non-INVALID fd
    // that the kernel no longer considers open — review 180841385: a stale/invalid fd would slip
    // past the == INVALID_SOCKET_FD guard, then AddFdEvent(EPOLL_CTL_ADD) returns EBADF, the client
    // stays in SOCKET_HEARTBEAT with no epoll monitor and IsClientLost never trips).
    auto isSocketFdValid = [](int fd) {
        return fd != INVALID_SOCKET_FD && fcntl(fd, F_GETFD) != -1;
    };
    if (socketHeartbeat && !isSocketFdValid(socketFd)) {
        LOG(WARNING) << "Client " << clientId << " requested SOCKET_HEARTBEAT but socketFd is invalid"
                     << " (fd=" << socketFd << "); falling back to RPC_HEARTBEAT.";
        socketHeartbeat = false;
    }
    auto hbType = socketHeartbeat ? HeartbeatType::SOCKET_HEARTBEAT : HeartbeatType::RPC_HEARTBEAT;
    Status regRc = ClientManager::Instance().RegisterLostHandler(
        clientId, std::bind(&WorkerOCServer::AfterClientLostHandler, this, clientId), hbType);
    if (regRc.IsError()) {
        // Roll back AddClient's state so a failed RegisterLostHandler does not leak ClientInfo,
        // lockId, pipelineQueueId, the socketFd, and the shm-ref table entry.
        LOG(WARNING) << "RegisterLostHandler failed for client " << clientId << ": " << regRc
                     << "; rolling back AddClient.";
        if (objCacheClientWorkerSvc_ != nullptr) {
            LOG_IF_ERROR(objCacheClientWorkerSvc_->RefreshMeta(clientId),
                         FormatString("RefreshMeta during AddClient rollback for %s", clientId));
        }
        ClientManager::Instance().RemoveClient(clientId);
        return regRc;
    }
    return Status::OK();
}

void WorkerOCServer::CheckRule(bool isAsyncTasksRunning, int &checkNum)
{
    int updateCheckNum = static_cast<int>(FLAGS_check_async_queue_empty_time_s / CHECK_ASYNC_SLEEP_TIME_S);
    // Has async tasks running.
    if (isAsyncTasksRunning) {
        checkNum = updateCheckNum;
        SetCheckAsyncTasksDone(false);
        return;
    }

    // Ensure that the async queue remains empty within FLAGS_check_async_queue_empty_time_s seconds.
    auto lastRequestArrivalTime = RequestCounter::GetInstance().GetLastArrivalTime();
    if (lastRequestArrivalTime_ != lastRequestArrivalTime) {
        LOG(WARNING) << "External requests is coming in[lastRequestArrivalTime: " << lastRequestArrivalTime_
                     << ", thisRequestArrivalTime: " << lastRequestArrivalTime << "], retry...";
        lastRequestArrivalTime_ = lastRequestArrivalTime;
        checkNum = updateCheckNum;
        SetCheckAsyncTasksDone(false);
        return;
    }

    // All async tasks finished, check FLAGS_check_async_queue_empty_time_s / CHECK_ASYNC_SLEEP_TIME_S times.
    if (checkNum > 0) {
        checkNum--;
        return;
    } else {
        LOG(INFO) << "AsyncTasks all finished.";
        checkNum = updateCheckNum;
        SetCheckAsyncTasksDone(true);
    }
}

bool WorkerOCServer::IsAsyncTasksRunning()
{
    // check etcd and persistence async task
    if (IsLocalMetadataMaster()) {
        return (objCacheClientWorkerSvc_ != nullptr && objCacheClientWorkerSvc_->HaveAsyncTasksRunning())
               || (objCacheMasterSvc_ != nullptr && objCacheMasterSvc_->HaveAsyncMetaRequest())
               || (streamCacheClientWorkerSvc_ != nullptr && streamCacheClientWorkerSvc_->HaveTasksToProcess());
    }
    return (objCacheClientWorkerSvc_ != nullptr && objCacheClientWorkerSvc_->HaveAsyncTasksRunning())
           || (streamCacheClientWorkerSvc_ != nullptr && streamCacheClientWorkerSvc_->HaveTasksToProcess());
}

void WorkerOCServer::CheckAsyncTasks()
{
    int checkNum = FLAGS_check_async_queue_empty_time_s / CHECK_ASYNC_SLEEP_TIME_S;
    LOG(INFO) << "Check async task thread start, check times: " << checkNum;
    RequestCounter::GetInstance().StartCount();
    while (checkThreadRunning_) {
        CheckRule(IsAsyncTasksRunning(), checkNum);
        std::this_thread::sleep_for(std::chrono::seconds(CHECK_ASYNC_SLEEP_TIME_S));
    }
}

void WorkerOCServer::SetCheckAsyncTasksDone(bool value)
{
    {
        std::lock_guard<std::mutex> lock(checkAsyncTasksDoneMutex_);
        checkAsyncTasksDone_ = value;
    }
    checkAsyncTasksDoneCv_.notify_all();
}

Status WorkerOCServer::UpdateConfig(const std::string &configJson)
{
    if (runtimeFlags_ == nullptr) {
        return Status(StatusCode::K_RUNTIME_ERROR, "UpdateConfig: worker not initialized");
    }
    if (!FLAGS_monitor_config_file.empty()) {
        return Status(StatusCode::K_INVALID,
                      "UpdateConfig: monitor_config_file must be empty when using UpdateConfig API");
    }
    DynamicConfigUpdater updater(*runtimeFlags_);
    return updater.ApplyJson(configJson, "UpdateConfig");
}

}  // namespace worker
}  // namespace datasystem
