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
 * Description: Implementation of worker server.
 */
#include "datasystem/worker/worker_oc_server.h"

#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

#include "datasystem/common/constants.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_health.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/string_intern/string_pool.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/protobuf_util.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/protos/object_posix.stub.rpc.pb.h"
#include "datasystem/protos/worker_object.service.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/stream_cache/metrics/sc_metrics_monitor.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/worker/worker_liveness_check.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"

DS_DEFINE_string(master_address, "", "Address of ds master and the value cannot be empty.");
DS_DEFINE_bool(enable_distributed_master, true, "Whether to support distributed master, default is true.");
DS_DEFINE_uint32(add_node_wait_time_s, 60, "Time to wait for the first node that wants to join a working hash ring.");
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
DS_DEFINE_bool(async_delete, false, "Master notify workers to delete objects asynchronously.");
DS_DEFINE_uint32(memory_reclamation_time_second, 600, "The memory reclamation time after free.");
DS_DEFINE_bool(cross_az_get_data_from_worker, true, "Control whether try to get data from other AZ's worker firstly.");
DS_DECLARE_uint32(node_timeout_s);
DS_DEFINE_int32(oc_worker_worker_direct_port, 0,
                "Direct tcp/ip port for WorkerWorkerOCService. 0 -- disable this direction connection");
DS_DEFINE_int32(sc_worker_worker_direct_port, 0,
                "Direct tcp/ip port for WorkerWorkerSCService. 0 -- disable this direction connection");
DS_DEFINE_bool(enable_hash_ring_self_healing, false,
               "Whether to support self-healing when the hash ring is in an abnormal state, default is false.");
DS_DEFINE_string(liveness_check_path, "",
                 "File will create after the worker check liveness successfully"
                 "It is used to detect whether the container is liveness in the k8s scenario."
                 "The path length must less than 4095 characters.");
DS_DEFINE_validator(liveness_check_path, &Validator::ValidatePathString);
DS_DEFINE_uint32(liveness_probe_timeout_s, 150, "Liveness probe timeout in seconds.");
DS_DEFINE_uint32(check_async_queue_empty_time_s, 15,
                 "The async queue needs to be empty for a certain period of time before worker can exist.");
DS_DECLARE_string(rocksdb_store_dir);
DS_DEFINE_string(sc_encrypt_secret_key, "",
                 "The encrypted secret key for stream cache. The key length is up to 1024 bytes and must be 32 bytes "
                 "after decryption.");
DS_DEFINE_validator(sc_encrypt_secret_key, &Validator::ValidateScEncryptSecretKey);
DS_DEFINE_int32(max_rpc_session_num, 2048,
                "Maximum number of sessions that can be cached, must be within [512, 10'000]");
DS_DEFINE_validator(max_rpc_session_num, &Validator::ValidateMaxRpcSessionNum);
DS_DEFINE_bool(enable_lossless_data_exit_mode, false,
               "Decide whether to migrate data to other nodes or not when current node exits, default is false.");
DS_DEFINE_bool(shared_memory_populate, false,
               "Avoiding page faults during copying improves runtime performance but may result in longer worker "
               "startup times (depending on shared_memory_size_mb).");
DS_DECLARE_uint32(arena_per_tenant);
DS_DECLARE_bool(enable_fallocate);

DS_DEFINE_int32(oc_worker_worker_parallel_nums, 0, "worker worker batch rsp control nums, default 0 means unlimited");
DS_DEFINE_int32(oc_worker_worker_parallel_min, 100,
                "Min data count for parallel worker worker batch rsp, default is 100");
DS_DEFINE_uint64(oc_worker_aggregate_single_max, 65536,
                 "Max single item size for batching worker worker batch rsp, default is 64KB");
DS_DEFINE_uint64(oc_worker_aggregate_merge_size, 2097152,
                 " Target batch size for worker worker responses, default is 2MB");

static bool ValidatePopulate(const char *flagName, bool value)
{
    if (!value) {
        return true;
    }
    if (FLAGS_arena_per_tenant > 1) {
        LOG(ERROR) << "If " << flagName << " is true, arena_per_tenant must be 1";
        return false;
    }
    if (FLAGS_enable_fallocate) {
        LOG(ERROR) << "If " << flagName << " is true, enable_fallocate must be false";
        return false;
    }
    return true;
}
DS_DEFINE_validator(shared_memory_populate, &ValidatePopulate);
DS_DECLARE_string(sfs_path);
DS_DECLARE_string(cluster_name);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace worker {
constexpr int32_t LIGHTWEIGHT_SERVICE_THREAD_NUM = 4;  // Smaller number of service threads.
constexpr int32_t DEFAULT_STREAM_SOCKET_NUM = 8;       // The default stream socket number.
constexpr double DFT_TIMEOUT_MULT = 1.0;          // A default timeout multiplier iartWorkerf file cache HA is used.
constexpr int32_t THREAD_POOL_SIZE_LIMIT = 4096;  // size limit of the thread pool
constexpr int32_t CHECK_ASYNC_SLEEP_TIME_S = 1;   // Check async task time interval.
static const std::string WORKER_OC_SERVER = "WorkerOcServer";

namespace {
bool EnableOCService()
{
    return FLAGS_rpc_thread_num > 0;
}

bool EnableSCService()
{
    return FLAGS_sc_regular_socket_num > 0 && FLAGS_sc_stream_socket_num > 0;
}
}  // namespace

WorkerOCServer::~WorkerOCServer()
{
    if (replicaManager_ != nullptr) {
        replicaManager_->Shutdown();
    }
    HashRingEvent::DataMigrationReady::GetInstance().RemoveSubscriber(WORKER_OC_SERVER);
    checkThreadRunning_ = false;
    if (checkAsyncTasksThread_ != nullptr) {
        checkAsyncTasksThread_->join();
        checkAsyncTasksThread_.reset();
    }
    if (clientsExitChecker_ != nullptr) {
        clientsExitChecker_->join();
        clientsExitChecker_.reset();
    }
    builder_ = RpcServer::Builder();
    objCacheMasterSvc_.reset();
    objCacheWorkerWkSvc_.reset();
    objCacheWorkerMsSvc_.reset();
    objCacheClientWorkerSvc_.reset();
    streamCacheWorkerWorkerSvc_.reset();
    streamCacheClientWorkerSvc_.reset();
    streamCacheMasterSvc_.reset();
    etcdCM_.reset();
    replicaSvc_.reset();
    datasystem::memory::Allocator::Instance()->Shutdown();
}

Status WorkerOCServer::InitWorkerOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheClientWorkerSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    cfg.udsEnabled_ = FLAGS_ipc_through_shared_memory;
    builder_.AddService(objCacheClientWorkerSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitWorkerWorkerOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheWorkerWkSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    // The external FLAGS_oc_worker_worker_direct_port will not take '*' as input
    // even though ZMQ will accept this value.
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::ValidatePort("FLAGS_oc_worker_worker_direct_port", FLAGS_oc_worker_worker_direct_port), K_INVALID,
        FormatString("Invalid tcp/ip port value %d", FLAGS_oc_worker_worker_direct_port));
    cfg.tcpDirect_ = std::to_string(FLAGS_oc_worker_worker_direct_port);
    builder_.AddService(objCacheWorkerWkSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitMasterWorkerOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheWorkerMsSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    builder_.AddService(objCacheWorkerMsSvc_.get(), cfg);
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
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = FLAGS_sc_regular_socket_num;
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    cfg.udsEnabled_ = FLAGS_ipc_through_shared_memory;
    builder_.AddService(streamCacheClientWorkerSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitWorkerWorkerSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheWorkerWorkerSvc_->Init());
    CHECK_FAIL_RETURN_STATUS(FLAGS_sc_stream_socket_num + FLAGS_sc_regular_socket_num <= THREAD_POOL_SIZE_LIMIT,
                             StatusCode::K_INVALID,
                             "The number of service threads exceeds the upper limit, please adjust it");
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = FLAGS_sc_regular_socket_num;
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        Validator::ValidatePort("FLAGS_sc_worker_worker_direct_port", FLAGS_sc_worker_worker_direct_port), K_INVALID,
        FormatString("Invalid tcp/ip port value %d", FLAGS_sc_worker_worker_direct_port));
    cfg.tcpDirect_ = std::to_string(FLAGS_sc_worker_worker_direct_port);
    builder_.AddService(streamCacheWorkerWorkerSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitMasterWorkerSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheMasterWorkerSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_sc_regular_socket_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = DEFAULT_STREAM_SOCKET_NUM;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    builder_.AddService(streamCacheMasterWorkerSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitWorkerService()
{
    RETURN_IF_NOT_OK(workerSvc_->Init());
    RpcServiceCfg cfg;
    // Explicitly set the regular thread number to the default, so we get matching number of message queues.
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_LIGHT_SERVICE_HWM;
    cfg.udsEnabled_ = false;
    builder_.AddService(workerSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitMasterService()
{
    RETURN_IF_NOT_OK(commonSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    builder_.AddService(commonSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitMasterOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RETURN_IF_NOT_OK(objCacheMasterSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    builder_.AddService(objCacheMasterSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitReplicaService()
{
    RETURN_IF_NOT_OK(replicaSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = LIGHTWEIGHT_SERVICE_THREAD_NUM;
    cfg.numStreamSockets_ = 0;
    builder_.AddService(replicaSvc_.get(), cfg);
    return Status::OK();
}

Status WorkerOCServer::InitMasterSCService()
{
    RETURN_OK_IF_TRUE(!EnableSCService());
    RETURN_IF_NOT_OK(streamCacheMasterSvc_->Init());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = std::max(FLAGS_rpc_thread_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
    cfg.numStreamSockets_ = 0;
    cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
    builder_.AddService(streamCacheMasterSvc_.get(), cfg);
    return Status::OK();
}

#ifdef WITH_TESTS
Status WorkerOCServer::InitUtOCService()
{
    RETURN_OK_IF_TRUE(!EnableOCService());
    RpcServiceCfg cfg;
    cfg.numRegularSockets_ = LIGHTWEIGHT_SERVICE_THREAD_NUM;
    cfg.numStreamSockets_ = 1;  // test stream function, set thread num as 1.
    builder_.AddService(utSvc_.get(), cfg);
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
    commonSvc_->SetClusterManager(etcdCM_.get());
    if (EnableOCService()) {
        // create MasterOCServiceImpl
        objCacheMasterSvc_ =
            std::make_unique<MasterOCServiceImpl>(hostPort_, persistenceApi_, akSkManager_, replicaManager_.get());
        objCacheMasterSvc_->SetClusterManager(etcdCM_.get());
    }
    if (EnableSCService()) {
        // create MasterSCServiceImpl
        rpcSessionManager_ = std::make_shared<RpcSessionManager>();
        streamCacheMasterSvc_ = std::make_unique<MasterSCServiceImpl>(hostPort_, akSkManager_, replicaManager_.get());
        streamCacheMasterSvc_->SetClusterManager(etcdCM_.get());
    }
    if (replicaManager_->MultiReplicaEnabled()) {
        replicaSvc_ = std::make_unique<ReplicationServiceImpl>(hostPort_, replicaManager_.get(), akSkManager_);
    }
}

void WorkerOCServer::CreateWorkerServices()
{
    using ObjectTable = SafeTable<ImmutableString, ObjectInterface>;
    LOG(INFO) << "Start create worker services";

    // Some classes need back pointer to the etcdCM so that they can perform their own health checks or other
    // cluster-related tasks.
    // create WorkerServiceImpl
    workerSvc_ = std::make_unique<WorkerServiceImpl>(hostPort_, masterAddr_, DFT_TIMEOUT_MULT, this, akSkManager_);
    workerSvc_->SetWorkerUuid(etcdCM_->GetLocalWorkerUuid());
    workerSvc_->SetClusterManager(etcdCM_.get());
    auto objectTable = std::make_shared<ObjectTable>();
    auto evictionManager = std::make_shared<object_cache::WorkerOcEvictionManager>(objectTable, hostPort_, masterAddr_,
                                                                                   objCacheMasterSvc_.get());
    if (EnableOCService()) {
        // create WorkerOCServices
        objCacheClientWorkerSvc_ = std::make_shared<datasystem::object_cache::WorkerOCServiceImpl>(
            hostPort_, masterAddr_, objectTable, akSkManager_, evictionManager, persistenceApi_, etcdStore_.get(),
            objCacheMasterSvc_.get());
        objCacheClientWorkerSvc_->SetClusterManager(etcdCM_.get());
        // create WorkerWorkerOCService
        objCacheWorkerWkSvc_ = std::make_shared<datasystem::object_cache::WorkerWorkerOCServiceImpl>(
            objCacheClientWorkerSvc_, akSkManager_, etcdStore_.get(), etcdCM_.get());
        // create MasterWorkerOCService
        objCacheWorkerMsSvc_ = std::make_shared<datasystem::object_cache::MasterWorkerOCServiceImpl>(
            objCacheClientWorkerSvc_, akSkManager_);
    }
    if (EnableSCService()) {
        auto scAllocateManager = std::make_shared<stream_cache::WorkerSCAllocateMemory>(evictionManager);
        // create ClientWorkerSCService
        streamCacheClientWorkerSvc_ = std::make_shared<stream_cache::ClientWorkerSCServiceImpl>(
            hostPort_, masterAddr_, streamCacheMasterSvc_.get(), akSkManager_, scAllocateManager);
        streamCacheClientWorkerSvc_->SetClusterManager(etcdCM_.get());
        // create MasterWorkerSCServiceImpl
        streamCacheMasterWorkerSvc_ = std::make_shared<stream_cache::MasterWorkerSCServiceImpl>(
            hostPort_, masterAddr_, streamCacheClientWorkerSvc_.get(), akSkManager_);
        // create WorkerWorkerSCService
        streamCacheWorkerWorkerSvc_ =
            std::make_unique<stream_cache::WorkerWorkerSCServiceImpl>(streamCacheClientWorkerSvc_.get(), akSkManager_);
    }
}

void WorkerOCServer::CreateAllServices()
{
    // In case of centralized master, create either master or worker services
    if (etcdCM_->IsCurrentNodeMaster()) {
        CreateMasterServices();
        CreateWorkerServices();
    } else {
        CreateWorkerServices();
    }
#ifdef WITH_TESTS
    // create StOCServiceImpl
    utSvc_ = std::make_unique<st::StOCServiceImpl>(objCacheClientWorkerSvc_.get(), etcdCM_.get(), replicaManager_.get(),
                                                   akSkManager_);
#endif
}

Status WorkerOCServer::InitializeMasterServices(const ClusterInfo &clusterInfo)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterService(), "InitMasterService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterOCService(), "InitMasterOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterSCService(), "InitMasterSCService failed");
    if (replicaManager_->MultiReplicaEnabled()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitReplicaService(), "InitReplicaService failed");
    }
    bool isRestart = false;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->IsRestart(isRestart), "Check IsRestart failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(replicaManager_->InitReplicaForStart(isRestart, clusterInfo),
                                     "InitReplicaForStart failed");
    return Status::OK();
}

Status WorkerOCServer::InitializeWorkerServices()
{
    if (FLAGS_enable_lossless_data_exit_mode && !FLAGS_enable_distributed_master) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID, "enable_lossless_data_exit_mode can be set to true only when enable_distributed_master is true");
    }
    // Init the services and hook them up to the RPC server.
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerOCService(), "InitWorkerOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerWorkerOCService(), "InitWorkerWorkerOCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterWorkerOCService(), "InitMasterWorkerOCService failed");
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
    if (etcdCM_->IsCurrentNodeMaster()) {
        EnableLocalBypass();
    }
    // Init the stream services and hook them up to the RPC server
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitClientWorkerSCService(), "InitClientWorkerSCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitMasterWorkerSCService(), "InitMasterWorkerSCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerWorkerSCService(), "InitWorkerWorkerSCService failed");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitWorkerService(), "InitWorkerService failed");
    return Status::OK();
}

Status WorkerOCServer::InitializeAllServices(const ClusterInfo &clusterInfo)
{
    // In case of centralized master, initialize either master or worker services
    if (etcdCM_->IsCurrentNodeMaster()) {
        RETURN_IF_NOT_OK(InitializeMasterServices(clusterInfo));
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

void WorkerOCServer::UpdateClusterInfoInRocksDb(const mvccpb::Event &event)
{
    const auto &key = event.kv().key();
    std::string tableName;
    if (key.find(ETCD_CLUSTER_TABLE) != std::string::npos) {
        tableName = CLUSTER_TABLE;
    } else if (key.find(ETCD_RING_PREFIX) != std::string::npos) {
        tableName = HASHRING_TABLE;
    } else if (event.kv().key().find(ETCD_REPLICA_GROUP_TABLE) != std::string::npos) {
        tableName = REPLICA_GROUP_TABLE;
    } else {
        LOG(ERROR) << "Event of PrefixType::OTHER, no need to enqueue and handle it.";
        return;
    }
    LOG_IF_ERROR(clusterStore_->Put(tableName, key, event.kv().value()), "UpdateClusterInfoInRocksDb failed");
}

Status WorkerOCServer::ConstructClusterStore()
{
    RETURN_IF_NOT_OK(Uri::NormalizePathWithUserHomeDir(FLAGS_rocksdb_store_dir, "~/.datasystem/rocksdb", "/master"));
    std::string clusterInfoRocksDir = FLAGS_rocksdb_store_dir + "/cluster_info";
    if (!FileExist(clusterInfoRocksDir)) {
        // The permission of ~/.datasystem/rocksdb/object_metadata.
        const int permission = 0700;
        RETURN_IF_NOT_OK(CreateDir(clusterInfoRocksDir, true, permission));
    }
    clusterStore_ = RocksStore::GetInstance(clusterInfoRocksDir);
    CHECK_FAIL_RETURN_STATUS(clusterStore_ != nullptr, StatusCode::K_RUNTIME_ERROR,
                             "Init rocksdb instance failed, dir: " + clusterInfoRocksDir);
    RETURN_IF_NOT_OK(clusterStore_->CreateTable(HASHRING_TABLE));
    RETURN_IF_NOT_OK(clusterStore_->CreateTable(REPLICA_GROUP_TABLE));
    RETURN_IF_NOT_OK(clusterStore_->CreateTable(CLUSTER_TABLE));
    return Status::OK();
}

Status WorkerOCServer::ReconcileClusterInfo(
    const HashRingPb &localHashRingPb,
    const std::unordered_map<std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi>, int64_t> &api2Tag)
{
    int reconcileSuccessNum = 0;
    for (const auto &pair : api2Tag) {
        GetClusterStateRspPb rsp;
        auto rc = pair.first->GetClusterStateAsyncRead(pair.second, rsp);
        if (rc.IsError()) {
            LOG(WARNING) << "CheckEtcdStateAsyncRead failed, dest addr: " << pair.first->Address()
                         << ", rc: " << rc.ToString();
            continue;
        }
        CHECK_FAIL_RETURN_STATUS(
            !rsp.etcd_available(), K_RUNTIME_ERROR,
            pair.first->Address() + " confirms that etcd is normal, so this node needs to be restarted.");
        CHECK_FAIL_RETURN_STATUS(CompareMapFields<HashRingPb>(rsp.hash_ring(), localHashRingPb, "workers"),
                                 K_RUNTIME_ERROR,
                                 FormatString("The hash ring is inconsistent, local: %s, remote[%s]: %s",
                                              LogHelper::IgnoreSensitive(localHashRingPb), pair.first->Address(),
                                              LogHelper::IgnoreSensitive(rsp.hash_ring())));
        ++reconcileSuccessNum;
    }
    CHECK_FAIL_RETURN_STATUS(reconcileSuccessNum > 0, K_RUNTIME_ERROR,
                             "The number of nodes with successful reconciliation is not enough to support the start "
                             "of this node. Success num: "
                                 + std::to_string(reconcileSuccessNum));
    return Status::OK();
}

Status WorkerOCServer::LoadHashRingFromRocksDb(ClusterInfo &clusterInfo, HashRingPb &localHashRingPb)
{
    std::vector<std::pair<std::string, std::string>> allHashRings;
    RETURN_IF_NOT_OK(clusterStore_->GetAll(HASHRING_TABLE, allHashRings));
    for (auto itr = allHashRings.begin(); itr != allHashRings.end();) {
        HashRingPb hashRingPb;
        if (!hashRingPb.ParseFromString(itr->second)) {
            return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
        }
        auto azName = GetSubStringBeforeField(itr->first, std::string(ETCD_RING_PREFIX) + "/").erase(0, 1);
        if (!FLAGS_cluster_name.empty() && azName != FLAGS_cluster_name) {
            clusterInfo.otherAzHashrings.emplace_back(std::move(azName), std::move(itr->second));
        } else {
            clusterInfo.localHashRing.emplace_back(std::move(*itr));
            localHashRingPb = std::move(hashRingPb);
        }
        itr = allHashRings.erase(itr);
    }
    return Status::OK();
}

Status WorkerOCServer::LoadWorkersFromRocksDb(ClusterInfo &clusterInfo,
                                              std::vector<std::string> &activeNodesInLocalCluster)
{
    std::vector<std::pair<std::string, std::string>> allWorkers;
    RETURN_IF_NOT_OK(clusterStore_->GetAll(CLUSTER_TABLE, allWorkers));

    for (auto itr = allWorkers.begin(); itr != allWorkers.end();) {
        auto workerAddr = GetSubStringAfterField(itr->first, std::string(ETCD_CLUSTER_TABLE) + "/");
        CHECK_FAIL_RETURN_STATUS(!workerAddr.empty(), K_RUNTIME_ERROR, "The loaded cluster information is incomplete");
        auto azName = GetSubStringBeforeField(itr->first, "/" + std::string(ETCD_CLUSTER_TABLE) + "/").erase(0, 1);
        if (!FLAGS_cluster_name.empty() && azName != FLAGS_cluster_name) {
            clusterInfo.otherAzWorkers.emplace_back(std::move(workerAddr), std::move(itr->second));
        } else {
            if (workerAddr != hostPort_.ToString()) {
                activeNodesInLocalCluster.emplace_back(workerAddr);
            }
            clusterInfo.workers.emplace_back(std::move(workerAddr), std::move(itr->second));
        }
        itr = allWorkers.erase(itr);
    }
    return Status::OK();
}

Status WorkerOCServer::ConstructClusterInfoDuringEtcdCrash(ClusterInfo &clusterInfo)
{
    CHECK_FAIL_RETURN_STATUS(FLAGS_enable_distributed_master, K_RUNTIME_ERROR,
                             "In the centralized master scenario, node restart lamely are not supported.");
    RETURN_IF_NOT_OK(EtcdClusterManager::CreateEtcdStoreTable(etcdStore_.get()));
    clusterInfo.etcdAvailable = false;
    // Load all hash rings from rocksdb and construct localHashRingPb.
    HashRingPb localHashRingPb;
    RETURN_IF_NOT_OK(LoadHashRingFromRocksDb(clusterInfo, localHashRingPb));
    // Load all workers from rocksdb and find active nodes in local cluster.
    std::vector<std::string> activeNodesInLocalCluster;
    RETURN_IF_NOT_OK(LoadWorkersFromRocksDb(clusterInfo, activeNodesInLocalCluster));
    // Load replica group from rocksdb.
    RETURN_IF_NOT_OK(clusterStore_->GetAll(REPLICA_GROUP_TABLE, clusterInfo.replicaGroups));
    // Reconcile hash ring with other nodes in local cluster based on loaded information.
    std::unordered_map<std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi>, int64_t> api2Tag;
    std::stringstream askNodeInfo;
    auto curSize = activeNodesInLocalCluster.size();
    auto needWorkerNum =
        curSize > MAX_QUERY_WORKER_NUM_FOR_RECONCILIATION ? MAX_QUERY_WORKER_NUM_FOR_RECONCILIATION : curSize;
    size_t curWorkerNum = 0;
    for (const auto &activeNode : activeNodesInLocalCluster) {
        std::shared_ptr<object_cache::WorkerRemoteWorkerOCApi> remoteWorkerApi;
        auto rc = CreateRemoteWorkerApi(activeNode, akSkManager_, remoteWorkerApi);
        if (rc.IsError()) {
            LOG(WARNING) << "CreateRemoteWorkerApi failed, dest addr: " << activeNode << ", rc: " << rc.ToString();
            continue;
        }
        GetClusterStateReqPb req;
        int64_t tag;
        rc = remoteWorkerApi->GetClusterStateAsyncWrite(req, tag);
        if (rc.IsError()) {
            LOG(WARNING) << "GetClusterStateAsyncWrite failed, dest addr: " << activeNode << ", rc: " << rc.ToString();
            continue;
        };
        askNodeInfo << activeNode << ";";
        api2Tag.emplace(std::move(remoteWorkerApi), tag);

        if (++curWorkerNum >= needWorkerNum) {
            break;
        }
    }
    LOG(INFO) << "The nodes to be queried are: " << askNodeInfo.str();
    return ReconcileClusterInfo(localHashRingPb, api2Tag);
}

Status WorkerOCServer::ConstructClusterInfo(ClusterInfo &clusterInfo)
{
    auto etcdRc = CheckEtcdHealth(FLAGS_etcd_address);
    if (etcdRc.IsError()) {
        LOG(INFO) << "ETCD fails and the worker tries to run downgraded: " << etcdRc.ToString();
        RETURN_IF_NOT_OK_APPEND_MSG(
            ConstructClusterInfoDuringEtcdCrash(clusterInfo),
            FormatString("The worker does not meet the conditions for starting when etcd fails[cause: %s]. If you want "
                         "to start the worker, etcd must be normal.",
                         etcdRc.GetMsg()));
    } else {
        RETURN_IF_NOT_OK(EtcdClusterManager::ConstructClusterInfoViaEtcd(etcdStore_.get(), clusterInfo));
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

    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::ServerLoadKeys(WORKER_SERVER_NAME, cred));
    builder_.SetCredential(cred);

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
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitializeFastTransportManager(hostPort_),
                                     "Fast transport (URMA/RDMA) init failed");
    RETURN_IF_NOT_OK(RpcStubCacheMgr::Instance().Init(FLAGS_max_rpc_session_num, hostPort_));
    if (IsSupportL2Storage(GetCurrentStorageType())) {
        persistenceApi_ = std::make_shared<PersistenceApi>();
        RETURN_IF_NOT_OK(persistenceApi_->Init());
    }

    replicaManager_ = std::make_unique<ReplicaManager>();

    // EtcdStore is owned by WorkerOcServer. It is used by multiple services.
    CHECK_FAIL_RETURN_STATUS(!FLAGS_etcd_address.empty(), K_RUNTIME_ERROR,
                             "ETCD server address is not given. Fault recovery is not possible");
    FLAGS_etcd_address = ShuffleStringWithDelimiter(FLAGS_etcd_address, ETCD_ADDR_PATTREN);
    LOG(INFO) << "etcd connect address: " << FLAGS_etcd_address;
    etcdStore_ = std::make_unique<EtcdStore>(FLAGS_etcd_address);
    RETURN_IF_NOT_OK(etcdStore_->Init());
    etcdStore_->SetUpdateClusterInfoInRocksDbHandler(
        std::bind(&WorkerOCServer::UpdateClusterInfoInRocksDb, this, std::placeholders::_1));
    // Need to start cluster manager first because many services relies on it, and cluster manager after start-up
    // could assign a master to this node by updating FLAGS_master_address.
    etcdCM_ = std::make_unique<EtcdClusterManager>(hostPort_, masterAddr_, etcdStore_.get(), akSkManager_,
                                                   replicaManager_.get());

    RETURN_IF_NOT_OK(ClientManager::Instance().Init());

    memory::Allocator::Instance()->SetCheckIfAllFdReleasedHandler(
        [](const std::vector<int> &workerFds) { return ClientManager::Instance().IsAllWorkerFdsReleased(workerFds); });

    RETURN_IF_NOT_OK(ConstructClusterStore());
    ClusterInfo clusterInfo;
    RETURN_IF_NOT_OK(ConstructClusterInfo(clusterInfo));
    LOG(INFO) << "The msg in cluster info: " << clusterInfo.ToString();

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdCM_->Init(clusterInfo), "etcd cluster manager init failed");
    LOG(INFO) << "etcd cluster management initialized.";
    if (masterAddr_.Empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(masterAddr_.ParseString(FLAGS_master_address),
                                         "The master_address/etcd_address can not be empty at the same time.");
    }

    CreateAllServices();

    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitReplicaManager(), "replica manager init failed");

    // after setting ETCD cluster manager, it time to initialize all services
    RETURN_IF_NOT_OK(InitializeAllServices(clusterInfo));

    HashRingEvent::DataMigrationReady::GetInstance().AddSubscriber(WORKER_OC_SERVER, [this]() {
        return IsClientsExist() || IsAsyncTasksRunning() ? Status(K_NOT_READY, "Not Ready") : Status::OK();
    });
    LOG(INFO) << "Worker init success.";

    return Status::OK();
}

Status WorkerOCServer::InitLivenessCheck()
{
    if (!FLAGS_liveness_check_path.empty()) {
        const int secToMs = 1000;
        // start liveiness check.
        livenessCheck_ = std::make_unique<WorkerLivenessCheck>(this, FLAGS_liveness_check_path,
                                                               FLAGS_liveness_probe_timeout_s * secToMs, bindHostPort_,
                                                               etcdCM_->GetLocalWorkerUuid(), akSkManager_);
        RETURN_IF_NOT_OK(livenessCheck_->Init());
    }
    return Status::OK();
}

Status WorkerOCServer::InitReplicaManager()
{
    ReplicaManagerParam param;
    param.dbRootPath = FLAGS_rocksdb_store_dir;
    param.currWorkerId = etcdCM_->GetLocalWorkerUuid();
    param.akSkManager = akSkManager_;
    param.etcdStore = etcdStore_.get();
    param.persistenceApi = persistenceApi_;
    param.masterAddress = hostPort_;
    param.etcdCM = etcdCM_.get();
    param.masterWorkerService = objCacheWorkerMsSvc_.get();
    param.workerWorkerService = objCacheWorkerWkSvc_.get();
    param.rpcSessionManager = rpcSessionManager_;
    param.isOcEnabled = EnableOCService();
    param.isScEnabled = EnableSCService();
    return replicaManager_->Init(param);
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
    // The usage of share memory
    instance.RegisterCollectHandler(ResMetricName::SHARED_MEMORY,
                                    []() { return memory::Allocator::Instance()->GetMemoryStatistics(); });
    // The usage of share disk
    instance.RegisterCollectHandler(ResMetricName::SHARED_DISK,
                                    []() { return memory::Allocator::Instance()->GetSharedDiskStatistics(); });
    // The usage of WorkerOCService
    instance.RegisterCollectHandler(ResMetricName::WORKER_OC_SERVICE_THREAD_POOL,
                                    [this]() { return GetRpcServicesUsage("WorkerOCService").ToString(); });
    // The usage of WorkerWorkerOCService
    instance.RegisterCollectHandler(ResMetricName::WORKER_WORKER_OC_SERVICE_THREAD_POOL,
                                    [this]() { return GetRpcServicesUsage("WorkerWorkerOCService").ToString(); });
    // The total number of clients
    instance.RegisterCollectHandler(ResMetricName::ACTIVE_CLIENT_COUNT,
                                    [] { return std::to_string(ClientManager::Instance().GetClientCount()); });

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
        instance.RegisterCollectHandler(ResMetricName::WORKER_SC_SERVICE_THREAD_POOL,
                                        [this]() { return GetRpcServicesUsage("ClientWorkerSCService").ToString(); });

        // The usage of WorkerSCService
        instance.RegisterCollectHandler(ResMetricName::WORKER_WORKER_SC_SERVICE_THREAD_POOL,
                                        [this]() { return GetRpcServicesUsage("WorkerWorkerSCService").ToString(); });

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
    instance.RegisterCollectHandler(ResMetricName::MASTER_WORKER_OC_SERVICE_THREAD_POOL,
                                    [this]() { return GetRpcServicesUsage("MasterWorkerOCService").ToString(); });
    // The usage of MasterOcService
    instance.RegisterCollectHandler(ResMetricName::MASTER_OC_SERVICE_THREAD_POOL,
                                    [this]() { return GetRpcServicesUsage("MasterOCService").ToString(); });

    if (EnableOCService()) {
        if (etcdCM_->IsCurrentNodeMaster()) {
            // The usage of etcd asynchronous write task queue
            instance.RegisterCollectHandler(ResMetricName::ETCD_QUEUE, [this]() {
                auto usage = objCacheMasterSvc_->GetETCDAsyncQueueUsage();
                return usage.empty() ? RES_ETCD_DEFAULT_USAGE : usage;
            });
            // The usage of master asyncPool_
            instance.RegisterCollectHandler(ResMetricName::MASTER_ASYNC_TASKS_THREAD_POOL, [this]() {
                auto usage = objCacheMasterSvc_->GetMasterAsyncPoolUsage();
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
        instance.RegisterCollectHandler(ResMetricName::MASTER_WORKER_SC_SERVICE_THREAD_POOL,
                                        [this]() { return GetRpcServicesUsage("MasterWorkerSCService").ToString(); });
        // The usage of MasterOcService
        instance.RegisterCollectHandler(ResMetricName::MASTER_SC_SERVICE_THREAD_POOL,
                                        [this]() { return GetRpcServicesUsage("MasterSCService").ToString(); });
    }
}

void WorkerOCServer::RegisteringThirdComponentCallbackFunc()
{
    auto &instance = ResMetricCollector::Instance();
    if (EnableOCService()) {
        // The success rate of etcd request
        instance.RegisterCollectHandler(ResMetricName::ETCD_REQUEST_SUCCESS_RATE,
                                        [this]() { return etcdStore_->GetEtcdRequestSuccessRate(); });
    }
    if (TESTFLAG(GetCurrentStorageType(), L2StorageType::OBS)) {
        // The success rate of obs request
        instance.RegisterCollectHandler(ResMetricName::OBS_REQUEST_SUCCESS_RATE,
                                        [this]() { return persistenceApi_->GetL2CacheRequestSuccessRate(); });
    }
}

Status WorkerOCServer::WaitForServiceReady()
{
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
    // Start the heartbeat thread after the server bind and before it start.
    etcdCM_->SetWorkerReady();
    RETURN_IF_NOT_OK_APPEND_MSG(CommonServer::Start(), "\nWorker Start failed.");
    RETURN_IF_NOT_OK(InitLivenessCheck());
    // The task via uds accept fd is started here.
    clientWorkerCommonSvcStatus_ = loadFunctor(*workerSvc_);
    if (etcdCM_->IsCurrentNodeMaster()) {
        if (EnableSCService()) {
            RETURN_IF_NOT_OK_APPEND_MSG(streamCacheMasterSvc_->StartCheckMetadata(), "\nmaster Start failed.");
        }
        if (EnableOCService()) {
            RETURN_IF_NOT_OK_APPEND_MSG(objCacheClientWorkerSvc_->WhetherNonRestart(), "\nWorker Start failed.");
        } else {
            RETURN_IF_NOT_OK(SetHealthProbe());
        }
    } else {
        if (EnableOCService()) {
            RETURN_IF_NOT_OK_APPEND_MSG(objCacheClientWorkerSvc_->WhetherNonRestart(), "\nWorker Start failed.");
        } else {
            RETURN_IF_NOT_OK(SetHealthProbe());
        }
    }
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
    // Set the exiting state to prevent 'frontend' from sending requests to me.
    (void)RetryUntilSuccessDuringGracefulExit([&] {
        if (checkThreadRunning_) {
            LOG(INFO) << "Update node state in etcd to exiting";
            return etcdStore_->UpdateNodeState(ETCD_NODE_EXITING);
        }
        return Status::OK();
    });
    LOG(INFO) << "[Graceful exit] Update node state to exiting.";

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
}

Status WorkerOCServer::PreShutDown()
{
    INJECT_POINT("worker.PreShutDown.skip");
    bool scaleIn = IsScaleIn();
    bool waitFlag = false;
    auto traceId = Trace::Instance().GetTraceID();

    if (EnableOCService() || EnableSCService()) {
        RETURN_IF_EXCEPTION_OCCURS(checkAsyncTasksThread_ = std::make_unique<Thread>([this, traceId]() {
                                       TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                                       CheckAsyncTasks();
                                   }));
        checkAsyncTasksThread_->set_name("CheckAsyncTask");
    } else {
        checkAsyncTasksDone_ = true;
    }

    if (scaleIn) {
        RETURN_IF_EXCEPTION_OCCURS(clientsExitChecker_ = std::make_unique<Thread>([this, traceId]() {
                                       TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
                                       WaitClientsExit();
                                   }));
        clientsExitChecker_->set_name("ClientsExitChecker");
        LOG(INFO) << "[Graceful exit] Begin to active reduction node.";
        RETURN_IF_NOT_OK(RetryUntilSuccessDuringGracefulExit([&] {
            if (!etcdCM_->CheckVoluntaryScaleDown()) {
                return etcdCM_->VoluntaryScaleDown();
            }
            return Status::OK();
        }));
    }

    while (!waitFlag) {
        if (scaleIn) {
            const int logEveryN = 5;
            auto isVoluntaryScaleDown = etcdCM_->CheckVoluntaryScaleDown();
            waitFlag = checkAsyncTasksDone_ && allClientsExited_ && isVoluntaryScaleDown;
            LOG_EVERY_N(INFO, logEveryN) << "[Graceful exit] The progress of voluntary scaling down is as follows: "
                                         << "checkAsyncTasksDone_: " << checkAsyncTasksDone_
                                         << ", allClientsExited_: " << allClientsExited_
                                         << ", isVoluntaryScaleDown: " << isVoluntaryScaleDown;
        } else {
            waitFlag = checkAsyncTasksDone_;
        }
        auto traceId = Trace::Instance().GetTraceID();
        if (scaleIn && !checkAsyncTasksDone_ && !objCacheClientWorkerSvc_->AsyncTaskHealth()) {
            auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-migrate-data");
            LOG(INFO) << "[Graceful exit] Async L2 queue need to migrate data to another node";
            auto objKeys = objCacheClientWorkerSvc_->StopAndGetAllUnfinishedObjects();
            objCacheClientWorkerSvc_->MigrateData(objKeys, "");
            objCacheClientWorkerSvc_->RemoveAsyncTasks(objKeys);
            LOG(INFO) << "[Graceful exit] Async L2 queue migrate data to another node finish";
        }
        (void)Trace::Instance().SetTraceNewID(traceId, true);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG_IF_ERROR(objCacheClientWorkerSvc_->RemoveWriteBackIdsLocation(), "RemoveWriteBackIdsLocation failed");
    return Status::OK();
}

void WorkerOCServer::StopLivenessCheck()
{
    if (livenessCheck_ != nullptr) {
        livenessCheck_->Stop();
    }
}

Status WorkerOCServer::Shutdown()
{
    INJECT_POINT("worker.BeforeShutdown");
    LOG(INFO) << "Worker process executing a shutdown.";
    StopLivenessCheck();
    // Stop the background resource collector to prevent the background resource collector from invoking the background
    // resource collector when some objects of the worker exit.
    ResMetricCollector::Instance().Stop();
    // Interrupt the service and wait for its completion.
    if (workerSvc_ && IsCallable<datasystem::worker::WorkerServiceImpl>()) {
        InterruptService(*workerSvc_);
        if (clientWorkerCommonSvcStatus_.valid()) {
            clientWorkerCommonSvcStatus_.get();
        }
    }

    if (objCacheMasterSvc_) {
        objCacheMasterSvc_->Shutdown();
    }

    if (streamCacheMasterSvc_) {
        streamCacheMasterSvc_->Shutdown();
    }

    if (etcdCM_) {
        Status rc = etcdCM_->Shutdown();
        if (rc.IsError()) {
            LOG(WARNING) << "Shutting down the EtcdClusterManager resulted in Status: " << rc.ToString();
        }
    }
    NotifyShutdownToEtcd();
    if (etcdStore_) {
        Status rc = etcdStore_->Shutdown();
        if (rc.IsError()) {
            LOG(WARNING) << "Shuts down EtcdStore but an error was given. Ignore and continue: " << rc.ToString();
        }
    }
    // CommonServer::Shutdown() only return status ok.
    (void)CommonServer::Shutdown();
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
                                 uint32_t &lockId)
{
    RETURN_IF_NOT_OK(ClientManager::Instance().AddClient(clientId, shmEnabled, socketFd, tenantId, enableCrossNode,
                                                         podName, lockId));
    return ClientManager::Instance().RegisterLostHandler(
        clientId, std::bind(&WorkerOCServer::AfterClientLostHandler, this, clientId), HeartbeatType::RPC_HEARTBEAT);
}

Status WorkerOCServer::GetExclConnSockPath(std::string &sockPath)
{
    return objCacheClientWorkerSvc_->GetExclConnSockPath(sockPath);
}

void WorkerOCServer::CheckRule(bool isAsyncTasksRunning, int &checkNum)
{
    int updateCheckNum = static_cast<int>(FLAGS_check_async_queue_empty_time_s / CHECK_ASYNC_SLEEP_TIME_S);
    // Has async tasks running.
    if (isAsyncTasksRunning) {
        checkNum = updateCheckNum;
        checkAsyncTasksDone_ = false;
        return;
    }

    // Ensure that the async queue remains empty within FLAGS_check_async_queue_empty_time_s seconds.
    auto lastRequestArrivalTime = RequestCounter::GetInstance().GetLastArrivalTime();
    if (lastRequestArrivalTime_ != lastRequestArrivalTime) {
        LOG(WARNING) << "External requests is coming in[lastRequestArrivalTime: " << lastRequestArrivalTime_
                     << ", thisRequestArrivalTime: " << lastRequestArrivalTime << "], retry...";
        lastRequestArrivalTime_ = lastRequestArrivalTime;
        checkNum = updateCheckNum;
        checkAsyncTasksDone_ = false;
        return;
    }

    // All async tasks finished, check FLAGS_check_async_queue_empty_time_s / CHECK_ASYNC_SLEEP_TIME_S times.
    if (checkNum > 0) {
        checkNum--;
        return;
    } else {
        LOG(INFO) << "AsyncTasks all finished.";
        checkAsyncTasksDone_ = true;
        checkNum = updateCheckNum;
    }
}

bool WorkerOCServer::IsAsyncTasksRunning()
{
    // check etcd and persistence async task
    if (etcdCM_->IsCurrentNodeMaster()) {
        return (objCacheClientWorkerSvc_ != nullptr && objCacheClientWorkerSvc_->HaveAsyncTasksRunning())
               || (objCacheMasterSvc_ != nullptr && objCacheMasterSvc_->HaveAsyncMetaRequest())
               || (streamCacheClientWorkerSvc_ != nullptr && streamCacheClientWorkerSvc_->HaveTasksToProcess())
               || (clusterStore_ != nullptr && !clusterStore_->IsAsyncQueueEmpty());
    }
    return (objCacheClientWorkerSvc_ != nullptr && objCacheClientWorkerSvc_->HaveAsyncTasksRunning())
           || (streamCacheClientWorkerSvc_ != nullptr && streamCacheClientWorkerSvc_->HaveTasksToProcess())
           || (clusterStore_ != nullptr && !clusterStore_->IsAsyncQueueEmpty());
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

void WorkerOCServer::NotifyShutdownToEtcd()
{
    if (etcdStore_) {
        constexpr int minRetryTimes = 3;
        constexpr int64_t maxTimeoutMs = minRetryTimes * 60 * 1000;
        int64_t timeoutMs = std::min(static_cast<int64_t>(FLAGS_node_timeout_s * 1000), maxTimeoutMs);
        auto rpcTimeoutMs = std::min(std::max(1'000, static_cast<int>(FLAGS_node_timeout_s * 1000 / minRetryTimes)),
                                     SEND_RPC_TIMEOUT_MS_DEFAULT);
        auto startTime = std::chrono::steady_clock::now();
        std::chrono::time_point<std::chrono::steady_clock> currTime;
        uint64_t retryIntervalMs = 1000;
        do {
            auto rc = etcdStore_->Delete(ETCD_CLUSTER_TABLE, hostPort_.ToString(), 0, rpcTimeoutMs);
            if (rc.IsOk() || rc.GetCode() == StatusCode::K_NOT_FOUND) {
                LOG(INFO) << "Delete " << hostPort_.ToString() << " from etcd with status:" << rc.ToString();
                break;
            } else if (rc.GetCode() == StatusCode::K_RPC_UNAVAILABLE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs));
                currTime = std::chrono::steady_clock::now();
            } else {
                LOG(ERROR) << "Failed to proactively delete own key in etcd during exit process: " << rc.ToString();
                break;
            }
        } while (std::chrono::duration_cast<std::chrono::milliseconds>(currTime - startTime).count() < timeoutMs);
    }
}
}  // namespace worker
}  // namespace datasystem
