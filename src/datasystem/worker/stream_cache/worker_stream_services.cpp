/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/worker/stream_cache/worker_stream_services.h"

#include <cstdint>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/validator.h"

#ifdef BUILD_STREAM_CACHE
#include <algorithm>
#include <securec.h>

#include "datasystem/cluster/executor/topology_phase_callbacks.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/metadata_manager_holder.h"
#include "datasystem/master/stream_cache/sc_metadata_manager.h"
#include "datasystem/master/stream_cache/sc_migrate_metadata_manager.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/master/stream_cache/master_sc_service_impl.h"
#include "datasystem/master/stream_cache/rpc_session_manager.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/master_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "datasystem/worker/stream_cache/worker_worker_sc_service_impl.h"
#include "datasystem/protos/master_stream.brpc.pb.h"
#include "datasystem/protos/stream_posix.brpc.pb.h"
#include "datasystem/protos/worker_stream.brpc.pb.h"
#include "datasystem/protos/stream_posix.auth.pb.h"
#include "datasystem/protos/worker_stream.auth.pb.h"
#include "datasystem/protos/master_stream.auth.pb.h"
#include "datasystem/worker/rpc_service_auth_registration.h"
#include "datasystem/common/log/log.h"
#endif

DS_DECLARE_int32(sc_regular_socket_num);
DS_DECLARE_int32(sc_stream_socket_num);
DS_DECLARE_bool(ipc_through_shared_memory);
DS_DECLARE_int32(sc_worker_worker_direct_port);

namespace {
static bool ValidateSharedPageSize(const char *flagname, uint32_t value)
{
    const int32_t minValue = 1;
    const int32_t maxValue = 16;
    if ((value < minValue) || (value > maxValue)) {
        LOG(ERROR) << "The " << flagname << " must be between " << minValue << " and " << maxValue << ".";
        return false;
    }
    return true;
}

static bool ValidateGroupCount(const char *flagname, uint32_t value)
{
    const int32_t minValue = 1;
    const int32_t maxValue = 64;
    if ((value < minValue) || (value > maxValue)) {
        LOG(ERROR) << "The " << flagname << " must be between " << minValue << " and " << maxValue << ".";
        return false;
    }
    return true;
}
}  // namespace

DS_DEFINE_int32(sc_thread_num, 128, "Number of threads for (non rpc) stream cache service work");
DS_DEFINE_validator(sc_thread_num, &Validator::ValidateThreadNum);
DS_DEFINE_uint32(sc_gc_interval_ms, 50, "Memory resource clean up interval. Default to 50ms");
DS_DEFINE_bool(enable_stream_data_verification, false, "Option to verify if data from a producer is out of order");

DS_DEFINE_int32(remote_send_thread_num, 8, "The num of threads used to send elements to remote worker.");
DS_DEFINE_validator(remote_send_thread_num, &Validator::ValidateThreadNum);

DS_DEFINE_int32(sc_scan_num_buckets, 1024, "Number of partitions for scanning streams");
DS_DEFINE_int32(sc_scan_interval_ms, 10, "Scan interval for remote send. Default to 10ms");
DS_DEFINE_int32(sc_scan_thread_num, 16, "Number of threads for scanning shared memory changes");
DS_DEFINE_validator(sc_scan_thread_num, &Validator::ValidateThreadNum);

DS_DEFINE_uint32(sc_cache_pages, 16, "Default number of cache pages");

DS_DEFINE_uint32(sc_shared_page_size_mb, 4, "the shared page size");
DS_DEFINE_validator(sc_shared_page_size_mb, &ValidateSharedPageSize);

DS_DEFINE_uint32(sc_shared_page_group_count, 4, "the shared page count for each remote worker");
DS_DEFINE_validator(sc_shared_page_group_count, &ValidateGroupCount);

DS_DEFINE_uint32(sc_metrics_log_interval_s, 60, "Interval between logging stream metrics. Default to 60s");
DS_DEFINE_validator(sc_metrics_log_interval_s, &Validator::ValidateUint32);

DS_DEFINE_int32(master_sc_thread_num, 128, "Max number of threads for (non rpc) master stream cache service work");

DS_DEFINE_string(sc_encrypt_secret_key, "",
                 "The encrypted secret key for stream cache. The key length is up to 1024 bytes and must be 32 bytes "
                 "after decryption.");
DS_DEFINE_validator(sc_encrypt_secret_key, &Validator::ValidateScEncryptSecretKey);

namespace datasystem {
namespace worker {
#ifdef BUILD_STREAM_CACHE
namespace {
constexpr int32_t LIGHTWEIGHT_SERVICE_THREAD_NUM = 4;
constexpr int32_t DEFAULT_STREAM_SOCKET_NUM = 8;
constexpr int32_t THREAD_POOL_SIZE_LIMIT = 4096;
}

struct WorkerStreamServices::Impl {
    std::shared_ptr<master::RpcSessionManager> rpcSessionManager_{ nullptr };
    std::unique_ptr<master::MasterSCServiceBrpcAdapter> brpcMasterScAdapter_{ nullptr };
    std::unique_ptr<ClientWorkerSCServiceBrpcAdapter> brpcClientWorkerScAdapter_{ nullptr };
    std::unique_ptr<WorkerWorkerSCServiceBrpcAdapter> brpcWorkerWorkerScAdapter_{ nullptr };
    std::unique_ptr<MasterWorkerSCServiceBrpcAdapter> brpcMasterWorkerScAdapter_{ nullptr };
    std::shared_ptr<stream_cache::ClientWorkerSCServiceImpl> streamCacheClientWorkerSvc_{ nullptr };
    std::shared_ptr<stream_cache::MasterWorkerSCServiceImpl> streamCacheMasterWorkerSvc_{ nullptr };
    std::shared_ptr<stream_cache::WorkerWorkerSCServiceImpl> streamCacheWorkerWorkerSvc_{ nullptr };
    std::unique_ptr<datasystem::master::MasterSCServiceImpl> streamCacheMasterSvc_{ nullptr };
};

WorkerStreamServices::WorkerStreamServices() : impl_(std::make_unique<Impl>())
{
}

bool WorkerStreamServices::Enabled() const
{
    return FLAGS_sc_regular_socket_num > 0 && FLAGS_sc_stream_socket_num > 0;
}

Status WorkerStreamServices::CheckEncryptSecretKey() const
{
    RETURN_OK_IF_TRUE(!Enabled());
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

Status WorkerStreamServices::MigrateTopologyMetadata(const cluster::TopologyCallbackContext &context) const
{
    RETURN_OK_IF_TRUE(!Enabled());
    return master::SCMigrateMetadataManager::Instance().MigrateTopologyMetadata(
        context.action, context.keyFilter, context.businessOperationId, context.deadline, context.cancellation);
}

Status WorkerStreamServices::PrepareFailureActions(MetadataManagerHolder &metadataManagers,
    StreamFailureActions &actions) const
{
    actions = {};
    RETURN_OK_IF_TRUE(!Enabled());
    std::shared_ptr<master::SCMetadataManager> metadata;
    RETURN_IF_NOT_OK(metadataManagers.GetScMetadataManager(metadata));
    RETURN_OK_IF_TRUE(metadata == nullptr);
    actions.recover = [metadata](const cluster::TopologyCallbackContext &context) {
        return metadata->RecoverTopologyFailure(context.action, context.keyFilter, context.businessOperationId,
                                                context.deadline, context.cancellation);
    };
    actions.cleanup = [metadata](const cluster::TopologyCallbackContext &context) {
        return metadata->CleanupTopologyFailedMember(context.action, context.businessOperationId,
                                                     context.deadline, context.cancellation);
    };
    return Status::OK();
}

void WorkerStreamServices::CreateMasterServices(
    const HostPort &host, const std::shared_ptr<AkSkManager> &akSkManager,
    MetadataManagerHolder *holder, const cluster::MembershipEndpointView &membership, bool isRestart)
{
    if (Enabled()) {
        impl_->rpcSessionManager_ = std::make_shared<master::RpcSessionManager>();
        impl_->streamCacheMasterSvc_ = std::make_unique<master::MasterSCServiceImpl>(
            host, akSkManager, holder, membership, host.ToString(), isRestart, true);
    }
}

void WorkerStreamServices::CreateWorkerServices(
    const HostPort &host, const std::shared_ptr<const RequestAuthenticator> &requestAuthenticator,
    const std::shared_ptr<object_cache::WorkerOcEvictionManager> &evictionManager,
    const MetadataRouteResolver &metadataRoute, const cluster::MembershipEndpointView &membership)
{
    if (Enabled()) {
        auto scAllocateManager = std::make_shared<stream_cache::WorkerSCAllocateMemory>(evictionManager);
        impl_->streamCacheClientWorkerSvc_ = std::make_shared<stream_cache::ClientWorkerSCServiceImpl>(
            host, host, impl_->streamCacheMasterSvc_.get(), requestAuthenticator, scAllocateManager,
            metadataRoute, membership);
        impl_->streamCacheMasterWorkerSvc_ = std::make_shared<stream_cache::MasterWorkerSCServiceImpl>(
            host, host, impl_->streamCacheClientWorkerSvc_.get());
        impl_->streamCacheWorkerWorkerSvc_ =
            std::make_unique<stream_cache::WorkerWorkerSCServiceImpl>(
                impl_->streamCacheClientWorkerSvc_.get());
    }
}

void WorkerStreamServices::ConfigureMetadataManagerHolderParam(MetadataManagerHolderParam &param) const
{
    param.rpcSessionManager = impl_->rpcSessionManager_;
}

void WorkerStreamServices::EnableLocalBypass(const HostPort &host)
{
    if (Enabled()) {
        impl_->rpcSessionManager_->SetLocalArgs(host, impl_->streamCacheMasterWorkerSvc_);
    }
}

Status WorkerStreamServices::StartCheckMetadata() const
{
    return impl_->streamCacheMasterSvc_->StartCheckMetadata();
}

void WorkerStreamServices::ShutdownMaster()
{
    if (impl_->streamCacheMasterSvc_) {
        impl_->streamCacheMasterSvc_->Shutdown();
    }
}

void WorkerStreamServices::ReleaseAdapters()
{
    impl_->brpcMasterScAdapter_.reset();
    impl_->brpcClientWorkerScAdapter_.reset();
    impl_->brpcWorkerWorkerScAdapter_.reset();
    impl_->brpcMasterWorkerScAdapter_.reset();
}

void WorkerStreamServices::ReleaseServices()
{
    impl_->streamCacheWorkerWorkerSvc_.reset();
    impl_->streamCacheClientWorkerSvc_.reset();
    impl_->streamCacheMasterSvc_.reset();
}

Status WorkerStreamServices::ClosePubSubForClientLost(const ClientKey &clientId) const
{
    return impl_->streamCacheClientWorkerSvc_ != nullptr
               ? impl_->streamCacheClientWorkerSvc_->ClosePubSubForClientLost(clientId)
               : Status::OK();
}

bool WorkerStreamServices::HaveTasksToProcess() const
{
    return impl_->streamCacheClientWorkerSvc_ != nullptr
           && impl_->streamCacheClientWorkerSvc_->HaveTasksToProcess();
}

Status WorkerStreamServices::InitClientWorkerSCService(
    RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
    const std::shared_ptr<AkSkManager> &akSkManager,
    std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices)
{
    RETURN_OK_IF_TRUE(!Enabled());
    RETURN_IF_NOT_OK(impl_->streamCacheClientWorkerSvc_->Init());
    CHECK_FAIL_RETURN_STATUS(FLAGS_sc_stream_socket_num + FLAGS_sc_regular_socket_num <= THREAD_POOL_SIZE_LIMIT,
                             StatusCode::K_INVALID,
                             "The number of service threads exceeds the upper limit, please adjust it");
    RETURN_IF_NOT_OK(CheckEncryptSecretKey());
    if (rpcServer && rpcServer->IsBrpc()) {
        impl_->brpcClientWorkerScAdapter_ =
            std::make_unique<ClientWorkerSCServiceBrpcAdapter>(*impl_->streamCacheClientWorkerSvc_);
        if (rpcAuthEnabled) {
            RETURN_IF_NOT_OK(RegisterRpcAuthService(
                *rpcServer, *impl_->brpcClientWorkerScAdapter_, *ClientWorkerSCServiceAuth::descriptor(),
                akSkManager, rpcAuthServices));
        } else {
            RETURN_IF_NOT_OK(rpcServer->AddBrpcService(impl_->brpcClientWorkerScAdapter_.get()));
        }
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = FLAGS_sc_regular_socket_num;
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        cfg.udsEnabled_ = FLAGS_ipc_through_shared_memory;
        builder.AddService(impl_->streamCacheClientWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerStreamServices::InitWorkerWorkerSCService(
    RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
    const std::shared_ptr<AkSkManager> &akSkManager,
    std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices)
{
    RETURN_OK_IF_TRUE(!Enabled());
    RETURN_IF_NOT_OK(impl_->streamCacheWorkerWorkerSvc_->Init());
    CHECK_FAIL_RETURN_STATUS(FLAGS_sc_stream_socket_num + FLAGS_sc_regular_socket_num <= THREAD_POOL_SIZE_LIMIT,
                             StatusCode::K_INVALID,
                             "The number of service threads exceeds the upper limit, please adjust it");
    if (rpcServer && rpcServer->IsBrpc()) {
        impl_->brpcWorkerWorkerScAdapter_ =
            std::make_unique<WorkerWorkerSCServiceBrpcAdapter>(*impl_->streamCacheWorkerWorkerSvc_);
        if (rpcAuthEnabled) {
            RETURN_IF_NOT_OK(RegisterRpcAuthService(
                *rpcServer, *impl_->brpcWorkerWorkerScAdapter_, *WorkerWorkerSCServiceAuth::descriptor(),
                akSkManager, rpcAuthServices));
        } else {
            RETURN_IF_NOT_OK(rpcServer->AddBrpcService(impl_->brpcWorkerWorkerScAdapter_.get()));
        }
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = FLAGS_sc_regular_socket_num;
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            Validator::ValidatePort("FLAGS_sc_worker_worker_direct_port", FLAGS_sc_worker_worker_direct_port),
            K_INVALID, FormatString("Invalid tcp/ip port value %d", FLAGS_sc_worker_worker_direct_port));
        cfg.tcpDirect_ = std::to_string(FLAGS_sc_worker_worker_direct_port);
        builder.AddService(impl_->streamCacheWorkerWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerStreamServices::InitMasterWorkerSCService(
    RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
    const std::shared_ptr<AkSkManager> &akSkManager,
    std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices)
{
    RETURN_OK_IF_TRUE(!Enabled());
    RETURN_IF_NOT_OK(impl_->streamCacheMasterWorkerSvc_->Init());
    if (rpcServer && rpcServer->IsBrpc()) {
        impl_->brpcMasterWorkerScAdapter_ =
            std::make_unique<MasterWorkerSCServiceBrpcAdapter>(*impl_->streamCacheMasterWorkerSvc_);
        if (rpcAuthEnabled) {
            RETURN_IF_NOT_OK(RegisterRpcAuthService(
                *rpcServer, *impl_->brpcMasterWorkerScAdapter_, *MasterWorkerSCServiceAuth::descriptor(),
                akSkManager, rpcAuthServices));
        } else {
            RETURN_IF_NOT_OK(rpcServer->AddBrpcService(impl_->brpcMasterWorkerScAdapter_.get()));
        }
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_sc_regular_socket_num, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = DEFAULT_STREAM_SOCKET_NUM;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        builder.AddService(impl_->streamCacheMasterWorkerSvc_.get(), cfg);
    }
    return Status::OK();
}

Status WorkerStreamServices::InitMasterSCService(
    RpcServer *rpcServer, RpcServer::Builder &builder, bool rpcAuthEnabled,
    const std::shared_ptr<AkSkManager> &akSkManager,
    std::vector<std::unique_ptr<BrpcAuthService>> &rpcAuthServices)
{
    RETURN_OK_IF_TRUE(!Enabled());
    RETURN_IF_NOT_OK(impl_->streamCacheMasterSvc_->Init());
    if (rpcServer && rpcServer->IsBrpc()) {
        impl_->brpcMasterScAdapter_ =
            std::make_unique<master::MasterSCServiceBrpcAdapter>(*impl_->streamCacheMasterSvc_);
        if (rpcAuthEnabled) {
            RETURN_IF_NOT_OK(RegisterRpcAuthService(
                *rpcServer, *impl_->brpcMasterScAdapter_, *master::MasterSCServiceAuth::descriptor(),
                akSkManager, rpcAuthServices));
        } else {
            RETURN_IF_NOT_OK(rpcServer->AddBrpcService(impl_->brpcMasterScAdapter_.get()));
        }
    } else {
        RpcServiceCfg cfg;
        cfg.numRegularSockets_ = std::max(FLAGS_brpc_server_num_threads, LIGHTWEIGHT_SERVICE_THREAD_NUM);
        cfg.numStreamSockets_ = 0;
        cfg.hwm_ = RPC_HEAVY_SERVICE_HWM;
        builder.AddService(impl_->streamCacheMasterSvc_.get(), cfg);
    }
    return Status::OK();
}

void WorkerStreamServices::RegisterWorkerResourceMetrics(ResMetricCollector &instance) const
{
    instance.RegisterCollectHandler(ResMetricName::STREAM_COUNT,
                                    [this]() { return impl_->streamCacheClientWorkerSvc_->GetTotalStreamCount(); });

    instance.RegisterCollectHandler(ResMetricName::STREAM_REMOTE_SEND_SUCCESS_RATE,
                                    [this]() {
                                        return impl_->streamCacheClientWorkerSvc_->GetSCRemoteSendSuccessRate();
                                    });

    instance.RegisterCollectHandler(ResMetricName::SC_LOCAL_CACHE, [this]() {
        return impl_->streamCacheWorkerWorkerSvc_->GetUsageMonitor().GetLocalMemoryUsed();
    });
}

#else
struct WorkerStreamServices::Impl {};

WorkerStreamServices::WorkerStreamServices() = default;

bool WorkerStreamServices::Enabled() const
{
    return false;
}

Status WorkerStreamServices::CheckEncryptSecretKey() const
{
    return Status::OK();
}

Status WorkerStreamServices::MigrateTopologyMetadata(const cluster::TopologyCallbackContext &) const
{
    return Status::OK();
}

Status WorkerStreamServices::PrepareFailureActions(MetadataManagerHolder &, StreamFailureActions &actions) const
{
    actions = {};
    return Status::OK();
}
void WorkerStreamServices::CreateMasterServices(
    const HostPort &, const std::shared_ptr<AkSkManager> &, MetadataManagerHolder *,
    const cluster::MembershipEndpointView &, bool)
{
}

void WorkerStreamServices::CreateWorkerServices(
    const HostPort &, const std::shared_ptr<const RequestAuthenticator> &,
    const std::shared_ptr<object_cache::WorkerOcEvictionManager> &,
    const MetadataRouteResolver &, const cluster::MembershipEndpointView &)
{
}

void WorkerStreamServices::ConfigureMetadataManagerHolderParam(MetadataManagerHolderParam &) const
{
}

void WorkerStreamServices::EnableLocalBypass(const HostPort &)
{
}

Status WorkerStreamServices::StartCheckMetadata() const
{
    return Status::OK();
}

void WorkerStreamServices::ShutdownMaster()
{
}

void WorkerStreamServices::ReleaseAdapters()
{
}

void WorkerStreamServices::ReleaseServices()
{
}

Status WorkerStreamServices::ClosePubSubForClientLost(const ClientKey &) const
{
    return Status::OK();
}

bool WorkerStreamServices::HaveTasksToProcess() const
{
    return false;
}

Status WorkerStreamServices::InitClientWorkerSCService(
    RpcServer *, RpcServer::Builder &, bool, const std::shared_ptr<AkSkManager> &,
    std::vector<std::unique_ptr<BrpcAuthService>> &)
{
    return Status::OK();
}

Status WorkerStreamServices::InitWorkerWorkerSCService(
    RpcServer *, RpcServer::Builder &, bool, const std::shared_ptr<AkSkManager> &,
    std::vector<std::unique_ptr<BrpcAuthService>> &)
{
    return Status::OK();
}

Status WorkerStreamServices::InitMasterWorkerSCService(
    RpcServer *, RpcServer::Builder &, bool, const std::shared_ptr<AkSkManager> &,
    std::vector<std::unique_ptr<BrpcAuthService>> &)
{
    return Status::OK();
}

Status WorkerStreamServices::InitMasterSCService(
    RpcServer *, RpcServer::Builder &, bool, const std::shared_ptr<AkSkManager> &,
    std::vector<std::unique_ptr<BrpcAuthService>> &)
{
    return Status::OK();
}

void WorkerStreamServices::RegisterWorkerResourceMetrics(ResMetricCollector &) const
{
}

#endif

WorkerStreamServices::~WorkerStreamServices() = default;
}  // namespace worker
}  // namespace datasystem
